import contextlib
import enum
import json
import logging
import os
import pathlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Tuple, Union

from determined import core, tensorboard
from determined.common import storage
from determined.common.api import bindings
from determined.common.experimental.session import Session

logger = logging.getLogger("determined.core")


class DownloadMode(enum.Enum):
    """
    ``DownloadMode`` defines the calling behavior of the .download() and the .restore_path() methods
    of ``CheckpointContext``.

    When mode is ``LocalWorkersShareDownload`` (the default), workers on the same physical node (the
    same ``distributed.cross_rank``) will share a single downloaded version of the checkpoint.  On
    an 8-GPU node, this will frequently result in 8x bandwidth savings.  In this mode, all workers
    must call ``.download()`` or ``.restore_path()`` in step.

    When mode is ``NoSharedDownload``, no coordination is done.  This is useful if you either have
    configured your own coordination, or if only a single worker needs a particular checkpoint.
    There is no in-step calling requirement.
    """

    LocalWorkersShareDownload = "LOCAL_WORKERS_SHARE_DOWNLOAD"
    NoSharedDownload = "NO_SHARED_DOWNLOAD"

def merge_resources(
    all_resources: List[List[Tuple[str, int]]], rank: int
) -> Tuple[List[Tuple[str, int]], Dict[str, List[int]]]:
    """
    Given a list of all resources, return:
      - a merged list of resources
      - a dict mapping conflicting files to ranks that would upload them

    Note that we allow multiple ranks to upload directories, but only one rank may upload any
    given file.
    """
    files = set()
    uploaders = {}
    merged = {}
    for rank, rscs in enumerate(all_resources):
        files_this_rank = []
        for name, size in rscs:
            if name.endswith(os.sep):
                # Dir name.
                stripped = name.rstrip("/")
                uploaders.setdefault(stripped, []).append(rank)
            else:
                # File name.
                files.add(name)
                uploaders.setdefault(name, []).append(rank)

            merged[name] = size

    # Overlapping name situations:
    #
    #  A uploads |  B uploads | Conflict |  Detection
    # -----------|------------|----------|-------------------------------
    #  dir       |  dir       | no       |  n/a
    #  dir       |  file      | yes      |  len(uploaders[name]) > 1
    #  file      |  file      | yes      |  len(uploaders[name]) > 1
    #
    # Conclusion: all conflicts can be detected by checking the names in `files`.
    conflicts = {}
    for name in files:
        uploading_ranks = uploaders[name]
        if len(uploading_ranks) > 1:
            conflicts[name] = uploading_ranks

    return merged, conflicts


class CheckpointContext:
    """
    ``CheckpointContext`` gives access to checkpoint-related features of a Determined cluster.
    """

    def __init__(
        self,
        dist: core.DistributedContext,
        storage_manager: storage.StorageManager,
        session: Session,
        task_id: str,
        allocation_id: str,
        tbd_sync_mode: core.TensorboardMode,
        tensorboard_manager: tensorboard.TensorboardManager,
    ) -> None:
        self._dist = dist
        self._storage_manager = storage_manager
        self._session = session
        self._task_id = task_id
        self._allocation_id = allocation_id
        self._tensorboard_mode = tbd_sync_mode
        self._tensorboard_manager = tensorboard_manager

    def upload(
        self, ckpt_dir: Union[str, os.PathLike], metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        ``upload()`` chooses a random ``storage_id``, then uploads the contents of ``ckpt_dir`` to
        checkpoint storage into a directory by the name of the ``storage_id``.  The name of the
        ``ckpt_dir`` is not preserved.

        When ``shard=False``, only the chief worker (``distributed.rank==0``) may call ``upload()``.

        When ``shard=True``, ``upload()`` becomes a synchronization point between workers, so all
        workers must call upload().  Those workers with nothing to upload may pass
        ``ckpt_dir=None``.  The final checkpoint stored in checkpoint storage will contain a union
        of the contents from each ckpt_dir.

        Returns:  The ``storage_id`` for this checkpoint.
        """
        if ckpt_dir is not None:
            ckpt_dir = os.fspath(ckpt_dir)

        if not shard and self._dist.rank != 0:
            raise RuntimeError(
                f"cannot call .upload(shard=False) from non-chief worker (rank={self._dist.rank})"
            )

        if ckpt_dir is None and not shard:
            raise RuntimeError(
                "cannot call .upload(ckpt_dir=None, shard=False), which would result in doing "
                "nothing at all"
            )

        if shard:
            ckpt_dir_mask = self._dist.allgather(ckpt_dir is not None)
            if not any(ckpt_dir_mask):
                raise RuntimeError(
                    "cannot call .upload(ckpt_dir=None, shard=True), from all ranks; "
                    "at least one rank must have a valid ckpt_dir"
                )
        else:
            ckpt_dir_mask = [True]

        if self._dist.rank == 0:
            storage_id = str(uuid.uuid4())

        if shard:
            storage_id = self._dist.broadcast(storage_id)

        # Deconflict locally-shared directories; if every worker uploads /tmp/ckpt, then only
        # the lowest rank on each node will actually upload this directory.
        if shard:
            if ckpt_dir is None:
                file_uid = None
            else:
                st = os.stat(ckpt_dir)
                file_uid = (st.st_dev, st.st_ino)
            all_file_uids = self._dist.allgather(file_uid)
            # Decide if our rank is the lowest rank trying to upload this ckpt_dir.
            want_upload = all_file_uids.index(file_uid) == self._dist.rank
        else:
            want_upload = True

        # Decide what we are going to upload.
        if not want_upload or ckpt_dir is None:
            resources = []
        else:
            resources = self._storage_manager._list_directory(ckpt_dir)

        if shard:
            all_resources = self._dist.allgather(resources)

            merged_resources, conflicts = merge_resources(all_resources, self._dist.rank)
            if conflicts:
                # Try to keep the logs easier to read; print the whole failure only on the chief.
                if self._dist.rank > 0:
                    raise RuntimeError("refusing to upload with file conflicts")
                msgs = [
                    f"    {f} uploaded by ranks {ranks}"
                    for f, ranks in sorted(conflicts.items())
                ]
                raise RuntimeError("refusing to upload with file conflicts:\n" + "\n".join(msgs))
        else:
            merged_resources = resources

        # The lowest-ranked worker with a non-None ckpt_dir wirtes to the metadata file.
        metadata_writer_rank = ckpt_dir_mask.index(True)
        if self._dist.rank == metadata_writer_rank:
            assert ckpt_dir is not None:
            # Add metadata pre-upload but without counting it among resources
            self._write_metadatanfile(ckpt_dir, metadata or {})
            # XXX: merge metadata too

        if ckpt_dir is not None:
            self._storage_manager.upload(src=ckpt_dir, dst=storage_id)

        # synchronize workers
        _ = self._dist.allgather(None)

        if self._dist.rank == 0:
            self._report_checkpoint(storage_id, merged_resources, metadata)

        return storage_id

    def download(
        self,
        storage_id: str,
        ckpt_dir: Union[str, os.PathLike],
        download_mode: DownloadMode = DownloadMode.LocalWorkersShareDownload,
    ) -> None:
        """
        Download the contents of a checkpoint from checkpoint storage into a directory specified by
        ``ckpt_dir``, which is created if it does not exist.

        .. note::

            This ``.download()`` method is similar to but less flexible than the ``.download()``
            method of the :class:`~determined.experiment.common.Checkpoint` class in the Determined
            Python SDK.  This ``.download()`` is here as a convenience.
        """
        ckpt_dir = os.fspath(ckpt_dir)
        download_mode = DownloadMode(download_mode)

        if download_mode == DownloadMode.NoSharedDownload:
            self._storage_manager.download(src=storage_id, dst=ckpt_dir)
            return

        # LocalWorkersShareDownload case.
        if self._dist.local_rank == 0:
            self._storage_manager.download(src=storage_id, dst=ckpt_dir)
            # Tell local workers we finished.
            _ = self._dist.broadcast_local(None)
        else:
            # Wait for chief to finish.
            _ = self._dist.broadcast_local(None)

    def get_metadata(self, storage_id: str) -> Dict[str, Any]:
        """
        Returns the current metadata associated with the checkpoint.
        """

        resp = bindings.get_GetCheckpoint(self._session, checkpointUuid=storage_id)
        if not resp.checkpoint or not resp.checkpoint.metadata:
            return {}
        return resp.checkpoint.metadata

    @contextlib.contextmanager
    def store_path(
        self, metadata: Optional[Dict[str, Any]] = None
    ) -> Iterator[Tuple[pathlib.Path, str]]:
        """
        ``store_path()`` is a context manager which chooses a random path and prepares a directory
        you should save your model to.  When the context manager exits, the model is automatically
        uploaded (at least, for cloud-backed checkpoint storage backends).

        Note that with multiple workers, only the chief worker (``distributed.rank==0``) is allowed
        to call ``store_path()``.

        Example:

        .. code::

           with core_context.checkpoint.store_path() as (path, storage_id):
               my_save_model(my_model, path)
               print(f"done saving checkpoint {storage_id}")
           print(f"done uploading checkpoint {storage_id}")
        """

        if self._dist.rank != 0:
            raise RuntimeError(
                "cannot call CheckpointContext.store_path() from non-chief worker "
                f"(rank={self._dist.rank})"
            )

        storage_id = str(uuid.uuid4())
        with self._storage_manager.store_path(storage_id) as path:
            yield path, storage_id
            resources = self._storage_manager._list_directory(path)
            self._write_metadata_file(os.fspath(path), metadata or {})

        self._report_checkpoint(storage_id, resources, metadata)

    @contextlib.contextmanager
    def restore_path(
        self,
        storage_id: str,
        download_mode: DownloadMode = DownloadMode.LocalWorkersShareDownload,
    ) -> Iterator[pathlib.Path]:
        """
        ``restore_path()`` is a context manager which downloads a checkpoint (if required by the
        storage backend) and cleans up the temporary files afterwards (if applicable).

        In multi-worker scenarios, with the default ``download_mode``
        (``LocalWorkersShareDownload``), all workers must call ``restore_path()`` but only the local
        chief worker on each node (``distributed.local_rank==0``) actually downloads data.

        Example:

        .. code::

           with core_context.checkpoint.restore_path(my_checkpoint_uuid) as path:
               my_model = my_load_model(path)
        """
        download_mode = DownloadMode(download_mode)

        if download_mode == DownloadMode.NoSharedDownload:
            with self._storage_manager.restore_path(storage_id) as path:
                yield path
            return

        # LocalWorkersShareDownload case.
        if self._dist.local_rank == 0:
            with self._storage_manager.restore_path(storage_id) as path:
                # Broadcast to local workers.
                _ = self._dist.broadcast_local(path)
                try:
                    yield path
                finally:
                    # Wait for local workers to finish.
                    _ = self._dist.gather_local(None)
        else:
            # Wait for local chief to broadcast.
            path = self._dist.broadcast_local(None)
            try:
                yield path
            finally:
                # Tell local chief we're done.
                _ = self._dist.gather_local(None)

    def delete(self, storage_id: str) -> None:
        """
        Delete a checkpoint from the storage backend.
        """
        self._storage_manager.delete(storage_id)

    def _write_metadata_file(self, ckpt_dir: str, metadata: Dict[str, Any]) -> None:
        metadata_path = pathlib.Path(ckpt_dir).joinpath("metadata.json")
        with metadata_path.open("w") as f:
            json.dump(metadata, f, indent=2)

    def _report_checkpoint(
        self,
        storage_id: str,
        resources: Optional[Dict[str, int]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        After having uploaded a checkpoint, report its existence to the master.
        """
        resources = resources or {}
        metadata = metadata or {}

        if "steps_completed" not in metadata:
            raise ValueError(
                "metadata for reported checkpoints, in the current implementation, requires a "
                "'steps_completed' item, which has not been provided"
            )

        ckpt = bindings.v1Checkpoint(
            allocationId=self._allocation_id,
            metadata=metadata,
            resources={k: str(v) for k, v in resources.items()},
            taskId=self._task_id,
            training=bindings.v1CheckpointTrainingMetadata(),
            uuid=storage_id,
            reportTime=datetime.now(timezone.utc).isoformat(),
            state=bindings.determinedcheckpointv1State.STATE_COMPLETED,
        )
        bindings.post_ReportCheckpoint(self._session, body=ckpt)
        logger.info(f"Reported checkpoint to master {storage_id}")

        # Also sync tensorboard.
        if self._tensorboard_mode == core.TensorboardMode.AUTO:
            self._tensorboard_manager.sync()


class DummyCheckpointContext(CheckpointContext):
    def __init__(
        self,
        dist: core.DistributedContext,
        storage_manager: storage.StorageManager,
    ) -> None:
        self._dist = dist
        self._storage_manager = storage_manager

    def _report_checkpoint(
        self,
        storage_id: str,
        resources: Optional[Dict[str, int]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        # No master to report to; just log the event.
        logger.info(f"saved checkpoint {storage_id}")

    def get_metadata(self, storage_id: str) -> Dict[str, Any]:
        # TODO: when the StorageManager supports downloading with a file filter, we should attempt
        # to download metadata.json from the checkpoint and read it here.
        raise NotImplementedError(
            "DummyCheckpointContext is not able to read metadata from checkpoint storage yet."
        )
