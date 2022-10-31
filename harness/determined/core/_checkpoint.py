import contextlib
import enum
import json
import logging
import os
import pathlib
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

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
    all_resources: List[List[Tuple[str, int]]],
) -> Tuple[List[Tuple[str, int]], Dict[str, List[int]]]:
    """
    Given a list of all resources, return a merged list of resources and a map of conflicts.
    """
    rdict = dict(f for sub in all_resources for f in sub)
    # Detect upload conflicts.
    conflicts = {}
    if len(rdict) < sum(len(sub) for sub in all_resources):
        # Find the conflicts.
        uploaded_by_rank: Dict[str, List[int]] = {}
        for rank, sub in enumerate(all_resources):
            for f, _ in sub:
                uploaded_by_rank.setdefault(f, []).append(rank)
        for f, ranks in uploaded_by_rank.items():
            if len(ranks) > 1:
                conflicts[f] = ranks
    return sorted(rdict.items()), conflicts


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
        self,
        ckpt_dir: Union[str, os.PathLike],
        metadata: Optional[Dict[str, Any]] = None,
        *,
        shard: bool = False,
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

        if not shard and self._dist.rank != 0:
            raise RuntimeError(
                f"cannot call .upload(shard=False) from non-chief worker (rank={self._dist.rank})"
            )

        if ckpt_dir is None and not shard:
            raise RuntimeError(
                "cannot call .upload(ckpt_dir=None, shard=False), which would result in doing "
                "nothing at all"
            )

        ckpt_dir_mask = self._dist.allgather(ckpt_dir is not None)
        if shard and not any(ckpt_dir_mask):
            raise RuntimeError(
                "cannot call .upload(ckpt_dir=None, shard=True), from all ranks; "
                "at least one rank must have a valid ckpt_dir"
            )

        if self._dist.rank == 0:
            storage_id = str(uuid.uuid4())

        if shard:
            storage_id = self._dist.broadcast(storage_id)

        if ckpt_dir is None:
            resources = []
        else:
            ckpt_dir = os.fspath(ckpt_dir)
            resources = self._storage_manager._list_directory(ckpt_dir)

        if shard:
            # Merge resources.
            all_resources = self._dist.gather(resources)
            if self._dist.rank == 0:
                assert all_resources is not None
                merged_resources, file_conflicts = merge_resources(all_resources)
                if file_conflicts:
                    msgs = [
                        f"    {f} uploaded by ranks {ranks}"
                        for f, ranks in sorted(file_conflicts.items())
                    ]
                    # XXX: what about directories? those will frequently collide
                    raise RuntimeError("Overalpping uploads across ranks:\n" + "\n".join(msgs))

                # XXX: Merge metadata.

        # The lowest-ranked worker with a non-None ckpt_dir writes the metadata file.
        metadata_writer_rank = ckpt_dir_mask.index(True)
        if self._dist.rank == metadata_writer_rank:
            # add metadata pre-upload but without counting it among resources
            # XXX: chief is not required to have a ckpt_dir!
            self._write_metadata_file(ckpt_dir, metadata or {})

        if ckpt_dir is not None:
            # XXX: this will result in many duplicate directory creations
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
        *,
        selector: Optional[Callable[[str], bool]] = None,
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
            self._storage_manager.download(src=storage_id, dst=ckpt_dir, selector=selector)
            return

        want_filter = any(self._dist.allgather(selector is not None))

        # LocalWorkersShareDownload case.
        if self._dist.local_rank == 0:

            def _selector(path: str) -> bool:
                if not want_filter:
                    return True
                # If anyone has a selector, coordinate every filename across workers' filters.
                # Functions can't be reliably serialized, so instead we pass each filename between
                # all workers.  But because this traffic is local (unix sockets by default) it
                # should be far faster than any download.
                _ = self._dist.broadcast_local(path)
                return any(self._dist.gather_local(selector(path) if selector is not None else True))

            self._storage_manager.download(src=storage_id, dst=ckpt_dir, selector=_selector)

            # Tell local workers we finished.
            _ = self._dist.broadcast_local(None)
        else:
            while True:
                name = self._dist.broadcast_local(None)
                if name is None:
                    # Chief is done downloading files.
                    break
                assert want_filter, "want_filter is not set but name was not None"
                _ = self._dist.gather_local(selector(name) if selector is not None else True)

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
        self, metadata: Optional[Dict[str, Any]] = None, *, shard: bool = False,
    ) -> Iterator[Tuple[pathlib.Path, str]]:
        """
        ``store_path()`` is a context manager which chooses a random path and prepares a directory
        you should save your model to.  When the context manager exits, the model is automatically
        uploaded (at least, for cloud-backed checkpoint storage backends).

        When ``shard=False``, only the chief worker (``distributed.rank==0``) may call
        ``store_path()``.

        When ``shard=True``, ``store_path()`` becomes a synchronization point between workers, so
        all workers must call store_path(), even workers which will not write any checkpoint files.

        Example:

        .. code::

           if core_context.distributed.rank == 0:
               with core_context.checkpoint.store_path() as (path, storage_id):
                   my_save_model(my_model, path)
                   print(f"done saving checkpoint {storage_id}")
               print(f"done uploading checkpoint {storage_id}")
        """

        if not shard and self._dist.rank != 0:
            raise RuntimeError(
                "cannot call .store_path(shard=False) from non-chief worker "
                f"(rank={self._dist.rank})"
            )

        if self._dist.local_rank == 0:
            # Local chief.
            storage_id = str(uuid.uuid4())
            path = self._storage_manager.pre_store_path(storage_id)
            if shard:
                _ = self._dist.broadcast_local(path)
        else:
            # Local worker.
            path = self._dist.broadcast_local(path)

        yield path, storage_id

        resources = self._storage_manager._list_directory(path)

        if not resources and not shard:
            raise RuntimeError(
                "detected an empty .store_path(ckpt_dir=None, shard=False), which would result in doing "
                "nothing at all"
            )


        all_resources = self._dist.allgather(resources)
        merged_resources, conflicts = merge_resources(all_resources)
        # XXX: all files will conflict in the shared_fs case

        if self._dist.rank == 0:
            # add metadata pre-upload but without counting it among resources
            self._write_metadata_file(path, metadata or {})

        if self._dist.local_rank == 0 and resources:
            # All local chiefs which have something to upload, upload now.
            self._storage_manager.post_store_path(path, storage_id)

        if shard:
            # Synchronize workers before chief worker reports checkpoint.
            _ = self._dist.allgather(None)

        if self._dist.rank == 0:
            # Checkpoint is complete and successful.
            self._report_checkpoint(storage_id, merged_resources, metadata)

    @contextlib.contextmanager
    def restore_path(
        self,
        storage_id: str,
        download_mode: DownloadMode = DownloadMode.LocalWorkersShareDownload,
        *,
        selector: Optional[Callable[[str], bool]] = None,
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
        # TODO: when the StorageManager supports downloading with a file selector, we should attempt
        # to download metadata.json from the checkpoint and read it here.
        raise NotImplementedError(
            "DummyCheckpointContext is not able to read metadata from checkpoint storage yet."
        )
