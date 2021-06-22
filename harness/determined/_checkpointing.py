import determined as det

import logging
log = logging.getLogger("generic")

class Checkpointing:
    """
    Some checkpoint-related REST API wrappers.

    XXX:
      - "resources" -> not required, and webui doesn't call it a 0-byte checkpoint
      - support generic metadata
      - "total_batches" -> generic metadata, which happens to be required for training checkpoints
      - "trial_id" -> generic metadata, which happens to be required for training checkpoints
      - "trial_run_id" -> "run_id" or "task_run_id" or something.
      - "framework" -> generic metadata
      - "format" -> generic metadata
      - can we have a way to update metadata for a checkpoint after-the-fact?
    """

    def __init__(self, session, api_path, static_metadata=None) -> None:
        self._session = session
        self._static_metadata = static_metadata or {}
        self._static_metadata["determined_version"] = det.__version__
        self._api_path = api_path

    def _report_checkpoint(self, uuid, resources=None, metadata=None):
        resources = resources or {}
        metadata = metadata or {}
        required = {"framework", "format", "total_batches"}
        allowed = required.union({ "total_records", "total_epochs"})
        missing = [k for k in required if k not in metadata]
        extra = [k for k in metadata.keys() if k not in allowed]
        if missing:
            raise ValueError(
                "metadata for reported checkpoints, in the current implementation, requires all of "
                f"the following items that have not been provided: {missing}"
            )
        if extra:
            raise ValueError(
                "metadata for reported checkpoints, in the current implementation, cannot support "
                f"the following items that were provided: {extra}"
            )
        # XXX: raising an error here does not exit training (4 slots per trial)

        body = {
            "uuid": uuid,
            "resources": resources,
            **self._static_metadata,
        }

        body.update(metadata)

        log.info(f"report_checkpoint({uuid})")
        self._session.post(self._api_path, body=body)
