import enum
from typing import Optional

import logging
log = logging.getLogger("generic")

from determined.common.api import errors

class EarlyExitReason(enum.Enum):
    INVALID_HP = "EXITED_REASON_INVALID_HP"
    # This is generally unnecessary; just exit early.
    USER_REQUESTED_STOP = "EXITED_REASON_USER_REQUESTED_STOP"

class Training:
    """
    Some training-related REST API wrappers.
    """

    def __init__(self, session, trial_id, run_id, exp_id) -> None:
        self._session = session
        self._trial_id = trial_id
        self._run_id = run_id
        self._exp_id = exp_id

    def set_status(self, status: str) -> None:
        body = {"state": status}
        log.info(f"set_status({status})")
        self._session.post(f"/api/v1/trials/{self._trial_id}/runner/metadata", body=body)

    def get_last_validation(self) -> Optional[int]:
        log.info(f"get_last_validation()")
        r = self._session.get(f"/api/v1/trials/{self._trial_id}")
        bestValidation = r.json()["trial"].get("bestValidation") or {}
        return bestValidation.get("totalBatches")

    # XXX: include "window size" for training metrics too?  Or assume it's "since last report?"
    def report_training_metrics(
            self,
            total_batches,
            metrics,
            *,
            batch_metrics=None,
            total_records=None,
            total_epochs=None,
    ):
        body = {
            "trial_run_id": self._run_id,
            "total_batches": total_batches,
            "metrics": metrics,
        }
        if batch_metrics is not None:
            body["batch_metrics"] = batch_metrics
        if total_records is not None:
            body["total_records"] = total_records
        if total_epochs is not None:
            body["total_epochs"] = total_epochs
        log.info(f"report_training_metrics(total_batches={total_batches}, metrics={metrics})")
        self._session.post(f"/api/v1/trials/{self._trial_id}/training_metrics", body=body)

    # XXX: num_records
    def report_validation_metrics(self, total_batches, metrics, total_records=None, total_epochs=None):
        body = {
            "trial_run_id": self._run_id,
            "total_batches": total_batches,
            "metrics": metrics,
        }
        if total_records is not None:
            body["total_records"] = total_records
        if total_epochs is not None:
            body["total_epochs"] = total_epochs
        log.info(f"report_validation_metrics(total_batches={total_batches}, metrics={metrics})")
        self._session.post(f"/api/v1/trials/{self._trial_id}/validation_metrics", body=body)

    def report_early_exit(self, reason: EarlyExitReason) -> None:
        body = {"reason": EarlyExitReason(reason).value}
        log.info(f"report_early_exit({reason})")
        self._session.post(f"/api/v1/trials/{self._trial_id}/early_exit", body=body)

    def get_experiment_best_validation(self) -> float:
        log.info(f"get_experiment_best_validation()")
        try:
            r = self._session.get(
                f"/api/v1/experiments/{self._exp_id}/searcher/best_searcher_validation_metric"
            )
        except errors.NotFoundException as e:
            return None
        return r.json()["metric"]