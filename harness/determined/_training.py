import enum
import logging
from typing import Any, Dict, List, Optional

from determined.common.api import errors
from determined.experimental import client

log = logging.getLogger("generic")


class EarlyExitReason(enum.Enum):
    INVALID_HP = "EXITED_REASON_INVALID_HP"
    # This is generally unnecessary; just exit early.
    USER_REQUESTED_STOP = "EXITED_REASON_USER_REQUESTED_STOP"


class Training:
    """
    Some training-related REST API wrappers.
    """

    def __init__(self, session: client.Session, trial_id: int, run_id: int, exp_id: int) -> None:
        self._session = session
        self._trial_id = trial_id
        self._run_id = run_id
        self._exp_id = exp_id

    def set_status(self, status: str) -> None:
        body = {"state": status}
        log.info(f"set_status({status})")
        self._session.post(f"/api/v1/trials/{self._trial_id}/runner/metadata", body=body)

    def get_last_validation(self) -> Optional[int]:
        log.info("get_last_validation()")
        r = self._session.get(f"/api/v1/trials/{self._trial_id}")
        bestValidation = r.json()["trial"].get("bestValidation") or {}
        return bestValidation.get("totalBatches")

    # XXX: include "window size" for training metrics too?  Or assume it's "since last report?"
    def report_training_metrics(
        self,
        total_batches: int,
        metrics: Dict[str, Any],
        *,
        batch_metrics: Optional[List] = None,
        total_records: Optional[int] = None,
        total_epochs: Optional[int] = None,
    ) -> None:
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
    def report_validation_metrics(
        self,
        total_batches: int,
        metrics: Dict[str, Any],
        *,
        total_records: Optional[int] = None,
        total_epochs: Optional[int] = None,
    ) -> None:
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

    def get_experiment_best_validation(self) -> Optional[float]:
        log.info("get_experiment_best_validation()")
        try:
            r = self._session.get(
                f"/api/v1/experiments/{self._exp_id}/searcher/best_searcher_validation_metric"
            )
        except errors.NotFoundException:
            # 404 means 'no validations yet'.
            return None
        return float(r.json()["metric"])
