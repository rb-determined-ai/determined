import enum
import logging
from typing import Any, Dict, List, Optional

import determined as det
from determined.common.api import errors
from determined.common.experimental.session import Session

logger = logging.getLogger("determined.generic")


class EarlyExitReason(enum.Enum):
    INVALID_HP = "EXITED_REASON_INVALID_HP"
    # This is generally unnecessary; just exit early.
    USER_REQUESTED_STOP = "EXITED_REASON_USER_REQUESTED_STOP"


class Training:
    """
    Some training-related REST API wrappers.
    """

    def __init__(self, session: Session, trial_id: int, run_id: int, exp_id: int) -> None:
        self._session = session
        self._trial_id = trial_id
        self._run_id = run_id
        self._exp_id = exp_id

    def set_status(self, status: str) -> None:
        body = {"state": status}
        logger.debug(f"set_status({status})")
        self._session.post(f"/api/v1/trials/{self._trial_id}/runner/metadata", json=body)

    def get_last_validation(self) -> Optional[int]:
        r = self._session.get(f"/api/v1/trials/{self._trial_id}")
        latest = r.json()["trial"].get("latestValidation") or {}
        latest_total_batches = latest.get("totalBatches")
        logger.debug(f"get_last_validation() -> {latest_total_batches}")
        return latest_total_batches

    def report_training_metrics(
        self,
        total_batches: int,
        metrics: Dict[str, Any],
        *,
        batch_metrics: Optional[List] = None,
        total_records: Optional[int] = None,
        total_epochs: Optional[int] = None,
        computed_records: Optional[int] = None,
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
        if computed_records is not None:
            body["computed_records"] = computed_records
        logger.info(f"report_training_metrics(total_batches={total_batches}, metrics={metrics})")
        self._session.post(
            f"/api/v1/trials/{self._trial_id}/training_metrics",
            data=det.util.json_encode(body),
        )

    def report_validation_metrics(
        self,
        total_batches: int,
        metrics: Dict[str, Any],
        *,
        total_records: Optional[int] = None,
        total_epochs: Optional[int] = None,
        computed_records: Optional[int] = None,
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
        if computed_records is not None:
            body["computed_records"] = computed_records
        logger.info(f"report_validation_metrics(total_batches={total_batches}, metrics={metrics})")
        self._session.post(
            f"/api/v1/trials/{self._trial_id}/validation_metrics",
            data=det.util.json_encode(body),
        )

    def report_early_exit(self, reason: EarlyExitReason) -> None:
        body = {"reason": EarlyExitReason(reason).value}
        logger.info(f"report_early_exit({reason})")
        r = self._session.post(
            f"/api/v1/trials/{self._trial_id}/early_exit",
            data=det.util.json_encode(body),
        )
        if r.status_code == 400:
            logger.warn("early exit has already been reported for this trial, ignoring new value")

    def get_experiment_best_validation(self) -> Optional[float]:
        logger.debug("get_experiment_best_validation()")
        try:
            r = self._session.get(
                f"/api/v1/experiments/{self._exp_id}/searcher/best_searcher_validation_metric"
            )
        except errors.NotFoundException:
            # 404 means 'no validations yet'.
            return None
        return float(r.json()["metric"])
