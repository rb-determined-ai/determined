import logging
import math
import pathlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast

import logging
import socket
import ssl
from typing import Any, Optional

import lomond
import lomond.session
import simplejson

import determined as det
from determined import layers, push, tensorboard, util, workload
from determined.common import storage, check

# class _GenericContext:
#     def __init__(
#         self,
#         env: det.EnvContext,
#         rendezvous_info: det.RendezvousInfo,
#         storage_manager: storage.StorageManager,
#         tensorboard_mgr: tensorboard.TensorboardManager,
#         metric_writer: tensorboard.BatchMetricWriter,
#     ) -> None:
#         self._env = env
#         self._rendezvous_info = rendezvous_info
#         self._storage_manager = storage_manager
#         self._tensorboard_mgr = tensorboard_mgr
#         self._metric_writer = metric_writer
#
#         self._api_client = push.PushClient()

WorkloadStreamElem = Tuple[
    workload.Workload, workload.Args, workload.ResponseFunc
]

WorkloadGenerator = Generator[WorkloadStreamElem, None, bool]

def yield_and_await_response(
    wkld: workload.Workload
) -> Generator[WorkloadStreamElem, None, workload.Metrics]:
    out: Optional[workload.Metrics] = None

    def respond(r: workload.Response) -> None:
        assert not isinstance(r, workload.SkippedWorkload)
        nonlocal response
        response = r

    yield wkld, [], respond

    assert out is not None

    return out


class Length:
    @property
    def units(self):
        pass

    @property
    def unit(self):
        pass


class UnitContext:
    pass


class WorkloadSequencer(workload.Source):
    """
    WorkloadSequencer is the python rewrite of the old golang
    TrialWorkloadSequencer.

    Like the go version, it fuses the dual stream of searcher operations +
    descheduling decisions into a single stream of Workload events.

    When the sequencer was in the master, the resulting stream of Workloads was
    the basis for all master/harness communications, but now that the sequencer
    lives in the harness, all master/harness communications are over the new
    push APIs.

    This Workoad stream (and the whole WorkloadSequencer) is only even needed
    for reverse-compatibility with the old TrialControllers that we don't care
    to update (TFKerasTrial and EstimatorTrial).
    """

    class SavableState:
        def __init__(
            self,
            want_initial_val,
            step_id,
            last_val,
            batches_completed,
        ):
            self.want_initial_val = want_initial_val
            self.step_id = step_id
            self.last_val = last_val
            self.last_ckpt = last_ckpt
            self.batches_completed = batches_completed

    def __init__(
        self,
        env: det.EnvContext,
        rendezvous_info: det.RendezvousInfo,
    ) -> None:
        self.env = env
        self.rendezvous_info = rendezvous_info
        self.api_client = push.PushClient()

        self.searcher_op: Optional[SearcherOp] = None

        self.state = self.SavableState(
            want_initial_val = self.env.exp_config.get(
                "perform_initial_validation", False
            )
        )

        self.ckpt_policy = self.env.exp_config.get(
            "checkpoint_policy", "best"
        )

        # precalculated periods, in batches
        self.min_val_period_batches = ...
        self.min_ckpt_period_batches = ...

        if self.load_path is not None:
            raise ValueError("I don't know how to load state yet")

    def get_state(self) -> Any:
        return vars(self.state)

    def load_state(self, state: Any) -> None:
        self.state = self.SavableState(**state)

    def train(self, num_batches: int) -> WorkloadGenerator:
        self.api_client.begin_training()

        wkld = workload.Workload(
            kind=workload.Workload.Kind.RUN_STEP,
            e_id=self.env.det_experiment_id,
            t_id=self.env.det_trial_id,
            s_id=self.state.step_id + 1,
            num_batches: num_batches,
            total_batches_processed: self.state.batches_completed,
        )

        response = yield from yield_and_await_response(wkld)

        metrics = response.get("metrics", {}).get("avg_metrics", {})
        self.state.batches_completed += num_batches
        self.client.report_training_metrics(
            batches_trained=self.state.batches_completed,
            metrics=r["metrics"]["avg_metrics"],
        )

        exited_reason = response.get("exited_reason")
        should_exit = exited_reason is not None

        if exited_reason == "INVALID_HP":
            self.client.report_invalid_hp()

        if should_exit:
            # Always checkpoint; we know we've trained since last time.
            wkld = self.make_checkpoint_wkld()
            _ = yield from yield_and_await_response(wkld)

        return should_exit

    def validate(self) -> WorkloadGenerator:
        self.api_client.begin_validation()

        wkld = workload.Workload(
            kind=workload.Workload.Kind.COMPUTE_VALIDATION_METRICS,
            e_id=self.env.det_experiment_id,
            t_id=self.env.det_trial_id,
            s_id=self.state.step_id,
            num_batches: 0,
            total_batches_processed: self.state.batches_completed,
        )

        response = yield from yield_and_await_response(wkld)

        exited_reason = response.get("exited_reason")
        if exited_reason == "INVALID_HP":
            self.client.report_invalid_hp()
            return True

        self.state.last_val = self.state.batches_completed
        self.client.report_validation_metrics(metrics=response["metrics"])

        should_exit = exited_reason is not None

        if should_exit = exited_reason:

        return should_exit

    def make_checkpoint_wkld(self) -> workload.Workload:
        return workload.Workload(
            kind=workload.Workload.Kind.CHECKPOINT_MODEL,
            e_id=self.env.det_experiment_id,
            t_id=self.env.det_trial_id,
            s_id=self.state.step_id,
            num_batches: 0,
            total_batches_processed: self.state.batches_completed,
        )

    def checkpoint(
        self
    ) -> Tuple[workload.Workload, workload.Args, workload.ResponseFunc]:
        self.api_client.begin_checkpoint()

        wkld = self.make_checkpoint_wkld()
        response = yield from yield_and_await_response(wkld)

        exited_reason = response.get("exited_reason")
        if exited_reason == "INVALID_HP":
            self.client.report_invalid_hp()
            return True

        self.state.last_val = self.state.batches_completed
        self.client.report_validation_metrics(metrics=response["metrics"])

        should_exit = exited_reason is not None
        return should_exit

    def make_terminate_wkld(self) -> workload.Workload:
        return workload.Workload(
            kind=workload.Workload.Kind.TERMINATE,
            e_id=self.env.det_experiment_id,
            t_id=self.env.det_trial_id,
            s_id=self.state.step_id,
            num_batches: 0,
            total_batches_processed: self.state.batches_completed,
        )

    def need_min_val(self) -> bool:
        return (
            self.state.batches_completed
            >= self.state.last_val + self.min_val_period_batches
        )

    def batches_until_val(self) -> int:
        return (
            self.state.last_val
            + self.min_val_period_batches
            - self.state.batches_completed
        )

    def need_min_ckpt(self) -> bool:
        return (
            self.state.batches_completed
            >= self.state.last_ckpt + self.min_ckpt_period_batches
        )

    def batches_until_ckpt(self) -> int:
        return (
            self.state.last_ckpt
            + self.min_ckpt_period_batches
            - self.state.batches_completed
        )

#   def need_graceful_stop_ckpt(self) -> int:
#       check for a graceful stop + batches since last ckpt != 0

#   def postValidationCheckpointNeeded(self) -> int:
#       check for a "all" policy or "best" + best validation metric

#   def need_post_graceful_stop_chkp(self) -> int:
#       pass

    def __iter__(self) -> workload.Stream:
        try:
            searcher = self.api_client.get_advanced_searcher()

            # Step-zero Validations.
            if self.state.want_initial_val:
                should_exit = yield from self.validate()
                if should_exit:
                    return

            for op in searcher.ops():
                if self.api_client.should_preempt():
                    # Decide if we want to checkpoint model.
                    pass

                op.complete(self.searcher_metric)
        finally:
            # Always yield a terminate message last.
            wkld = self.make_terminate_wkld()
            _ = yield from self.yield_and_await_response()
