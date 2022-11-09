import contextlib
import random

import determined as det
from determined import core, horovod
from determined import pytorch
from typing import Dict, Optional, cast, Union
import torch
from determined.horovod import hvd
import torch.distributed as dist
import logging

from determined.profiler import ProfilerAgent, DummyProfilerAgent
from determined.pytorch import PyTorchTrialContext, PyTorchTrialController, TrainUnit, Batch, Epoch, Record


class Trainer:
    def __init__(self, trial: pytorch.PyTorchTrial, context: PyTorchTrialContext):
        self._trial = trial
        self._context = context
        self._cluster_info = det.get_cluster_info()
        self._core = self._context._core
        self._distributed_backend = det._DistributedBackend()
        self._det_profiler = DummyProfilerAgent()
        self._trial_controller = None
        self._local_training = self._cluster_info is None

    def configure_profiler(self, sync_timings: bool, enabled: bool, begin_on_batch: int, end_after_batch: int):
        assert self._cluster_info, "Determined profiler must be run on cluster"
        self._det_profiler = ProfilerAgent(
            trial_id=str(self._cluster_info.trial.trial_id),
            agent_id=self._cluster_info.agent_id,
            master_url=self._cluster_info.master_url,
            profiling_is_enabled=enabled,
            global_rank=self._core.distributed.get_rank(),
            local_rank=self._core.distributed.get_rank(),
            begin_on_batch=begin_on_batch,
            end_after_batch=end_after_batch,
            sync_timings=sync_timings,
        )

    def train(
        self,
        max_epochs: Optional[int] = None,
        # OR
        max_batches: Optional[int] = None,
        min_checkpoint_period: Union[pytorch.TrainUnit, int] = 1,
        min_validation_period: Union[pytorch.TrainUnit, int] = 1,
        average_training_metrics: Optional[bool] = True,
        average_aggregated_gradients: Optional[bool] = True,
        aggregation_frequency: Optional[int] = 1,
        checkpoint_policy: Optional[str] = "best",
        smaller_is_better: Optional[bool] = True,
    ):

        if self._local_training:
            assert (max_epochs is None) ^ (max_batches is None), \
                "Either max_batches or max_epochs must be defined in local training mode"
        else:
            if max_batches or max_epochs:
                logging.warning("max_batches and max_epochs is ignored in when training on cluster. "
                                "Please configure the searcher length instead.")

        # Set context and training variables
        self._context._aggregation_frequency = aggregation_frequency
        self._context._average_aggregated_gradients = average_aggregated_gradients

        max_length = None
        if max_batches:
            max_length = Batch(max_batches)
        elif max_epochs:
            max_length = Epoch(max_epochs)

        # Convert validation/checkpoint periods to training units.
        # Without a specified training unit, periods will be assumed to be same as max_length in local training mode,
        # and the searcher unit when training on-cluster
        if isinstance(min_checkpoint_period, int):
            min_checkpoint_period = self._convert_period_to_train_unit(min_checkpoint_period, max_length)

        if isinstance(min_validation_period, int):
            min_validation_period = self._convert_period_to_train_unit(min_validation_period, max_length)

        if self._local_training:
            self._trial_controller = PyTorchTrialController(
                trial_inst=self._trial,
                context=self._context,
                max_length=max_length,
                min_validation_period=min_validation_period,
                min_checkpoint_period=min_checkpoint_period,
                average_training_metrics=average_training_metrics,
                checkpoint_policy=checkpoint_policy,
                smaller_is_better=smaller_is_better,
                local_training=True,
            )
        else:
            self._trial_controller = PyTorchTrialController(
                trial_inst=self._trial,
                context=self._context,
                min_checkpoint_period=min_checkpoint_period,
                min_validation_period=min_validation_period,
                average_training_metrics=average_training_metrics,
                checkpoint_policy=checkpoint_policy,
                smaller_is_better=smaller_is_better,
                searcher_metric_name=self._cluster_info.trial._config["searcher"]["metric"],
                local_training=False,
                det_profiler=self._det_profiler,
                steps_completed=self._cluster_info.trial._steps_completed,
            )

        self._trial_controller.run()

    def _train_for(self, max_length: TrainUnit):
        assert self._local_training, "train_for must only be called in local training mode"

    def _convert_period_to_train_unit(self, period: int, train_unit: TrainUnit):
        # Local training will assume same period as max_length
        if self._local_training:
            if isinstance(train_unit, Batch):
                return Batch(period)
            elif isinstance(train_unit, Epoch):
                return Epoch(period)
            elif isinstance(train_unit, Record):
                return Record(period)

        # On-cluster training assumes period lengths with the searcher
        searcher_unit = self._core.searcher.get_configured_units()
        return TrainUnit._from_searcher_unit(period, searcher_unit)


def _initialize_distributed_backend():
    distributed_backend = det._DistributedBackend()
    if distributed_backend.use_horovod():
        hvd.require_horovod_type("torch", "PyTorchTrial is in use.")
        hvd.init()
        return core.DistributedContext.from_horovod(horovod.hvd)
    elif distributed_backend.use_torch():
        if torch.cuda.is_available():
            dist.init_process_group(backend="nccl")  # type: ignore
        else:
            dist.init_process_group(backend="gloo")  # type: ignore
        return core.DistributedContext.from_torch_distributed()
    else:
        print(f"Backend {distributed_backend} not supported")


def _generate_local_seed():
    return random.randint(0, 1 << 31)


@contextlib.contextmanager
def init(hparams: Optional[Dict] = None,
         distributed_context: Optional[core.DistributedContext] = None):

    cluster_info = det.get_cluster_info()
    local_training = cluster_info is None

    # Pre-execute steps: initialize distributed backend and set trial seeds
    distributed_context = distributed_context or _initialize_distributed_backend()
    if local_training:
        trial_seed = _generate_local_seed()
    else:
        if hparams and cluster_info.trial.hparams:
            logging.warning("hparams are specified in Trainer and experiment config. Trainer hparams will be ignored")
        hparams = cluster_info.trial.hparams
        trial_seed = cluster_info.trial.trial_seed

    PyTorchTrialController._set_random_seeds(trial_seed)

    logging.basicConfig(level=logging.INFO, format=det.LOG_FORMAT)

    with core.init(distributed=distributed_context,
                   preempt_mode=core.PreemptMode.ChiefOnly,
                   tensorboard_mode=core.TensorboardMode.MANUAL) as core_context:

        if local_training:
            context = PyTorchTrialContext(
                hparams=hparams,
                core_context=core_context,
                trial_seed=trial_seed
            )
        else:
            exp_conf = cluster_info.trial._config
            context = PyTorchTrialContext(hparams=hparams,
                                          core_context=core_context,
                                          trial_seed=trial_seed,
                                          num_gpus=len(cluster_info.gpu_uuids),
                                          exp_conf=exp_conf,
                                          slots_per_trial=cast(int, exp_conf["resources"]["slots_per_trial"]),
                                          aggregation_frequency=cast(bool, exp_conf["optimizations"]["aggregation_frequency"]),
                                          fp16_compression=cast(bool, exp_conf["optimizations"]["gradient_compression"]),
                                          average_aggregated_gradients=cast(bool, exp_conf["optimizations"]["average_aggregated_gradients"]),
                                          steps_completed=cluster_info.trial._steps_completed
                                          )
        yield context

