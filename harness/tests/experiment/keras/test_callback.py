import json
import os
import pathlib
import re
import subprocess
import sys
from unittest import mock
from typing import List, Tuple, Any

import pytest
import numpy as np
import tensorflow as tf
import keras

import determined as det
import determined.keras
from determined import core
from determined.common import storage


class Events:
    def __init__(self, initial=None, hook=None):
        self.items = initial or []
        self.hook = hook

    def append(self, item):
        self.items.append(item)
        self.hook and self.hook(*item)


def assert_events_match(events, *patterns):
    """
    Patterns can be negative (starting with a '!') or positive.

    Positive patterns must match events in order.

    Negative patterns must not match before the next positive pattern matches.
    """
    matched_data = []

    def display_events():
        return " - " + "\n - ".join(summary for summary, data in events.items)

    def iter_patterns(patterns):
        negatives = []
        for pattern in patterns:
            # Gather up chunks of negative patterns until the next positive pattern.
            if pattern.startswith("!"):
                negatives.append(re.compile(pattern[1:]))
            else:
                positive = re.compile(pattern)
                yield negatives, positive
                negatives = []
        # Possibly yield a final chunk of just negative patterns.
        yield negatives, None

    event_iter = iter(events.items)
    for negatives, positive in iter_patterns(patterns):
        for event, data in event_iter:
            # Negatives must not match.
            matches = [n.pattern for n in negatives if n.search(event)]
            if matches:
                if positive:
                    raise AssertionError(
                        f"negative pattern (!{matches[0]}) matched to event ({event}) before "
                        f"{positive.pattern} was found\nevents were:\n{display_events()}"
                    )
                else:
                    raise AssertionError(
                        f"negative pattern (!{matches[0]}) matched to event ({event}) "
                        f"after final postive pattern\nevents were:\n{display_events()}"
                    )
            if positive and positive.search(event):
                # Positive pattern matched, capture data and move on to the next group.
                matched_data.append(data)
                break
        else:
            # End of events... did we match all of our postives?
            if positive is None:
                return matched_data
            raise AssertionError(
                f"did not match positive expression ({positive.pattern})\n"
                f"events were:\n{display_events()}"
            )
    # Out of patterns.
    return matched_data


def test_assert_events_match():
    """
    Make sure our little test utility actually works.
    """

    def expect_success(events, *patterns):
        try:
            assert_events_match(events, *patterns)
        except AssertionError:
            raise AssertionError(f"expected success: {patterns}")

    def expect_failure(events, *patterns):
        try:
            assert_events_match(events, *patterns)
        except AssertionError:
            pass
        else:
            raise AssertionError(f"expected failure: {patterns}")

    events = Events([("1", None), ("2", None), ("3", None)])

    expect_success(events, "1")
    expect_success(events, "2")
    expect_success(events, "3")
    expect_success(events, "1", "2", "3")
    expect_success(events, "1", "!4")
    expect_success(events, "!0", "2")
    expect_success(events, "!2", "1", "2")

    expect_failure(events, "1", "3", "4")
    expect_failure(events, "1", "!2")
    expect_failure(events, "1", "!2", "3")
    expect_failure(events, "!1", "2")


def mock_core_context(path, events, dist=None):
    # Set up a functional DistributedContext.
    dist = dist or core.DummyDistributedContext()
    # Set up a functional CheckpointContext.
    storage_manager = storage.SharedFSStorageManager(str(path))
    checkpoint = core.DummyCheckpointContext(dist, storage_manager)

    # Mock everything else, logging report-like calls to events.

    def report_metrics(group, steps_completed, metrics) -> None:
        events.append((f"report_metrics:{group}:{steps_completed}", metrics))

    def report_progress(progress) -> None:
        fourdigits = "%.4f"%progress
        events.append((f"report_progress:{fourdigits}", progress))

    def set_status(status) -> None:
        events.append((f"set_status:{status}", None))

    preempted = False

    def should_preempt() -> bool:
        nonlocal preempted
        return preempted

    core_context = mock.Mock()
    core_context.distributed = dist
    core_context.preempt.should_preempt.side_effect = should_preempt
    core_context.checkpoint = checkpoint
    core_context.train.report_metrics.side_effect = report_metrics
    core_context.train.report_progress.side_effect = report_progress
    core_context.train.set_status.side_effect = set_status

    def set_preempt() -> None:
        nonlocal preempted
        preempted = True

    return core_context, set_preempt


class DeterminedCallbackForTesting(det.keras.DeterminedCallback):
    """
    For testing purposes, log events that happen during training for evaluation after training.
    """
    def __init__(self, events: List[Tuple[str, Any]], *args, **kwargs):
        self.events = events
        super().__init__(*args, **kwargs)

    def on_train_begin(self, logs):
        super().on_train_begin(logs)
        weight = self.model.layers[0].get_weights()[0][0]
        fourdigits = "%.4f"%weight
        self.events.append((f"after_train_begin:{fourdigits}", weight))

    def on_epoch_end(self, epoch, logs):
        weight = self.model.layers[0].get_weights()[0][0]
        fourdigits = "%.4f"%weight
        self.events.append((f"before_epoch_end:{epoch}", weight))
        super().on_epoch_end(epoch, logs)
        self.events.append((f"after_epoch_end:{epoch}", weight))

    def on_train_end(self, logs):
        self.events.append((f"before_train_end", None))
        super().on_train_end(logs)

    def save_model(self, model, path, distributed):
        super().save_model(model, path, distributed)
        ckpt_uuid = os.path.basename(os.path.dirname(path))
        self.events.append(("save_model", ckpt_uuid))

    def load_model(self, *args, **kwargs):
        super().load_model(*args, **kwargs)
        self.events.append(("load_model", None))


def build_model(eager=False):
    layer = keras.layers.Dense(
        1, activation=None, use_bias=False, kernel_initializer="zeros", input_shape=(8,)
    )
    model = keras.models.Sequential([layer])
    model.compile(
        loss=keras.losses.MeanSquaredError(),
        optimizer=keras.optimizers.SGD(),
        run_eagerly=eager,
    )
    return model


def do_fit(
    path,
    model=None,
    distributed=None,
    checkpoint=None,
    continue_id=1,
    checkpoint_epochs=1,
    train_metrics_report_period="epoch",
    eager=False,
    epochs=2,
    verbose=0,
    set_preempt_on_event=None
):
    x = np.ones((64, 8))
    y = np.ones((64, 8))
    validation_data = (np.ones((64, 8)), np.ones((64, 8)))

    model = model or build_model(eager=eager)
    events = Events()
    core_context, set_preempt = mock_core_context(str(path), events, distributed)

    if set_preempt_on_event:
        # Configure a hook for our Events that calls set_preempt() when a matching event arrives.
        p = re.compile(set_preempt_on_event)

        def hook(summary, data):
            if p.search(summary):
                set_preempt()

        events.hook = hook

    det_cb = DeterminedCallbackForTesting(
        events,
        core_context,
        checkpoint=checkpoint,
        continue_id=continue_id,
        train_metrics_report_period=train_metrics_report_period,
        checkpoint_epochs=checkpoint_epochs,
    )

    model.fit(
        x=x,
        y=y,
        validation_data=validation_data,
        batch_size=8,
        epochs=epochs,
        callbacks=[det_cb],
        verbose=verbose,
    )
    return events


def test_basic_logic(tmp_path: pathlib.Path):
    # make sure verbose=1 doesn't puke (though we don't really check the output)
    events = do_fit(tmp_path, verbose=1)

    # Checks that:
    #   - set_status() gets called
    #   - report_metrics() gets called
    #   - report_progress() gets called
    assert_events_match(
        events,
        "!load_model",
        "after_train_begin",
        "set_status:training",
        "set_status:validating",
        "report_metrics:validation",
        "before_epoch_end:0",
        "report_metrics:training",
        "report_progress:0.5000",
        "set_status:checkpointing",
        "save_model",
        "after_epoch_end:0",
        "before_epoch_end:1",
        "report_progress:1.000",
        "save_model",
        "after_epoch_end:1",
        "before_train_end",
        "!save_model",  # No final checkpoint.
        "set_status:finishing",
    )


# Pick this test to run eagerly because it both saves and loads checkpoints, which feel like it
# could matter if run_eagerly was set or not.
@pytest.mark.parametrize("eager", [False, True])
def test_save_restore_and_warm_start(tmp_path: pathlib.Path, eager: bool):
    # Train-from-scratch, then check that:
    # - initial weight is 0 (no checkpoint was loaded)
    # - initial epoch is 0 (no training state was loaded)
    # - checkpoint gets saved
    events = do_fit(tmp_path, eager=eager, checkpoint=None, continue_id=1)
    data = assert_events_match(
        events,
        "!load_model",
        "after_train_begin:0.0000",
        "before_epoch_end:0",
        "save_model",
        "after_epoch_end:0",
        "before_epoch_end:1",
        "save_model",
        "after_epoch_end:1",
    )

    # Grab the weight (formatted to 4 decimal places) from the "before_epoch_end:0" match.
    weight = data[1]
    # Grab the checkpoint uuid from the "save_model" match.
    ckpt = data[2]

    # Continue training (continue_id does match), then check that:
    # - initial weight is nonzero (checkpoint was loaded)
    # - initial epoch is nonzero (training state was loaded)
    # - steps_completed was properly restored
    events = do_fit(tmp_path, eager=eager, checkpoint=ckpt, continue_id=1)
    assert_events_match(
        events,
        "set_status:restoring",
        "load_model",
        "after_train_begin:%.4f"%weight,
        "!after_epoch_end:0",
        "before_epoch_end:1",
        "report_metrics:training:16",
        "after_epoch_end:1",
        "!after_epoch_end",  # Don't do two epochs if we started with one already from one.
    )

    # Warm-start training (continue_id does not match), then check that:
    # - initial weight is nonzero (no checkpoint was loaded)
    # - initial epoch is zero (no training state was loaded)
    # - steps_completed was properly reset
    events = do_fit(tmp_path, eager=eager, checkpoint=ckpt, continue_id=2)
    assert_events_match(
        events,
        "set_status:restoring",
        "load_model",
        "after_train_begin:%.4f"%weight,
        "report_metrics:training:8",
        "after_epoch_end:0",
        "after_epoch_end:1",
        "!after_epoch_end",
    )


def test_checkpoint_epochs(tmp_path: pathlib.Path):
    # Never checkpoint, except on preemption or completion
    events = do_fit(tmp_path, checkpoint_epochs=0, epochs=4)
    assert_events_match(
        events,
        # The only save is after the final on_epoch_end
        "!save_model",
        "after_epoch_end:3",
        "!after_epoch_end",
        "before_train_end",
        "save_model",
    )

    # Same thing, but trigger a checkpoint mid-training.
    events = do_fit(
        tmp_path, checkpoint_epochs=0, set_preempt_on_event="report_progress:0.5000"
    )
    assert_events_match(
        events,
        "!save_model",  # The preemption-caused checkpoint is in on_train_end, not on_epoch_end.
        "after_epoch_end:0",
        "!after_epoch_end",
        "before_train_end",
        "save_model",
    )

    # Checkpoint every other epoch, exiting on a natural checkpoint.
    events = do_fit(tmp_path, checkpoint_epochs=2, epochs=4)
    assert_events_match(
        events,
        "!save_model",
        "before_epoch_end:1",
        "save_model",
        "after_epoch_end:1",
        "!save_model",
        "before_epoch_end:3",
        "save_model",
        "after_epoch_end:3",
        # There is nothing to save in the on_train_end hook.
        "!after_epoch_end",
        "!save_model",
    )

    # Checkpoint every other epoch, and also at the end, if there is uncheckpointed work.
    events = do_fit(tmp_path, checkpoint_epochs=2, epochs=3)
    assert_events_match(
        events,
        "!save_model",
        "before_epoch_end:1",
        "save_model",
        "after_epoch_end:1",
        "!save_model",
        "after_epoch_end:2",
        "!save_model",
        "!after_epoch_end",
        # Expect an on_train_end checkpoint.
        "before_train_end",
        "save_model",
    )

    # Checkpoint every other epoch, preempting after a natural checkpoint.
    events = do_fit(
        tmp_path, checkpoint_epochs=2, epochs=4, set_preempt_on_event="report_progress:0.5000"
    )
    assert_events_match(
        events,
        "!save_model",
        "before_epoch_end:1",
        "save_model",
        "after_epoch_end:1",
        # No on_train_end checkpoint.
        "!after_epoch_end",
        "!save_model",
    )

    # Checkpoint every other epoch, preempting when there wasn't a checkpoint.
    events = do_fit(
        tmp_path, checkpoint_epochs=2, epochs=4, set_preempt_on_event="report_progress:0.2500"
    )
    assert_events_match(
        events,
        "!save_model",
        "after_epoch_end:0",
        "!after_epcoh_end",
        "!save_model",
        # Expect an on_train_end checkpoint.
        "before_train_end",
        "save_model",
    )


def test_report_period(tmp_path: pathlib.Path):
    events = do_fit(tmp_path, train_metrics_report_period=3)
    # There are 8 batches per epoch.
    assert_events_match(
        events,
        "report_metrics:training:3",
        "report_metrics:training:6",
        "report_metrics:validation:8",
        "report_metrics:training:9",
        "report_metrics:training:12",
        "report_metrics:training:15",
        "report_metrics:validation:16",
    )


# Pick this test to run eagerly because multi-gpu training, feel like it might be eager-senstive.
@pytest.mark.parametrize("eager", [False, True])
@pytest.mark.parametrize("multinode", [False, True])
@pytest.mark.skipif(len(tf.config.list_physical_devices("GPU")) < 2, reason="not enough gpus")
@pytest.mark.gpu_parallel
def test_multi_gpu(tmp_path: pathlib.Path, eager: bool, multinode: bool):
    """
    Getting an environment where this test can actually pass can be a real pain.

    If you are running on bare metal or a vm with multiple gpus, you can run the test directly,
    but you must have your nvidia driver and cuda library installations all squared away.  That is
    surprisingly difficult to achieve, at least I (rb) couldn't make it work.

    The tedious alternative, but which I find more reliable, is to run it in a docker container
    that is compatible with your GPU and driver.  I selected an NGC tensorflow image from the NGC
    image support matrix:

        https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html

    Then I built a little dockerfile like this:

        FROM ncr.io/nvidia/tesnroflow:$YOUR_NGC_IMAGE
        RUN pip install determined pytest && pip uninstall --yes determined
        ENV PYTHONUNBUFFERED=1

    Then I configured /etc/docker/daemon.json with some common settings to make dtrain happy:

        {
            "runtimes": {
                "nvidia": {
                    "args": [],
                    "path": "nvidia-container-runtime"
                }
            },
            "default-shm-size": "4G",
            "default-ulimits": {
                "memlock": {
                    "Name": "memlock",
                    "Hard": -1,
                    "Soft": -1
                },
                "stack": {
                    "Name": "stack",
                    "Hard": 67108864,
                    "Soft": 67108864
                }
            }
        }

    Restarted docker:

        sudo systemctl restart docker

    Then I mounted the entire determined project into a container with that new image:

        cd /path/to/determined
        docker run -it --rm -v $PWD:$PWD --gpus=all $YOUR_CUSTOM_IMAGE

    And finally, inside the container, I navigate to the harness directory and install determined
    with the editable setting:

        cd /path/to/determined/harness
        pip install -e .

    And voila, I can finally run the tests:

        pytest -v -s --tb=native tests/experiment/keras/test_callback.py -k test_multi_gpu

    I can also edit the tests from outside the container and rerun them immediately within the
    container because I mounted the whole project into the container and used `-e` with the pip
    install.
    """

    script = os.path.join(os.path.dirname(__file__), "train.py")
    cmd = [sys.executable, script, str(tmp_path)]
    if eager:
        cmd += ["--eager"]
    # NCCL can hit failures in this test, so make it easy to debug.
    env = {**os.environ, "NCCL_DEBUG": "info"}
    if multinode:
        tf_config = {
            "cluster": {"worker": ["localhost:12345", "localhost:12346"]},
            "task": {"type": "worker", "index": 0},
        }
        # Start worker 0.
        env["TF_CONFIG"] = json.dumps(tf_config)
        env["CUDA_VISIBLE_DEVICES"] = "0"
        p1 = subprocess.Popen(cmd, env=env)
        # Start worker 1.
        tf_config["task"]["index"] = 1
        env["TF_CONFIG"] = json.dumps(tf_config)
        env["CUDA_VISIBLE_DEVICES"] = "1"
        p2 = subprocess.Popen(cmd, env=env)
        ret1 = p1.wait()
        ret2 = p2.wait()
        assert ret1 == ret2 == 0, (ret1, ret2)
    else:
        env.pop("TF_CONFIG", None)
        env.pop("CUDA_VISIBLE_DEVICES", None)
        subprocess.run(cmd, check=True)
