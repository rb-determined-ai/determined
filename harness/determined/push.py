#!/usr/bin/env python3

"""
A dummy push architecture server, plus client methods, plus a cli, plus a unit test.

Designed to assist in developing push architecture changes for the harness while the master-side
changes are still pending.
"""

import time
import json
import os
import requests
import sys
import tempfile
import threading
from typing import Optional

## Dummy Server ##

def run_server():
    import flask

    app = flask.Flask(__name__)
    preempt_flag = False
    preempt_cond = threading.Condition()
    searcher_ops = [10, 20, 30]
    completed_ops = 0

    def do_reset():
        nonlocal preempt_flag
        nonlocal completed_ops
        preempt_flag = False
        completed_ops = 0

    # Training Metrics APIs.

    @app.route("/begin_training", methods=["POST"])
    def begin_training():
        print(f"began training at {time.time()}")
        return "OK"

    @app.route("/report_training_metrics", methods=["POST"])
    def report_training_metrics():
        print(f"completed training at {time.time()}")
        body = json.loads(flask.request.get_data().decode("utf8"))
        print(f"metrics = {body['metrics']}")
        return "OK"

    # Validation Metrics APIs.

    @app.route("/begin_validation", methods=["POST"])
    def begin_validation():
        print("began validation at {time.time()}")
        return "OK"

    @app.route("/report_validation_metrics", methods=["POST"])
    def report_validation_metrics():
        print(f"completed validation at {time.time()}")
        body = json.loads(flask.request.get_data().decode("utf8"))
        print(body)
        print(f"metrics = {body['metrics']}")
        return "OK"

    # Checkpoint APIs.

    @app.route("/begin_checkpoint", methods=["POST"])
    def begin_checkpoint():
        print("began checkpoint at {time.time()}")
        return "OK"

    @app.route("/report_checkpoint", methods=["POST"])
    def report_checkpoint():
        print(f"completed checkpoint at {time.time()}")
        body = json.loads(flask.request.get_data().decode("utf8"))
        print(f"uuid = {body['uuid']}")
        return "OK"

    # Searcher APIs.

    @app.route("/get_searcher_op")
    def get_searcher_op(methods=["GET"]):
        if completed_ops >= len(searcher_ops):
            return "null"
        return json.dumps({"unit": "epochs", "length": searcher_ops[completed_ops]})

    @app.route("/report_searcher_progress", methods=["POST"])
    def report_searcher_progress():
        goal = searcher_ops[completed_ops]
        body = json.loads(flask.request.get_data().decode("utf8"))
        print(f"searcher progress: {body}/{goal} epochs")
        return "OK"

    @app.route("/complete_searcher_op", methods=["POST"])
    def complete_searcher_op():
        nonlocal completed_ops
        assert completed_ops < len(searcher_ops)
        body = json.loads(flask.request.get_data().decode("utf8"))
        print(f"finished searcher op {completed_ops}, searcher_metric = {body}")
        completed_ops += 1
        return "OK"

    # Preemption API

    @app.route("/should_preempt")
    def should_preempt(methods=["GET"]):
        # implement long-polling without timeouts
        # (the dummest dummy possible)
        with preempt_cond:
            while not preempt_flag:
                preempt_cond.wait()
        return "true"

    # External controls

    @app.route("/_trigger_preempt", methods=["POST"])
    def _trigger_preempt():
        """Trigger the dummy server to return preemption responses"""
        nonlocal preempt_flag
        print("preempting")
        with preempt_cond:
            preempt_flag = True
            preempt_cond.notify_all()
        return "OK"

    @app.route("/_reset", methods=["POST"])
    def reset():
        """Reset the dummy server between runs"""
        print("resetting")
        do_reset()
        return "OK"

    app.run(host="0.0.0.0")


## Client Code ##


class PreemptCheckerThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._flag = False

    def run(self):
        # TODO: use non-blocking requests so we can cancel/join the thread
        r = requests.get("http://localhost:5000/should_preempt")
        r.raise_for_status()
        self._flag = True

    def check(self):
        return self._flag

EPOCHS = "epochs"
RECORDS = "records"
BATCHES = "batches"

class SearcherOp:
    def __init__(self, client: "PushClient", unit: str, length: int) -> None:
        self._client = client
        self._unit = unit      # one of EPOCHS, BATCHES, or RECORDS
        self._length = length  # int
        self._completed = False

    @property
    def unit(self) -> None:
        return self._unit

    @property
    def length(self) -> None:
        return self._length

    @property
    def records(self) -> None:
        assert self._unit == RECORDS
        return self._length

    @property
    def batches(self) -> None:
        assert self._unit == BATCHES
        return self._length

    @property
    def epochs(self) -> None:
        assert self._unit == EPOCHS
        return self._length

    def report_progress(self, length: int) ->None:
        assert not self._completed
        self._client._report_searcher_progress(length)

    def complete(self, searcher_metric: float) -> None:
        assert not self._completed
        self._client._complete_searcher_op(searcher_metric)
        self._completed = True


class AdvancedSearcher:
    """A namespaced API to make it clear which searcher-related things go to which API"""
    def __init__(self, client):
        self._client = client

    def ops(self):
        """Iterate through all the ops this searcher has to offer"""
        while True:
            op = self._client._get_searcher_op()
            if op is None:
                break
            yield op
            assert op._completed, "you must call op.complete() on each op"


class PushClient:
    def __init__(self):
        self.uri = "http://localhost:5000"
        self.preempt_thread = PreemptCheckerThread()
        self.preempt_thread.start()

    # Training Metrics APIs.

    def begin_training(self):
        r = requests.post(self.uri + "/begin_training")
        r.raise_for_status()

    def report_training_metrics(self, batches_trained:int, metrics: dict) -> None:
        body = json.dumps({"batches_trained": batches_trained, "metrics": metrics})
        r = requests.post(self.uri + "/report_training_metrics", data=body)
        r.raise_for_status()

    # Validation Metrics APIs.

    def begin_validation(self):
        r = requests.post(self.uri + "/begin_validation")
        r.raise_for_status()

    def report_validation_metrics(self, metrics: dict) -> None:
        body = json.dumps({"metrics": metrics})
        r = requests.post(self.uri + "/report_validation_metrics", data=body)
        r.raise_for_status()

    # Checkpoint APIs.

    def _begin_checkpoint(self):
        r = requests.post(self.uri + "/begin_checkpoint")
        r.raise_for_status()

    def _report_checkpoint(self, uuid: str) -> None:
        body = json.dumps({"uuid": uuid})
        r = requests.post(self.uri + "/report_checkpoint", data=body)
        r.raise_for_status()

    # Searcher APIs.

    def _get_searcher_op(self) -> Optional[SearcherOp]:
        r = requests.get(self.uri + "/get_searcher_op")
        r.raise_for_status()
        body = r.json()
        if body is None:
            return None
        return SearcherOp(self, **body)

    def _report_searcher_progress(self, length: int) -> None:
        body = json.dumps(length)
        r = requests.post(self.uri + "/report_searcher_progress", data=body)
        r.raise_for_status()

    def _complete_searcher_op(self, searcher_metric) -> None:
        body = json.dumps(searcher_metric)
        r = requests.post(self.uri + "/complete_searcher_op", data=body)

    def get_advanced_searcher(self):
        return AdvancedSearcher(self)

    # Preemption API

    def should_preempt(self):
        # TODO: support dtrain
        # TODO: support nonblocking API
        return self.preempt_thread.check()

## Utility Code ##

def reset_server():
    r = requests.post("http://localhost:5000/_reset")
    r.raise_for_status()

def trigger_preempt():
    r = requests.post("http://localhost:5000/_trigger_preempt")
    r.raise_for_status()

def run_tests():
    """Assume a live server is running elsewhere."""
    reset_server()
    client = PushClient()

    # Training Metrics APIs
    client.begin_training()
    client.report_training_metrics(batches_trained=10, metrics={"loss": 0.1})

    # Validation Metrics APIs
    client.begin_validation()
    client.report_validation_metrics(metrics={"loss": 0.1})

    # Searcher APIs
    searcher = client.get_advanced_searcher()
    ops = []
    epochs = 0
    for op in searcher.ops():
        assert op.unit == EPOCHS
        for epochs in range(epochs + 1, op.epochs):
            op.report_progress(epochs)
        ops.append(op.epochs)
        op.complete(searcher_metric = 1 - .1 * len(ops))
    assert ops == [10, 20, 30]

    # Preemption APIs.
    assert client.should_preempt() == False
    trigger_preempt()
    # hack: block until the preempt thread finishes
    client.preempt_thread.join()
    assert client.should_preempt() == True

    reset_server()

    print("PASS")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("command", choices=["server", "reset", "preempt", "test"])

    args = parser.parse_args()

    if args.command == "server":
        run_server()
    elif args.command == "reset":
        reset_server()
    elif args.command == "preempt":
        trigger_preempt()
    elif args.command == "test":
        run_tests()
