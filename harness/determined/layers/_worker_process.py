import logging
import os
import pathlib
import pickle
import signal
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, cast

import psutil

import determined as det
from determined import constants, horovod, ipc, workload
from determined.common import check


class WorkerProcessContext:
    def __init__(
        self,
        pid_server_port: int,
        debug: bool,
        hvd_config: horovod.HorovodContext,
        rendezvous_info: det.RendezvousInfo,
        env: det.EnvContext,
    ) -> None:
        self.pid_server_port = pid_server_port
        self.debug = debug
        self.hvd_config = hvd_config
        self.rendezvous_info = rendezvous_info
        self.env = env

    @staticmethod
    def from_file(path: pathlib.Path) -> "WorkerProcessContext":
        with path.open(mode="rb") as f:
            obj = pickle.load(f)
        check.is_instance(obj, WorkerProcessContext, "did not find WorkerProcessContext in file")
        return cast(WorkerProcessContext, obj)

    def to_file(self, path: pathlib.Path) -> None:
        with path.open(mode="wb") as f:
            pickle.dump(self, f)


class SubprocessLauncher:
    def __init__(
        self,
        env: det.EnvContext,
        rendezvous_info: det.RendezvousInfo,
        hvd_config: horovod.HorovodContext,
        python_subprocess_entrypoint: Optional[str] = None,
    ) -> None:
        self.env = env
        self.rendezvous_info = rendezvous_info
        self.hvd_config = hvd_config
        self._python_subprocess_entrypoint = python_subprocess_entrypoint

        self.debug = self.env.experiment_config.debug_enabled()

        # Horovod will have a separate training process for each slot.
        self.num_proc = len(self.env.slot_ids) if self.hvd_config.use else 1

        # Step 1: Establish the server for detecting subprocess crashes.
        self.pid_server = ipc.PIDServer(self.num_proc).start()

        # Step 2: Configure the per-machine WorkerProcessContext.
        self._init_worker_process_env()

        self.is_chief_machine = self.rendezvous_info.get_rank() == 0
        chief_addr = self.rendezvous_info.get_ip_addresses()[0]
        chief_port = self.rendezvous_info.get_ports()[0]

        if self.is_chief_machine:
            # Step 3 (chief): Wait for any peer machines to launch sshd, then launch horovodrun.
            if self.rendezvous_info.get_size() > 1:
                with ipc.ZMQServer(ports=[chief_port], num_connections=1) as server:
                    num_peers = self.rendezvous_info.get_size() - 1
                    responses = server.barrier(num_connections=num_peers, timeout=20)
                    if len(responses) < num_peers:
                        raise AssertionError(
                            f"Chief received sshd ready signal only from {len(responses)} "
                            f"of {num_peers} machines."
                        )
                    logging.debug("Chief finished sshd barrier.")

            if self.hvd_config.use:
                self._subproc = self._launch_horovodrun()
            else:
                self._subproc = self._launch_python_subprocess()

        else:
            # Step 3 (non-chief): launch sshd, wait for it to come up, then signal to the chief.
            self._subproc = self._launch_sshd()

            self._wait_for_sshd_to_start()

            with ipc.ZMQClient(chief_addr, chief_port) as client:
                client.barrier()

    def _init_worker_process_env(self) -> None:
        """
        Initialize the environment variables for the training process.

        TODO(DET-1330): Serialize all environment variables used by training process.
        """

        worker_process_env = WorkerProcessContext(
            pid_server_port=self.pid_server.get_port(),
            debug=self.debug,
            hvd_config=self.hvd_config,
            rendezvous_info=self.rendezvous_info,
            env=self.env,
        )
        self._worker_process_env_path = pathlib.Path(
            "{}-{}-{}".format(
                constants.TRAIN_PROCESS_ENVIRONMENT_VARIABLE_PATH,
                self.env.det_experiment_id,
                self.env.det_trial_id,
            )
        )
        worker_process_env.to_file(self._worker_process_env_path)

    def _launch_horovodrun(self) -> subprocess.Popen:
        check.true(self.hvd_config.use)
        logging.debug(f"Starting training process on: {self.rendezvous_info.get_rank()}.")

        horovod_process_cmd = horovod.create_run_command(
            num_proc_per_machine=self.num_proc,
            ip_addresses=self.rendezvous_info.get_ip_addresses(),
            env=self.env,
            debug=self.env.experiment_config.debug_enabled(),
            optional_args=self.env.experiment_config.horovod_optional_args(),
            worker_process_env_path=self._worker_process_env_path,
        )
        subprocess_env = {
            **os.environ,
            "NCCL_DEBUG": "INFO",
            "DET_HOROVOD_GLOO_RENDEZVOUS_PORT": str(
                constants.HOROVOD_GLOO_RENDEZVOUS_PORT + self.env.det_trial_unique_port_offset
            ),
        }
        return subprocess.Popen(horovod_process_cmd, env=subprocess_env)

    def _launch_sshd(self) -> subprocess.Popen:
        run_sshd_command = [
            "/usr/sbin/sshd",
            "-p",
            str(constants.HOROVOD_SSH_PORT),
            "-f",
            "/run/determined/ssh/sshd_config",
            "-D",
        ]
        logging.debug(
            f"Non-chief [{self.rendezvous_info.get_rank()}] training process launch "
            f"command: {run_sshd_command}."
        )
        return subprocess.Popen(run_sshd_command)

    def _wait_for_sshd_to_start(self) -> None:
        connection_attempts = 0
        logging.debug(f"Non-chief [{self.rendezvous_info.get_rank()}] waiting for sshd service.")
        while True:
            ssh_attempt_cmd = ["ssh", "localhost", "-p", str(constants.HOROVOD_SSH_PORT), "ls"]
            ssh_attempt_process = subprocess.run(
                ssh_attempt_cmd, timeout=10
            )
            if ssh_attempt_process.returncode == 0:
                logging.debug(
                    f"Non-chief [{self.rendezvous_info.get_rank()}] successfully "
                    "started sshd service."
                )
                break

            # Check that training subprocess is still alive and well.
            self._health_check()

            connection_attempts += 1
            if connection_attempts == 10:
                raise AssertionError("Training process failed to start sshd.")

            logging.info("Waiting for training process to start sshd ...")
            time.sleep(1)

    def _launch_python_subprocess(self) -> subprocess.Popen:
        """
        Call training process without using horovodrun. Only used internally when testing.
        """

        check.is_not_none(self._python_subprocess_entrypoint)
        self._python_subprocess_entrypoint = cast(str, self._python_subprocess_entrypoint)

        # Construct the command to launch the non-horovod training subprocess.
        python = sys.executable
        python_cmd = [
            python,
            "-m",
            self._python_subprocess_entrypoint,
            str(self._worker_process_env_path),
        ]
        return subprocess.Popen(python_cmd)

    def run(self) -> None:
        with self.pid_server:
            try:
                self.pid_server.run(self._health_check)
            except det.errors.WorkerError:
                if self.is_chief_machine:
                    # Give horovodrun a few seconds to notice the process has died.
                    time.sleep(3)
                    # Use SIGINT on horovod, because it freaks out with SIGTERM.
                    self._subproc.send_signal(signal.SIGINT)
                else:
                    self._subproc.kill()
                self._subproc.wait()
                # Give some time to finish logging.
                time.sleep(1)
                raise

            if self.is_chief_machine:
                # wait for horovod run to exit normally.
                ret = self._subproc.wait()
                if ret != 0:
                    raise ValueError("horovodrun crashed after all our subprocesses exited")
            else:
                self._subproc.kill()
                self._subproc.wait()

    def _health_check(self) -> None:
        """
        Raise an error if the main subprocess or if any workers die.
        """

        if self._subproc.poll() is not None:
            raise det.errors.WorkerError("main subprocess died")
