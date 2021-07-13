import faulthandler
import logging
import pathlib
import subprocess
import sys

from determined import ipc, layers


def config_logging(worker_process_env: layers.WorkerProcessContext) -> None:
    log_level = logging.DEBUG if worker_process_env.debug else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s:%(levelname)s [%(process)s]: %(message)s"
    )
    logging.getLogger().setLevel(log_level)
    logging.debug("Starting worker_process.")


def main() -> int:
    if len(sys.argv) != 2:
        print("worker_process_env_path must be provided as a commandline argument", file=sys.stderr)
        return 1

    # Load the worker process env.
    worker_process_env_path = pathlib.Path(sys.argv[1])
    worker_process_env = layers.WorkerProcessContext.from_file(worker_process_env_path)

    config_logging(worker_process_env)

    if worker_process_env.env.experiment_config.debug_enabled():
        faulthandler.dump_traceback_later(30, repeat=True)

    # Establish the connection to the PIDServer in this container.
    with ipc.PIDClient(worker_process_env.pid_server_port) as pid_client:
        p = subprocess.Popen([sys.executable, "-m", "determined.exec.harness", *sys.argv[1:]])
        while True:
            try:
                ret = p.wait(timeout=60)
                pid_client.close(graceful=(ret == 0))
                return ret
            except subprocess.TimeoutExpired:
                pid_client.keep_alive()


if __name__ == "__main__":
    sys.exit(main())
