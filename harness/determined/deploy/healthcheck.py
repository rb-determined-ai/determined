import time
from typing import Optional

import requests

from determined.common import api
from determined.common.api import certs
from determined.deploy import errors

DEFAULT_TIMEOUT = 100


def wait_for_master(
    master_url: str,
    timeout: int = DEFAULT_TIMEOUT,
    cert: Optional[certs.Cert] = None,
) -> None:
    POLL_INTERVAL = 2
    polling = False
    start_time = time.time()

    try:
        while time.time() - start_time < timeout:
            try:
                sess = api.UnauthSession(master_url, cert=cert)
                r = sess.get("info")
                if r.status_code == requests.codes.ok:
                    return
            except api.errors.MasterNotFoundException:
                pass
            if not polling:
                polling = True
                print("Waiting for master instance to be available...", end="", flush=True)
            time.sleep(POLL_INTERVAL)
            print(".", end="", flush=True)

        raise errors.MasterTimeoutExpired
    finally:
        if polling:
            print()
