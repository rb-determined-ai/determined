import logging
import threading
import time
from typing import Any, Optional

import requests

import determined as det

log = logging.getLogger("generic")


class _PreemptionWatcher(threading.Thread):
    """
    _PreemptionWatcher connects to the master and asynchronously waits for a preemption signal.

    _PreemptionWatcher.should_preempt() is non-blocking (after the initial contact is made with the
    master) and returns a bool indicating if a preemption signal has been received yet.

    Example usage:

    .. code:: python

       with _PreemptionWatcher(session, trial_id) as p:
           print("started!")
           for i in range(10):
               if p.should_preempt():
                   print("preempted!")
                   break
               print('no preemption yet, waiting...')
               time.sleep(1)
           else:
               print('finished without preemption signal')
    """

    def __init__(self, session: str, trial_id: int) -> None:
        self._session = session
        self._trial_id = trial_id

        self._should_preempt = None  # type: Optional[bool]
        self._should_quit = False

        self._cond = threading.Condition()

        # Set daemon=True, since the requests library only supports blocking reads.  Under the hood,
        # the requests library uses buffered IO on top of the socket, which means that we can't even
        # use select() to know if a read would block; select() won't know that some data is
        # available in the buffer.  We would probably have to move to an async-based HTTP library
        # to make the PreemptionWatcher properly preemptible.
        super().__init__(daemon=True)

    def _get_preemption(self, longpoll_time: int) -> bool:
        log.info(f"_get_preemption({longpoll_time})")
        return (
            self._session.get(
                f"/api/v1/trials/{self._trial_id}/signals/preemption",
                params={"timeout_seconds": str(longpoll_time)},
                timeout=longpoll_time + 10,
            ).json()["preempt"]
            is True
        )

    def run(self) -> None:
        # Do a rapid check for the initial value.
        with self._cond:
            try:
                self._should_preempt = self._get_preemption(0)
            except requests.Timeout:
                logging.exception("timeout during initial preemption API check, continuing")
                self._should_preempt = False
            except Exception:
                logging.exception("failureduring initial preemption API check, continuing")
                self._should_preempt = False
            finally:
                # Wake the main thread in case it was waiting for the initial response.
                self._cond.notify()

        # Continuously poll for preemption status to change.  Always retry after network failures;
        # if the master is unreachable, either user code will exit due to some more critical API
        # failure, or the user will kill the workload.
        while not self._should_preempt and not self._should_quit:
            try:
                self._should_preempt = self._get_preemption(60)
            except requests.Timeout:
                logging.exception("timeout communicating with preemption API, retrying")
            except Exception:
                logging.exception("failure communicating with preemption API, retrying in 10s")
                time.sleep(10)

    def close(self) -> None:
        # TODO: For now we have to set daemon=True for the thread, so there's no point in joining.
        # self.join()
        self._should_quit = True

    def __enter__(self) -> "_PreemptionWatcher":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def should_preempt(self) -> bool:
        # Optimize to avoid locking the threading.Conditional object if we can avoid it.
        if self._should_preempt is not None:
            return self._should_preempt

        # Block until the Preemption API has streamed the initial response.
        with self._cond:
            while self._should_preempt is None:
                self._cond.wait()
        return self._should_preempt


class Preemption:
    """
    Some preemption-related APIs.
    """

    def __init__(
        self,
        session,
        trial_id,
        dist: det.DistributedContext,
    ) -> None:
        self._session = session
        self._trial_id = trial_id
        self._dist = dist
        if self._dist.get_rank() == 0:
            self._watcher = _PreemptionWatcher(
                session, trial_id
            )  # type: Optional[_PreemptionWatcher]
        else:
            self._watcher = None
        self._ack_sent = False

    def start(self):
        if self._watcher is not None:
            self._watcher.start()

    def close(self):
        if self._watcher is not None:
            self._watcher.close()

    def __enter__(self) -> "Preemption":
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def should_preempt(self, chief_only=False, auto_ack=True) -> bool:
        """
        Currently, we only support blocking behavior when checking should_preempt(), so it is not
        performant enough to call every batch.

        Arguments:
            chief_only (``bool``, default ``False``): In multi-worker training, chief_only
                configures whether or not the result of should_preempt() is being checked on every
                worker or whether it is only being checked on the chief worker.  When all workers
                are checking should_preempt() (the default case), the chief will broadcast its
                decision to all the workers in order to guarantee that all workers agree on the
                decision to preempt.  You should set chief_only if only the chief worker is making
                the call to should_preempt(), in which case it is your responsiblity to have the
                chief shut down training on all workers via some other means.
            auto_ack (``bool``, default ``True``): In order for the workload to be restarted after
                shutting down due to preemption, the workload must tell acknowledge the preemption
                signal to the Determined master.  When auto_ack is True (the default case) this
                acknowledgement is automatically sent the first time that should_preempt() returns
                True, and it is assumed that the caller will shut down in response to the preemption
                signal unconditionally afterwards (i.e., that you would not ignore a preemption
                signal after seeing it).  If auto_ack is set to False, it is the caller's
                responsibility to call acknowledge_preemption_signal() when shutting down due to
                preemption.
        """
        if self._watcher is not None:
            # Chief.
            out = self._watcher.should_preempt()
            if auto_ack and out and not self._ack_sent:
                # Tell the master that user code has received the preemption signal.
                self.acknowledge_preemption_signal()
            if not chief_only:
                _ = self._dist._zmq_broadcast(out)
        else:
            # Non-chief.
            if chief_only:
                raise ValueError(
                    "should_preempt(chief_only=True) was called from non-chief worker of "
                    f"rank={self._dist.get_rank()}"
                )
            out = self._dist._zmq_broadcast(None)
            assert isinstance(out, bool)

        log.info(f"should_preempt() -> {out}")
        return out

    def acknowledge_preemption_signal(self) -> None:
        """
        acknowledge_preemption_signal() tells the Determined master that you are shutting down, but
        you have not finished your work and you expect to be restarted later to complete it.

        This is important to tell the master explicitly, since normally if the python process exits
        with a nonzero exit code, the master would interpret that as a completed workload, and it
        would not get rescheduled at a later time.

        acknowledge_preemption_signal() is normally called automatically the first time that
        should_preempt() returns True, unless should_preempt() is called with auto_ack=False.
        """
        log.info("acknowledge_preemption_signal()")
        self._session.post(f"/api/v1/trials/{self._trial_id}/signals/ack_preemption")
