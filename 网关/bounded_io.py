"""Small bounded executor lanes for blocking file and SQLite work.

``asyncio``'s default executor has an unbounded submission queue.  The gateway
uses this wrapper so a slow disk cannot silently turn every incoming request
into another queued thread-pool item.
"""

from __future__ import annotations

import asyncio
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable


class IoLaneFull(RuntimeError):
    """Raised before submission when a bounded I/O lane has no capacity."""

    def __init__(self, lane_name: str, max_pending: int) -> None:
        super().__init__(
            "I/O lane is full: lane=%s max_pending=%s"
            % (lane_name, max_pending)
        )
        self.lane_name = lane_name
        self.max_pending = max_pending
        self.code = "GATEWAY_IO_BUSY"


class BoundedExecutorLane:
    """A fixed-size executor whose submitted plus running work is bounded.

    Capacity is reserved synchronously before the first ``await``.  This is
    important: merely putting a semaphore in front of ``run_in_executor``
    would still allow an unbounded number of coroutines to wait on it.
    """

    def __init__(self, name: str, max_workers: int, max_pending: int) -> None:
        workers = max(1, int(max_workers))
        pending = max(workers, int(max_pending))
        self.name = str(name or "io")
        self.max_workers = workers
        self.max_pending = pending
        self._executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="qmt-%s" % self.name,
        )
        self._lock = threading.Lock()
        self._pending = 0
        self._closed = False

    @property
    def pending(self) -> int:
        with self._lock:
            return self._pending

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def _reserve(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("I/O lane is closed: %s" % self.name)
            if self._pending >= self.max_pending:
                raise IoLaneFull(self.name, self.max_pending)
            self._pending += 1

    def _release(self) -> None:
        with self._lock:
            if self._pending > 0:
                self._pending -= 1

    async def run(self, func: Callable[..., Any], *args: Any) -> Any:
        self._reserve()
        loop = asyncio.get_running_loop()
        call = functools.partial(func, *args)
        try:
            future = loop.run_in_executor(self._executor, call)
        except BaseException:
            self._release()
            raise
        # Cancellation of the awaiting request must not free capacity while
        # its non-cancellable OS/SQLite call is still running in the worker.
        future.add_done_callback(lambda _completed: self._release())
        return await asyncio.shield(future)

    def close(self, wait: bool = True) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=False)


__all__ = ["BoundedExecutorLane", "IoLaneFull"]
