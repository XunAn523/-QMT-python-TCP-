"""Ordered callbacks with fail-closed reliable-delivery completion."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import logging
import queue
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)
Handler = Callable[[Dict[str, Any]], Any]
Completion = Callable[["DispatchResult"], None]


@dataclass(frozen=True)
class DispatchResult:
    handled: bool
    succeeded: bool
    handler_count: int
    failed_handlers: Tuple[str, ...] = ()

    @property
    def acknowledgeable(self) -> bool:
        return self.handled and self.succeeded


@dataclass
class _DispatchItem:
    message: Dict[str, Any]
    completion: Optional[Completion]


_STOP = object()


class AsyncMessageDispatcher:
    """Execute callbacks on one bounded worker while preserving wire order."""

    def __init__(self, max_queue_size: int = 1024) -> None:
        if int(max_queue_size) <= 0:
            raise ValueError("max_queue_size must be positive")
        self._handlers: Dict[str, List[Handler]] = {}
        self._handlers_lock = threading.RLock()
        self._queue: "queue.Queue[object]" = queue.Queue(maxsize=int(max_queue_size))
        self._thread: Optional[threading.Thread] = None
        self._lifecycle_lock = threading.Lock()

    @property
    def worker_thread_id(self) -> Optional[int]:
        thread = self._thread
        return thread.ident if thread else None

    def register(self, msg_type: str, handler: Handler) -> None:
        if not msg_type or not callable(handler):
            raise ValueError("msg_type and callable handler are required")
        with self._handlers_lock:
            handlers = self._handlers.setdefault(str(msg_type), [])
            if handler not in handlers:
                handlers.append(handler)

    def unregister(self, msg_type: str, handler: Handler) -> None:
        with self._handlers_lock:
            handlers = self._handlers.get(str(msg_type), [])
            self._handlers[str(msg_type)] = [item for item in handlers if item is not handler]

    def start(self, name: str = "qmt-local-dispatch") -> None:
        with self._lifecycle_lock:
            if self._thread and self._thread.is_alive():
                return
            self._thread = threading.Thread(target=self._run, name=name, daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        with self._lifecycle_lock:
            thread = self._thread
            if not thread:
                return
            try:
                self._queue.put(_STOP, timeout=max(0.01, float(timeout)))
            except queue.Full:
                logger.error("dispatcher did not stop: queue remained full")
                return
        if thread is threading.current_thread():
            return
        thread.join(timeout=max(0.01, float(timeout)))
        with self._lifecycle_lock:
            if self._thread is thread and not thread.is_alive():
                self._thread = None

    def submit(
        self,
        message: Dict[str, Any],
        completion: Optional[Completion] = None,
    ) -> bool:
        try:
            self._queue.put_nowait(_DispatchItem(dict(message), completion))
            return True
        except queue.Full:
            return False

    def dispatch_now(self, message: Dict[str, Any]) -> DispatchResult:
        msg_type = str(message.get("type") or "")
        with self._handlers_lock:
            handlers = tuple(self._handlers.get(msg_type, ()))
        if not handlers:
            logger.warning("no handler registered for message type=%s", msg_type)
            return DispatchResult(False, False, 0)
        failures = []
        for handler in handlers:
            try:
                result = handler(message)
                if inspect.isawaitable(result):
                    asyncio.run(result)
            except Exception:
                name = getattr(handler, "__qualname__", repr(handler))
                failures.append(name)
                logger.exception("message handler failed type=%s handler=%s", msg_type, name)
        return DispatchResult(True, not failures, len(handlers), tuple(failures))

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is _STOP:
                    return
                assert isinstance(item, _DispatchItem)
                result = self.dispatch_now(item.message)
                if item.completion is not None:
                    try:
                        item.completion(result)
                    except Exception:
                        logger.exception("dispatch completion callback failed")
            finally:
                self._queue.task_done()
