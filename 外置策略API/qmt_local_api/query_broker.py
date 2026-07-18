"""Bounded QUERY waiters that never read the TCP socket themselves."""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
import uuid
from typing import Any, Callable, Dict, Optional


@dataclass
class _PendingQuery:
    event: threading.Event = field(default_factory=threading.Event)
    response: Optional[Dict[str, Any]] = None


class QueryBroker:
    """Correlate responses by msg_id; only the poll thread calls resolve()."""

    def __init__(self, max_pending: int = 128) -> None:
        if int(max_pending) <= 0:
            raise ValueError("max_pending must be positive")
        self._max_pending = int(max_pending)
        self._pending: Dict[str, _PendingQuery] = {}
        self._lock = threading.Lock()

    @staticmethod
    def new_msg_id() -> str:
        return uuid.uuid4().hex[:16]

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def request(
        self,
        sender: Callable[..., str],
        *,
        query_type: str = "",
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        timeout = min(max(float(timeout), 0.01), 60.0)
        msg_id = self.new_msg_id()
        pending = _PendingQuery()
        with self._lock:
            if len(self._pending) >= self._max_pending:
                raise RuntimeError("query broker capacity exceeded")
            self._pending[msg_id] = pending
        try:
            sender(query_type=query_type, params=params or {}, msg_id=msg_id)
            if not pending.event.wait(timeout):
                return None
            if pending.response and pending.response.get("cancelled"):
                return None
            return pending.response
        finally:
            with self._lock:
                self._pending.pop(msg_id, None)

    def resolve(self, message: Dict[str, Any]) -> bool:
        msg_id = str(message.get("msg_id") or "")
        if not msg_id:
            return False
        with self._lock:
            pending = self._pending.get(msg_id)
            if pending is None:
                return False
            pending.response = message
            pending.event.set()
            return True

    def cancel_all(self) -> None:
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for item in pending:
            item.response = {"type": "QUERY_CANCELLED", "cancelled": True}
            item.event.set()
