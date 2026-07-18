"""Shared safe helpers for the runnable external-strategy examples."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import sqlite3
import sys
import threading
import time
from typing import Callable, Dict, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_ROOT = PROJECT_ROOT / "外置策略API"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api import LocalQmtApi, LocalRuntimeConfig, redact_for_output


ENV_FILE = PROJECT_ROOT / ".env"
EVENT_TYPES = (
    "ASYNC_ORDER",
    "ASYNC_ORDER_RESPONSE",
    "ASYNC_CANCEL",
    "ASYNC_CANCEL_RESPONSE",
    "EXEC_REPORT",
    "ORDER_UPDATE",
    "TRADE_NOTIFY",
    "ORDER_ERROR",
    "ASSET_UPDATE",
    "POSITIONS_SNAPSHOT",
    "QMT_STATUS",
    "ERROR",
    "RECONCILE_REQUIRED",
)


def configure_logging(runtime: LocalRuntimeConfig) -> None:
    logging.basicConfig(
        level=getattr(logging, runtime.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_api(runtime: LocalRuntimeConfig) -> LocalQmtApi:
    return LocalQmtApi(
        runtime.connection,
        query_timeout=runtime.query_timeout,
        max_pending_queries=runtime.max_pending_queries,
        dispatch_queue_size=runtime.dispatch_queue_size,
        completed_delivery_cache=runtime.completed_delivery_cache,
        runtime=runtime,
    )


def connect_or_raise(api: LocalQmtApi) -> None:
    if api.connect():
        return
    if api.identity_guard_failed:
        raise RuntimeError(
            "local Gateway identity handshake failed: " + api.identity_guard_reason
        )
    raise ConnectionError("local Gateway is temporarily unavailable")


class EventJournal:
    """Commit reliable events durably before their callback returns."""

    def __init__(self, path: str) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._database = sqlite3.connect(str(self.path), timeout=5.0, check_same_thread=False)
        with self._database:
            self._database.execute("PRAGMA journal_mode=WAL")
            self._database.execute("PRAGMA synchronous=FULL")
            self._database.execute(
                "CREATE TABLE IF NOT EXISTS bridge_events ("
                "delivery_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, "
                "payload_json TEXT NOT NULL, committed_at_ns INTEGER NOT NULL)"
            )

    def persist(self, message: Dict[str, object]) -> None:
        delivery_id = str(message.get("delivery_id") or "")
        if not delivery_id:
            return
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._database:
            self._database.execute(
                "INSERT OR IGNORE INTO bridge_events("
                "delivery_id,event_type,payload_json,committed_at_ns) VALUES(?,?,?,?)",
                (
                    delivery_id,
                    str(message.get("type") or ""),
                    payload,
                    time.time_ns(),
                ),
            )

    def close(self) -> None:
        with self._lock:
            self._database.close()


def register_journal_handlers(
    api: LocalQmtApi,
    journal: EventJournal,
    after_commit: Optional[Callable[[Dict[str, object]], None]] = None,
) -> None:
    def persist_then_publish(message):
        journal.persist(message)
        print(
            json.dumps(
                redact_for_output(message),
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        if after_commit is not None:
            after_commit(message)

    for event_type in EVENT_TYPES:
        api.on(event_type, persist_then_publish)
