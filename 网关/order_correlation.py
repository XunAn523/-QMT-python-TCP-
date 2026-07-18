#!/usr/bin/env python3
"""Persistent order idempotency and QMT callback correlation for the gateway."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


class IdempotencyConflict(RuntimeError):
    pass


class WriterLeaseError(RuntimeError):
    pass


def qmt_correlation_value(event: Dict[str, Any]) -> str:
    """Return an explicit QMT id or a validated generated-id remark fallback."""
    explicit = str((event or {}).get("qmt_user_order_id") or "").strip()
    if explicit:
        return explicit
    remark = str((event or {}).get("order_remark") or "").strip()
    prefix, separator, suffix = remark.rpartition("_")
    if (
        separator
        and len(remark) <= 23
        and 1 <= len(prefix) <= 6
        and all(character.isalnum() for character in prefix)
        and len(suffix) == 16
        and all(character.lower() in "0123456789abcdef" for character in suffix)
    ):
        return remark
    return ""


class OrderCorrelationStore:
    STAGE_RANK = {
        "RESERVED": 0,
        "BRIDGE_QUEUED": 1,
        "SUBMIT_UNKNOWN": 2,
        "QMT_SUBMITTED": 3,
        "QMT_ORDER_CREATED": 4,
        "BROKER_ACCEPTED": 5,
        "PARTIAL": 6,
        "FILLED": 7,
        "CANCELLED": 7,
        "REJECTED": 7,
    }
    TERMINAL_STAGES = {"FILLED", "CANCELLED", "REJECTED"}

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(self.path), timeout=2.0, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS order_correlation (
                account_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                trace_id TEXT NOT NULL,
                msg_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                qmt_user_order_id TEXT NOT NULL,
                trader_name TEXT NOT NULL DEFAULT '',
                authenticated_trader_key TEXT NOT NULL DEFAULT '',
                side TEXT NOT NULL DEFAULT '',
                order_type INTEGER NOT NULL DEFAULT 0,
                intent_hash TEXT NOT NULL,
                stage TEXT NOT NULL,
                order_id TEXT,
                order_sysid TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                terminal_at REAL,
                PRIMARY KEY (account_id, client_order_id),
                UNIQUE (account_id, msg_id),
                UNIQUE (account_id, qmt_user_order_id)
            );
            CREATE INDEX IF NOT EXISTS idx_order_correlation_order_id
            ON order_correlation(account_id, order_id);
            CREATE INDEX IF NOT EXISTS idx_order_correlation_order_sysid
            ON order_correlation(account_id, order_sysid);
            CREATE TABLE IF NOT EXISTS gateway_event_dedupe (
                account_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (account_id, event_id)
            );
            """
        )
        columns = {
            str(row[1]) for row in self._db.execute("PRAGMA table_info(order_correlation)").fetchall()
        }
        for name, definition in (
            ("authenticated_trader_key", "TEXT NOT NULL DEFAULT ''"),
            ("side", "TEXT NOT NULL DEFAULT ''"),
            ("order_type", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if name in columns:
                continue
            try:
                self._db.execute(
                    "ALTER TABLE order_correlation ADD COLUMN %s %s" % (name, definition)
                )
            except sqlite3.OperationalError:
                refreshed = {
                    str(row[1])
                    for row in self._db.execute("PRAGMA table_info(order_correlation)").fetchall()
                }
                if name not in refreshed:
                    raise
            columns.add(name)
        self._db.commit()

    @staticmethod
    def _row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        return dict(row) if row is not None else None

    def reserve(self, item: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        account_id = str(item.get("account_id") or "")
        client_order_id = str(item.get("client_order_id") or "")
        intent_hash = str(item.get("intent_hash") or "")
        qmt_user_order_id = str(item.get("qmt_user_order_id") or "")
        if not account_id or not client_order_id or not intent_hash or not qmt_user_order_id:
            raise ValueError("account_id/client_order_id/qmt_user_order_id/intent_hash are required")
        ts = time.time()
        try:
            order_type = int(item.get("order_type") or 0)
        except (TypeError, ValueError):
            order_type = 0
        authenticated_trader_key = str(item.get("authenticated_trader_key") or "").strip()
        values = (
            account_id,
            client_order_id,
            str(item.get("trace_id") or ""),
            str(item.get("msg_id") or ""),
            str(item.get("request_id") or ""),
            qmt_user_order_id,
            str(item.get("trader_name") or ""),
            authenticated_trader_key,
            str(item.get("side") or "").strip().upper(),
            order_type,
            intent_hash,
            str(item.get("stage") or "RESERVED"),
            ts,
            ts,
        )
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    "SELECT * FROM order_correlation WHERE account_id=? AND client_order_id=?",
                    (account_id, client_order_id),
                ).fetchone()
                if row is not None:
                    existing = dict(row)
                    if existing["intent_hash"] != intent_hash:
                        raise IdempotencyConflict("client_order_id is already bound to a different intent")
                    existing_trader_key = str(
                        existing.get("authenticated_trader_key") or ""
                    ).strip()
                    if (
                        authenticated_trader_key
                        and existing_trader_key
                        and authenticated_trader_key != existing_trader_key
                    ):
                        raise IdempotencyConflict(
                            "client_order_id is already bound to a different authenticated trader"
                        )
                    if authenticated_trader_key and not existing_trader_key:
                        self._db.execute(
                            """UPDATE order_correlation
                            SET authenticated_trader_key=?, updated_at=?
                            WHERE account_id=? AND client_order_id=?""",
                            (
                                authenticated_trader_key,
                                ts,
                                account_id,
                                client_order_id,
                            ),
                        )
                        existing["authenticated_trader_key"] = authenticated_trader_key
                        existing["updated_at"] = ts
                    self._db.commit()
                    return existing, True
                self._db.execute(
                    """INSERT INTO order_correlation
                    (account_id,client_order_id,trace_id,msg_id,request_id,qmt_user_order_id,
                     trader_name,authenticated_trader_key,side,order_type,intent_hash,stage,
                     created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
        return self.get(account_id, client_order_id) or dict(item), False

    def get(self, account_id: str, client_order_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._row(self._db.execute(
                "SELECT * FROM order_correlation WHERE account_id=? AND client_order_id=?",
                (account_id, client_order_id),
            ).fetchone())

    def update_stage(self, account_id: str, client_order_id: str, stage: str, **ids: Any) -> None:
        stage = str(stage or "")
        with self._lock:
            row = self._db.execute(
                "SELECT stage FROM order_correlation WHERE account_id=? AND client_order_id=?",
                (account_id, client_order_id),
            ).fetchone()
            if row is None:
                return
            current = str(row["stage"] or "RESERVED")
            if current in self.TERMINAL_STAGES:
                return
            if self.STAGE_RANK.get(stage, -1) < self.STAGE_RANK.get(current, -1):
                return
            terminal_at = time.time() if stage in self.TERMINAL_STAGES else None
            self._db.execute(
                """UPDATE order_correlation SET stage=?, order_id=COALESCE(NULLIF(?,''),order_id),
                order_sysid=COALESCE(NULLIF(?,''),order_sysid), updated_at=?,
                terminal_at=COALESCE(?,terminal_at) WHERE account_id=? AND client_order_id=?""",
                (stage, str(ids.get("order_id") or ""), str(ids.get("order_sysid") or ""),
                 time.time(), terminal_at, account_id, client_order_id),
            )
            self._db.commit()

    def resolve(self, account_id: str, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidates = (
            ("order_id", str(event.get("order_id") or "")),
            ("order_sysid", str(event.get("order_sysid") or "")),
            ("qmt_user_order_id", qmt_correlation_value(event)),
        )
        with self._lock:
            for column, value in candidates:
                if not value or value == "0":
                    continue
                row = self._db.execute(
                    "SELECT * FROM order_correlation WHERE account_id=? AND %s=? ORDER BY created_at DESC LIMIT 1" % column,
                    (account_id, value),
                ).fetchone()
                if row is not None:
                    return dict(row)
        return None

    def resolve_many(self, account_id: str, events: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        """Resolve a snapshot batch with bounded SQLite queries.

        Rows are returned oldest first so callers can populate an insertion-ordered
        cache while keeping the newest correlation authoritative for duplicate QMT
        identifiers.
        """
        candidates = {
            "order_id": set(),
            "order_sysid": set(),
            "qmt_user_order_id": set(),
        }
        for event in events or []:
            if not isinstance(event, dict):
                continue
            values = {
                "order_id": str(event.get("order_id") or ""),
                "order_sysid": str(event.get("order_sysid") or ""),
                "qmt_user_order_id": qmt_correlation_value(event),
            }
            for column, value in values.items():
                if value and value != "0":
                    candidates[column].add(value)

        rows_by_client: Dict[Tuple[str, str], Tuple[float, int, Dict[str, Any]]] = {}
        chunk_size = 400  # Keep account_id + IN values below legacy SQLite limits.
        db = sqlite3.connect(str(self.path), timeout=2.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        try:
            for column, unique_values in candidates.items():
                values = list(unique_values)
                for offset in range(0, len(values), chunk_size):
                    chunk = values[offset:offset + chunk_size]
                    placeholders = ",".join("?" for _ in chunk)
                    rows = db.execute(
                        "SELECT rowid AS correlation_rowid, * FROM order_correlation "
                        "WHERE account_id=? AND %s IN (%s) "
                        "ORDER BY created_at DESC, rowid DESC" % (column, placeholders),
                        [account_id, *chunk],
                    ).fetchall()
                    for row in rows:
                        item = dict(row)
                        rowid = int(item.pop("correlation_rowid", 0) or 0)
                        key = (item["account_id"], item["client_order_id"])
                        rows_by_client[key] = (
                            float(item.get("created_at") or 0.0), rowid, item,
                        )
        finally:
            db.close()
        ordered = sorted(rows_by_client.values(), key=lambda value: (value[0], value[1]))
        return [value[2] for value in ordered]

    def pending(self, account_id: str) -> list[Dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                """SELECT * FROM order_correlation WHERE account_id=?
                AND stage IN ('RESERVED','BRIDGE_QUEUED','SUBMIT_UNKNOWN')""",
                (account_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def recent(self, account_id: str, limit: int = 20000) -> list[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 100000))
        with self._lock:
            rows = self._db.execute(
                """SELECT * FROM order_correlation WHERE account_id=?
                ORDER BY created_at DESC LIMIT ?""",
                (account_id, safe_limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def event_seen(self, account_id: str, event_id: str) -> bool:
        if not event_id:
            return False
        with self._lock:
            return self._db.execute(
                "SELECT 1 FROM gateway_event_dedupe WHERE account_id=? AND event_id=?",
                (account_id, event_id),
            ).fetchone() is not None

    def mark_event(self, account_id: str, event_id: str) -> None:
        if not event_id:
            return
        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO gateway_event_dedupe(account_id,event_id,created_at) VALUES (?,?,?)",
                (account_id, event_id, time.time()),
            )
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()


class WriterLease:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.token = uuid.uuid4().hex
        self.acquired = False
        self._file = None

    def acquire(self) -> str:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            handle.close()
            raise WriterLeaseError("active writer lease is held: %s" % self.path) from exc
        payload = json.dumps({"pid": os.getpid(), "token": self.token, "created_at": time.time()}) + "\n"
        handle.seek(0)
        handle.truncate()
        handle.write(payload.encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
        self._file = handle
        self.acquired = True
        return self.token

    def release(self) -> None:
        if not self.acquired:
            return
        handle = self._file
        try:
            if handle is not None:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()
        finally:
            self._file = None
            self.acquired = False
