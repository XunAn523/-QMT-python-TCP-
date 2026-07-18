#!/usr/bin/env python3
"""Persistent order idempotency and QMT callback correlation for the gateway."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Tuple


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
    EVENT_QUERY_CHUNK_SIZE = 400
    MAX_EVENT_BATCH_SIZE = 4096
    MAX_CLEANUP_ROWS_PER_KIND = 10000
    MIN_COMPLETED_RETENTION_SECONDS = 7 * 24 * 60 * 60.0

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
        self._stage_rank_sql = "CASE stage %s ELSE -1 END" % " ".join(
            "WHEN '%s' THEN %d" % (stage.replace("'", "''"), rank)
            for stage, rank in self.STAGE_RANK.items()
        )
        self._terminal_stages = tuple(sorted(self.TERMINAL_STAGES))
        self._update_stage_sql = (
            "UPDATE order_correlation SET stage=?, "
            "order_id=COALESCE(NULLIF(?,''),order_id), "
            "order_sysid=COALESCE(NULLIF(?,''),order_sysid), updated_at=?, "
            "terminal_at=COALESCE(?,terminal_at) "
            "WHERE account_id=? AND client_order_id=? "
            "AND stage NOT IN (%s) AND (%s) <= ?"
        ) % (
            ",".join("?" for _ in self._terminal_stages),
            self._stage_rank_sql,
        )
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
            CREATE INDEX IF NOT EXISTS idx_order_correlation_terminal_at
            ON order_correlation(terminal_at) WHERE terminal_at IS NOT NULL;
            CREATE TABLE IF NOT EXISTS gateway_event_dedupe (
                account_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (account_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS idx_gateway_event_dedupe_created_at
            ON gateway_event_dedupe(created_at);
            CREATE TABLE IF NOT EXISTS gateway_pending_response (
                account_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                item_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (account_id, request_id)
            );
            CREATE INDEX IF NOT EXISTS idx_gateway_pending_response_updated_at
            ON gateway_pending_response(updated_at);
            CREATE TABLE IF NOT EXISTS gateway_effect_request (
                account_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                state TEXT NOT NULL,
                result_json TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (account_id, request_id)
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
        pending_response_columns = {
            str(row[1])
            for row in self._db.execute(
                "PRAGMA table_info(gateway_pending_response)"
            ).fetchall()
        }
        if "fingerprint" not in pending_response_columns:
            try:
                self._db.execute(
                    "ALTER TABLE gateway_pending_response "
                    "ADD COLUMN fingerprint TEXT NOT NULL DEFAULT ''"
                )
            except sqlite3.OperationalError:
                refreshed = {
                    str(row[1])
                    for row in self._db.execute(
                        "PRAGMA table_info(gateway_pending_response)"
                    ).fetchall()
                }
                if "fingerprint" not in refreshed:
                    raise
        effect_request_columns = {
            str(row[1])
            for row in self._db.execute(
                "PRAGMA table_info(gateway_effect_request)"
            ).fetchall()
        }
        # Existing fingerprint-only rows may already have reached QMT.  They
        # migrate to UNKNOWN, never PREPARED, so an upgrade cannot re-execute
        # an old trading effect after Helper guard retention expires.
        for name, definition in (
            ("state", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
            ("result_json", "TEXT NOT NULL DEFAULT ''"),
            ("updated_at", "REAL NOT NULL DEFAULT 0"),
        ):
            if name in effect_request_columns:
                continue
            self._db.execute(
                "ALTER TABLE gateway_effect_request ADD COLUMN %s %s"
                % (name, definition)
            )
            effect_request_columns.add(name)
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
        return {
            "account_id": account_id,
            "client_order_id": client_order_id,
            "trace_id": str(item.get("trace_id") or ""),
            "msg_id": str(item.get("msg_id") or ""),
            "request_id": str(item.get("request_id") or ""),
            "qmt_user_order_id": qmt_user_order_id,
            "trader_name": str(item.get("trader_name") or ""),
            "authenticated_trader_key": authenticated_trader_key,
            "side": str(item.get("side") or "").strip().upper(),
            "order_type": order_type,
            "intent_hash": intent_hash,
            "stage": str(item.get("stage") or "RESERVED"),
            "order_id": None,
            "order_sysid": None,
            "created_at": ts,
            "updated_at": ts,
            "terminal_at": None,
        }, False

    def release_reservation(
        self,
        account_id: str,
        client_order_id: str,
        intent_hash: str,
    ) -> bool:
        """Release only an exact reservation that has not entered execution."""
        account = str(account_id or "")
        client_order = str(client_order_id or "")
        intent = str(intent_hash or "")
        if not account or not client_order or not intent:
            raise ValueError("account_id/client_order_id/intent_hash are required")
        with self._lock:
            try:
                cursor = self._db.execute(
                    "DELETE FROM order_correlation "
                    "WHERE account_id=? AND client_order_id=? "
                    "AND intent_hash=? AND stage='RESERVED'",
                    (account, client_order, intent),
                )
                self._db.commit()
                return cursor.rowcount == 1
            except Exception:
                self._db.rollback()
                raise

    def release_unstarted_order(
        self,
        account_id: str,
        client_order_id: str,
        intent_hash: str,
        request_id: str = "",
        pending_fingerprint: str = "",
    ) -> bool:
        """Atomically release an order and its async ledger before enqueue.

        Both deletes are conditional.  This method must never remove a row
        that has advanced beyond RESERVED or a pending response owned by a
        different effect fingerprint.
        """
        account = str(account_id or "")
        client_order = str(client_order_id or "")
        intent = str(intent_hash or "")
        request = str(request_id or "")
        fingerprint = str(pending_fingerprint or "")
        if not account or not client_order or not intent:
            raise ValueError("account_id/client_order_id/intent_hash are required")
        if bool(request) != bool(fingerprint):
            raise ValueError(
                "request_id and pending_fingerprint must be provided together"
            )
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    "DELETE FROM order_correlation "
                    "WHERE account_id=? AND client_order_id=? "
                    "AND intent_hash=? AND stage='RESERVED'",
                    (account, client_order, intent),
                )
                if cursor.rowcount != 1:
                    raise IdempotencyConflict(
                        "order reservation is not an exact RESERVED state"
                    )
                if request:
                    pending_cursor = self._db.execute(
                        "DELETE FROM gateway_pending_response "
                        "WHERE account_id=? AND request_id=? AND fingerprint=?",
                        (account, request, fingerprint),
                    )
                    if pending_cursor.rowcount == 0 and self._db.execute(
                        "SELECT 1 FROM gateway_pending_response "
                        "WHERE account_id=? AND request_id=?",
                        (account, request),
                    ).fetchone() is not None:
                        raise IdempotencyConflict(
                            "pending response belongs to a different effect"
                        )
                    state_cursor = self._db.execute(
                        "UPDATE gateway_effect_request SET state='PREPARED',"
                        "result_json='',updated_at=? WHERE account_id=? "
                        "AND request_id=? AND fingerprint=? "
                        "AND state IN ('PREPARED','DISPATCHING')",
                        (time.time(), account, request, fingerprint),
                    )
                    if state_cursor.rowcount != 1:
                        raise IdempotencyConflict(
                            "effect request is not an exact retryable record"
                        )
                self._db.commit()
                return True
            except Exception:
                self._db.rollback()
                raise

    def release_unstarted_cancel(
        self,
        account_id: str,
        request_id: str,
        fingerprint: str,
    ) -> bool:
        account = str(account_id or "")
        request = str(request_id or "")
        effect_fingerprint = str(fingerprint or "")
        if not account or not request or not effect_fingerprint:
            raise ValueError("account_id/request_id/fingerprint are required")
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                pending_cursor = self._db.execute(
                    "DELETE FROM gateway_pending_response WHERE account_id=? "
                    "AND request_id=? AND fingerprint=?",
                    (account, request, effect_fingerprint),
                )
                if pending_cursor.rowcount == 0 and self._db.execute(
                    "SELECT 1 FROM gateway_pending_response "
                    "WHERE account_id=? AND request_id=?",
                    (account, request),
                ).fetchone() is not None:
                    raise IdempotencyConflict(
                        "pending cancel response belongs to a different effect"
                    )
                cursor = self._db.execute(
                    "UPDATE gateway_effect_request SET state='PREPARED',"
                    "result_json='',updated_at=? WHERE account_id=? "
                    "AND request_id=? AND fingerprint=? "
                    "AND state IN ('PREPARED','DISPATCHING')",
                    (time.time(), account, request, effect_fingerprint),
                )
                if cursor.rowcount != 1:
                    raise IdempotencyConflict(
                        "cancel effect request is not retryable"
                    )
                self._db.commit()
                return True
            except Exception:
                self._db.rollback()
                raise

    def get(self, account_id: str, client_order_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._row(self._db.execute(
                "SELECT * FROM order_correlation WHERE account_id=? AND client_order_id=?",
                (account_id, client_order_id),
            ).fetchone())

    def update_stage(self, account_id: str, client_order_id: str, stage: str, **ids: Any) -> None:
        stage = str(stage or "")
        stage_rank = self.STAGE_RANK.get(stage, -1)
        ts = time.time()
        terminal_at = ts if stage in self.TERMINAL_STAGES else None
        with self._lock:
            try:
                self._db.execute(
                    self._update_stage_sql,
                    (
                        stage,
                        str(ids.get("order_id") or ""),
                        str(ids.get("order_sysid") or ""),
                        ts,
                        terminal_at,
                        account_id,
                        client_order_id,
                        *self._terminal_stages,
                        stage_rank,
                    ),
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

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

    def save_pending_response(
        self,
        account_id: str,
        request_id: str,
        item: Dict[str, Any],
    ) -> None:
        """Durably upsert response-delivery state before file enqueue."""
        account = str(account_id or "").strip()
        request = str(request_id or "").strip()
        if not account or not request:
            raise ValueError("account_id and request_id are required")
        if not isinstance(item, dict):
            raise TypeError("pending response item must be a dict")
        kind = str(item.get("kind") or "").strip().lower()
        fingerprint = str(item.get("fingerprint") or "").strip()
        if not kind or not fingerprint:
            raise IdempotencyConflict(
                "pending response kind and fingerprint are required"
            )
        stored_item = dict(item)
        stored_item["kind"] = kind
        stored_item["fingerprint"] = fingerprint
        encoded = json.dumps(
            stored_item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        timestamp = time.time()
        with self._lock:
            try:
                cursor = self._db.execute(
                    "INSERT INTO gateway_pending_response("
                    "account_id,request_id,kind,fingerprint,item_json,"
                    "created_at,updated_at"
                    ") VALUES(?,?,?,?,?,?,?) "
                    "ON CONFLICT(account_id,request_id) DO UPDATE SET "
                    "item_json=excluded.item_json,updated_at=excluded.updated_at "
                    "WHERE gateway_pending_response.kind=excluded.kind "
                    "AND gateway_pending_response.fingerprint=excluded.fingerprint",
                    (
                        account,
                        request,
                        kind,
                        fingerprint,
                        encoded,
                        timestamp,
                        timestamp,
                    ),
                )
                if cursor.rowcount != 1:
                    raise IdempotencyConflict(
                        "request_id is already bound to a different pending response"
                    )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def load_pending_responses(
        self,
        account_id: str,
        limit: int = 4096,
    ) -> list[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 10000))
        with self._lock:
            rows = self._db.execute(
                "SELECT request_id,kind,fingerprint,item_json "
                "FROM gateway_pending_response "
                "WHERE account_id=? ORDER BY created_at,request_id LIMIT ?",
                (str(account_id or ""), safe_limit),
            ).fetchall()
        result = []
        for row in rows:
            try:
                item = json.loads(str(row["item_json"]))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "invalid persisted pending response: %s" % row["request_id"]
                ) from exc
            if not isinstance(item, dict):
                raise ValueError(
                    "persisted pending response is not an object: %s"
                    % row["request_id"]
                )
            item["request_id"] = str(row["request_id"])
            item["kind"] = str(row["kind"] or "")
            item["fingerprint"] = str(row["fingerprint"] or "")
            result.append(item)
        return result

    def adopt_legacy_pending_response(
        self,
        account_id: str,
        request_id: str,
        pending_kind: str,
        effect_kind: str,
        fingerprint: str,
        item: Dict[str, Any],
    ) -> None:
        """Atomically bind a pre-fingerprint pending row fail-closed."""
        account = str(account_id or "").strip()
        request = str(request_id or "").strip()
        normalized_pending_kind = str(pending_kind or "").strip().lower()
        normalized_effect_kind = str(effect_kind or "").strip().lower()
        normalized_fingerprint = str(fingerprint or "").strip()
        if not all((
            account,
            request,
            normalized_pending_kind,
            normalized_effect_kind,
            normalized_fingerprint,
        )):
            raise ValueError("legacy pending response identity is incomplete")
        stored_item = dict(item)
        stored_item["kind"] = normalized_pending_kind
        stored_item["fingerprint"] = normalized_fingerprint
        encoded = json.dumps(
            stored_item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        timestamp = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    "UPDATE gateway_pending_response SET kind=?,fingerprint=?,"
                    "item_json=?,updated_at=? WHERE account_id=? AND request_id=? "
                    "AND fingerprint=''",
                    (
                        normalized_pending_kind,
                        normalized_fingerprint,
                        encoded,
                        timestamp,
                        account,
                        request,
                    ),
                )
                if cursor.rowcount != 1:
                    raise IdempotencyConflict(
                        "legacy pending response was already adopted or changed"
                    )
                effect_cursor = self._db.execute(
                    "INSERT OR IGNORE INTO gateway_effect_request("
                    "account_id,request_id,kind,fingerprint,state,result_json,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        account,
                        request,
                        normalized_effect_kind,
                        normalized_fingerprint,
                        "UNKNOWN",
                        "",
                        timestamp,
                        timestamp,
                    ),
                )
                if effect_cursor.rowcount != 1:
                    existing = self._db.execute(
                        "SELECT kind,fingerprint FROM gateway_effect_request "
                        "WHERE account_id=? AND request_id=?",
                        (account, request),
                    ).fetchone()
                    if (
                        existing is None
                        or str(existing["kind"] or "") != normalized_effect_kind
                        or str(existing["fingerprint"] or "")
                        != normalized_fingerprint
                    ):
                        raise IdempotencyConflict(
                            "legacy pending response conflicts with effect registry"
                        )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def reserve_effect_request(
        self,
        account_id: str,
        request_id: str,
        kind: str,
        fingerprint: str,
    ) -> Tuple[Dict[str, Any], bool]:
        """Permanently bind an effectful request id to one kind and fingerprint."""
        account = str(account_id or "").strip()
        request = str(request_id or "").strip()
        normalized_kind = str(kind or "").strip().lower()
        normalized_fingerprint = str(fingerprint or "").strip()
        if not account or not request:
            raise ValueError("account_id and request_id are required")
        if not normalized_kind or not normalized_fingerprint:
            raise IdempotencyConflict(
                "effect request kind and fingerprint are required"
            )
        created_at = time.time()
        record = {
            "account_id": account,
            "request_id": request,
            "kind": normalized_kind,
            "fingerprint": normalized_fingerprint,
            "state": "PREPARED",
            "result": None,
            "created_at": created_at,
            "updated_at": created_at,
        }
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    "INSERT OR IGNORE INTO gateway_effect_request("
                    "account_id,request_id,kind,fingerprint,state,result_json,"
                    "created_at,updated_at"
                    ") VALUES(?,?,?,?,?,?,?,?)",
                    (
                        account,
                        request,
                        normalized_kind,
                        normalized_fingerprint,
                        "PREPARED",
                        "",
                        created_at,
                        created_at,
                    ),
                )
                if cursor.rowcount == 1:
                    self._db.commit()
                    return record, False
                row = self._db.execute(
                    "SELECT account_id,request_id,kind,fingerprint,state,"
                    "result_json,created_at,updated_at "
                    "FROM gateway_effect_request "
                    "WHERE account_id=? AND request_id=?",
                    (account, request),
                ).fetchone()
                existing = self._decode_effect_request_row(row)
                if (
                    existing is not None
                    and str(existing.get("kind") or "") == normalized_kind
                    and str(existing.get("fingerprint") or "")
                    == normalized_fingerprint
                ):
                    self._db.commit()
                    return existing, True
                raise IdempotencyConflict(
                    "request_id is already bound to a different effect request"
                )
            except Exception:
                self._db.rollback()
                raise

    @staticmethod
    def _decode_effect_request_row(
        row: Optional[sqlite3.Row],
    ) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        result = dict(row)
        encoded = str(result.pop("result_json", "") or "")
        if not encoded:
            result["result"] = None
            return result
        try:
            decoded = json.loads(encoded)
        except (TypeError, ValueError):
            decoded = None
        result["result"] = decoded if isinstance(decoded, dict) else None
        return result

    def transition_effect_request(
        self,
        account_id: str,
        request_id: str,
        fingerprint: str,
        state: str,
        result: Optional[Dict[str, Any]] = None,
        allowed_from: Iterable[str] = ("PREPARED",),
    ) -> bool:
        normalized_state = str(state or "").strip().upper()
        allowed = tuple(
            str(value or "").strip().upper()
            for value in allowed_from
            if str(value or "").strip()
        )
        if normalized_state not in {
            "PREPARED", "DISPATCHING", "ENQUEUED", "UNKNOWN", "TERMINAL"
        }:
            raise ValueError("invalid effect request state")
        if not allowed:
            raise ValueError("allowed_from must not be empty")
        if result is not None and not isinstance(result, dict):
            raise TypeError("effect request result must be a dict")
        encoded = (
            json.dumps(
                result,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            if result is not None
            else None
        )
        placeholders = ",".join("?" for _ in allowed)
        with self._lock:
            try:
                cursor = self._db.execute(
                    "UPDATE gateway_effect_request SET state=?,"
                    "result_json=CASE WHEN ? IS NULL THEN result_json ELSE ? END,"
                    "updated_at=? WHERE account_id=? AND request_id=? "
                    "AND fingerprint=? AND state IN (%s)" % placeholders,
                    (
                        normalized_state,
                        encoded,
                        encoded,
                        time.time(),
                        str(account_id or "").strip(),
                        str(request_id or "").strip(),
                        str(fingerprint or "").strip(),
                        *allowed,
                    ),
                )
                self._db.commit()
                return cursor.rowcount == 1
            except Exception:
                self._db.rollback()
                raise

    def get_effect_request(
        self,
        account_id: str,
        request_id: str,
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._decode_effect_request_row(
                self._db.execute(
                    "SELECT account_id,request_id,kind,fingerprint,state,"
                    "result_json,created_at,updated_at "
                    "FROM gateway_effect_request "
                    "WHERE account_id=? AND request_id=?",
                    (str(account_id or "").strip(), str(request_id or "").strip()),
                ).fetchone()
            )

    def remove_pending_response(self, account_id: str, request_id: str) -> None:
        with self._lock:
            try:
                self._db.execute(
                    "DELETE FROM gateway_pending_response "
                    "WHERE account_id=? AND request_id=?",
                    (str(account_id or ""), str(request_id or "")),
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def event_seen(self, account_id: str, event_id: str) -> bool:
        if not event_id:
            return False
        with self._lock:
            return self._db.execute(
                "SELECT 1 FROM gateway_event_dedupe WHERE account_id=? AND event_id=?",
                (account_id, event_id),
            ).fetchone() is not None

    @classmethod
    def _normalize_event_ids(cls, event_ids: Iterable[str]) -> list[str]:
        if isinstance(event_ids, (str, bytes)):
            raise TypeError("event_ids must be an iterable of event id values")
        result = []
        seen = set()
        for raw_value in event_ids:
            if not raw_value:
                continue
            value = str(raw_value)
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
            if len(result) > cls.MAX_EVENT_BATCH_SIZE:
                raise ValueError(
                    "event batch exceeds maximum size %d" % cls.MAX_EVENT_BATCH_SIZE
                )
        return result

    def events_seen_many(self, account_id: str, event_ids: Iterable[str]) -> Set[str]:
        """Return the subset of event ids already committed as delivered."""
        values = self._normalize_event_ids(event_ids)
        if not values:
            return set()
        result: Set[str] = set()
        with self._lock:
            for offset in range(0, len(values), self.EVENT_QUERY_CHUNK_SIZE):
                chunk = values[offset:offset + self.EVENT_QUERY_CHUNK_SIZE]
                placeholders = ",".join("?" for _ in chunk)
                rows = self._db.execute(
                    "SELECT event_id FROM gateway_event_dedupe "
                    "WHERE account_id=? AND event_id IN (%s)" % placeholders,
                    [account_id, *chunk],
                ).fetchall()
                result.update(str(row["event_id"]) for row in rows)
        return result

    def mark_events(self, account_id: str, event_ids: Iterable[str]) -> int:
        """Atomically mark a bounded event batch and return newly inserted rows."""
        values = self._normalize_event_ids(event_ids)
        if not values:
            return 0
        created_at = time.time()
        with self._lock:
            before = self._db.total_changes
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.executemany(
                    "INSERT OR IGNORE INTO gateway_event_dedupe"
                    "(account_id,event_id,created_at) VALUES (?,?,?)",
                    ((account_id, event_id, created_at) for event_id in values),
                )
                inserted = self._db.total_changes - before
                self._db.commit()
                return int(inserted)
            except Exception:
                self._db.rollback()
                raise

    def mark_event(self, account_id: str, event_id: str) -> None:
        if not event_id:
            return
        self.mark_events(account_id, [event_id])

    @classmethod
    def _validate_cleanup_limit(cls, value: int, name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("%s must be an integer" % name)
        if value < 0 or value > cls.MAX_CLEANUP_ROWS_PER_KIND:
            raise ValueError(
                "%s must be between 0 and %d"
                % (name, cls.MAX_CLEANUP_ROWS_PER_KIND)
            )
        return value

    def cleanup_completed(
        self,
        before: float,
        event_limit: int = 1000,
        order_limit: int = 1000,
    ) -> Dict[str, Any]:
        """Delete only old delivered events and terminal orders in bounded batches."""
        try:
            cutoff = float(before)
        except (TypeError, ValueError) as exc:
            raise ValueError("before must be a finite timestamp") from exc
        if not math.isfinite(cutoff):
            raise ValueError("before must be a finite timestamp")
        newest_allowed = time.time() - self.MIN_COMPLETED_RETENTION_SECONDS
        if cutoff > newest_allowed:
            raise ValueError(
                "before must retain at least %.0f seconds of completed state"
                % self.MIN_COMPLETED_RETENTION_SECONDS
            )
        event_limit = self._validate_cleanup_limit(event_limit, "event_limit")
        order_limit = self._validate_cleanup_limit(order_limit, "order_limit")
        result: Dict[str, Any] = {
            "before": cutoff,
            "events_deleted": 0,
            "orders_deleted": 0,
        }
        if event_limit == 0 and order_limit == 0:
            return result
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                if event_limit:
                    before_changes = self._db.total_changes
                    self._db.execute(
                        "DELETE FROM gateway_event_dedupe WHERE rowid IN ("
                        "SELECT rowid FROM gateway_event_dedupe "
                        "WHERE created_at < ? ORDER BY created_at, rowid LIMIT ?)",
                        (cutoff, event_limit),
                    )
                    result["events_deleted"] = (
                        self._db.total_changes - before_changes
                    )
                if order_limit:
                    before_changes = self._db.total_changes
                    placeholders = ",".join("?" for _ in self._terminal_stages)
                    self._db.execute(
                        "DELETE FROM order_correlation WHERE rowid IN ("
                        "SELECT rowid FROM order_correlation "
                        "WHERE terminal_at IS NOT NULL AND terminal_at < ? "
                        "AND stage IN (%s) "
                        "ORDER BY terminal_at, rowid LIMIT ?)" % placeholders,
                        (cutoff, *self._terminal_stages, order_limit),
                    )
                    result["orders_deleted"] = (
                        self._db.total_changes - before_changes
                    )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
        return result

    def checkpoint_wal(self) -> Dict[str, int]:
        """Run a non-blocking PASSIVE WAL checkpoint for an explicitly idle period."""
        with self._lock:
            if self._db.in_transaction:
                raise RuntimeError("cannot checkpoint WAL during an active transaction")
            row = self._db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        if row is None:
            return {"busy": 0, "log_frames": 0, "checkpointed_frames": 0}
        return {
            "busy": int(row[0] or 0),
            "log_frames": int(row[1] or 0),
            "checkpointed_frames": int(row[2] or 0),
        }

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
