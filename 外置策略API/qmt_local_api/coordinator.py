"""Durable single-account coordinator for multiple external strategies.

The Gateway deliberately has one reliable-event primary per account.  This
module is the layer above that boundary: one coordinator owns the only
``LocalQmtApi`` instance, persists Gateway deliveries, applies account-level
reservations, and exposes per-strategy command and event-outbox operations.

It intentionally does not change the Gateway protocol or make the Helper a
multi-client service.  A process boundary (for example a loopback service) can
call this class, while strategy processes must never share the Gateway token.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence


_STRATEGY_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,47}$")
_TERMINAL_STAGES = {"FILLED", "CANCELLED", "CANCELED", "REJECTED", "FAILED"}
_UNKNOWN_STAGES = {
    "UNKNOWN",
    "SUBMIT_UNKNOWN",
    "EFFECT_STATE_UNKNOWN",
    "POST_ENQUEUE_STATE_UNCERTAIN",
}
_BROADCAST_EVENT_TYPES = {"QMT_STATUS", "RECONCILE_REQUIRED"}
_GATEWAY_EVENT_TYPES = (
    "ASYNC_ORDER",
    "ASYNC_ORDER_RESPONSE",
    "ASYNC_CANCEL",
    "ASYNC_CANCEL_RESPONSE",
    "ORDER_UPDATE",
    "TRADE_NOTIFY",
    "ORDER_ERROR",
    "ASSET_UPDATE",
    "POSITIONS_SNAPSHOT",
    "QMT_STATUS",
    "RECONCILE_REQUIRED",
    "ERROR",
)


class CoordinatorError(RuntimeError):
    """Base exception for Coordinator failures that do not reach QMT."""


class CoordinatorConflict(CoordinatorError):
    """A strategy idempotency key is already bound to another intent."""


class CoordinatorRiskRejected(CoordinatorError):
    """The order would exceed a strategy or account risk limit."""


class CoordinatorUnavailable(CoordinatorError):
    """The account is not healthy enough to begin a new trading effect."""


class CoordinatorClient(Protocol):
    """The narrow public ``LocalQmtApi`` surface used by the coordinator."""

    config: Any

    @property
    def is_connected(self) -> bool:
        ...

    def on(self, msg_type: str, handler: Any) -> None:
        ...

    def connect(self, timeout: Optional[float] = None) -> bool:
        ...

    def stop(self, timeout: float = 5.0) -> None:
        ...

    def query(
        self,
        query_type: str = "",
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        ...

    def place_order_async(self, *args: Any, **kwargs: Any) -> str:
        ...

    def cancel_order_async(self, *args: Any, **kwargs: Any) -> str:
        ...


@dataclass(frozen=True)
class RiskLimits:
    """Notional limits where zero means no limit for that dimension."""

    max_order_notional: float = 0.0
    max_pending_notional: float = 0.0

    def validate(self) -> "RiskLimits":
        for name, value in (
            ("max_order_notional", self.max_order_notional),
            ("max_pending_notional", self.max_pending_notional),
        ):
            if not math.isfinite(float(value)) or float(value) < 0:
                raise ValueError("%s must be a finite non-negative number" % name)
        return self


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _as_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    result = dict(row)
    for key in ("intent_json", "detail_json", "event_json"):
        raw = result.get(key)
        if not raw:
            result.pop(key, None)
            continue
        try:
            result[key[:-5] if key.endswith("_json") else key] = json.loads(raw)
        except (TypeError, ValueError):
            result[key[:-5] if key.endswith("_json") else key] = {}
        result.pop(key, None)
    return result


class AccountCoordinator:
    """One durable owner of a single account's ``LocalQmtApi`` connection.

    The class is deliberately synchronous.  ``LocalQmtApi`` already owns the
    one TCP reader and invokes handlers on its ordered dispatcher.  All SQLite
    access is serialised here so an API dispatcher thread and strategy request
    threads cannot race command identity, reservations, or event ACK state.
    """

    def __init__(
        self,
        api: CoordinatorClient,
        database_path: str | Path,
        *,
        account_limits: RiskLimits = RiskLimits(),
        clock: Any = time.time,
    ) -> None:
        if not getattr(api, "config", None):
            raise ValueError("a configured LocalQmtApi is required")
        account_id = str(getattr(api.config, "account_id", "") or "").strip()
        account_name = str(getattr(api.config, "account_name", "") or "").strip()
        if not account_id or not account_name:
            raise ValueError("LocalQmtApi account identity is required")
        self.api = api
        self.account_id = account_id
        self.account_name = account_name
        self.account_limits = account_limits.validate()
        self.path = Path(database_path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(
            str(self.path),
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._clock = clock
        self._handlers_registered = False
        self._started = False
        self._trading_halted = True
        self._last_account_status: Dict[str, Any] = {}
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA foreign_keys=ON;

                CREATE TABLE IF NOT EXISTS coordinator_strategy (
                    strategy_id TEXT PRIMARY KEY,
                    auth_token_hash TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    max_order_notional REAL NOT NULL,
                    max_pending_notional REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS coordinator_command (
                    command_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    strategy_order_id TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    intent_hash TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    target_command_id INTEGER,
                    symbol TEXT NOT NULL DEFAULT '',
                    side TEXT NOT NULL DEFAULT '',
                    quantity REAL NOT NULL DEFAULT 0,
                    price REAL NOT NULL DEFAULT 0,
                    notional REAL NOT NULL DEFAULT 0,
                    stage TEXT NOT NULL,
                    gateway_msg_id TEXT NOT NULL DEFAULT '',
                    order_id TEXT NOT NULL DEFAULT '',
                    order_sysid TEXT NOT NULL DEFAULT '',
                    qmt_user_order_id TEXT NOT NULL DEFAULT '',
                    intent_json TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    terminal_at REAL,
                    UNIQUE(account_id, strategy_id, strategy_order_id, action_kind),
                    UNIQUE(account_id, client_order_id),
                    UNIQUE(account_id, request_id)
                );
                CREATE INDEX IF NOT EXISTS idx_coordinator_command_request
                    ON coordinator_command(account_id, request_id);
                CREATE INDEX IF NOT EXISTS idx_coordinator_command_qmt_keys
                    ON coordinator_command(account_id, order_id, order_sysid, qmt_user_order_id);

                CREATE TABLE IF NOT EXISTS coordinator_risk_reservation (
                    command_id INTEGER PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    notional REAL NOT NULL,
                    state TEXT NOT NULL,
                    release_reason TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(command_id) REFERENCES coordinator_command(command_id)
                );
                CREATE INDEX IF NOT EXISTS idx_coordinator_reservation_active
                    ON coordinator_risk_reservation(account_id, strategy_id, state);

                CREATE TABLE IF NOT EXISTS coordinator_event_inbox (
                    event_key TEXT PRIMARY KEY,
                    delivery_id TEXT NOT NULL DEFAULT '',
                    account_id TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    command_id INTEGER,
                    event_json TEXT NOT NULL,
                    received_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS coordinator_strategy_event_outbox (
                    coordinator_event_id TEXT PRIMARY KEY,
                    event_key TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    last_delivered_at REAL,
                    acknowledged_at REAL,
                    UNIQUE(event_key, strategy_id),
                    FOREIGN KEY(event_key) REFERENCES coordinator_event_inbox(event_key)
                );
                CREATE INDEX IF NOT EXISTS idx_coordinator_outbox_pending
                    ON coordinator_strategy_event_outbox(strategy_id, state, created_at);
                """
            )

    @staticmethod
    def _validate_strategy_id(strategy_id: str) -> str:
        value = str(strategy_id or "").strip()
        if _STRATEGY_ID.fullmatch(value) is None:
            raise ValueError("strategy_id must match ^[A-Za-z][A-Za-z0-9_-]{0,47}$")
        return value

    @staticmethod
    def _token_hash(token: str) -> str:
        value = str(token or "")
        if len(value) < 16:
            raise ValueError("strategy auth token must contain at least 16 characters")
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _finite_positive(name: str, value: Any, *, allow_zero: bool = False) -> float:
        try:
            normalized = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("%s must be a finite number" % name) from exc
        if not math.isfinite(normalized) or normalized < 0 or (not allow_zero and normalized == 0):
            raise ValueError("%s must be a finite %s number" % (name, "non-negative" if allow_zero else "positive"))
        return normalized

    @staticmethod
    def _sanitize_segment(value: str, fallback: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value or "").strip())
        normalized = normalized.strip("-_")
        return (normalized or fallback)[:24]

    def _now(self) -> float:
        return float(self._clock())

    def _begin(self) -> None:
        self._db.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._db.commit()

    def _rollback(self) -> None:
        self._db.rollback()

    def register_strategy(
        self,
        strategy_id: str,
        auth_token: str,
        *,
        enabled: bool = True,
        priority: int = 0,
        limits: RiskLimits = RiskLimits(),
    ) -> None:
        """Create or update one strategy's credential and risk envelope."""
        strategy = self._validate_strategy_id(strategy_id)
        token_hash = self._token_hash(auth_token)
        valid_limits = limits.validate()
        now = self._now()
        with self._lock:
            self._begin()
            try:
                self._db.execute(
                    """INSERT INTO coordinator_strategy
                    (strategy_id,auth_token_hash,enabled,priority,max_order_notional,
                     max_pending_notional,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(strategy_id) DO UPDATE SET
                    auth_token_hash=excluded.auth_token_hash,
                    enabled=excluded.enabled,
                    priority=excluded.priority,
                    max_order_notional=excluded.max_order_notional,
                    max_pending_notional=excluded.max_pending_notional,
                    updated_at=excluded.updated_at""",
                    (
                        strategy,
                        token_hash,
                        1 if enabled else 0,
                        int(priority),
                        float(valid_limits.max_order_notional),
                        float(valid_limits.max_pending_notional),
                        now,
                        now,
                    ),
                )
                self._commit()
            except Exception:
                self._rollback()
                raise

    def authenticate_strategy(self, strategy_id: str, auth_token: str) -> bool:
        """Check a strategy's coordinator credential without exposing account data."""
        strategy = self._validate_strategy_id(strategy_id)
        digest = hashlib.sha256(str(auth_token or "").encode("utf-8")).hexdigest()
        with self._lock:
            row = self._db.execute(
                "SELECT auth_token_hash,enabled FROM coordinator_strategy WHERE strategy_id=?",
                (strategy,),
            ).fetchone()
        return bool(
            row is not None
            and int(row["enabled"]) == 1
            and hmac.compare_digest(str(row["auth_token_hash"]), digest)
        )

    def _register_gateway_handlers(self) -> None:
        if self._handlers_registered:
            return
        for message_type in _GATEWAY_EVENT_TYPES:
            self.api.on(message_type, self._on_gateway_event)
        self._handlers_registered = True

    def start(self, timeout: Optional[float] = None) -> bool:
        """Connect the one API client, then require a fresh ready account status."""
        self._register_gateway_handlers()
        if not self.api.connect(timeout=timeout):
            self._trading_halted = True
            return False
        self._started = True
        return self.reconcile(timeout=timeout)

    def stop(self, timeout: float = 5.0) -> None:
        self._trading_halted = True
        self._started = False
        try:
            self.api.stop(timeout=timeout)
        finally:
            with self._lock:
                self._db.close()

    close = stop

    def reconcile(self, timeout: Optional[float] = None) -> bool:
        """Refresh only the Gateway health gate; unknown effects remain halted."""
        if not self.api.is_connected:
            self._trading_halted = True
            return False
        response = self.api.query("ACCOUNT_STATUS", timeout=timeout)
        account_status = (response or {}).get("account_status")
        healthy = bool(
            isinstance(response, dict)
            and response.get("success", True)
            and not response.get("cache_fallback", False)
            and isinstance(account_status, dict)
            and account_status.get("ready") is True
            and str(account_status.get("state") or "ready").lower() == "ready"
        )
        with self._lock:
            self._last_account_status = dict(account_status or {})
            unknown = self._db.execute(
                """SELECT COUNT(*) FROM coordinator_command
                WHERE account_id=? AND stage IN (%s)"""
                % ",".join("?" for _ in _UNKNOWN_STAGES),
                (self.account_id, *sorted(_UNKNOWN_STAGES)),
            ).fetchone()[0]
        self._trading_halted = not healthy or int(unknown) > 0
        return not self._trading_halted

    @property
    def trading_halted(self) -> bool:
        return self._trading_halted

    def account_status(self) -> Dict[str, Any]:
        with self._lock:
            active = self._db.execute(
                """SELECT COALESCE(SUM(notional),0) FROM coordinator_risk_reservation
                WHERE account_id=? AND state='ACTIVE'""",
                (self.account_id,),
            ).fetchone()[0]
        return {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "gateway_connected": bool(self.api.is_connected),
            "trading_halted": self._trading_halted,
            "pending_notional": float(active or 0.0),
            "account_status": dict(self._last_account_status),
        }

    def _assert_ready_to_trade(self) -> None:
        if not self._started or not self.api.is_connected or self._trading_halted:
            raise CoordinatorUnavailable("coordinator account is not ready for new trading effects")

    def _strategy_row_locked(self, strategy: str) -> sqlite3.Row:
        row = self._db.execute(
            "SELECT * FROM coordinator_strategy WHERE strategy_id=?", (strategy,)
        ).fetchone()
        if row is None or int(row["enabled"]) != 1:
            raise CoordinatorRiskRejected("strategy is not enabled")
        return row

    def _next_identifiers_locked(self, strategy: str, action_suffix: str = "") -> tuple[str, str, str]:
        sequence = int(
            self._db.execute(
                "SELECT COALESCE(MAX(command_id),0)+1 FROM coordinator_command"
            ).fetchone()[0]
        )
        day = time.strftime("%Y%m%d", time.localtime(self._now()))
        account = self._sanitize_segment(self.account_name, "account")
        client_order_id = "%s-%s-%s-%06d%s" % (
            account,
            strategy,
            day,
            sequence,
            action_suffix,
        )
        request_id = "%s-effect" % client_order_id
        trace_id = "coordinator-%s" % client_order_id
        return client_order_id, request_id, trace_id

    def _pending_notional_locked(self, strategy: Optional[str] = None) -> float:
        if strategy:
            row = self._db.execute(
                """SELECT COALESCE(SUM(notional),0) FROM coordinator_risk_reservation
                WHERE account_id=? AND strategy_id=? AND state='ACTIVE'""",
                (self.account_id, strategy),
            ).fetchone()
        else:
            row = self._db.execute(
                """SELECT COALESCE(SUM(notional),0) FROM coordinator_risk_reservation
                WHERE account_id=? AND state='ACTIVE'""",
                (self.account_id,),
            ).fetchone()
        return float(row[0] or 0.0)

    @staticmethod
    def _check_limit(name: str, limit: float, value: float) -> None:
        if float(limit) > 0 and value > float(limit):
            raise CoordinatorRiskRejected("%s would exceed configured limit" % name)

    @staticmethod
    def _intent_hash(intent: Dict[str, Any]) -> str:
        return hashlib.sha256(_json(intent).encode("utf-8")).hexdigest()

    def submit_order(
        self,
        strategy_id: str,
        strategy_order_id: str,
        symbol: str,
        side: str,
        quantity: Any,
        price: Any,
        *,
        price_type: int = 11,
        order_type: int = 0,
        order_remark: str = "",
        trace_id: str = "",
        spread: float = 0.0,
        business_order_type: str = "limit",
        credit_mode: str = "",
    ) -> Dict[str, Any]:
        """Persist an order intent and send exactly one async Gateway request.

        A duplicate strategy action with the same canonical intent returns its
        original record.  A send exception occurs after durable intent creation
        and is therefore marked ``UNKNOWN`` rather than retried automatically.
        """
        self._assert_ready_to_trade()
        strategy = self._validate_strategy_id(strategy_id)
        order_key = str(strategy_order_id or "").strip()
        if not order_key or len(order_key) > 128:
            raise ValueError("strategy_order_id must contain 1..128 characters")
        normalized_symbol = str(symbol or "").strip()
        normalized_side = str(side or "").strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol is required")
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        normalized_quantity = self._finite_positive("quantity", quantity)
        normalized_price = self._finite_positive("price", price, allow_zero=True)
        notional = normalized_quantity * normalized_price
        intent = {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "quantity": normalized_quantity,
            "price": normalized_price,
            "price_type": int(price_type),
            "order_type": int(order_type),
            "order_remark": str(order_remark or ""),
            "spread": float(spread),
            "business_order_type": str(business_order_type or "limit"),
            "credit_mode": str(credit_mode or ""),
        }
        fingerprint = self._intent_hash(intent)
        now = self._now()
        with self._lock:
            self._begin()
            try:
                existing = self._db.execute(
                    """SELECT * FROM coordinator_command
                    WHERE account_id=? AND strategy_id=? AND strategy_order_id=?
                    AND action_kind='NEW'""",
                    (self.account_id, strategy, order_key),
                ).fetchone()
                if existing is not None:
                    if str(existing["intent_hash"]) != fingerprint:
                        raise CoordinatorConflict(
                            "strategy_order_id is already bound to a different intent"
                        )
                    self._commit()
                    return _as_dict(existing) or {}

                strategy_row = self._strategy_row_locked(strategy)
                self._check_limit(
                    "strategy max_order_notional",
                    float(strategy_row["max_order_notional"]),
                    notional,
                )
                self._check_limit(
                    "account max_order_notional",
                    float(self.account_limits.max_order_notional),
                    notional,
                )
                self._check_limit(
                    "strategy max_pending_notional",
                    float(strategy_row["max_pending_notional"]),
                    self._pending_notional_locked(strategy) + notional,
                )
                self._check_limit(
                    "account max_pending_notional",
                    float(self.account_limits.max_pending_notional),
                    self._pending_notional_locked() + notional,
                )
                client_order_id, request_id, generated_trace = self._next_identifiers_locked(strategy)
                cursor = self._db.execute(
                    """INSERT INTO coordinator_command
                    (account_id,strategy_id,strategy_order_id,action_kind,intent_hash,
                     client_order_id,request_id,symbol,side,quantity,price,notional,
                     stage,intent_json,detail_json,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        self.account_id,
                        strategy,
                        order_key,
                        "NEW",
                        fingerprint,
                        client_order_id,
                        request_id,
                        normalized_symbol,
                        normalized_side,
                        normalized_quantity,
                        normalized_price,
                        notional,
                        "GATEWAY_SUBMITTING",
                        _json(intent),
                        _json({"trace_id": trace_id or generated_trace}),
                        now,
                        now,
                    ),
                )
                command_id = int(cursor.lastrowid)
                self._db.execute(
                    """INSERT INTO coordinator_risk_reservation
                    (command_id,account_id,strategy_id,notional,state,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?)""",
                    (command_id, self.account_id, strategy, notional, "ACTIVE", now, now),
                )
                row = self._db.execute(
                    "SELECT * FROM coordinator_command WHERE command_id=?", (command_id,)
                ).fetchone()
                self._commit()
            except Exception:
                self._rollback()
                raise

        try:
            sender = getattr(self.api, "place_order_async", None) or getattr(
                self.api, "send_order_async"
            )
            gateway_msg_id = sender(
                normalized_symbol,
                normalized_side,
                normalized_quantity,
                normalized_price,
                client_order_id=client_order_id,
                request_id=request_id,
                price_type=int(price_type),
                order_type=int(order_type),
                strategy_name=strategy,
                order_remark=str(order_remark or ""),
                trace_id=trace_id or generated_trace,
                spread=float(spread),
                business_order_type=str(business_order_type or "limit"),
                credit_mode=str(credit_mode or ""),
            )
        except Exception as exc:
            self._mark_unknown(request_id, "gateway_send_failed:%s" % type(exc).__name__)
            raise CoordinatorUnavailable(
                "Gateway send result is unknown; reconcile before retry"
            ) from exc

        with self._lock:
            self._db.execute(
                """UPDATE coordinator_command SET gateway_msg_id=?,updated_at=?
                WHERE account_id=? AND request_id=?
                AND stage IN ('GATEWAY_SUBMITTING','BRIDGE_QUEUED')""",
                (str(gateway_msg_id or ""), self._now(), self.account_id, request_id),
            )
            saved = self._db.execute(
                "SELECT * FROM coordinator_command WHERE account_id=? AND request_id=?",
                (self.account_id, request_id),
            ).fetchone()
        return _as_dict(saved) or {}

    def request_cancel(
        self,
        strategy_id: str,
        strategy_cancel_id: str,
        target_strategy_order_id: str,
    ) -> Dict[str, Any]:
        """Request one owned-order cancellation with a distinct stable effect ID."""
        self._assert_ready_to_trade()
        strategy = self._validate_strategy_id(strategy_id)
        cancel_key = str(strategy_cancel_id or "").strip()
        target_key = str(target_strategy_order_id or "").strip()
        if not cancel_key or not target_key:
            raise ValueError("strategy_cancel_id and target_strategy_order_id are required")
        now = self._now()
        with self._lock:
            self._begin()
            try:
                existing = self._db.execute(
                    """SELECT * FROM coordinator_command
                    WHERE account_id=? AND strategy_id=? AND strategy_order_id=?
                    AND action_kind='CANCEL'""",
                    (self.account_id, strategy, cancel_key),
                ).fetchone()
                if existing is not None:
                    stored_intent = json.loads(str(existing["intent_json"]))
                    if stored_intent.get("target_strategy_order_id") != target_key:
                        raise CoordinatorConflict(
                            "strategy_cancel_id is already bound to another target order"
                        )
                    self._commit()
                    return _as_dict(existing) or {}
                target = self._db.execute(
                    """SELECT * FROM coordinator_command
                    WHERE account_id=? AND strategy_id=? AND strategy_order_id=?
                    AND action_kind='NEW'""",
                    (self.account_id, strategy, target_key),
                ).fetchone()
                if target is None:
                    raise CoordinatorConflict("target order is not owned by this strategy")
                if str(target["stage"]).upper() in _TERMINAL_STAGES:
                    raise CoordinatorConflict("target order is already terminal")
                order_id = str(target["order_id"] or "").strip()
                if not order_id:
                    raise CoordinatorConflict("target QMT order_id is not known; reconcile first")
                client_order_id, request_id, _ = self._next_identifiers_locked(strategy, "-c")
                intent = {"target_strategy_order_id": target_key, "order_id": order_id}
                cursor = self._db.execute(
                    """INSERT INTO coordinator_command
                    (account_id,strategy_id,strategy_order_id,action_kind,intent_hash,
                     client_order_id,request_id,target_command_id,symbol,side,stage,
                     intent_json,detail_json,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        self.account_id,
                        strategy,
                        cancel_key,
                        "CANCEL",
                        self._intent_hash(intent),
                        client_order_id,
                        request_id,
                        int(target["command_id"]),
                        str(target["symbol"]),
                        str(target["side"]),
                        "GATEWAY_SUBMITTING",
                        _json(intent),
                        "{}",
                        now,
                        now,
                    ),
                )
                command_id = int(cursor.lastrowid)
                row = self._db.execute(
                    "SELECT * FROM coordinator_command WHERE command_id=?", (command_id,)
                ).fetchone()
                self._commit()
            except Exception:
                self._rollback()
                raise
        try:
            sender = getattr(self.api, "cancel_order_async", None) or getattr(
                self.api, "send_cancel_async"
            )
            gateway_msg_id = sender(order_id, request_id=request_id)
        except Exception as exc:
            self._mark_unknown(request_id, "gateway_cancel_failed:%s" % type(exc).__name__)
            raise CoordinatorUnavailable(
                "Gateway cancel result is unknown; reconcile before retry"
            ) from exc
        with self._lock:
            self._db.execute(
                """UPDATE coordinator_command SET gateway_msg_id=?,updated_at=?
                WHERE account_id=? AND request_id=? AND stage='GATEWAY_SUBMITTING'""",
                (str(gateway_msg_id or ""), self._now(), self.account_id, request_id),
            )
            saved = self._db.execute(
                "SELECT * FROM coordinator_command WHERE account_id=? AND request_id=?",
                (self.account_id, request_id),
            ).fetchone()
        return _as_dict(saved) or {}

    def _mark_unknown(self, request_id: str, reason: str) -> None:
        now = self._now()
        with self._lock:
            self._begin()
            try:
                row = self._db.execute(
                    "SELECT command_id,detail_json FROM coordinator_command WHERE account_id=? AND request_id=?",
                    (self.account_id, request_id),
                ).fetchone()
                if row is not None:
                    detail = json.loads(str(row["detail_json"] or "{}"))
                    detail["unknown_reason"] = reason
                    self._db.execute(
                        """UPDATE coordinator_command SET stage='UNKNOWN',detail_json=?,updated_at=?
                        WHERE command_id=? AND stage NOT IN ('FILLED','CANCELLED','CANCELED','REJECTED','FAILED')""",
                        (_json(detail), now, int(row["command_id"])),
                    )
                self._commit()
            except Exception:
                self._rollback()
                raise
        self._trading_halted = True

    @staticmethod
    def _event_key(message: Dict[str, Any]) -> str:
        delivery_id = str(message.get("delivery_id") or "").strip()
        if delivery_id:
            return "delivery:%s" % delivery_id
        return "best-effort:%s" % hashlib.sha256(_json(message).encode("utf-8")).hexdigest()

    def _find_command_locked(self, message: Dict[str, Any]) -> Optional[sqlite3.Row]:
        fields = (
            ("request_id", "request_id"),
            ("client_order_id", "client_order_id"),
            ("qmt_user_order_id", "qmt_user_order_id"),
            ("order_id", "order_id"),
            ("order_sysid", "order_sysid"),
        )
        for message_key, column in fields:
            value = str(message.get(message_key) or "").strip()
            if not value:
                continue
            row = self._db.execute(
                "SELECT * FROM coordinator_command WHERE account_id=? AND %s=? "
                "ORDER BY command_id DESC LIMIT 1" % column,
                (self.account_id, value),
            ).fetchone()
            if row is not None:
                return row
        return None

    @staticmethod
    def _stage_from_event(message: Dict[str, Any]) -> str:
        message_type = str(message.get("type") or "").upper()
        status = str(message.get("stage") or message.get("status") or message.get("order_status") or "").upper()
        if message_type in {"ASYNC_ORDER", "ASYNC_CANCEL"}:
            if status == "SENT":
                return "BRIDGE_QUEUED"
            if status == "REJECTED":
                return "REJECTED"
        if status in _UNKNOWN_STAGES:
            return "UNKNOWN"
        if status in _TERMINAL_STAGES:
            return "CANCELLED" if status == "CANCELED" else status
        if status in {"QMT_SUBMITTED", "ORDER_ACTIVE", "BRIDGE_QUEUED"}:
            return status
        if message_type == "ASYNC_ORDER_RESPONSE":
            return "QMT_SUBMITTED"
        if message_type == "ASYNC_CANCEL_RESPONSE":
            return "CANCEL_REQUESTED"
        return ""

    def _update_command_from_event_locked(
        self, command: sqlite3.Row, message: Dict[str, Any], now: float
    ) -> str:
        stage = self._stage_from_event(message)
        current = str(command["stage"] or "")
        if current in _TERMINAL_STAGES:
            stage = current
        if not stage:
            stage = current
        order_id = str(message.get("order_id") or command["order_id"] or "")
        order_sysid = str(message.get("order_sysid") or command["order_sysid"] or "")
        qmt_user_order_id = str(
            message.get("qmt_user_order_id") or command["qmt_user_order_id"] or ""
        )
        detail = json.loads(str(command["detail_json"] or "{}"))
        detail["last_event_type"] = str(message.get("type") or "")
        detail["last_event_at"] = now
        terminal_at = now if stage in _TERMINAL_STAGES else command["terminal_at"]
        self._db.execute(
            """UPDATE coordinator_command
            SET stage=?,order_id=?,order_sysid=?,qmt_user_order_id=?,detail_json=?,
                updated_at=?,terminal_at=? WHERE command_id=?""",
            (
                stage,
                order_id,
                order_sysid,
                qmt_user_order_id,
                _json(detail),
                now,
                terminal_at,
                int(command["command_id"]),
            ),
        )
        if stage in _TERMINAL_STAGES:
            self._db.execute(
                """UPDATE coordinator_risk_reservation
                SET state='RELEASED',release_reason=?,updated_at=?
                WHERE command_id=? AND state='ACTIVE'""",
                (stage, now, int(command["command_id"])),
            )
        if stage == "UNKNOWN":
            self._trading_halted = True
        return stage

    def _target_strategies_locked(
        self, message: Dict[str, Any], command: Optional[sqlite3.Row]
    ) -> Sequence[str]:
        if command is not None:
            return (str(command["strategy_id"]),)
        message_type = str(message.get("type") or "").upper()
        if message_type not in _BROADCAST_EVENT_TYPES:
            return ()
        rows = self._db.execute(
            "SELECT strategy_id FROM coordinator_strategy WHERE enabled=1 ORDER BY priority DESC,strategy_id"
        ).fetchall()
        return tuple(str(row["strategy_id"]) for row in rows)

    def _on_gateway_event(self, message: Dict[str, Any]) -> None:
        """Persist a Gateway event before the API handler returns and ACKs it."""
        if not isinstance(message, dict):
            raise TypeError("Gateway event must be an object")
        supplied_account = str(message.get("account_id") or "").strip()
        if supplied_account and supplied_account != self.account_id:
            raise CoordinatorError("Gateway event account identity mismatch")
        event_key = self._event_key(message)
        now = self._now()
        with self._lock:
            self._begin()
            try:
                duplicate = self._db.execute(
                    "SELECT 1 FROM coordinator_event_inbox WHERE event_key=?", (event_key,)
                ).fetchone()
                if duplicate is not None:
                    self._commit()
                    return
                command = self._find_command_locked(message)
                stage = ""
                if command is not None:
                    stage = self._update_command_from_event_locked(command, message, now)
                message_type = str(message.get("type") or "")
                if message_type == "RECONCILE_REQUIRED" or stage == "UNKNOWN":
                    self._trading_halted = True
                if message_type == "QMT_STATUS":
                    qmt_status = message.get("qmt_status") or message
                    if isinstance(qmt_status, dict) and qmt_status.get("ready") is False:
                        self._trading_halted = True
                self._db.execute(
                    """INSERT INTO coordinator_event_inbox
                    (event_key,delivery_id,account_id,message_type,command_id,event_json,received_at)
                    VALUES (?,?,?,?,?,?,?)""",
                    (
                        event_key,
                        str(message.get("delivery_id") or ""),
                        self.account_id,
                        message_type,
                        int(command["command_id"]) if command is not None else None,
                        _json(message),
                        now,
                    ),
                )
                for strategy in self._target_strategies_locked(message, command):
                    coordinator_event_id = "cev-" + hashlib.sha256(
                        (event_key + "|" + strategy).encode("utf-8")
                    ).hexdigest()[:24]
                    event = {
                        "coordinator_event_id": coordinator_event_id,
                        "strategy_id": strategy,
                        "source_event": message,
                    }
                    self._db.execute(
                        """INSERT OR IGNORE INTO coordinator_strategy_event_outbox
                        (coordinator_event_id,event_key,strategy_id,event_json,state,created_at)
                        VALUES (?,?,?,?,?,?)""",
                        (
                            coordinator_event_id,
                            event_key,
                            strategy,
                            _json(event),
                            "PENDING",
                            now,
                        ),
                    )
                self._commit()
            except Exception:
                self._rollback()
                raise

    def poll_events(self, strategy_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Return a strategy's durable events; callers must ACK each returned ID."""
        strategy = self._validate_strategy_id(strategy_id)
        safe_limit = max(1, min(int(limit), 1000))
        now = self._now()
        with self._lock:
            self._begin()
            try:
                self._strategy_row_locked(strategy)
                rows = self._db.execute(
                    """SELECT * FROM coordinator_strategy_event_outbox
                    WHERE strategy_id=? AND state IN ('PENDING','DELIVERED')
                    ORDER BY created_at,coordinator_event_id LIMIT ?""",
                    (strategy, safe_limit),
                ).fetchall()
                ids = [str(row["coordinator_event_id"]) for row in rows]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    self._db.execute(
                        """UPDATE coordinator_strategy_event_outbox
                        SET state='DELIVERED',attempts=attempts+1,last_delivered_at=?
                        WHERE coordinator_event_id IN (%s)""" % placeholders,
                        (now, *ids),
                    )
                self._commit()
            except Exception:
                self._rollback()
                raise
        result = []
        for row in rows:
            event = json.loads(str(row["event_json"]))
            event["attempt"] = int(row["attempts"]) + 1
            result.append(event)
        return result

    def acknowledge_event(self, strategy_id: str, coordinator_event_id: str) -> bool:
        """Mark one owned strategy outbox event as durably consumed."""
        strategy = self._validate_strategy_id(strategy_id)
        event_id = str(coordinator_event_id or "").strip()
        if not event_id:
            raise ValueError("coordinator_event_id is required")
        with self._lock:
            self._begin()
            try:
                self._strategy_row_locked(strategy)
                cursor = self._db.execute(
                    """UPDATE coordinator_strategy_event_outbox
                    SET state='ACKNOWLEDGED',acknowledged_at=?
                    WHERE coordinator_event_id=? AND strategy_id=?
                    AND state IN ('PENDING','DELIVERED')""",
                    (self._now(), event_id, strategy),
                )
                self._commit()
                return cursor.rowcount == 1
            except Exception:
                self._rollback()
                raise

    def get_command(
        self, strategy_id: str, strategy_order_id: str, *, action_kind: str = "NEW"
    ) -> Optional[Dict[str, Any]]:
        strategy = self._validate_strategy_id(strategy_id)
        action = str(action_kind or "NEW").upper()
        with self._lock:
            row = self._db.execute(
                """SELECT * FROM coordinator_command WHERE account_id=? AND strategy_id=?
                AND strategy_order_id=? AND action_kind=?""",
                (self.account_id, strategy, str(strategy_order_id or "").strip(), action),
            ).fetchone()
        return _as_dict(row)

    def pending_commands(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                """SELECT * FROM coordinator_command WHERE account_id=?
                AND stage NOT IN ('FILLED','CANCELLED','CANCELED','REJECTED','FAILED')
                ORDER BY command_id""",
                (self.account_id,),
            ).fetchall()
        return [_as_dict(row) or {} for row in rows]


__all__ = [
    "AccountCoordinator",
    "CoordinatorClient",
    "CoordinatorConflict",
    "CoordinatorError",
    "CoordinatorRiskRejected",
    "CoordinatorUnavailable",
    "RiskLimits",
]
