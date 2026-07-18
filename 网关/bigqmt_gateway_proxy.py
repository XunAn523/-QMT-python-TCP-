#!/usr/bin/env python3
"""Loopback-only Big QMT gateway using local file-queue IPC.

The external Windows strategy API uses the unchanged bridge protocol:
4-byte big-endian length header + UTF-8 JSON body.

This process owns the 127.0.0.1 TCP session. Big QMT embedded Python only
drains local JSON request files from handlebar(ContextInfo).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import math
import ntpath
import os
import signal
import socket
import struct
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from order_correlation import (
    IdempotencyConflict,
    OrderCorrelationStore,
    WriterLease,
    qmt_correlation_value,
)


DEFAULT_MAX_FRAME_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_DIR = ""
DEFAULT_RUNTIME_ROOT = ""
PROXY_BUILD_ID = "xuanling_local_qmt_gateway_20260717_low_latency_v2_auth_guard"
EXPECTED_LOCAL_HELPER_BUILD_ID = "xuanling_bigqmt_file_queue_helper_20260716_low_latency_v4_identity_guard"
ORDER_INTENT_DEDUPE_SECONDS = 3.0
ORDER_SIDE_INTENT_SECONDS = 6 * 60 * 60.0
ORDER_CORRELATION_CACHE_MAX_KEYS = 60000
ORDER_CORRELATION_MISS_TTL_SECONDS = 30.0
ASYNC_NORMALIZE_YIELD_EVERY = 512

STANDARD_BUY_OP_TYPES = {23, 27, 29, 33, 35, 40, 42, 50, 53, 56, 60, 80, 82}
STANDARD_SELL_OP_TYPES = {24, 28, 30, 31, 32, 34, 36, 41, 43, 44, 45, 51, 52, 54, 55, 61, 81, 83}
STANDARD_ORDER_TYPES = STANDARD_BUY_OP_TYPES | STANDARD_SELL_OP_TYPES
COMPAT_BUY_DIRECTION_CODES = {1}
COMPAT_SELL_DIRECTION_CODES = {2}


def json_default(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return ""


def reject_nonfinite_json(value: str) -> None:
    raise ValueError("non-finite JSON number is forbidden: %s" % value)


def stable_hash(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=json_default,
        allow_nan=False,
    )


def now() -> float:
    return time.time()


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        result = float(value)
        return result if math.isfinite(result) else default
    except Exception:
        return default


def normalize_windows_runtime_dir(value: Any) -> str:
    """Return the case-insensitive Windows path identity used by both bridge sides."""
    text = safe_str(value).strip().replace("/", "\\")
    if not text:
        return ""
    return ntpath.normcase(ntpath.normpath(text))


def fixed_decimal(value: Any, places: str = "0.001") -> str:
    try:
        return format(Decimal(str(value or 0)).quantize(Decimal(places)), "f")
    except (InvalidOperation, TypeError, ValueError):
        return format(Decimal("0").quantize(Decimal(places)), "f")


def normalize_side(value: Any) -> str:
    text = safe_str(value, "").strip().upper()
    if text in ("BUY", "B", "LONG", "\u4e70", "\u4e70\u5165") or "\u4e70" in text:
        return "BUY"
    if text in ("SELL", "S", "SHORT", "\u5356", "\u5356\u51fa") or "\u5356" in text:
        return "SELL"
    return ""


def normalized_side_from_candidates(*values: Any) -> tuple[str, str]:
    first_text = ""
    for value in values:
        text = safe_str(value, "").strip()
        if not text:
            continue
        if not first_text:
            first_text = text
        side = normalize_side(text)
        if side:
            return side, text
    return "", first_text


def side_from_standard_order_type(order_type: int) -> str:
    if order_type in STANDARD_BUY_OP_TYPES:
        return "BUY"
    if order_type in STANDARD_SELL_OP_TYPES:
        return "SELL"
    return ""


def normalize_standard_order_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return an order payload whose order_type follows the standard opType contract."""
    item = dict(payload or {})
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    current_order_type = safe_int(item.get("order_type"), 0)
    order_type_candidates = (
        item.get("raw_order_type"),
        item.get("order_type"),
        item.get("op_type"),
        item.get("m_nOpType"),
        item.get("m_nOrderType"),
        item.get("m_eOperationType"),
        raw.get("m_nOpType"),
        raw.get("op_type"),
        raw.get("order_type"),
        raw.get("m_nOrderType"),
        raw.get("m_eOperationType"),
    )
    has_explicit_raw_order_type = "raw_order_type" in item
    raw_order_type = safe_int(item.get("raw_order_type"), 0) if has_explicit_raw_order_type else 0
    standard_order_type = 0
    for candidate in order_type_candidates:
        value = safe_int(candidate, 0)
        if value and not raw_order_type and not has_explicit_raw_order_type:
            raw_order_type = value
        if value in STANDARD_ORDER_TYPES:
            standard_order_type = value
            break
    raw_direction = safe_int(item.get("raw_direction", item.get("direction")), 0)
    if not raw_direction and raw:
        raw_direction = safe_int(raw.get("m_nDirection"), 0) or safe_int(raw.get("direction"), 0)
    offset_flag = safe_int(item.get("offset_flag"), 0)
    if not offset_flag and raw:
        offset_flag = safe_int(raw.get("m_nOffsetFlag"), 0) or safe_int(raw.get("offset_flag"), 0)
    side, matched_side_text = normalized_side_from_candidates(
        item.get("side"),
        item.get("raw_side_text"),
        raw.get("side"),
        raw.get("raw_side_text"),
        raw.get("m_strOptName"),
        raw.get("m_strSide"),
        raw.get("m_strDirection"),
        raw.get("m_strOrderType"),
        raw.get("m_strOperationType"),
        raw.get("m_strBuySell"),
        raw.get("buy_sell"),
        raw.get("entrust_bs"),
        raw.get("business_name"),
        raw.get("order_type_name"),
        raw.get("operation_type_name"),
        raw.get("direction_name"),
    )
    raw_side_text = (
        safe_str(item.get("raw_side_text"), "").strip()
        or safe_str(raw.get("raw_side_text"), "").strip()
        or safe_str(raw.get("m_strOptName"), "").strip()
        or matched_side_text
    )
    declared_source = safe_str(item.get("order_type_source"), "")
    if declared_source == "unknown":
        declared_source = ""
    side_source = safe_str(item.get("side_source"), "")
    if side_source == "unknown":
        side_source = ""

    effective_order_type = 0
    source = ""
    standard_candidates = (
        (current_order_type, "order_type"),
        (standard_order_type, "raw_order_type"),
        (raw_direction, "raw_direction_op_type"),
    )
    if side:
        for candidate, candidate_source in standard_candidates:
            if candidate in STANDARD_ORDER_TYPES and side_from_standard_order_type(candidate) == side:
                effective_order_type = candidate
                source = declared_source or candidate_source
                break
        if not effective_order_type:
            effective_order_type = 23 if side == "BUY" else 24
            has_conflict = any(candidate in STANDARD_ORDER_TYPES for candidate, _ in standard_candidates)
            source = "side_conflict_default" if has_conflict else "side_default"
        side_source = side_source or "explicit_side"
    else:
        for candidate, candidate_source in standard_candidates:
            if candidate in STANDARD_ORDER_TYPES:
                effective_order_type = candidate
                source = declared_source or candidate_source
                break

    if not effective_order_type and offset_flag == 48:
        effective_order_type = 23
        side = "BUY"
        source = "offset_flag_side_default"
        side_source = side_source or "offset_flag"
    elif not effective_order_type and offset_flag == 49:
        effective_order_type = 24
        side = "SELL"
        source = "offset_flag_side_default"
        side_source = side_source or "offset_flag"
    elif not effective_order_type and raw_direction in COMPAT_BUY_DIRECTION_CODES:
        effective_order_type = 23
        source = "raw_direction_compat"
        side = "BUY"
        side_source = side_source or "raw_direction_compat"
    elif not effective_order_type and raw_direction in COMPAT_SELL_DIRECTION_CODES:
        effective_order_type = 24
        source = "raw_direction_compat"
        side = "SELL"
        side_source = side_source or "raw_direction_compat"
    if not effective_order_type:
        source = "unknown"

    normalized_side = side or side_from_standard_order_type(effective_order_type)
    item["raw_order_type"] = raw_order_type
    item["raw_direction"] = raw_direction
    item["raw_side_text"] = raw_side_text
    item["offset_flag"] = offset_flag
    item["order_type"] = effective_order_type
    item["order_type_source"] = source
    item["order_type_valid"] = bool(effective_order_type)
    item["side_source"] = side_source or ("order_type" if normalized_side else "unknown")
    if normalized_side:
        item["side"] = normalized_side
    return item


def normalize_standard_orders(items: Any, runtime: Any = None) -> List[Dict[str, Any]]:
    result = []
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        payload = runtime.apply_order_side_intent(item) if runtime else item
        result.append(normalize_standard_order_payload(payload))
    return result


async def normalize_standard_orders_async(
    items: Any, runtime: Any = None,
) -> List[Dict[str, Any]]:
    source = [item for item in (items or []) if isinstance(item, dict)]
    payloads = await runtime.apply_order_side_intents_async(source) if runtime else source
    result = []
    for index, payload in enumerate(payloads):
        result.append(normalize_standard_order_payload(payload))
        if index and index % ASYNC_NORMALIZE_YIELD_EVERY == 0:
            await asyncio.sleep(0)
    return result


def _order_match_keys(item: Dict[str, Any]) -> List[str]:
    keys = []
    for prefix, value in (
        ("order", item.get("order_id")),
        ("sys", item.get("order_sysid")),
        ("qmt", item.get("qmt_user_order_id")),
    ):
        text = safe_str(value)
        if text and text != "0":
            keys.append("%s:%s" % (prefix, text))
    return keys


def _correlation_match_keys(item: Dict[str, Any]) -> List[str]:
    keys = _order_match_keys(item)
    qmt_value = qmt_correlation_value(item or {})
    qmt_key = "qmt:%s" % qmt_value if qmt_value and qmt_value != "0" else ""
    if qmt_key and qmt_key not in keys:
        keys.append(qmt_key)
    return keys


def build_order_side_lookup(orders: Any) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for order in (orders or []):
        if not isinstance(order, dict):
            continue
        side = normalize_side(order.get("side") or order.get("raw_side_text"))
        order_type = safe_int(order.get("order_type"), 0)
        if not side:
            side = side_from_standard_order_type(order_type)
        if not order_type and side == "BUY":
            order_type = 23
        elif not order_type and side == "SELL":
            order_type = 24
        if not side and not order_type:
            continue
        matched = {
            "side": side,
            "order_type": order_type,
            "order_type_source": safe_str(order.get("order_type_source")) or "matched_order",
        }
        for key in _order_match_keys(order):
            lookup[key] = matched
    return lookup


def apply_order_side_lookup(item: Dict[str, Any], lookup: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    payload = dict(item or {})
    if not lookup:
        return payload
    matched = None
    for key in _order_match_keys(payload):
        matched = lookup.get(key)
        if matched:
            break
    if not matched:
        return payload

    original_side_text = safe_str(payload.get("raw_side_text") or payload.get("side"))
    side = normalize_side(matched.get("side"))
    if side:
        payload["side"] = side
        payload["side_source"] = "matched_order"
        payload.setdefault("raw_side_text", original_side_text or side)

    matched_order_type = safe_int(matched.get("order_type"), 0)
    current_order_type = safe_int(payload.get("order_type"), 0)
    if matched_order_type in STANDARD_ORDER_TYPES:
        if current_order_type and current_order_type != matched_order_type:
            payload.setdefault("raw_order_type", current_order_type)
        payload["order_type"] = matched_order_type
        payload["order_type_source"] = "matched_order"
        payload["order_type_valid"] = True
    return payload


def normalize_standard_trades(items: Any, orders: Any = None, runtime: Any = None) -> List[Dict[str, Any]]:
    lookup = build_order_side_lookup(orders)
    result = []
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        payload = runtime.apply_order_side_intent(item) if runtime else item
        payload = apply_order_side_lookup(payload, lookup)
        result.append(normalize_standard_order_payload(payload))
    return result


async def normalize_standard_trades_async(
    items: Any, orders: Any = None, runtime: Any = None,
) -> List[Dict[str, Any]]:
    lookup = build_order_side_lookup(orders)
    source = [item for item in (items or []) if isinstance(item, dict)]
    payloads = await runtime.apply_order_side_intents_async(source) if runtime else source
    result = []
    for index, payload in enumerate(payloads):
        result.append(normalize_standard_order_payload(apply_order_side_lookup(payload, lookup)))
        if index and index % ASYNC_NORMALIZE_YIELD_EVERY == 0:
            await asyncio.sleep(0)
    return result


def make_request_id(prefix: str = "proxy") -> str:
    return "%s-%d-%s" % (prefix, int(time.time() * 1000), uuid.uuid4().hex[:8])


def safe_filename(value: Any) -> str:
    text = safe_str(value, "")
    chars = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_", "."):
            chars.append(ch)
        else:
            chars.append("_")
    name = "".join(chars).strip("._")
    return name or make_request_id("file")


def bounded_json_files(folder: Path, limit: int) -> List[Path]:
    result: List[Path] = []
    if limit <= 0:
        return result
    try:
        entries = os.scandir(str(folder))
    except OSError:
        return result
    try:
        for entry in entries:
            if entry.name.endswith(".json") and entry.is_file():
                result.append(Path(entry.path))
                if len(result) >= limit:
                    break
    finally:
        entries.close()
    result.sort(key=lambda path: path.name)
    return result


def atomic_write_json(path: Path, payload: Dict[str, Any], ensure_parent: bool = True) -> None:
    if ensure_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("%s.%s.%s.tmp" % (path.name, os.getpid(), int(now() * 1000000)))
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=True, default=json_default)
        f.write("\n")
    try:
        os.replace(str(tmp), str(path))
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def read_json_file(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else (default or {})
    except FileNotFoundError:
        return default or {}


def probe_tcp_port(host: str, port: int, timeout: float = 0.2) -> tuple[bool, str, float]:
    started = time.perf_counter()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(max(0.05, timeout))
    try:
        sock.connect((host, port))
        return True, "", (time.perf_counter() - started) * 1000
    except OSError as exc:
        return False, safe_str(exc), (time.perf_counter() - started) * 1000
    finally:
        try:
            sock.close()
        except Exception:
            pass


async def run_blocking(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args))


@dataclass
class AccountConfig:
    name: str
    account_id: str
    account_type: str
    tcp_host: str
    tcp_port: int
    runtime_dir: str
    poll_interval_seconds: float = 1.0
    request_timeout_seconds: float = 8.0
    query_timeout_seconds: float = 6.0
    trade_enqueue_timeout_seconds: float = 1.0
    heartbeat_stale_seconds: float = 15.0
    response_watch_interval_seconds: float = 0.01
    event_watch_interval_seconds: float = 0.01
    maintenance_interval_seconds: float = 60.0
    query_concurrency: int = 1
    expected_helper_build_id: str = ""

    expected_protocol_version: int = 0
    expected_command_interval_ms: int = 0


@dataclass
class GatewayConfig:
    auth_token_sha256: str = ""
    listen_backlog: int = 16
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
    tcp_idle_timeout_seconds: float = 60.0
    accounts: List[AccountConfig] = field(default_factory=list)


HELPER_IDENTITY_FIELDS = (
    "account_id",
    "account_type",
    "name",
    "runtime_dir",
    "build_id",
    "protocol_version",
    "command_interval_ms",
)


def normalize_helper_identity_value(field_name: str, value: Any) -> Any:
    if field_name in ("protocol_version", "command_interval_ms"):
        return safe_int(value, 0)
    if field_name == "account_type":
        return safe_str(value).strip().upper()
    if field_name == "runtime_dir":
        return normalize_windows_runtime_dir(value)
    return safe_str(value).strip()


def collect_helper_identity(
    heartbeat: Dict[str, Any],
    state: Dict[str, Any],
    readiness: Dict[str, Any],
) -> Dict[str, Any]:
    """Collect only helper-reported identity and reject missing/conflicting files."""
    documents = {
        "heartbeat": heartbeat,
        "state": state,
        "readiness": readiness,
    }
    reported: Dict[str, Dict[str, Any]] = {}
    normalized: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    conflicts: List[str] = []

    for document_name, document in documents.items():
        reported[document_name] = {}
        normalized[document_name] = {}
        for field_name in HELPER_IDENTITY_FIELDS:
            raw_value = document.get(field_name)
            value = normalize_helper_identity_value(field_name, raw_value)
            reported[document_name][field_name] = raw_value
            normalized[document_name][field_name] = value
            if value in ("", 0, None):
                missing.append("%s.%s" % (document_name, field_name))

    identity: Dict[str, Any] = {}
    for field_name in HELPER_IDENTITY_FIELDS:
        values = [
            normalized[document_name][field_name]
            for document_name in ("heartbeat", "state", "readiness")
            if normalized[document_name][field_name] not in ("", 0, None)
        ]
        identity[field_name] = values[0] if values else (0 if field_name in ("protocol_version", "command_interval_ms") else "")
        distinct = []
        for value in values:
            if value not in distinct:
                distinct.append(value)
        if len(distinct) > 1:
            detail = ",".join(
                "%s=%s" % (document_name, normalized[document_name][field_name])
                for document_name in ("heartbeat", "state", "readiness")
            )
            conflicts.append("%s(%s)" % (field_name, detail))

    identity["reported_identity"] = reported
    identity["normalized_identity"] = normalized
    identity["identity_missing"] = missing
    identity["identity_conflicts"] = conflicts
    identity["identity_consistent"] = not missing and not conflicts
    return identity


def helper_identity_mismatches(cfg: AccountConfig, health: Dict[str, Any]) -> List[str]:
    failures = list(health.get("identity_missing") or [])
    failures.extend(health.get("identity_conflicts") or [])
    if health.get("identity_consistent") is not True and not failures:
        failures.append("helper identity is not consistently reported by all health files")
    comparisons = (
        ("account_id", safe_str(cfg.account_id).strip(), safe_str(health.get("account_id")).strip()),
        ("account_type", safe_str(cfg.account_type).strip().upper(), safe_str(health.get("account_type")).strip().upper()),
        ("name", safe_str(cfg.name).strip(), safe_str(health.get("name")).strip()),
        ("runtime_dir", normalize_windows_runtime_dir(cfg.runtime_dir), normalize_windows_runtime_dir(health.get("runtime_dir"))),
        ("build_id", safe_str(cfg.expected_helper_build_id).strip(), safe_str(health.get("build_id")).strip()),
        ("protocol_version", safe_int(cfg.expected_protocol_version, 0), safe_int(health.get("protocol_version"), 0)),
        ("command_interval_ms", safe_int(cfg.expected_command_interval_ms, 0), safe_int(health.get("command_interval_ms"), 0)),
    )
    for field_name, expected, actual in comparisons:
        if expected in ("", 0, None):
            failures.append("cfg.%s is empty" % field_name)
        elif actual in ("", 0, None):
            failures.append("helper.%s is empty" % field_name)
        elif actual != expected:
            failures.append("%s expected=%s actual=%s" % (field_name, expected, actual))
    return failures


class HelperError(RuntimeError):
    def __init__(self, message: str, code: str = "HELPER_ERROR") -> None:
        super().__init__(message)
        self.code = code


class HelperUnavailable(HelperError):
    pass


class HelperTimeout(HelperError):
    pass


class OutboundFrameTooLarge(ValueError):
    def __init__(self, actual_bytes: int, max_bytes: int) -> None:
        super().__init__(
            "outbound frame exceeds limit: actual=%s max=%s" % (actual_bytes, max_bytes)
        )
        self.actual_bytes = actual_bytes
        self.max_bytes = max_bytes
        self.code = "FRAME_TOO_LARGE"


class FileQueueHelperClient:
    def __init__(self, cfg: AccountConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.root = Path(cfg.runtime_dir)
        self.inbox = self.root / "inbox"
        self.commands = self.inbox / "commands"
        self.queries = self.inbox / "queries"
        self.processing = self.root / "processing"
        self.processing_commands = self.processing / "commands"
        self.processing_queries = self.processing / "queries"
        self.archive = self.root / "archive"
        self.done = self.archive / "done"
        self.responses = self.root / "responses"
        self.request_state = self.root / "request_state"
        self.events = self.root / "events"
        self.events_live = self.events / "live"
        self.events_processing = self.events / "processing"
        self.events_failed = self.events / "failed"
        self.snapshots = self.root / "snapshots"
        self.heartbeat_file = self.root / "heartbeat.json"
        self.state_file = self.root / "state.json"
        self.readiness_file = self.root / "readiness.json"
        self.logger = logger
        self.last_error = ""
        self.last_success_at: Optional[float] = None
        self.last_failure_at: Optional[float] = None
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        for path in (
            self.root,
            self.inbox,
            self.commands,
            self.queries,
            self.processing,
            self.processing_commands,
            self.processing_queries,
            self.archive,
            self.done,
            self.responses,
            self.request_state,
            self.events,
            self.events_live,
            self.events_processing,
            self.events_failed,
            self.snapshots,
        ):
            path.mkdir(parents=True, exist_ok=True)

    async def health(self) -> Dict[str, Any]:
        return await run_blocking(self.health_sync)

    def health_sync(self) -> Dict[str, Any]:
        heartbeat = read_json_file(self.heartbeat_file)
        state = read_json_file(self.state_file)
        readiness = read_json_file(self.readiness_file)
        identity = collect_helper_identity(heartbeat, state, readiness)
        last_handlebar_at = safe_float(
            heartbeat.get("last_handlebar_at") or heartbeat.get("timestamp") or heartbeat.get("updated_at"),
            0.0,
        )
        age = now() - last_handlebar_at if last_handlebar_at else 10**9
        alive = bool(last_handlebar_at and age <= self.cfg.heartbeat_stale_seconds)
        account_ready = bool(heartbeat.get("account_ready") or state.get("account_ready"))
        context_ready = bool(heartbeat.get("context_ready") or state.get("context_ready"))
        running_state = safe_str(heartbeat.get("state") or state.get("state") or "offline")
        run_time_ready = bool(heartbeat.get("run_time_ready") or state.get("run_time_ready"))
        last_command_cycle_at = safe_float(
            readiness.get("last_command_cycle_at")
            or heartbeat.get("last_command_cycle_at")
            or state.get("last_command_cycle_at"),
            0.0,
        )
        reported_command_age_ms = safe_float(
            heartbeat.get("last_command_cycle_age_ms", state.get("last_command_cycle_age_ms")),
            -1.0,
        )
        if readiness:
            command_age_ms = (now() - last_command_cycle_at) * 1000 if last_command_cycle_at else 10**9
            readiness_updated_at = safe_float(readiness.get("updated_at"), 0.0)
            readiness_age_ms = (now() - readiness_updated_at) * 1000 if readiness_updated_at else 10**9
            command_timer_ready = command_age_ms <= 250.0 and readiness_age_ms <= 250.0
        else:
            command_age_ms = (
                reported_command_age_ms
                if reported_command_age_ms >= 0
                else ((now() - last_command_cycle_at) * 1000 if last_command_cycle_at else 10**9)
            )
            readiness_age_ms = 10**9
            command_timer_ready = (
                command_age_ms <= 250.0
                and self.cfg.expected_command_interval_ms <= 0
            )
        ready = (
            alive
            and account_ready
            and context_ready
            and run_time_ready
            and command_timer_ready
            and running_state == "running"
            and identity["identity_consistent"]
        )
        data = {
            "ready": ready,
            "alive": alive,
            "state": "ready" if ready else ("degraded" if alive else "offline"),
            "helper_state": running_state,
            "account_id": identity["account_id"],
            "account_type": identity["account_type"],
            "name": identity["name"],
            "runtime_dir": identity["runtime_dir"],
            "last_handlebar_at": last_handlebar_at,
            "heartbeat_age_seconds": age,
            "account_ready": account_ready,
            "context_ready": context_ready,
            "run_time_ready": run_time_ready,
            "last_command_cycle_age_ms": command_age_ms,
            "last_command_cycle_at": last_command_cycle_at,
            "readiness_age_ms": readiness_age_ms,
            "last_error": safe_str(heartbeat.get("last_error") or state.get("last_error") or ""),
            "build_id": identity["build_id"],
            "protocol_version": identity["protocol_version"],
            "command_interval_ms": identity["command_interval_ms"],
            "reported_identity": identity["reported_identity"],
            "normalized_identity": identity["normalized_identity"],
            "identity_missing": identity["identity_missing"],
            "identity_conflicts": identity["identity_conflicts"],
            "identity_consistent": identity["identity_consistent"],
        }
        if ready:
            self._record_success()
        else:
            self._record_failure(data["last_error"] or data["state"])
        return data

    def _response_path(self, request_id: str) -> Path:
        return self.responses / (safe_filename(request_id) + ".json")

    def _decode_existing_response_for_enqueue(self, request_id: str, response_path: Path) -> Dict[str, Any]:
        try:
            response = read_json_file(response_path)
        except Exception as exc:
            return {
                "status": "failed",
                "queued": False,
                "request_id": request_id,
                "idempotent": True,
                "dedupe_layer": "helper_response",
                "error": "invalid helper response: %s" % exc,
            }
        data = response.get("data")
        result = dict(data) if isinstance(data, dict) else {}
        result.setdefault("status", "done" if response.get("ok") is not False else "failed")
        result["queued"] = False
        result["request_id"] = request_id
        result["idempotent"] = True
        result["dedupe_layer"] = "helper_response"
        if response.get("ok") is False and not result.get("error"):
            result["error"] = safe_str(response.get("error") or "helper request failed")
        self._record_success()
        return result

    def _find_existing_request_file(self, request_id: str) -> tuple[Optional[Path], str]:
        name = safe_filename(request_id) + ".json"
        for stage, folder in (
            ("commands", self.commands),
            ("queries", self.queries),
            ("processing_commands", self.processing_commands),
            ("processing_queries", self.processing_queries),
        ):
            path = folder / name
            if path.exists():
                return path, stage
        guard = self.request_state / name
        if guard.exists():
            return guard, "request_state"
        return None, ""

    async def request(
        self,
        action: str,
        payload: Dict[str, Any],
        timeout: float,
        timeout_as_queued: bool = False,
    ) -> Dict[str, Any]:
        return await run_blocking(self.request_sync, action, payload or {}, timeout, timeout_as_queued)

    def enqueue_action(self, action: str, payload: Dict[str, Any], timeout: float = 0.0) -> Dict[str, Any]:
        request_id = safe_str(payload.get("request_id") or payload.get("msg_id") or payload.get("client_order_id") or make_request_id(action))
        payload = dict(payload)
        payload["request_id"] = request_id
        response_path = self._response_path(request_id)
        if response_path.exists():
            return self._decode_existing_response_for_enqueue(request_id, response_path)
        existing_path, existing_stage = self._find_existing_request_file(request_id)
        if existing_path:
            return {
                "status": "queued",
                "queued": True,
                "request_id": request_id,
                "request_path": str(existing_path),
                "idempotent": True,
                "dedupe_layer": "helper_queue",
                "duplicate_stage": existing_stage,
            }
        request = self._build_request(action, payload, request_id, timeout)
        path = self.commands / (safe_filename(request_id) + ".json")
        atomic_write_json(path, request, ensure_parent=False)
        return {
            "status": "queued",
            "queued": True,
            "request_id": request_id,
            "request_path": str(path),
            "idempotent": False,
        }

    def request_sync(
        self,
        action: str,
        payload: Dict[str, Any],
        timeout: float,
        timeout_as_queued: bool = False,
    ) -> Dict[str, Any]:
        self.ensure_dirs()
        request_id = safe_str(payload.get("request_id") or payload.get("msg_id") or payload.get("client_order_id") or make_request_id(action))
        payload = dict(payload)
        payload["request_id"] = request_id
        response_path = self._response_path(request_id)
        if response_path.exists():
            return self._decode_response(response_path)
        request = self._build_request(action, payload, request_id, timeout)
        existing_path, _ = self._find_existing_request_file(request_id)
        if not existing_path:
            queue_dir = self.commands if action in ("place_order", "cancel_order") else self.queries
            request_path = queue_dir / (safe_filename(request_id) + ".json")
            atomic_write_json(request_path, request)
        deadline = time.monotonic() + max(0.01, timeout)
        while time.monotonic() < deadline:
            if response_path.exists():
                return self._decode_response(response_path)
            time.sleep(0.02)
        if timeout_as_queued:
            return {
                "status": "queued",
                "queued": True,
                "request_id": request_id,
                "timeout": True,
            }
        message = "helper response timeout action=%s request_id=%s" % (action, request_id)
        self._record_failure(message)
        raise HelperTimeout(message, "HELPER_TIMEOUT")

    def _build_request(self, action: str, payload: Dict[str, Any], request_id: str, timeout: float = 0.0) -> Dict[str, Any]:
        created = now()
        return {
            "version": 1,
            "request_id": request_id,
            "msg_id": safe_str(payload.get("msg_id") or request_id),
            "account_id": self.cfg.account_id,
            "account_type": self.cfg.account_type,
            "action": action,
            "payload": payload,
            "created_at": created,
            "deadline_at": created + timeout if timeout else 0,
            "source": "bigqmt_gateway_proxy",
            "sync": bool(timeout),
        }

    def _decode_response(self, response_path: Path) -> Dict[str, Any]:
        try:
            response = read_json_file(response_path)
        except Exception as exc:
            message = "invalid helper response %s: %s" % (response_path, exc)
            self._record_failure(message)
            raise HelperError(message, "INVALID_RESPONSE") from exc
        if response.get("ok") is False:
            message = safe_str(response.get("error") or response.get("message") or "helper request failed")
            code = safe_str(response.get("code") or "HELPER_ERROR")
            self._record_failure(message)
            try:
                response_path.unlink()
            except OSError:
                pass
            raise HelperError(message, code)
        self._record_success()
        data = response.get("data")
        result = data if isinstance(data, dict) else {"value": data}
        try:
            response_path.unlink()
        except OSError:
            pass
        return result

    async def consume_response(self, request_id: str) -> Dict[str, Any]:
        return await run_blocking(self.consume_response_sync, request_id)

    def consume_response_sync(self, request_id: str) -> Dict[str, Any]:
        path = self._response_path(request_id)
        if not path.exists():
            return {}
        return read_json_file(path)

    async def ack_response(self, request_id: str) -> None:
        await run_blocking(self.ack_response_sync, request_id)

    def ack_response_sync(self, request_id: str) -> None:
        path = self._response_path(request_id)
        try:
            path.unlink()
        except OSError:
            pass

    async def snapshot(self) -> Dict[str, Any]:
        return await self.request("snapshot", {}, self.cfg.query_timeout_seconds)

    async def account(self) -> Dict[str, Any]:
        return await self.request("account", {}, self.cfg.query_timeout_seconds)

    async def positions(self, **filters: Any) -> Dict[str, Any]:
        return await self.request("positions", filters, self.cfg.query_timeout_seconds)

    async def orders(self, **filters: Any) -> Dict[str, Any]:
        return await self.request("orders", filters, self.cfg.query_timeout_seconds)

    async def trades(self, **filters: Any) -> Dict[str, Any]:
        return await self.request("trades", filters, self.cfg.query_timeout_seconds)

    async def order_status(self, order_id: str) -> Dict[str, Any]:
        return await self.request("order_status", {"order_id": order_id}, self.cfg.query_timeout_seconds)

    async def place_order(self, payload: Dict[str, Any], wait: bool) -> Dict[str, Any]:
        if not wait:
            return await run_blocking(
                self.enqueue_action,
                "place_order",
                payload,
                self.cfg.trade_enqueue_timeout_seconds,
            )
        return await self.request("place_order", payload, self.cfg.request_timeout_seconds, timeout_as_queued=True)

    async def cancel_order(self, payload: Dict[str, Any], wait: bool) -> Dict[str, Any]:
        if not wait:
            return await run_blocking(
                self.enqueue_action,
                "cancel_order",
                payload,
                self.cfg.trade_enqueue_timeout_seconds,
            )
        return await self.request("cancel_order", payload, self.cfg.request_timeout_seconds, timeout_as_queued=True)

    async def generic_action(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request(action, payload, self.cfg.request_timeout_seconds)

    async def latest_snapshot(self) -> Dict[str, Any]:
        return await run_blocking(lambda: read_json_file(self.snapshots / "latest.json"))

    async def read_events(self, max_batch: int = 100) -> List[Dict[str, Any]]:
        return await run_blocking(self.read_events_sync, max_batch)

    def read_events_sync(self, max_batch: int = 100) -> List[Dict[str, Any]]:
        processing = bounded_json_files(self.events_processing, max_batch)
        live = bounded_json_files(self.events_live, max(0, max_batch - len(processing)))
        files = sorted(processing + live, key=lambda path: path.name)
        result: List[Dict[str, Any]] = []
        for path in files:

            claimed = path
            try:
                claimed = path if path.parent == self.events_processing else self.events_processing / path.name
                if claimed != path:
                    os.replace(str(path), str(claimed))
                event = read_json_file(claimed)
                if event:
                    event["_gateway_event_path"] = str(claimed)
                    result.append(event)
            except Exception as exc:
                self.logger.warning("event_read_failed path=%s error=%s", path, exc)
                try:
                    source = claimed if claimed.exists() else path
                    target = self.events_failed / source.name
                    os.replace(str(source), str(target))
                except Exception:
                    pass
        return result

    async def ack_event(self, event: Dict[str, Any]) -> None:
        await run_blocking(self.ack_event_sync, event)

    def ack_event_sync(self, event: Dict[str, Any]) -> None:
        path = Path(safe_str(event.get("_gateway_event_path")))
        if path.is_file():
            path.unlink()

    async def fail_event(self, event: Dict[str, Any]) -> None:
        await run_blocking(self.fail_event_sync, event)

    def fail_event_sync(self, event: Dict[str, Any]) -> None:
        path = Path(safe_str(event.get("_gateway_event_path")))
        if not path.is_file():
            return
        target = self.events_failed / path.name
        if target.exists():
            target = self.events_failed / ("%d-%s" % (int(now() * 1000), path.name))
        os.replace(str(path), str(target))

    async def retry_event(self, event: Dict[str, Any]) -> bool:
        return await run_blocking(self.retry_event_sync, event)

    def retry_event_sync(self, event: Dict[str, Any]) -> bool:
        path = Path(safe_str(event.get("_gateway_event_path")))
        if not path.is_file():
            return False
        retry_count = safe_int(event.get("_gateway_retry_count"), 0) + 1
        if retry_count >= 3:
            self.fail_event_sync(event)
            return True
        target = self.events_live / path.name
        if target.exists():
            target = self.events_live / ("%d-%s" % (int(now() * 1000), path.name))
        payload = {
            key: value for key, value in event.items()
            if key != "_gateway_event_path"
        }
        payload["_gateway_retry_count"] = retry_count
        atomic_write_json(target, payload, ensure_parent=False)
        path.unlink()
        return False

    def _record_success(self) -> None:
        self.last_error = ""
        self.last_success_at = now()

    def _record_failure(self, message: str) -> None:
        self.last_error = safe_str(message)
        self.last_failure_at = now()


class AccountRuntime:
    def __init__(self, cfg: AccountConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.helper = FileQueueHelperClient(cfg, logger)
        runtime_root = Path(cfg.runtime_dir)
        self.correlation = OrderCorrelationStore(runtime_root / "gateway_state.sqlite3")
        self.writer_lease = WriterLease(runtime_root / "active_writer.lock")
        self.server: Optional[asyncio.AbstractServer] = None
        self.clients: Set[TcpClientSession] = set()
        self.clients_lock = asyncio.Lock()
        self.primary: Optional[TcpClientSession] = None
        self.done: Dict[str, Dict[str, Any]] = {}
        self.done_order: List[str] = []
        self.order_intent_cache: Dict[str, Dict[str, Any]] = {}
        self.order_side_intents: Dict[str, Dict[str, Any]] = {}
        self.persisted_order_correlations: Dict[str, Dict[str, Any]] = {}
        self.persisted_order_correlation_misses: Dict[str, float] = {}
        self.correlation_lookup_lock = asyncio.Lock()
        for row in reversed(self.correlation.recent(cfg.account_id)):
            self.remember_order_correlation(row)
        pending_rows = self.correlation.pending(cfg.account_id)
        for row in pending_rows:
            self.remember_order_correlation(row)
        self.pending_responses: Dict[str, Dict[str, Any]] = {}
        for row in pending_rows:
            request_id = safe_str(row.get("request_id"))
            if request_id:
                self.pending_responses[request_id] = {
                    "payload": dict(row),
                    "queued_at": safe_float(row.get("created_at"), now()),
                    "deadline_at": now() + cfg.request_timeout_seconds,
                    "recovered": True,
                }
        self.pending_lock = asyncio.Lock()
        self.delivery_waiters: Dict[str, asyncio.Event] = {}
        self.delivery_lock = asyncio.Lock()
        self.query_semaphore = asyncio.Semaphore(max(1, cfg.query_concurrency))
        self.query_singleflight: Dict[str, asyncio.Task] = {}
        self.query_lock = asyncio.Lock()
        self.last_asset_hash = ""
        self.last_positions_hash = ""
        self.last_orders_hash = ""
        self.snapshot_baseline_ready = False
        self.seen_trade_keys: Set[str] = set()
        self.seen_event_ids: Set[str] = set()
        self.seen_order_versions: Set[str] = set()
        self.poll_failures = 0
        self.qmt_status: Dict[str, Any] = {
            "state": "unknown",
            "ready": False,
            "account_id": cfg.account_id,
            "account_type": cfg.account_type,
            "last_error": "",
            "updated_at": now(),
        }

    def remember(self, msg_id: str, reply: Dict[str, Any]) -> None:
        if not msg_id:
            return
        self.done[msg_id] = dict(reply)
        self.done_order.append(msg_id)
        while len(self.done_order) > 2000:
            old = self.done_order.pop(0)
            self.done.pop(old, None)

    def get_order_intent_duplicate(self, key: str) -> Optional[Dict[str, Any]]:
        if not key:
            return None
        now_ts = now()
        for existing_key in list(self.order_intent_cache.keys()):
            if safe_float(self.order_intent_cache.get(existing_key, {}).get("expires_at"), 0.0) <= now_ts:
                self.order_intent_cache.pop(existing_key, None)
        item = self.order_intent_cache.get(key)
        if not item:
            return None
        return dict(item)

    def remember_order_intent(self, key: str, payload: Dict[str, Any], reply: Dict[str, Any]) -> None:
        if not key:
            return
        self.order_intent_cache[key] = {
            "expires_at": now() + ORDER_INTENT_DEDUPE_SECONDS,
            "payload": dict(payload or {}),
            "reply": dict(reply or {}),
            "request_id": safe_str((payload or {}).get("request_id") or (reply or {}).get("request_id") or ""),
            "client_order_id": safe_str((payload or {}).get("client_order_id") or (reply or {}).get("client_order_id") or ""),
        }
        self.remember_order_side_intent(payload, reply)

    def _prune_order_side_intents(self) -> None:
        now_ts = now()
        for existing_key in list(self.order_side_intents.keys()):
            if safe_float(self.order_side_intents.get(existing_key, {}).get("expires_at"), 0.0) <= now_ts:
                self.order_side_intents.pop(existing_key, None)

    def remember_order_correlation(self, item: Dict[str, Any]) -> None:
        row = dict(item or {})
        for key in _correlation_match_keys(row):
            self.persisted_order_correlations.pop(key, None)
            self.persisted_order_correlations[key] = row
            self.persisted_order_correlation_misses.pop(key, None)
        while len(self.persisted_order_correlations) > ORDER_CORRELATION_CACHE_MAX_KEYS:
            self.persisted_order_correlations.pop(next(iter(self.persisted_order_correlations)))

    def resolve_order_correlation(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Resolve only from the in-memory LRU; never perform I/O here."""
        keys = _correlation_match_keys(item or {})
        for key in keys:
            row = self.persisted_order_correlations.pop(key, None)
            if row:
                self.persisted_order_correlations[key] = row
                return row
        return None

    def _correlation_lookup_required(self, item: Dict[str, Any], now_ts: float) -> bool:
        keys = _correlation_match_keys(item or {})
        return bool(keys) and not all(
            self.persisted_order_correlation_misses.get(key, 0.0) > now_ts
            for key in keys
        )

    def _remember_order_correlation_miss(self, item: Dict[str, Any], now_ts: float) -> None:
        keys = _correlation_match_keys(item or {})
        for key in keys:
            self.persisted_order_correlation_misses.pop(key, None)
            self.persisted_order_correlation_misses[key] = (
                now_ts + ORDER_CORRELATION_MISS_TTL_SECONDS
            )
        while len(self.persisted_order_correlation_misses) > ORDER_CORRELATION_CACHE_MAX_KEYS:
            self.persisted_order_correlation_misses.pop(
                next(iter(self.persisted_order_correlation_misses))
            )

    def _apply_persisted_order_correlation(
        self, payload: Dict[str, Any], correlation: Dict[str, Any],
    ) -> Dict[str, Any]:
        original_side_text = safe_str(payload.get("raw_side_text") or payload.get("side"))
        for field in (
            "trace_id", "client_order_id", "msg_id", "request_id", "qmt_user_order_id",
        ):
            value = correlation.get(field)
            if value not in (None, ""):
                payload.setdefault(field, value)
        trader_name = safe_str(correlation.get("trader_name"))
        if trader_name:
            payload["trader_name"] = trader_name
            payload["order_remark"] = trader_name
        authenticated_trader_key = safe_str(
            correlation.get("authenticated_trader_key")
        ).strip()
        if authenticated_trader_key:
            payload["authenticated_trader_key"] = authenticated_trader_key
        side = normalize_side(correlation.get("side"))
        if side:
            payload["side"] = side
            payload["side_source"] = "correlation_intent"
            payload.setdefault("raw_side_text", original_side_text or side)
        order_type = safe_int(correlation.get("order_type"), 0)
        current_order_type = safe_int(payload.get("order_type"), 0)
        if order_type in STANDARD_ORDER_TYPES:
            if current_order_type and current_order_type != order_type:
                payload.setdefault("raw_order_type", current_order_type)
            payload["order_type"] = order_type
            payload["order_type_source"] = "correlation_intent"
            payload["order_type_valid"] = True
        return payload

    async def apply_order_side_intents_async(
        self, items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Apply memory intent first, then resolve all SQLite misses off-loop in one batch."""
        payloads: List[Dict[str, Any]] = []
        unresolved_indexes = []
        now_ts = now()
        for index, item in enumerate(items or []):
            payload = self.apply_order_side_intent(item)
            payloads.append(payload)
            if (
                self.resolve_order_correlation(payload) is None
                and self._correlation_lookup_required(payload, now_ts)
            ):
                unresolved_indexes.append(index)
            if index and index % ASYNC_NORMALIZE_YIELD_EVERY == 0:
                await asyncio.sleep(0)

        if not unresolved_indexes:
            return payloads

        async with self.correlation_lookup_lock:
            lookup_ts = now()
            fresh_unresolved_indexes = []
            for offset, index in enumerate(unresolved_indexes):
                if (
                    self.resolve_order_correlation(payloads[index]) is None
                    and self._correlation_lookup_required(payloads[index], lookup_ts)
                ):
                    fresh_unresolved_indexes.append(index)
                if offset and offset % ASYNC_NORMALIZE_YIELD_EVERY == 0:
                    await asyncio.sleep(0)
            unresolved_indexes = fresh_unresolved_indexes
            if not unresolved_indexes:
                return payloads

            lookup_items = [payloads[index] for index in unresolved_indexes]
            rows = await run_blocking(
                self.correlation.resolve_many, self.cfg.account_id, lookup_items,
            )
            for index, row in enumerate(rows):
                self.remember_order_correlation(row)
                if index and index % ASYNC_NORMALIZE_YIELD_EVERY == 0:
                    await asyncio.sleep(0)

            miss_ts = now()
            for offset, payload_index in enumerate(unresolved_indexes):
                payload = payloads[payload_index]
                correlation = self.resolve_order_correlation(payload)
                if correlation:
                    payloads[payload_index] = self._apply_persisted_order_correlation(
                        payload, correlation,
                    )
                else:
                    self._remember_order_correlation_miss(payload, miss_ts)
                if offset and offset % ASYNC_NORMALIZE_YIELD_EVERY == 0:
                    await asyncio.sleep(0)
        return payloads

    def remember_order_side_intent(self, payload: Dict[str, Any], reply: Dict[str, Any]) -> None:
        side = normalize_side((payload or {}).get("side") or (reply or {}).get("side"))
        if not side:
            return
        self._prune_order_side_intents()
        item = {
            "expires_at": now() + ORDER_SIDE_INTENT_SECONDS,
            "side": side,
            "order_type": safe_int(
                (payload or {}).get("order_type") or (reply or {}).get("order_type"), 0
            ),
            "symbol": safe_str((payload or {}).get("symbol") or (payload or {}).get("stock_code") or (reply or {}).get("symbol")),
            "quantity": safe_int((payload or {}).get("quantity") or (reply or {}).get("quantity"), 0),
            "order_remark": safe_str((payload or {}).get("order_remark") or (reply or {}).get("order_remark")),
            "request_id": safe_str((payload or {}).get("request_id") or (reply or {}).get("request_id")),
            "client_order_id": safe_str((payload or {}).get("client_order_id") or (reply or {}).get("client_order_id")),
        }
        for prefix, value in (
            ("order", (reply or {}).get("order_id")),
            ("sys", (reply or {}).get("order_sysid")),
        ):
            text = safe_str(value)
            if text and text != "0":
                self.order_side_intents["%s:%s" % (prefix, text)] = dict(item)

    def apply_order_side_intent(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Apply transient and persisted intent from memory without blocking I/O."""
        payload = dict(item or {})
        original_side_text = safe_str(payload.get("raw_side_text") or payload.get("side"))
        self._prune_order_side_intents()
        for key in _order_match_keys(payload):
            intent = self.order_side_intents.get(key)
            if not intent:
                continue
            side = normalize_side(intent.get("side"))
            if side:
                payload["side"] = side
                payload["side_source"] = "proxy_order_intent"
                payload.setdefault("raw_side_text", original_side_text or side)
            intent_order_type = safe_int(intent.get("order_type"), 0)
            current_order_type = safe_int(payload.get("order_type"), 0)
            if intent_order_type in STANDARD_ORDER_TYPES:
                if current_order_type and current_order_type != intent_order_type:
                    payload.setdefault("raw_order_type", current_order_type)
                payload["order_type"] = intent_order_type
                payload["order_type_source"] = "proxy_order_intent"
                payload["order_type_valid"] = True
            break

        correlation = self.resolve_order_correlation(payload)
        if correlation:
            payload = self._apply_persisted_order_correlation(payload, correlation)
        return payload

    def update_status_ready(self, health: Dict[str, Any]) -> None:
        self.poll_failures = 0
        self.qmt_status = {
            "state": "ready" if health.get("ready") else safe_str(health.get("state") or "degraded"),
            "ready": bool(health.get("ready")),
            "account_id": self.cfg.account_id,
            "account_type": self.cfg.account_type,
            "last_error": safe_str(health.get("last_error")),
            "helper": health,
            "updated_at": now(),
        }

    def update_status_offline(self, exc: Any) -> None:
        self.poll_failures += 1
        self.qmt_status = {
            "state": "offline",
            "ready": False,
            "account_id": self.cfg.account_id,
            "account_type": self.cfg.account_type,
            "last_error": safe_str(exc),
            "updated_at": now(),
        }

    def next_poll_delay(self, failed: bool) -> float:
        base = max(0.1, float(self.cfg.poll_interval_seconds or 1.0))
        if not failed:
            return base
        return min(10.0, base * (1 + min(self.poll_failures, 5)))


class TcpClientSession:
    def __init__(
        self,
        runtime: AccountRuntime,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    ) -> None:
        self.runtime = runtime
        self.reader = reader
        self.writer = writer
        self.max_frame_bytes = min(
            DEFAULT_MAX_FRAME_BYTES,
            max(1, safe_int(max_frame_bytes, DEFAULT_MAX_FRAME_BYTES)),
        )
        self.send_lock = asyncio.Lock()
        self.closed = False
        peer = writer.get_extra_info("peername")
        self.peer = "%s:%s" % peer if isinstance(peer, tuple) and len(peer) >= 2 else safe_str(peer)
        self.last_recv_at = now()
        self.dispatch_tasks: Set[asyncio.Task] = set()
        self.registered = False

    async def send(self, msg: Dict[str, Any]) -> bool:
        if self.closed:
            raise ConnectionError("TCP client session is closed")
        body = await run_blocking(
            lambda: json.dumps(
                msg,
                ensure_ascii=False,
                default=json_default,
                allow_nan=False,
            ).encode("utf-8")
        )
        if len(body) > self.max_frame_bytes:
            actual_frame_bytes = len(body)
            if safe_str(msg.get("type")) == "QUERY_RESPONSE":
                body = await run_blocking(
                    lambda: json.dumps({
                        "protocol_version": 2,
                        "type": "QUERY_RESPONSE",
                        "msg_id": safe_str(msg.get("msg_id")),
                        "success": False,
                        "status": "REJECTED",
                        "code": "FRAME_TOO_LARGE",
                        "account_id": self.runtime.cfg.account_id,
                        "account_type": self.runtime.cfg.account_type,
                        "max_frame_bytes": self.max_frame_bytes,
                        "actual_frame_bytes": actual_frame_bytes,
                        "reject_reason": "query response exceeds the outbound frame limit",
                        "timestamp": now(),
                    }, ensure_ascii=False, default=json_default, allow_nan=False).encode("utf-8")
                )
            if len(body) > self.max_frame_bytes:
                raise OutboundFrameTooLarge(len(body), self.max_frame_bytes)
        async with self.send_lock:
            if self.closed:
                raise ConnectionError("TCP client session closed before send")
            self.writer.write(struct.pack(">I", len(body)) + body)
            await self.writer.drain()
            return True

    async def close(self, reason: str = "") -> None:
        if self.closed:
            return
        self.closed = True
        try:
            async with self.send_lock:
                self.writer.close()
                await self.writer.wait_closed()
        except Exception:
            pass


async def read_frame(reader: asyncio.StreamReader, max_frame_bytes: int) -> Dict[str, Any]:
    header = await reader.readexactly(4)
    size = struct.unpack(">I", header)[0]
    if size <= 0 or size > max_frame_bytes:
        raise ValueError("invalid frame size: %s" % size)
    body = await reader.readexactly(size)
    data = json.loads(
        body.decode("utf-8"),
        parse_constant=reject_nonfinite_json,
    )
    if not isinstance(data, dict):
        raise ValueError("frame body must be a JSON object")
    return data


class BigQmtGatewayProxy:
    def __init__(self, cfg: GatewayConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.max_frame_bytes = min(
            DEFAULT_MAX_FRAME_BYTES,
            max(1, safe_int(cfg.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES)),
        )
        self.runtimes = [AccountRuntime(account, logger) for account in cfg.accounts]
        self.running = False
        self.poll_tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.logger.info(
            "proxy_start build=%s accounts=%s helper_mode=file_queue",
            PROXY_BUILD_ID,
            len(self.runtimes),
        )
        for runtime in self.runtimes:
            lease_token = runtime.writer_lease.acquire()
            server = await asyncio.start_server(
                lambda r, w, rt=runtime: self.handle_client(rt, r, w),
                host=runtime.cfg.tcp_host,
                port=runtime.cfg.tcp_port,
                backlog=self.cfg.listen_backlog,
            )
            runtime.server = server
            self.logger.info(
                "tcp_listen account=%s name=%s host=%s port=%s runtime_dir=%s writer_token=%s",
                runtime.cfg.account_id,
                runtime.cfg.name,
                runtime.cfg.tcp_host,
                runtime.cfg.tcp_port,
                runtime.cfg.runtime_dir,
                lease_token[:8],

            )
            self.poll_tasks.append(asyncio.create_task(self.poll_account_loop(runtime)))
            self.poll_tasks.append(asyncio.create_task(self.response_watcher_loop(runtime)))
            self.poll_tasks.append(asyncio.create_task(self.live_event_watcher_loop(runtime)))
            self.poll_tasks.append(asyncio.create_task(self.maintenance_loop(runtime)))

    async def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        self.logger.info("proxy_stop requested")
        for task in self.poll_tasks:
            task.cancel()
        await asyncio.gather(*self.poll_tasks, return_exceptions=True)
        self.poll_tasks.clear()
        for runtime in self.runtimes:
            if runtime.server:
                runtime.server.close()
                await runtime.server.wait_closed()
            async with runtime.clients_lock:
                clients = list(runtime.clients)
            await asyncio.gather(*(client.close("proxy_stop") for client in clients), return_exceptions=True)
            runtime.correlation.close()
            runtime.writer_lease.release()

    async def register_client(self, runtime: AccountRuntime, session: TcpClientSession) -> None:
        async with runtime.clients_lock:
            old_clients = list(runtime.clients)
            runtime.clients.add(session)
            runtime.primary = session
        for old in old_clients:
            asyncio.create_task(old.close("replaced"))

    async def unregister_client(self, runtime: AccountRuntime, session: TcpClientSession) -> None:
        async with runtime.clients_lock:
            runtime.clients.discard(session)
            if runtime.primary is session:
                runtime.primary = None

    async def handle_client(self, runtime: AccountRuntime, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = TcpClientSession(runtime, reader, writer, self.max_frame_bytes)
        self.logger.info("tcp_accept port=%s peer=%s", runtime.cfg.tcp_port, session.peer)
        try:
            while self.running and not session.closed:
                msg = await asyncio.wait_for(read_frame(reader, self.max_frame_bytes), timeout=self.cfg.tcp_idle_timeout_seconds)
                session.last_recv_at = now()
                msg["_gateway_received_at_ns"] = time.time_ns()
                raw_msg_id = msg.get("msg_id")
                msg_id_matches_auth_token = (
                    isinstance(raw_msg_id, str)
                    and hmac.compare_digest(
                        hashlib.sha256(raw_msg_id.encode("utf-8")).hexdigest(),
                        self.cfg.auth_token_sha256,
                    )
                )
                if msg_id_matches_auth_token:
                    msg["msg_id"] = "<redacted-secret>"
                if session.registered and msg_id_matches_auth_token:
                    await session.send({
                        "type": "ERROR",
                        "msg_id": "<redacted-secret>",
                        "status": "REJECTED",
                        "code": "INVALID_MESSAGE",
                        "reject_reason": "msg_id must not contain a credential",
                        "timestamp": now(),
                    })
                    break
                if not session.registered:
                    if safe_str(msg.get("type")) != "PING":
                        await session.send({
                            "type": "ERROR",
                            "msg_id": safe_str(msg.get("msg_id")),
                            "status": "REJECTED",
                            "code": "HANDSHAKE_REQUIRED",
                            "reject_reason": "PING handshake required before registering client",
                            "timestamp": now(),
                        })
                        break
                    provided_raw = msg.pop("auth_token", "")
                    provided_token = provided_raw if isinstance(provided_raw, str) else ""
                    provided_digest = hashlib.sha256(provided_token.encode("utf-8")).hexdigest()
                    handshake_valid = (
                        not msg_id_matches_auth_token
                        and len(provided_token) == 64
                        and hmac.compare_digest(provided_digest, self.cfg.auth_token_sha256)
                        and type(msg.get("protocol_version")) is int
                        and msg.get("protocol_version") == 2
                        and isinstance(msg.get("account_id"), str)
                        and msg.get("account_id") == runtime.cfg.account_id
                        and isinstance(msg.get("account_name"), str)
                        and msg.get("account_name") == runtime.cfg.name
                    )
                    if not handshake_valid:
                        await session.send({
                            "type": "ERROR",
                            "msg_id": safe_str(msg.get("msg_id")),
                            "status": "REJECTED",
                            "code": "HANDSHAKE_REJECTED",
                            "reject_reason": "authentication or identity mismatch",
                            "timestamp": now(),
                        })
                        self.logger.warning(
                            "tcp_handshake_rejected port=%s peer=%s",
                            runtime.cfg.tcp_port,
                            session.peer,
                        )
                        break
                    handshake_started = time.perf_counter()
                    reply = await self.dispatch(runtime, session, msg)
                    if reply is not None:
                        await session.send(reply)
                    await self.register_client(runtime, session)
                    session.registered = True
                    self.logger.info(
                        "tcp_handshake account=%s port=%s peer=%s msg_id=%s elapsed_ms=%.1f",
                        runtime.cfg.account_id,
                        runtime.cfg.tcp_port,
                        session.peer,
                        safe_str(msg.get("msg_id")),
                        (time.perf_counter() - handshake_started) * 1000,
                    )
                    continue
                task = asyncio.create_task(self._dispatch_and_send(runtime, session, msg))
                session.dispatch_tasks.add(task)
                task.add_done_callback(session.dispatch_tasks.discard)
        except asyncio.IncompleteReadError:
            pass
        except asyncio.TimeoutError:
            self.logger.info("tcp_idle_timeout account=%s port=%s peer=%s", runtime.cfg.account_id, runtime.cfg.tcp_port, session.peer)
        except Exception as exc:
            self.logger.warning("tcp_client_error account=%s port=%s peer=%s error=%s", runtime.cfg.account_id, runtime.cfg.tcp_port, session.peer, exc)
        finally:
            for task in list(session.dispatch_tasks):
                task.cancel()
            await asyncio.gather(*session.dispatch_tasks, return_exceptions=True)
            await self.unregister_client(runtime, session)
            await session.close("client_loop_end")

    async def _dispatch_and_send(self, runtime: AccountRuntime, session: TcpClientSession, msg: Dict[str, Any]) -> None:
        started = time.perf_counter()
        try:
            reply = await self.dispatch(runtime, session, msg)
            if reply is not None:
                await session.send(reply)
            if msg.get("type") == "PING":
                self.logger.info(
                    "tcp_handshake account=%s port=%s peer=%s msg_id=%s elapsed_ms=%.1f",
                    runtime.cfg.account_id,
                    runtime.cfg.tcp_port,
                    session.peer,
                    safe_str(msg.get("msg_id")),
                    (time.perf_counter() - started) * 1000,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.warning(
                "dispatch_failed account=%s type=%s msg_id=%s error=%s",
                runtime.cfg.account_id,
                safe_str(msg.get("type")),
                safe_str(msg.get("msg_id")),
                exc,
            )
            if not session.closed:
                await session.send({
                    "type": "ERROR",
                    "msg_id": safe_str(msg.get("msg_id")),
                    "status": "REJECTED",
                    "reject_reason": safe_str(exc),
                    "timestamp": now(),
                })

    async def dispatch(self, runtime: AccountRuntime, session: TcpClientSession, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        msg_type = safe_str(msg.get("type"), "")
        msg_id = safe_str(msg.get("msg_id"), "")
        if msg_type in {
            "NEW", "NEW_ASYNC", "CANCEL", "CANCEL_ASYNC",
            "CANCEL_SYSID", "CANCEL_SYSID_ASYNC", "QUERY",
        }:
            raw_account_id = msg.get("account_id")
            raw_account_name = msg.get("account_name")
            requested_account_id = raw_account_id if isinstance(raw_account_id, str) else ""
            requested_account_name = raw_account_name if isinstance(raw_account_name, str) else ""
            configured_account_id = runtime.cfg.account_id
            if (
                requested_account_id != configured_account_id
                or requested_account_name != runtime.cfg.name
            ):
                return {
                    "protocol_version": 2,
                    "type": "ERROR",
                    "msg_id": msg_id,
                    "status": "REJECTED",
                    "code": "ACCOUNT_MISMATCH",
                    "account_id": configured_account_id,
                    "reject_reason": "message account identity does not match the authenticated connection",
                    "timestamp": now(),
                }
        if msg_id and msg_type != "QUERY" and msg_id in runtime.done:
            cached = dict(runtime.done[msg_id])
            cached["cached"] = True
            return cached
        if msg_type == "PING":
            return {
                "type": "PONG",
                "msg_id": msg_id,
                "timestamp": now(),
                "gateway": "local_qmt_gateway",
                "protocol_version": 2,
                "build_id": PROXY_BUILD_ID,
                "writer_token": runtime.writer_lease.token,
                "account_id": runtime.cfg.account_id,
                "account_name": runtime.cfg.name,
                "qmt_status": runtime.qmt_status,
            }
        if msg_type == "PONG":
            return None
        if msg_type == "DELIVERY_ACK":
            delivery_id = safe_str(msg.get("delivery_id"))
            async with runtime.delivery_lock:
                waiter = runtime.delivery_waiters.get(delivery_id)
                if waiter is not None:
                    waiter.set()
            return None
        if msg_type == "QUERY":
            return await self.handle_query_brokered(runtime, msg)
        if msg_type in ("NEW", "NEW_ASYNC"):
            reply = await self.handle_new(runtime, msg, async_mode=(msg_type == "NEW_ASYNC"))
            runtime.remember(msg_id, reply)
            return reply
        if msg_type in ("CANCEL", "CANCEL_ASYNC"):
            reply = await self.handle_cancel(runtime, msg, async_mode=(msg_type == "CANCEL_ASYNC"))
            runtime.remember(msg_id, reply)
            return reply
        if msg_type in ("CANCEL_SYSID", "CANCEL_SYSID_ASYNC"):
            reply = await self.handle_cancel_sysid(runtime, msg, async_mode=(msg_type == "CANCEL_SYSID_ASYNC"))
            runtime.remember(msg_id, reply)
            return reply
        if msg_type == "FUND_TRANSFER":
            return await self.handle_fund_transfer(runtime, msg)
        if msg_type == "SYNC_TRADE":
            return await self.handle_sync_trade(runtime, msg)
        if msg_type == "SMT_NEGOTIATE":
            return await self.handle_smt_negotiate(runtime, msg)
        if msg_type in ("SUBSCRIBE", "UNSUBSCRIBE"):
            return {
                "type": msg_type + "_ACK",
                "msg_id": msg_id,
                "status": "OK",
                "account_id": runtime.cfg.account_id,
                "timestamp": now(),
            }
        return {
            "type": "ERROR",
            "msg_id": msg_id,
            "status": "REJECTED",
            "reject_reason": "unknown message type: %s" % msg_type,
            "timestamp": now(),
        }

    async def _ensure_helper_ready(self, runtime: AccountRuntime) -> Dict[str, Any]:
        health = await runtime.helper.health()
        runtime.update_status_ready(health)
        identity_failures = helper_identity_mismatches(runtime.cfg, health)
        if identity_failures:
            raise HelperUnavailable(
                "helper identity mismatch: %s" % ", ".join(identity_failures),
                "HELPER_IDENTITY_MISMATCH",
            )
        failures = []
        if not health.get("ready"):
            failures.append("state=%s" % health.get("state"))
        if failures:
            raise HelperUnavailable(
                "helper is not ready: %s" % ", ".join(failures),
                "HELPER_NOT_READY",
            )
        return health

    async def handle_query_brokered(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        key = stable_hash({
            "query_type": safe_str(msg.get("query_type")).upper(),
            "params": msg.get("params") if isinstance(msg.get("params"), dict) else {},
        })
        async with runtime.query_lock:
            task = runtime.query_singleflight.get(key)
            if task is None or task.done():
                task = asyncio.create_task(self._run_serial_query(runtime, msg))
                runtime.query_singleflight[key] = task
        try:
            reply = dict(await asyncio.shield(task))
            reply["msg_id"] = safe_str(msg.get("msg_id"))
            return reply
        finally:
            async with runtime.query_lock:
                if runtime.query_singleflight.get(key) is task and task.done():
                    runtime.query_singleflight.pop(key, None)

    async def _run_serial_query(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        async with runtime.query_semaphore:
            return await self.handle_query(runtime, msg)

    async def handle_query(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        msg_id = safe_str(msg.get("msg_id"), "")
        query_type = safe_str(msg.get("query_type"), "").upper()
        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
        try:
            health = await self._ensure_helper_ready(runtime)
            if not query_type:
                data = await runtime.helper.latest_snapshot()
                if not data:
                    data = await runtime.helper.snapshot()
                return await self.query_response(runtime, msg_id, True, data)
            if query_type == "POSITION":
                data = await runtime.helper.positions(**params)
                positions = data.get("positions") or []
                stock_code = safe_str(params.get("stock_code") or params.get("symbol") or params.get("security"), "")
                position = positions[0] if stock_code and positions else {}
                return {
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": True,
                    "query_type": query_type,
                    "position": position,
                    "position_found": bool(position),
                    "positions": positions,
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            if query_type == "ORDER":
                order_id = safe_str(params.get("order_id") or params.get("order_sysid") or msg.get("order_id"), "")
                data = await runtime.helper.order_status(order_id)
                order = data.get("order") if isinstance(data.get("order"), dict) else {}
                if order:
                    enriched = await runtime.apply_order_side_intents_async([order])
                    order = normalize_standard_order_payload(enriched[0])
                return {
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": True,
                    "query_type": query_type,
                    "order": order,
                    "order_found": bool(data.get("found")),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            if query_type in ("DEAL", "TRADE"):
                data = await runtime.helper.trades(**params)
                try:
                    snapshot = await runtime.helper.latest_snapshot()
                except Exception:
                    snapshot = {}
                matched_orders = await normalize_standard_orders_async(
                    (snapshot or {}).get("orders") or [], runtime,
                )
                trades = await normalize_standard_trades_async(
                    data.get("trades") or [], matched_orders, runtime,
                )
                return {
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": True,
                    "query_type": query_type,
                    "trades": trades,
                    "trade_count": len(trades),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            if query_type in ("ACCOUNT", "ASSET", "ACCOUNT_INFOS", "COM_FUND"):
                data = await runtime.helper.account()
                asset = data.get("asset") or {}
                return {
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": True,
                    "query_type": query_type,
                    "asset": asset,
                    "asset_available": bool(asset),
                    "accounts": data.get("accounts") or [],
                    "account_infos": data.get("accounts") or [],
                    "com_fund": asset,
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            if query_type == "COM_POSITION":
                data = await runtime.helper.positions(**params)
                return {
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": True,
                    "query_type": query_type,
                    "com_position": data.get("positions") or [],
                    "positions": data.get("positions") or [],
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            if query_type == "ACCOUNT_STATUS":
                return {
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": True,
                    "query_type": query_type,
                    "account_status": health,
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            return {
                "type": "QUERY_RESPONSE",
                "msg_id": msg_id,
                "success": False,
                "query_type": query_type,
                "code": "UNSUPPORTED_BIGQMT_QUERY",
                "reject_reason": "unsupported BigQMT query_type: %s" % query_type,
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }
        except Exception as exc:
            runtime.update_status_offline(exc)
            if safe_str(getattr(exc, "code", "")) == "HELPER_IDENTITY_MISMATCH":
                return {
                    "protocol_version": 2,
                    "type": "QUERY_RESPONSE",
                    "msg_id": msg_id,
                    "success": False,
                    "status": "REJECTED",
                    "query_type": query_type,
                    "code": "HELPER_IDENTITY_MISMATCH",
                    "account_id": runtime.cfg.account_id,
                    "account_type": runtime.cfg.account_type,
                    "reject_reason": safe_str(exc),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            cached = await self.cached_query_response(runtime, msg_id, query_type, params, safe_str(exc))
            if cached:
                return cached
            return await self.query_response(
                runtime, msg_id, False, {},
                reject_reason=safe_str(exc), query_type=query_type,
            )

    async def cached_query_response(
        self,
        runtime: AccountRuntime,
        msg_id: str,
        query_type: str,
        params: Dict[str, Any],
        reject_reason: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            snapshot = await runtime.helper.latest_snapshot()
        except Exception:
            snapshot = {}
        if not snapshot:
            return None
        qmt_status = dict(runtime.qmt_status)
        qmt_status["state"] = "degraded"
        qmt_status["ready"] = False
        qmt_status["cache_fallback"] = True
        qmt_status["last_error"] = reject_reason

        asset = snapshot.get("asset") or {}
        positions = snapshot.get("positions") or []
        orders = await normalize_standard_orders_async(snapshot.get("orders") or [], runtime)
        trades = await normalize_standard_trades_async(
            snapshot.get("trades") or [], orders, runtime,
        )

        if not runtime.snapshot_baseline_ready:
            for order in orders:
                runtime.seen_order_versions.add(self._order_version_key(order))
            for trade in trades:
                runtime.seen_trade_keys.add(self._trade_key(trade))
            runtime.snapshot_baseline_ready = True
        stock_code = safe_str(params.get("stock_code") or params.get("symbol") or params.get("security"), "")

        if not query_type:
            reply = await self.query_response(
                runtime, msg_id, True, snapshot, reject_reason=reject_reason,
            )
        elif query_type in ("ACCOUNT", "ASSET", "ACCOUNT_INFOS", "COM_FUND"):
            reply = {
                "type": "QUERY_RESPONSE",
                "msg_id": msg_id,
                "success": True,
                "query_type": query_type,
                "asset": asset,
                "asset_available": bool(asset),
                "accounts": snapshot.get("accounts") or ([asset] if asset else []),
                "account_infos": snapshot.get("accounts") or ([asset] if asset else []),
                "com_fund": asset,
                "reject_reason": reject_reason,
                "timestamp": now(),
            }
        elif query_type in ("POSITION", "COM_POSITION"):
            filtered = [
                item for item in positions
                if not stock_code or item.get("stock_code") == stock_code or item.get("symbol") == stock_code
            ]
            reply = {
                "type": "QUERY_RESPONSE",
                "msg_id": msg_id,
                "success": True,
                "query_type": query_type,
                "position": filtered[0] if query_type == "POSITION" and filtered else {},
                "position_found": bool(query_type == "POSITION" and filtered),
                "positions": filtered,
                "com_position": filtered,
                "reject_reason": reject_reason,
                "timestamp": now(),
            }
        elif query_type == "ORDER":
            order_id = safe_str(params.get("order_id") or params.get("order_sysid"), "")
            filtered_orders = [
                item for item in orders
                if not order_id or safe_str(item.get("order_id")) == order_id or safe_str(item.get("order_sysid")) == order_id
            ]
            reply = {
                "type": "QUERY_RESPONSE",
                "msg_id": msg_id,
                "success": True,
                "query_type": query_type,
                "order": filtered_orders[0] if filtered_orders else {},
                "order_found": bool(filtered_orders),
                "orders": filtered_orders,
                "reject_reason": reject_reason,
                "timestamp": now(),
            }
        elif query_type in ("DEAL", "TRADE"):
            reply = {
                "type": "QUERY_RESPONSE",
                "msg_id": msg_id,
                "success": True,
                "query_type": query_type,
                "trades": trades,
                "trade_count": len(trades),
                "reject_reason": reject_reason,
                "timestamp": now(),
            }
        elif query_type == "ACCOUNT_STATUS":
            reply = {
                "type": "QUERY_RESPONSE",
                "msg_id": msg_id,
                "success": True,
                "query_type": query_type,
                "account_status": qmt_status,
                "reject_reason": reject_reason,
                "timestamp": now(),
            }
        else:
            return None
        reply["cache_fallback"] = True
        reply["qmt_status"] = qmt_status
        return reply


    async def query_response(
        self,
        runtime: AccountRuntime,
        msg_id: str,
        success: bool,
        data: Dict[str, Any],
        reject_reason: str = "",
        query_type: str = "",
    ) -> Dict[str, Any]:
        positions = data.get("positions") or []
        orders = await normalize_standard_orders_async(data.get("orders") or [], runtime)
        trades = await normalize_standard_trades_async(data.get("trades") or [], orders, runtime)
        asset = data.get("asset") or {}
        return {
            "type": "QUERY_RESPONSE",
            "msg_id": msg_id,
            "success": success,
            "query_type": query_type,
            "account_id": runtime.cfg.account_id,
            "account_type": runtime.cfg.account_type,
            "asset": asset,
            "asset_available": bool(asset),
            "positions": positions,
            "position_count": len(positions),
            "orders": orders,
            "order_count": len(orders),
            "trades": trades,
            "trade_count": len(trades),
            "qmt_status": runtime.qmt_status,
            "reject_reason": reject_reason,
            "timestamp": now(),
        }

    async def handle_new(self, runtime: AccountRuntime, msg: Dict[str, Any], async_mode: bool = False) -> Dict[str, Any]:
        msg_id = safe_str(msg.get("msg_id"), "")
        if not safe_str(msg.get("client_order_id") or "").strip():
            return {
                "protocol_version": 2,
                "type": "ASYNC_ORDER" if async_mode else "EXEC_REPORT",
                "msg_id": msg_id,
                "status": "REJECTED",
                "stage": "REJECTED",
                "code": "CLIENT_ORDER_ID_REQUIRED",
                "reject_reason": "client_order_id is required for safe idempotency",
                "timestamp": now(),
            }
        payload = self.order_payload(runtime, msg)
        intent_key = self.order_intent_key(payload)
        upstream_intent_hash = safe_str(payload.pop("upstream_intent_hash", ""))
        if upstream_intent_hash and upstream_intent_hash != intent_key:
            return {
                "protocol_version": 2,
                "type": "ASYNC_ORDER" if async_mode else "EXEC_REPORT",
                "msg_id": msg_id,
                "status": "REJECTED",
                "stage": "REJECTED",
                "code": "INTENT_HASH_MISMATCH",
                "reject_reason": "upstream canonical intent hash does not match gateway",
                "client_order_id": payload.get("client_order_id", ""),
                "trace_id": payload.get("trace_id", ""),
                "timestamp": now(),
            }
        payload["intent_hash"] = intent_key
        try:
            await self._ensure_helper_ready(runtime)
        except Exception as exc:
            runtime.update_status_offline(exc)
            return {
                "protocol_version": 2,
                "type": "ASYNC_ORDER" if async_mode else "EXEC_REPORT",
                "msg_id": msg_id,
                "seq": -1 if async_mode else 0,
                "status": "REJECTED",
                "stage": "REJECTED",
                "code": safe_str(getattr(exc, "code", "HELPER_NOT_READY")),
                "reject_reason": safe_str(exc),
                "request_id": payload.get("request_id", ""),
                "client_order_id": payload.get("client_order_id", ""),
                "trace_id": payload.get("trace_id", ""),
                "qmt_user_order_id": payload.get("qmt_user_order_id", ""),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }
        try:
            existing, idempotent = await run_blocking(runtime.correlation.reserve, {
                **payload,
                "stage": "RESERVED",
            })
        except IdempotencyConflict as exc:
            return {
                "protocol_version": 2,
                "type": "ASYNC_ORDER" if async_mode else "EXEC_REPORT",
                "msg_id": msg_id,
                "status": "REJECTED",
                "stage": "REJECTED",
                "code": "IDEMPOTENCY_CONFLICT",
                "reject_reason": safe_str(exc),
                "client_order_id": payload["client_order_id"],
                "trace_id": payload["trace_id"],
                "timestamp": now(),
            }
        except Exception as exc:
            return {
                "protocol_version": 2,
                "type": "ASYNC_ORDER" if async_mode else "EXEC_REPORT",
                "msg_id": msg_id,
                "request_id": payload.get("request_id", ""),
                "status": "REJECTED",
                "stage": "REJECTED",
                "code": "CORRELATION_RESERVE_FAILED",
                "reject_reason": safe_str(exc),
                "client_order_id": payload.get("client_order_id", ""),
                "trace_id": payload.get("trace_id", ""),
                "qmt_user_order_id": payload.get("qmt_user_order_id", ""),
                "timestamp": now(),
            }
        runtime.remember_order_correlation(existing)
        if idempotent:
            return {
                "protocol_version": 2,
                "type": "ASYNC_ORDER" if async_mode else "EXEC_REPORT",
                "msg_id": msg_id,
                "status": "SENT" if async_mode else "ACCEPTED",
                "stage": safe_str(existing.get("stage") or "BRIDGE_QUEUED"),
                "request_id": existing.get("request_id"),
                "client_order_id": existing.get("client_order_id"),
                "trace_id": existing.get("trace_id"),
                "qmt_user_order_id": existing.get("qmt_user_order_id"),
                "order_id": existing.get("order_id") or "",
                "idempotent": True,
                "dedupe_layer": "gateway_correlation",
                "timestamp": now(),
            }
        effect_started = False
        try:
            effect_started = True
            data = await runtime.helper.place_order(payload, wait=not async_mode)
            submit_status = safe_str(data.get("status") or "queued")
            failed = submit_status == "failed"
            if async_mode:
                queued_at_ns = time.time_ns()
                reply = {
                    "protocol_version": 2,
                    "type": "ASYNC_ORDER",
                    "msg_id": msg_id,
                    "seq": 0 if not failed else -1,
                    "status": "SENT" if not failed else "REJECTED",
                    "stage": "BRIDGE_QUEUED" if not failed else "REJECTED",
                    "request_id": data.get("request_id") or payload["request_id"],
                    "client_order_id": payload.get("client_order_id", ""),
                    "trace_id": payload.get("trace_id", ""),
                    "qmt_user_order_id": payload.get("qmt_user_order_id", ""),
                    "intent_hash": payload.get("intent_hash", ""),
                    "gateway_received_at_ns": payload.get("gateway_received_at_ns", 0),
                    "queued_at_ns": queued_at_ns,
                    "bridge_queue_elapsed_ms": max(
                        0.0,
                        (queued_at_ns - safe_int(payload.get("gateway_received_at_ns"), queued_at_ns)) / 1000000.0,
                    ),
                    "transport_to_bridge_ms": max(
                        0.0,
                        (queued_at_ns - safe_int(payload.get("created_at_ns"), queued_at_ns)) / 1000000.0,
                    ),
                    "idempotent": bool(data.get("idempotent")),
                    "submit_status": submit_status,
                    "reject_reason": data.get("error", ""),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
                if not failed:
                    await run_blocking(runtime.correlation.update_stage, runtime.cfg.account_id, payload["client_order_id"], "BRIDGE_QUEUED")
                    async with runtime.pending_lock:
                        runtime.pending_responses[payload["request_id"]] = {
                            "payload": dict(payload),
                            "queued_at": now(),
                            "deadline_at": now() + runtime.cfg.request_timeout_seconds,
                        }
                else:
                    await run_blocking(runtime.correlation.update_stage, runtime.cfg.account_id, payload["client_order_id"], "REJECTED")
                return reply
            reply = {
                "type": "EXEC_REPORT",
                "msg_id": msg_id,
                "order_id": safe_str(data.get("order_id") or ""),
                "status": "ACCEPTED" if not failed else "REJECTED",
                "submit_status": submit_status,
                "request_id": data.get("request_id") or payload["request_id"],
                "client_order_id": payload.get("client_order_id", ""),
                "symbol": payload["symbol"],
                "side": payload["side"],
                "quantity": payload["quantity"],
                "price": payload["price"],
                "order_type": payload.get("order_type"),
                "price_type": payload.get("price_type"),
                "filled_qty": 0,
                "filled_price": 0.0,
                "final": False,
                "reject_reason": data.get("error", ""),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }
            stage = "REJECTED" if failed else ("SUBMIT_UNKNOWN" if submit_status == "submit_unknown" else "QMT_SUBMITTED")
            await run_blocking(
                lambda: runtime.correlation.update_stage(
                    runtime.cfg.account_id, payload["client_order_id"], stage,
                    order_id=safe_str(data.get("order_id")),
                )
            )
            reply["stage"] = stage
            reply["trace_id"] = payload.get("trace_id", "")
            reply["qmt_user_order_id"] = payload.get("qmt_user_order_id", "")
            return reply
        except Exception as exc:
            runtime.update_status_offline(exc)
            uncertain = effect_started
            fallback_stage = "SUBMIT_UNKNOWN" if uncertain else "REJECTED"
            try:
                await run_blocking(
                    runtime.correlation.update_stage,
                    runtime.cfg.account_id,
                    payload["client_order_id"],
                    fallback_stage,
                )
            except Exception:
                pass
            if async_mode:
                return {
                    "protocol_version": 2,
                    "type": "ASYNC_ORDER",
                    "msg_id": msg_id,
                    "seq": 0 if uncertain else -1,
                    "status": "SENT" if uncertain else "REJECTED",
                    "stage": fallback_stage,
                    "code": "POST_ENQUEUE_STATE_UNCERTAIN" if uncertain else "HELPER_NOT_READY",
                    "reject_reason": safe_str(exc),
                    "client_order_id": payload.get("client_order_id", ""),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            return {
                "type": "EXEC_REPORT",
                "msg_id": msg_id,
                "order_id": "",
                "status": "ACCEPTED" if uncertain else "REJECTED",
                "stage": fallback_stage,
                "symbol": payload.get("symbol", ""),
                "side": payload.get("side", ""),
                "quantity": payload.get("quantity", 0),
                "price": payload.get("price", 0.0),
                "filled_qty": 0,
                "filled_price": 0.0,
                "reject_reason": safe_str(exc),
                "client_order_id": payload.get("client_order_id", ""),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }

    def order_payload(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        symbol = safe_str(msg.get("symbol") or msg.get("stock_code") or msg.get("security"), "").strip()
        side = safe_str(msg.get("side") or "BUY").strip().upper()
        client_order_id = safe_str(msg.get("client_order_id") or "").strip()
        msg_id = safe_str(msg.get("msg_id") or "").strip()
        request_id = safe_str(msg.get("request_id") or client_order_id or msg_id or make_request_id("order")).strip()
        qmt_user_order_id = safe_str(msg.get("qmt_user_order_id") or "").strip()
        if not qmt_user_order_id:
            trader_prefix = "".join(
                ch for ch in safe_str(msg.get("trader_name") or msg.get("order_remark")) if ch.isascii() and ch.isalnum()
            )[:4] or "XL"
            suffix_len = max(8, 22 - len(trader_prefix))
            qmt_user_order_id = trader_prefix + "-" + hashlib.sha256(client_order_id.encode("utf-8")).hexdigest()[:suffix_len]
        raw_order_type = msg.get("order_type")
        direct_cash_repay = safe_int(raw_order_type, 0) in (32, 75)
        quantity = (
            safe_float(msg.get("quantity", msg.get("volume", 0)), 0.0)
            if direct_cash_repay
            else safe_int(msg.get("quantity", msg.get("volume", 0)), 0)
        )
        intent_volume = msg.get("intent_volume") if "intent_volume" in msg else quantity
        payload = {
            "protocol_version": 2,
            "trace_id": safe_str(msg.get("trace_id") or client_order_id).strip(),
            "request_id": request_id,
            "msg_id": msg_id,
            "client_order_id": client_order_id,
            "qmt_user_order_id": qmt_user_order_id[:23],
            "account_id": runtime.cfg.account_id,
            "account_type": runtime.cfg.account_type,
            "symbol": symbol,
            "stock_code": symbol,
            "side": side,
            "quantity": quantity,
            "intent_volume": safe_int(intent_volume, 0),
            "price": safe_float(msg.get("price", 0.0), 0.0),
            "effective_price": safe_float(msg.get("effective_price", msg.get("price", 0.0)), 0.0),
            "price_type": safe_int(msg.get("price_type", 11), 11),
            "order_type": raw_order_type,
            "business_order_type": safe_str(msg.get("business_order_type") or "limit").strip().lower(),
            "spread": safe_float(msg.get("spread"), 0.0),
            "credit_mode": safe_str(msg.get("credit_mode")).strip().lower(),
            "qmt_order_type": msg.get("qmt_order_type", msg.get("orderType")),
            "strategy_name": safe_str(msg.get("strategy_name") or "xuanling").strip(),
            "order_remark": safe_str(msg.get("order_remark") or "").strip(),
            "trader_name": safe_str(msg.get("trader_name") or msg.get("order_remark") or "").strip(),
            "authenticated_trader_key": safe_str(
                msg.get("authenticated_trader_key") or ""
            ).strip(),
            "created_at_ns": safe_int(msg.get("created_at_ns"), int(now() * 1000000000)),
            "gateway_received_at_ns": safe_int(msg.get("_gateway_received_at_ns"), time.time_ns()),
        }
        payload["upstream_intent_hash"] = safe_str(msg.get("intent_hash"))
        payload["intent_hash"] = self.order_intent_key(payload)
        return payload

    def order_intent_key(self, payload: Dict[str, Any]) -> str:
        if not payload:
            return ""
        canonical = stable_hash({
            "account_id": safe_str(payload.get("account_id")).strip(),
            "stock_code": safe_str(payload.get("symbol") or payload.get("stock_code")).strip().upper(),
            "side": safe_str(payload.get("side")).strip().upper(),
            "volume": safe_int(payload.get("intent_volume", payload.get("quantity")), 0),
            "effective_price": fixed_decimal(payload.get("effective_price", payload.get("price"))),
            "price_type": safe_int(payload.get("price_type"), 0),
            "business_order_type": safe_str(payload.get("business_order_type")).strip().lower(),
            "spread": fixed_decimal(payload.get("spread")),
            "credit_mode": safe_str(payload.get("credit_mode")).strip().lower(),
            "strategy_name": safe_str(payload.get("strategy_name") or "xuanling").strip(),
            "trader_name": safe_str(payload.get("trader_name") or payload.get("order_remark")).strip(),
            "authenticated_trader_key": safe_str(
                payload.get("authenticated_trader_key")
            ).strip(),
        })
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    async def handle_cancel(self, runtime: AccountRuntime, msg: Dict[str, Any], async_mode: bool = False) -> Dict[str, Any]:
        return await self._handle_cancel_payload(runtime, msg, async_mode, order_sysid=False)

    async def handle_cancel_sysid(self, runtime: AccountRuntime, msg: Dict[str, Any], async_mode: bool = False) -> Dict[str, Any]:
        return await self._handle_cancel_payload(runtime, msg, async_mode, order_sysid=True)

    async def _handle_cancel_payload(self, runtime: AccountRuntime, msg: Dict[str, Any], async_mode: bool, order_sysid: bool) -> Dict[str, Any]:
        msg_id = safe_str(msg.get("msg_id"), "")
        payload = {
            "request_id": msg_id or make_request_id("cancel"),
            "msg_id": msg_id,
            "account_id": runtime.cfg.account_id,
            "account_type": runtime.cfg.account_type,
            "order_id": "" if order_sysid else safe_str(msg.get("order_id") or ""),
            "order_sysid": safe_str(msg.get("order_sysid") or msg.get("order_id") or "") if order_sysid else safe_str(msg.get("order_sysid") or ""),
            "market": msg.get("market", 0),
        }
        try:
            await self._ensure_helper_ready(runtime)
            data = await runtime.helper.cancel_order(payload, wait=not async_mode)
            submit_status = safe_str(data.get("status") or "queued")
            failed = submit_status == "failed"
            if async_mode:
                return {
                    "type": "ASYNC_CANCEL",
                    "msg_id": msg_id,
                    "seq": 0 if not failed else -1,
                    "status": "SENT" if not failed else "REJECTED",
                    "request_id": data.get("request_id") or payload["request_id"],
                    "cancel_status": submit_status,
                    "reject_reason": data.get("error", ""),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            return {
                "type": "EXEC_REPORT",
                "msg_id": msg_id,
                "order_id": payload["order_id"],
                "order_sysid": payload["order_sysid"],
                "status": "CANCELLED" if not failed else "REJECTED",
                "cancel_status": "cancel_sent" if not failed else "failed",
                "submit_status": submit_status,
                "final": False,
                "reject_reason": data.get("error", ""),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }
        except Exception as exc:
            runtime.update_status_offline(exc)
            if async_mode:
                return {
                    "type": "ASYNC_CANCEL",
                    "msg_id": msg_id,
                    "seq": -1,
                    "status": "REJECTED",
                    "code": safe_str(getattr(exc, "code", "HELPER_NOT_READY")),
                    "reject_reason": safe_str(exc),
                    "qmt_status": runtime.qmt_status,
                    "timestamp": now(),
                }
            return {
                "type": "EXEC_REPORT",
                "msg_id": msg_id,
                "order_id": payload["order_id"],
                "order_sysid": payload["order_sysid"],
                "status": "REJECTED",
                "cancel_status": "failed",
                "final": False,
                "code": safe_str(getattr(exc, "code", "HELPER_NOT_READY")),
                "reject_reason": safe_str(exc),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }

    async def handle_fund_transfer(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        return await self.handle_generic_action(runtime, msg, "fund_transfer", "FUND_TRANSFER_RESULT")

    async def handle_sync_trade(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        return await self.handle_generic_action(runtime, msg, "sync_trade", "SYNC_TRADE_RESULT")

    async def handle_smt_negotiate(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> Dict[str, Any]:
        return await self.handle_generic_action(runtime, msg, "smt_negotiate", "SMT_NEGOTIATE_RESPONSE")

    async def handle_generic_action(self, runtime: AccountRuntime, msg: Dict[str, Any], action: str, reply_type: str) -> Dict[str, Any]:
        msg_id = safe_str(msg.get("msg_id"), "")
        payload = dict(msg)
        payload["request_id"] = msg_id or make_request_id(action)
        try:
            await self._ensure_helper_ready(runtime)
            data = await runtime.helper.generic_action(action, payload)
            failed = data.get("status") == "failed"
            reply = {
                "type": reply_type,
                "msg_id": msg_id,
                "success": not failed,
                "status": "SENT" if not failed else "REJECTED",
                "result": data,
                "code": data.get("code", ""),
                "msg": data.get("error", ""),
                "reject_reason": data.get("error", ""),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }
            if reply_type == "SMT_NEGOTIATE_RESPONSE":
                reply["seq"] = data.get("seq", 0 if not failed else -1)
            return reply
        except HelperError as exc:
            return {
                "type": reply_type,
                "msg_id": msg_id,
                "success": False,
                "status": "REJECTED",
                "code": exc.code,
                "msg": safe_str(exc),
                "reject_reason": safe_str(exc),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }
        except Exception as exc:
            runtime.update_status_offline(exc)
            return {
                "type": reply_type,
                "msg_id": msg_id,
                "success": False,
                "status": "REJECTED",
                "code": "HELPER_OFFLINE",
                "msg": safe_str(exc),
                "reject_reason": safe_str(exc),
                "qmt_status": runtime.qmt_status,
                "timestamp": now(),
            }

    async def broadcast(self, runtime: AccountRuntime, msg: Dict[str, Any]) -> bool:
        async with runtime.clients_lock:
            clients = list(runtime.clients)
        if not clients:
            return False
        active = [client for client in clients if not client.closed]
        if not active:
            return False
        results = await asyncio.gather(*(client.send(msg) for client in active), return_exceptions=True)
        return any(result is True for result in results)

    async def broadcast_confirmed(
        self,
        runtime: AccountRuntime,
        msg: Dict[str, Any],
        delivery_id: str,
        timeout: float = 1.0,
    ) -> bool:
        outbound = dict(msg)
        outbound["delivery_id"] = delivery_id
        waiter = asyncio.Event()
        async with runtime.delivery_lock:
            runtime.delivery_waiters[delivery_id] = waiter
        try:
            if not await self.broadcast(runtime, outbound):
                return False
            try:
                await asyncio.wait_for(waiter.wait(), timeout=max(0.05, timeout))
                return True
            except asyncio.TimeoutError:
                return False
        finally:
            async with runtime.delivery_lock:
                if runtime.delivery_waiters.get(delivery_id) is waiter:
                    runtime.delivery_waiters.pop(delivery_id, None)

    async def response_watcher_loop(self, runtime: AccountRuntime) -> None:
        interval = max(0.005, runtime.cfg.response_watch_interval_seconds)
        while self.running:
            try:
                async with runtime.clients_lock:
                    has_client = runtime.primary is not None and not runtime.primary.closed
                if not has_client:
                    await asyncio.sleep(interval)
                    continue

                async with runtime.pending_lock:
                    pending = list(runtime.pending_responses.items())[:64]
                if pending:
                    results = await asyncio.gather(*(
                        self._process_pending_response(runtime, request_id, item)
                        for request_id, item in pending
                    ), return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            self.logger.warning(
                                "pending_response_delivery_failed account=%s error=%s",
                                runtime.cfg.account_id,
                                result,
                            )
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("response_watcher_failed account=%s error=%s", runtime.cfg.account_id, exc)
                await asyncio.sleep(min(1.0, interval * 10))

    async def _process_pending_response(
        self,
        runtime: AccountRuntime,
        request_id: str,
        item: Dict[str, Any],
    ) -> None:
        response = await runtime.helper.consume_response(request_id)
        if response:
            delivered = await self._emit_async_response(runtime, request_id, item, response)
            if delivered:
                await runtime.helper.ack_response(request_id)
                async with runtime.pending_lock:
                    runtime.pending_responses.pop(request_id, None)
            return
        now_ts = now()
        if now_ts < safe_float(item.get("deadline_at"), now_ts + 1):
            return
        if item.get("timeout_notified"):
            async with runtime.pending_lock:
                runtime.pending_responses.pop(request_id, None)
            return
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        msg = self._async_response_message(runtime, payload, {}, "SUBMIT_UNKNOWN")
        msg["code"] = "HELPER_RESPONSE_TIMEOUT"
        msg["reject_reason"] = "helper response not observed; do not retry automatically"
        delivered = await self.broadcast_confirmed(
            runtime,
            msg,
            "response:%s:SUBMIT_UNKNOWN" % request_id,
        )
        if delivered:
            await run_blocking(
                runtime.correlation.update_stage,
                runtime.cfg.account_id,
                safe_str(payload.get("client_order_id")),
                "SUBMIT_UNKNOWN",
            )
            item["timeout_notified"] = True
            item["deadline_at"] = now_ts + 300.0

    def _async_response_message(
        self,
        runtime: AccountRuntime,
        payload: Dict[str, Any],
        data: Dict[str, Any],
        stage: str,
    ) -> Dict[str, Any]:
        return {
            "protocol_version": 2,
            "type": "ASYNC_ORDER_RESPONSE",
            "stage": stage,
            "submit_result": "UNKNOWN" if not data.get("order_id") else "KNOWN",
            "trace_id": safe_str(payload.get("trace_id")),
            "client_order_id": safe_str(payload.get("client_order_id")),
            "msg_id": safe_str(payload.get("msg_id")),
            "request_id": safe_str(payload.get("request_id")),
            "qmt_user_order_id": safe_str(payload.get("qmt_user_order_id") or data.get("qmt_user_order_id")),
            "account_id": runtime.cfg.account_id,
            "order_id": safe_str(data.get("order_id")),
            "queue_wait_ms": safe_float(data.get("queue_wait_ms"), 0.0),
            "passorder_elapsed_ms": safe_float(data.get("passorder_elapsed_ms"), 0.0),
            "passorder_return": data.get("passorder_return"),
            "passorder_started_at_ns": safe_int(data.get("passorder_started_at_ns"), 0),
            "passorder_finished_at_ns": safe_int(data.get("passorder_finished_at_ns"), 0),
            "gateway_received_at_ns": safe_int(payload.get("gateway_received_at_ns"), 0),
            "gateway_response_at_ns": time.time_ns(),
            "timestamp": now(),
        }

    async def _emit_async_response(
        self,
        runtime: AccountRuntime,
        request_id: str,
        item: Dict[str, Any],
        response: Dict[str, Any],
    ) -> bool:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        submit_status = safe_str(data.get("status")).lower()
        failed = response.get("ok") is False or submit_status == "failed"
        stage = (
            "REJECTED" if failed
            else "SUBMIT_UNKNOWN" if submit_status == "submit_unknown"
            else "QMT_SUBMITTED"
        )
        msg = self._async_response_message(runtime, payload, data, stage)
        if safe_str(data.get("order_id") or data.get("order_sysid")):
            runtime.remember_order_side_intent(payload, data)
        if failed:
            msg["code"] = safe_str(response.get("code") or data.get("code") or "QMT_ERROR")
            msg["reject_reason"] = safe_str(response.get("error") or data.get("error"))
        elif stage == "SUBMIT_UNKNOWN":
            msg["code"] = "QMT_SUBMIT_RESULT_UNKNOWN"
            msg["reject_reason"] = "QMT returned no stable order id; do not retry automatically"
        delivered = await self.broadcast_confirmed(
            runtime,
            msg,
            "response:%s:%s" % (request_id, stage),
        )
        if delivered:
            await run_blocking(
                lambda: runtime.correlation.update_stage(
                    runtime.cfg.account_id, safe_str(payload.get("client_order_id")), stage,
                    order_id=safe_str(data.get("order_id")),
                )
            )
        return delivered

    async def live_event_watcher_loop(self, runtime: AccountRuntime) -> None:
        interval = max(0.005, runtime.cfg.event_watch_interval_seconds)
        while self.running:
            try:
                async with runtime.clients_lock:
                    has_client = runtime.primary is not None and not runtime.primary.closed
                if not has_client:
                    await asyncio.sleep(interval)
                    continue
                events = await runtime.helper.read_events(100)
                if events:
                    await asyncio.gather(*(
                        self._deliver_live_event(runtime, event) for event in events
                    ), return_exceptions=True)
                await asyncio.sleep(interval if not events else 0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("live_event_watcher_failed account=%s error=%s", runtime.cfg.account_id, exc)
                await asyncio.sleep(min(1.0, interval * 10))

    async def _deliver_live_event(self, runtime: AccountRuntime, event: Dict[str, Any]) -> None:
        try:
            await self.emit_event(runtime, event)
            await runtime.helper.ack_event(event)
        except Exception as exc:
            exhausted = await runtime.helper.retry_event(event)
            self.logger.warning(
                "live_event_delivery_failed account=%s event_id=%s error=%s",
                runtime.cfg.account_id, event.get("event_id"), exc,
            )
            if exhausted:
                runtime.qmt_status = {
                    **runtime.qmt_status,
                    "state": "delivery_degraded",
                    "ready": False,
                    "last_error": "live event delivery exhausted; reconcile required",
                    "updated_at": now(),
                }
                await self.broadcast(runtime, {
                    "protocol_version": 2,
                    "type": "RECONCILE_REQUIRED",
                    "account_id": runtime.cfg.account_id,
                    "reason": "live_event_delivery_exhausted",
                    "event_id": safe_str(event.get("event_id")),
                    "timestamp": now(),
                })

    async def maintenance_loop(self, runtime: AccountRuntime) -> None:
        interval = max(10.0, runtime.cfg.maintenance_interval_seconds)
        while self.running:
            await asyncio.sleep(interval)
            try:
                cutoff = now() - 86400.0
                await run_blocking(self._cleanup_runtime_files, runtime, cutoff)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("maintenance_failed account=%s error=%s", runtime.cfg.account_id, exc)

    @staticmethod
    def _cleanup_runtime_files(runtime: AccountRuntime, cutoff: float) -> None:
        pending = set(runtime.pending_responses.keys())
        remaining = 200
        for folder in (runtime.helper.responses, runtime.helper.events_failed, runtime.helper.done):
            entries = None
            try:
                entries = os.scandir(str(folder))
            except OSError:
                continue
            try:
                scanned = 0
                for entry in entries:
                    scanned += 1
                    if scanned > max(64, remaining * 4):
                        break
                    if not entry.name.endswith(".json") or Path(entry.name).stem in pending:
                        continue
                    try:
                        if entry.is_file() and entry.stat().st_mtime < cutoff:
                            Path(entry.path).unlink()
                            remaining -= 1
                            if remaining <= 0:
                                return
                    except OSError:
                        pass
            finally:
                try:
                    entries.close()
                except OSError:
                    pass

    async def poll_account_loop(self, runtime: AccountRuntime) -> None:
        while self.running:
            try:
                health = await runtime.helper.health()
                runtime.update_status_ready(health)
                if not health.get("ready"):
                    await self.broadcast_status(runtime)
                    await asyncio.sleep(runtime.next_poll_delay(False))
                    continue
                async with runtime.clients_lock:
                    has_client = runtime.primary is not None and not runtime.primary.closed
                if not has_client:
                    await asyncio.sleep(runtime.next_poll_delay(False))
                    continue
                snapshot = await runtime.helper.latest_snapshot()
                if snapshot:
                    await self.emit_snapshot_diffs(runtime, snapshot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                runtime.update_status_offline(exc)
                await self.broadcast_status(runtime)
                await asyncio.sleep(runtime.next_poll_delay(True))
                continue
            await asyncio.sleep(runtime.next_poll_delay(False))

    async def broadcast_status(self, runtime: AccountRuntime) -> None:
        await self.broadcast(runtime, {
            "type": "QMT_STATUS",
            "account_id": runtime.cfg.account_id,
            "account_type": runtime.cfg.account_type,
            "qmt_status": runtime.qmt_status,
            "status": runtime.qmt_status.get("state"),
            "timestamp": now(),
        })

    async def emit_event(self, runtime: AccountRuntime, event: Dict[str, Any]) -> bool:
        event_type = safe_str(event.get("type"), "")
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        if not event_type:
            return True
        event_id = safe_str(event.get("event_id"))
        if event_id:
            if event_id in runtime.seen_event_ids or await run_blocking(runtime.correlation.event_seen, runtime.cfg.account_id, event_id):
                return True
        trade_key = ""
        order_version = ""
        if event_type == "TRADE_NOTIFY":
            trade_key = self._trade_key(data)
            if not event_id and trade_key in runtime.seen_trade_keys:
                return True
        elif event_type == "ORDER_UPDATE":
            order_version = self._order_version_key(data)
            if not event_id and order_version in runtime.seen_order_versions:
                return True
        correlation = runtime.resolve_order_correlation(data)
        if correlation is None:
            correlation = await run_blocking(
                runtime.correlation.resolve, runtime.cfg.account_id, data,
            )
            if correlation:
                runtime.remember_order_correlation(correlation)
        if correlation:
            data = runtime._apply_persisted_order_correlation(dict(data), correlation)
            stage = self._stage_for_event(event_type, data)
            if stage:
                await run_blocking(
                    lambda: runtime.correlation.update_stage(
                        runtime.cfg.account_id, safe_str(correlation.get("client_order_id")), stage,
                        order_id=safe_str(data.get("order_id")),
                        order_sysid=safe_str(data.get("order_sysid")),
                    )
                )
            runtime.remember_order_correlation({
                **correlation,
                "order_id": safe_str(data.get("order_id") or correlation.get("order_id")),
                "order_sysid": safe_str(data.get("order_sysid") or correlation.get("order_sysid")),
            })
            data["correlation"] = "resolved"
        elif event_type in ("ORDER_UPDATE", "TRADE_NOTIFY", "ORDER_ERROR"):
            data = dict(data)
            data["correlation"] = "unresolved"
            data["correlation_reason"] = "missing_mapping"
        msg = {
            "protocol_version": 2,
            "type": event_type,
            "event_id": event_id,
            "event_seq": safe_int(event.get("event_seq"), 0),
            "account_id": runtime.cfg.account_id,
            "account_type": runtime.cfg.account_type,
            "timestamp": event.get("created_at") or now(),
            "source_ts_ns": safe_int(event.get("source_ts_ns"), 0),
            "gateway_ts_ns": int(now() * 1000000000),
            "source": safe_str(event.get("source")),
        }
        if event_type == "POSITIONS_SNAPSHOT":
            msg["positions"] = data.get("positions") or []
            msg["asset"] = data.get("asset") or {}
        else:
            if event_type in ("ORDER_UPDATE", "TRADE_NOTIFY"):
                msg.update(normalize_standard_order_payload(runtime.apply_order_side_intent(data)))
            else:
                msg.update(data)
        delivered = await self.broadcast_confirmed(
            runtime,
            msg,
            "event:%s" % (event_id or stable_hash(msg)),
        )
        if not delivered:
            raise ConnectionError("live event has no active TCP recipient")
        if event_id:
            runtime.seen_event_ids.add(event_id)
            await run_blocking(runtime.correlation.mark_event, runtime.cfg.account_id, event_id)
        if trade_key:
            runtime.seen_trade_keys.add(trade_key)
        if order_version:
            runtime.seen_order_versions.add(order_version)
        if len(runtime.seen_event_ids) > 20000:
            # event_id dedupe is persisted in SQLite; the set is only a hot cache.
            runtime.seen_event_ids.clear()
        return True

    @staticmethod
    def _trade_key(trade: Dict[str, Any]) -> str:
        return "|".join([
            safe_str(trade.get("trade_date") or trade.get("trading_day")),
            safe_str(trade.get("trade_id") or trade.get("traded_id")),
            safe_str(trade.get("order_id")),
            safe_str(trade.get("symbol") or trade.get("stock_code")),
            safe_str(trade.get("quantity") or trade.get("traded_volume")),
            safe_str(trade.get("price") or trade.get("traded_price")),
            safe_str(trade.get("traded_time") or trade.get("trade_time")),
        ])

    @staticmethod
    def _order_version_key(order: Dict[str, Any]) -> str:
        identity = safe_str(
            order.get("order_id") or order.get("order_sysid") or
            order.get("qmt_user_order_id") or order.get("client_order_id")
        )
        return "|".join([
            identity,
            safe_str(order.get("order_status") or order.get("status")),
            safe_str(order.get("traded_volume") or order.get("filled_qty")),
            safe_str(order.get("update_seq")),
            safe_str(order.get("update_time") or order.get("order_time")),
        ])

    @staticmethod
    def _stage_for_event(event_type: str, data: Dict[str, Any]) -> str:
        if event_type == "TRADE_NOTIFY":
            return "PARTIAL"
        if event_type == "ORDER_ERROR":
            return "REJECTED"
        status = safe_str(data.get("order_status") or data.get("status"))
        return {
            "48": "QMT_ORDER_CREATED",
            "49": "QMT_ORDER_CREATED",
            "50": "BROKER_ACCEPTED",
            "51": "BROKER_ACCEPTED",
            "52": "PARTIAL",
            "55": "PARTIAL",
            "53": "CANCELLED",
            "54": "CANCELLED",
            "56": "FILLED",
            "57": "REJECTED",
            "58": "",
        }.get(status, "QMT_ORDER_CREATED")

    async def emit_snapshot_diffs(self, runtime: AccountRuntime, snapshot: Dict[str, Any]) -> None:
        asset = snapshot.get("asset") or {}
        positions = snapshot.get("positions") or []
        orders = await normalize_standard_orders_async(snapshot.get("orders") or [], runtime)
        trades = await normalize_standard_trades_async(
            snapshot.get("trades") or [], orders, runtime,
        )

        asset_hash = stable_hash(asset)
        if asset and asset_hash != runtime.last_asset_hash:
            runtime.last_asset_hash = asset_hash
            await self.broadcast(runtime, {
                "type": "ASSET_UPDATE",
                "account_id": runtime.cfg.account_id,
                "account_type": runtime.cfg.account_type,
                "cash": asset.get("available_cash", asset.get("cash", 0)),
                "frozen_cash": asset.get("frozen_cash", 0),
                "market_value": asset.get("market_value", 0),
                "total_asset": asset.get("total_asset", 0),
                "asset": asset,
                "timestamp": now(),
            })

        positions_hash = stable_hash(sorted(positions, key=lambda item: safe_str(item.get("stock_code") or item.get("symbol"))))
        if positions_hash != runtime.last_positions_hash:
            runtime.last_positions_hash = positions_hash
            await self.broadcast(runtime, {
                "type": "POSITIONS_SNAPSHOT",
                "account_id": runtime.cfg.account_id,
                "account_type": runtime.cfg.account_type,
                "positions": positions,
                "asset": asset,
                "timestamp": now(),
            })

        orders_hash = stable_hash(sorted(orders, key=lambda item: safe_str(item.get("order_id") or item.get("order_sysid"))))
        if not runtime.snapshot_baseline_ready:
            runtime.last_orders_hash = orders_hash
            for order in orders:
                runtime.seen_order_versions.add(self._order_version_key(order))
            for trade in trades:
                runtime.seen_trade_keys.add(self._trade_key(trade))
            runtime.snapshot_baseline_ready = True
            return
        if orders_hash != runtime.last_orders_hash:
            runtime.last_orders_hash = orders_hash
            for order in orders:
                version_key = self._order_version_key(order)
                if version_key in runtime.seen_order_versions:
                    continue
                runtime.seen_order_versions.add(version_key)
                msg = {
                    "type": "ORDER_UPDATE",
                    "account_id": runtime.cfg.account_id,
                    "account_type": runtime.cfg.account_type,
                    "timestamp": now(),
                }
                msg.update(order)
                await self.broadcast(runtime, msg)

        for trade in trades:
            key = self._trade_key(trade)
            if key in runtime.seen_trade_keys:
                continue
            runtime.seen_trade_keys.add(key)
            msg = {
                "type": "TRADE_NOTIFY",
                "account_id": runtime.cfg.account_id,
                "account_type": runtime.cfg.account_type,
                "timestamp": now(),
            }
            msg.update(trade)
            await self.broadcast(runtime, msg)


def load_config(path: Path) -> GatewayConfig:
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    allowed_top_level = {
        "auth_token_sha256", "listen_backlog", "max_frame_bytes",
        "tcp_idle_timeout_seconds", "response_watch_interval_seconds",
        "event_watch_interval_seconds", "maintenance_interval_seconds",
        "query_concurrency", "expected_helper_build_id",
        "expected_protocol_version", "expected_command_interval_ms", "accounts",
    }
    unknown_top_level = sorted(set(raw) - allowed_top_level)
    if unknown_top_level:
        raise ValueError("unknown generated gateway config keys: %s" % ", ".join(unknown_top_level))
    fixed_values = {
        "listen_backlog": 16,
        "max_frame_bytes": DEFAULT_MAX_FRAME_BYTES,
        "tcp_idle_timeout_seconds": 60.0,
        "response_watch_interval_seconds": 0.01,
        "event_watch_interval_seconds": 0.01,
        "maintenance_interval_seconds": 60.0,
        "query_concurrency": 1,
        "expected_helper_build_id": EXPECTED_LOCAL_HELPER_BUILD_ID,
        "expected_protocol_version": 2,
        "expected_command_interval_ms": 50,
    }
    for key, expected in fixed_values.items():
        if raw.get(key) != expected:
            raise ValueError("%s must remain %r in the generated local config" % (key, expected))
    auth_token_sha256 = safe_str(raw.get("auth_token_sha256") or "").strip().lower()
    if len(auth_token_sha256) != 64 or any(
        char not in "0123456789abcdef" for char in auth_token_sha256
    ):
        raise ValueError("auth_token_sha256 must be a 64-character SHA-256 digest")
    accounts = []
    for item in raw.get("accounts", []):
        allowed_account = {
            "name", "account_id", "account_type", "tcp_host", "tcp_port",
            "runtime_dir", "poll_interval_seconds", "request_timeout_seconds",
            "query_timeout_seconds", "trade_enqueue_timeout_seconds",
            "heartbeat_stale_seconds",
        }
        unknown_account = sorted(set(item) - allowed_account)
        if unknown_account:
            raise ValueError("unknown generated account config keys: %s" % ", ".join(unknown_account))
        name = safe_str(item.get("name") or "").strip()
        account_id = safe_str(item.get("account_id") or "").strip()
        account_type = safe_str(item.get("account_type") or "").strip().upper()
        tcp_port = safe_int(item.get("tcp_port"), 0)
        runtime_dir = safe_str(item.get("runtime_dir") or "")
        if not name or not account_id:
            raise ValueError("account name and account_id are required")
        if account_type not in ("STOCK", "CREDIT"):
            raise ValueError("account_type must be STOCK or CREDIT")
        if not 1 <= tcp_port <= 65535:
            raise ValueError("tcp_port must be in 1..65535")
        if not runtime_dir:
            raise ValueError("runtime_dir is required and must come from the project .env")
        drive, tail = ntpath.splitdrive(runtime_dir.replace("/", "\\"))
        if runtime_dir.startswith(("\\\\", "//")) or len(drive) != 2 or not tail.startswith("\\"):
            raise ValueError("runtime_dir must be an absolute local drive path")
        if any(character in tail for character in '<>:"|?*'):
            raise ValueError("runtime_dir contains a Windows-forbidden path character")
        tcp_host = safe_str(item.get("tcp_host") or item.get("host") or "")
        if tcp_host != "127.0.0.1":
            raise ValueError("the one-machine gateway must bind exactly 127.0.0.1")
        accounts.append(AccountConfig(
            name=name,
            account_id=account_id,
            account_type=account_type,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            runtime_dir=runtime_dir,
            poll_interval_seconds=safe_float(item.get("poll_interval_seconds"), 1.0),
            request_timeout_seconds=safe_float(item.get("request_timeout_seconds"), 8.0),
            query_timeout_seconds=safe_float(item.get("query_timeout_seconds"), 6.0),
            trade_enqueue_timeout_seconds=safe_float(item.get("trade_enqueue_timeout_seconds"), 1.0),
            heartbeat_stale_seconds=safe_float(item.get("heartbeat_stale_seconds"), 15.0),
            response_watch_interval_seconds=safe_float(item.get("response_watch_interval_seconds", raw.get("response_watch_interval_seconds")), 0.01),
            event_watch_interval_seconds=safe_float(item.get("event_watch_interval_seconds", raw.get("event_watch_interval_seconds")), 0.01),
            maintenance_interval_seconds=safe_float(item.get("maintenance_interval_seconds", raw.get("maintenance_interval_seconds")), 60.0),
            query_concurrency=max(1, safe_int(item.get("query_concurrency", raw.get("query_concurrency")), 1)),
            expected_helper_build_id=safe_str(
                item.get("expected_helper_build_id", raw.get("expected_helper_build_id")), "",
            ),
            expected_protocol_version=safe_int(
                item.get("expected_protocol_version", raw.get("expected_protocol_version")), 0,
            ),
            expected_command_interval_ms=safe_int(
                item.get("expected_command_interval_ms", raw.get("expected_command_interval_ms")), 0,
            ),
        ))
        account_fixed = {
            "poll_interval_seconds": 30.0,
            "request_timeout_seconds": 8.0,
            "query_timeout_seconds": 6.0,
            "trade_enqueue_timeout_seconds": 1.0,
            "heartbeat_stale_seconds": 2.5,
        }
        for key, expected in account_fixed.items():
            if item.get(key) != expected:
                raise ValueError("%s must remain %r in the generated local config" % (key, expected))

    if len(accounts) != 1:
        raise ValueError("the one-machine gateway requires exactly one account")
    return GatewayConfig(
        auth_token_sha256=auth_token_sha256,
        listen_backlog=safe_int(raw.get("listen_backlog"), 16),
        max_frame_bytes=safe_int(raw.get("max_frame_bytes"), DEFAULT_MAX_FRAME_BYTES),
        tcp_idle_timeout_seconds=safe_float(raw.get("tcp_idle_timeout_seconds"), 60.0),
        accounts=accounts,
    )


def setup_logging(log_dir: Optional[str]) -> logging.Logger:
    logger = logging.getLogger("bigqmt_gateway_proxy")
    logger.setLevel(logging.INFO)
    logger.handlers[:] = []
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    if log_dir:
        path = Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(path / "bigqmt_gateway_proxy.log"), encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Big QMT file-queue gateway proxy")
    parser.add_argument("--config", required=True, help="generated by tools/project_env.py from the root .env")
    parser.add_argument("--log-dir", required=True, help="resolved from QMT_LOCAL_LOG_DIR")
    return parser.parse_args(argv)


async def async_main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logger = setup_logging(args.log_dir)
    cfg = load_config(Path(args.config))
    proxy = BigQmtGatewayProxy(cfg, logger)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def request_stop() -> None:
        stop_event.set()

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, request_stop)
        except (NotImplementedError, RuntimeError):
            pass

    await proxy.start()
    try:
        await stop_event.wait()
    finally:
        await proxy.stop()
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    try:
        return asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
