#!/usr/bin/env python3
"""Completely offline benchmark for the local Big QMT file-queue bridge.

The runner deliberately does not start the TCP Gateway and never resolves a
real QMT symbol.  It reuses the production ``FileQueueHelperClient`` and
``drain_file_requests`` implementation plus the production SQLite WAL durable
effect registry inside a system TemporaryDirectory, with ``passorder``
replaced by an in-process probe.
"""

from __future__ import annotations

import argparse
from collections import Counter
import importlib.util
import json
import logging
import math
import os
from pathlib import Path
import random
import statistics
import sys
import tempfile
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


SCHEMA_VERSION = 2
PROJECT_ROOT = Path(__file__).resolve().parents[1]
_CORE_CACHE: Optional[Tuple[Any, Any]] = None


class BenchmarkInvariantError(RuntimeError):
    """The offline harness observed a bridge safety-contract violation."""


def _unique_path(filename: str, *, parent_name: str = "") -> Path:
    matches = [
        path
        for path in PROJECT_ROOT.rglob(filename)
        if not parent_name or path.parent.name == parent_name
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one %s under project root, found %d"
            % (filename, len(matches))
        )
    return matches[0]


def _load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load module from %s" % path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_core_modules() -> Tuple[Any, Any]:
    """Load production Gateway and Helper modules without starting services."""
    global _CORE_CACHE
    if _CORE_CACHE is not None:
        return _CORE_CACHE
    gateway_path = _unique_path("bigqmt_gateway_proxy.py")
    helper_path = _unique_path("bigqmt_file_queue_helper.py", parent_name="src")
    gateway_dir = str(gateway_path.parent)
    inserted = gateway_dir not in sys.path
    previous_dont_write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    if inserted:
        sys.path.insert(0, gateway_dir)
    try:
        gateway = _load_module("_qmt_offline_benchmark_gateway", gateway_path)
        helper = _load_module("_qmt_offline_benchmark_helper", helper_path)
    finally:
        sys.dont_write_bytecode = previous_dont_write_bytecode
        if inserted:
            try:
                sys.path.remove(gateway_dir)
            except ValueError:
                pass
    _CORE_CACHE = gateway, helper
    return _CORE_CACHE


def _bind_helper_root(helper: Any, root: Path) -> None:
    """Redirect every Helper path and QMT side effect into one temp root."""
    helper.RUNTIME_DIR = str(root)
    helper.INBOX_DIR = os.path.join(helper.RUNTIME_DIR, "inbox")
    helper.INBOX_COMMANDS_DIR = os.path.join(helper.INBOX_DIR, "commands")
    helper.INBOX_QUERIES_DIR = os.path.join(helper.INBOX_DIR, "queries")
    helper.PROCESSING_DIR = os.path.join(helper.RUNTIME_DIR, "processing")
    helper.PROCESSING_COMMANDS_DIR = os.path.join(helper.PROCESSING_DIR, "commands")
    helper.PROCESSING_QUERIES_DIR = os.path.join(helper.PROCESSING_DIR, "queries")
    helper.RESPONSES_DIR = os.path.join(helper.RUNTIME_DIR, "responses")
    helper.REQUEST_STATE_DIR = os.path.join(helper.RUNTIME_DIR, "request_state")
    helper.EVENTS_DIR = os.path.join(helper.RUNTIME_DIR, "events")
    helper.EVENTS_LIVE_DIR = os.path.join(helper.EVENTS_DIR, "live")
    helper.EVENTS_FAILED_DIR = os.path.join(helper.EVENTS_DIR, "failed")
    helper.EVENTS_DONE_DIR = os.path.join(helper.EVENTS_DIR, "done")
    helper.SNAPSHOTS_DIR = os.path.join(helper.RUNTIME_DIR, "snapshots")
    helper.ARCHIVE_DIR = os.path.join(helper.RUNTIME_DIR, "archive")
    helper.DONE_DIR = os.path.join(helper.ARCHIVE_DIR, "done")
    helper.FAILED_DIR = os.path.join(helper.ARCHIVE_DIR, "failed")
    helper.STATE_FILE = os.path.join(helper.RUNTIME_DIR, "state.json")
    helper.HEARTBEAT_FILE = os.path.join(helper.RUNTIME_DIR, "heartbeat.json")
    helper.METRICS_FILE = os.path.join(helper.RUNTIME_DIR, "metrics.json")
    helper.READINESS_FILE = os.path.join(helper.RUNTIME_DIR, "readiness.json")
    helper.ACCOUNT_ID = "OFFLINE_BENCH_ACCOUNT"
    helper.ACCOUNT_NAME = "offline_bench"
    helper.ACCOUNT_TYPE = "STOCK"
    helper.ENABLE_TRADING = True
    helper.ENABLE_CANCEL_ORDER = False
    helper.G_PROCESSING_REQUEST_IDS = set()
    helper.ensure_runtime_dirs()


def _percentile(sorted_values: Sequence[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    position = (len(sorted_values) - 1) * fraction
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return float(sorted_values[low])
    weight = position - low
    return float(sorted_values[low] + (sorted_values[high] - sorted_values[low]) * weight)


def metric_summary(values: Iterable[float]) -> Dict[str, Any]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {
            "count": 0,
            "mean": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }
    return {
        "count": len(ordered),
        "mean": statistics.fmean(ordered),
        "p50": _percentile(ordered, 0.50),
        "p95": _percentile(ordered, 0.95),
        "p99": _percentile(ordered, 0.99),
        "max": ordered[-1],
    }


def _payload(rng: random.Random, repeat: int, index: int, phase: str) -> Dict[str, Any]:
    request_id = "offline-%s-r%03d-i%06d" % (phase, repeat, index)
    side = rng.choice(("BUY", "SELL"))
    return {
        "protocol_version": 2,
        "request_id": request_id,
        "msg_id": request_id,
        "trace_id": request_id,
        "client_order_id": request_id,
        "qmt_user_order_id": "OB%03d%06d" % (repeat, index),
        "account_id": "OFFLINE_BENCH_ACCOUNT",
        "account_type": "STOCK",
        "symbol": rng.choice(("600000.SH", "000001.SZ", "159915.SZ")),
        "side": side,
        "quantity": rng.choice((100, 200, 300)),
        "price": round(rng.uniform(5.0, 50.0), 2),
        "price_type": 11,
        "order_type": 23 if side == "BUY" else 24,
        "strategy_name": "offline_benchmark",
        "order_remark": "mock_only",
        "created_at_ns": time.time_ns(),
    }


def _gateway_enqueue_sequence(repeat: int, index: int, phase: str) -> int:
    """Return a deterministic, positive and phase-ordered queue sequence."""
    phase_rank = {
        "warmup": 1,
        "measured": 2,
        "probe": 3,
        "fault": 4,
    }.get(phase, 9)
    return (repeat + 1) * 100_000_000 + phase_rank * 10_000_000 + index + 1


def _prepare_effect_payload(
    gateway: Any,
    payload: Dict[str, Any],
    repeat: int,
    index: int,
    phase: str,
) -> Dict[str, Any]:
    """Attach the exact canonical identities used by the production Gateway."""
    prepared = dict(payload)
    prepared["gateway_enqueue_seq"] = _gateway_enqueue_sequence(
        repeat, index, phase
    )
    prepared["intent_hash"] = gateway.BigQmtGatewayProxy.order_intent_key(
        None, prepared
    )
    effect_kind, fingerprint = gateway.durable_effect_identity("order", prepared)
    _require(effect_kind == "order", "production effect identity changed kind")
    prepared["gateway_effect_fingerprint"] = fingerprint
    return prepared


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise BenchmarkInvariantError(message)


def _run_normal_sample(
    gateway: Any,
    helper: Any,
    queue_client: Any,
    effect_store: Any,
    payload: Dict[str, Any],
    repeat: int,
    index: int,
    phase: str,
    call_counts: Counter,
) -> Dict[str, Any]:
    request_id = str(payload["request_id"])
    account_id = str(payload["account_id"])
    intent_hash = gateway.BigQmtGatewayProxy.order_intent_key(None, payload)
    effect_kind, fingerprint = gateway.durable_effect_identity("order", payload)
    _require(payload.get("intent_hash") == intent_hash, "intent hash is not canonical")
    _require(
        payload.get("gateway_effect_fingerprint") == fingerprint,
        "effect fingerprint is not canonical",
    )

    durable_started_ns = time.perf_counter_ns()
    reserve_started_ns = durable_started_ns
    prepared_record, reserve_duplicate = effect_store.reserve_effect_request(
        account_id, request_id, effect_kind, fingerprint
    )
    reserved_ns = time.perf_counter_ns()
    _require(not reserve_duplicate, "new effect request was reported as duplicate")
    _require(prepared_record.get("state") == "PREPARED", "effect was not PREPARED")
    _require(prepared_record.get("result") is None, "PREPARED effect has a result")

    dispatch_started_ns = time.perf_counter_ns()
    dispatched = effect_store.transition_effect_request(
        account_id,
        request_id,
        fingerprint,
        "DISPATCHING",
        allowed_from=("PREPARED",),
    )
    dispatched_ns = time.perf_counter_ns()
    _require(dispatched, "effect did not transition to DISPATCHING")

    # These original timestamps intentionally surround only the file-queue
    # path.  Registry overhead has separate metrics below.
    started_ns = time.perf_counter_ns()
    enqueue_result = queue_client.enqueue_action("place_order", payload, 1.0)
    enqueued_ns = time.perf_counter_ns()
    request_path = Path(str(enqueue_result.get("request_path") or ""))
    _require(request_path.is_file(), "Gateway did not create the command file")

    processed = helper.drain_file_requests(
        None, 1, "command", helper.COMMAND_BUDGET_MS
    )
    drained_ns = time.perf_counter_ns()
    _require(processed == 1, "Helper did not process exactly one command")

    response = queue_client.consume_response_sync(request_id)
    response_read_ns = time.perf_counter_ns()
    response_path = Path(helper._response_path(request_id))
    response_present = response_path.is_file()
    final_guard = helper._read_request_guard(request_id)
    _require(response_present, "Helper response is missing before ACK")
    _require(isinstance(final_guard, dict), "final request guard is missing")
    _require(final_guard.get("state") == "submitted", "final request guard is not submitted")
    _require(call_counts[request_id] == 1, "mock passorder call count is not exactly one")

    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    _require(response.get("ok") is True, "Helper returned a failed normal response")
    _require(data.get("status") == "accepted", "mock passorder was not accepted")

    duplicate = queue_client.enqueue_action("place_order", payload, 1.0)
    _require(duplicate.get("idempotent") is True, "response-layer duplicate was not deduplicated")
    _require(call_counts[request_id] == 1, "response-layer duplicate retried passorder")
    queue_client.ack_response_sync(request_id)
    acked_ns = time.perf_counter_ns()

    effect_result = {
        "ok": True,
        "request_id": request_id,
        "stage": "BRIDGE_QUEUED",
        "status": "accepted",
    }
    commit_started_ns = time.perf_counter_ns()
    committed = effect_store.transition_effect_request(
        account_id,
        request_id,
        fingerprint,
        "ENQUEUED",
        result=effect_result,
        allowed_from=("DISPATCHING",),
    )
    committed_ns = time.perf_counter_ns()
    _require(committed, "effect did not transition to ENQUEUED")
    final_effect_record = effect_store.get_effect_request(account_id, request_id)
    _require(isinstance(final_effect_record, dict), "final effect record is missing")

    replay_started_ns = time.perf_counter_ns()
    replay_record, replay_duplicate = effect_store.reserve_effect_request(
        account_id, request_id, effect_kind, fingerprint
    )
    replayed_ns = time.perf_counter_ns()
    effect_record_valid = bool(
        final_effect_record.get("kind") == effect_kind
        and final_effect_record.get("fingerprint") == fingerprint
        and final_effect_record.get("state") == "ENQUEUED"
        and final_effect_record.get("result") == effect_result
        and replay_record.get("state") == "ENQUEUED"
        and replay_record.get("result") == effect_result
    )
    _require(replay_duplicate, "effect registry replay was not deduplicated")
    _require(effect_record_valid, "effect registry did not preserve state/result")

    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "sample",
        "phase": phase,
        "repeat": repeat,
        "index": index,
        "request_id": request_id,
        "client_order_id": str(payload["client_order_id"]),
        "intent_hash": intent_hash,
        "gateway_effect_fingerprint": fingerprint,
        "effect_kind": effect_kind,
        "effect_state": str(final_effect_record.get("state") or ""),
        "effect_result_stage": str(
            (final_effect_record.get("result") or {}).get("stage") or ""
        ),
        "effect_registry_duplicate_replayed": replay_duplicate,
        "effect_registry_record_valid": effect_record_valid,
        "symbol": str(payload["symbol"]),
        "side": str(payload["side"]),
        "quantity": int(payload["quantity"]),
        "price": float(payload["price"]),
        "ok": True,
        "error": "",
        "enqueue_ms": (enqueued_ns - started_ns) / 1_000_000.0,
        "helper_drain_ms": (drained_ns - enqueued_ns) / 1_000_000.0,
        "response_read_ms": (response_read_ns - drained_ns) / 1_000_000.0,
        "ack_and_invariant_ms": (acked_ns - response_read_ns) / 1_000_000.0,
        "total_to_response_ms": (response_read_ns - started_ns) / 1_000_000.0,
        "queue_wait_ms": float(data.get("queue_wait_ms") or 0.0),
        "mock_passorder_elapsed_ms": float(data.get("passorder_elapsed_ms") or 0.0),
        "effect_reserve_ms": (reserved_ns - reserve_started_ns) / 1_000_000.0,
        "effect_dispatch_barrier_ms": (
            dispatched_ns - dispatch_started_ns
        ) / 1_000_000.0,
        "effect_result_commit_ms": (
            committed_ns - commit_started_ns
        ) / 1_000_000.0,
        "effect_replay_probe_ms": (
            replayed_ns - replay_started_ns
        ) / 1_000_000.0,
        "durable_total_to_final_state_ms": (
            committed_ns - durable_started_ns
        ) / 1_000_000.0,
        "guard_probe_in_timed_sample": False,
        "final_guard_state": str(final_guard.get("state") or ""),
        "response_present_before_ack": response_present,
        "mock_passorder_call_count": call_counts[request_id],
    }


def _success_guard_probe(
    gateway: Any,
    helper: Any,
    queue_client: Any,
    effect_store: Any,
    repeat: int,
    seed: int,
) -> Dict[str, Any]:
    """Prove the success guard contract outside all measured samples."""
    request_id = "offline-success-probe-r%03d" % repeat
    payload = _payload(random.Random(seed ^ 0x51CCE55), repeat, 9_999_998, "probe")
    payload["request_id"] = request_id
    payload["msg_id"] = request_id
    payload["trace_id"] = request_id
    payload["client_order_id"] = request_id
    payload = _prepare_effect_payload(
        gateway, payload, repeat, 9_999_998, "probe"
    )
    account_id = str(payload["account_id"])
    effect_kind, fingerprint = gateway.durable_effect_identity("order", payload)
    prepared_record, reserve_duplicate = effect_store.reserve_effect_request(
        account_id, request_id, effect_kind, fingerprint
    )
    effect_dispatched = effect_store.transition_effect_request(
        account_id,
        request_id,
        fingerprint,
        "DISPATCHING",
        allowed_from=("PREPARED",),
    )
    calls = Counter()
    guard_visible = {"value": False}

    def probing_passorder(args: Dict[str, Any], context: Any) -> str:
        del context
        current_id = str(args.get("request_id") or "")
        calls[current_id] += 1
        guard = helper._read_request_guard(current_id)
        guard_visible["value"] = bool(
            isinstance(guard, dict) and guard.get("state") == "processing"
        )
        return "SIM-PROBE-" + current_id

    helper.call_passorder = probing_passorder
    queue_client.enqueue_action("place_order", payload, 1.0)
    first_drain = helper.drain_file_requests(
        None, 1, "command", helper.COMMAND_BUDGET_MS
    )
    response_path = Path(helper._response_path(request_id))
    response_present_before_ack = response_path.is_file()
    guard = helper._read_request_guard(request_id)
    response = queue_client.consume_response_sync(request_id)
    effect_result = {
        "ok": True,
        "request_id": request_id,
        "stage": "BRIDGE_QUEUED",
        "status": "accepted",
    }
    effect_committed = effect_store.transition_effect_request(
        account_id,
        request_id,
        fingerprint,
        "ENQUEUED",
        result=effect_result,
        allowed_from=("DISPATCHING",),
    )
    final_effect_record = effect_store.get_effect_request(account_id, request_id)
    replay_effect_record, effect_replay_duplicate = (
        effect_store.reserve_effect_request(
            account_id, request_id, effect_kind, fingerprint
        )
    )
    conflicting_fingerprint = "sha256:" + ("0" * 64)
    if conflicting_fingerprint == fingerprint:
        conflicting_fingerprint = "sha256:" + ("1" * 64)
    effect_conflict_rejected = False
    try:
        effect_store.reserve_effect_request(
            account_id, request_id, effect_kind, conflicting_fingerprint
        )
    except gateway.IdempotencyConflict:
        effect_conflict_rejected = True
    response_duplicate = queue_client.enqueue_action("place_order", payload, 1.0)
    calls_after_response_replay = calls[request_id]
    queue_client.ack_response_sync(request_id)
    guard_duplicate = queue_client.enqueue_action("place_order", payload, 1.0)
    second_drain = helper.drain_file_requests(
        None, 1, "command", helper.COMMAND_BUDGET_MS
    )
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    checks = {
        "first_drain_count": first_drain,
        "second_drain_count": second_drain,
        "mock_passorder_calls": calls[request_id],
        "guard_visible_before_passorder": guard_visible["value"],
        "final_guard_state": str((guard or {}).get("state") or ""),
        "response_present_before_ack": response_present_before_ack,
        "response_status": str(data.get("status") or ""),
        "response_replay_idempotent": bool(response_duplicate.get("idempotent")),
        "response_replay_call_count": calls_after_response_replay,
        "guard_replay_idempotent": bool(guard_duplicate.get("idempotent")),
        "guard_replay_dedupe_layer": str(
            guard_duplicate.get("dedupe_layer") or ""
        ),
        "guard_replay_stage": str(guard_duplicate.get("duplicate_stage") or ""),
        "intent_hash": str(payload.get("intent_hash") or ""),
        "gateway_effect_fingerprint": fingerprint,
        "effect_prepared": bool(
            not reserve_duplicate and prepared_record.get("state") == "PREPARED"
        ),
        "effect_dispatched": effect_dispatched,
        "effect_committed": effect_committed,
        "effect_state": str((final_effect_record or {}).get("state") or ""),
        "effect_result_stage": str(
            ((final_effect_record or {}).get("result") or {}).get("stage") or ""
        ),
        "effect_replay_duplicate": effect_replay_duplicate,
        "effect_replay_state": str(replay_effect_record.get("state") or ""),
        "effect_conflict_rejected": effect_conflict_rejected,
    }
    checks["passed"] = bool(
        checks["first_drain_count"] == 1
        and checks["second_drain_count"] == 0
        and checks["mock_passorder_calls"] == 1
        and checks["guard_visible_before_passorder"]
        and checks["final_guard_state"] == "submitted"
        and checks["response_present_before_ack"]
        and checks["response_status"] == "accepted"
        and checks["response_replay_idempotent"]
        and checks["response_replay_call_count"] == 1
        and checks["guard_replay_idempotent"]
        and checks["guard_replay_dedupe_layer"] == "helper_guard"
        and checks["intent_hash"].startswith("sha256:")
        and checks["gateway_effect_fingerprint"].startswith("sha256:")
        and checks["effect_prepared"]
        and checks["effect_dispatched"]
        and checks["effect_committed"]
        and checks["effect_state"] == "ENQUEUED"
        and checks["effect_result_stage"] == "BRIDGE_QUEUED"
        and checks["effect_replay_duplicate"]
        and checks["effect_replay_state"] == "ENQUEUED"
        and checks["effect_conflict_rejected"]
    )
    return checks


def _fault_injection(
    gateway: Any,
    helper: Any,
    queue_client: Any,
    effect_store: Any,
    repeat: int,
    seed: int,
) -> Dict[str, Any]:
    request_id = "offline-fault-r%03d" % repeat
    payload = _payload(random.Random(seed ^ 0xBAD5EED), repeat, 9_999_999, "fault")
    payload["request_id"] = request_id
    payload["msg_id"] = request_id
    payload["trace_id"] = request_id
    payload["client_order_id"] = request_id
    payload = _prepare_effect_payload(
        gateway, payload, repeat, 9_999_999, "fault"
    )
    account_id = str(payload["account_id"])
    effect_kind, fingerprint = gateway.durable_effect_identity("order", payload)
    prepared_record, reserve_duplicate = effect_store.reserve_effect_request(
        account_id, request_id, effect_kind, fingerprint
    )
    effect_dispatched = effect_store.transition_effect_request(
        account_id,
        request_id,
        fingerprint,
        "DISPATCHING",
        allowed_from=("PREPARED",),
    )
    calls = Counter()
    guard_visible = {"value": False}

    def failing_passorder(args: Dict[str, Any], context: Any) -> Any:
        del context
        current_id = str(args.get("request_id") or "")
        calls[current_id] += 1
        guard = helper._read_request_guard(current_id)
        guard_visible["value"] = bool(
            isinstance(guard, dict) and guard.get("state") == "processing"
        )
        raise RuntimeError("intentional offline benchmark failure")

    helper.call_passorder = failing_passorder
    queue_client.enqueue_action("place_order", payload, 1.0)
    first_drain = helper.drain_file_requests(
        None, 1, "command", helper.COMMAND_BUDGET_MS
    )
    response_path = Path(helper._response_path(request_id))
    response_present_before_ack = response_path.is_file()
    guard = helper._read_request_guard(request_id)
    response = queue_client.consume_response_sync(request_id)
    effect_result = {
        "ok": False,
        "request_id": request_id,
        "stage": "SUBMIT_UNKNOWN",
        "status": "submit_unknown",
    }
    effect_committed = effect_store.transition_effect_request(
        account_id,
        request_id,
        fingerprint,
        "UNKNOWN",
        result=effect_result,
        allowed_from=("DISPATCHING",),
    )
    final_effect_record = effect_store.get_effect_request(account_id, request_id)
    replay_effect_record, effect_replay_duplicate = (
        effect_store.reserve_effect_request(
            account_id, request_id, effect_kind, fingerprint
        )
    )
    replay = queue_client.enqueue_action("place_order", payload, 1.0)
    second_drain = helper.drain_file_requests(
        None, 1, "command", helper.COMMAND_BUDGET_MS
    )
    queue_client.ack_response_sync(request_id)
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    checks = {
        "first_drain_count": first_drain,
        "second_drain_count": second_drain,
        "mock_passorder_calls": calls[request_id],
        "guard_visible_before_exception": guard_visible["value"],
        "final_guard_state": str((guard or {}).get("state") or ""),
        "response_present_before_ack": response_present_before_ack,
        "response_status": str(data.get("status") or ""),
        "replay_idempotent": bool(replay.get("idempotent")),
        "intent_hash": str(payload.get("intent_hash") or ""),
        "gateway_effect_fingerprint": fingerprint,
        "effect_prepared": bool(
            not reserve_duplicate and prepared_record.get("state") == "PREPARED"
        ),
        "effect_dispatched": effect_dispatched,
        "effect_committed": effect_committed,
        "effect_state": str((final_effect_record or {}).get("state") or ""),
        "effect_result_stage": str(
            ((final_effect_record or {}).get("result") or {}).get("stage") or ""
        ),
        "effect_replay_duplicate": effect_replay_duplicate,
        "effect_replay_state": str(replay_effect_record.get("state") or ""),
    }
    checks["passed"] = bool(
        checks["first_drain_count"] == 1
        and checks["second_drain_count"] == 0
        and checks["mock_passorder_calls"] == 1
        and checks["guard_visible_before_exception"]
        and checks["final_guard_state"] == "unknown"
        and checks["response_present_before_ack"]
        and checks["response_status"] == "submit_unknown"
        and checks["replay_idempotent"]
        and checks["intent_hash"].startswith("sha256:")
        and checks["gateway_effect_fingerprint"].startswith("sha256:")
        and checks["effect_prepared"]
        and checks["effect_dispatched"]
        and checks["effect_committed"]
        and checks["effect_state"] == "UNKNOWN"
        and checks["effect_result_stage"] == "SUBMIT_UNKNOWN"
        and checks["effect_replay_duplicate"]
        and checks["effect_replay_state"] == "UNKNOWN"
    )
    return checks


def _summarize(
    records: Sequence[Dict[str, Any]],
    *,
    samples: int,
    repeats: int,
    warmup: int,
    seed: int,
    measured_seconds: float,
    call_counts: Counter,
    expected_normal_intents: int,
    safety: Dict[str, Any],
    cleanup_verified: bool,
) -> Dict[str, Any]:
    error_count = sum(1 for record in records if not record.get("ok"))
    metric_names = (
        "enqueue_ms",
        "helper_drain_ms",
        "response_read_ms",
        "ack_and_invariant_ms",
        "total_to_response_ms",
        "queue_wait_ms",
        "mock_passorder_elapsed_ms",
        "effect_reserve_ms",
        "effect_dispatch_barrier_ms",
        "effect_result_commit_ms",
        "effect_replay_probe_ms",
        "durable_total_to_final_state_ms",
    )
    metrics = {
        name: metric_summary(
            record[name]
            for record in records
            if record.get("ok") and isinstance(record.get(name), (int, float))
        )
        for name in metric_names
    }
    all_counts_once = bool(
        len(call_counts) == expected_normal_intents
        and all(count == 1 for count in call_counts.values())
    )
    all_safety_checks_passed = bool(
        error_count == 0
        and all_counts_once
        and safety.get("success_guard_probe", {}).get("passed")
        and safety.get("guard_layer_duplicate_no_retry")
        and safety.get("fault_injection", {}).get("passed")
        and safety.get("durable_effect_registry", {}).get("passed")
        and cleanup_verified
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "summary",
        "mode": "offline_file_queue_mock",
        "samples_per_repeat": samples,
        "repeats": repeats,
        "warmup_per_repeat": warmup,
        "seed": seed,
        "sample_records": len(records),
        "errors": error_count,
        "measured_seconds": measured_seconds,
        "throughput_orders_per_second": (
            len(records) / measured_seconds if measured_seconds > 0 else 0.0
        ),
        "metrics_ms": metrics,
        "normal_unique_intents_including_warmup": expected_normal_intents,
        "normal_mock_passorder_calls": sum(call_counts.values()),
        "every_unique_intent_called_once": all_counts_once,
        "safety": safety,
        "temporary_directory_cleanup_verified": cleanup_verified,
        "network_used": False,
        "qmt_or_broker_connected": False,
        "all_safety_checks_passed": all_safety_checks_passed,
    }


def run_benchmark(
    *,
    samples: int = 100,
    repeats: int = 1,
    warmup: int = 10,
    seed: int = 20260718,
) -> Dict[str, Any]:
    """Run the offline benchmark and return raw records plus one summary."""
    if samples <= 0:
        raise ValueError("samples must be positive")
    if repeats <= 0:
        raise ValueError("repeats must be positive")
    if warmup < 0:
        raise ValueError("warmup cannot be negative")

    gateway, helper = load_core_modules()
    logger = logging.getLogger("qmt_offline_benchmark")
    logger.addHandler(logging.NullHandler())
    temporary = tempfile.TemporaryDirectory(prefix="qmt-local-offline-benchmark-")
    temporary_path = Path(temporary.name)
    records: List[Dict[str, Any]] = []
    call_counts: Counter = Counter()
    expected_normal_intents = repeats * (warmup + samples)
    measured_seconds = 0.0
    safety: Dict[str, Any] = {}
    last_queue_client: Any = None
    last_effect_store: Any = None
    effect_stores: List[Any] = []

    try:
        for repeat in range(repeats):
            repeat_root = temporary_path / ("repeat-%03d" % repeat)
            _bind_helper_root(helper, repeat_root)
            effect_store = gateway.OrderCorrelationStore(
                repeat_root / "benchmark_gateway_state.sqlite3"
            )
            effect_stores.append(effect_store)
            config = gateway.AccountConfig(
                name="offline_bench",
                account_id="OFFLINE_BENCH_ACCOUNT",
                account_type="STOCK",
                tcp_host="127.0.0.1",
                tcp_port=0,
                runtime_dir=str(repeat_root),
            )
            queue_client = gateway.FileQueueHelperClient(config, logger)
            rng = random.Random(seed + repeat)

            def mock_passorder(args: Dict[str, Any], context: Any) -> str:
                del context
                request_id = str(args.get("request_id") or "")
                call_counts[request_id] += 1
                return "SIM-" + request_id

            helper.call_passorder = mock_passorder
            for index in range(warmup):
                warmup_payload = _payload(rng, repeat, index, "warmup")
                warmup_payload = _prepare_effect_payload(
                    gateway, warmup_payload, repeat, index, "warmup"
                )
                _run_normal_sample(
                    gateway,
                    helper,
                    queue_client,
                    effect_store,
                    warmup_payload,
                    repeat,
                    index,
                    "warmup",
                    call_counts,
                )

            repeat_started = time.perf_counter()
            for index in range(samples):
                payload = _payload(rng, repeat, index, "measured")
                payload = _prepare_effect_payload(
                    gateway, payload, repeat, index, "measured"
                )
                try:
                    record = _run_normal_sample(
                        gateway,
                        helper,
                        queue_client,
                        effect_store,
                        payload,
                        repeat,
                        index,
                        "measured",
                        call_counts,
                    )
                except Exception as exc:
                    record = {
                        "schema_version": SCHEMA_VERSION,
                        "record_type": "sample",
                        "phase": "measured",
                        "repeat": repeat,
                        "index": index,
                        "request_id": str(payload.get("request_id") or ""),
                        "client_order_id": str(payload.get("client_order_id") or ""),
                        "intent_hash": str(payload.get("intent_hash") or ""),
                        "gateway_effect_fingerprint": str(
                            payload.get("gateway_effect_fingerprint") or ""
                        ),
                        "ok": False,
                        "error": "%s: %s" % (type(exc).__name__, exc),
                    }
                records.append(record)
            measured_seconds += time.perf_counter() - repeat_started
            last_queue_client = queue_client
            last_effect_store = effect_store

        _require(last_queue_client is not None, "benchmark created no queue client")
        _require(last_effect_store is not None, "benchmark created no effect store")
        safety["success_guard_probe"] = _success_guard_probe(
            gateway,
            helper,
            last_queue_client,
            last_effect_store,
            repeats - 1,
            seed,
        )
        safety["guard_layer_duplicate_no_retry"] = bool(
            safety["success_guard_probe"].get("guard_replay_idempotent")
            and safety["success_guard_probe"].get("guard_replay_dedupe_layer")
            == "helper_guard"
            and safety["success_guard_probe"].get("second_drain_count") == 0
            and safety["success_guard_probe"].get("mock_passorder_calls") == 1
        )
        safety["guard_layer_replays_checked"] = 1
        safety["fault_injection"] = _fault_injection(
            gateway,
            helper,
            last_queue_client,
            last_effect_store,
            repeats - 1,
            seed,
        )
        normal_registry_valid = bool(
            len(records) == samples * repeats
            and all(
                record.get("ok")
                and record.get("effect_kind") == "order"
                and record.get("effect_state") == "ENQUEUED"
                and record.get("effect_result_stage") == "BRIDGE_QUEUED"
                and record.get("effect_registry_duplicate_replayed")
                and record.get("effect_registry_record_valid")
                for record in records
            )
        )
        success_probe = safety["success_guard_probe"]
        fault_probe = safety["fault_injection"]
        durable_registry = {
            "measured_records_checked": len(records),
            "measured_records_valid": normal_registry_valid,
            "success_state": str(success_probe.get("effect_state") or ""),
            "success_result_stage": str(
                success_probe.get("effect_result_stage") or ""
            ),
            "fault_state": str(fault_probe.get("effect_state") or ""),
            "fault_result_stage": str(
                fault_probe.get("effect_result_stage") or ""
            ),
            "exact_replays_deduplicated": bool(
                success_probe.get("effect_replay_duplicate")
                and fault_probe.get("effect_replay_duplicate")
            ),
            "conflict_rejected": bool(
                success_probe.get("effect_conflict_rejected")
            ),
        }
        durable_registry["passed"] = bool(
            durable_registry["measured_records_valid"]
            and durable_registry["success_state"] == "ENQUEUED"
            and durable_registry["success_result_stage"] == "BRIDGE_QUEUED"
            and durable_registry["fault_state"] == "UNKNOWN"
            and durable_registry["fault_result_stage"] == "SUBMIT_UNKNOWN"
            and durable_registry["exact_replays_deduplicated"]
            and durable_registry["conflict_rejected"]
        )
        safety["durable_effect_registry"] = durable_registry
    finally:
        try:
            for effect_store in reversed(effect_stores):
                effect_store.close()
        finally:
            temporary.cleanup()

    cleanup_verified = not temporary_path.exists()
    if not cleanup_verified:
        raise BenchmarkInvariantError(
            "TemporaryDirectory still exists after cleanup: %s" % temporary_path
        )
    summary = _summarize(
        records,
        samples=samples,
        repeats=repeats,
        warmup=warmup,
        seed=seed,
        measured_seconds=measured_seconds,
        call_counts=call_counts,
        expected_normal_intents=expected_normal_intents,
        safety=safety,
        cleanup_verified=cleanup_verified,
    )
    return {"records": records, "summary": summary}


def write_jsonl(result: Dict[str, Any], output: Optional[Path] = None) -> None:
    records = list(result["records"]) + [result["summary"]]
    lines = [
        json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        for record in records
    ]
    if output is None:
        for line in lines:
            print(line)
        return
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as stream:
        for line in lines:
            stream.write(line + "\n")
    print(json.dumps(result["summary"], ensure_ascii=False, allow_nan=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="completely offline Big QMT local bridge file-queue benchmark"
    )
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--output", type=Path, help="optional JSONL output path")
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="force a fast 3-sample/1-repeat/1-warmup safety run",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.self_test:
        args.samples = 3
        args.repeats = 1
        args.warmup = 1
    result = run_benchmark(
        samples=args.samples,
        repeats=args.repeats,
        warmup=args.warmup,
        seed=args.seed,
    )
    write_jsonl(result, args.output)
    summary = result["summary"]
    return 0 if summary["errors"] == 0 and summary["all_safety_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
