#coding:gbk
"""
Big QMT single-thread file-queue helper generation template.

Never load the copy under src directly. Use the project-root generate_helper.ps1
to compile a fail-closed, account-bound helper, then load the installed
bigqmt_loader.py in QMT. Generated helpers open no TCP/HTTP ports or threads.
"""

import hashlib
import json
import math
import os
import time
import traceback
import uuid


# XUANLING_HELPER_CONFIG_START
HELPER_NAME = "template"
ACCOUNT_ID = "000000"
ACCOUNT_NAME = "template"
ACCOUNT_TYPE = "STOCK"
RUNTIME_DIR = r"C:\Quant\TradeBridge\runtime\bigqmt\0000"
ENABLE_TRADING = False
ENABLE_CANCEL_ORDER = False
MAX_COMMANDS_PER_TICK = 4
MAX_QUERIES_PER_TICK = 1
COMMAND_BUDGET_MS = 15.0
COMMAND_INTERVAL_MS = 25
QUERY_INTERVAL_MS = 500
RECONCILE_INTERVAL_SECONDS = 30
MAINTENANCE_INTERVAL_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 1
READINESS_INTERVAL_MS = 100
ALLOW_QMT_QUERY_DURING_TRADING = False
REQUEST_GUARD_TTL_SECONDS = 604800.0
MAX_FILE_AGE_SECONDS = 86400.0
MAX_CLEANUP_FILES_PER_TICK = 100
LOW_PRIORITY_QUIET_SECONDS = 1.0
ENABLE_RUN_TIME_TIMER = True
STRATEGY_NAME = "xuanling_local"
DEFAULT_REMARK = "local_tcp_signal"
PASSORDER_QUICK_TRADE = 2
QMT_ORDER_TYPE_DEFAULT = 1101
QMT_USER_ORDER_ID_MAX_LENGTH = 23
BUILD_ID = "xuanling_bigqmt_file_queue_helper_20260718_low_latency_v12_fail_closed_sibling_scan"
# XUANLING_HELPER_CONFIG_END


# Bound maintenance metadata work independently from the delete budget. This is
# intentionally a fixed build-time safety limit, not a deployment knob.
MAX_CLEANUP_SCAN_ENTRIES_PER_TICK = 512
MAX_PROCESSING_RECOVERY_FILES_PER_TICK = 8
MAX_COMMAND_SIBLING_PREFLIGHT_FILES = 32
MAX_COMMAND_SIBLING_SCAN_ENTRIES_PER_REQUEST = 512
MAX_REQUEST_QUEUE_SCAN_ENTRIES_PER_CYCLE = 512
MAX_STALE_ATOMIC_TMP_FILES_PER_TICK = 32
MAX_STALE_ATOMIC_TMP_SCAN_ENTRIES_PER_TICK = 256
STALE_ATOMIC_TMP_AGE_SECONDS = 300.0


INBOX_DIR = os.path.join(RUNTIME_DIR, "inbox")
INBOX_COMMANDS_DIR = os.path.join(INBOX_DIR, "commands")
INBOX_QUERIES_DIR = os.path.join(INBOX_DIR, "queries")
PROCESSING_DIR = os.path.join(RUNTIME_DIR, "processing")
PROCESSING_COMMANDS_DIR = os.path.join(PROCESSING_DIR, "commands")
PROCESSING_QUERIES_DIR = os.path.join(PROCESSING_DIR, "queries")
RESPONSES_DIR = os.path.join(RUNTIME_DIR, "responses")
REQUEST_STATE_DIR = os.path.join(RUNTIME_DIR, "request_state")
EVENTS_DIR = os.path.join(RUNTIME_DIR, "events")
EVENTS_LIVE_DIR = os.path.join(EVENTS_DIR, "live")
EVENTS_FAILED_DIR = os.path.join(EVENTS_DIR, "failed")
# Kept as a read-only compatibility alias for older diagnostics.
EVENTS_DONE_DIR = os.path.join(EVENTS_DIR, "done")
SNAPSHOTS_DIR = os.path.join(RUNTIME_DIR, "snapshots")
ARCHIVE_DIR = os.path.join(RUNTIME_DIR, "archive")
DONE_DIR = os.path.join(ARCHIVE_DIR, "done")
FAILED_DIR = os.path.join(ARCHIVE_DIR, "failed")
STATE_FILE = os.path.join(RUNTIME_DIR, "state.json")
HEARTBEAT_FILE = os.path.join(RUNTIME_DIR, "heartbeat.json")
METRICS_FILE = os.path.join(RUNTIME_DIR, "metrics.json")
READINESS_FILE = os.path.join(RUNTIME_DIR, "readiness.json")

G_CONTEXT = None
G_ACCOUNT_READY = False
G_LAST_ERROR = ""
G_HANDLEBAR_COUNT = 0
G_LAST_SNAPSHOT_AT = 0.0
G_RUN_TIME_READY = False
G_LAST_COMMAND_CYCLE_AT = 0.0
G_LAST_COMMAND_ACTIVITY_AT = 0.0
G_LAST_HEARTBEAT_AT = 0.0
G_COMMAND_CYCLE_RUNNING = False
G_QUERY_CYCLE_RUNNING = False
G_LAST_CALLBACK_SOURCE = ""
G_LAST_ASSET_HASH = ""
G_LAST_POSITIONS_HASH = ""
G_LAST_ORDERS = {}
G_SEEN_TRADE_KEYS = set()
G_EVENT_SEQ = 0
G_BASELINE_READY = False
G_RECONCILE_NEEDED = False
G_METRICS = {
    "requests_total": 0,
    "requests_ok": 0,
    "requests_failed": 0,
    "snapshots_total": 0,
    "command_cycles_total": 0,
    "query_cycles_total": 0,
    "command_timer_overrun_total": 0,
    "maintenance_deferred_for_command_total": 0,
    "callback_events_total": 0,
    "cleanup_scanned_total": 0,
    "cleanup_scan_budget_exhausted_total": 0,
    "stale_tmp_cleanup_deleted_total": 0,
    "stale_tmp_cleanup_scanned_total": 0,
    "stale_tmp_cleanup_scan_budget_exhausted_total": 0,
    "processing_recovery_total": 0,
    "processing_recovery_returned_total": 0,
    "processing_recovery_completed_total": 0,
    "processing_recovery_uncertain_total": 0,
    "processing_recovery_failed_total": 0,
    "processing_recovery_error_total": 0,
    "queued_sibling_conflict_total": 0,
    "queued_sibling_scan_incomplete_total": 0,
    "request_queue_scan_failed_total": 0,
    "request_queue_scan_limit_exceeded_total": 0,
    "request_queue_scan_unreadable_total": 0,
    "last_request_elapsed_ms": 0.0,
    "last_snapshot_elapsed_ms": 0.0,
}
G_PROCESSING_REQUEST_IDS = set()
G_CLEANUP_FOLDER_CURSOR = 0
G_STALE_TMP_CLEANUP_FOLDER_CURSOR = 0
G_PROCESSING_RECOVERY_FOLDER_CURSOR = 0


ORDER_STATUS_TEXT_MAP = {
    "\u672a\u62a5": "48",
    "\u5f85\u62a5": "49",
    "\u5df2\u62a5": "50",
    "\u5df2\u62a5\u5f85\u64a4": "51",
    "\u90e8\u6210\u5f85\u64a4": "52",
    "\u90e8\u64a4": "53",
    "\u5df2\u64a4": "54",
    "\u90e8\u6210": "55",
    "\u5df2\u6210": "56",
    "\u5e9f\u5355": "57",
    "\u5df2\u5e9f": "57",
    "\u64a4\u5e9f": "58",
}

BUY_OP_TYPES = set([23, 27, 29, 33, 35, 40, 42, 50, 53, 56, 60, 80, 82])
SELL_OP_TYPES = set([24, 28, 30, 31, 32, 34, 36, 41, 43, 44, 45, 51, 52, 54, 55, 61, 81, 83])
STANDARD_OP_TYPES = BUY_OP_TYPES | SELL_OP_TYPES
BUY_OFFSET_FLAGS = set([48])
SELL_OFFSET_FLAGS = set([49])


def _now():
    return time.time()


def _log(msg):
    try:
        print("[bigqmt_file_queue_helper][%s][%s] %s" % (HELPER_NAME, time.strftime("%Y-%m-%d %H:%M:%S"), msg))
    except Exception:
        pass


def _safe_str(value, default=""):
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _json_default(value):
    try:
        return str(value)
    except Exception:
        return ""


def _stable_hash(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)
    except Exception:
        return _safe_str(value)


def _make_request_id(prefix="qmt"):
    return "%s-%d-%s" % (prefix, int(time.time() * 1000), uuid.uuid4().hex[:8])


def _safe_filename(value):
    text = _safe_str(value, "")
    out = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    name = "".join(out).strip("._")
    return name or _make_request_id("file")


def _request_file_key(request_id):
    return hashlib.sha256(_safe_str(request_id).encode("utf-8")).hexdigest()


def _set_last_error(message):
    global G_LAST_ERROR
    G_LAST_ERROR = _safe_str(message)


def _ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def ensure_runtime_dirs():
    for path in (
        RUNTIME_DIR,
        INBOX_DIR,
        INBOX_COMMANDS_DIR,
        INBOX_QUERIES_DIR,
        PROCESSING_DIR,
        PROCESSING_COMMANDS_DIR,
        PROCESSING_QUERIES_DIR,
        RESPONSES_DIR,
        REQUEST_STATE_DIR,
        EVENTS_DIR,
        EVENTS_LIVE_DIR,
        EVENTS_FAILED_DIR,
        SNAPSHOTS_DIR,
        ARCHIVE_DIR,
        DONE_DIR,
        FAILED_DIR,
    ):
        _ensure_dir(path)


def _atomic_write_json(path, payload, ensure_parent=True):
    parent = os.path.dirname(path)
    if parent and ensure_parent:
        _ensure_dir(parent)
    recovered_parent = False
    while True:
        tmp = "%s.%s.%d.tmp" % (path, os.getpid(), int(_now() * 1000000))
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    payload,
                    f,
                    ensure_ascii=False,
                    default=_json_default,
                    separators=(",", ":"),
                )
                f.write("\n")
            os.replace(tmp, path)
            return
        except FileNotFoundError:
            try:
                os.remove(tmp)
            except Exception:
                pass
            if ensure_parent or not parent or recovered_parent:
                raise
            # Runtime directories are created during init. Recover once if an
            # operator or external process removed one after initialization.
            _ensure_dir(parent)
            recovered_parent = True
        except Exception:
            try:
                os.remove(tmp)
            except Exception:
                pass
            raise


def _safe_runtime_write(label, func, *args):
    try:
        return func(*args)
    except Exception as exc:
        _set_last_error("%s failed: %s" % (label, exc))
        _log("%s failed: %s" % (label, exc))
        return None


def _read_json(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def _move_file(src, dest_dir):
    _ensure_dir(dest_dir)
    dest = os.path.join(dest_dir, os.path.basename(src))
    if os.path.exists(dest):
        base, ext = os.path.splitext(os.path.basename(src))
        dest = os.path.join(dest_dir, "%s-%d%s" % (base, int(_now() * 1000), ext))
    os.replace(src, dest)
    return dest


def _request_guard_path(request_id):
    return os.path.join(REQUEST_STATE_DIR, _request_file_key(request_id) + ".json")


def _legacy_request_guard_path(request_id):
    return os.path.join(REQUEST_STATE_DIR, _safe_filename(request_id) + ".json")


def _read_request_guard(request_id):
    record, conflict, _, migrations = _resolve_artifact_pair(
        request_id,
        _request_guard_path(request_id),
        _legacy_request_guard_path(request_id),
        "guard",
    )
    if conflict:
        return None
    _apply_artifact_migrations(migrations)
    return record


def _write_request_guard(request_id, state, request, response=None):
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    fingerprint = _safe_str(
        request.get("gateway_effect_fingerprint")
        or payload.get("gateway_effect_fingerprint")
    )
    guard = {
        "request_id": request_id,
        "state": state,
        "action": _safe_str(request.get("action")),
        "account_id": _safe_str(request.get("account_id") or ACCOUNT_ID),
        "trace_id": _safe_str(request.get("trace_id") or payload.get("trace_id")),
        "client_order_id": _safe_str(request.get("client_order_id") or payload.get("client_order_id")),
        "qmt_user_order_id": _safe_str(request.get("qmt_user_order_id") or payload.get("qmt_user_order_id")),
        "intent_hash": _safe_str(request.get("intent_hash") or payload.get("intent_hash")),
        "cancel_order_id": _safe_str(payload.get("order_id")),
        "cancel_order_sysid": _safe_str(payload.get("order_sysid")),
        "updated_at": _now(),
    }
    if fingerprint:
        guard["gateway_effect_fingerprint"] = fingerprint
    _apply_request_effect_metadata(guard, request)
    if isinstance(response, dict):
        guard["response"] = response
    _atomic_write_json(_request_guard_path(request_id), guard, False)
    return guard


def _duplicate_state(request):
    resolution = _resolve_request_artifacts(request)
    if resolution.get("conflict"):
        return _fail_closed_artifact_conflict(request, resolution), None
    response = resolution.get("response")
    guard = resolution.get("guard")
    if isinstance(response, dict):
        response = dict(response)
        response["idempotent"] = True
        response["duplicate_stage"] = "helper_response"
        return response, guard
    if isinstance(guard, dict) and isinstance(guard.get("response"), dict):
        response = dict(guard.get("response"))
        response["idempotent"] = True
        response["duplicate_stage"] = "helper_guard"
        return response, guard
    return None, guard


def _duplicate_response(request):
    response, _ = _duplicate_state(request)
    return response


def _qmt_func(name):
    return globals().get(name)


def qmt_account_type():
    value = _safe_str(ACCOUNT_TYPE, "STOCK").strip()
    upper = value.upper()
    if upper in ("STOCK", "CASH", "SECURITY"):
        return "stock"
    if upper in ("CREDIT", "MARGIN"):
        return "credit"
    return value


def object_to_raw(obj):
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    raw = {}
    try:
        names = dir(obj)
    except Exception:
        names = []
    for name in names:
        if name.startswith("__"):
            continue
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if callable(value):
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            raw[name] = value
        else:
            raw[name] = _safe_str(value)
    return raw


def pick(raw, names, default=None):
    for name in names:
        if name in raw and raw.get(name) not in (None, ""):
            return raw.get(name)
    return default


def normalize_order_status(value):
    text = _safe_str(value, "").strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    return ORDER_STATUS_TEXT_MAP.get(text, text)


def side_from_order_type(order_type):
    value = _safe_int(order_type, 0)
    if value in BUY_OP_TYPES:
        return "BUY"
    if value in SELL_OP_TYPES:
        return "SELL"
    return ""


def resolve_order_types(raw):
    names = [
        "raw_order_type", "order_type", "op_type", "m_nOpType",
        "m_nOrderType", "m_eOperationType",
    ]
    has_explicit_raw_order_type = "raw_order_type" in raw
    raw_order_type = (
        _safe_int(raw.get("raw_order_type"), 0)
        if has_explicit_raw_order_type else 0
    )
    standard_order_type = 0
    for name in names:
        if name not in raw or raw.get(name) in (None, ""):
            continue
        value = _safe_int(raw.get(name), 0)
        if not has_explicit_raw_order_type and not raw_order_type and value:
            raw_order_type = value
        if value in STANDARD_OP_TYPES:
            standard_order_type = value
            break
    return raw_order_type, standard_order_type


def side_from_text(value):
    text = _safe_str(value, "").strip().upper()
    if text:
        if text in ("BUY", "B", "LONG") or "\u4e70" in text:
            return "BUY"
        if text in ("SELL", "S", "SHORT") or "\u5356" in text:
            return "SELL"
    return ""


def side_from_raw_text(raw):
    first_text = ""
    for name in [
        "side", "raw_side_text", "m_strOptName", "m_strSide", "m_strDirection",
        "m_strOrderType", "m_strOperationType",
        "m_strBuySell", "buy_sell", "entrust_bs", "business_name", "order_type_name",
        "operation_type_name", "direction_name",
    ]:
        text = _safe_str(raw.get(name, "") if isinstance(raw, dict) else "", "").strip()
        if not text:
            continue
        if not first_text:
            first_text = text
        side = side_from_text(text)
        if side:
            return side, text
    return "", first_text


def normalize_order_side(raw, order_type, offset_flag):
    side, text = side_from_raw_text(raw)
    if side:
        return side, "raw_side_text", text
    side = side_from_order_type(order_type)
    if side:
        return side, "order_type", text
    offset = _safe_int(offset_flag, 0)
    if offset in BUY_OFFSET_FLAGS:
        return "BUY", "offset_flag", text
    if offset in SELL_OFFSET_FLAGS:
        return "SELL", "offset_flag", text
    return "", "unknown", text


def effective_order_type(raw_order_type, side):
    op_type = _safe_int(raw_order_type, 0)
    op_side = side_from_order_type(op_type)
    if op_type in STANDARD_OP_TYPES and (not side or op_side == side):
        return op_type, "order_type"
    if side == "BUY":
        return 23, "side_conflict_default" if op_type in STANDARD_OP_TYPES else "side_default"
    if side == "SELL":
        return 24, "side_conflict_default" if op_type in STANDARD_OP_TYPES else "side_default"
    return 0, "unknown"


def normalize_symbol(raw):
    stock_code = pick(raw, [
        "stock_code", "symbol", "security", "m_strInstrumentID", "m_strStockCode",
        "m_strCode", "m_strOrderCode", "instrument_id", "code"
    ], "")
    exchange_id = pick(raw, ["m_strExchangeID", "exchange_id", "market"], "")
    stock_code = _safe_str(stock_code, "")
    if stock_code and exchange_id and "." not in stock_code:
        market = _safe_str(exchange_id).upper()
        if market in ("SH", "SSE", "1"):
            stock_code = "%s.SH" % stock_code
        elif market in ("SZ", "SZSE", "0", "2"):
            stock_code = "%s.SZ" % stock_code
    return stock_code


def normalize_position(obj):
    raw = object_to_raw(obj)
    stock_code = normalize_symbol(raw)
    volume = _safe_int(pick(raw, ["volume", "m_nVolume", "m_nCanUseVolume"], 0))
    can_use = _safe_int(pick(raw, ["can_use_volume", "available_volume", "m_nCanUseVolume", "m_nEnableVolume"], 0))
    avg_price = _safe_float(pick(raw, ["avg_price", "cost_price", "m_dOpenPrice", "m_dCostPrice"], 0))
    profit = _safe_float(pick(raw, ["profit", "float_profit", "m_dFloatProfit", "m_dPositionProfit"], 0))
    return {
        "account_id": _safe_str(pick(raw, ["account_id", "m_strAccountID"], ACCOUNT_ID)),
        "account_type": ACCOUNT_TYPE,
        "symbol": stock_code,
        "stock_code": stock_code,
        "stock_name": _safe_str(pick(raw, ["stock_name", "instrument_name", "m_strInstrumentName"], "")),
        "volume": volume,
        "available_volume": can_use,
        "can_use_volume": can_use,
        "open_price": _safe_float(pick(raw, ["open_price", "m_dOpenPrice"], 0)),
        "avg_price": avg_price,
        "cost_price": avg_price,
        "market_value": _safe_float(pick(raw, ["market_value", "m_dMarketValue"], 0)),
        "frozen_volume": _safe_int(pick(raw, ["frozen_volume", "m_nFrozenVolume"], 0)),
        "on_road_volume": _safe_int(pick(raw, ["on_road_volume", "m_nOnRoadVolume"], 0)),
        "yesterday_volume": _safe_int(pick(raw, ["yesterday_volume", "m_nYdVolume"], 0)),
        "last_price": _safe_float(pick(raw, ["last_price", "m_dLastPrice"], 0)),
        "float_profit": profit,
        "position_profit": _safe_float(pick(raw, ["position_profit", "m_dPositionProfit"], profit)),
        "profit": profit,
        "direction": _safe_int(pick(raw, ["direction", "m_nDirection"], 0)),
        "raw": raw,
    }


def normalize_order(obj):
    raw = object_to_raw(obj)
    raw_order_type, standard_order_type = resolve_order_types(raw)
    raw_direction = _safe_int(pick(raw, ["raw_direction", "direction", "m_nDirection"], 0))
    offset_flag = _safe_int(pick(raw, ["offset_flag", "m_nOffsetFlag"], 0))
    side, side_source, raw_side = normalize_order_side(raw, standard_order_type, offset_flag)
    order_type, order_type_source = effective_order_type(standard_order_type, side)
    raw_status = pick(raw, ["order_status", "status", "m_nOrderStatus", "m_strOrderStatus"], "")
    order_volume = _safe_int(pick(raw, [
        "order_volume", "quantity", "m_nVolumeTotalOriginal", "m_nOrderVolume", "m_nVolume",
    ], 0))
    traded_volume = _safe_int(pick(raw, [
        "traded_volume", "filled_qty", "m_nVolumeTraded", "m_nTradedVolume", "m_nDealVolume",
    ], 0))
    status = normalize_order_status(raw_status)
    stock_code = normalize_symbol(raw)
    return {
        "account_id": _safe_str(pick(raw, ["account_id", "m_strAccountID"], ACCOUNT_ID)),
        "account_type": ACCOUNT_TYPE,
        "symbol": stock_code,
        "stock_code": stock_code,
        "stock_name": _safe_str(pick(raw, ["stock_name", "instrument_name", "m_strInstrumentName"], "")),
        "order_id": _safe_str(pick(raw, ["order_id", "m_strOrderID", "m_strOrderSysID", "m_strEntrustNo"], "")),
        "order_sysid": _safe_str(pick(raw, ["order_sysid", "m_strOrderSysID", "m_strEntrustNo"], "")),
        "order_time": _safe_str(pick(raw, ["order_time", "m_strInsertTime", "m_strOrderTime", "m_nOrderTime"], "")),
        "order_type": order_type,
        "raw_order_type": raw_order_type,
        "order_type_source": order_type_source,
        "side": side,
        "raw_side_text": raw_side,
        "side_source": side_source,
        "quantity": order_volume,
        "order_volume": order_volume,
        "price_type": _safe_int(pick(raw, ["price_type", "m_nPriceType", "m_ePriceType"], 0)),
        "price": _safe_float(pick(raw, ["price", "m_dPrice", "m_dLimitPrice"], 0)),
        "filled_qty": traded_volume,
        "traded_volume": traded_volume,
        "traded_price": _safe_float(pick(raw, ["traded_price", "filled_price", "m_dTradedPrice", "m_dAveragePrice"], 0)),
        "status": status,
        "order_status": status,
        "status_text": _safe_str(pick(raw, ["status_text", "m_strOrderStatus"], "")),
        "status_msg": _safe_str(pick(raw, ["status_msg", "m_strStatusMsg", "m_strErrorMsg", "m_strOrderStatus"], "")),
        "strategy_name": _safe_str(pick(raw, ["strategy_name", "m_strStrategyName"], STRATEGY_NAME)),
        "order_remark": _safe_str(pick(raw, ["order_remark", "remark", "m_strRemark", "m_strUserOrderId"], "")),
        "qmt_user_order_id": _safe_str(pick(raw, ["qmt_user_order_id", "m_strUserOrderId", "m_strRemark"], "")),
        "direction": order_type,
        "raw_direction": raw_direction,
        "offset_flag": offset_flag,
        "raw": raw,
    }


def normalize_trade(obj):
    raw = object_to_raw(obj)
    raw_order_type, standard_order_type = resolve_order_types(raw)
    raw_direction = _safe_int(pick(raw, ["raw_direction", "direction", "m_nDirection"], 0))
    offset_flag = _safe_int(pick(raw, ["offset_flag", "m_nOffsetFlag"], 0))
    side, side_source, raw_side = normalize_order_side(raw, standard_order_type, offset_flag)
    order_type, order_type_source = effective_order_type(standard_order_type, side)
    stock_code = normalize_symbol(raw)
    traded_volume = _safe_int(pick(raw, ["traded_volume", "quantity", "m_nTradedVolume", "m_nVolume"], 0))
    traded_price = _safe_float(pick(raw, ["traded_price", "price", "m_dTradedPrice", "m_dPrice"], 0))
    trade_date = _safe_str(pick(raw, [
        "trade_date", "trading_day", "m_strTradeDate", "m_strTradingDate", "m_nTradeDate",
    ], time.strftime("%Y-%m-%d")))
    return {
        "account_id": _safe_str(pick(raw, ["account_id", "m_strAccountID"], ACCOUNT_ID)),
        "account_type": ACCOUNT_TYPE,
        "symbol": stock_code,
        "stock_code": stock_code,
        "stock_name": _safe_str(pick(raw, ["stock_name", "instrument_name", "m_strInstrumentName"], "")),
        "side": side,
        "order_type": order_type,
        "raw_order_type": raw_order_type,
        "order_type_source": order_type_source,
        "raw_side_text": raw_side,
        "side_source": side_source,
        "trade_id": _safe_str(pick(raw, ["trade_id", "traded_id", "m_strTradeID", "m_strDealID"], "")),
        "traded_id": _safe_str(pick(raw, ["traded_id", "trade_id", "m_strTradeID", "m_strDealID"], "")),
        "trade_time": _safe_str(pick(raw, ["trade_time", "traded_time", "m_strTradeTime", "m_nTradeTime"], "")),
        "traded_time": _safe_str(pick(raw, ["traded_time", "trade_time", "m_strTradeTime", "m_nTradeTime"], "")),
        "trade_date": trade_date,
        "price": traded_price,
        "traded_price": traded_price,
        "quantity": traded_volume,
        "traded_volume": traded_volume,
        "amount": _safe_float(pick(raw, ["amount", "traded_amount", "m_dTradedAmount", "m_dTradeAmount"], 0)),
        "traded_amount": _safe_float(pick(raw, ["traded_amount", "amount", "m_dTradedAmount", "m_dTradeAmount"], 0)),
        "order_id": _safe_str(pick(raw, ["order_id", "m_strOrderID", "m_strOrderSysID", "m_strEntrustNo"], "")),
        "order_sysid": _safe_str(pick(raw, ["order_sysid", "m_strOrderSysID", "m_strEntrustNo"], "")),
        "strategy_name": _safe_str(pick(raw, ["strategy_name", "m_strStrategyName"], STRATEGY_NAME)),
        "order_remark": _safe_str(pick(raw, ["order_remark", "remark", "m_strRemark", "m_strUserOrderId"], "")),
        "qmt_user_order_id": _safe_str(pick(raw, ["qmt_user_order_id", "m_strUserOrderId", "m_strRemark"], "")),
        "direction": order_type,
        "raw_direction": raw_direction,
        "offset_flag": offset_flag,
        "raw": raw,
    }


def normalize_account(obj):
    raw = object_to_raw(obj)
    return {
        "account_id": _safe_str(pick(raw, ["account_id", "m_strAccountID"], ACCOUNT_ID)),
        "account_type": ACCOUNT_TYPE,
        "total_asset": _safe_float(pick(raw, ["total_asset", "m_dBalance", "m_dTotalAsset", "m_dAssureAsset"], 0)),
        "available_cash": _safe_float(pick(raw, ["available_cash", "cash", "m_dAvailable", "m_dFetchBalance"], 0)),
        "frozen_cash": _safe_float(pick(raw, ["frozen_cash", "m_dFrozenCash"], 0)),
        "market_value": _safe_float(pick(raw, ["market_value", "m_dMarketValue", "m_dStockValue"], 0)),
        "total_debt": _safe_float(pick(raw, ["m_dTotalDebt", "total_debt"], 0)),
        "status": _safe_str(pick(raw, ["status", "m_strStatus", "m_nStatus"], "")),
        "raw": raw,
    }


def _canonical_qmt_effect(request):
    request = request if isinstance(request, dict) else {}
    action = _safe_str(request.get("action"), "").strip()
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    account_id = _safe_str(request.get("account_id") or ACCOUNT_ID, "").strip()
    account_type = _safe_str(request.get("account_type") or ACCOUNT_TYPE, "").strip()
    if not action or not account_id or not account_type:
        return None, "action and account identity are required"
    if action == "place_order":
        try:
            args = build_order_args(payload)
        except Exception as exc:
            return None, "place_order arguments are invalid: %s" % _safe_str(exc)
        op_type = _safe_int(args.get("op_type"), 0)
        return {
            "action": action,
            "account_id": account_id,
            "account_type": account_type,
            "op_type": op_type,
            "qmt_order_type": _safe_int(args.get("order_type"), 0),
            "symbol": _safe_str(args.get("symbol"), ""),
            "price_type": _safe_int(args.get("price_type"), 0),
            "raw_price": _safe_float(args.get("price"), 0.0),
            "quantity": args.get("quantity"),
            "strategy_name": _safe_str(args.get("strategy_name"), ""),
            "order_remark": _safe_str(args.get("order_remark"), ""),
            "qmt_user_order_id": _safe_str(args.get("user_order_id"), ""),
            "quick_trade": _safe_int(args.get("quick_trade"), 0),
            "side": _safe_str(args.get("side"), ""),
            "passorder_signature": (
                "direct_cash_repay" if op_type in (32, 75) else "standard"
            ),
        }, ""
    if action == "cancel_order":
        order_id = _safe_str(payload.get("order_id"), "").strip()
        order_sysid = _safe_str(payload.get("order_sysid"), "").strip()
        target_order_id = order_id or order_sysid
        if not target_order_id:
            return None, "cancel_order target is required"
        return {
            "action": action,
            "account_id": account_id,
            "account_type": account_type,
            "qmt_account_type": qmt_account_type(),
            "order_id": order_id,
            "order_sysid": order_sysid,
            "target_order_id": target_order_id,
            "market": _safe_str(payload.get("market"), ""),
        }, ""
    return None, "unsupported guarded action=%s" % action


def _request_effect_metadata(request):
    request = request if isinstance(request, dict) else {}
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    metadata = {
        "action": _safe_str(request.get("action")),
        "account_id": _safe_str(request.get("account_id") or ACCOUNT_ID),
        "client_order_id": _safe_str(
            request.get("client_order_id") or payload.get("client_order_id")
        ),
        "intent_hash": _safe_str(request.get("intent_hash") or payload.get("intent_hash")),
        "cancel_order_id": _safe_str(payload.get("order_id")),
        "cancel_order_sysid": _safe_str(payload.get("order_sysid")),
    }
    fingerprint = _safe_str(
        request.get("gateway_effect_fingerprint")
        or payload.get("gateway_effect_fingerprint")
    )
    if fingerprint:
        metadata["gateway_effect_fingerprint"] = fingerprint
    qmt_effect, _ = _canonical_qmt_effect(request)
    if isinstance(qmt_effect, dict):
        metadata["qmt_effect"] = qmt_effect
    return metadata


def _apply_request_effect_metadata(target, request):
    metadata = _request_effect_metadata(request)
    for key, value in metadata.items():
        if value:
            target[key] = value
    return target


def make_response(ok, data=None, error="", code="", request=None, action=""):
    response = {
        "version": 1,
        "ok": bool(ok),
        "request_id": _safe_str((request or {}).get("request_id"), ""),
        "msg_id": _safe_str((request or {}).get("msg_id"), ""),
        "account_id": ACCOUNT_ID,
        "action": action or _safe_str((request or {}).get("action"), ""),
        "data": data if data is not None else {},
        "error": _safe_str(error),
        "code": _safe_str(code),
        "status": "done" if ok else "failed",
        "finished_at": _now(),
        "build_id": BUILD_ID,
    }
    return _apply_request_effect_metadata(response, request)


def qmt_get_details(data_type):
    func = _qmt_func("get_trade_detail_data")
    if not func:
        return []
    errors = []
    for account_type in (ACCOUNT_TYPE, qmt_account_type()):
        try:
            return func(ACCOUNT_ID, account_type, data_type) or []
        except Exception as exc:
            errors.append(_safe_str(exc))
    if errors:
        _set_last_error("query %s failed: %s" % (data_type, "; ".join(errors)))
    return []


def query_snapshot():
    positions = [normalize_position(x) for x in qmt_get_details("POSITION")]
    orders = [normalize_order(x) for x in qmt_get_details("ORDER")]
    trades = [normalize_trade(x) for x in qmt_get_details("DEAL")]
    accounts = [normalize_account(x) for x in qmt_get_details("ACCOUNT")]
    asset = accounts[0] if accounts else {}
    return {
        "account_id": ACCOUNT_ID,
        "asset": asset,
        "positions": positions,
        "orders": orders,
        "trades": trades,
        "accounts": accounts,
        "position_count": len(positions),
        "order_count": len(orders),
        "trade_count": len(trades),
        "created_at": _now(),
    }


def _symbol_filter(payload):
    return _safe_str(payload.get("symbol") or payload.get("stock_code") or payload.get("security"), "")


def _filter_by_symbol(items, symbol):
    if not symbol:
        return items
    return [item for item in items if item.get("stock_code") == symbol or item.get("symbol") == symbol]


def _filter_orders(items, payload):
    order_id = _safe_str(payload.get("order_id", ""))
    if order_id:
        items = [item for item in items if _safe_str(item.get("order_id")) == order_id or _safe_str(item.get("order_sysid")) == order_id]
    items = _filter_by_symbol(items, _symbol_filter(payload))
    if payload.get("cancelable_only"):
        cancelable_status = set(["48", "49", "50", "51", "52", "53", "55"])
        items = [item for item in items if _safe_str(item.get("order_status") or item.get("status")) in cancelable_status]
    return items


def query_positions(payload):
    items = [normalize_position(x) for x in qmt_get_details("POSITION")]
    return {"positions": _filter_by_symbol(items, _symbol_filter(payload))}


def query_orders(payload):
    items = [normalize_order(x) for x in qmt_get_details("ORDER")]
    return {"orders": _filter_orders(items, payload)}


def query_trades(payload):
    items = [normalize_trade(x) for x in qmt_get_details("DEAL")]
    symbol = _symbol_filter(payload)
    if symbol:
        items = [item for item in items if item.get("stock_code") == symbol or item.get("symbol") == symbol]
    return {"trades": items}


def query_account(payload):
    accounts = [normalize_account(x) for x in qmt_get_details("ACCOUNT")]
    return {"asset": accounts[0] if accounts else {}, "accounts": accounts}


def query_order_status(payload):
    order_id = _safe_str(payload.get("order_id") or payload.get("order_sysid"), "")
    if not order_id:
        return {"order": {}, "found": False}
    orders = _filter_orders([normalize_order(x) for x in qmt_get_details("ORDER")], {"order_id": order_id})
    if orders:
        return {"order": orders[0], "found": True}
    func = _qmt_func("get_value_by_order_id")
    if func:
        for account_type in (ACCOUNT_TYPE, qmt_account_type()):
            try:
                obj = func(order_id, ACCOUNT_ID, account_type, "ORDER")
                if obj:
                    return {"order": normalize_order(obj), "found": True}
            except Exception:
                pass
    return {"order": {}, "found": False}


def build_order_args(payload):
    side = _safe_str(payload.get("side", "BUY")).upper()
    op_type = payload.get("order_type", None)
    if op_type is None:
        if side == "BUY":
            op_type = 23
        elif side == "SELL":
            op_type = 24
        else:
            raise ValueError("order_type is required when side is not BUY/SELL")
    symbol = _safe_str(payload.get("symbol") or payload.get("stock_code") or payload.get("security"), "")
    direct_cash_repay = _safe_int(op_type, 0) in (32, 75)
    quantity = (
        _safe_float(payload.get("quantity", payload.get("volume", 0)), 0)
        if direct_cash_repay
        else _safe_int(payload.get("quantity", payload.get("volume", 0)), 0)
    )
    price = _safe_float(payload.get("price", 0), 0)
    price_type = payload.get("price_type", payload.get("prType", None))
    if price_type is None:
        price_type = 11 if price > 0 else 5
    qmt_order_type = _safe_int(payload.get("qmt_order_type", payload.get("orderType", QMT_ORDER_TYPE_DEFAULT)), QMT_ORDER_TYPE_DEFAULT)
    strategy_name = _safe_str(payload.get("strategy_name", STRATEGY_NAME), STRATEGY_NAME)
    order_remark = _safe_str(payload.get("order_remark", payload.get("remark", DEFAULT_REMARK)), DEFAULT_REMARK)
    request_id = _safe_str(payload.get("request_id") or payload.get("msg_id") or "", "")
    user_order_id = _safe_str(payload.get("qmt_user_order_id") or order_remark or payload.get("trader_name") or payload.get("client_order_id") or request_id, "")
    if not user_order_id:
        user_order_id = _make_request_id("xl")
    if len(user_order_id) > QMT_USER_ORDER_ID_MAX_LENGTH:
        user_order_id = user_order_id[:QMT_USER_ORDER_ID_MAX_LENGTH]
    quick_trade = _safe_int(payload.get("quick_trade", payload.get("quickTrade", PASSORDER_QUICK_TRADE)), PASSORDER_QUICK_TRADE)
    if not symbol:
        raise ValueError("symbol is required")
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if direct_cash_repay and (_safe_int(price_type, 11) != 5 or price != 0):
        raise ValueError("direct cash repay requires price_type=5 and price=0")
    if not direct_cash_repay and _safe_int(price_type, 11) in (11, 49) and price <= 0:
        raise ValueError("price must be positive for fixed price order")
    return {
        "op_type": _safe_int(op_type, 0),
        "order_type": qmt_order_type,
        "symbol": symbol,
        "price_type": _safe_int(price_type, 11),
        "price": price,
        "quantity": quantity,
        "strategy_name": strategy_name,
        "quick_trade": quick_trade,
        "user_order_id": user_order_id,
        "order_remark": order_remark,
        "side": side,
        "request_id": request_id,
    }


def call_passorder(args, context):
    func = _qmt_func("passorder")
    if not func:
        raise RuntimeError("passorder is not available")
    # Never retry passorder after an exception. A TypeError may be raised after
    # the native API accepted the order, so a compatibility retry can duplicate
    # a real trade. The deployed helper requires the quickTrade-capable API.
    if _safe_int(args.get("op_type"), 0) in (32, 75):
        return func(
            args["op_type"],
            args["order_type"],
            ACCOUNT_ID,
            args["symbol"],
            args["price_type"],
            args["price"],
            args["quantity"],
            args["quick_trade"],
            context,
        )
    return func(
        args["op_type"],
        args["order_type"],
        ACCOUNT_ID,
        args["symbol"],
        args["price_type"],
        args["price"],
        args["quantity"],
        args["strategy_name"],
        args["quick_trade"],
        args["user_order_id"],
        context,
    )


def _normalize_passorder_order_id(result):
    if result is None or isinstance(result, bool):
        return ""
    if isinstance(result, int):
        return _safe_str(result, "").strip() if result > 0 else ""
    if isinstance(result, float):
        return _safe_str(result, "").strip() if math.isfinite(result) and result > 0 else ""
    if not isinstance(result, str):
        return ""
    text = result.strip()
    lowered = text.lower()
    if lowered in (
        "", "0", "none", "null", "nil", "false", "no", "undefined",
        "nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity",
        "fail", "failed", "failure", "error", "reject", "rejected",
    ):
        return ""
    try:
        numeric = float(text)
    except Exception:
        return text
    return text if math.isfinite(numeric) and numeric > 0 else ""


def call_cancel(payload, context):
    func = _qmt_func("cancel")
    if not func:
        raise RuntimeError("cancel is not available")
    order_id = _safe_str(payload.get("order_id") or payload.get("order_sysid"), "")
    if not order_id:
        raise ValueError("order_id or order_sysid is required")
    # A native TypeError can happen after QMT accepted the cancellation. Never
    # retry a side-effecting call with another signature; the caller records an
    # unknown submit state and requires reconciliation.
    return func(order_id, ACCOUNT_ID, qmt_account_type(), context)


def dispatch_qmt_action(context, action, payload):
    if action == "health":
        return health_payload()
    if action == "snapshot":
        return query_snapshot()
    if action == "account":
        return query_account(payload)
    if action == "positions":
        return query_positions(payload)
    if action == "orders":
        return query_orders(payload)
    if action == "trades":
        return query_trades(payload)
    if action == "order_status":
        return query_order_status(payload)
    if action == "place_order":
        if not ENABLE_TRADING:
            return {"status": "failed", "error": "trading disabled"}
        args = build_order_args(payload)
        passorder_started_at = _now()
        monotonic_started = time.perf_counter()
        result = call_passorder(args, context)
        passorder_finished_at = _now()
        passorder_elapsed_ms = max(0.0, (time.perf_counter() - monotonic_started) * 1000)
        order_id = _normalize_passorder_order_id(result)
        status = "accepted" if order_id else "submit_unknown"
        return {
            "status": status,
            "request_id": _safe_str(payload.get("request_id") or payload.get("msg_id") or ""),
            "order_id": order_id,
            "order_ref": order_id,
            "passorder_return": result,
            "stage": "QMT_SUBMITTED" if order_id else "SUBMIT_UNKNOWN",
            "submit_result": "KNOWN" if order_id else "UNKNOWN",
            "passorder_started_at_ns": int(passorder_started_at * 1000000000),
            "passorder_finished_at_ns": int(passorder_finished_at * 1000000000),
            "passorder_elapsed_ms": passorder_elapsed_ms,
            "qmt_user_order_id": args["user_order_id"],
            "strategy_name": args["strategy_name"],
            "order_remark": args["order_remark"],
            "symbol": args["symbol"],
            "side": args["side"],
            "quantity": args["quantity"],
            "price": args["price"],
            "price_type": args["price_type"],
            "order_type": args["op_type"],
        }
    if action == "cancel_order":
        if not ENABLE_CANCEL_ORDER:
            return {"status": "failed", "error": "cancel disabled"}
        result = call_cancel(payload, context)
        ok = result is True or (
            not isinstance(result, bool)
            and (result == 0 or _safe_str(result) == "0")
        )
        return {
            "status": "accepted" if ok else "submit_unknown",
            "stage": "QMT_SUBMITTED" if ok else "SUBMIT_UNKNOWN",
            "submit_result": "KNOWN" if ok else "UNKNOWN",
            "request_id": _safe_str(payload.get("request_id") or payload.get("msg_id") or ""),
            "order_id": _safe_str(payload.get("order_id") or ""),
            "order_sysid": _safe_str(payload.get("order_sysid") or ""),
            "cancel_return": result,
        }
    if action in ("fund_transfer", "sync_trade", "smt_negotiate"):
        return {
            "status": "failed",
            "error": "%s is not available in BigQMT file queue helper" % action,
            "code": "UNSUPPORTED_BIGQMT_API",
        }
    return {"status": "failed", "error": "unsupported action: %s" % action, "code": "UNSUPPORTED_ACTION"}


def _response_path(request_id):
    return os.path.join(RESPONSES_DIR, _request_file_key(request_id) + ".json")


def _legacy_response_path(request_id):
    return os.path.join(RESPONSES_DIR, _safe_filename(request_id) + ".json")


def _write_response(request, response):
    request_id = _safe_str(request.get("request_id") or request.get("msg_id") or _make_request_id("response"))
    response["request_id"] = request_id
    response["msg_id"] = _safe_str(request.get("msg_id") or response.get("msg_id") or "")
    if "started_at" not in response:
        response["started_at"] = request.get("_started_at", _now())
    if "finished_at" not in response:
        response["finished_at"] = _now()
    response["elapsed_ms"] = max(0.0, (response["finished_at"] - response["started_at"]) * 1000)
    _apply_request_effect_metadata(response, request)
    _atomic_write_json(_response_path(request_id), response, False)


def _list_processing_recovery_files(folder, max_files):
    files = []
    max_files = max(0, _safe_int(max_files, 0))
    if not max_files:
        return files
    entries = None
    try:
        entries = os.scandir(folder)
        scanned = 0
        for entry in entries:
            if scanned >= max_files:
                break
            scanned += 1
            if entry.is_file():
                files.append(entry.path)
    except Exception:
        return files
    finally:
        try:
            if entries is not None:
                entries.close()
        except Exception:
            pass
    files.sort()
    return files


def _return_processing_request(path, inbox_dir):
    _ensure_dir(inbox_dir)
    name = os.path.basename(path)
    target = os.path.join(inbox_dir, name)
    if os.path.exists(target):
        base, ext = os.path.splitext(name)
        target = os.path.join(
            inbox_dir,
            "%s-recovered-%d-%s%s"
            % (base, int(_now() * 1000), uuid.uuid4().hex[:8], ext),
        )
    os.replace(path, target)
    return target


def _read_artifact_candidate(path, request_id, label):
    try:
        record = _read_json(path)
    except FileNotFoundError:
        return False, None, ""
    except Exception as exc:
        return True, None, "%s unreadable path=%s error=%s" % (label, path, exc)
    if not isinstance(record, dict):
        return True, None, "%s is not a JSON object path=%s" % (label, path)
    embedded_request_id = _safe_str(record.get("request_id"), "")
    if embedded_request_id != request_id:
        return True, None, (
            "%s embedded request_id mismatch expected=%s actual=%s path=%s"
            % (label, request_id, embedded_request_id, path)
        )
    return True, record, ""


def _same_artifact_path(left, right):
    return os.path.normcase(os.path.abspath(left)) == os.path.normcase(os.path.abspath(right))


def _resolve_artifact_pair(request_id, hashed_path, legacy_path, label):
    hashed_present, hashed_record, hashed_error = _read_artifact_candidate(
        hashed_path, request_id, "hashed_%s" % label
    )
    legacy_present = False
    legacy_record = None
    legacy_error = ""
    if not _same_artifact_path(hashed_path, legacy_path):
        legacy_present, legacy_record, legacy_error = _read_artifact_candidate(
            legacy_path, request_id, "legacy_%s" % label
        )
    conflicts = [item for item in (hashed_error, legacy_error) if item]
    if (
        hashed_record is not None
        and legacy_record is not None
        and hashed_record != legacy_record
    ):
        conflicts.append("hashed and legacy %s contents differ" % label)
    paths = []
    if hashed_present:
        paths.append(hashed_path)
    if legacy_present:
        paths.append(legacy_path)
    migrations = []
    record = hashed_record if hashed_record is not None else legacy_record
    if not conflicts:
        if hashed_record is None and legacy_record is not None:
            migrations.append(("replace", legacy_path, hashed_path))
        elif hashed_record is not None and legacy_record is not None:
            migrations.append(("remove", legacy_path, ""))
    return record, "; ".join(conflicts), paths, migrations


def _apply_artifact_migrations(migrations):
    succeeded = True
    for action, source, target in migrations:
        try:
            if action == "replace":
                os.replace(source, target)
            elif action == "remove":
                os.remove(source)
            G_METRICS["legacy_artifact_migrated_total"] = (
                G_METRICS.get("legacy_artifact_migrated_total", 0) + 1
            )
        except FileNotFoundError:
            if action == "replace" and not os.path.isfile(target):
                succeeded = False
        except Exception as exc:
            succeeded = False
            _set_last_error("legacy artifact migration failed path=%s: %s" % (source, exc))
    return succeeded


def _artifact_records(response, guard):
    records = []
    if isinstance(response, dict):
        records.append(response)
    if isinstance(guard, dict):
        records.append(guard)
        guard_response = guard.get("response")
        if isinstance(guard_response, dict):
            records.append(guard_response)
    return records


def _artifact_values(records, keys, include_data=False):
    values = []
    for record in records:
        sources = [record]
        if include_data and isinstance(record.get("data"), dict):
            sources.append(record.get("data"))
        for source in sources:
            for key in keys:
                value = _safe_str(source.get(key), "").strip()
                if value:
                    values.append(value)
    return values


def _required_artifact_value(label, expected, values):
    expected = _safe_str(expected, "").strip()
    distinct = set(values)
    if not expected:
        return "%s missing from incoming request" % label
    if not distinct:
        return "%s missing from persisted artifact" % label
    if distinct != set([expected]):
        return "%s mismatch expected=%s actual=%s" % (
            label, expected, ",".join(sorted(distinct))
        )
    return ""


def _artifact_effect_error(request, response, guard):
    records = _artifact_records(response, guard)
    if not records:
        return ""
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    expected_fingerprint = _safe_str(
        request.get("gateway_effect_fingerprint")
        or payload.get("gateway_effect_fingerprint")
    ).strip()
    stored_fingerprints = _artifact_values(
        records, ("gateway_effect_fingerprint",)
    )
    fingerprint_proven = False
    if expected_fingerprint and stored_fingerprints:
        fingerprint_error = _required_artifact_value(
            "gateway_effect_fingerprint",
            expected_fingerprint,
            stored_fingerprints,
        )
        if fingerprint_error:
            return fingerprint_error
        fingerprint_proven = all(
            _safe_str(record.get("gateway_effect_fingerprint"), "").strip()
            == expected_fingerprint
            for record in records
        )
    elif len(set(stored_fingerprints)) > 1:
        return "gateway_effect_fingerprint differs across persisted artifacts"

    action = _safe_str(request.get("action"), "").strip()
    account_id = _safe_str(request.get("account_id") or ACCOUNT_ID, "").strip()
    error = _required_artifact_value(
        "action", action, _artifact_values(records, ("action",))
    )
    if error:
        return error
    error = _required_artifact_value(
        "account_id", account_id, _artifact_values(records, ("account_id",))
    )
    if error:
        return error
    if fingerprint_proven:
        return ""
    expected_effect, effect_error = _canonical_qmt_effect(request)
    if effect_error or not isinstance(expected_effect, dict):
        return "canonical QMT effect cannot be proven from incoming request: %s" % (
            effect_error or "missing effect"
        )
    expected_effect_hash = _stable_hash(expected_effect)
    for record in records:
        stored_effect = record.get("qmt_effect")
        if not isinstance(stored_effect, dict):
            return "canonical QMT effect missing from persisted artifact"
        if _stable_hash(stored_effect) != expected_effect_hash:
            return "canonical QMT effect mismatch across persisted artifacts"
    if action == "place_order":
        client_order_id = _safe_str(
            request.get("client_order_id") or payload.get("client_order_id")
        ).strip()
        intent_hash = _safe_str(
            request.get("intent_hash") or payload.get("intent_hash")
        ).strip()
        error = _required_artifact_value(
            "client_order_id",
            client_order_id,
            _artifact_values(records, ("client_order_id",)),
        )
        if error:
            return error
        return _required_artifact_value(
            "intent_hash", intent_hash, _artifact_values(records, ("intent_hash",))
        )
    if action == "cancel_order":
        order_sysid = _safe_str(payload.get("order_sysid"), "").strip()
        order_id = _safe_str(payload.get("order_id"), "").strip()
        if order_sysid:
            return _required_artifact_value(
                "cancel_order_sysid",
                order_sysid,
                _artifact_values(
                    records,
                    ("cancel_order_sysid", "order_sysid"),
                    True,
                ),
            )
        return _required_artifact_value(
            "cancel_order_id",
            order_id,
            _artifact_values(
                records,
                ("cancel_order_id", "order_id"),
                True,
            ),
        )
    return "unsupported guarded action=%s" % action


def _backfill_resolved_artifacts(request, response, guard):
    request_id = _safe_str(request.get("request_id") or request.get("msg_id"), "")
    if isinstance(response, dict):
        before = dict(response)
        _apply_request_effect_metadata(response, request)
        if response != before:
            _atomic_write_json(_response_path(request_id), response, False)
    if isinstance(guard, dict):
        before = dict(guard)
        _apply_request_effect_metadata(guard, request)
        guard_response = guard.get("response")
        if isinstance(guard_response, dict):
            guard_response = dict(guard_response)
            _apply_request_effect_metadata(guard_response, request)
            guard["response"] = guard_response
        if guard != before:
            _atomic_write_json(_request_guard_path(request_id), guard, False)


def _resolve_request_artifacts(request):
    request_id = _safe_str(request.get("request_id") or request.get("msg_id"), "")
    response, response_conflict, response_paths, response_migrations = (
        _resolve_artifact_pair(
            request_id,
            _response_path(request_id),
            _legacy_response_path(request_id),
            "response",
        )
    )
    guard, guard_conflict, guard_paths, guard_migrations = _resolve_artifact_pair(
        request_id,
        _request_guard_path(request_id),
        _legacy_request_guard_path(request_id),
        "guard",
    )
    conflicts = [item for item in (response_conflict, guard_conflict) if item]
    if not conflicts and (response is not None or guard is not None):
        effect_error = _artifact_effect_error(request, response, guard)
        if effect_error:
            conflicts.append(effect_error)
    migrations = response_migrations + guard_migrations
    if not conflicts:
        migrated = _apply_artifact_migrations(migrations)
        if migrated:
            try:
                _backfill_resolved_artifacts(request, response, guard)
            except Exception as exc:
                _set_last_error("resolved artifact backfill failed: %s" % exc)
    return {
        "response": response,
        "guard": guard,
        "conflict": "; ".join(conflicts),
        "paths": response_paths + guard_paths,
    }


def _isolate_artifact_paths(paths):
    isolated = set()
    _ensure_dir(FAILED_DIR)
    for path in paths:
        normalized = os.path.normcase(os.path.abspath(path))
        if normalized in isolated or not os.path.isfile(path):
            continue
        isolated.add(normalized)
        try:
            source_kind = _safe_filename(os.path.basename(os.path.dirname(path)))
            target = os.path.join(
                FAILED_DIR,
                "artifact-%s-%s-%s"
                % (source_kind, uuid.uuid4().hex[:12], os.path.basename(path)),
            )
            os.replace(path, target)
        except Exception as exc:
            _set_last_error("artifact isolation failed path=%s: %s" % (path, exc))


def _fail_closed_artifact_conflict(request, resolution):
    global G_RECONCILE_NEEDED
    _isolate_artifact_paths(resolution.get("paths") or [])
    response = _processing_recovery_unknown_response(
        request,
        {"state": "artifact_conflict"},
    )
    response["artifact_conflict"] = _safe_str(resolution.get("conflict"))
    response["data"]["recovery_reason"] = "request_artifact_conflict"
    response["data"]["artifact_conflict"] = response["artifact_conflict"]
    request_id = _safe_str(request.get("request_id") or request.get("msg_id"), "")
    _write_request_guard(request_id, "unknown", request, response)
    G_RECONCILE_NEEDED = True
    G_METRICS["request_artifact_conflict_total"] = (
        G_METRICS.get("request_artifact_conflict_total", 0) + 1
    )
    return response


def _queued_command_sibling_paths(request_id, current_path):
    hashed_name = _request_file_key(request_id) + ".json"
    legacy_name = _safe_filename(request_id) + ".json"
    candidates = [
        current_path,
        os.path.join(INBOX_COMMANDS_DIR, hashed_name),
        os.path.join(INBOX_COMMANDS_DIR, legacy_name),
        os.path.join(PROCESSING_COMMANDS_DIR, hashed_name),
        os.path.join(PROCESSING_COMMANDS_DIR, legacy_name),
        os.path.join(INBOX_DIR, hashed_name),
        os.path.join(INBOX_DIR, legacy_name),
    ]
    result = []
    seen = set()
    for path in candidates:
        normalized = os.path.normcase(os.path.abspath(path))
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(path)
    return result


def _is_ascii_digits(value, min_length=1, max_length=0):
    value = _safe_str(value, "")
    if len(value) < min_length or (max_length and len(value) > max_length):
        return False
    return all(ch in "0123456789" for ch in value)


def _is_lower_hex(value, length):
    value = _safe_str(value, "")
    return len(value) == length and all(ch in "0123456789abcdef" for ch in value)


def _is_regular_command_sibling_stem(stem, request_id):
    hashed = _request_file_key(request_id)
    legacy = _safe_filename(request_id)
    if stem == hashed or stem == legacy:
        return True
    return (
        len(stem) == 85
        and _is_ascii_digits(stem[:20], 20, 20)
        and stem[20:21] == "-"
        and stem[21:] == hashed
    )


def _is_command_sibling_filename(filename, request_id):
    if not filename.endswith(".json"):
        return False
    stem = filename[:-5]
    if _is_regular_command_sibling_stem(stem, request_id):
        return True
    while True:
        base, marker, suffix = stem.rpartition("-recovered-")
        if not marker:
            return False
        parts = suffix.split("-")
        if (
            len(parts) != 2
            or not _is_ascii_digits(parts[0], 10, 20)
            or not _is_lower_hex(parts[1], 8)
        ):
            return False
        stem = base
        if _is_regular_command_sibling_stem(stem, request_id):
            return True


def _discover_queued_command_siblings(request_id, current_path):
    paths = []
    seen = set()
    candidate_budget_exhausted = False
    incomplete_reasons = []

    def add_incomplete(reason):
        reason = _safe_str(reason, "queued sibling scan incomplete")
        if reason in incomplete_reasons:
            return
        if len(incomplete_reasons) >= MAX_COMMAND_SIBLING_PREFLIGHT_FILES:
            return
        incomplete_reasons.append(reason)

    def add_path(path):
        nonlocal candidate_budget_exhausted
        normalized = os.path.normcase(os.path.abspath(path))
        if normalized in seen:
            return
        if len(paths) >= MAX_COMMAND_SIBLING_PREFLIGHT_FILES:
            candidate_budget_exhausted = True
            return
        seen.add(normalized)
        paths.append(path)

    # The current processing artifact is already open and parsed by the caller.
    # All other candidates come exclusively from the complete directory scans
    # below.  Fixed-name isfile probes cannot prove absence and only duplicate
    # NTFS metadata work on the normal path.
    add_path(current_path)

    scanned = 0
    scan_budget_exhausted = False
    for folder in (INBOX_COMMANDS_DIR, PROCESSING_COMMANDS_DIR, INBOX_DIR):
        entries = None
        try:
            entries = os.scandir(folder)
        except Exception as exc:
            add_incomplete(
                "queued sibling scan incomplete: directory open failed "
                "folder=%s error=%s" % (folder, exc)
            )
            continue
        try:
            try:
                iterator = iter(entries)
            except Exception as exc:
                add_incomplete(
                    "queued sibling scan incomplete: directory iteration "
                    "setup failed folder=%s error=%s" % (folder, exc)
                )
                continue
            while True:
                try:
                    entry = next(iterator)
                except StopIteration:
                    break
                except Exception as exc:
                    add_incomplete(
                        "queued sibling scan incomplete: directory iteration "
                        "failed folder=%s error=%s" % (folder, exc)
                    )
                    break
                # Fetch one entry past the budget so an exactly-full, complete
                # directory is not incorrectly reported as truncated.
                if scanned >= MAX_COMMAND_SIBLING_SCAN_ENTRIES_PER_REQUEST:
                    scan_budget_exhausted = True
                    break
                scanned += 1
                try:
                    entry_name = entry.name
                except Exception as exc:
                    add_incomplete(
                        "queued sibling scan incomplete: entry name unreadable "
                        "folder=%s error=%s" % (folder, exc)
                    )
                    continue
                if not _is_command_sibling_filename(entry_name, request_id):
                    continue
                try:
                    is_file = entry.is_file()
                except Exception as exc:
                    add_incomplete(
                        "queued sibling scan incomplete: matching entry metadata "
                        "unreadable folder=%s name=%s error=%s"
                        % (folder, entry_name, exc)
                    )
                    continue
                if not is_file:
                    continue
                try:
                    add_path(entry.path)
                except Exception as exc:
                    add_incomplete(
                        "queued sibling scan incomplete: matching entry path "
                        "unreadable folder=%s name=%s error=%s"
                        % (folder, entry_name, exc)
                    )
        finally:
            try:
                if entries is not None:
                    entries.close()
            except Exception:
                pass
        if scan_budget_exhausted:
            break
    G_METRICS["queued_sibling_scanned_total"] = (
        G_METRICS.get("queued_sibling_scanned_total", 0) + scanned
    )
    if scan_budget_exhausted:
        G_METRICS["queued_sibling_scan_budget_exhausted_total"] = (
            G_METRICS.get("queued_sibling_scan_budget_exhausted_total", 0) + 1
        )
    if candidate_budget_exhausted:
        G_METRICS["queued_sibling_candidate_budget_exhausted_total"] = (
            G_METRICS.get("queued_sibling_candidate_budget_exhausted_total", 0) + 1
        )
    if incomplete_reasons:
        G_METRICS["queued_sibling_scan_incomplete_total"] = (
            G_METRICS.get("queued_sibling_scan_incomplete_total", 0) + 1
        )
    return {
        "paths": paths,
        "scanned": scanned,
        "scan_budget_exhausted": scan_budget_exhausted,
        "candidate_budget_exhausted": candidate_budget_exhausted,
        "scan_incomplete": bool(incomplete_reasons),
        "incomplete_reasons": incomplete_reasons,
    }


def _queued_identity_value_error(label, expected, actual):
    expected = _safe_str(expected, "").strip()
    actual = _safe_str(actual, "").strip()
    if not expected or not actual:
        return "%s cannot be proven across queued siblings" % label
    if expected != actual:
        return "%s differs across queued siblings" % label
    return ""


def _queued_request_effect_error(expected, candidate):
    expected_action = _safe_str(expected.get("action"), "").strip()
    candidate_action = _safe_str(candidate.get("action"), "").strip()
    error = _queued_identity_value_error(
        "action", expected_action, candidate_action
    )
    if error:
        return error
    error = _queued_identity_value_error(
        "account_id",
        expected.get("account_id"),
        candidate.get("account_id"),
    )
    if error:
        return error
    expected_payload = (
        expected.get("payload") if isinstance(expected.get("payload"), dict) else {}
    )
    candidate_payload = (
        candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    )
    expected_fingerprint = _safe_str(
        expected.get("gateway_effect_fingerprint")
        or expected_payload.get("gateway_effect_fingerprint")
    ).strip()
    candidate_fingerprint = _safe_str(
        candidate.get("gateway_effect_fingerprint")
        or candidate_payload.get("gateway_effect_fingerprint")
    ).strip()
    if expected_fingerprint and candidate_fingerprint:
        return _queued_identity_value_error(
            "gateway_effect_fingerprint",
            expected_fingerprint,
            candidate_fingerprint,
        )
    expected_effect, expected_effect_error = _canonical_qmt_effect(expected)
    candidate_effect, candidate_effect_error = _canonical_qmt_effect(candidate)
    if expected_effect_error or candidate_effect_error:
        return "canonical QMT effect cannot be proven across queued siblings: %s" % (
            expected_effect_error or candidate_effect_error
        )
    if (
        not isinstance(expected_effect, dict)
        or not isinstance(candidate_effect, dict)
        or _stable_hash(expected_effect) != _stable_hash(candidate_effect)
    ):
        return "canonical QMT effect differs across queued siblings"
    if expected_action == "place_order":
        error = _queued_identity_value_error(
            "client_order_id",
            expected.get("client_order_id")
            or expected_payload.get("client_order_id"),
            candidate.get("client_order_id")
            or candidate_payload.get("client_order_id"),
        )
        if error:
            return error
        return _queued_identity_value_error(
            "intent_hash",
            expected.get("intent_hash") or expected_payload.get("intent_hash"),
            candidate.get("intent_hash") or candidate_payload.get("intent_hash"),
        )
    if expected_action == "cancel_order":
        expected_sysid = _safe_str(expected_payload.get("order_sysid"), "").strip()
        candidate_sysid = _safe_str(candidate_payload.get("order_sysid"), "").strip()
        if expected_sysid or candidate_sysid:
            return _queued_identity_value_error(
                "cancel_order_sysid", expected_sysid, candidate_sysid
            )
        return _queued_identity_value_error(
            "cancel_order_id",
            expected_payload.get("order_id"),
            candidate_payload.get("order_id"),
        )
    return "unsupported queued guarded action=%s" % expected_action


def _preflight_command_siblings(request, current_path):
    request_id = _safe_str(request.get("request_id") or request.get("msg_id"), "")
    current_normalized = os.path.normcase(os.path.abspath(current_path))
    paths = []
    conflicts = []
    discovery = _discover_queued_command_siblings(request_id, current_path)
    if discovery.get("scan_budget_exhausted"):
        conflicts.append("queued sibling scan budget exhausted")
    if discovery.get("candidate_budget_exhausted"):
        conflicts.append("queued sibling candidate budget exhausted")
    conflicts.extend(discovery.get("incomplete_reasons") or [])
    for path in discovery.get("paths") or []:
        paths.append(path)
        normalized = os.path.normcase(os.path.abspath(path))
        if normalized == current_normalized:
            candidate = request
        else:
            try:
                candidate = _read_json(path)
            except FileNotFoundError:
                # A sibling may be consumed or isolated after scandir.  Only an
                # explicit not-found race is safe to treat as disappeared.
                continue
            except Exception as exc:
                conflicts.append("queued sibling unreadable: %s" % exc)
                continue
            if not isinstance(candidate, dict):
                conflicts.append("queued sibling is not a JSON object")
                continue
        embedded_request_id = _safe_str(
            candidate.get("request_id") or candidate.get("msg_id"), ""
        )
        if embedded_request_id != request_id:
            conflicts.append("queued sibling embedded request_id mismatch")
            continue
        effect_error = _queued_request_effect_error(request, candidate)
        if effect_error:
            conflicts.append(effect_error)
    if conflicts:
        # Preserve fixed hashed/legacy artifact isolation on the rare conflict
        # path without issuing redundant metadata probes before a normal QMT
        # dispatch.  _isolate_artifact_paths performs its own guarded checks.
        known = {
            os.path.normcase(os.path.abspath(path)) for path in paths
        }
        for path in _queued_command_sibling_paths(request_id, current_path):
            normalized = os.path.normcase(os.path.abspath(path))
            if normalized not in known:
                known.add(normalized)
                paths.append(path)
    return {
        "conflict": "; ".join(sorted(set(conflicts))),
        "paths": paths,
    }


def _fail_closed_queued_sibling_conflict(request, resolution):
    global G_RECONCILE_NEEDED
    request_id = _safe_str(request.get("request_id") or request.get("msg_id"), "")
    paths = list(resolution.get("paths") or [])
    paths.extend([
        _response_path(request_id),
        _legacy_response_path(request_id),
        _request_guard_path(request_id),
        _legacy_request_guard_path(request_id),
    ])
    _isolate_artifact_paths(paths)
    response = _processing_recovery_unknown_response(
        request,
        {"state": "queued_sibling_conflict"},
    )
    response["queued_sibling_conflict"] = _safe_str(resolution.get("conflict"))
    response["data"]["recovery_reason"] = "queued_sibling_conflict"
    response["data"]["queued_sibling_conflict"] = response[
        "queued_sibling_conflict"
    ]
    _write_request_guard(request_id, "unknown", request, response)
    G_RECONCILE_NEEDED = True
    G_METRICS["queued_sibling_conflict_total"] = (
        G_METRICS.get("queued_sibling_conflict_total", 0) + 1
    )
    return response


def _processing_recovery_unknown_response(request, guard):
    request_id = _safe_str(request.get("request_id"), "")
    action = _safe_str(request.get("action"), "")
    guard_state = _safe_str((guard or {}).get("state"), "unknown")
    return make_response(
        True,
        {
            "status": "submit_unknown",
            "stage": "SUBMIT_UNKNOWN",
            "request_id": request_id,
            "reconcile_required": True,
            "recovery_reason": "processing_recovered_with_guard",
            "recovery_guard_state": guard_state,
        },
        "",
        "RECONCILE_REQUIRED",
        request,
        action,
    )


def _recover_processing_file(path, inbox_dir):
    global G_RECONCILE_NEEDED
    try:
        request = _read_json(path)
        if not isinstance(request, dict):
            raise ValueError("processing request must be a JSON object")
        request_id = _safe_str(request.get("request_id") or request.get("msg_id"), "")
        if not request_id:
            raise ValueError("processing request_id is required")
        request["request_id"] = request_id
    except Exception:
        _move_file(path, FAILED_DIR)
        return "failed"

    action = _safe_str(request.get("action"), "")
    if action not in ("place_order", "cancel_order"):
        _return_processing_request(path, inbox_dir)
        return "returned"

    resolution = _resolve_request_artifacts(request)
    if resolution.get("conflict"):
        response = _fail_closed_artifact_conflict(request, resolution)
        _write_response(request, response)
        os.remove(path)
        return "uncertain"

    stable_response = resolution.get("response")
    if stable_response is not None:
        os.remove(path)
        return "completed"

    guard = resolution.get("guard")
    if not isinstance(guard, dict):
        _return_processing_request(path, inbox_dir)
        return "returned"

    guard_response = guard.get("response") if isinstance(guard, dict) else None
    if isinstance(guard_response, dict) and guard_response:
        recovered_response = dict(guard_response)
        _write_response(request, recovered_response)
        result = recovered_response.get("data") if isinstance(recovered_response.get("data"), dict) else {}
        if result.get("status") == "submit_unknown":
            G_RECONCILE_NEEDED = True
        os.remove(path)
        return "completed"

    response = _processing_recovery_unknown_response(request, guard)
    _write_request_guard(request_id, "unknown", request, response)
    _write_response(request, response)
    G_RECONCILE_NEEDED = True
    os.remove(path)
    return "uncertain"


def _recover_processing_requests(max_files=MAX_PROCESSING_RECOVERY_FILES_PER_TICK):
    global G_PROCESSING_RECOVERY_FOLDER_CURSOR
    limit = max(1, _safe_int(max_files, MAX_PROCESSING_RECOVERY_FILES_PER_TICK))
    folders = [
        (PROCESSING_COMMANDS_DIR, INBOX_COMMANDS_DIR),
        (PROCESSING_QUERIES_DIR, INBOX_QUERIES_DIR),
    ]
    start = G_PROCESSING_RECOVERY_FOLDER_CURSOR % len(folders)
    ordered_folders = folders[start:] + folders[:start]
    G_PROCESSING_RECOVERY_FOLDER_CURSOR = (start + 1) % len(folders)
    processed = 0
    for folder, inbox_dir in ordered_folders:
        remaining = limit - processed
        if remaining <= 0:
            break
        for path in _list_processing_recovery_files(folder, remaining):
            try:
                outcome = _recover_processing_file(path, inbox_dir)
                key = "processing_recovery_%s_total" % outcome
                G_METRICS[key] = G_METRICS.get(key, 0) + 1
            except Exception as exc:
                _set_last_error("processing recovery failed path=%s: %s" % (path, exc))
                G_METRICS["processing_recovery_error_total"] = (
                    G_METRICS.get("processing_recovery_error_total", 0) + 1
                )
            processed += 1
            G_METRICS["processing_recovery_total"] = (
                G_METRICS.get("processing_recovery_total", 0) + 1
            )
    return processed


def _list_request_files(queue_kind="all", max_files=32):
    folders = []
    if queue_kind in ("command", "all"):
        folders.append(INBOX_COMMANDS_DIR)
    if queue_kind in ("query", "all"):
        folders.append(INBOX_QUERIES_DIR)
    if queue_kind in ("command", "all"):
        folders.append(INBOX_DIR)  # v1 compatibility queue
    files = []
    max_files = max(1, _safe_int(max_files, 32))
    scanned = 0
    try:
        for folder in folders:
            entries = None
            try:
                entries = os.scandir(folder)
            except Exception as exc:
                G_METRICS["request_queue_scan_unreadable_total"] = (
                    G_METRICS.get("request_queue_scan_unreadable_total", 0) + 1
                )
                raise RuntimeError(
                    "request queue directory unreadable folder=%s error=%s"
                    % (folder, exc)
                )
            try:
                for entry in entries:
                    scanned += 1
                    if scanned > MAX_REQUEST_QUEUE_SCAN_ENTRIES_PER_CYCLE:
                        G_METRICS["request_queue_scan_limit_exceeded_total"] = (
                            G_METRICS.get(
                                "request_queue_scan_limit_exceeded_total", 0
                            )
                            + 1
                        )
                        raise RuntimeError(
                            "request queue scan limit exceeded limit=%d"
                            % MAX_REQUEST_QUEUE_SCAN_ENTRIES_PER_CYCLE
                        )
                    if not entry.name.endswith(".json"):
                        continue
                    try:
                        is_file = entry.is_file()
                    except Exception as exc:
                        G_METRICS["request_queue_scan_unreadable_total"] = (
                            G_METRICS.get(
                                "request_queue_scan_unreadable_total", 0
                            )
                            + 1
                        )
                        raise RuntimeError(
                            "request queue entry unreadable path=%s error=%s"
                            % (entry.path, exc)
                        )
                    if is_file:
                        files.append(entry.path)
            finally:
                try:
                    entries.close()
                except Exception:
                    pass
    finally:
        G_METRICS["request_queue_scanned_total"] = (
            G_METRICS.get("request_queue_scanned_total", 0) + scanned
        )
        G_METRICS["request_queue_last_scanned"] = scanned
    files.sort(
        key=lambda path: (
            os.path.basename(path),
            os.path.normcase(os.path.abspath(path)),
        )
    )
    return files[:max_files]


def _guard_duplicate_response(request, request_id, action):
    duplicate, guard = _duplicate_state(request)
    if duplicate is not None:
        return duplicate
    if not isinstance(guard, dict):
        return None
    state = _safe_str(guard.get("state"), "processing")
    if state == "processing":
        data = {
            "status": "submit_unknown",
            "stage": "SUBMIT_UNKNOWN",
            "request_id": request_id,
            "idempotent": True,
            "duplicate_stage": "helper_guard_processing",
        }
        return make_response(True, data, "", "", request, action)
    return make_response(False, {}, "duplicate request state=%s" % state, "DUPLICATE_REQUEST", request, action)


def _request_identity_error(request):
    request_account_id = _safe_str(request.get("account_id"), "").strip()
    request_account_type = _safe_str(request.get("account_type"), "").strip()
    expected_account_id = _safe_str(ACCOUNT_ID, "").strip()
    expected_account_type = _safe_str(ACCOUNT_TYPE, "").strip()
    if not request_account_id or not request_account_type:
        return "request account_id and account_type are required"
    if request_account_id != expected_account_id:
        return "request account_id does not match helper account"
    if request_account_type != expected_account_type:
        return "request account_type does not match helper account"
    return ""


def drain_file_requests(context, limit, queue_kind="all", budget_ms=0.0):
    global G_METRICS
    recovered = _recover_processing_requests(MAX_PROCESSING_RECOVERY_FILES_PER_TICK)
    if recovered >= MAX_PROCESSING_RECOVERY_FILES_PER_TICK:
        G_METRICS["processing_recovery_saturated_total"] = (
            G_METRICS.get("processing_recovery_saturated_total", 0) + 1
        )
        return 0
    cycle_started = time.perf_counter()
    processed = 0
    processing_dir = PROCESSING_COMMANDS_DIR if queue_kind == "command" else PROCESSING_QUERIES_DIR
    scan_limit = max(limit, min(64, limit * 4))
    try:
        request_files = _list_request_files(queue_kind, scan_limit)
    except Exception as exc:
        _set_last_error("request queue scan failed closed: %s" % exc)
        G_METRICS["request_queue_scan_failed_total"] = (
            G_METRICS.get("request_queue_scan_failed_total", 0) + 1
        )
        return 0
    for path in request_files:
        if processed >= limit:
            break
        if budget_ms and (time.perf_counter() - cycle_started) * 1000 >= budget_ms:
            break
        started = _now()
        processing_path = os.path.join(processing_dir, os.path.basename(path))
        try:
            os.replace(path, processing_path)
        except Exception:
            continue
        request = {}
        response = None
        qmt_dispatched = False
        try:
            request = _read_json(processing_path)
            request["_started_at"] = started
            request_id = _safe_str(request.get("request_id") or request.get("msg_id") or _make_request_id("req"))
            request["request_id"] = request_id
            action = _safe_str(request.get("action"), "")
            deadline_at = _safe_float(request.get("deadline_at"), 0.0)
            guarded_action = action in ("place_order", "cancel_order")
            identity_error = _request_identity_error(request)
            duplicate = None
            if not identity_error and guarded_action:
                sibling_resolution = _preflight_command_siblings(
                    request, processing_path
                )
                if sibling_resolution.get("conflict"):
                    duplicate = _fail_closed_queued_sibling_conflict(
                        request, sibling_resolution
                    )
                else:
                    duplicate = _guard_duplicate_response(
                        request, request_id, action
                    )
            if identity_error:
                response = make_response(
                    False,
                    {"status": "failed"},
                    identity_error,
                    "ACCOUNT_MISMATCH",
                    request,
                    action,
                )
            elif duplicate is not None:
                response = duplicate
            elif deadline_at and deadline_at < _now():
                response = make_response(False, {}, "request deadline exceeded", "DEADLINE_EXCEEDED", request, action)
            elif (
                queue_kind == "query"
                and _is_continuous_trading_window()
                and not ALLOW_QMT_QUERY_DURING_TRADING
            ):
                response = make_response(
                    False,
                    {},
                    "native QMT query is disabled during continuous trading; use gateway cache",
                    "QUERY_DEFERRED_DURING_TRADING",
                    request,
                    action,
                )
            else:
                if guarded_action:
                    _write_request_guard(request_id, "processing", request)
                    G_PROCESSING_REQUEST_IDS.add(request_id)
                try:
                    qmt_dispatched = guarded_action
                    data = dispatch_qmt_action(context, action, request.get("payload") if isinstance(request.get("payload"), dict) else {})
                    failed = isinstance(data, dict) and data.get("status") == "failed"
                    if isinstance(data, dict):
                        data.setdefault("queue_wait_ms", max(0.0, (started - _safe_float(request.get("created_at"), started)) * 1000))
                    response = make_response(not failed, data, data.get("error", "") if failed else "", data.get("code", "") if failed else "", request, action)
                finally:
                    if guarded_action:
                        G_PROCESSING_REQUEST_IDS.discard(request_id)
                if guarded_action:
                    result = response.get("data") if isinstance(response.get("data"), dict) else {}
                    state = "rejected" if response.get("ok") is False else ("unknown" if result.get("status") == "submit_unknown" else "submitted")
                    _write_request_guard(request_id, state, request, response)
            _write_response(request, response)
            try:
                os.remove(processing_path)
            except Exception:
                pass
            G_METRICS["requests_ok"] += 1 if response.get("ok") else 0
            G_METRICS["requests_failed"] += 0 if response.get("ok") else 1
        except Exception as exc:
            _set_last_error(exc)
            G_METRICS["requests_failed"] += 1
            try:
                if not request:
                    request = {"request_id": _make_request_id("invalid-request"), "_started_at": started}
                if qmt_dispatched:
                    response = make_response(True, {
                        "status": "submit_unknown",
                        "stage": "SUBMIT_UNKNOWN",
                        "traceback": traceback.format_exc(),
                    }, "", "SUBMIT_STATE_UNCERTAIN", request, request.get("action", ""))
                else:
                    response = make_response(False, {"traceback": traceback.format_exc()}, exc, "QMT_ERROR", request, request.get("action", ""))
                _write_response(request, response)
                request_id = _safe_str(request.get("request_id"))
                if request_id and request.get("action") in ("place_order", "cancel_order"):
                    _write_request_guard(request_id, "unknown" if qmt_dispatched else "rejected", request, response)
            except Exception:
                pass
            try:
                _move_file(processing_path, FAILED_DIR)
            except Exception:
                pass
        finally:
            processed += 1
            G_METRICS["requests_total"] += 1
            G_METRICS["last_request_elapsed_ms"] = max(0.0, (_now() - started) * 1000)
    return processed


def health_payload():
    command_cycle_age_ms = max(0.0, (_now() - G_LAST_COMMAND_CYCLE_AT) * 1000) if G_LAST_COMMAND_CYCLE_AT else 1000000000.0
    return {
        "ready": G_CONTEXT is not None and G_ACCOUNT_READY and G_RUN_TIME_READY and command_cycle_age_ms <= 250.0,
        "context_ready": G_CONTEXT is not None,
        "account_ready": G_ACCOUNT_READY,
        "name": HELPER_NAME,
        "account_id": ACCOUNT_ID,
        "account_name": ACCOUNT_NAME,
        "account_type": ACCOUNT_TYPE,
        "qmt_account_type": qmt_account_type(),
        "runtime_dir": RUNTIME_DIR,
        "build_id": BUILD_ID,
        "protocol_version": 2,
        "handlebar_count": G_HANDLEBAR_COUNT,
        "last_snapshot_at": G_LAST_SNAPSHOT_AT,
        "run_time_enabled": ENABLE_RUN_TIME_TIMER,
        "run_time_ready": G_RUN_TIME_READY,
        "command_interval_ms": COMMAND_INTERVAL_MS,
        "query_interval_ms": QUERY_INTERVAL_MS,
        "reconcile_interval_seconds": RECONCILE_INTERVAL_SECONDS,
        "maintenance_interval_seconds": MAINTENANCE_INTERVAL_SECONDS,
        "readiness_interval_ms": READINESS_INTERVAL_MS,
        "allow_qmt_query_during_trading": ALLOW_QMT_QUERY_DURING_TRADING,
        "last_command_cycle_at": G_LAST_COMMAND_CYCLE_AT,
        "last_command_cycle_age_ms": command_cycle_age_ms,
        "last_callback_source": G_LAST_CALLBACK_SOURCE,
        "reconcile_needed": G_RECONCILE_NEEDED,
        "trading_enabled": ENABLE_TRADING,
        "cancel_order_enabled": ENABLE_CANCEL_ORDER,
        "last_error": G_LAST_ERROR,
        "timestamp": _now(),
    }


def write_state(state="running"):
    payload = health_payload()
    payload["state"] = state
    _atomic_write_json(STATE_FILE, payload)


def write_heartbeat(state="running", last_drain_count=0):
    payload = health_payload()
    payload["state"] = state
    payload["last_handlebar_at"] = _now()
    payload["last_drain_count"] = last_drain_count
    _atomic_write_json(HEARTBEAT_FILE, payload)


def write_metrics():
    payload = dict(G_METRICS)
    payload["account_id"] = ACCOUNT_ID
    payload["updated_at"] = _now()
    _atomic_write_json(METRICS_FILE, payload)


def write_readiness():
    command_age_ms = max(0.0, (_now() - G_LAST_COMMAND_CYCLE_AT) * 1000) if G_LAST_COMMAND_CYCLE_AT else 1000000000.0
    _atomic_write_json(READINESS_FILE, {
        "protocol_version": 2,
        "build_id": BUILD_ID,
        "name": HELPER_NAME,
        "account_id": ACCOUNT_ID,
        "account_type": ACCOUNT_TYPE,
        "runtime_dir": RUNTIME_DIR,
        "command_interval_ms": COMMAND_INTERVAL_MS,
        "run_time_ready": G_RUN_TIME_READY,
        "last_command_cycle_at": G_LAST_COMMAND_CYCLE_AT,
        "last_command_cycle_age_ms": command_age_ms,
        "updated_at": _now(),
    })


def _write_event(event_type, data, source="snapshot_diff"):
    global G_EVENT_SEQ
    G_EVENT_SEQ += 1
    event_id = "evt-%d-%06d" % (int(_now() * 1000), G_EVENT_SEQ)
    payload = {
        "protocol_version": 2,
        "version": 2,
        "event_id": event_id,
        "event_seq": G_EVENT_SEQ,
        "account_id": ACCOUNT_ID,
        "account_type": ACCOUNT_TYPE,
        "type": event_type,
        "data": data or {},
        "created_at": _now(),
        "source_ts_ns": int(_now() * 1000000000),
        "source": source,
    }
    _atomic_write_json(os.path.join(EVENTS_LIVE_DIR, event_id + ".json"), payload)
    return payload


def _write_callback_event(event_type, data, source):
    """Single-attempt callback writer: never query, sleep or retry in QMT callback."""
    global G_EVENT_SEQ, G_METRICS, G_RECONCILE_NEEDED
    entered_at = _now()
    G_EVENT_SEQ += 1
    event_id = "evt-%d-%06d" % (int(entered_at * 1000), G_EVENT_SEQ)
    payload = {
        "protocol_version": 2,
        "version": 2,
        "event_id": event_id,
        "event_seq": G_EVENT_SEQ,
        "account_id": ACCOUNT_ID,
        "account_type": ACCOUNT_TYPE,
        "type": event_type,
        "data": data or {},
        "created_at": entered_at,
        "source_ts_ns": int(entered_at * 1000000000),
        "source": source,
    }
    path = os.path.join(EVENTS_LIVE_DIR, event_id + ".json")
    tmp = "%s.%s.tmp" % (path, os.getpid())
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=_json_default, sort_keys=True)
            f.write("\n")
        os.replace(tmp, path)
        G_METRICS["callback_events_total"] = G_METRICS.get("callback_events_total", 0) + 1
        return True
    except Exception as exc:
        G_METRICS["callback_write_failure_total"] = G_METRICS.get("callback_write_failure_total", 0) + 1
        G_RECONCILE_NEEDED = True
        _set_last_error("callback event write failed: %s" % exc)
        try:
            os.remove(tmp)
        except Exception:
            pass
        return False


def order_callback(ContextInfo, orderInfo):
    _write_callback_event("ORDER_UPDATE", normalize_order(orderInfo), "qmt_order_callback")


def deal_callback(ContextInfo, dealInfo):
    _write_callback_event("TRADE_NOTIFY", normalize_trade(dealInfo), "qmt_deal_callback")


def orderError_callback(ContextInfo, orderArgs, errMsg):
    raw = object_to_raw(orderArgs)
    data = {
        "error_msg": _safe_str(errMsg),
        "reject_reason": _safe_str(errMsg),
        "qmt_user_order_id": _safe_str(pick(raw, ["qmt_user_order_id", "m_strUserOrderId", "m_strRemark"], "")),
        "order_id": _safe_str(pick(raw, ["order_id", "m_strOrderID", "m_strOrderSysID"], "")),
        "raw": raw,
    }
    _write_callback_event("ORDER_ERROR", data, "qmt_order_error_callback")


def maybe_write_snapshot_and_events(force=False):
    global G_LAST_SNAPSHOT_AT, G_LAST_ASSET_HASH, G_LAST_POSITIONS_HASH, G_LAST_ORDERS
    global G_SEEN_TRADE_KEYS, G_METRICS, G_BASELINE_READY, G_RECONCILE_NEEDED
    if not force and _now() - G_LAST_SNAPSHOT_AT < RECONCILE_INTERVAL_SECONDS:
        return
    started = _now()
    snapshot = query_snapshot()
    _atomic_write_json(os.path.join(SNAPSHOTS_DIR, "latest.json"), snapshot)
    G_LAST_SNAPSHOT_AT = _now()
    G_METRICS["snapshots_total"] += 1
    G_METRICS["last_snapshot_elapsed_ms"] = max(0.0, (_now() - started) * 1000)

    asset = snapshot.get("asset") or {}
    asset_hash = _stable_hash(asset)
    positions = snapshot.get("positions") or []
    positions_hash = _stable_hash(sorted(positions, key=lambda item: _safe_str(item.get("stock_code") or item.get("symbol"))))

    if not G_BASELINE_READY:
        G_LAST_ASSET_HASH = asset_hash
        G_LAST_POSITIONS_HASH = positions_hash
        baseline_orders = {}
        for order in snapshot.get("orders") or []:
            key = "|".join([
                _safe_str(order.get("order_id") or order.get("order_sysid")),
                _safe_str(order.get("stock_code") or order.get("symbol")),
            ])
            if key == "|":
                key = _stable_hash(order)
            baseline_orders[key] = _stable_hash(order)
        G_LAST_ORDERS = baseline_orders
        for trade in snapshot.get("trades") or []:
            G_SEEN_TRADE_KEYS.add("|".join([
                _safe_str(trade.get("trade_date")),
                _safe_str(trade.get("trade_id") or trade.get("traded_id")),
                _safe_str(trade.get("order_id")),
                _safe_str(trade.get("stock_code") or trade.get("symbol")),
                _safe_str(trade.get("traded_volume") or trade.get("quantity")),
                _safe_str(trade.get("traded_price") or trade.get("price")),
            ]))
        G_BASELINE_READY = True
        G_RECONCILE_NEEDED = False
        return

    if asset and asset_hash != G_LAST_ASSET_HASH:
        G_LAST_ASSET_HASH = asset_hash
        _write_event("ASSET_UPDATE", asset)

    if positions_hash != G_LAST_POSITIONS_HASH:
        G_LAST_POSITIONS_HASH = positions_hash
        _write_event("POSITIONS_SNAPSHOT", {"positions": positions, "asset": asset})

    current_orders = {}
    for order in snapshot.get("orders") or []:
        key = "|".join([
            _safe_str(order.get("order_id") or order.get("order_sysid")),
            _safe_str(order.get("stock_code") or order.get("symbol")),
        ])
        if key == "|":
            key = _stable_hash(order)
        order_hash = _stable_hash(order)
        current_orders[key] = order_hash
        if G_LAST_ORDERS.get(key) != order_hash:
            _write_event("ORDER_UPDATE", order)
    G_LAST_ORDERS = current_orders

    for trade in snapshot.get("trades") or []:
        key = "|".join([
            _safe_str(trade.get("trade_date")),
            _safe_str(trade.get("trade_id") or trade.get("traded_id")),
            _safe_str(trade.get("order_id")),
            _safe_str(trade.get("stock_code") or trade.get("symbol")),
            _safe_str(trade.get("traded_volume") or trade.get("quantity")),
            _safe_str(trade.get("traded_price") or trade.get("price")),
        ])
        if key in G_SEEN_TRADE_KEYS:
            continue
        G_SEEN_TRADE_KEYS.add(key)
        _write_event("TRADE_NOTIFY", trade)
    G_RECONCILE_NEEDED = False


def _register_run_time_timers(ContextInfo):
    global G_RUN_TIME_READY
    G_RUN_TIME_READY = False
    if not ENABLE_RUN_TIME_TIMER:
        _log("run_time timer disabled")
        return
    runner = getattr(ContextInfo, "run_time", None)
    if runner is None:
        _set_last_error("ContextInfo.run_time is not available")
        _log("run_time setup skipped: ContextInfo.run_time is not available")
        return
    try:
        schedules = (
            ("bigqmt_command_timer", "%dnMilliSecond" % max(10, COMMAND_INTERVAL_MS)),
            ("bigqmt_query_timer", "%dnMilliSecond" % max(100, QUERY_INTERVAL_MS)),
            ("bigqmt_heartbeat_timer", "%dnSecond" % max(1, HEARTBEAT_INTERVAL_SECONDS)),
            ("bigqmt_reconcile_timer", "%dnSecond" % max(1, RECONCILE_INTERVAL_SECONDS)),
            ("bigqmt_maintenance_timer", "%dnSecond" % max(1, MAINTENANCE_INTERVAL_SECONDS)),
            ("bigqmt_readiness_timer", "%dnMilliSecond" % max(50, READINESS_INTERVAL_MS)),
        )
        for func_name, interval in schedules:
            runner(func_name, interval, "2020-01-01 00:00:00")
        G_RUN_TIME_READY = True
        _log("run_time setup ok schedules=%s" % ",".join([x[1] for x in schedules]))
    except Exception as exc:
        G_RUN_TIME_READY = False
        _set_last_error("run_time setup failed: %s" % exc)
        _log("run_time setup failed: %s" % exc)


def _run_command_cycle(ContextInfo, source):
    global G_CONTEXT, G_HANDLEBAR_COUNT, G_LAST_CALLBACK_SOURCE, G_LAST_COMMAND_CYCLE_AT
    global G_LAST_COMMAND_ACTIVITY_AT, G_LAST_HEARTBEAT_AT, G_COMMAND_CYCLE_RUNNING
    if G_COMMAND_CYCLE_RUNNING:
        G_METRICS["command_timer_reentry_total"] = G_METRICS.get("command_timer_reentry_total", 0) + 1
        return 0
    G_COMMAND_CYCLE_RUNNING = True
    cycle_started = time.perf_counter()
    try:
        G_CONTEXT = ContextInfo
        G_HANDLEBAR_COUNT += 1
        G_LAST_CALLBACK_SOURCE = source
        drained = drain_file_requests(ContextInfo, MAX_COMMANDS_PER_TICK, "command", COMMAND_BUDGET_MS)
        finished_at = _now()
        G_LAST_COMMAND_CYCLE_AT = finished_at
        if drained:
            G_LAST_COMMAND_ACTIVITY_AT = finished_at
        G_METRICS["command_cycles_total"] = G_METRICS.get("command_cycles_total", 0) + 1
        elapsed_ms = (time.perf_counter() - cycle_started) * 1000
        G_METRICS["last_command_cycle_elapsed_ms"] = elapsed_ms
        if elapsed_ms > COMMAND_BUDGET_MS:
            G_METRICS["command_timer_overrun_total"] = G_METRICS.get("command_timer_overrun_total", 0) + 1
        return drained
    finally:
        G_COMMAND_CYCLE_RUNNING = False


def _directory_has_json(folder):
    entries = None
    try:
        entries = os.scandir(folder)
        for entry in entries:
            if entry.name.endswith(".json") and entry.is_file():
                return True
    except Exception:
        pass
    finally:
        try:
            if entries is not None:
                entries.close()
        except Exception:
            pass
    return False


def _is_continuous_trading_window():
    current = time.localtime()
    if current.tm_wday >= 5:
        return False
    minute = current.tm_hour * 60 + current.tm_min
    return (9 * 60 <= minute <= 11 * 60 + 35) or (12 * 60 + 55 <= minute <= 15 * 60 + 5)


def _run_query_cycle(ContextInfo, source):
    global G_CONTEXT, G_LAST_CALLBACK_SOURCE, G_QUERY_CYCLE_RUNNING
    if G_QUERY_CYCLE_RUNNING:
        G_METRICS["query_timer_reentry_total"] = G_METRICS.get("query_timer_reentry_total", 0) + 1
        return 0
    if _directory_has_json(INBOX_COMMANDS_DIR) or _directory_has_json(PROCESSING_COMMANDS_DIR):
        G_METRICS["query_deferred_for_command_total"] = G_METRICS.get("query_deferred_for_command_total", 0) + 1
        return 0
    if G_LAST_COMMAND_ACTIVITY_AT and _now() - G_LAST_COMMAND_ACTIVITY_AT < LOW_PRIORITY_QUIET_SECONDS:
        G_METRICS["query_deferred_for_command_total"] = G_METRICS.get("query_deferred_for_command_total", 0) + 1
        return 0
    G_QUERY_CYCLE_RUNNING = True
    G_CONTEXT = ContextInfo
    G_LAST_CALLBACK_SOURCE = source
    try:
        drained = drain_file_requests(ContextInfo, MAX_QUERIES_PER_TICK, "query", 0.0)
        G_METRICS["query_cycles_total"] = G_METRICS.get("query_cycles_total", 0) + 1
        return drained
    finally:
        G_QUERY_CYCLE_RUNNING = False


def _run_reconcile_cycle(ContextInfo, source):
    global G_CONTEXT, G_LAST_CALLBACK_SOURCE, G_RECONCILE_NEEDED
    if _directory_has_json(INBOX_COMMANDS_DIR) or _directory_has_json(PROCESSING_COMMANDS_DIR):
        G_METRICS["reconcile_deferred_for_command_total"] = G_METRICS.get("reconcile_deferred_for_command_total", 0) + 1
        return 0
    if G_LAST_COMMAND_ACTIVITY_AT and _now() - G_LAST_COMMAND_ACTIVITY_AT < LOW_PRIORITY_QUIET_SECONDS:
        G_METRICS["reconcile_deferred_for_command_total"] = G_METRICS.get("reconcile_deferred_for_command_total", 0) + 1
        return 0
    if _is_continuous_trading_window():
        if not ALLOW_QMT_QUERY_DURING_TRADING or not G_RECONCILE_NEEDED:
            G_METRICS["periodic_reconcile_skipped_in_session_total"] = G_METRICS.get("periodic_reconcile_skipped_in_session_total", 0) + 1
            return 0
    G_CONTEXT = ContextInfo
    G_LAST_CALLBACK_SOURCE = source
    try:
        maybe_write_snapshot_and_events(True)
    except Exception as exc:
        _set_last_error(exc)
        G_RECONCILE_NEEDED = True
    return 1


def _path_is_within_runtime(path):
    try:
        runtime = os.path.normcase(os.path.realpath(os.path.abspath(RUNTIME_DIR)))
        candidate = os.path.normcase(os.path.realpath(os.path.abspath(path)))
        return os.path.commonpath([runtime, candidate]) == runtime
    except Exception:
        return False


def _is_callback_atomic_tmp_filename(filename):
    if not filename.endswith(".tmp"):
        return False
    body = filename[:-4]
    parts = body.rsplit(".", 1)
    if len(parts) != 2 or not _is_ascii_digits(parts[1], 1, 10):
        return False
    target = parts[0]
    if not target.endswith(".json"):
        return False
    event_parts = target[:-5].split("-")
    return (
        len(event_parts) == 3
        and event_parts[0] == "evt"
        and _is_ascii_digits(event_parts[1], 10, 20)
        and _is_ascii_digits(event_parts[2], 6, 6)
    )


def _is_atomic_json_tmp_filename(filename, allow_callback=False):
    if not filename.endswith(".tmp"):
        return False
    body = filename[:-4]
    parts = body.rsplit(".", 2)
    if (
        len(parts) == 3
        and len(parts[0]) > 5
        and parts[0].endswith(".json")
        and _is_ascii_digits(parts[1], 1, 10)
        and _is_ascii_digits(parts[2], 10, 20)
    ):
        return True
    return bool(allow_callback and _is_callback_atomic_tmp_filename(filename))


def _cleanup_stale_atomic_tmp_files(
    folder, cutoff, max_delete=32, max_scan=256, allow_callback=False
):
    deleted = 0
    scanned = 0
    max_delete = max(0, _safe_int(max_delete, 0))
    max_scan = max(0, _safe_int(max_scan, 0))
    if (
        not max_delete
        or not max_scan
        or not _path_is_within_runtime(folder)
    ):
        return deleted, scanned
    entries = None
    try:
        entries = os.scandir(folder)
    except Exception:
        return deleted, scanned
    try:
        for entry in entries:
            if scanned >= max_scan:
                break
            scanned += 1
            if not _is_atomic_json_tmp_filename(entry.name, allow_callback):
                continue
            try:
                if (
                    not entry.is_file(follow_symlinks=False)
                    or entry.stat(follow_symlinks=False).st_mtime >= cutoff
                ):
                    continue
                os.remove(entry.path)
                deleted += 1
                if deleted >= max_delete:
                    break
            except Exception:
                continue
    finally:
        try:
            entries.close()
        except Exception:
            pass
    return deleted, scanned


def _run_stale_atomic_tmp_cleanup():
    global G_STALE_TMP_CLEANUP_FOLDER_CURSOR
    folders = [
        RUNTIME_DIR,
        INBOX_DIR,
        INBOX_COMMANDS_DIR,
        INBOX_QUERIES_DIR,
        PROCESSING_DIR,
        PROCESSING_COMMANDS_DIR,
        PROCESSING_QUERIES_DIR,
        RESPONSES_DIR,
        REQUEST_STATE_DIR,
        EVENTS_DIR,
        EVENTS_LIVE_DIR,
        EVENTS_FAILED_DIR,
        SNAPSHOTS_DIR,
        ARCHIVE_DIR,
        DONE_DIR,
        FAILED_DIR,
    ]
    start = G_STALE_TMP_CLEANUP_FOLDER_CURSOR % len(folders)
    ordered_folders = folders[start:] + folders[:start]
    G_STALE_TMP_CLEANUP_FOLDER_CURSOR = (start + 1) % len(folders)
    remaining = max(1, MAX_STALE_ATOMIC_TMP_FILES_PER_TICK)
    scan_remaining = max(1, MAX_STALE_ATOMIC_TMP_SCAN_ENTRIES_PER_TICK)
    deleted = 0
    scanned = 0
    live_events = os.path.normcase(os.path.abspath(EVENTS_LIVE_DIR))
    for folder in ordered_folders:
        count, folder_scanned = _cleanup_stale_atomic_tmp_files(
            folder,
            _now() - STALE_ATOMIC_TMP_AGE_SECONDS,
            remaining,
            scan_remaining,
            os.path.normcase(os.path.abspath(folder)) == live_events,
        )
        deleted += count
        scanned += folder_scanned
        remaining -= count
        scan_remaining -= folder_scanned
        if remaining <= 0 or scan_remaining <= 0:
            break
    G_METRICS["stale_tmp_cleanup_deleted_total"] = (
        G_METRICS.get("stale_tmp_cleanup_deleted_total", 0) + deleted
    )
    G_METRICS["stale_tmp_cleanup_scanned_total"] = (
        G_METRICS.get("stale_tmp_cleanup_scanned_total", 0) + scanned
    )
    G_METRICS["stale_tmp_cleanup_last_deleted"] = deleted
    G_METRICS["stale_tmp_cleanup_last_scanned"] = scanned
    G_METRICS["stale_tmp_cleanup_folder_cursor"] = (
        G_STALE_TMP_CLEANUP_FOLDER_CURSOR
    )
    if scan_remaining <= 0:
        G_METRICS["stale_tmp_cleanup_scan_budget_exhausted_total"] = (
            G_METRICS.get(
                "stale_tmp_cleanup_scan_budget_exhausted_total", 0
            )
            + 1
        )
    return deleted, scanned


def _cleanup_old_files(folder, cutoff, preserve_guards=False, max_delete=100, max_scan=400):
    deleted = 0
    scanned = 0
    max_delete = max(0, _safe_int(max_delete, 0))
    max_scan = max(0, _safe_int(max_scan, 0))
    if not max_delete or not max_scan:
        return deleted, scanned
    entries = None
    try:
        entries = os.scandir(folder)
    except Exception:
        return deleted, scanned
    try:
        for entry in entries:
            if scanned >= max_scan:
                break
            scanned += 1
            try:
                if (
                    not entry.name.endswith(".json")
                    or not entry.is_file()
                    or entry.stat().st_mtime >= cutoff
                ):
                    continue
                path = entry.path
                if preserve_guards:
                    guard = _read_json(path)
                    if _safe_str(guard.get("state")) in ("processing", "unknown"):
                        continue
                os.remove(path)
                deleted += 1
                if deleted >= max_delete:
                    break
            except Exception:
                continue
    finally:
        try:
            entries.close()
        except Exception:
            pass
    return deleted, scanned


def _run_maintenance_cycle(ContextInfo, source):
    global G_CONTEXT, G_LAST_CALLBACK_SOURCE, G_CLEANUP_FOLDER_CURSOR
    if _directory_has_json(INBOX_COMMANDS_DIR) or _directory_has_json(PROCESSING_COMMANDS_DIR):
        G_METRICS["maintenance_deferred_for_command_total"] = G_METRICS.get("maintenance_deferred_for_command_total", 0) + 1
        return 0
    if G_LAST_COMMAND_ACTIVITY_AT and _now() - G_LAST_COMMAND_ACTIVITY_AT < LOW_PRIORITY_QUIET_SECONDS:
        G_METRICS["maintenance_deferred_for_command_total"] = G_METRICS.get("maintenance_deferred_for_command_total", 0) + 1
        return 0
    G_CONTEXT = ContextInfo
    G_LAST_CALLBACK_SOURCE = source
    ensure_runtime_dirs()
    started = time.perf_counter()
    _run_stale_atomic_tmp_cleanup()
    cutoff = _now() - MAX_FILE_AGE_SECONDS
    remaining = max(1, MAX_CLEANUP_FILES_PER_TICK)
    scan_remaining = max(1, MAX_CLEANUP_SCAN_ENTRIES_PER_TICK)
    deleted = 0
    scanned = 0
    cleanup_folders = [
        (EVENTS_FAILED_DIR, cutoff, False),
        (DONE_DIR, cutoff, False),
        (FAILED_DIR, cutoff, False),
        (REQUEST_STATE_DIR, _now() - REQUEST_GUARD_TTL_SECONDS, True),
    ]
    start = G_CLEANUP_FOLDER_CURSOR % len(cleanup_folders)
    ordered_folders = cleanup_folders[start:] + cleanup_folders[:start]
    # Move the first folder every successful maintenance cycle. If one large
    # directory consumes the full scan budget, the others still get priority
    # on later cycles.
    G_CLEANUP_FOLDER_CURSOR = (start + 1) % len(cleanup_folders)
    for folder, folder_cutoff, preserve_guards in ordered_folders:
        count, folder_scanned = _cleanup_old_files(
            folder,
            folder_cutoff,
            preserve_guards,
            remaining,
            scan_remaining,
        )
        deleted += count
        scanned += folder_scanned
        remaining -= count
        scan_remaining -= folder_scanned
        if remaining <= 0 or scan_remaining <= 0:
            break
    G_METRICS["cleanup_deleted_total"] = G_METRICS.get("cleanup_deleted_total", 0) + deleted
    G_METRICS["cleanup_scanned_total"] = G_METRICS.get("cleanup_scanned_total", 0) + scanned
    G_METRICS["cleanup_last_deleted"] = deleted
    G_METRICS["cleanup_last_scanned"] = scanned
    G_METRICS["cleanup_folder_cursor"] = G_CLEANUP_FOLDER_CURSOR
    if scan_remaining <= 0:
        G_METRICS["cleanup_scan_budget_exhausted_total"] = (
            G_METRICS.get("cleanup_scan_budget_exhausted_total", 0) + 1
        )
    G_METRICS["cleanup_elapsed_ms"] = (time.perf_counter() - started) * 1000
    _safe_runtime_write("write_heartbeat", write_heartbeat, "running", 0)
    _safe_runtime_write("write_state", write_state, "running")
    _safe_runtime_write("write_metrics", write_metrics)
    _safe_runtime_write("write_readiness", write_readiness)
    return 1


def init(ContextInfo):
    global G_CONTEXT, G_ACCOUNT_READY
    G_CONTEXT = ContextInfo
    G_ACCOUNT_READY = False
    ensure_runtime_dirs()
    _recover_processing_requests(MAX_PROCESSING_RECOVERY_FILES_PER_TICK)
    _log("init account=%s type=%s runtime_dir=%s build=%s" % (ACCOUNT_ID, ACCOUNT_TYPE, RUNTIME_DIR, BUILD_ID))
    try:
        setter = getattr(ContextInfo, "set_account", None)
        if setter is not None:
            setter(ACCOUNT_ID)
        G_ACCOUNT_READY = True
        _log("account binding ok account=%s" % ACCOUNT_ID)
    except Exception as exc:
        _set_last_error(exc)
        _log("ContextInfo.set_account failed: %s" % exc)
    _safe_runtime_write("write_state", write_state, "running")
    _safe_runtime_write("write_heartbeat", write_heartbeat, "running", 0)
    _safe_runtime_write("write_metrics", write_metrics)
    _register_run_time_timers(ContextInfo)
    _safe_runtime_write("write_heartbeat", write_heartbeat, "running", 0)
    _safe_runtime_write("write_readiness", write_readiness)


def after_init(ContextInfo):
    global G_CONTEXT
    G_CONTEXT = ContextInfo
    _run_command_cycle(ContextInfo, "after_init")
    _safe_runtime_write("write_readiness", write_readiness)
    _run_query_cycle(ContextInfo, "after_init")
    _run_reconcile_cycle(ContextInfo, "after_init")


def handlebar(ContextInfo):
    # Compatibility fallback for terminals where a run_time callback is delayed.
    _run_command_cycle(ContextInfo, "handlebar")


def bigqmt_file_queue_timer(ContextInfo):
    # v1 callback name retained for existing deployed strategy configurations.
    _run_command_cycle(ContextInfo, "run_time")


def bigqmt_command_timer(ContextInfo):
    _run_command_cycle(ContextInfo, "command_timer")


def bigqmt_query_timer(ContextInfo):
    _run_query_cycle(ContextInfo, "query_timer")


def bigqmt_heartbeat_timer(ContextInfo):
    global G_CONTEXT, G_LAST_CALLBACK_SOURCE, G_LAST_HEARTBEAT_AT
    G_CONTEXT = ContextInfo
    G_LAST_CALLBACK_SOURCE = "heartbeat_timer"
    G_LAST_HEARTBEAT_AT = _now()
    _safe_runtime_write("write_heartbeat", write_heartbeat, "running", 0)


def bigqmt_reconcile_timer(ContextInfo):
    _run_reconcile_cycle(ContextInfo, "reconcile_timer")


def bigqmt_maintenance_timer(ContextInfo):
    _run_maintenance_cycle(ContextInfo, "maintenance_timer")


def bigqmt_readiness_timer(ContextInfo):
    global G_CONTEXT, G_LAST_CALLBACK_SOURCE
    G_CONTEXT = ContextInfo
    G_LAST_CALLBACK_SOURCE = "readiness_timer"
    _safe_runtime_write("write_readiness", write_readiness)


def stop(ContextInfo):
    global G_CONTEXT
    G_CONTEXT = ContextInfo
    ensure_runtime_dirs()
    _safe_runtime_write("write_state", write_state, "stopped")
    _safe_runtime_write("write_heartbeat", write_heartbeat, "stopped", 0)
    _safe_runtime_write("write_metrics", write_metrics)
    _safe_runtime_write("write_readiness", write_readiness)
    _log("stopped")
