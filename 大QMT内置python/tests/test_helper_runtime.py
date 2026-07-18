import ast
import hashlib
import importlib
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
import bigqmt_file_queue_helper as helper


class DummyContext:
    def __init__(self):
        self.account_id = None
        self.run_time_calls = []

    def set_account(self, account_id):
        self.account_id = account_id

    def run_time(self, name, interval, start):
        self.run_time_calls.append((name, interval, start))


class HelperRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bigqmt-embedded-test-")
        root = self.tmp
        helper.RUNTIME_DIR = root
        helper.INBOX_DIR = os.path.join(root, "inbox")
        helper.INBOX_COMMANDS_DIR = os.path.join(root, "inbox", "commands")
        helper.INBOX_QUERIES_DIR = os.path.join(root, "inbox", "queries")
        helper.PROCESSING_DIR = os.path.join(root, "processing")
        helper.PROCESSING_COMMANDS_DIR = os.path.join(root, "processing", "commands")
        helper.PROCESSING_QUERIES_DIR = os.path.join(root, "processing", "queries")
        helper.RESPONSES_DIR = os.path.join(root, "responses")
        helper.REQUEST_STATE_DIR = os.path.join(root, "request_state")
        helper.EVENTS_DIR = os.path.join(root, "events")
        helper.EVENTS_LIVE_DIR = os.path.join(root, "events", "live")
        helper.EVENTS_FAILED_DIR = os.path.join(root, "events", "failed")
        helper.EVENTS_DONE_DIR = os.path.join(root, "events", "done")
        helper.SNAPSHOTS_DIR = os.path.join(root, "snapshots")
        helper.ARCHIVE_DIR = os.path.join(root, "archive")
        helper.DONE_DIR = os.path.join(root, "archive", "done")
        helper.FAILED_DIR = os.path.join(root, "archive", "failed")
        helper.STATE_FILE = os.path.join(root, "state.json")
        helper.HEARTBEAT_FILE = os.path.join(root, "heartbeat.json")
        helper.METRICS_FILE = os.path.join(root, "metrics.json")
        helper.READINESS_FILE = os.path.join(root, "readiness.json")
        helper.ensure_runtime_dirs()
        helper.G_CONTEXT = DummyContext()
        # Runtime behavior tests explicitly opt in; the source template itself
        # remains fail-closed so an accidental direct load cannot trade.
        helper.ENABLE_TRADING = True
        helper.ENABLE_CANCEL_ORDER = True
        helper.G_ACCOUNT_READY = True
        helper.G_RUN_TIME_READY = True
        helper.G_COMMAND_CYCLE_RUNNING = False
        helper.G_QUERY_CYCLE_RUNNING = False
        helper.G_LAST_COMMAND_ACTIVITY_AT = 0.0
        helper.G_LAST_COMMAND_CYCLE_AT = time.time()
        helper.G_PROCESSING_REQUEST_IDS = set()
        helper.G_BASELINE_READY = False
        helper.G_RECONCILE_NEEDED = False
        helper.G_LAST_ASSET_HASH = ""
        helper.G_LAST_POSITIONS_HASH = ""
        helper.G_LAST_ORDERS = {}
        helper.G_SEEN_TRADE_KEYS = set()
        helper.G_EVENT_SEQ = 0
        helper.G_METRICS = {
            "requests_total": 0, "requests_ok": 0, "requests_failed": 0,
            "snapshots_total": 0, "command_cycles_total": 0,
            "query_cycles_total": 0, "command_timer_overrun_total": 0,
            "callback_events_total": 0, "last_request_elapsed_ms": 0.0,
            "last_snapshot_elapsed_ms": 0.0,
        }

    def tearDown(self):
        for name in ("passorder", "cancel"):
            if hasattr(helper, name):
                delattr(helper, name)
        shutil.rmtree(self.tmp, ignore_errors=True)
        importlib.reload(helper)

    def write_request(self, folder, request_id, action, payload=None, identity=None, deadline=None):
        request = {
            "protocol_version": 2, "request_id": request_id, "msg_id": request_id,
            "action": action, "payload": payload or {}, "created_at": time.time(),
            "deadline_at": time.time() + 5 if deadline is None else deadline,
        }
        request.update(
            {"account_id": helper.ACCOUNT_ID, "account_type": helper.ACCOUNT_TYPE}
            if identity is None else identity
        )
        helper._atomic_write_json(os.path.join(folder, request_id + ".json"), request)

    def response(self, request_id):
        return json.loads(Path(helper.RESPONSES_DIR, request_id + ".json").read_text(encoding="utf-8"))

    def order_payload(self, request_id):
        return {
            "request_id": request_id, "qmt_user_order_id": "XL-" + request_id,
            "symbol": "600000.SH", "side": "BUY", "quantity": 100,
            "price": 10.0, "price_type": 11,
        }

    def test_01_full_template_hash_ascii_python36(self):
        data = (SRC / "bigqmt_file_queue_helper.py").read_bytes()
        baseline = json.loads((ROOT / "SOURCE_BASELINE.json").read_text(encoding="utf-8"))
        self.assertEqual(hashlib.sha256(data).hexdigest().upper(), baseline["hardened_helper_sha256"])
        self.assertEqual(baseline["upstream_helper_sha256"], "9B7E272D0C5FF1DB881DB22C64F4624B2E398AB9189E04D2CE0258E921BB89AB")
        source = data.decode("ascii")
        self.assertNotIn("os.environ.get", source)
        self.assertNotIn("os.getenv(", source)
        tree = ast.parse(source, feature_version=(3, 6))
        assignments = {
            node.targets[0].id: node.value
            for node in tree.body
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        }
        for name in ("ENABLE_TRADING", "ENABLE_CANCEL_ORDER"):
            self.assertIsInstance(assignments[name], ast.Constant)
            self.assertIs(assignments[name].value, False)

    def test_02_complete_function_surface(self):
        names = (
            "query_snapshot", "query_account", "query_positions", "query_orders",
            "query_trades", "query_order_status", "call_passorder", "call_cancel",
            "drain_file_requests", "order_callback", "deal_callback",
            "orderError_callback", "maybe_write_snapshot_and_events",
            "bigqmt_command_timer", "bigqmt_query_timer", "bigqmt_reconcile_timer",
            "bigqmt_maintenance_timer", "bigqmt_readiness_timer",
        )
        self.assertTrue(all(callable(getattr(helper, name, None)) for name in names))

    def test_03_init_exact_timers_and_identity_files(self):
        context = DummyContext()
        helper.G_CONTEXT = None
        helper.G_ACCOUNT_READY = False
        helper.G_RUN_TIME_READY = False
        helper.init(context)
        self.assertEqual([(a, b) for a, b, _ in context.run_time_calls], [
            ("bigqmt_command_timer", "50nMilliSecond"),
            ("bigqmt_query_timer", "500nMilliSecond"),
            ("bigqmt_heartbeat_timer", "1nSecond"),
            ("bigqmt_reconcile_timer", "30nSecond"),
            ("bigqmt_maintenance_timer", "60nSecond"),
            ("bigqmt_readiness_timer", "100nMilliSecond"),
        ])
        expected = {
            "name": helper.HELPER_NAME, "account_id": helper.ACCOUNT_ID,
            "account_type": helper.ACCOUNT_TYPE, "runtime_dir": helper.RUNTIME_DIR,
            "build_id": helper.BUILD_ID, "protocol_version": 2,
            "command_interval_ms": 50,
        }
        for path in (helper.STATE_FILE, helper.HEARTBEAT_FILE, helper.READINESS_FILE):
            document = json.loads(Path(path).read_text(encoding="utf-8"))
            self.assertEqual({key: document.get(key) for key in expected}, expected)

    def test_04_command_timer_does_not_query_snapshot(self):
        original = helper.query_snapshot
        helper.query_snapshot = lambda: (_ for _ in ()).throw(AssertionError("query on command path"))
        try:
            self.write_request(helper.INBOX_COMMANDS_DIR, "health", "health")
            helper.bigqmt_command_timer(DummyContext())
        finally:
            helper.query_snapshot = original
        self.assertTrue(self.response("health")["ok"])

    def test_05_query_yields_to_pending_command(self):
        self.write_request(helper.INBOX_COMMANDS_DIR, "command", "health")
        self.write_request(helper.INBOX_QUERIES_DIR, "query", "health")
        helper.bigqmt_query_timer(DummyContext())
        self.assertTrue(Path(helper.INBOX_QUERIES_DIR, "query.json").exists())

    def test_06_processing_guard_never_replays_passorder(self):
        calls = []
        helper.passorder = lambda *args: calls.append(args)
        payload = self.order_payload("guarded")
        helper._write_request_guard("guarded", "processing", {"payload": payload})
        self.write_request(helper.INBOX_COMMANDS_DIR, "guarded", "place_order", payload)
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(calls, [])
        self.assertEqual(self.response("guarded")["data"]["stage"], "SUBMIT_UNKNOWN")

    def test_07_wrong_account_never_calls_passorder_or_guard(self):
        calls = []
        helper.passorder = lambda *args: calls.append(args)
        self.write_request(
            helper.INBOX_COMMANDS_DIR, "wrong-account", "place_order",
            self.order_payload("wrong-account"),
            identity={"account_id": "wrong", "account_type": helper.ACCOUNT_TYPE},
        )
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(calls, [])
        self.assertEqual(self.response("wrong-account")["code"], "ACCOUNT_MISMATCH")
        self.assertFalse(Path(helper._request_guard_path("wrong-account")).exists())

    def test_08_wrong_type_and_missing_identity_never_call_qmt(self):
        cancel_calls, query_calls = [], []
        helper.cancel = lambda *args: cancel_calls.append(args)
        original = helper.query_account
        helper.query_account = lambda payload: query_calls.append(payload)
        wrong_type = "CREDIT" if helper.ACCOUNT_TYPE == "STOCK" else "STOCK"
        try:
            self.write_request(helper.INBOX_COMMANDS_DIR, "bad-type", "cancel_order", {"order_id": "O1"},
                               identity={"account_id": helper.ACCOUNT_ID, "account_type": wrong_type})
            self.write_request(helper.INBOX_QUERIES_DIR, "missing-id", "account", {}, identity={})
            helper.bigqmt_command_timer(DummyContext())
            helper.G_LAST_COMMAND_ACTIVITY_AT = 0.0
            helper.bigqmt_query_timer(DummyContext())
        finally:
            helper.query_account = original
        self.assertEqual(cancel_calls, [])
        self.assertEqual(query_calls, [])
        self.assertEqual(self.response("bad-type")["code"], "ACCOUNT_MISMATCH")
        self.assertEqual(self.response("missing-id")["code"], "ACCOUNT_MISMATCH")

    def test_09_expired_order_never_reaches_passorder(self):
        calls = []
        helper.passorder = lambda *args: calls.append(args)
        self.write_request(helper.INBOX_COMMANDS_DIR, "expired", "place_order",
                           self.order_payload("expired"), deadline=time.time() - 1)
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(calls, [])
        self.assertEqual(self.response("expired")["code"], "DEADLINE_EXCEEDED")

    def test_10_passorder_type_error_never_retries(self):
        calls = []
        def fail(*args):
            calls.append(args)
            raise TypeError("after native entry")
        helper.passorder = fail
        with self.assertRaises(TypeError):
            helper.call_passorder(helper.build_order_args(self.order_payload("single")), DummyContext())
        self.assertEqual(len(calls), 1)

    def test_11_zero_passorder_is_submit_unknown(self):
        helper.passorder = lambda *args: 0
        self.write_request(helper.INBOX_COMMANDS_DIR, "zero", "place_order", self.order_payload("zero"))
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(self.response("zero")["data"]["stage"], "SUBMIT_UNKNOWN")

    def test_12_callbacks_write_atomic_incremental_events(self):
        helper.order_callback(None, {"m_strOrderID": "O1", "m_strStockCode": "600000.SH", "m_strOrderStatus": "50"})
        helper.deal_callback(None, {"m_strTradeID": "T1", "m_strOrderID": "O1", "m_strStockCode": "600000.SH", "m_nTradedVolume": 100})
        events = [json.loads(path.read_text(encoding="utf-8")) for path in Path(helper.EVENTS_LIVE_DIR).glob("*.json")]
        self.assertEqual({event["type"] for event in events}, {"ORDER_UPDATE", "TRADE_NOTIFY"})
        self.assertEqual(list(Path(helper.EVENTS_LIVE_DIR).glob("*.tmp")), [])

    def test_13_cancel_signature_fallback(self):
        calls = []
        def cancel(*args):
            calls.append(args)
            if len(calls) == 1:
                raise TypeError("signature")
            return 0
        helper.cancel = cancel
        self.assertEqual(helper.call_cancel({"order_id": "O1"}, DummyContext()), 0)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
