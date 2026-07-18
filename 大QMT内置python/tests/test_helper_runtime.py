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
from unittest import mock


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
        helper.G_CLEANUP_FOLDER_CURSOR = 0
        helper.G_STALE_TMP_CLEANUP_FOLDER_CURSOR = 0
        helper.G_PROCESSING_RECOVERY_FOLDER_CURSOR = 0
        helper.G_METRICS = {
            "requests_total": 0, "requests_ok": 0, "requests_failed": 0,
            "snapshots_total": 0, "command_cycles_total": 0,
            "query_cycles_total": 0, "command_timer_overrun_total": 0,
            "maintenance_deferred_for_command_total": 0,
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
            "legacy_artifact_migrated_total": 0,
            "request_artifact_conflict_total": 0,
            "queued_sibling_conflict_total": 0,
            "queued_sibling_scan_incomplete_total": 0,
            "request_queue_scan_failed_total": 0,
            "request_queue_scan_limit_exceeded_total": 0,
            "request_queue_scan_unreadable_total": 0,
            "callback_events_total": 0, "last_request_elapsed_ms": 0.0,
            "last_snapshot_elapsed_ms": 0.0,
        }

    def tearDown(self):
        for name in ("passorder", "cancel"):
            if hasattr(helper, name):
                delattr(helper, name)
        shutil.rmtree(self.tmp, ignore_errors=True)
        importlib.reload(helper)

    def write_request(
        self, folder, request_id, action, payload=None, identity=None,
        deadline=None, filename=None,
    ):
        request = {
            "protocol_version": 2, "request_id": request_id, "msg_id": request_id,
            "action": action, "payload": payload or {}, "created_at": time.time(),
            "deadline_at": time.time() + 5 if deadline is None else deadline,
        }
        request["gateway_effect_fingerprint"] = (payload or {}).get(
            "gateway_effect_fingerprint", ""
        )
        request.update(
            {"account_id": helper.ACCOUNT_ID, "account_type": helper.ACCOUNT_TYPE}
            if identity is None else identity
        )
        helper._atomic_write_json(
            os.path.join(folder, (filename or request_id) + ".json"), request
        )
        return request

    def response(self, request_id):
        return json.loads(Path(helper._response_path(request_id)).read_text(encoding="utf-8"))

    def order_payload(self, request_id):
        return {
            "request_id": request_id, "qmt_user_order_id": "XL-" + request_id,
            "client_order_id": request_id,
            "intent_hash": "intent:" + hashlib.sha256(request_id.encode("utf-8")).hexdigest(),
            "gateway_effect_fingerprint": "sha256:" + hashlib.sha256(
                ("effect:" + request_id).encode("utf-8")
            ).hexdigest(),
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
        expected_low_latency = {
            "MAX_COMMANDS_PER_TICK": 4,
            "COMMAND_BUDGET_MS": 15.0,
            "COMMAND_INTERVAL_MS": 25,
            "BUILD_ID": "xuanling_bigqmt_file_queue_helper_20260718_low_latency_v12_fail_closed_sibling_scan",
        }
        for name, expected in expected_low_latency.items():
            self.assertIsInstance(assignments[name], ast.Constant)
            self.assertEqual(assignments[name].value, expected)

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
            ("bigqmt_command_timer", "25nMilliSecond"),
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
            "command_interval_ms": 25,
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

    def test_05_maintenance_yields_to_pending_and_recent_commands(self):
        original_cleanup = helper._cleanup_old_files
        original_runtime_write = helper._safe_runtime_write
        cleanup_calls = []
        runtime_write_calls = []

        def record_cleanup(*args, **kwargs):
            cleanup_calls.append((args, kwargs))
            return 0, 0

        helper._cleanup_old_files = record_cleanup
        helper._safe_runtime_write = lambda *args: runtime_write_calls.append(args)
        try:
            self.write_request(helper.INBOX_COMMANDS_DIR, "maintenance-inbox", "health")
            self.assertEqual(helper._run_maintenance_cycle(DummyContext(), "test"), 0)
            self.assertEqual(cleanup_calls, [])
            self.assertEqual(runtime_write_calls, [])
            Path(helper.INBOX_COMMANDS_DIR, "maintenance-inbox.json").unlink()

            self.write_request(helper.PROCESSING_COMMANDS_DIR, "maintenance-processing", "health")
            self.assertEqual(helper._run_maintenance_cycle(DummyContext(), "test"), 0)
            self.assertEqual(cleanup_calls, [])
            self.assertEqual(runtime_write_calls, [])
            Path(helper.PROCESSING_COMMANDS_DIR, "maintenance-processing.json").unlink()

            helper.G_LAST_COMMAND_ACTIVITY_AT = time.time()
            self.assertEqual(helper._run_maintenance_cycle(DummyContext(), "test"), 0)
            self.assertEqual(cleanup_calls, [])
            self.assertEqual(runtime_write_calls, [])
            self.assertEqual(helper.G_METRICS["maintenance_deferred_for_command_total"], 3)

            helper.G_LAST_COMMAND_ACTIVITY_AT = time.time() - helper.LOW_PRIORITY_QUIET_SECONDS - 1
            self.assertEqual(helper._run_maintenance_cycle(DummyContext(), "test"), 1)
            self.assertGreater(len(cleanup_calls), 0)
            self.assertEqual(len(runtime_write_calls), 4)
            self.assertEqual(helper.G_METRICS["maintenance_deferred_for_command_total"], 3)
        finally:
            helper._cleanup_old_files = original_cleanup
            helper._safe_runtime_write = original_runtime_write

    def test_06_atomic_write_fast_path_and_missing_parent_recovery(self):
        fast_parent = Path(self.tmp, "atomic-fast")
        fast_parent.mkdir()
        fast_path = fast_parent / "value.json"
        with mock.patch.object(
            helper,
            "_ensure_dir",
            side_effect=AssertionError("existing hot-path parent must not be probed"),
        ):
            helper._atomic_write_json(str(fast_path), {"b": 2, "a": 1}, False)
        self.assertEqual(json.loads(fast_path.read_text(encoding="utf-8")), {"a": 1, "b": 2})
        self.assertNotIn(": ", fast_path.read_text(encoding="utf-8"))

        recovered_path = Path(self.tmp, "atomic-recovered", "value.json")
        original_ensure_dir = helper._ensure_dir
        recovered_parents = []

        def record_recovery(path):
            recovered_parents.append(path)
            return original_ensure_dir(path)

        with mock.patch.object(helper, "_ensure_dir", side_effect=record_recovery):
            helper._atomic_write_json(str(recovered_path), {"ready": True}, False)
        self.assertEqual(recovered_parents, [str(recovered_path.parent)])
        self.assertEqual(json.loads(recovered_path.read_text(encoding="utf-8")), {"ready": True})
        self.assertEqual(list(recovered_path.parent.glob("*.tmp")), [])

    def test_06_new_order_probes_response_and_guard_once_without_exists(self):
        request_id = "single-probe"
        payload = self.order_payload(request_id)
        response_path = helper._response_path(request_id)
        guard_path = helper._request_guard_path(request_id)
        read_counts = {response_path: 0, guard_path: 0}
        passorder_calls = []
        original_read_json = helper._read_json
        original_exists = os.path.exists

        def counted_read(path):
            if path in read_counts:
                read_counts[path] += 1
            return original_read_json(path)

        def reject_duplicate_probe(path):
            if path in read_counts:
                raise AssertionError("response/guard lookup must use direct open")
            return original_exists(path)

        def passorder(*args):
            guard = original_read_json(guard_path)
            self.assertEqual(guard["state"], "processing")
            passorder_calls.append(args)
            return "ORDER-SINGLE-PROBE"

        helper.passorder = passorder
        self.write_request(helper.INBOX_COMMANDS_DIR, request_id, "place_order", payload)
        with mock.patch.object(helper, "_read_json", side_effect=counted_read), mock.patch.object(
            helper.os.path, "exists", side_effect=reject_duplicate_probe
        ):
            helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(read_counts, {response_path: 1, guard_path: 1})
        self.assertEqual(len(passorder_calls), 1)
        self.assertEqual(helper._read_request_guard(request_id)["state"], "submitted")
        self.assertTrue(self.response(request_id)["ok"])
        self.assertEqual(
            helper._read_request_guard(request_id)["gateway_effect_fingerprint"],
            payload["gateway_effect_fingerprint"],
        )
        self.assertEqual(
            self.response(request_id)["gateway_effect_fingerprint"],
            payload["gateway_effect_fingerprint"],
        )

    def test_06_request_file_key_is_utf8_sha256_and_prevents_safe_name_collision(self):
        first_request_id = "alpha/beta"
        second_request_id = "alpha?beta"
        unicode_request_id = "\u8ba2\u5355/114514"
        self.assertEqual(
            helper._request_file_key(unicode_request_id),
            hashlib.sha256(unicode_request_id.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(helper._safe_filename(first_request_id), helper._safe_filename(second_request_id))
        self.assertNotEqual(helper._request_file_key(first_request_id), helper._request_file_key(second_request_id))
        for request_id in (first_request_id, second_request_id):
            expected_key = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
            self.assertEqual(helper._request_file_key(request_id), expected_key)
            self.assertEqual(Path(helper._response_path(request_id)).name, expected_key + ".json")
            self.assertEqual(Path(helper._request_guard_path(request_id)).name, expected_key + ".json")

        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args) or "ORDER-%d" % len(passorder_calls)
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            first_request_id,
            "place_order",
            self.order_payload(first_request_id),
            filename="opaque-command-a",
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            second_request_id,
            "place_order",
            self.order_payload(second_request_id),
            filename="opaque-command-b",
        )
        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(len(passorder_calls), 2)
        self.assertEqual(self.response(first_request_id)["request_id"], first_request_id)
        self.assertEqual(self.response(second_request_id)["request_id"], second_request_id)
        self.assertEqual(helper._read_request_guard(first_request_id)["request_id"], first_request_id)
        self.assertEqual(helper._read_request_guard(second_request_id)["request_id"], second_request_id)

    def test_06_processing_recovery_crash_after_claim_returns_to_inbox(self):
        command_request_id = "crash-after-claim-command"
        query_request_id = "crash-after-claim-query"
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args) or "ORDER-RECOVERED"
        self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            command_request_id,
            "place_order",
            self.order_payload(command_request_id),
            filename="claimed-command",
        )
        self.write_request(
            helper.PROCESSING_QUERIES_DIR,
            query_request_id,
            "health",
            filename="claimed-query",
        )

        helper.init(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertEqual(list(Path(helper.PROCESSING_COMMANDS_DIR).glob("*.json")), [])
        self.assertEqual(list(Path(helper.PROCESSING_QUERIES_DIR).glob("*.json")), [])
        self.assertTrue(Path(helper.INBOX_COMMANDS_DIR, "claimed-command.json").is_file())
        self.assertTrue(Path(helper.INBOX_QUERIES_DIR, "claimed-query.json").is_file())
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(len(passorder_calls), 1)
        self.assertEqual(self.response(command_request_id)["data"]["order_id"], "ORDER-RECOVERED")
        helper.G_LAST_COMMAND_ACTIVITY_AT = 0.0
        helper.bigqmt_query_timer(DummyContext())
        self.assertTrue(self.response(query_request_id)["ok"])

    def test_06_processing_recovery_crash_after_guard_is_submit_unknown(self):
        request_id = "crash-after-guard"
        request = self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename="guarded-claim",
        )
        helper._write_request_guard(request_id, "processing", request)
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.init(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertFalse(Path(helper.PROCESSING_COMMANDS_DIR, "guarded-claim.json").exists())
        response = self.response(request_id)
        self.assertTrue(response["ok"])
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertTrue(response["data"]["reconcile_required"])
        guard = helper._read_request_guard(request_id)
        self.assertEqual(guard["state"], "unknown")
        self.assertEqual(guard["response"]["code"], "RECONCILE_REQUIRED")
        self.assertTrue(helper.G_RECONCILE_NEEDED)

    def test_06_processing_recovery_completed_guard_rebuilds_response(self):
        request_id = "crash-after-completed-guard"
        request = self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename="completed-claim",
        )
        completed = helper.make_response(
            True,
            {"status": "submitted", "order_id": "ORDER-FROM-GUARD"},
            "",
            "",
            request,
            "place_order",
        )
        helper._write_request_guard(request_id, "submitted", request, completed)
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.init(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertFalse(Path(helper.PROCESSING_COMMANDS_DIR, "completed-claim.json").exists())
        self.assertEqual(self.response(request_id)["data"]["order_id"], "ORDER-FROM-GUARD")
        self.assertEqual(helper.G_METRICS["processing_recovery_completed_total"], 1)

    def test_06_processing_recovery_is_bounded_and_bad_json_moves_failed(self):
        bad_path = Path(helper.PROCESSING_COMMANDS_DIR, "bad.json")
        bad_path.write_text("{", encoding="utf-8")
        self.assertEqual(helper._recover_processing_requests(1), 1)
        self.assertFalse(bad_path.exists())
        self.assertTrue(any(Path(helper.FAILED_DIR).glob("bad*.json")))

        helper.G_PROCESSING_RECOVERY_FOLDER_CURSOR = 1
        total = helper.MAX_PROCESSING_RECOVERY_FILES_PER_TICK + 2
        for index in range(total):
            request_id = "bounded-query-%02d" % index
            self.write_request(
                helper.PROCESSING_QUERIES_DIR,
                request_id,
                "health",
                filename="bounded-query-%02d" % index,
            )
        recovered = helper._recover_processing_requests(
            helper.MAX_PROCESSING_RECOVERY_FILES_PER_TICK
        )
        self.assertEqual(recovered, helper.MAX_PROCESSING_RECOVERY_FILES_PER_TICK)
        self.assertEqual(
            len(list(Path(helper.PROCESSING_QUERIES_DIR).glob("*.json"))),
            2,
        )

    def test_06_legacy_artifacts_dual_read_and_one_way_migrate(self):
        request_id = "legacy-order-migrate"
        request = self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename="legacy-order-command",
        )
        completed = helper.make_response(
            True,
            {"status": "submitted", "order_id": "ORDER-LEGACY"},
            "",
            "",
            request,
            "place_order",
        )
        helper._write_request_guard(request_id, "submitted", request, completed)
        helper._write_response(request, completed)
        response = json.loads(Path(helper._response_path(request_id)).read_text(encoding="utf-8"))
        guard = json.loads(Path(helper._request_guard_path(request_id)).read_text(encoding="utf-8"))
        response.pop("gateway_effect_fingerprint", None)
        guard.pop("gateway_effect_fingerprint", None)
        guard["response"].pop("gateway_effect_fingerprint", None)
        Path(helper._response_path(request_id)).unlink()
        Path(helper._request_guard_path(request_id)).unlink()
        helper._atomic_write_json(helper._legacy_response_path(request_id), response, False)
        helper._atomic_write_json(helper._legacy_request_guard_path(request_id), guard, False)
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertFalse(Path(helper._legacy_response_path(request_id)).exists())
        self.assertFalse(Path(helper._legacy_request_guard_path(request_id)).exists())
        self.assertTrue(Path(helper._response_path(request_id)).is_file())
        self.assertTrue(Path(helper._request_guard_path(request_id)).is_file())
        self.assertEqual(self.response(request_id)["data"]["order_id"], "ORDER-LEGACY")
        self.assertEqual(
            self.response(request_id)["gateway_effect_fingerprint"],
            request["gateway_effect_fingerprint"],
        )
        self.assertEqual(
            helper._read_request_guard(request_id)["gateway_effect_fingerprint"],
            request["gateway_effect_fingerprint"],
        )
        self.assertEqual(helper.G_METRICS["legacy_artifact_migrated_total"], 2)

    def test_06_legacy_cancel_without_fingerprint_validates_target(self):
        request_id = "legacy-cancel-migrate"
        payload = {
            "request_id": request_id,
            "order_id": "ORDER-CANCEL-TARGET",
            "gateway_effect_fingerprint": "sha256:" + "c" * 64,
        }
        request = self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "cancel_order",
            payload,
            filename="legacy-cancel-command",
        )
        completed = helper.make_response(
            True,
            {"status": "accepted", "order_id": payload["order_id"]},
            "",
            "",
            request,
            "cancel_order",
        )
        helper._write_request_guard(request_id, "submitted", request, completed)
        helper._write_response(request, completed)
        response = json.loads(Path(helper._response_path(request_id)).read_text(encoding="utf-8"))
        guard = json.loads(Path(helper._request_guard_path(request_id)).read_text(encoding="utf-8"))
        response.pop("gateway_effect_fingerprint", None)
        guard.pop("gateway_effect_fingerprint", None)
        guard["response"].pop("gateway_effect_fingerprint", None)
        Path(helper._response_path(request_id)).unlink()
        Path(helper._request_guard_path(request_id)).unlink()
        helper._atomic_write_json(helper._legacy_response_path(request_id), response, False)
        helper._atomic_write_json(helper._legacy_request_guard_path(request_id), guard, False)
        cancel_calls = []
        helper.cancel = lambda *args: cancel_calls.append(args)

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(cancel_calls, [])
        self.assertEqual(self.response(request_id)["data"]["order_id"], payload["order_id"])
        self.assertFalse(Path(helper._legacy_response_path(request_id)).exists())
        self.assertFalse(Path(helper._legacy_request_guard_path(request_id)).exists())

    def test_06_legacy_embedded_id_mismatch_isolated_without_qmt_replay(self):
        request_id = "alpha/beta"
        colliding_request_id = "alpha?beta"
        self.assertEqual(helper._safe_filename(request_id), helper._safe_filename(colliding_request_id))
        self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename="legacy-collision-claim",
        )
        helper._atomic_write_json(
            helper._legacy_request_guard_path(request_id),
            {
                "request_id": colliding_request_id,
                "state": "processing",
                "action": "place_order",
                "account_id": helper.ACCOUNT_ID,
            },
            False,
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.init(DummyContext())

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn("embedded request_id mismatch", response["artifact_conflict"])
        self.assertTrue(any(Path(helper.FAILED_DIR).glob("artifact-*-alpha_beta.json")))

    def test_06_hashed_legacy_or_fingerprint_conflict_fails_closed(self):
        request_id = "hashed-legacy-content-conflict"
        request = self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename="hashed-legacy-conflict-claim",
        )
        hashed_guard = helper._write_request_guard(request_id, "processing", request)
        legacy_guard = dict(hashed_guard)
        legacy_guard["state"] = "submitted"
        helper._atomic_write_json(
            helper._legacy_request_guard_path(request_id), legacy_guard, False
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.init(DummyContext())

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertIn("contents differ", response["artifact_conflict"])
        self.assertEqual(helper.G_METRICS["request_artifact_conflict_total"], 1)

        second_request_id = "fingerprint-conflict"
        incoming = self.write_request(
            helper.INBOX_COMMANDS_DIR,
            second_request_id,
            "place_order",
            self.order_payload(second_request_id),
            filename="fingerprint-conflict-command",
        )
        persisted = dict(incoming)
        persisted["payload"] = dict(incoming["payload"])
        persisted["gateway_effect_fingerprint"] = "sha256:" + "f" * 64
        persisted["payload"]["gateway_effect_fingerprint"] = persisted[
            "gateway_effect_fingerprint"
        ]
        helper._write_request_guard(second_request_id, "processing", persisted)
        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        second_response = self.response(second_request_id)
        self.assertEqual(second_response["code"], "RECONCILE_REQUIRED")
        self.assertIn("gateway_effect_fingerprint mismatch", second_response["artifact_conflict"])

    def test_06_legacy_without_provable_intent_fails_closed(self):
        request_id = "legacy-insufficient-identity"
        self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename="legacy-insufficient-claim",
        )
        helper._atomic_write_json(
            helper._legacy_request_guard_path(request_id),
            {
                "request_id": request_id,
                "state": "processing",
                "client_order_id": request_id,
            },
            False,
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.init(DummyContext())

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertIn("action missing from persisted artifact", response["artifact_conflict"])

    def test_06_queued_hashed_legacy_fingerprint_conflict_never_dispatches(self):
        request_id = "queued-fingerprint-conflict"
        first_payload = self.order_payload(request_id)
        second_payload = dict(first_payload)
        second_payload["gateway_effect_fingerprint"] = "sha256:" + "d" * 64
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            first_payload,
            filename=helper._request_file_key(request_id),
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            second_payload,
            filename=helper._safe_filename(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn(
            "gateway_effect_fingerprint differs",
            response["queued_sibling_conflict"],
        )
        self.assertEqual(helper.G_METRICS["queued_sibling_conflict_total"], 1)
        self.assertFalse(Path(
            helper.INBOX_COMMANDS_DIR,
            helper._safe_filename(request_id) + ".json",
        ).exists())
        self.assertEqual(list(Path(helper.PROCESSING_COMMANDS_DIR).glob("*.json")), [])

    def test_06_queued_legacy_intent_or_embedded_id_conflict_fails_closed(self):
        request_id = "queued-legacy-intent-conflict"
        first_payload = self.order_payload(request_id)
        legacy_payload = dict(first_payload)
        legacy_payload.pop("gateway_effect_fingerprint", None)
        legacy_payload["intent_hash"] = "intent:different"
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            first_payload,
            filename=helper._request_file_key(request_id),
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            legacy_payload,
            filename=helper._safe_filename(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(passorder_calls, [])
        self.assertIn("intent_hash differs", self.response(request_id)["queued_sibling_conflict"])

        second_request_id = "queued/collision"
        colliding_request_id = "queued?collision"
        self.assertEqual(
            helper._safe_filename(second_request_id),
            helper._safe_filename(colliding_request_id),
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            second_request_id,
            "place_order",
            self.order_payload(second_request_id),
            filename=helper._request_file_key(second_request_id),
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            colliding_request_id,
            "place_order",
            self.order_payload(colliding_request_id),
            filename=helper._safe_filename(second_request_id),
        )
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(passorder_calls, [])
        self.assertIn(
            "embedded request_id mismatch",
            self.response(second_request_id)["queued_sibling_conflict"],
        )

    def test_06_identical_queued_hashed_legacy_siblings_dispatch_once(self):
        request_id = "queued-identical-siblings"
        payload = self.order_payload(request_id)
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename=helper._request_file_key(request_id),
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename=helper._safe_filename(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args) or "ORDER-ONCE"

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(len(passorder_calls), 1)
        self.assertEqual(self.response(request_id)["data"]["order_id"], "ORDER-ONCE")
        self.assertEqual(helper.G_METRICS["queued_sibling_conflict_total"], 0)

    def test_06_queued_sibling_scan_collects_all_formats_without_direct_stat(self):
        request_id = "queued-probe-bound"
        payload = self.order_payload(request_id)
        request_key = helper._request_file_key(request_id)
        request = self.write_request(
            helper.PROCESSING_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename="opaque-current",
        )
        direct_paths = [
            Path(helper.INBOX_COMMANDS_DIR, request_key + ".json"),
            Path(helper.INBOX_COMMANDS_DIR, helper._safe_filename(request_id) + ".json"),
            Path(helper.PROCESSING_COMMANDS_DIR, request_key + ".json"),
            Path(helper.PROCESSING_COMMANDS_DIR, helper._safe_filename(request_id) + ".json"),
            Path(helper.INBOX_DIR, request_key + ".json"),
            Path(helper.INBOX_DIR, helper._safe_filename(request_id) + ".json"),
        ]
        variant_paths = [
            Path(
                helper.INBOX_COMMANDS_DIR,
                "%020d-%s.json" % (7, request_key),
            ),
            Path(
                helper.PROCESSING_COMMANDS_DIR,
                "%020d-%s-recovered-1721300000-deadbeef.json"
                % (8, request_key),
            ),
            Path(
                helper.INBOX_DIR,
                "%020d-%s-recovered-1721300001-cafebabe-"
                "recovered-1721300002-acde1234.json" % (9, request_key),
            ),
        ]
        for path in direct_paths + variant_paths:
            helper._atomic_write_json(str(path), request, False)
        unrelated = []
        for index in range(20):
            path = Path(helper.INBOX_COMMANDS_DIR, "unrelated-%02d.json" % index)
            helper._atomic_write_json(str(path), {"request_id": "other-%02d" % index}, False)
            unrelated.append(str(path))
        original_read_json = helper._read_json
        read_paths = []

        def counted_read(path):
            read_paths.append(path)
            return original_read_json(path)

        with mock.patch.object(
            helper,
            "_queued_command_sibling_paths",
            side_effect=AssertionError("normal preflight used direct path probes"),
        ), mock.patch.object(
            helper.os.path,
            "isfile",
            side_effect=AssertionError("normal preflight issued a direct stat"),
        ), mock.patch.object(helper, "_read_json", side_effect=counted_read):
            resolution = helper._preflight_command_siblings(
                request,
                os.path.join(helper.PROCESSING_COMMANDS_DIR, "opaque-current.json"),
            )

        self.assertEqual(resolution["conflict"], "")
        current_path = Path(helper.PROCESSING_COMMANDS_DIR, "opaque-current.json")
        expected_paths = direct_paths + variant_paths + [current_path]
        normalize = lambda path: os.path.normcase(os.path.abspath(str(path)))
        self.assertEqual(
            {normalize(path) for path in resolution["paths"]},
            {normalize(path) for path in expected_paths},
        )
        self.assertLessEqual(
            len(resolution["paths"]),
            helper.MAX_COMMAND_SIBLING_PREFLIGHT_FILES,
        )
        self.assertEqual(
            {normalize(path) for path in read_paths},
            {normalize(path) for path in direct_paths + variant_paths},
        )
        self.assertTrue(set(read_paths).isdisjoint(unrelated))

    def test_06_queued_sibling_scandir_open_failure_never_dispatches(self):
        request_id = "queued-scandir-open-failure"
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename=helper._request_file_key(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)
        original_scandir = os.scandir
        target = os.path.normcase(os.path.abspath(helper.INBOX_COMMANDS_DIR))
        target_calls = [0]

        def fail_preflight_scandir(path):
            if os.path.normcase(os.path.abspath(path)) == target:
                target_calls[0] += 1
                if target_calls[0] == 2:
                    raise OSError("injected sibling scandir open failure")
            return original_scandir(path)

        with mock.patch.object(helper.os, "scandir", side_effect=fail_preflight_scandir):
            helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn("directory open failed", response["queued_sibling_conflict"])
        self.assertEqual(helper.G_METRICS["queued_sibling_scan_incomplete_total"], 1)

    def test_06_queued_sibling_scandir_iteration_failure_never_dispatches(self):
        request_id = "queued-scandir-iteration-failure"
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename=helper._request_file_key(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)
        original_scandir = os.scandir
        target = os.path.normcase(os.path.abspath(helper.INBOX_COMMANDS_DIR))
        target_calls = [0]

        class FailingIteration:
            def __init__(self, delegate):
                self.delegate = delegate

            def __iter__(self):
                return self

            def __next__(self):
                raise OSError("injected sibling scandir iteration failure")

            def close(self):
                self.delegate.close()

        def fail_preflight_iteration(path):
            if os.path.normcase(os.path.abspath(path)) == target:
                target_calls[0] += 1
                if target_calls[0] == 2:
                    return FailingIteration(original_scandir(path))
            return original_scandir(path)

        with mock.patch.object(helper.os, "scandir", side_effect=fail_preflight_iteration):
            helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn("directory iteration failed", response["queued_sibling_conflict"])

    def test_06_queued_sibling_matching_is_file_failure_never_cancels(self):
        request_id = "queued-matching-is-file-failure"
        payload = {
            "request_id": request_id,
            "order_id": "ORDER-METADATA-FAILURE",
            "gateway_effect_fingerprint": "sha256:" + "9" * 64,
        }
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "cancel_order",
            payload,
            filename=helper._request_file_key(request_id),
        )
        cancel_calls = []
        helper.cancel = lambda *args: cancel_calls.append(args)
        original_scandir = os.scandir
        target = os.path.normcase(os.path.abspath(helper.INBOX_COMMANDS_DIR))
        target_calls = [0]

        class MatchingUnreadableEntry:
            name = helper._request_file_key(request_id) + ".json"
            path = os.path.join(helper.INBOX_COMMANDS_DIR, name)

            def is_file(self):
                raise OSError("injected matching entry metadata failure")

        class OneEntryScan:
            def __init__(self):
                self.done = False

            def __iter__(self):
                return self

            def __next__(self):
                if self.done:
                    raise StopIteration
                self.done = True
                return MatchingUnreadableEntry()

            def close(self):
                pass

        def fail_matching_metadata(path):
            if os.path.normcase(os.path.abspath(path)) == target:
                target_calls[0] += 1
                if target_calls[0] == 2:
                    return OneEntryScan()
            return original_scandir(path)

        with mock.patch.object(helper.os, "scandir", side_effect=fail_matching_metadata):
            helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(cancel_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn(
            "matching entry metadata unreadable",
            response["queued_sibling_conflict"],
        )

    def test_06_discovered_sibling_read_error_never_dispatches(self):
        request_id = "queued-sibling-read-failure"
        payload = self.order_payload(request_id)
        primary_name = "%020d-%s" % (0, helper._request_file_key(request_id))
        sibling_path = os.path.join(
            helper.INBOX_COMMANDS_DIR,
            helper._safe_filename(request_id) + ".json",
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename=primary_name,
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename=helper._safe_filename(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)
        original_read_json = helper._read_json
        unreadable = os.path.normcase(os.path.abspath(sibling_path))

        def fail_sibling_read(path):
            if os.path.normcase(os.path.abspath(path)) == unreadable:
                raise PermissionError("injected queued sibling read failure")
            return original_read_json(path)

        with mock.patch.object(helper, "_read_json", side_effect=fail_sibling_read):
            helper.drain_file_requests(DummyContext(), 1, "command", 0.0)

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["code"], "RECONCILE_REQUIRED")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn("queued sibling unreadable", response["queued_sibling_conflict"])

    def test_06_queued_sibling_scan_budget_exhaustion_never_dispatches(self):
        request_id = "queued-sibling-scan-budget"
        primary_name = "%020d-%s" % (0, helper._request_file_key(request_id))
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename=primary_name,
        )
        helper._atomic_write_json(
            os.path.join(helper.INBOX_COMMANDS_DIR, "zz-unrelated.json"),
            {"request_id": "unrelated", "action": "health"},
            False,
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        with mock.patch.object(
            helper, "MAX_COMMAND_SIBLING_SCAN_ENTRIES_PER_REQUEST", 0
        ):
            helper.drain_file_requests(DummyContext(), 1, "command", 0.0)

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn(
            "queued sibling scan budget exhausted",
            response["queued_sibling_conflict"],
        )

    def test_06_queued_sibling_candidate_budget_exhaustion_never_dispatches(self):
        request_id = "queued-sibling-candidate-budget"
        payload = self.order_payload(request_id)
        primary_name = "%020d-%s" % (0, helper._request_file_key(request_id))
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename=primary_name,
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            payload,
            filename=helper._safe_filename(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        with mock.patch.object(helper, "MAX_COMMAND_SIBLING_PREFLIGHT_FILES", 1):
            helper.drain_file_requests(DummyContext(), 1, "command", 0.0)

        self.assertEqual(passorder_calls, [])
        response = self.response(request_id)
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertIn(
            "queued sibling candidate budget exhausted",
            response["queued_sibling_conflict"],
        )

    def test_06_sequenced_and_recovered_siblings_fail_closed(self):
        request_id = "sequenced-sibling-conflict"
        first_payload = self.order_payload(request_id)
        second_payload = dict(first_payload)
        second_payload["gateway_effect_fingerprint"] = "sha256:" + "e" * 64
        key = helper._request_file_key(request_id)
        first_name = "%020d-%s" % (1, key)
        second_name = "%020d-%s" % (2, key)
        self.assertTrue(helper._is_command_sibling_filename(first_name + ".json", request_id))
        self.write_request(
            helper.INBOX_COMMANDS_DIR, request_id, "place_order",
            first_payload, filename=first_name,
        )
        self.write_request(
            helper.INBOX_DIR, request_id, "place_order",
            second_payload, filename=second_name,
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertIn(
            "gateway_effect_fingerprint differs",
            self.response(request_id)["queued_sibling_conflict"],
        )

        recovered_id = "processing-recovery-collision"
        live_payload = self.order_payload(recovered_id)
        recovered_payload = dict(live_payload)
        recovered_payload["gateway_effect_fingerprint"] = "sha256:" + "a" * 64
        recovered_key = helper._request_file_key(recovered_id)
        self.write_request(
            helper.INBOX_COMMANDS_DIR, recovered_id, "place_order",
            live_payload, filename=recovered_key,
        )
        self.write_request(
            helper.PROCESSING_COMMANDS_DIR, recovered_id, "place_order",
            recovered_payload, filename=recovered_key,
        )
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(passorder_calls, [])
        self.assertIn(
            "gateway_effect_fingerprint differs",
            self.response(recovered_id)["queued_sibling_conflict"],
        )
        self.assertFalse(any(Path(helper.INBOX_COMMANDS_DIR).glob(
            recovered_key + "-recovered-*.json"
        )))

    def test_06_fingerprintless_siblings_compare_complete_qmt_effect(self):
        request_id = "canonical-effect-conflict"
        first_payload = self.order_payload(request_id)
        first_payload.pop("gateway_effect_fingerprint", None)
        second_payload = dict(first_payload)
        second_payload["price"] = 10.01
        self.write_request(
            helper.INBOX_COMMANDS_DIR, request_id, "place_order",
            first_payload, filename=helper._request_file_key(request_id),
        )
        self.write_request(
            helper.INBOX_COMMANDS_DIR, request_id, "place_order",
            second_payload, filename=helper._safe_filename(request_id),
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertIn(
            "canonical QMT effect differs",
            self.response(request_id)["queued_sibling_conflict"],
        )

        baseline = {
            "request_id": request_id,
            "action": "place_order",
            "account_id": helper.ACCOUNT_ID,
            "account_type": helper.ACCOUNT_TYPE,
            "payload": first_payload,
        }
        for field, value in (
            ("order_type", 24), ("qmt_order_type", 1201),
            ("symbol", "000001.SZ"), ("price_type", 49),
            ("quantity", 200), ("strategy_name", "other"),
            ("order_remark", "other"), ("qmt_user_order_id", "other"),
            ("quick_trade", 1),
        ):
            candidate = dict(baseline)
            candidate["payload"] = dict(first_payload)
            candidate["payload"][field] = value
            self.assertIn(
                "canonical QMT effect differs",
                helper._queued_request_effect_error(baseline, candidate),
                field,
            )

    def test_06_legacy_artifact_without_canonical_effect_fails_closed(self):
        request_id = "legacy-missing-canonical-effect"
        payload = self.order_payload(request_id)
        payload.pop("gateway_effect_fingerprint", None)
        self.write_request(
            helper.INBOX_COMMANDS_DIR, request_id, "place_order", payload,
            filename=helper._request_file_key(request_id),
        )
        helper._atomic_write_json(
            helper._legacy_request_guard_path(request_id),
            {
                "request_id": request_id,
                "state": "processing",
                "action": "place_order",
                "account_id": helper.ACCOUNT_ID,
                "client_order_id": payload["client_order_id"],
                "intent_hash": payload["intent_hash"],
            },
            False,
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(passorder_calls, [])
        self.assertIn(
            "canonical QMT effect missing",
            self.response(request_id)["artifact_conflict"],
        )

    def test_06_response_and_guard_replay_remain_idempotent(self):
        request_id = "replay-once"
        payload = self.order_payload(request_id)
        calls = []

        def passorder(*args):
            calls.append(args)
            return "ORDER-REPLAY-ONCE"

        helper.passorder = passorder
        self.write_request(helper.INBOX_COMMANDS_DIR, request_id, "place_order", payload)
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(len(calls), 1)

        self.write_request(helper.INBOX_COMMANDS_DIR, request_id, "place_order", payload)
        helper.bigqmt_command_timer(DummyContext())
        response_replay = self.response(request_id)
        self.assertEqual(len(calls), 1)
        self.assertTrue(response_replay["idempotent"])
        self.assertEqual(response_replay["duplicate_stage"], "helper_response")

        Path(helper._response_path(request_id)).unlink()
        self.write_request(helper.INBOX_COMMANDS_DIR, request_id, "place_order", payload)
        helper.bigqmt_command_timer(DummyContext())
        guard_replay = self.response(request_id)
        self.assertEqual(len(calls), 1)
        self.assertTrue(guard_replay["idempotent"])
        self.assertEqual(guard_replay["duplicate_stage"], "helper_guard")

    def test_06_cleanup_scan_budget_and_folder_rotation_are_bounded(self):
        self.assertEqual(helper.MAX_CLEANUP_SCAN_ENTRIES_PER_TICK, 512)
        old_folder = Path(self.tmp, "old-files")
        old_folder.mkdir()
        old_time = time.time() - 100
        for index in range(10):
            path = old_folder / ("old-%02d.json" % index)
            path.write_text("{}", encoding="utf-8")
            os.utime(str(path), (old_time, old_time))
        deleted, scanned = helper._cleanup_old_files(
            str(old_folder), time.time() - 10, False, 10, 3
        )
        self.assertEqual((deleted, scanned), (3, 3))
        self.assertEqual(len(list(old_folder.glob("*.json"))), 7)

        cleanup_calls = []

        def consume_scan_budget(folder, cutoff, preserve_guards, max_delete, max_scan):
            cleanup_calls.append((folder, preserve_guards, max_delete, max_scan))
            return 0, max_scan

        with mock.patch.object(helper, "_cleanup_old_files", side_effect=consume_scan_budget), mock.patch.object(
            helper, "_safe_runtime_write", return_value=None
        ):
            helper._run_maintenance_cycle(DummyContext(), "budget-1")
            helper._run_maintenance_cycle(DummyContext(), "budget-2")

        self.assertEqual(len(cleanup_calls), 2)
        self.assertEqual(cleanup_calls[0][0], helper.EVENTS_FAILED_DIR)
        self.assertEqual(cleanup_calls[1][0], helper.DONE_DIR)
        self.assertNotIn(helper.RESPONSES_DIR, [call[0] for call in cleanup_calls])
        self.assertNotIn(helper.EVENTS_LIVE_DIR, [call[0] for call in cleanup_calls])
        self.assertTrue(all(call[3] == helper.MAX_CLEANUP_SCAN_ENTRIES_PER_TICK for call in cleanup_calls))
        self.assertEqual(helper.G_METRICS["cleanup_scan_budget_exhausted_total"], 2)
        self.assertEqual(helper.G_METRICS["cleanup_scanned_total"], helper.MAX_CLEANUP_SCAN_ENTRIES_PER_TICK * 2)

        all_cleanup_calls = []

        def record_all_cleanup(folder, cutoff, preserve_guards, max_delete, max_scan):
            all_cleanup_calls.append(folder)
            return 0, 0

        helper.G_CLEANUP_FOLDER_CURSOR = 0
        with mock.patch.object(helper, "_cleanup_old_files", side_effect=record_all_cleanup), mock.patch.object(
            helper, "_safe_runtime_write", return_value=None
        ):
            helper._run_maintenance_cycle(DummyContext(), "reliable-event-retention")
        self.assertEqual(
            set(all_cleanup_calls),
            {
                helper.EVENTS_FAILED_DIR,
                helper.DONE_DIR,
                helper.FAILED_DIR,
                helper.REQUEST_STATE_DIR,
            },
        )
        self.assertNotIn(helper.RESPONSES_DIR, all_cleanup_calls)
        self.assertNotIn(helper.EVENTS_LIVE_DIR, all_cleanup_calls)

    def test_06_stale_atomic_tmp_cleanup_is_strict_aged_and_bounded(self):
        old_time = time.time() - helper.STALE_ATOMIC_TMP_AGE_SECONDS - 10
        old_atomic = Path(
            helper.RESPONSES_DIR,
            "response.json.123.1234567890123.tmp",
        )
        fresh_atomic = Path(
            helper.RESPONSES_DIR,
            "fresh.json.123.1234567890124.tmp",
        )
        unrelated_tmp = Path(helper.RESPONSES_DIR, "keep-me.tmp")
        callback_tmp = Path(
            helper.EVENTS_LIVE_DIR,
            "evt-1234567890123-000001.json.123.tmp",
        )
        callback_shape_outside_live = Path(
            helper.RESPONSES_DIR,
            "evt-1234567890123-000002.json.123.tmp",
        )
        for path in (
            old_atomic, fresh_atomic, unrelated_tmp, callback_tmp,
            callback_shape_outside_live,
        ):
            path.write_text("{}", encoding="utf-8")
        for path in (
            old_atomic, unrelated_tmp, callback_tmp,
            callback_shape_outside_live,
        ):
            os.utime(str(path), (old_time, old_time))

        deleted, scanned = helper._run_stale_atomic_tmp_cleanup()

        self.assertGreaterEqual(deleted, 2)
        self.assertLessEqual(
            scanned, helper.MAX_STALE_ATOMIC_TMP_SCAN_ENTRIES_PER_TICK
        )
        self.assertFalse(old_atomic.exists())
        self.assertFalse(callback_tmp.exists())
        self.assertTrue(fresh_atomic.exists())
        self.assertTrue(unrelated_tmp.exists())
        self.assertTrue(callback_shape_outside_live.exists())
        self.assertGreaterEqual(
            helper.G_METRICS["stale_tmp_cleanup_deleted_total"], 2
        )

        bounded_folder = Path(helper.DONE_DIR)
        for index in range(5):
            path = bounded_folder / (
                "bounded-%d.json.123.12345678901%02d.tmp" % (index, index)
            )
            path.write_text("{}", encoding="utf-8")
            os.utime(str(path), (old_time, old_time))
        bounded_deleted, bounded_scanned = helper._cleanup_stale_atomic_tmp_files(
            str(bounded_folder), time.time() - 1, 2, 3, False
        )
        self.assertLessEqual(bounded_deleted, 2)
        self.assertLessEqual(bounded_scanned, 3)

        outside = Path(tempfile.mkdtemp(prefix="bigqmt-outside-runtime-"))
        try:
            outside_tmp = outside / "outside.json.123.1234567890123.tmp"
            outside_tmp.write_text("{}", encoding="utf-8")
            os.utime(str(outside_tmp), (old_time, old_time))
            self.assertEqual(
                helper._cleanup_stale_atomic_tmp_files(
                    str(outside), time.time() - 1, 10, 10, False
                ),
                (0, 0),
            )
            self.assertTrue(outside_tmp.exists())
        finally:
            shutil.rmtree(str(outside), ignore_errors=True)

    def test_06_processing_guard_never_replays_passorder(self):
        calls = []
        helper.passorder = lambda *args: calls.append(args)
        payload = self.order_payload("guarded")
        helper._write_request_guard("guarded", "processing", {"payload": payload})
        self.write_request(helper.INBOX_COMMANDS_DIR, "guarded", "place_order", payload)
        helper.bigqmt_command_timer(DummyContext())
        self.assertEqual(calls, [])
        self.assertEqual(self.response("guarded")["data"]["stage"], "SUBMIT_UNKNOWN")

    def test_06_request_queue_sorts_full_bounded_scan_before_truncation(self):
        total = 100
        for sequence in reversed(range(total)):
            request_id = "ordered-%03d" % sequence
            payload = self.order_payload(request_id)
            payload["qmt_user_order_id"] = "SEQ-%03d" % sequence
            filename = "%020d-%s" % (
                sequence + 1,
                helper._request_file_key(request_id),
            )
            self.write_request(
                helper.INBOX_COMMANDS_DIR,
                request_id,
                "place_order",
                payload,
                filename=filename,
            )

        original_scandir = helper.os.scandir
        reversed_once = [False]

        class EntryIterator:
            def __init__(self, entries):
                self.entries = iter(entries)

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.entries)

            def close(self):
                return None

        def reverse_command_enumeration(folder):
            if (
                os.path.normcase(os.path.abspath(folder))
                == os.path.normcase(os.path.abspath(helper.INBOX_COMMANDS_DIR))
                and not reversed_once[0]
            ):
                reversed_once[0] = True
                entries = original_scandir(folder)
                try:
                    values = list(entries)
                finally:
                    entries.close()
                values.sort(key=lambda entry: entry.name, reverse=True)
                return EntryIterator(values)
            return original_scandir(folder)

        passorder_calls = []

        def passorder(*args):
            passorder_calls.append(args)
            return "ORDER-%03d" % len(passorder_calls)

        helper.passorder = passorder
        with mock.patch.object(
            helper.os, "scandir", side_effect=reverse_command_enumeration
        ):
            drained = helper.drain_file_requests(
                DummyContext(), 1, "command", 0.0
            )

        self.assertEqual(drained, 1)
        self.assertEqual(len(passorder_calls), 1)
        self.assertEqual(passorder_calls[0][9], "SEQ-000")
        self.assertEqual(self.response("ordered-000")["data"]["order_id"], "ORDER-001")
        self.assertGreaterEqual(helper.G_METRICS["request_queue_last_scanned"], total)

    def test_06_request_queue_scan_failure_is_fail_closed(self):
        request_id = "scan-fail-closed"
        filename = "%020d-%s" % (1, helper._request_file_key(request_id))
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "place_order",
            self.order_payload(request_id),
            filename=filename,
        )
        passorder_calls = []
        helper.passorder = lambda *args: passorder_calls.append(args)
        original_scandir = helper.os.scandir

        def unreadable_root(folder):
            if os.path.normcase(os.path.abspath(folder)) == os.path.normcase(
                os.path.abspath(helper.INBOX_DIR)
            ):
                raise PermissionError("denied")
            return original_scandir(folder)

        with mock.patch.object(helper.os, "scandir", side_effect=unreadable_root):
            self.assertEqual(
                helper.drain_file_requests(DummyContext(), 1, "command", 0.0),
                0,
            )
        self.assertEqual(passorder_calls, [])
        self.assertTrue(Path(helper.INBOX_COMMANDS_DIR, filename + ".json").is_file())
        self.assertEqual(helper.G_METRICS["request_queue_scan_failed_total"], 1)
        self.assertEqual(helper.G_METRICS["request_queue_scan_unreadable_total"], 1)

        class OverflowEntry:
            def __init__(self, index):
                self.name = "junk-%04d.tmp" % index
                self.path = os.path.join(helper.INBOX_COMMANDS_DIR, self.name)

        class OverflowIterator:
            def __init__(self):
                self.index = 0

            def __iter__(self):
                return self

            def __next__(self):
                if self.index > helper.MAX_REQUEST_QUEUE_SCAN_ENTRIES_PER_CYCLE:
                    raise StopIteration
                entry = OverflowEntry(self.index)
                self.index += 1
                return entry

            def close(self):
                return None

        def overflow_commands(folder):
            if os.path.normcase(os.path.abspath(folder)) == os.path.normcase(
                os.path.abspath(helper.INBOX_COMMANDS_DIR)
            ):
                return OverflowIterator()
            return original_scandir(folder)

        with mock.patch.object(helper.os, "scandir", side_effect=overflow_commands):
            self.assertEqual(
                helper.drain_file_requests(DummyContext(), 1, "command", 0.0),
                0,
            )
        self.assertEqual(passorder_calls, [])
        self.assertEqual(helper.G_METRICS["request_queue_scan_failed_total"], 2)
        self.assertEqual(
            helper.G_METRICS["request_queue_scan_limit_exceeded_total"], 1
        )

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

    def test_11_passorder_result_normalization_is_fail_closed(self):
        cases = (
            ("false", False, ""),
            ("negative", -1, ""),
            ("none", None, ""),
            ("positive", 123, "123"),
            ("order-text", "ORDER-COMPAT", "ORDER-COMPAT"),
        )
        for suffix, native_result, expected_order_id in cases:
            request_id = "passorder-result-" + suffix
            calls = []

            def passorder(*args):
                calls.append(args)
                return native_result

            helper.passorder = passorder
            self.write_request(
                helper.INBOX_COMMANDS_DIR,
                request_id,
                "place_order",
                self.order_payload(request_id),
                filename=helper._request_file_key(request_id),
            )
            helper.bigqmt_command_timer(DummyContext())
            response = self.response(request_id)
            self.assertEqual(len(calls), 1, suffix)
            self.assertEqual(
                response["data"]["order_id"], expected_order_id, suffix
            )
            self.assertEqual(
                response["data"]["stage"],
                "QMT_SUBMITTED" if expected_order_id else "SUBMIT_UNKNOWN",
                suffix,
            )

        for invalid in (
            float("nan"), float("inf"), float("-inf"),
            "false", "failed", "-1", "0.0", "NaN", "Infinity",
        ):
            self.assertEqual(helper._normalize_passorder_order_id(invalid), "")

    def test_12_callbacks_write_atomic_incremental_events(self):
        helper.order_callback(None, {"m_strOrderID": "O1", "m_strStockCode": "600000.SH", "m_strOrderStatus": "50"})
        helper.deal_callback(None, {"m_strTradeID": "T1", "m_strOrderID": "O1", "m_strStockCode": "600000.SH", "m_nTradedVolume": 100})
        events = [json.loads(path.read_text(encoding="utf-8")) for path in Path(helper.EVENTS_LIVE_DIR).glob("*.json")]
        self.assertEqual({event["type"] for event in events}, {"ORDER_UPDATE", "TRADE_NOTIFY"})
        self.assertEqual(list(Path(helper.EVENTS_LIVE_DIR).glob("*.tmp")), [])

    def test_13_cancel_type_error_is_single_call_and_submit_unknown(self):
        calls = []

        def cancel(*args):
            calls.append(args)
            raise TypeError("after native entry")

        helper.cancel = cancel
        request_id = "cancel-type-error-once"
        payload = {
            "request_id": request_id,
            "order_id": "O1",
            "gateway_effect_fingerprint": "sha256:" + "1" * 64,
        }
        self.write_request(
            helper.INBOX_COMMANDS_DIR,
            request_id,
            "cancel_order",
            payload,
            filename=helper._request_file_key(request_id),
        )

        helper.bigqmt_command_timer(DummyContext())

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], helper.qmt_account_type())
        response = self.response(request_id)
        self.assertEqual(response["code"], "SUBMIT_STATE_UNCERTAIN")
        self.assertEqual(response["data"]["stage"], "SUBMIT_UNKNOWN")
        self.assertEqual(helper._read_request_guard(request_id)["state"], "unknown")

    def test_14_cancel_non_success_return_is_submit_unknown(self):
        cases = (
            ("false", False, False),
            ("negative-one", -1, False),
            ("none", None, False),
            ("true", True, True),
            ("integer-zero", 0, True),
            ("string-zero", "0", True),
        )
        for suffix, native_result, expected_ok in cases:
            request_id = "cancel-result-" + suffix
            payload = {
                "request_id": request_id,
                "order_id": "ORDER-" + suffix,
                "gateway_effect_fingerprint": "sha256:" + hashlib.sha256(
                    request_id.encode("utf-8")
                ).hexdigest(),
            }
            calls = []

            def cancel(*args):
                calls.append(args)
                return native_result

            helper.cancel = cancel
            self.write_request(
                helper.INBOX_COMMANDS_DIR,
                request_id,
                "cancel_order",
                payload,
                filename=helper._request_file_key(request_id),
            )
            helper.bigqmt_command_timer(DummyContext())
            response = self.response(request_id)
            self.assertEqual(len(calls), 1, suffix)
            self.assertIs(response["ok"], True, suffix)
            self.assertEqual(
                response["data"]["status"],
                "accepted" if expected_ok else "submit_unknown",
                suffix,
            )
            self.assertEqual(
                response["data"]["stage"],
                "QMT_SUBMITTED" if expected_ok else "SUBMIT_UNKNOWN",
                suffix,
            )
            self.assertEqual(
                response["data"]["submit_result"],
                "KNOWN" if expected_ok else "UNKNOWN",
                suffix,
            )
            self.assertEqual(
                helper._read_request_guard(request_id)["state"],
                "submitted" if expected_ok else "unknown",
                suffix,
            )


if __name__ == "__main__":
    unittest.main()
