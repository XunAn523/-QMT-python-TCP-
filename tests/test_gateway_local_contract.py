import asyncio
import json
import logging
import shutil
import socket
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DIR = ROOT / (chr(0x7F51) + chr(0x5173))
API_DIR = ROOT / (chr(0x5916) + chr(0x7F6E) + chr(0x7B56) + chr(0x7565) + "API")
for path in (ROOT, GATEWAY_DIR, API_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools import project_env
import bigqmt_gateway_proxy as gateway_module
from bigqmt_gateway_proxy import (
    PROXY_BUILD_ID,
    BigQmtGatewayProxy,
    HelperTimeout,
    load_config,
    read_frame,
    setup_logging,
)
from qmt_local_api import ConnectionConfig, LocalQmtApi


EXAMPLE_AUTH_TOKEN = "0" * 64


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


async def send_frame(writer, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    writer.write(struct.pack(">I", len(body)) + body)
    await writer.drain()


async def wait_until(predicate, timeout=1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.005)


class GatewayLocalContractTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="qmt-local-gateway-test-"))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def make_config(self, **changes):
        deployment = project_env.load_deployment(
            ROOT / ".env.example", allow_example=True, environ={}
        )
        config = deployment["gateway_config"]
        config["accounts"][0]["tcp_port"] = free_port()
        config["accounts"][0]["runtime_dir"] = str(self.temp_dir)
        config.update(changes)
        path = self.temp_dir / "gateway.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    def test_generated_config_loads_one_loopback_account(self):
        config = load_config(self.make_config())
        self.assertEqual(PROXY_BUILD_ID, project_env.GATEWAY_BUILD_ID)
        self.assertEqual(len(config.accounts), 1)
        self.assertEqual(config.accounts[0].tcp_host, "127.0.0.1")
        self.assertEqual(config.accounts[0].response_watch_interval_seconds, 0.01)
        self.assertEqual(config.accounts[0].event_watch_interval_seconds, 0.01)

    def test_direct_config_cannot_widen_bind_or_weaken_hot_path(self):
        path = self.make_config()
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["accounts"][0]["tcp_host"] = "0.0.0.0"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "127.0.0.1"):
            load_config(path)
        path = self.make_config(response_watch_interval_seconds=0.1)
        with self.assertRaisesRegex(ValueError, "response_watch_interval_seconds"):
            load_config(path)
        path = self.make_config()
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["auth_token"] = EXAMPLE_AUTH_TOKEN
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unknown generated gateway config keys"):
            load_config(path)
        path = self.make_config()
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["accounts"][0]["host"] = "127.0.0.1"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unknown generated account config keys"):
            load_config(path)

    def test_first_frame_ping_returns_exact_local_identity(self):
        asyncio.run(self._ping_contract())

    def test_real_external_api_handshakes_with_real_gateway(self):
        asyncio.run(self._api_gateway_contract())

    def test_gateway_rejects_non_finite_json_numbers(self):
        async def reject():
            reader = asyncio.StreamReader()
            body = b'{"price":NaN}'
            reader.feed_data(struct.pack(">I", len(body)) + body)
            reader.feed_eof()
            with self.assertRaisesRegex(ValueError, "non-finite"):
                await read_frame(reader, 10 * 1024 * 1024)

        asyncio.run(reject())

    def test_async_helper_request_does_not_occupy_executor_while_waiting(self):
        asyncio.run(self._async_helper_request_contract())

    def test_sync_new_timeout_is_explicit_submit_unknown(self):
        asyncio.run(self._sync_new_timeout_contract())

    def test_sync_new_post_effect_exception_is_explicit_submit_unknown(self):
        asyncio.run(self._sync_new_post_effect_exception_contract())

    def test_sync_new_direct_submit_unknown_is_never_accepted(self):
        asyncio.run(self._sync_new_direct_submit_unknown_contract())

    def test_idempotent_replay_status_follows_persisted_stage(self):
        asyncio.run(self._idempotent_replay_stage_contract())

    def test_backpressure_rejects_before_effect_or_file_enqueue(self):
        asyncio.run(self._backpressure_contract())

    def test_whitespace_request_id_uses_client_order_id_end_to_end(self):
        asyncio.run(self._whitespace_request_id_contract())

    def test_response_delivery_is_independent_and_ack_gated(self):
        asyncio.run(self._response_delivery_contract())

    def test_bounded_response_scan_rotates_targeted_fallback(self):
        asyncio.run(self._response_fallback_contract())

    def test_stop_cancels_delivery_tasks_and_done_callback_is_identity_safe(self):
        asyncio.run(self._stop_delivery_contract())

    def test_stop_settles_effect_dispatch_without_cancelling_it(self):
        asyncio.run(self._stop_effect_dispatch_contract())

    async def _async_helper_request_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.helper.request_sync = mock.Mock(
            side_effect=AssertionError("async request must not call request_sync")
        )
        try:
            queued = await runtime.helper.request(
                "place_order",
                {"request_id": "async-timeout-queued"},
                0.03,
                timeout_as_queued=True,
            )
            self.assertEqual(queued["status"], "queued")
            self.assertTrue(queued["timeout"])
            self.assertEqual(queued["request_id"], "async-timeout-queued")
            with self.assertRaises(HelperTimeout):
                await runtime.helper.request(
                    "account",
                    {"request_id": "async-timeout-error"},
                    0.03,
                    timeout_as_queued=False,
                )
            runtime.helper.request_sync.assert_not_called()
            self.assertTrue(
                (runtime.helper.commands / "async-timeout-queued.json").is_file()
            )
            self.assertTrue(
                (runtime.helper.queries / "async-timeout-error.json").is_file()
            )
        finally:
            runtime.correlation.close()

    async def _sync_new_timeout_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def queued_timeout(payload, wait):
            self.assertTrue(wait)
            return {
                "status": "queued",
                "queued": True,
                "timeout": True,
                "request_id": payload["request_id"],
            }

        runtime.helper.place_order = queued_timeout
        try:
            reply = await proxy.handle_new(
                runtime,
                self.order_message("NEW", "sync-timeout"),
                async_mode=False,
            )
            self.assertEqual(reply["status"], "UNKNOWN")
            self.assertNotEqual(reply["status"], "ACCEPTED")
            self.assertEqual(reply["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(reply["submit_result"], "UNKNOWN")
            self.assertEqual(reply["code"], "HELPER_RESPONSE_TIMEOUT")
            self.assertIn("do not retry", reply["reject_reason"])
        finally:
            runtime.correlation.close()

    async def _sync_new_post_effect_exception_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        runtime.helper.place_order = mock.AsyncMock(
            side_effect=RuntimeError("helper result lost after enqueue")
        )
        try:
            reply = await proxy.handle_new(
                runtime,
                self.order_message("NEW", "sync-post-effect-error"),
                async_mode=False,
            )
            self.assertEqual(reply["status"], "UNKNOWN")
            self.assertNotEqual(reply["status"], "ACCEPTED")
            self.assertEqual(reply["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(reply["submit_result"], "UNKNOWN")
            self.assertEqual(reply["code"], "POST_ENQUEUE_STATE_UNCERTAIN")
            self.assertIn("do not retry automatically", reply["reject_reason"])
        finally:
            runtime.correlation.close()

    async def _sync_new_direct_submit_unknown_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def submit_unknown(payload, wait):
            self.assertTrue(wait)
            return {
                "status": "submit_unknown",
                "request_id": payload["request_id"],
                "error": "QMT returned no stable order id",
            }

        runtime.helper.place_order = submit_unknown
        try:
            reply = await proxy.handle_new(
                runtime,
                self.order_message("NEW", "sync-direct-unknown"),
                async_mode=False,
            )
            self.assertEqual(reply["status"], "UNKNOWN")
            self.assertNotEqual(reply["status"], "ACCEPTED")
            self.assertEqual(reply["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(reply["submit_result"], "UNKNOWN")
            self.assertEqual(reply["code"], "QMT_SUBMIT_RESULT_UNKNOWN")
            self.assertIn("do not retry automatically", reply["reject_reason"])
        finally:
            runtime.correlation.close()

    async def _idempotent_replay_stage_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        runtime.helper.place_order = mock.AsyncMock(
            side_effect=AssertionError("idempotent replay must not enqueue again")
        )
        cases = {
            "SUBMIT_UNKNOWN": ("UNKNOWN", "UNKNOWN", "QMT_SUBMIT_RESULT_UNKNOWN"),
            "REJECTED": ("REJECTED", "REJECTED", "ORDER_REJECTED"),
            "QMT_SUBMITTED": ("ACCEPTED", "KNOWN", ""),
            "RESERVED": ("UNKNOWN", "PENDING", "REQUEST_IN_PROGRESS"),
            "BRIDGE_QUEUED": ("UNKNOWN", "PENDING", "REQUEST_IN_PROGRESS"),
        }
        try:
            for stage, (sync_status, submit_result, code) in cases.items():
                with self.subTest(stage=stage, mode="sync"):
                    identity = "replay-" + stage.lower().replace("_", "-")
                    original = self.order_message("NEW", identity)
                    payload = proxy.order_payload(runtime, original)
                    runtime.correlation.reserve({**payload, "stage": "RESERVED"})
                    if stage != "RESERVED":
                        runtime.correlation.update_stage(
                            runtime.cfg.account_id,
                            identity,
                            stage,
                            order_id="1001" if stage == "QMT_SUBMITTED" else "",
                        )
                    replay = dict(original)
                    replay["msg_id"] = identity + "-different-msg"
                    replay["request_id"] = identity + "-different-request"
                    sync_reply = await proxy.handle_new(
                        runtime,
                        replay,
                        async_mode=False,
                    )
                    self.assertTrue(sync_reply["idempotent"])
                    self.assertEqual(sync_reply["msg_id"], replay["msg_id"])
                    self.assertEqual(sync_reply["stage"], stage)
                    self.assertEqual(sync_reply["status"], sync_status)
                    self.assertEqual(sync_reply["submit_result"], submit_result)
                    if code:
                        self.assertEqual(sync_reply["code"], code)
                    else:
                        self.assertNotIn("code", sync_reply)

                with self.subTest(stage=stage, mode="async"):
                    async_replay = dict(replay)
                    async_replay["type"] = "NEW_ASYNC"
                    async_replay["msg_id"] = identity + "-async-msg"
                    async_reply = await proxy.handle_new(
                        runtime,
                        async_replay,
                        async_mode=True,
                    )
                    expected_async_status = (
                        "SENT" if stage == "QMT_SUBMITTED"
                        else "REJECTED" if stage == "REJECTED"
                        else "UNKNOWN"
                    )
                    self.assertEqual(async_reply["status"], expected_async_status)
                    self.assertEqual(async_reply["stage"], stage)
                    self.assertEqual(async_reply["submit_result"], submit_result)
            runtime.helper.place_order.assert_not_awaited()
        finally:
            runtime.correlation.close()

    async def _backpressure_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(
            side_effect=AssertionError("capacity rejection must precede helper readiness")
        )
        delivery_blocker = asyncio.Event()
        delivery_task = None
        try:
            with mock.patch.object(
                gateway_module, "MAX_EFFECT_INFLIGHT_PER_ACCOUNT", 1
            ):
                runtime.effect_inflight = 1
                reply = await proxy.dispatch(
                    runtime, None, self.order_message("NEW_ASYNC", "effect-full")
                )
                self.assert_busy(reply, "account_effect_inflight")
            runtime.effect_inflight = 0

            with mock.patch.object(
                gateway_module, "MAX_PENDING_RESPONSES_PER_ACCOUNT", 1
            ):
                runtime.pending_responses["occupied"] = {
                    "payload": {},
                    "deadline_at": gateway_module.now() + 10,
                }
                reply = await proxy.dispatch(
                    runtime, None, self.order_message("NEW_ASYNC", "pending-full")
                )
                self.assert_busy(reply, "account_pending_response")
            runtime.pending_responses.clear()

            delivery_task = asyncio.create_task(delivery_blocker.wait())
            runtime.response_delivery_tasks["delivering"] = delivery_task
            with mock.patch.object(
                gateway_module, "MAX_RESPONSE_DELIVERY_TASKS_PER_ACCOUNT", 1
            ):
                reply = await proxy.dispatch(
                    runtime, None, self.order_message("NEW_ASYNC", "delivery-full")
                )
                self.assert_busy(reply, "account_pending_response")

            with mock.patch.object(
                gateway_module, "MAX_PENDING_RESPONSES_PER_ACCOUNT", 2
            ), mock.patch.object(
                gateway_module, "MAX_RESPONSE_DELIVERY_TASKS_PER_ACCOUNT", 32
            ):
                runtime.pending_responses["same-request"] = {
                    "payload": {},
                    "deadline_at": gateway_module.now() + 10,
                }
                runtime.response_delivery_tasks["same-request"] = delivery_task
                available, owned = await runtime.try_reserve_pending_response(
                    "new-request"
                )
                self.assertTrue(available)
                self.assertTrue(owned)
                await runtime.release_pending_response_reservation("new-request")

            self.assertEqual(runtime.effect_inflight, 0)
            self.assertFalse(runtime.pending_response_reservations)
            self.assertFalse(list(runtime.helper.commands.glob("*.json")))
        finally:
            runtime.response_delivery_tasks.clear()
            if delivery_task is not None:
                delivery_task.cancel()
                await asyncio.gather(delivery_task, return_exceptions=True)
            runtime.correlation.close()

    async def _whitespace_request_id_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def queued(payload, wait):
            self.assertFalse(wait)
            return {"status": "queued", "request_id": payload["request_id"]}

        runtime.helper.place_order = queued
        message = self.order_message("NEW_ASYNC", "whitespace-id")
        message["request_id"] = "   "
        try:
            reply = await proxy.dispatch(runtime, None, message)
            self.assertEqual(reply["request_id"], "whitespace-id")
            self.assertIn("whitespace-id", runtime.pending_responses)
            self.assertNotIn("", runtime.pending_responses)
            self.assertFalse(runtime.pending_response_reservations)
        finally:
            runtime.correlation.close()

    async def _response_delivery_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.primary = SimpleNamespace(closed=False)
        payload = self.order_message("NEW_ASYNC", "response-one")
        payload["request_id"] = "response-one"
        runtime.pending_responses["response-one"] = {
            "payload": payload,
            "deadline_at": gateway_module.now() + 10,
        }
        runtime.pending_responses["still-scannable"] = {
            "payload": self.order_message("NEW_ASYNC", "still-scannable"),
            "deadline_at": gateway_module.now() + 10,
        }
        response_path = runtime.helper._response_path("response-one")
        response_path.write_text(
            json.dumps({"ok": True, "data": {"status": "done", "order_id": "1"}}),
            encoding="utf-8",
        )
        delivery_started = asyncio.Event()
        release_delivery = asyncio.Event()
        scan_count = 0
        original_scan = runtime.helper.read_available_responses

        async def counted_scan(*args, **kwargs):
            nonlocal scan_count
            scan_count += 1
            return await original_scan(*args, **kwargs)

        async def blocked_delivery(*args, **kwargs):
            delivery_started.set()
            await release_delivery.wait()
            return True

        runtime.helper.read_available_responses = counted_scan
        proxy.broadcast_confirmed = blocked_delivery
        proxy.running = True
        watcher = asyncio.create_task(proxy.response_watcher_loop(runtime))
        try:
            await asyncio.wait_for(delivery_started.wait(), timeout=1.0)
            self.assertTrue(response_path.exists())
            await wait_until(lambda: scan_count >= 2)
            self.assertTrue(response_path.exists())
            release_delivery.set()
            await wait_until(
                lambda: "response-one" not in runtime.pending_responses
                and not response_path.exists()
            )

            retained_path = runtime.helper._response_path("response-retained")
            retained_response = {
                "ok": True,
                "data": {"status": "done", "order_id": "2"},
            }
            retained_path.write_text(json.dumps(retained_response), encoding="utf-8")
            retained_item = {
                "payload": self.order_message("NEW_ASYNC", "response-retained"),
                "deadline_at": gateway_module.now() + 10,
            }
            runtime.pending_responses["response-retained"] = retained_item

            async def unconfirmed(*args, **kwargs):
                return False

            proxy.broadcast_confirmed = unconfirmed
            await proxy._process_pending_response(
                runtime,
                "response-retained",
                retained_item,
                retained_response,
            )
            self.assertTrue(retained_path.exists())
            self.assertIn("response-retained", runtime.pending_responses)
        finally:
            proxy.running = False
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            for task in list(runtime.response_delivery_tasks.values()):
                task.cancel()
            await asyncio.gather(
                *runtime.response_delivery_tasks.values(), return_exceptions=True
            )
            runtime.response_delivery_tasks.clear()
            runtime.correlation.close()

    async def _response_fallback_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.primary = SimpleNamespace(closed=False)
        request_ids = ["fallback-%02d" % index for index in range(17)]
        for request_id in request_ids:
            payload = self.order_message("NEW_ASYNC", request_id)
            payload["request_id"] = request_id
            runtime.pending_responses[request_id] = {
                "payload": payload,
                "deadline_at": gateway_module.now() + 10,
            }
        target = request_ids[-1]
        target_path = runtime.helper._response_path(target)
        target_path.write_text(
            json.dumps({"ok": True, "data": {"status": "done", "order_id": "17"}}),
            encoding="utf-8",
        )
        checked = []
        original_targeted = runtime.helper.read_targeted_responses

        async def incomplete_scan(*args, **kwargs):
            return {}, set(), False

        async def tracked_targeted(request_id_set):
            checked.extend(request_id_set)
            return await original_targeted(request_id_set)

        async def confirmed(*args, **kwargs):
            return True

        runtime.helper.read_available_responses = incomplete_scan
        runtime.helper.read_targeted_responses = tracked_targeted
        proxy.broadcast_confirmed = confirmed
        proxy.running = True
        watcher = asyncio.create_task(proxy.response_watcher_loop(runtime))
        try:
            await wait_until(
                lambda: target not in runtime.pending_responses,
                timeout=1.0,
            )
            self.assertIn(target, checked)
            self.assertFalse(target_path.exists())
        finally:
            proxy.running = False
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            for task in list(runtime.response_delivery_tasks.values()):
                task.cancel()
            await asyncio.gather(
                *runtime.response_delivery_tasks.values(), return_exceptions=True
            )
            runtime.response_delivery_tasks.clear()
            runtime.correlation.close()

    async def _stop_delivery_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        blocker = asyncio.Event()
        old_task = asyncio.create_task(blocker.wait())
        replacement_task = asyncio.create_task(blocker.wait())
        old_task.cancel()
        await asyncio.gather(old_task, return_exceptions=True)
        runtime.response_delivery_tasks["race"] = replacement_task
        proxy._response_delivery_done(runtime, "race", old_task)
        self.assertIs(runtime.response_delivery_tasks["race"], replacement_task)
        await proxy.stop()
        self.assertTrue(replacement_task.cancelled())
        self.assertFalse(runtime.response_delivery_tasks)

    async def _stop_effect_dispatch_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]

        class FakeWriter:
            def __init__(self):
                self.closed = False

            def get_extra_info(self, name):
                return ("127.0.0.1", 1) if name == "peername" else None

            def close(self):
                self.closed = True

            async def wait_closed(self):
                return None

        session = gateway_module.TcpClientSession(
            runtime,
            asyncio.StreamReader(),
            FakeWriter(),
        )
        ordinary_cancelled = asyncio.Event()
        effect_release = asyncio.Event()
        effect_finished = asyncio.Event()

        async def ordinary_work():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                ordinary_cancelled.set()
                raise

        async def effect_work():
            await effect_release.wait()
            effect_finished.set()

        ordinary_task = asyncio.create_task(ordinary_work())
        effect_task = asyncio.create_task(effect_work())
        session.dispatch_tasks.update((ordinary_task, effect_task))
        session.effect_dispatch_tasks.add(effect_task)
        runtime.clients.add(session)
        runtime.primary = session
        proxy.running = True
        stop_task = asyncio.create_task(proxy.stop())
        try:
            await asyncio.wait_for(ordinary_cancelled.wait(), timeout=1.0)
            self.assertTrue(ordinary_task.cancelled())
            self.assertFalse(effect_task.cancelled())
            self.assertFalse(stop_task.done())
            self.assertTrue(session.closed)
            effect_release.set()
            await asyncio.wait_for(stop_task, timeout=1.0)
            self.assertTrue(effect_finished.is_set())
            self.assertTrue(effect_task.done())
            self.assertFalse(effect_task.cancelled())
            self.assertFalse(session.dispatch_tasks)
            self.assertFalse(session.effect_dispatch_tasks)
            self.assertFalse(runtime.clients)
            await proxy._settle_session_dispatch_tasks(session)
            self.assertFalse(session.dispatch_tasks)
        finally:
            effect_release.set()
            if not stop_task.done():
                await asyncio.gather(stop_task, return_exceptions=True)

    def order_message(self, message_type, identity):
        return {
            "type": message_type,
            "msg_id": identity + "-msg",
            "request_id": identity,
            "client_order_id": identity,
            "trace_id": identity,
            "account_id": project_env.load_deployment(
                ROOT / ".env.example", allow_example=True, environ={}
            )["gateway_config"]["accounts"][0]["account_id"],
            "account_name": project_env.load_deployment(
                ROOT / ".env.example", allow_example=True, environ={}
            )["gateway_config"]["accounts"][0]["name"],
            "symbol": "600000.SH",
            "side": "BUY",
            "quantity": 100,
            "price": 10.0,
        }

    def assert_busy(self, reply, capacity):
        self.assertEqual(reply["status"], "REJECTED")
        self.assertEqual(reply["stage"], "REJECTED")
        self.assertEqual(reply["code"], "GATEWAY_BUSY")
        self.assertFalse(reply["effect_started"])
        self.assertEqual(reply["capacity"], capacity)

    async def _ping_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        await proxy.start()
        account = config.accounts[0]
        try:
            invalid_handshakes = [
                {"type": "PING", "msg_id": "missing-token", "protocol_version": 2,
                 "account_id": account.account_id, "account_name": account.name},
                {"type": "PING", "msg_id": "wrong-protocol", "protocol_version": 1,
                 "account_id": account.account_id, "account_name": account.name,
                 "auth_token": EXAMPLE_AUTH_TOKEN},
                {"type": "PING", "msg_id": "string-protocol", "protocol_version": "2",
                 "account_id": account.account_id, "account_name": account.name,
                 "auth_token": EXAMPLE_AUTH_TOKEN},
                {"type": "PING", "msg_id": "wrong-account", "protocol_version": 2,
                 "account_id": "WRONG", "account_name": account.name,
                 "auth_token": EXAMPLE_AUTH_TOKEN},
                {"type": "PING", "msg_id": "padded-account", "protocol_version": 2,
                 "account_id": account.account_id + " ", "account_name": account.name,
                 "auth_token": EXAMPLE_AUTH_TOKEN},
                {"type": "PING", "msg_id": EXAMPLE_AUTH_TOKEN, "protocol_version": 2,
                 "account_id": account.account_id, "account_name": account.name,
                 "auth_token": EXAMPLE_AUTH_TOKEN},
            ]
            for invalid in invalid_handshakes:
                reader, writer = await asyncio.open_connection("127.0.0.1", account.tcp_port)
                await send_frame(writer, invalid)
                rejected = await asyncio.wait_for(
                    read_frame(reader, 10 * 1024 * 1024), timeout=2.0
                )
                self.assertEqual(rejected["type"], "ERROR")
                self.assertEqual(rejected["code"], "HANDSHAKE_REJECTED")
                self.assertNotIn("account_id", rejected)
                self.assertNotIn("account_name", rejected)
                self.assertNotIn(EXAMPLE_AUTH_TOKEN, json.dumps(rejected))
                writer.close()
                await writer.wait_closed()
            self.assertIsNone(proxy.runtimes[0].primary)

            reader, writer = await asyncio.open_connection("127.0.0.1", account.tcp_port)
            await send_frame(writer, {
                "type": "PING",
                "msg_id": "hello-local",
                "protocol_version": 2,
                "account_id": account.account_id,
                "account_name": account.name,
                "auth_token": EXAMPLE_AUTH_TOKEN,
            })
            pong = await asyncio.wait_for(read_frame(reader, 10 * 1024 * 1024), timeout=2.0)
            self.assertEqual(pong["type"], "PONG")
            self.assertEqual(pong["msg_id"], "hello-local")
            self.assertEqual(pong["protocol_version"], 2)
            self.assertEqual(pong["build_id"], project_env.GATEWAY_BUILD_ID)
            self.assertEqual(pong["account_id"], account.account_id)
            self.assertEqual(pong["account_name"], account.name)
            self.assertNotIn("auth_token", pong)
            await send_frame(writer, {
                "type": "QUERY", "msg_id": "missing-business-identity",
                "query_type": "ACCOUNT_STATUS", "params": {},
            })
            rejected = await asyncio.wait_for(
                read_frame(reader, 10 * 1024 * 1024), timeout=2.0
            )
            self.assertEqual(rejected["code"], "ACCOUNT_MISMATCH")
            await send_frame(writer, {
                "type": "QUERY", "msg_id": "padded-business-identity",
                "account_id": account.account_id,
                "account_name": account.name + " ",
                "query_type": "ACCOUNT_STATUS", "params": {},
            })
            rejected = await asyncio.wait_for(
                read_frame(reader, 10 * 1024 * 1024), timeout=2.0
            )
            self.assertEqual(rejected["code"], "ACCOUNT_MISMATCH")
            await send_frame(writer, {
                "type": "NEW_ASYNC", "msg_id": "missing-client-order-id",
                "account_id": account.account_id, "account_name": account.name,
                "symbol": "600000.SH", "side": "BUY", "quantity": 100,
                "price": 10.0,
            })
            rejected = await asyncio.wait_for(
                read_frame(reader, 10 * 1024 * 1024), timeout=2.0
            )
            self.assertEqual(rejected["code"], "CLIENT_ORDER_ID_REQUIRED")
            await send_frame(writer, {
                "type": "QUERY", "msg_id": EXAMPLE_AUTH_TOKEN,
                "account_id": account.account_id, "account_name": account.name,
                "query_type": "ACCOUNT_STATUS", "params": {},
            })
            rejected = await asyncio.wait_for(
                read_frame(reader, 10 * 1024 * 1024), timeout=2.0
            )
            self.assertEqual(rejected["code"], "INVALID_MESSAGE")
            self.assertNotIn(EXAMPLE_AUTH_TOKEN, json.dumps(rejected))
            writer.close()
            await writer.wait_closed()
        finally:
            await proxy.stop()

    async def _api_gateway_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        await proxy.start()
        account = config.accounts[0]
        api = LocalQmtApi(ConnectionConfig(
            account_name=account.name,
            account_id=account.account_id,
            account_type=account.account_type,
            auth_token=EXAMPLE_AUTH_TOKEN,
            port=account.tcp_port,
            auto_reconnect=False,
        ))
        try:
            self.assertTrue(await asyncio.to_thread(api.connect, 2.0))
            self.assertTrue(api.is_connected)
            self.assertFalse(api.identity_guard_failed)
            response = await asyncio.to_thread(api.query, "ACCOUNT_STATUS", {}, 2.0)
            self.assertIsInstance(response, dict)
            self.assertEqual(response.get("msg_id") is not None, True)
        finally:
            await asyncio.to_thread(api.stop)
            await proxy.stop()


if __name__ == "__main__":
    unittest.main()
