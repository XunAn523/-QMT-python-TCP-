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
    HelperUnavailable,
    HelperTimeout,
    atomic_write_json,
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

    def test_helper_readiness_hot_path_uses_fresh_fail_closed_cache(self):
        asyncio.run(self._helper_health_cache_contract())

    def test_async_cancel_tracks_and_delivers_final_helper_response(self):
        asyncio.run(self._async_cancel_response_contract())

    def test_pending_cancel_response_recovers_after_gateway_restart(self):
        asyncio.run(self._pending_cancel_restart_contract())

    def test_start_failure_rolls_back_watchers_lease_and_io_lanes(self):
        asyncio.run(self._start_failure_rollback_contract())

    def test_automatic_maintenance_never_releases_order_idempotency(self):
        asyncio.run(self._maintenance_idempotency_contract())

    def test_async_cancel_without_ids_gets_one_canonical_capacity_key(self):
        asyncio.run(self._canonical_cancel_request_id_contract())

    def test_submit_unknown_correlation_alone_is_not_a_pending_delivery(self):
        asyncio.run(self._correlation_is_not_delivery_ledger_contract())

    def test_atomic_write_recovers_when_hot_path_parent_was_removed(self):
        target = self.temp_dir / "recreated" / "request.json"
        atomic_write_json(target, {"request_id": "one"}, ensure_parent=False)
        self.assertEqual(
            json.loads(target.read_text(encoding="utf-8")),
            {"request_id": "one"},
        )

    def test_request_id_cannot_be_rebound_to_a_different_intent(self):
        asyncio.run(self._request_id_intent_conflict_contract())

    def test_prepared_effect_is_safely_taken_over_after_restart(self):
        asyncio.run(self._prepared_crash_takeover_contract())

    def test_dispatching_restart_is_fail_closed_and_result_replays_new_msg_id(self):
        asyncio.run(self._dispatching_restart_contract())

    def test_file_io_full_is_not_enqueued_and_same_key_can_retry(self):
        asyncio.run(self._file_io_full_retry_contract())

    def test_new_pre_start_generic_failure_remains_retryable(self):
        asyncio.run(self._new_pre_start_generic_failure_contract())

    def test_sync_new_post_enqueue_poll_io_full_stays_fail_closed(self):
        asyncio.run(self._sync_new_post_enqueue_poll_io_full_contract())

    def test_sync_cancel_post_enqueue_poll_io_full_stays_fail_closed(self):
        asyncio.run(self._sync_cancel_post_enqueue_poll_io_full_contract())

    def test_sync_new_transient_post_enqueue_io_full_recovers_response(self):
        asyncio.run(self._sync_new_transient_post_enqueue_io_full_contract())

    def test_offline_response_is_captured_and_delivered_after_restart(self):
        asyncio.run(self._offline_response_capture_contract())

    def test_unverifiable_helper_response_becomes_submit_unknown(self):
        asyncio.run(self._unverifiable_response_contract())

    def test_command_filenames_sort_by_gateway_sequence(self):
        self._command_filename_sequence_contract()

    def test_sequenced_command_file_is_idempotent_and_all_siblings_validate(self):
        self._sequenced_command_idempotency_contract()

    def test_recovered_and_v1_command_siblings_are_fail_closed(self):
        self._recovered_and_v1_command_sibling_contract()

    def test_v1_inbox_json_counts_toward_command_high_water(self):
        self._v1_command_depth_contract()

    def test_sync_cancel_timeout_is_explicit_submit_unknown(self):
        asyncio.run(self._sync_cancel_timeout_contract())

    def test_delivery_ack_is_bound_to_the_target_session(self):
        asyncio.run(self._session_bound_delivery_ack_contract())

    def test_rejected_client_send_failure_cannot_poison_enqueue_order(self):
        asyncio.run(self._rejected_send_failure_enqueue_contract())

    def test_effect_files_publish_in_tcp_receive_order(self):
        asyncio.run(self._effect_enqueue_order_contract())

    def test_live_event_failure_stops_later_callback_delivery(self):
        asyncio.run(self._live_event_failure_order_contract())

    def test_reliable_helper_event_requires_stable_event_id(self):
        asyncio.run(self._reliable_event_identity_contract())

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
                (
                    runtime.helper.commands
                    / gateway_module.command_request_filename(
                        "async-timeout-queued",
                        {"request_id": "async-timeout-queued"},
                    )
                ).is_file()
            )
            self.assertTrue(
                (
                    runtime.helper.queries
                    / (gateway_module.request_file_key("async-timeout-error") + ".json")
                ).is_file()
            )
            self.assertFalse(
                (runtime.helper.commands / "async-timeout-queued.json").exists()
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

        async def queued_timeout(payload, wait, enqueue_done=None):
            self.assertTrue(wait)
            if enqueue_done is not None:
                enqueue_done()
            return {
                "status": "queued",
                "queued": True,
                "timeout": True,
                "request_id": payload["request_id"],
            }

        runtime.helper.place_order = queued_timeout
        try:
            reply = await proxy.dispatch(
                runtime,
                None,
                self.order_message("NEW", "sync-timeout"),
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
            reply = await proxy.dispatch(
                runtime,
                None,
                self.order_message("NEW", "sync-post-effect-error"),
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

        async def submit_unknown(payload, wait, enqueue_done=None):
            self.assertTrue(wait)
            if enqueue_done is not None:
                enqueue_done()
            return {
                "status": "submit_unknown",
                "request_id": payload["request_id"],
                "error": "QMT returned no stable order id",
            }

        runtime.helper.place_order = submit_unknown
        try:
            reply = await proxy.dispatch(
                runtime,
                None,
                self.order_message("NEW", "sync-direct-unknown"),
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
                    sync_reply = await proxy.dispatch(
                        runtime,
                        None,
                        replay,
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
                    async_replay["request_id"] = identity + "-async-request"
                    async_reply = await proxy.dispatch(
                        runtime,
                        None,
                        async_replay,
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
            runtime.command_queue_depth = (
                gateway_module.COMMAND_QUEUE_HIGH_WATER_PER_ACCOUNT
            )
            reply = await proxy.dispatch(
                runtime, None, self.order_message("NEW_ASYNC", "command-full")
            )
            self.assert_busy(reply, "account_command_queue")
            runtime.command_queue_depth = 0

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
                    "kind": "order",
                    "fingerprint": "occupied-fingerprint",
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
                    "kind": "order",
                    "fingerprint": "same-request-fingerprint",
                    "payload": {},
                    "deadline_at": gateway_module.now() + 10,
                }
                runtime.response_delivery_tasks["same-request"] = delivery_task
                available, owned, code = await runtime.try_reserve_pending_response(
                    "new-request",
                    "new-request-fingerprint",
                )
                self.assertTrue(available)
                self.assertTrue(owned)
                self.assertEqual(code, "")
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

    async def _helper_health_cache_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.helper.health = mock.AsyncMock(return_value={
            "ready": True,
            "state": "ready",
            "last_error": "",
            "command_queue_depth": 3,
        })
        try:
            await proxy._sample_helper_health(runtime)
            with mock.patch.object(
                gateway_module,
                "helper_identity_mismatches",
                return_value=[],
            ):
                first = await proxy._ensure_helper_ready(runtime)
                second = await proxy._ensure_helper_ready(runtime)
                self.assertTrue(first["ready"])
                self.assertTrue(second["ready"])
                runtime.helper.health.assert_awaited_once()
                self.assertEqual(runtime.command_queue_depth, 3)

                runtime.helper_health_sampled_monotonic -= (
                    gateway_module.HELPER_HEALTH_CACHE_MAX_AGE_SECONDS + 1.0
                )
                with self.assertRaises(HelperUnavailable) as raised:
                    await proxy._ensure_helper_ready(runtime)
                self.assertEqual(raised.exception.code, "HELPER_HEALTH_STALE")
                runtime.helper.health.assert_awaited_once()
        finally:
            runtime.correlation.close()
            runtime.db_io.close()
            runtime.file_io.close()

    async def _async_cancel_response_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        async def cancel_after_durable_reservation(
            payload,
            wait,
            enqueue_done=None,
        ):
            self.assertFalse(wait)
            if enqueue_done is not None:
                enqueue_done()
            persisted = runtime.correlation.load_pending_responses(
                runtime.cfg.account_id
            )
            self.assertEqual(len(persisted), 1)
            self.assertEqual(persisted[0]["request_id"], "cancel-track")
            return {"status": "queued", "request_id": payload["request_id"]}

        runtime.helper.cancel_order = mock.AsyncMock(
            side_effect=cancel_after_durable_reservation
        )
        delivered_messages = []

        async def delivered(_runtime, message, _delivery_id, timeout=1.0):
            delivered_messages.append(dict(message))
            return True

        proxy.broadcast_confirmed = delivered
        runtime.helper.ack_response = mock.AsyncMock()
        message = {
            "type": "CANCEL_ASYNC",
            "msg_id": "cancel-track-msg",
            "request_id": "cancel-track",
            "account_id": runtime.cfg.account_id,
            "account_name": runtime.cfg.name,
            "order_id": "1001",
        }
        try:
            reply = await proxy.dispatch(runtime, None, message)
            self.assertEqual(reply["type"], "ASYNC_CANCEL")
            self.assertIn("cancel-track", runtime.pending_responses)
            self.assertEqual(
                runtime.pending_responses["cancel-track"]["kind"], "cancel",
            )
            await proxy._process_pending_response(
                runtime,
                "cancel-track",
                runtime.pending_responses["cancel-track"],
                {
                    "ok": True,
                    "request_id": "cancel-track",
                    "gateway_effect_fingerprint": runtime.pending_responses[
                        "cancel-track"
                    ]["fingerprint"],
                    "data": {"status": "done", "order_id": "1001"},
                },
            )
            self.assertNotIn("cancel-track", runtime.pending_responses)
            self.assertEqual(
                runtime.correlation.load_pending_responses(runtime.cfg.account_id),
                [],
            )
            runtime.helper.ack_response.assert_awaited_once_with("cancel-track")
            self.assertEqual(delivered_messages[0]["type"], "ASYNC_CANCEL_RESPONSE")
            self.assertEqual(delivered_messages[0]["stage"], "CANCEL_SUBMITTED")
        finally:
            runtime.correlation.close()
            runtime.db_io.close()
            runtime.file_io.close()

    async def _pending_cancel_restart_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        first_proxy = BigQmtGatewayProxy(config, logger)
        first = first_proxy.runtimes[0]
        fingerprint = "sha256:cancel-recovered-fingerprint"
        item = {
            "kind": "cancel",
            "fingerprint": fingerprint,
            "payload": {
                "request_id": "cancel-recovered",
                "msg_id": "cancel-recovered-msg",
                "order_id": "1002",
            },
            "queued_at": gateway_module.now(),
            "deadline_at": gateway_module.now() + 8.0,
        }
        first.correlation.reserve_effect_request(
            first.cfg.account_id,
            "cancel-recovered",
            "cancel_order",
            fingerprint,
        )
        first.correlation.transition_effect_request(
            first.cfg.account_id,
            "cancel-recovered",
            fingerprint,
            "ENQUEUED",
        )
        await first.commit_pending_response("cancel-recovered", item)
        first.correlation.close()
        first.db_io.close()
        first.file_io.close()

        second_proxy = BigQmtGatewayProxy(config, logger)
        second = second_proxy.runtimes[0]
        try:
            self.assertIn("cancel-recovered", second.pending_responses)
            self.assertEqual(
                second.pending_responses["cancel-recovered"]["kind"],
                "cancel",
            )
            self.assertEqual(
                second.pending_responses["cancel-recovered"]["payload"]["order_id"],
                "1002",
            )
            await second.remove_pending_response("cancel-recovered")
        finally:
            second.correlation.close()
            second.db_io.close()
            second.file_io.close()

    async def _start_failure_rollback_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind((runtime.cfg.tcp_host, runtime.cfg.tcp_port))
        blocker.listen(1)
        try:
            with self.assertRaises(OSError):
                await proxy.start()
            self.assertFalse(proxy.running)
            self.assertFalse(runtime.writer_lease.acquired)
            self.assertIsNone(runtime.response_change_watcher)
            self.assertIsNone(runtime.event_change_watcher)
            self.assertTrue(runtime.file_io.closed)
            self.assertTrue(runtime.db_io.closed)
            self.assertFalse(proxy.poll_tasks)
            await proxy.stop()
        finally:
            blocker.close()

    async def _maintenance_idempotency_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        message = self.order_message("NEW_ASYNC", "retained-idempotency")
        payload = proxy.order_payload(runtime, message)
        runtime.correlation.reserve({**payload, "stage": "RESERVED"})
        runtime.correlation.update_stage(
            runtime.cfg.account_id,
            "retained-idempotency",
            "FILLED",
        )
        old = gateway_module.now() - 10 * 86400.0
        runtime.correlation.mark_event(runtime.cfg.account_id, "old-event")
        with runtime.correlation._lock:
            runtime.correlation._db.execute(
                "UPDATE order_correlation SET terminal_at=? "
                "WHERE account_id=? AND client_order_id=?",
                (old, runtime.cfg.account_id, "retained-idempotency"),
            )
            runtime.correlation._db.execute(
                "UPDATE gateway_event_dedupe SET created_at=? "
                "WHERE account_id=? AND event_id=?",
                (old, runtime.cfg.account_id, "old-event"),
            )
            runtime.correlation._db.commit()
        try:
            result = await runtime.db_io.run(
                proxy._maintain_correlation_store,
                runtime,
                gateway_module.now() - 8 * 86400.0,
            )
            self.assertEqual(result["orders_deleted"], 0)
            self.assertEqual(result["events_deleted"], 1)
            self.assertIsNotNone(
                runtime.correlation.get(
                    runtime.cfg.account_id,
                    "retained-idempotency",
                )
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _canonical_cancel_request_id_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def queued(payload, wait, enqueue_done=None):
            self.assertFalse(wait)
            if enqueue_done is not None:
                enqueue_done()
            self.assertTrue(payload["request_id"])
            return {"status": "queued", "request_id": payload["request_id"]}

        runtime.helper.cancel_order = mock.AsyncMock(side_effect=queued)
        try:
            reply = await proxy.dispatch(runtime, None, {
                "type": "CANCEL_ASYNC",
                "account_id": runtime.cfg.account_id,
                "account_name": runtime.cfg.name,
                "order_id": "1003",
            })
            request_id = reply["request_id"]
            self.assertTrue(request_id.startswith("cancel-"))
            self.assertNotIn("", runtime.pending_responses)
            self.assertIn(request_id, runtime.pending_responses)
            self.assertFalse(runtime.pending_response_reservations)
            await runtime.remove_pending_response(request_id)
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _correlation_is_not_delivery_ledger_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        first_proxy = BigQmtGatewayProxy(config, logger)
        first = first_proxy.runtimes[0]
        payload = first_proxy.order_payload(
            first,
            self.order_message("NEW", "unknown-without-ledger"),
        )
        first.correlation.reserve({**payload, "stage": "RESERVED"})
        first.correlation.update_stage(
            first.cfg.account_id,
            "unknown-without-ledger",
            "SUBMIT_UNKNOWN",
        )
        first.db_io.close()
        first.correlation.close()
        first.file_io.close()

        second_proxy = BigQmtGatewayProxy(config, logger)
        second = second_proxy.runtimes[0]
        try:
            self.assertEqual(second.pending_responses, {})
            self.assertEqual(
                second.correlation.get(
                    second.cfg.account_id,
                    "unknown-without-ledger",
                )["stage"],
                "SUBMIT_UNKNOWN",
            )
        finally:
            second.db_io.close()
            second.correlation.close()
            second.file_io.close()

    async def _whitespace_request_id_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def queued(payload, wait, enqueue_done=None):
            self.assertFalse(wait)
            if enqueue_done is not None:
                enqueue_done()
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
        response_fingerprint = "sha256:response-one-fingerprint"
        payload = self.order_message("NEW_ASYNC", "response-one")
        payload["request_id"] = "response-one"
        runtime.pending_responses["response-one"] = {
            "kind": "order",
            "fingerprint": response_fingerprint,
            "payload": payload,
            "deadline_at": gateway_module.now() + 10,
        }
        runtime.pending_responses["still-scannable"] = {
            "kind": "order",
            "fingerprint": "sha256:still-scannable-fingerprint",
            "payload": self.order_message("NEW_ASYNC", "still-scannable"),
            "deadline_at": gateway_module.now() + 10,
        }
        runtime.correlation.reserve_effect_request(
            runtime.cfg.account_id,
            "response-one",
            "order",
            response_fingerprint,
        )
        runtime.correlation.transition_effect_request(
            runtime.cfg.account_id,
            "response-one",
            response_fingerprint,
            "ENQUEUED",
        )
        response_path = runtime.helper._response_path("response-one")
        response_path.write_text(
            json.dumps({
                "ok": True,
                "request_id": "response-one",
                "gateway_effect_fingerprint": response_fingerprint,
                "data": {"status": "done", "order_id": "1"},
            }),
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
            self.assertFalse(response_path.exists())
            self.assertTrue(
                runtime.pending_responses["response-one"]["response_captured"]
            )
            await wait_until(lambda: scan_count >= 2)
            self.assertFalse(response_path.exists())
            self.assertIn("response-one", runtime.pending_responses)
            release_delivery.set()
            await wait_until(
                lambda: "response-one" not in runtime.pending_responses
                and not response_path.exists()
            )

            retained_path = runtime.helper._response_path("response-retained")
            retained_fingerprint = "sha256:response-retained-fingerprint"
            retained_response = {
                "ok": True,
                "request_id": "response-retained",
                "gateway_effect_fingerprint": retained_fingerprint,
                "data": {"status": "done", "order_id": "2"},
            }
            retained_path.write_text(json.dumps(retained_response), encoding="utf-8")
            retained_item = {
                "kind": "order",
                "fingerprint": retained_fingerprint,
                "payload": self.order_message("NEW_ASYNC", "response-retained"),
                "deadline_at": gateway_module.now() + 10,
            }
            runtime.pending_responses["response-retained"] = retained_item
            runtime.correlation.reserve_effect_request(
                runtime.cfg.account_id,
                "response-retained",
                "order",
                retained_fingerprint,
            )
            runtime.correlation.transition_effect_request(
                runtime.cfg.account_id,
                "response-retained",
                retained_fingerprint,
                "ENQUEUED",
            )

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
            fingerprint = "sha256:%s-fingerprint" % request_id
            payload = self.order_message("NEW_ASYNC", request_id)
            payload["request_id"] = request_id
            runtime.pending_responses[request_id] = {
                "kind": "order",
                "fingerprint": fingerprint,
                "payload": payload,
                "deadline_at": gateway_module.now() + 10,
            }
        target = request_ids[-1]
        target_fingerprint = runtime.pending_responses[target]["fingerprint"]
        runtime.correlation.reserve_effect_request(
            runtime.cfg.account_id,
            target,
            "order",
            target_fingerprint,
        )
        runtime.correlation.transition_effect_request(
            runtime.cfg.account_id,
            target,
            target_fingerprint,
            "ENQUEUED",
        )
        target_path = runtime.helper._response_path(target)
        target_path.write_text(
            json.dumps({
                "ok": True,
                "request_id": target,
                "gateway_effect_fingerprint": target_fingerprint,
                "data": {"status": "done", "order_id": "17"},
            }),
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

    async def _request_id_intent_conflict_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        runtime.helper.place_order = mock.AsyncMock(
            return_value={
                "status": "done",
                "request_id": "intent-conflict",
                "order_id": "9001",
            }
        )
        first = self.order_message("NEW", "intent-conflict")
        try:
            accepted = await proxy.dispatch(runtime, None, first)
            self.assertEqual(accepted["status"], "ACCEPTED")
            original_record = runtime.correlation.get_effect_request(
                runtime.cfg.account_id,
                "intent-conflict",
            )
            self.assertEqual(original_record["state"], "ENQUEUED")

            conflicting = dict(first)
            conflicting["msg_id"] = "intent-conflict-second-msg"
            conflicting["price"] = 10.01
            rejected = await proxy.dispatch(runtime, None, conflicting)
            self.assertEqual(rejected["status"], "REJECTED")
            self.assertEqual(rejected["code"], "REQUEST_ID_CONFLICT")
            self.assertFalse(rejected["retryable"])
            runtime.helper.place_order.assert_awaited_once()
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    "intent-conflict",
                ),
                original_record,
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _prepared_crash_takeover_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        first_proxy = BigQmtGatewayProxy(config, logger)
        first = first_proxy.runtimes[0]
        message = self.order_message("NEW_ASYNC", "prepared-takeover")
        effect_kind, fingerprint = first_proxy.effect_request_identity(first, message)
        prepared_message = dict(message)
        prepared_message["_gateway_effect_fingerprint"] = fingerprint
        prepared_message["_gateway_enqueue_seq"] = first.next_gateway_enqueue_seq()
        payload = first_proxy.order_payload(first, prepared_message)
        first.correlation.reserve({**payload, "stage": "RESERVED"})
        first.correlation.reserve_effect_request(
            first.cfg.account_id,
            "prepared-takeover",
            effect_kind,
            fingerprint,
        )
        await first.commit_pending_response(
            "prepared-takeover",
            {
                "kind": "order",
                "fingerprint": fingerprint,
                "payload": dict(payload),
                "queued_at": gateway_module.now(),
                "deadline_at": gateway_module.now() + 8.0,
            },
        )
        first.db_io.close()
        first.correlation.close()
        first.file_io.close()

        second_proxy = BigQmtGatewayProxy(config, logger)
        second = second_proxy.runtimes[0]
        second_proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        second.helper.place_order = mock.AsyncMock(
            return_value={"status": "queued", "request_id": "prepared-takeover"}
        )
        try:
            self.assertEqual(second.pending_responses, {})
            self.assertIsNone(
                second.correlation.get(second.cfg.account_id, "prepared-takeover")
            )
            self.assertEqual(
                second.correlation.get_effect_request(
                    second.cfg.account_id,
                    "prepared-takeover",
                )["state"],
                "PREPARED",
            )
            retry = dict(message)
            retry["msg_id"] = "prepared-takeover-retry-msg"
            reply = await second_proxy.dispatch(second, None, retry)
            self.assertEqual(reply["status"], "SENT")
            second.helper.place_order.assert_awaited_once()
            self.assertEqual(
                second.correlation.get_effect_request(
                    second.cfg.account_id,
                    "prepared-takeover",
                )["state"],
                "ENQUEUED",
            )
            self.assertEqual(
                second.correlation.get(
                    second.cfg.account_id,
                    "prepared-takeover",
                )["stage"],
                "BRIDGE_QUEUED",
            )
            self.assertIn("prepared-takeover", second.pending_responses)
        finally:
            second.db_io.close()
            second.correlation.close()
            second.file_io.close()

    async def _dispatching_restart_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        first_proxy = BigQmtGatewayProxy(config, logger)
        first = first_proxy.runtimes[0]
        message = self.order_message("NEW", "dispatching-restart")
        effect_kind, fingerprint = first_proxy.effect_request_identity(first, message)
        first.correlation.reserve_effect_request(
            first.cfg.account_id,
            "dispatching-restart",
            effect_kind,
            fingerprint,
        )
        self.assertTrue(first.correlation.transition_effect_request(
            first.cfg.account_id,
            "dispatching-restart",
            fingerprint,
            "DISPATCHING",
        ))
        first.db_io.close()
        first.correlation.close()
        first.file_io.close()

        second_proxy = BigQmtGatewayProxy(config, logger)
        second = second_proxy.runtimes[0]
        second.helper.place_order = mock.AsyncMock(
            side_effect=AssertionError("DISPATCHING replay must never reach Helper")
        )
        try:
            after_restart = dict(message)
            after_restart["msg_id"] = "dispatching-restart-new-msg"
            unknown = await second_proxy.dispatch(second, None, after_restart)
            self.assertEqual(unknown["status"], "UNKNOWN")
            self.assertEqual(unknown["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(unknown["code"], "EFFECT_STATE_UNKNOWN")
            self.assertTrue(unknown["effect_started"])
            second.helper.place_order.assert_not_awaited()

            persisted_result = {
                "type": "EXEC_REPORT",
                "msg_id": "stale-msg-id",
                "request_id": "dispatching-restart",
                "client_order_id": "dispatching-restart",
                "trace_id": "stale-trace",
                "status": "UNKNOWN",
                "stage": "SUBMIT_UNKNOWN",
                "submit_result": "UNKNOWN",
                "code": "POST_ENQUEUE_STATE_UNCERTAIN",
            }
            self.assertTrue(second.correlation.transition_effect_request(
                second.cfg.account_id,
                "dispatching-restart",
                fingerprint,
                "UNKNOWN",
                persisted_result,
                allowed_from=("DISPATCHING",),
            ))
            replay_message = dict(message)
            replay_message["msg_id"] = "dispatching-restart-replay-msg"
            replay_message["trace_id"] = "dispatching-restart-new-trace"
            replay = await second_proxy.dispatch(second, None, replay_message)
            self.assertTrue(replay["idempotent"])
            self.assertTrue(replay["cached"])
            self.assertEqual(replay["dedupe_layer"], "gateway_effect_registry")
            self.assertEqual(replay["msg_id"], replay_message["msg_id"])
            self.assertEqual(replay["trace_id"], replay_message["trace_id"])
            self.assertEqual(replay["code"], "POST_ENQUEUE_STATE_UNCERTAIN")
            second.helper.place_order.assert_not_awaited()
        finally:
            second.db_io.close()
            second.correlation.close()
            second.file_io.close()

    async def _new_pre_start_generic_failure_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        original_commit_pending_response = runtime.commit_pending_response
        runtime.commit_pending_response = mock.AsyncMock(
            side_effect=RuntimeError("intentional pre-start pending ledger failure")
        )
        runtime.helper.place_order = mock.AsyncMock(
            return_value={
                "status": "queued",
                "request_id": "pre-start-generic-retry",
            }
        )
        message = self.order_message("NEW_ASYNC", "pre-start-generic-retry")
        try:
            rejected = await proxy.dispatch(runtime, None, dict(message))
            self.assertEqual(rejected["status"], "REJECTED")
            self.assertEqual(rejected["stage"], "REJECTED")
            self.assertEqual(rejected["code"], "HELPER_NOT_READY")
            self.assertFalse(rejected["effect_started"])
            self.assertTrue(rejected["retryable"])
            self.assertIsNone(
                runtime.correlation.get(
                    runtime.cfg.account_id,
                    message["client_order_id"],
                )
            )
            self.assertNotIn(message["request_id"], runtime.pending_responses)
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    message["request_id"],
                )["state"],
                "PREPARED",
            )
            runtime.helper.place_order.assert_not_awaited()

            runtime.commit_pending_response = original_commit_pending_response
            retry = dict(message)
            retry["msg_id"] = "pre-start-generic-retry-second-msg"
            sent = await proxy.dispatch(runtime, None, retry)
            self.assertEqual(sent["status"], "SENT")
            self.assertEqual(sent["stage"], "BRIDGE_QUEUED")
            runtime.helper.place_order.assert_awaited_once()
            self.assertIn(message["request_id"], runtime.pending_responses)
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    message["request_id"],
                )["state"],
                "ENQUEUED",
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _file_io_full_retry_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        real_place_order = runtime.helper.place_order
        runtime.helper.place_order = mock.AsyncMock(
            side_effect=gateway_module.IoLaneFull(runtime.file_io.name, 1)
        )
        message = self.order_message("NEW_ASYNC", "file-lane-retry")
        try:
            busy = await proxy.dispatch(runtime, None, message)
            self.assert_busy(busy, "account_file_io")
            self.assertEqual(list(runtime.helper.commands.glob("*.json")), [])
            self.assertIsNone(
                runtime.correlation.get(runtime.cfg.account_id, "file-lane-retry")
            )
            self.assertNotIn("file-lane-retry", runtime.pending_responses)
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    "file-lane-retry",
                )["state"],
                "PREPARED",
            )

            runtime.helper.place_order = real_place_order
            retry = dict(message)
            retry["msg_id"] = "file-lane-retry-second-msg"
            sent = await proxy.dispatch(runtime, None, retry)
            self.assertEqual(sent["status"], "SENT")
            files = list(runtime.helper.commands.glob("*.json"))
            self.assertEqual(len(files), 1)
            request = json.loads(files[0].read_text(encoding="utf-8"))
            self.assertEqual(request["request_id"], "file-lane-retry")
            self.assertEqual(
                files[0].name,
                gateway_module.command_request_filename(
                    "file-lane-retry",
                    request["payload"],
                ),
            )
            self.assertEqual(
                request["gateway_effect_fingerprint"],
                runtime.pending_responses["file-lane-retry"]["fingerprint"],
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _sync_new_post_enqueue_poll_io_full_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.cfg.request_timeout_seconds = 0.02
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def publish_then_saturate_poll(func, *args):
            if getattr(func, "__name__", "") == "_read_response_if_ready_sync":
                raise gateway_module.IoLaneFull(runtime.file_io.name, 1)
            return func(*args)

        runtime.helper._io_runner = publish_then_saturate_poll
        message = self.order_message("NEW", "post-enqueue-order-io-full")
        try:
            reply = await proxy.dispatch(runtime, None, dict(message))
            self.assertEqual(reply["status"], "UNKNOWN")
            self.assertEqual(reply["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(reply["code"], "HELPER_RESPONSE_TIMEOUT")
            files = list(runtime.helper.commands.glob("*.json"))
            self.assertEqual(len(files), 1)
            self.assertEqual(
                json.loads(files[0].read_text(encoding="utf-8"))["request_id"],
                message["request_id"],
            )
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    message["request_id"],
                )["state"],
                "UNKNOWN",
            )
            self.assertEqual(
                runtime.correlation.get(
                    runtime.cfg.account_id,
                    message["client_order_id"],
                )["stage"],
                "SUBMIT_UNKNOWN",
            )

            retry_message = dict(message)
            retry_message["msg_id"] = "post-enqueue-order-io-full-retry-msg"
            replay = await proxy.dispatch(runtime, None, retry_message)
            self.assertTrue(replay["idempotent"])
            self.assertEqual(replay["dedupe_layer"], "gateway_effect_registry")
            self.assertEqual(replay["status"], "UNKNOWN")
            self.assertEqual(len(list(runtime.helper.commands.glob("*.json"))), 1)
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _sync_cancel_post_enqueue_poll_io_full_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.cfg.request_timeout_seconds = 0.02
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def publish_then_saturate_poll(func, *args):
            if getattr(func, "__name__", "") == "_read_response_if_ready_sync":
                raise gateway_module.IoLaneFull(runtime.file_io.name, 1)
            return func(*args)

        runtime.helper._io_runner = publish_then_saturate_poll
        message = {
            "type": "CANCEL",
            "msg_id": "post-enqueue-cancel-io-full-msg",
            "request_id": "post-enqueue-cancel-io-full",
            "account_id": runtime.cfg.account_id,
            "account_name": runtime.cfg.name,
            "order_id": "9401",
        }
        try:
            reply = await proxy.dispatch(runtime, None, dict(message))
            self.assertEqual(reply["status"], "UNKNOWN")
            self.assertEqual(reply["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(reply["code"], "HELPER_RESPONSE_TIMEOUT")
            files = list(runtime.helper.commands.glob("*.json"))
            self.assertEqual(len(files), 1)
            self.assertEqual(
                json.loads(files[0].read_text(encoding="utf-8"))["request_id"],
                message["request_id"],
            )
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    message["request_id"],
                )["state"],
                "UNKNOWN",
            )

            retry_message = dict(message)
            retry_message["msg_id"] = "post-enqueue-cancel-io-full-retry-msg"
            replay = await proxy.dispatch(runtime, None, retry_message)
            self.assertTrue(replay["idempotent"])
            self.assertEqual(replay["dedupe_layer"], "gateway_effect_registry")
            self.assertEqual(replay["status"], "UNKNOWN")
            self.assertEqual(len(list(runtime.helper.commands.glob("*.json"))), 1)
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _sync_new_transient_post_enqueue_io_full_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        poll_attempts = 0

        async def publish_then_recover_poll(func, *args):
            nonlocal poll_attempts
            if getattr(func, "__name__", "") == "_read_response_if_ready_sync":
                poll_attempts += 1
                if poll_attempts == 1:
                    raise gateway_module.IoLaneFull(runtime.file_io.name, 1)
                request_id, action, payload = args
                atomic_write_json(
                    runtime.helper._response_path(request_id),
                    {
                        "ok": True,
                        "request_id": request_id,
                        "action": action,
                        "gateway_effect_fingerprint": payload[
                            "gateway_effect_fingerprint"
                        ],
                        "data": {
                            "status": "done",
                            "request_id": request_id,
                            "order_id": "9501",
                        },
                    },
                    ensure_parent=False,
                )
            return func(*args)

        runtime.helper._io_runner = publish_then_recover_poll
        message = self.order_message("NEW", "post-enqueue-io-recovered")
        try:
            reply = await proxy.dispatch(runtime, None, message)
            self.assertEqual(poll_attempts, 2)
            self.assertEqual(reply["status"], "ACCEPTED")
            self.assertEqual(reply["stage"], "QMT_SUBMITTED")
            self.assertEqual(reply["order_id"], "9501")
            self.assertEqual(len(list(runtime.helper.commands.glob("*.json"))), 1)
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    message["request_id"],
                )["state"],
                "ENQUEUED",
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _offline_response_capture_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        first_proxy = BigQmtGatewayProxy(config, logger)
        first = first_proxy.runtimes[0]
        first_proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        first.helper.place_order = mock.AsyncMock(
            return_value={"status": "queued", "request_id": "offline-capture"}
        )
        message = self.order_message("NEW_ASYNC", "offline-capture")
        sent = await first_proxy.dispatch(first, None, message)
        self.assertEqual(sent["status"], "SENT")
        item = first.pending_responses["offline-capture"]
        fingerprint = item["fingerprint"]
        response = {
            "ok": True,
            "request_id": "offline-capture",
            "gateway_effect_fingerprint": fingerprint,
            "data": {"status": "done", "order_id": "9101"},
        }
        response_path = first.helper._response_path("offline-capture")
        response_path.write_text(json.dumps(response), encoding="utf-8")
        await first_proxy._process_pending_response(
            first,
            "offline-capture",
            item,
            response,
        )
        self.assertFalse(response_path.exists())
        self.assertTrue(first.pending_responses["offline-capture"]["response_captured"])
        self.assertEqual(
            first.pending_responses["offline-capture"]["ready_response"],
            response,
        )
        first.db_io.close()
        first.correlation.close()
        first.file_io.close()

        second_proxy = BigQmtGatewayProxy(config, logger)
        second = second_proxy.runtimes[0]
        delivered = []

        async def confirmed(_runtime, outbound, _delivery_id, timeout=1.0):
            delivered.append(dict(outbound))
            return True

        second_proxy.broadcast_confirmed = confirmed
        second.helper.ack_response = mock.AsyncMock()
        try:
            recovered = second.pending_responses["offline-capture"]
            self.assertTrue(recovered["response_captured"])
            await second_proxy._process_pending_response(
                second,
                "offline-capture",
                recovered,
                dict(recovered["ready_response"]),
            )
            self.assertEqual(delivered[0]["stage"], "QMT_SUBMITTED")
            self.assertEqual(delivered[0]["order_id"], "9101")
            self.assertNotIn("offline-capture", second.pending_responses)
            self.assertEqual(
                second.correlation.load_pending_responses(second.cfg.account_id),
                [],
            )
            second.helper.ack_response.assert_not_awaited()
        finally:
            second.db_io.close()
            second.correlation.close()
            second.file_io.close()

    async def _unverifiable_response_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        async def queued(payload, wait, enqueue_done=None):
            if enqueue_done is not None:
                enqueue_done()
            return {
                "status": "queued",
                "request_id": payload["request_id"],
            }

        runtime.helper.place_order = mock.AsyncMock(side_effect=queued)
        runtime.helper.ack_response = mock.AsyncMock()
        delivered = []

        async def confirmed(_runtime, outbound, _delivery_id, timeout=1.0):
            delivered.append(dict(outbound))
            return True

        proxy.broadcast_confirmed = confirmed
        variants = ("missing-identity", "wrong-fingerprint")
        try:
            for variant in variants:
                identity = "bad-response-" + variant
                reply = await proxy.dispatch(
                    runtime,
                    None,
                    self.order_message("NEW_ASYNC", identity),
                )
                self.assertEqual(reply["status"], "SENT")
                item = runtime.pending_responses[identity]
                response = {
                    "ok": True,
                    "data": {"status": "done", "order_id": "9201"},
                }
                if variant == "wrong-fingerprint":
                    response["request_id"] = identity
                    response["gateway_effect_fingerprint"] = "sha256:wrong"
                await proxy._process_pending_response(
                    runtime,
                    identity,
                    item,
                    response,
                )
                final = delivered[-1]
                self.assertEqual(final["stage"], "SUBMIT_UNKNOWN")
                self.assertEqual(
                    final["code"],
                    "HELPER_RESPONSE_IDENTITY_MISMATCH",
                )
                self.assertTrue(final["reconcile_required"])
                self.assertNotIn(identity, runtime.pending_responses)
                self.assertEqual(
                    runtime.correlation.get_effect_request(
                        runtime.cfg.account_id,
                        identity,
                    )["state"],
                    "UNKNOWN",
                )
                self.assertEqual(
                    runtime.correlation.get(runtime.cfg.account_id, identity)["stage"],
                    "SUBMIT_UNKNOWN",
                )
            self.assertEqual(runtime.helper.ack_response.await_count, len(variants))
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    def _command_filename_sequence_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        records = (
            (30, "sequence-third"),
            (10, "sequence-first"),
            (20, "sequence-second"),
        )
        try:
            for sequence, request_id in records:
                payload = {
                    "request_id": request_id,
                    "msg_id": request_id + "-msg",
                    "gateway_enqueue_seq": sequence,
                    "gateway_effect_fingerprint": "sha256:" + request_id,
                }
                result = runtime.helper.enqueue_action(
                    "place_order",
                    payload,
                )
                self.assertEqual(
                    Path(result["request_path"]).name,
                    gateway_module.command_request_filename(request_id, payload),
                )
                self.assertTrue(
                    Path(result["request_path"]).name.endswith(
                        gateway_module.request_file_key(request_id) + ".json"
                    )
                )
            ordered_files = gateway_module.bounded_json_files(
                runtime.helper.commands,
                10,
            )
            ordered_request_ids = [
                json.loads(path.read_text(encoding="utf-8"))["request_id"]
                for path in ordered_files
            ]
            self.assertEqual(
                ordered_request_ids,
                ["sequence-first", "sequence-second", "sequence-third"],
            )
            self.assertFalse(
                (runtime.helper.commands / "sequence-first.json").exists()
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    def _sequenced_command_idempotency_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.helper.ensure_dirs()
        request_id = "sequenced-idempotency"
        payload = {
            "request_id": request_id,
            "msg_id": request_id + "-msg",
            "client_order_id": request_id,
            "intent_hash": "sha256:intent",
            "gateway_enqueue_seq": 10,
            "gateway_effect_fingerprint": "sha256:effect",
        }
        try:
            first = runtime.helper.enqueue_action("place_order", dict(payload))
            retry_payload = dict(payload)
            retry_payload["gateway_enqueue_seq"] = 20
            second = runtime.helper.enqueue_action(
                "place_order",
                retry_payload,
            )
            self.assertFalse(first["idempotent"])
            self.assertTrue(second["idempotent"])
            self.assertEqual(second["dedupe_layer"], "helper_queue")
            files = list(runtime.helper.commands.glob("*.json"))
            self.assertEqual(len(files), 1)
            self.assertEqual(Path(second["request_path"]), files[0])

            conflicting_payload = dict(payload)
            conflicting_payload["gateway_enqueue_seq"] = 30
            conflicting_payload["gateway_effect_fingerprint"] = "sha256:conflict"
            conflicting_request = runtime.helper._build_request(
                "place_order",
                conflicting_payload,
                request_id,
            )
            conflicting_path = runtime.helper.commands / (
                gateway_module.command_request_filename(
                    request_id,
                    conflicting_payload,
                )
            )
            atomic_write_json(
                conflicting_path,
                conflicting_request,
                ensure_parent=False,
            )
            with self.assertRaises(gateway_module.HelperError) as captured:
                runtime.helper.enqueue_action(
                    "place_order",
                    {**payload, "gateway_enqueue_seq": 40},
                )
            self.assertEqual(captured.exception.code, "REQUEST_ID_CONFLICT")

            exact_id = "exact-hashed-legacy-conflict"
            exact_payload = {
                **payload,
                "request_id": exact_id,
                "msg_id": exact_id + "-msg",
                "client_order_id": exact_id,
                "gateway_enqueue_seq": 45,
                "gateway_effect_fingerprint": "sha256:exact-pair",
            }
            exact_request = runtime.helper._build_request(
                "place_order",
                exact_payload,
                exact_id,
            )
            altered_exact_request = dict(exact_request)
            altered_exact_request["deadline_at"] = (
                float(exact_request.get("deadline_at") or 0.0) + 1.0
            )
            atomic_write_json(
                runtime.helper.commands
                / (gateway_module.request_file_key(exact_id) + ".json"),
                exact_request,
                ensure_parent=False,
            )
            atomic_write_json(
                runtime.helper.commands
                / (gateway_module.safe_filename(exact_id) + ".json"),
                altered_exact_request,
                ensure_parent=False,
            )
            with self.assertRaises(gateway_module.HelperError) as exact_conflict:
                runtime.helper.enqueue_action("place_order", exact_payload)
            self.assertEqual(
                exact_conflict.exception.code,
                "REQUEST_ID_CONFLICT",
            )

            bounded_payload = {
                **payload,
                "request_id": "sequenced-scan-bounded",
                "msg_id": "sequenced-scan-bounded-msg",
                "client_order_id": "sequenced-scan-bounded",
                "gateway_enqueue_seq": 50,
                "gateway_effect_fingerprint": "sha256:bounded",
            }
            with mock.patch.object(
                gateway_module,
                "MAX_EXISTING_COMMAND_SCAN_ENTRIES_PER_DIRECTORY",
                1,
            ):
                with self.assertRaises(gateway_module.HelperError) as bounded:
                    runtime.helper.enqueue_action(
                        "place_order",
                        bounded_payload,
                    )
            self.assertEqual(
                bounded.exception.code,
                "HELPER_QUEUE_SCAN_INCOMPLETE",
            )
            self.assertFalse(
                (
                    runtime.helper.commands
                    / gateway_module.command_request_filename(
                        bounded_payload["request_id"],
                        bounded_payload,
                    )
                ).exists()
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    def _recovered_and_v1_command_sibling_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.helper.ensure_dirs()
        try:
            recovered_id = "recovered-only-idempotency"
            recovered_payload = {
                "request_id": recovered_id,
                "msg_id": recovered_id + "-msg",
                "client_order_id": recovered_id,
                "intent_hash": "sha256:recovered-intent",
                "gateway_enqueue_seq": 10,
                "gateway_effect_fingerprint": "sha256:recovered-effect",
            }
            recovered_request = runtime.helper._build_request(
                "place_order",
                recovered_payload,
                recovered_id,
            )
            # A short legacy base keeps the multi-recovered test portable on
            # Windows hosts that have not enabled long-path support.
            regular_stem = gateway_module.safe_filename(recovered_id)
            recovered_name = (
                regular_stem
                + "-recovered-1720000000000-deadbeef"
                + "-recovered-1720000000001-cafebabe.json"
            )
            recovered_path = runtime.helper.commands / recovered_name
            atomic_write_json(
                recovered_path,
                recovered_request,
                ensure_parent=False,
            )
            recovered_retry = dict(recovered_payload)
            recovered_retry["gateway_enqueue_seq"] = 20
            deduped = runtime.helper.enqueue_action(
                "place_order",
                recovered_retry,
            )
            self.assertTrue(deduped["idempotent"])
            self.assertEqual(Path(deduped["request_path"]), recovered_path)
            self.assertFalse(
                (
                    runtime.helper.commands
                    / gateway_module.command_request_filename(
                        recovered_id,
                        recovered_retry,
                    )
                ).exists()
            )

            conflicting_payload = dict(recovered_payload)
            conflicting_payload["gateway_effect_fingerprint"] = (
                "sha256:recovered-conflict"
            )
            conflicting_request = runtime.helper._build_request(
                "place_order",
                conflicting_payload,
                recovered_id,
            )
            conflicting_path = runtime.helper.commands / (
                regular_stem
                + "-recovered-1720000000002-feedface.json"
            )
            atomic_write_json(
                conflicting_path,
                conflicting_request,
                ensure_parent=False,
            )
            with self.assertRaises(gateway_module.HelperError) as conflict:
                runtime.helper.enqueue_action(
                    "place_order",
                    {**recovered_payload, "gateway_enqueue_seq": 30},
                )
            self.assertEqual(conflict.exception.code, "REQUEST_ID_CONFLICT")

            v1_id = "v1/legacy:idempotency"
            v1_payload = {
                "request_id": v1_id,
                "msg_id": "v1-legacy-msg",
                "client_order_id": "v1-legacy-client-order",
                "intent_hash": "sha256:v1-intent",
                "gateway_enqueue_seq": 40,
                "gateway_effect_fingerprint": "sha256:v1-effect",
            }
            v1_request = runtime.helper._build_request(
                "place_order",
                v1_payload,
                v1_id,
            )
            v1_legacy_path = runtime.helper.inbox / (
                gateway_module.safe_filename(v1_id) + ".json"
            )
            atomic_write_json(v1_legacy_path, v1_request, ensure_parent=False)
            v1_result = runtime.helper.enqueue_action(
                "place_order",
                v1_payload,
            )
            v1_hashed_path = runtime.helper.inbox / (
                gateway_module.request_file_key(v1_id) + ".json"
            )
            self.assertTrue(v1_result["idempotent"])
            self.assertEqual(v1_result["duplicate_stage"], "inbox_v1")
            self.assertEqual(Path(v1_result["request_path"]), v1_hashed_path)
            self.assertTrue(v1_hashed_path.is_file())
            self.assertFalse(v1_legacy_path.exists())
            self.assertFalse(
                (
                    runtime.helper.commands
                    / gateway_module.command_request_filename(v1_id, v1_payload)
                ).exists()
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    def _v1_command_depth_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        runtime.helper.ensure_dirs()
        try:
            atomic_write_json(
                runtime.helper.commands / "v2-command.json",
                {"request_id": "v2-command"},
                ensure_parent=False,
            )
            atomic_write_json(
                runtime.helper.inbox / "v1-command-one.json",
                {"request_id": "v1-command-one"},
                ensure_parent=False,
            )
            atomic_write_json(
                runtime.helper.inbox / "v1-command-two.json",
                {"request_id": "v1-command-two"},
                ensure_parent=False,
            )
            self.assertEqual(runtime.helper.command_queue_depth_sync(10), 3)
            self.assertEqual(runtime.helper.command_queue_depth_sync(2), 2)
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _sync_cancel_timeout_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})

        async def queued_timeout(payload, wait, enqueue_done=None):
            self.assertTrue(wait)
            if enqueue_done is not None:
                enqueue_done()
            return {
                "status": "queued",
                "queued": True,
                "timeout": True,
                "request_id": payload["request_id"],
            }

        runtime.helper.cancel_order = mock.AsyncMock(side_effect=queued_timeout)
        message = {
            "type": "CANCEL",
            "msg_id": "sync-cancel-timeout-msg",
            "request_id": "sync-cancel-timeout",
            "account_id": runtime.cfg.account_id,
            "account_name": runtime.cfg.name,
            "order_id": "9301",
        }
        try:
            reply = await proxy.dispatch(runtime, None, message)
            self.assertEqual(reply["status"], "UNKNOWN")
            self.assertEqual(reply["stage"], "SUBMIT_UNKNOWN")
            self.assertEqual(reply["submit_result"], "UNKNOWN")
            self.assertEqual(reply["code"], "HELPER_RESPONSE_TIMEOUT")
            self.assertEqual(reply["cancel_status"], "unknown")
            self.assertIn("reconcile", reply["reject_reason"])
            self.assertEqual(
                runtime.correlation.get_effect_request(
                    runtime.cfg.account_id,
                    "sync-cancel-timeout",
                )["state"],
                "UNKNOWN",
            )
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _session_bound_delivery_ack_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        target_session = object()
        other_session = object()
        waiter = asyncio.Event()
        runtime.primary = target_session
        runtime.delivery_waiters["delivery-session-bound"] = (
            target_session,
            waiter,
        )
        try:
            await proxy.dispatch(runtime, other_session, {
                "type": "DELIVERY_ACK",
                "delivery_id": "delivery-session-bound",
            })
            self.assertFalse(waiter.is_set())
            await proxy.dispatch(runtime, target_session, {
                "type": "DELIVERY_ACK",
                "delivery_id": "delivery-session-bound",
            })
            self.assertTrue(waiter.is_set())

            stale_waiter = asyncio.Event()
            runtime.delivery_waiters["stale-session"] = (
                target_session,
                stale_waiter,
            )
            runtime.primary = other_session
            await proxy.dispatch(runtime, target_session, {
                "type": "DELIVERY_ACK",
                "delivery_id": "stale-session",
            })
            self.assertFalse(stale_waiter.is_set())
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _rejected_send_failure_enqueue_contract(self):
        class FakeWriter:
            def __init__(self):
                self.closed = False

            def get_extra_info(self, name):
                return ("127.0.0.1", 12345) if name == "peername" else None

            def close(self):
                self.closed = True

            async def wait_closed(self):
                return None

        for reject_path in ("credential", "capacity"):
            case_dir = self.temp_dir / reject_path
            case_dir.mkdir()
            original_temp = self.temp_dir
            self.temp_dir = case_dir
            config = load_config(self.make_config())
            self.temp_dir = original_temp
            logger = setup_logging(None)
            logger.setLevel(logging.CRITICAL)
            proxy = BigQmtGatewayProxy(config, logger)
            runtime = proxy.runtimes[0]
            ping = {
                "type": "PING",
                "msg_id": "handshake-" + reject_path,
                "auth_token": EXAMPLE_AUTH_TOKEN,
                "protocol_version": 2,
                "account_id": runtime.cfg.account_id,
                "account_name": runtime.cfg.name,
            }
            effect = self.order_message("NEW", "poison-" + reject_path)
            if reject_path == "credential":
                effect["msg_id"] = EXAMPLE_AUTH_TOKEN
            proxy.running = True
            send_results = [None, OSError("injected writer failure")]
            capacity = reject_path != "capacity"
            try:
                with mock.patch.object(
                    gateway_module,
                    "read_frame",
                    new=mock.AsyncMock(side_effect=[ping, effect]),
                ), mock.patch.object(
                    gateway_module.TcpClientSession,
                    "send",
                    new=mock.AsyncMock(side_effect=send_results),
                ), mock.patch.object(
                    gateway_module.TcpClientSession,
                    "dispatch_capacity_available",
                    return_value=capacity,
                ):
                    await proxy.handle_client(
                        runtime,
                        asyncio.StreamReader(),
                        FakeWriter(),
                    )
                self.assertIsNotNone(runtime.effect_enqueue_tail)
                self.assertTrue(runtime.effect_enqueue_tail.done())
                successor = {"type": "NEW"}
                runtime.attach_effect_enqueue_turn(successor)
                predecessor = successor["_gateway_enqueue_predecessor"]
                self.assertTrue(predecessor.done())
                runtime.finish_effect_enqueue_turn(successor)
            finally:
                proxy.running = False
                runtime.db_io.close()
                runtime.correlation.close()
                runtime.file_io.close()

    async def _effect_enqueue_order_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy._ensure_helper_ready = mock.AsyncMock(return_value={"ready": True})
        first = self.order_message("NEW", "receive-order-first")
        second = self.order_message("NEW", "receive-order-second")
        for message in (first, second):
            message["_gateway_enqueue_seq"] = runtime.next_gateway_enqueue_seq()
            runtime.attach_effect_enqueue_turn(message)
        first_started = asyncio.Event()
        release_first_enqueue = asyncio.Event()
        published = []

        async def ordered_place(payload, wait, enqueue_done=None):
            request_id = payload["request_id"]
            published.append(request_id)
            if request_id == "receive-order-first":
                first_started.set()
                await release_first_enqueue.wait()
            if enqueue_done is not None:
                enqueue_done()
            return {
                "status": "done",
                "request_id": request_id,
                "order_id": "qmt-" + request_id,
            }

        runtime.helper.place_order = mock.AsyncMock(side_effect=ordered_place)
        first_task = asyncio.create_task(proxy.dispatch(runtime, None, first))
        second_task = asyncio.create_task(proxy.dispatch(runtime, None, second))
        try:
            await asyncio.wait_for(first_started.wait(), timeout=1.0)
            await asyncio.sleep(0)
            self.assertEqual(published, ["receive-order-first"])
            release_first_enqueue.set()
            first_reply, second_reply = await asyncio.wait_for(
                asyncio.gather(first_task, second_task),
                timeout=2.0,
            )
            self.assertEqual(published, [
                "receive-order-first",
                "receive-order-second",
            ])
            self.assertEqual(first_reply["status"], "ACCEPTED")
            self.assertEqual(second_reply["status"], "ACCEPTED")
        finally:
            release_first_enqueue.set()
            for task in (first_task, second_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(first_task, second_task, return_exceptions=True)
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _live_event_failure_order_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        events = [
            {"event_id": "ordered-event-%d" % index, "event_seq": index}
            for index in (1, 2, 3)
        ]
        delivered = []

        async def emit(_runtime, event, **_kwargs):
            delivered.append(event["event_id"])
            if event["event_id"] == "ordered-event-2":
                raise RuntimeError("injected handler failure")

        proxy.emit_event = mock.AsyncMock(side_effect=emit)
        runtime.helper.retry_event = mock.AsyncMock(return_value=False)
        runtime.helper.ack_event = mock.AsyncMock()
        try:
            await proxy._deliver_live_event_batch(runtime, list(reversed(events)))
            self.assertEqual(
                delivered,
                ["ordered-event-1", "ordered-event-2"],
            )
            runtime.helper.retry_event.assert_awaited_once_with(events[1])
            runtime.helper.ack_event.assert_awaited_once_with(events[0])
            self.assertTrue(runtime.correlation.event_seen(
                runtime.cfg.account_id, "ordered-event-1"
            ))
            self.assertFalse(runtime.correlation.event_seen(
                runtime.cfg.account_id, "ordered-event-2"
            ))
            self.assertFalse(runtime.correlation.event_seen(
                runtime.cfg.account_id, "ordered-event-3"
            ))
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

    async def _reliable_event_identity_contract(self):
        config = load_config(self.make_config())
        logger = setup_logging(None)
        logger.setLevel(logging.CRITICAL)
        proxy = BigQmtGatewayProxy(config, logger)
        runtime = proxy.runtimes[0]
        proxy.broadcast_confirmed = mock.AsyncMock(
            side_effect=AssertionError("unidentified event must not be sent")
        )
        try:
            with self.assertRaisesRegex(ValueError, "event_id is required"):
                await proxy.emit_event(runtime, {
                    "type": "ORDER_UPDATE",
                    "data": {"order_id": "unidentified"},
                })
            proxy.broadcast_confirmed.assert_not_awaited()
        finally:
            runtime.db_io.close()
            runtime.correlation.close()
            runtime.file_io.close()

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
