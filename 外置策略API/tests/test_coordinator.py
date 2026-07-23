"""Offline and loopback contract tests for the multi-strategy Coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest


API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api import (
    AccountCoordinator,
    ConnectionConfig,
    CoordinatorConflict,
    CoordinatorLocalServer,
    CoordinatorRiskRejected,
    EXPECTED_GATEWAY_BUILD_ID,
    LocalQmtApi,
    RiskLimits,
)

from tests.fake_gateway import FakeGateway, TEST_AUTH_TOKEN, recv_message, send_message


def connection(port: int = 9550) -> ConnectionConfig:
    return ConnectionConfig(
        account_name="account_main",
        account_id="TEST_ACCOUNT",
        account_type="STOCK",
        auth_token=TEST_AUTH_TOKEN,
        host="127.0.0.1",
        local_host="127.0.0.1",
        port=port,
        expected_gateway_build_id=EXPECTED_GATEWAY_BUILD_ID,
        connect_timeout=1.0,
        recv_timeout=0.02,
        handshake_timeout=1.0,
        heartbeat_interval=5.0,
        heartbeat_timeout=15.0,
        auto_reconnect=False,
    )


@dataclass
class _StubConfig:
    account_id: str = "TEST_ACCOUNT"
    account_name: str = "account_main"


class RecordingApi:
    """No-network API double for persistence, recovery, and risk tests."""

    def __init__(self) -> None:
        self.config = _StubConfig()
        self.handlers = {}
        self.connected = False
        self.orders = []
        self.cancels = []

    @property
    def is_connected(self):
        return self.connected

    def on(self, message_type, handler):
        self.handlers.setdefault(message_type, []).append(handler)

    def connect(self, timeout=None):
        self.connected = True
        return True

    def stop(self, timeout=5.0):
        self.connected = False

    def query(self, query_type="", params=None, timeout=None):
        self.last_query = (query_type, params, timeout)
        return {
            "type": "QUERY_RESPONSE",
            "success": True,
            "query_type": query_type,
            "account_status": {"ready": True, "state": "ready"},
        }

    def place_order_async(self, *args, **kwargs):
        self.orders.append((args, dict(kwargs)))
        return "gateway-order-%d" % len(self.orders)

    def cancel_order_async(self, *args, **kwargs):
        self.cancels.append((args, dict(kwargs)))
        return "gateway-cancel-%d" % len(self.cancels)

    def deliver(self, message):
        for handler in self.handlers.get(message["type"], []):
            handler(dict(message))


class CoordinatorTests(unittest.TestCase):
    def _register_two(self, coordinator):
        coordinator.register_strategy("alpha", "alpha-local-token-0001")
        coordinator.register_strategy("beta", "beta-local-token-0002")

    def test_two_strategy_loopback_orders_receive_only_owned_reliable_events(self):
        orders = []
        acknowledgements = []

        def handler(conn, state):
            query = recv_message(conn)
            self.assertEqual(query["type"], "QUERY")
            self.assertEqual(query["query_type"], "ACCOUNT_STATUS")
            send_message(conn, {
                "type": "QUERY_RESPONSE",
                "msg_id": query["msg_id"],
                "success": True,
                "query_type": "ACCOUNT_STATUS",
                "account_status": {"ready": True, "state": "ready"},
            })
            while len(orders) < 2 or len(acknowledgements) < 4:
                message = recv_message(conn, timeout=2.0)
                if message.get("type") == "NEW_ASYNC":
                    orders.append(message)
                    ordinal = len(orders)
                    send_message(conn, {
                        "type": "ASYNC_ORDER",
                        "msg_id": message["msg_id"],
                        "request_id": message["request_id"],
                        "client_order_id": message["client_order_id"],
                        "status": "SENT",
                        "stage": "BRIDGE_QUEUED",
                    })
                    send_message(conn, {
                        "type": "ASYNC_ORDER_RESPONSE",
                        "delivery_id": "response:%s" % message["request_id"],
                        "request_id": message["request_id"],
                        "client_order_id": message["client_order_id"],
                        "order_id": "QMT-%d" % ordinal,
                        "stage": "QMT_SUBMITTED",
                    })
                    send_message(conn, {
                        "type": "ORDER_UPDATE",
                        "delivery_id": "order:%d" % ordinal,
                        "client_order_id": message["client_order_id"],
                        "order_id": "QMT-%d" % ordinal,
                        "stage": "FILLED",
                    })
                elif message.get("type") == "DELIVERY_ACK":
                    acknowledgements.append(message)
                else:
                    self.fail("unexpected client frame %r" % message)

        gateway = FakeGateway(handler)
        api = LocalQmtApi(connection(gateway.port))
        with tempfile.TemporaryDirectory(prefix="coordinator-loopback-") as temporary:
            coordinator = AccountCoordinator(api, Path(temporary) / "coordinator.sqlite3")
            self._register_two(coordinator)
            try:
                self.assertTrue(coordinator.start(timeout=1.0))
                alpha = coordinator.submit_order(
                    "alpha", "alpha-001", "600000.SH", "BUY", 100, 10.0
                )
                beta = coordinator.submit_order(
                    "beta", "beta-001", "600001.SH", "SELL", 200, 8.0
                )
                gateway.assert_clean(timeout=4.0)
                self.assertNotEqual(alpha["client_order_id"], beta["client_order_id"])
                self.assertIn("-alpha-", alpha["client_order_id"])
                self.assertIn("-beta-", beta["client_order_id"])
                self.assertEqual(len(orders), 2)
                self.assertEqual(len(acknowledgements), 4)
                self.assertEqual(
                    {item["delivery_id"] for item in acknowledgements},
                    {
                        "response:%s" % orders[0]["request_id"],
                        "response:%s" % orders[1]["request_id"],
                        "order:1",
                        "order:2",
                    },
                )
                alpha_events = coordinator.poll_events("alpha")
                beta_events = coordinator.poll_events("beta")
                # Each strategy receives its immediate bridge result plus the
                # two reliable Gateway events (submit result and final update).
                self.assertEqual(len(alpha_events), 3)
                self.assertEqual(len(beta_events), 3)
                self.assertTrue(all(event["strategy_id"] == "alpha" for event in alpha_events))
                self.assertTrue(all(event["strategy_id"] == "beta" for event in beta_events))
                self.assertEqual(
                    {event["source_event"]["client_order_id"] for event in alpha_events},
                    {alpha["client_order_id"]},
                )
                self.assertEqual(
                    {event["source_event"]["client_order_id"] for event in beta_events},
                    {beta["client_order_id"]},
                )
                for event in alpha_events + beta_events:
                    self.assertTrue(
                        coordinator.acknowledge_event(
                            event["strategy_id"], event["coordinator_event_id"]
                        )
                    )
                self.assertEqual(coordinator.poll_events("alpha"), [])
                self.assertEqual(coordinator.poll_events("beta"), [])
                self.assertEqual(coordinator.account_status()["pending_notional"], 0.0)
            finally:
                coordinator.stop()
                gateway.close()

    def test_idempotency_risk_unknown_and_outbox_recovery_are_fail_closed(self):
        with tempfile.TemporaryDirectory(prefix="coordinator-recovery-") as temporary:
            database = Path(temporary) / "coordinator.sqlite3"
            api = RecordingApi()
            coordinator = AccountCoordinator(
                api,
                database,
                account_limits=RiskLimits(max_pending_notional=1000.0),
            )
            coordinator.register_strategy(
                "alpha",
                "alpha-local-token-0001",
                limits=RiskLimits(max_order_notional=600.0, max_pending_notional=600.0),
            )
            coordinator.register_strategy("beta", "beta-local-token-0002")
            self.assertTrue(coordinator.start(timeout=0.1))
            first = coordinator.submit_order(
                "alpha", "alpha-001", "600000.SH", "BUY", 50, 10.0
            )
            duplicate = coordinator.submit_order(
                "alpha", "alpha-001", "600000.SH", "BUY", 50, 10.0
            )
            self.assertEqual(first["command_id"], duplicate["command_id"])
            self.assertEqual(len(api.orders), 1)
            with self.assertRaises(CoordinatorConflict):
                coordinator.submit_order(
                    "alpha", "alpha-001", "600000.SH", "BUY", 51, 10.0
                )
            with self.assertRaises(CoordinatorRiskRejected):
                coordinator.submit_order(
                    "beta", "beta-001", "600001.SH", "BUY", 60, 10.0
                )

            reliable = {
                "type": "ASYNC_ORDER_RESPONSE",
                "delivery_id": "response:%s" % first["request_id"],
                "request_id": first["request_id"],
                "client_order_id": first["client_order_id"],
                "order_id": "QMT-ALPHA-1",
                "stage": "QMT_SUBMITTED",
            }
            api.deliver(reliable)
            api.deliver(reliable)  # Gateway at-least-once duplicate.
            events = coordinator.poll_events("alpha")
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["attempt"], 1)
            self.assertEqual(coordinator.poll_events("alpha")[0]["attempt"], 2)
            coordinator.stop()

            restarted_api = RecordingApi()
            restarted = AccountCoordinator(
                restarted_api,
                database,
                account_limits=RiskLimits(max_pending_notional=1000.0),
            )
            try:
                self.assertTrue(restarted.start(timeout=0.1))
                recovered = restarted.poll_events("alpha")
                self.assertEqual(len(recovered), 1)
                self.assertGreaterEqual(recovered[0]["attempt"], 3)
                self.assertTrue(
                    restarted.acknowledge_event(
                        "alpha", recovered[0]["coordinator_event_id"]
                    )
                )
                self.assertEqual(restarted.poll_events("alpha"), [])

                restarted_api.deliver({
                    "type": "RECONCILE_REQUIRED",
                    "delivery_id": "reconcile:alpha-1",
                    "request_id": first["request_id"],
                    "client_order_id": first["client_order_id"],
                })
                self.assertTrue(restarted.trading_halted)
                with self.assertRaisesRegex(Exception, "not ready"):
                    restarted.submit_order(
                        "beta", "beta-002", "600001.SH", "BUY", 1, 1.0
                    )
            finally:
                restarted.stop()

    def test_authenticated_loopback_server_keeps_strategy_credentials_and_events_isolated(self):
        with tempfile.TemporaryDirectory(prefix="coordinator-server-") as temporary:
            api = RecordingApi()
            coordinator = AccountCoordinator(api, Path(temporary) / "coordinator.sqlite3")
            coordinator.register_strategy("alpha", "alpha-local-token-0001")
            coordinator.register_strategy("beta", "beta-local-token-0002")
            self.assertTrue(coordinator.start(timeout=0.1))
            server = CoordinatorLocalServer(coordinator)
            server.start()
            try:
                with socket.create_connection(("127.0.0.1", server.port), timeout=1.0) as conn:
                    send_message(conn, {
                        "type": "COORDINATOR_HELLO",
                        "strategy_id": "alpha",
                        "auth_token": "alpha-local-token-0001",
                    })
                    self.assertEqual(recv_message(conn)["type"], "COORDINATOR_PONG")
                    send_message(conn, {
                        "type": "ORDER_INTENT",
                        "msg_id": "strategy-order-1",
                        "strategy_order_id": "alpha-001",
                        "symbol": "600000.SH",
                        "side": "BUY",
                        "quantity": 100,
                        "price": 10.0,
                    })
                    result = recv_message(conn)
                    self.assertEqual(result["type"], "ORDER_RESULT")
                    self.assertTrue(result["success"])
                    self.assertEqual(result["command"]["strategy_id"], "alpha")
                    self.assertEqual(len(api.orders), 1)
                    api.deliver({
                        "type": "ASYNC_ORDER_RESPONSE",
                        "delivery_id": "server-response-1",
                        "request_id": result["command"]["request_id"],
                        "client_order_id": result["command"]["client_order_id"],
                        "order_id": "QMT-SERVER-1",
                        "stage": "QMT_SUBMITTED",
                    })
                    send_message(conn, {"type": "POLL_EVENTS", "msg_id": "poll-1"})
                    events = recv_message(conn)
                    self.assertEqual(events["type"], "EVENT_BATCH")
                    self.assertEqual(len(events["events"]), 1)
                    self.assertEqual(events["events"][0]["strategy_id"], "alpha")
                    send_message(conn, {
                        "type": "ACK_EVENT",
                        "msg_id": "ack-1",
                        "coordinator_event_id": events["events"][0]["coordinator_event_id"],
                    })
                    self.assertTrue(recv_message(conn)["acknowledged"])
                with socket.create_connection(("127.0.0.1", server.port), timeout=1.0) as rejected:
                    send_message(rejected, {
                        "type": "COORDINATOR_HELLO",
                        "strategy_id": "beta",
                        "auth_token": "wrong-token-value",
                    })
                    self.assertEqual(
                        recv_message(rejected)["code"], "COORDINATOR_HANDSHAKE_REJECTED"
                    )
            finally:
                server.stop()
                coordinator.stop()


if __name__ == "__main__":
    unittest.main()
