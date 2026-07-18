import socket
from pathlib import Path
import sys
import threading
import time
import unittest
from dataclasses import replace

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api import (
    BridgeClient,
    ConnectionConfig,
    EXPECTED_GATEWAY_BUILD_ID,
    FrameDecoder,
    TradeTransport,
    TransportDisconnected,
)

from tests.fake_gateway import FakeGateway, TEST_AUTH_TOKEN, recv_message, send_message


def connection(port, *, auto_reconnect=False):
    return ConnectionConfig(
        account_name="account_main",
        account_id="TEST_ACCOUNT",
        account_type="STOCK",
        auth_token=TEST_AUTH_TOKEN,
        host="127.0.0.1",
        local_host="127.0.0.1",
        port=port,
        recv_timeout=0.05,
        heartbeat_interval=10.0,
        heartbeat_timeout=30.0,
        auto_reconnect=auto_reconnect,
    )


class RecordingTransport(TradeTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.receive_callers = []

    def receive(self, timeout=None):
        self.receive_callers.append(threading.get_ident())
        return super().receive(timeout=timeout)


class ReconnectIdentityTransport:
    def __init__(self):
        self.connected = False
        self.connect_profiles = []
        self.receive_callers = []
        self.reader_ident = None
        self.last_handshake_response = {}

    @property
    def is_connected(self):
        return self.connected

    @property
    def reader_thread_id(self):
        return self.reader_ident

    def connect(self, profile="normal"):
        self.connect_profiles.append(profile)
        self.connected = True
        build_id = (
            EXPECTED_GATEWAY_BUILD_ID
            if len(self.connect_profiles) == 1
            else "untrusted-reconnect-build"
        )
        self.last_handshake_response = {
            "type": "PONG",
            "protocol_version": 2,
            "build_id": build_id,
            "account_id": "TEST_ACCOUNT",
            "account_name": "account_main",
        }
        return True

    def close(self):
        self.connected = False

    def claim_reader(self):
        self.reader_ident = threading.get_ident()

    def release_reader(self):
        self.reader_ident = None

    def receive(self, timeout=None):
        self.receive_callers.append(threading.get_ident())
        self.connected = False
        raise TransportDisconnected("force reconnect")

    def maintain_heartbeat(self):
        return self.connected


class ConcurrentDetectSocket:
    def __init__(self):
        self.active = 0
        self.max_active = 0
        self.frames = []
        self.lock = threading.Lock()

    def sendall(self, frame):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.002)
        self.frames.append(frame)
        with self.lock:
            self.active -= 1


class ClientContractTests(unittest.TestCase):
    def test_config_requires_all_local_pong_identity_values(self):
        valid = connection(9550)
        for field in ("account_id", "account_name", "expected_gateway_build_id", "auth_token"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, field):
                    BridgeClient(replace(valid, **{field: ""}))
        with self.assertRaisesRegex(ValueError, "loopback"):
            BridgeClient(replace(valid, host="0.0.0.0"))
        for invalid in ("a" * 63, "g" * 64, "a" * 65):
            with self.subTest(auth_token=invalid[:8]):
                with self.assertRaisesRegex(ValueError, "auth_token"):
                    BridgeClient(replace(valid, auth_token=invalid))
        self.assertNotIn(TEST_AUTH_TOKEN, repr(valid))

    def test_first_frame_is_identity_ping_and_socket_is_low_latency(self):
        gateway = FakeGateway(lambda conn, state: time.sleep(0.1))
        client = BridgeClient(connection(gateway.port))
        try:
            self.assertTrue(client.start())
            first = gateway.first_frames[0]
            self.assertEqual(first["type"], "PING")
            self.assertEqual(first["protocol_version"], 2)
            self.assertEqual(first["account_id"], "TEST_ACCOUNT")
            self.assertEqual(first["account_name"], "account_main")
            self.assertEqual(first["auth_token"], TEST_AUTH_TOKEN)
            self.assertNotIn("auth_token", client.transport.last_handshake_response)
            sock = client.transport._socket
            self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 1)
            self.assertEqual(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 1)
            gateway.assert_clean()
        finally:
            client.stop()
            gateway.close()

    def test_heartbeat_ping_never_repeats_auth_token(self):
        heartbeats = []

        def handler(conn, state):
            heartbeats.append(recv_message(conn, timeout=1.0))

        gateway = FakeGateway(handler)
        config = replace(
            connection(gateway.port),
            recv_timeout=0.01,
            heartbeat_interval=0.02,
            heartbeat_timeout=0.5,
        )
        client = BridgeClient(config)
        try:
            self.assertTrue(client.start())
            gateway.assert_clean()
            self.assertEqual(len(heartbeats), 1)
            self.assertEqual(heartbeats[0]["type"], "PING")
            self.assertNotIn("auth_token", heartbeats[0])
        finally:
            client.stop()
            gateway.close()

    def test_pong_protocol_build_id_and_name_are_all_fail_closed(self):
        cases = (
            ("protocol_version", {"protocol_version": 1}),
            ("build_id", {"build_id": "outdated-build"}),
            ("account_id", {"account_id": "WRONG_ACCOUNT"}),
            ("account_name", {"account_name": "wrong_name"}),
            ("handshake_rejected", {"handshake_error_code": "HANDSHAKE_REJECTED"}),
        )
        for expected, kwargs in cases:
            with self.subTest(field=expected):
                gateway = FakeGateway(lambda conn, state: time.sleep(0.03), **kwargs)
                client = BridgeClient(connection(gateway.port))
                try:
                    with self.assertLogs("qmt_local_api.client", level="ERROR") as captured:
                        self.assertFalse(client.start())
                    self.assertTrue(client.identity_guard_failed)
                    self.assertEqual(client.identity_guard_reason, expected)
                    self.assertIsNone(client.poll_thread_id)
                    rendered = "\n".join(captured.output)
                    self.assertNotIn("TEST_ACCOUNT", rendered)
                    self.assertNotIn("WRONG_ACCOUNT", rendered)
                    gateway.assert_clean()
                finally:
                    client.stop()
                    gateway.close()

    def test_reconnect_revalidates_identity_before_more_business_reads(self):
        transport = ReconnectIdentityTransport()
        client = BridgeClient(connection(9550, auto_reconnect=True), transport=transport)
        try:
            with self.assertLogs("qmt_local_api.client", level="ERROR"):
                self.assertTrue(client.start())
                deadline = time.monotonic() + 2.0
                while not client.identity_guard_failed and time.monotonic() < deadline:
                    time.sleep(0.01)
            self.assertEqual(client.identity_guard_reason, "build_id")
            self.assertEqual(transport.connect_profiles, ["startup", "reconnect"])
            self.assertEqual(len(transport.receive_callers), 1)
        finally:
            client.stop()

    def test_send_lock_serializes_every_writer(self):
        transport = TradeTransport(local_host="", auth_token=TEST_AUTH_TOKEN)
        capture = ConcurrentDetectSocket()
        with transport._state_lock:
            transport._socket = capture
            transport._connected = True
        threads = [
            threading.Thread(
                target=lambda n=n: transport.send_message({"type": "X", "msg_id": str(n)})
            )
            for n in range(24)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2.0)
        self.assertEqual(capture.max_active, 1)
        self.assertEqual(len(capture.frames), 24)

    def test_query_waiter_never_reads_socket(self):
        def handler(conn, state):
            request = recv_message(conn)
            self.assertEqual(request["type"], "QUERY")
            send_message(conn, {
                "type": "QUERY_RESPONSE",
                "msg_id": request["msg_id"],
                "success": True,
                "positions": [],
            })
            time.sleep(0.1)

        gateway = FakeGateway(handler)
        config = connection(gateway.port)
        transport = RecordingTransport(
            host=config.host,
            port=config.port,
            local_host=config.local_host,
            account_id=config.account_id,
            account_name=config.account_name,
            auth_token=config.auth_token,
            connect_timeout=config.connect_timeout,
            recv_timeout=config.recv_timeout,
            handshake_timeout=config.handshake_timeout,
            heartbeat_interval=config.heartbeat_interval,
            heartbeat_timeout=config.heartbeat_timeout,
        )
        client = BridgeClient(config, transport=transport)
        caller = threading.get_ident()
        try:
            self.assertTrue(client.start())
            response = client.query("POSITION", {"stock_code": "600000.SH"}, timeout=1.0)
            self.assertTrue(response["success"])
            self.assertTrue(transport.receive_callers)
            self.assertEqual(set(transport.receive_callers), {client.poll_thread_id})
            self.assertNotEqual(caller, client.poll_thread_id)
            with self.assertRaisesRegex(RuntimeError, "claimed poll thread"):
                transport.receive(timeout=0.01)
            gateway.assert_clean()
        finally:
            client.stop()
            gateway.close()

    def test_delivery_ack_waits_for_every_handler(self):
        second_entered = threading.Event()
        release_second = threading.Event()
        ack_received = threading.Event()
        early_ack = []

        def gateway_handler(conn, state):
            send_message(conn, {
                "type": "ORDER_UPDATE",
                "delivery_id": "event:order-1",
                "order_id": "ORDER-1",
            })
            self.assertTrue(second_entered.wait(1.0))
            try:
                early_ack.append(recv_message(conn, timeout=0.15))
            except socket.timeout:
                pass
            release_second.set()
            ack = recv_message(conn, timeout=1.0)
            self.assertEqual(ack["type"], "DELIVERY_ACK")
            self.assertEqual(ack["delivery_id"], "event:order-1")
            ack_received.set()

        def second_handler(message):
            second_entered.set()
            if not release_second.wait(1.0):
                raise TimeoutError("test gate did not open")

        gateway = FakeGateway(gateway_handler)
        client = BridgeClient(connection(gateway.port))
        client.on("ORDER_UPDATE", lambda message: None)
        client.on("ORDER_UPDATE", second_handler)
        try:
            self.assertTrue(client.start())
            gateway.assert_clean()
            self.assertTrue(ack_received.is_set())
            self.assertEqual(early_ack, [])
            self.assertTrue(client.wait_delivery_acknowledged("event:order-1", timeout=0.2))
            self.assertNotEqual(client.dispatcher.worker_thread_id, client.poll_thread_id)
        finally:
            client.stop()
            gateway.close()

    def test_handler_failure_sends_no_ack_and_disconnects(self):
        observed = []

        def gateway_handler(conn, state):
            send_message(conn, {
                "type": "TRADE_NOTIFY",
                "delivery_id": "event:trade-fail",
                "trade_id": "T-1",
            })
            try:
                observed.append(recv_message(conn, timeout=0.6))
            except (EOFError, ConnectionResetError, ConnectionAbortedError, socket.timeout, OSError):
                pass

        def fail(message):
            raise RuntimeError("persistence failed")

        gateway = FakeGateway(gateway_handler)
        client = BridgeClient(connection(gateway.port))
        client.on("TRADE_NOTIFY", fail)
        try:
            with self.assertLogs("qmt_local_api", level="ERROR"):
                self.assertTrue(client.start())
                gateway.assert_clean()
            self.assertFalse(any(item.get("type") == "DELIVERY_ACK" for item in observed))
            self.assertFalse(client.is_connected)
        finally:
            client.stop()
            gateway.close()

    def test_order_and_async_cancel_public_messages_preserve_identity(self):
        transport = TradeTransport(local_host="", auth_token=TEST_AUTH_TOKEN)
        capture = ConcurrentDetectSocket()
        with transport._state_lock:
            transport._socket = capture
            transport._connected = True
        client = BridgeClient(connection(9550), transport=transport)
        client._connected.set()
        order_id = client.send_order_async(
            "600000.SH",
            "BUY",
            100,
            10.23,
            client_order_id="strategy-order-1",
        )
        cancel_id = client.send_cancel_async("QMT-ORDER-1")
        messages = [FrameDecoder().feed(frame)[0] for frame in capture.frames]
        self.assertEqual(messages[0]["type"], "NEW_ASYNC")
        self.assertEqual(messages[0]["protocol_version"], 2)
        self.assertEqual(messages[0]["account_name"], "account_main")
        self.assertEqual(messages[0]["msg_id"], order_id)
        self.assertEqual(messages[1]["type"], "CANCEL_ASYNC")
        self.assertEqual(messages[1]["account_id"], "TEST_ACCOUNT")
        self.assertEqual(messages[1]["msg_id"], cancel_id)

    def test_stop_interrupts_reconnect_and_leaks_no_api_threads(self):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        client = BridgeClient(connection(closed_port, auto_reconnect=True))
        with self.assertLogs("qmt_local_api.transport", level="WARNING"):
            self.assertFalse(client.start())
        started = time.monotonic()
        client.stop(timeout=1.0)
        self.assertLess(time.monotonic() - started, 1.0)
        leaked = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("qmt-local-")
        ]
        self.assertEqual(leaked, [])


if __name__ == "__main__":
    unittest.main()
