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


ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DIR = ROOT / (chr(0x7F51) + chr(0x5173))
API_DIR = ROOT / (chr(0x5916) + chr(0x7F6E) + chr(0x7B56) + chr(0x7565) + "API")
for path in (ROOT, GATEWAY_DIR, API_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools import project_env
from bigqmt_gateway_proxy import (
    PROXY_BUILD_ID,
    BigQmtGatewayProxy,
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
