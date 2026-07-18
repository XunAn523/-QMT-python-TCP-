"""Small real-loopback Gateway used only by the API contract tests."""

from __future__ import annotations

import socket
import struct
from pathlib import Path
import sys
import threading
from typing import Callable, Dict, List, Optional

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api.config import EXPECTED_GATEWAY_BUILD_ID
from qmt_local_api.protocol import encode_frame

TEST_AUTH_TOKEN = "a" * 64


def _recv_exact(conn: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = conn.recv(size - len(result))
        if not chunk:
            raise EOFError("peer closed")
        result.extend(chunk)
    return bytes(result)


def recv_message(conn: socket.socket, timeout: float = 2.0) -> Dict[str, object]:
    import json

    conn.settimeout(timeout)
    size = struct.unpack(">I", _recv_exact(conn, 4))[0]
    return json.loads(_recv_exact(conn, size).decode("utf-8"))


def send_message(conn: socket.socket, message: Dict[str, object]) -> None:
    conn.sendall(encode_frame(message))


class FakeGateway:
    def __init__(
        self,
        handler: Callable[[socket.socket, "FakeGateway"], None],
        *,
        account_id: str = "TEST_ACCOUNT",
        account_name: str = "account_main",
        protocol_version: int = 2,
        build_id: str = EXPECTED_GATEWAY_BUILD_ID,
        auth_token: str = TEST_AUTH_TOKEN,
        expected_client_account_id: str = "TEST_ACCOUNT",
        expected_client_account_name: str = "account_main",
        expected_client_protocol_version: int = 2,
        handshake_error_code: str = "",
    ) -> None:
        self.handler = handler
        self.account_id = account_id
        self.account_name = account_name
        self.protocol_version = protocol_version
        self.build_id = build_id
        self.auth_token = auth_token
        self.expected_client_account_id = expected_client_account_id
        self.expected_client_account_name = expected_client_account_name
        self.expected_client_protocol_version = expected_client_protocol_version
        self.handshake_error_code = handshake_error_code
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = int(self.listener.getsockname()[1])
        self.first_frames: List[Dict[str, object]] = []
        self.error: Optional[BaseException] = None
        self.done = threading.Event()
        self._conn: Optional[socket.socket] = None
        self._thread = threading.Thread(target=self._run, name="fake-local-gateway", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            conn, _ = self.listener.accept()
            self._conn = conn
            first = recv_message(conn)
            self.first_frames.append(first)
            if first.get("type") != "PING":
                raise AssertionError("first application frame was not PING")
            if first.get("protocol_version") != self.expected_client_protocol_version:
                raise AssertionError("first PING protocol_version did not match")
            if first.get("account_id") != self.expected_client_account_id:
                raise AssertionError("first PING account_id did not match")
            if first.get("account_name") != self.expected_client_account_name:
                raise AssertionError("first PING account_name did not match")
            if first.get("auth_token") != self.auth_token:
                raise AssertionError("first PING auth_token did not match")
            if self.handshake_error_code:
                send_message(conn, {
                    "type": "ERROR",
                    "msg_id": first.get("msg_id", ""),
                    "status": "REJECTED",
                    "code": self.handshake_error_code,
                    "reject_reason": "authentication or identity mismatch",
                })
                return
            send_message(conn, {
                "type": "PONG",
                "msg_id": first.get("msg_id", ""),
                "protocol_version": self.protocol_version,
                "build_id": self.build_id,
                "account_id": self.account_id,
                "account_name": self.account_name,
            })
            self.handler(conn, self)
        except BaseException as exc:
            self.error = exc
        finally:
            self.done.set()
            if self._conn is not None:
                try:
                    self._conn.close()
                except OSError:
                    pass
            try:
                self.listener.close()
            except OSError:
                pass

    def assert_clean(self, timeout: float = 3.0) -> None:
        if not self.done.wait(timeout):
            raise AssertionError("fake Gateway did not finish")
        self._thread.join(timeout=timeout)
        if self.error is not None:
            raise self.error

    def close(self) -> None:
        try:
            self.listener.close()
        except OSError:
            pass
        if self._conn is not None:
            try:
                self._conn.close()
            except OSError:
                pass
