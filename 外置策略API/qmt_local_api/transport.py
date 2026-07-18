"""Thread-safe low-latency TCP transport for a local Windows Gateway."""

from __future__ import annotations

from collections import deque
import logging
import socket
import threading
import time
import uuid
from typing import Any, Deque, Dict, Optional

from .config import validate_auth_token
from .protocol import (
    FrameDecoder,
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    ProtocolError,
    encode_frame,
)


logger = logging.getLogger(__name__)


class TransportDisconnected(ConnectionError):
    """The identity-validated local Gateway connection is unavailable."""


class TradeTransport:
    """One serialized-writer, single-reader local TCP connection."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9550,
        *,
        local_host: str = "127.0.0.1",
        account_id: str = "",
        account_name: str = "",
        auth_token: str = "",
        connect_timeout: float = 5.0,
        recv_timeout: float = 0.2,
        handshake_timeout: float = 5.0,
        heartbeat_interval: float = 5.0,
        heartbeat_timeout: float = 15.0,
        max_frame_bytes: int = MAX_FRAME_BYTES,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.local_host = str(local_host)
        self.account_id = str(account_id)
        self.account_name = str(account_name)
        self.auth_token = validate_auth_token(auth_token)
        self.connect_timeout = float(connect_timeout)
        self.recv_timeout = float(recv_timeout)
        self.handshake_timeout = float(handshake_timeout)
        self.heartbeat_interval = float(heartbeat_interval)
        self.heartbeat_timeout = float(heartbeat_timeout)
        self.max_frame_bytes = int(max_frame_bytes)

        self._socket: Optional[socket.socket] = None
        self._connected = False
        self._state_lock = threading.RLock()
        self._connect_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._reader_lock = threading.Lock()
        self._reader_ident: Optional[int] = None
        self._decoder = FrameDecoder(max_frame_bytes=self.max_frame_bytes)
        self._decoded: Deque[Dict[str, Any]] = deque()
        self._last_inbound = 0.0
        self._last_ping = 0.0
        self.last_connect_metrics: Dict[str, Any] = {}
        self.last_handshake_response: Dict[str, Any] = {}

    @staticmethod
    def new_msg_id() -> str:
        return uuid.uuid4().hex[:16]

    @property
    def is_connected(self) -> bool:
        with self._state_lock:
            return self._connected and self._socket is not None

    @property
    def reader_thread_id(self) -> Optional[int]:
        with self._reader_lock:
            return self._reader_ident

    def claim_reader(self) -> None:
        ident = threading.get_ident()
        with self._reader_lock:
            if self._reader_ident not in (None, ident):
                raise RuntimeError("TCP receive already belongs to another thread")
            self._reader_ident = ident

    def release_reader(self) -> None:
        ident = threading.get_ident()
        with self._reader_lock:
            if self._reader_ident == ident:
                self._reader_ident = None

    def _assert_reader(self) -> None:
        with self._reader_lock:
            if self._reader_ident != threading.get_ident():
                raise RuntimeError("receive() may only be called by the claimed poll thread")

    @staticmethod
    def _enable_keepalive(sock: socket.socket) -> None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Windows exposes millisecond keepalive tuning through ioctl.  The
        # guarded POSIX options keep local contract tests portable.
        ioctl_option = getattr(socket, "SIO_KEEPALIVE_VALS", None)
        if ioctl_option is not None and hasattr(sock, "ioctl"):
            try:
                sock.ioctl(ioctl_option, (1, 10_000, 3_000))
            except OSError:
                logger.debug("Windows keepalive interval tuning is unavailable")
        for name, value in (("TCP_KEEPIDLE", 10), ("TCP_KEEPINTVL", 3), ("TCP_KEEPCNT", 3)):
            option = getattr(socket, name, None)
            if option is not None:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, option, value)
                except OSError:
                    logger.debug("socket option %s is unavailable", name)

    def connect(self, profile: str = "normal") -> bool:
        """Connect and require the matching PONG before exposing readiness."""
        with self._connect_lock:
            if self.is_connected:
                return True
            self.close()
            self.last_handshake_response = {}
            started = time.perf_counter()
            tcp_connected = started
            ping_sent = started
            msg_id = self.new_msg_id()
            sock: Optional[socket.socket] = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self._enable_keepalive(sock)
                sock.settimeout(self.connect_timeout)
                if self.local_host:
                    sock.bind((self.local_host, 0))
                with self._state_lock:
                    self._socket = sock
                    self._connected = False
                    self._decoder.reset()
                    self._decoded.clear()
                sock.connect((self.host, self.port))
                tcp_connected = time.perf_counter()
                sock.settimeout(self.recv_timeout)
                self._send_encoded(
                    encode_frame({
                        "type": "PING",
                        "msg_id": msg_id,
                        "protocol_version": PROTOCOL_VERSION,
                        "account_id": self.account_id,
                        "account_name": self.account_name,
                        "auth_token": self.auth_token,
                        "timestamp": time.time(),
                    }),
                    require_ready=False,
                )
                ping_sent = time.perf_counter()
                response = self._receive_frame(self.handshake_timeout)
                self.last_handshake_response = dict(response or {})
                if not response or response.get("type") != "PONG":
                    raise TransportDisconnected("Gateway did not answer the PING handshake")
                if str(response.get("msg_id") or "") != msg_id:
                    raise ProtocolError("handshake PONG msg_id does not match PING")
                now_mono = time.monotonic()
                with self._state_lock:
                    self._connected = True
                    self._last_inbound = now_mono
                    self._last_ping = now_mono
                self._record_connect_metrics(
                    profile, True, msg_id, started, tcp_connected, ping_sent,
                    time.perf_counter(), "",
                )
                return True
            except Exception as exc:
                error = repr(exc)
                self._record_connect_metrics(
                    profile, False, msg_id, started, tcp_connected, ping_sent,
                    time.perf_counter(), error,
                )
                logger.warning(
                    "local QMT connect failed endpoint=%s:%s error=%s",
                    self.host,
                    self.port,
                    error,
                )
                self.close()
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass
                return False

    def _record_connect_metrics(
        self,
        profile: str,
        ok: bool,
        msg_id: str,
        started: float,
        tcp_connected: float,
        ping_sent: float,
        ended: float,
        error: str,
    ) -> None:
        self.last_connect_metrics = {
            "host": self.host,
            "port": self.port,
            "profile": profile,
            "ok": ok,
            "tcp_connect_ms": max(0.0, (tcp_connected - started) * 1000.0),
            "ping_pong_ms": max(0.0, (ended - ping_sent) * 1000.0),
            "total_ms": max(0.0, (ended - started) * 1000.0),
            "msg_id": msg_id,
            "error": error,
        }

    def close(self) -> None:
        with self._state_lock:
            sock = self._socket
            self._socket = None
            self._connected = False
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def send_message(self, message: Dict[str, Any]) -> str:
        frame = encode_frame(message, self.max_frame_bytes)
        self._send_encoded(frame, require_ready=True)
        return str(message.get("msg_id") or "")

    def _send_encoded(self, frame: bytes, *, require_ready: bool) -> None:
        with self._send_lock:
            with self._state_lock:
                sock = self._socket
                ready = self._connected
            if sock is None or (require_ready and not ready):
                raise TransportDisconnected("local QMT bridge is not connected")
            try:
                sock.sendall(frame)
            except (ConnectionError, OSError) as exc:
                with self._state_lock:
                    self._connected = False
                raise TransportDisconnected("send failed") from exc

    def receive(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Receive one business message; only the claimed poll thread may call."""
        self._assert_reader()
        effective = self.recv_timeout if timeout is None else max(0.0, float(timeout))
        deadline = time.monotonic() + effective
        while self.is_connected:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            message = self._receive_frame(remaining)
            if message is None:
                return None
            msg_type = str(message.get("type") or "")
            if msg_type == "PING":
                self.send_message({
                    "type": "PONG",
                    "msg_id": str(message.get("msg_id") or ""),
                    "timestamp": time.time(),
                })
                continue
            if msg_type == "PONG":
                continue
            return message
        raise TransportDisconnected("local QMT bridge is not connected")

    def _receive_frame(self, timeout: float) -> Optional[Dict[str, Any]]:
        if self._decoded:
            return self._decoded.popleft()
        with self._state_lock:
            sock = self._socket
        if sock is None:
            raise TransportDisconnected("local QMT bridge is not connected")
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                sock.settimeout(remaining)
                chunk = sock.recv(65536)
            except socket.timeout:
                return None
            except (ConnectionError, OSError) as exc:
                with self._state_lock:
                    self._connected = False
                raise TransportDisconnected("receive failed") from exc
            if not chunk:
                with self._state_lock:
                    self._connected = False
                raise TransportDisconnected("Gateway closed the connection")
            messages = self._decoder.feed(chunk)
            self._last_inbound = time.monotonic()
            if messages:
                self._decoded.extend(messages[1:])
                return messages[0]

    def maintain_heartbeat(self) -> bool:
        if not self.is_connected:
            return False
        now_mono = time.monotonic()
        if now_mono - self._last_inbound >= self.heartbeat_timeout:
            return False
        if now_mono - self._last_ping >= self.heartbeat_interval:
            self.send_message({
                "type": "PING",
                "msg_id": self.new_msg_id(),
                "timestamp": time.time(),
            })
            self._last_ping = now_mono
        return True
