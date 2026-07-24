"""Authenticated loopback RPC server for :mod:`qmt_local_api.coordinator`.

This is deliberately a small internal strategy API, not another Gateway.  It
never handles the QMT token, never touches the Helper runtime, and delegates
all trading effects to the one ``AccountCoordinator`` instance.
"""

from __future__ import annotations

import json
import logging
import socket
import struct
import threading
from typing import Any, Dict, Optional, Set

from .coordinator import (
    AccountCoordinator,
    CoordinatorConflict,
    CoordinatorError,
    CoordinatorRiskRejected,
    CoordinatorUnavailable,
)
from .protocol import MAX_FRAME_BYTES, encode_frame


logger = logging.getLogger(__name__)


def _reject_non_finite(value: str) -> None:
    raise ValueError("non-finite JSON numbers are not allowed: %s" % value)


def _read_exact(conn: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        part = conn.recv(size - len(result))
        if not part:
            raise EOFError("peer closed")
        result.extend(part)
    return bytes(result)


def _read_message(conn: socket.socket) -> Dict[str, Any]:
    size = struct.unpack(">I", _read_exact(conn, 4))[0]
    if not 0 < size <= MAX_FRAME_BYTES:
        raise ValueError("invalid frame length")
    decoded = json.loads(
        _read_exact(conn, size).decode("utf-8"), parse_constant=_reject_non_finite
    )
    if not isinstance(decoded, dict):
        raise ValueError("frame body must be a JSON object")
    return decoded


class CoordinatorLocalServer:
    """Bounded, IPv4-loopback-only RPC endpoint for independent strategies.

    Wire contract:

    1. every connection sends ``COORDINATOR_HELLO`` with ``strategy_id`` and
       its coordinator-specific ``auth_token``;
    2. the server responds with ``COORDINATOR_PONG`` only after validation;
    3. the client can issue ``ORDER_INTENT``, ``CANCEL_INTENT``,
       ``POLL_EVENTS``, ``ACK_EVENT`` and ``ACCOUNT_STATUS`` frames.

    The server uses pull-based durable events.  ``POLL_EVENTS`` may redeliver
    an event until the strategy sends ``ACK_EVENT``; this keeps a slow or
    offline strategy isolated from Gateway ``DELIVERY_ACK`` progress.
    """

    def __init__(
        self,
        coordinator: AccountCoordinator,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        max_clients: int = 32,
    ) -> None:
        if host != "127.0.0.1":
            raise ValueError("CoordinatorLocalServer must bind exactly 127.0.0.1")
        if not 0 <= int(port) <= 65535:
            raise ValueError("port must be in 0..65535")
        if int(max_clients) <= 0:
            raise ValueError("max_clients must be positive")
        self.coordinator = coordinator
        self.host = host
        self.requested_port = int(port)
        self.max_clients = int(max_clients)
        self._listener: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._slots = threading.BoundedSemaphore(self.max_clients)
        self._clients: Set[socket.socket] = set()
        self._clients_lock = threading.Lock()

    @property
    def port(self) -> int:
        listener = self._listener
        if listener is None:
            return 0
        return int(listener.getsockname()[1])

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> int:
        if self.is_running:
            return self.port
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.requested_port))
        listener.listen(self.max_clients)
        listener.settimeout(0.2)
        self._listener = listener
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._serve, name="qmt-coordinator-local-server", daemon=True
        )
        self._thread.start()
        return self.port

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        with self._clients_lock:
            clients = tuple(self._clients)
        for conn in clients:
            try:
                conn.close()
            except OSError:
                pass
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.01, float(timeout)))
        if self._thread is thread and (thread is None or not thread.is_alive()):
            self._thread = None

    close = stop

    def _serve(self) -> None:
        while not self._stop.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                conn, peer = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if peer[0] != "127.0.0.1" or not self._slots.acquire(blocking=False):
                try:
                    conn.close()
                except OSError:
                    pass
                continue
            conn.settimeout(1.0)
            with self._clients_lock:
                self._clients.add(conn)
            threading.Thread(
                target=self._serve_client,
                args=(conn,),
                name="qmt-coordinator-strategy",
                daemon=True,
            ).start()

    @staticmethod
    def _reply(conn: socket.socket, message: Dict[str, Any]) -> None:
        conn.sendall(encode_frame(message))

    def _serve_client(self, conn: socket.socket) -> None:
        try:
            try:
                hello = _read_message(conn)
            except (EOFError, OSError, ValueError, UnicodeDecodeError):
                return
            strategy_id = str(hello.get("strategy_id") or "").strip()
            try:
                authenticated = (
                    hello.get("type") == "COORDINATOR_HELLO"
                    and self.coordinator.authenticate_strategy(
                        strategy_id, str(hello.get("auth_token") or "")
                    )
                )
            except ValueError:
                authenticated = False
            if not authenticated:
                self._reply(conn, {
                    "type": "ERROR",
                    "code": "COORDINATOR_HANDSHAKE_REJECTED",
                })
                return
            self._reply(conn, {
                "type": "COORDINATOR_PONG",
                "strategy_id": strategy_id,
                "ready": not self.coordinator.trading_halted,
            })
            while not self._stop.is_set():
                try:
                    request = _read_message(conn)
                except socket.timeout:
                    continue
                except (EOFError, OSError, ValueError, UnicodeDecodeError):
                    return
                self._reply(conn, self._handle_request(strategy_id, request))
        except OSError:
            return
        finally:
            with self._clients_lock:
                self._clients.discard(conn)
            try:
                conn.close()
            except OSError:
                pass
            self._slots.release()

    def _handle_request(self, strategy_id: str, request: Dict[str, Any]) -> Dict[str, Any]:
        message_type = str(request.get("type") or "").upper()
        message_id = str(request.get("msg_id") or "")
        try:
            if message_type == "PING":
                return {"type": "PONG", "msg_id": message_id, "ready": not self.coordinator.trading_halted}
            if message_type == "ORDER_INTENT":
                command = self.coordinator.submit_order(
                    strategy_id,
                    str(request.get("strategy_order_id") or ""),
                    str(request.get("symbol") or ""),
                    str(request.get("side") or ""),
                    request.get("quantity"),
                    request.get("price"),
                    price_type=int(request.get("price_type") or 11),
                    order_type=int(request.get("order_type") or 0),
                    order_remark=str(request.get("order_remark") or ""),
                    trace_id=str(request.get("trace_id") or ""),
                    spread=float(request.get("spread") or 0.0),
                    business_order_type=str(request.get("business_order_type") or "limit"),
                    credit_mode=str(request.get("credit_mode") or ""),
                )
                return {"type": "ORDER_RESULT", "msg_id": message_id, "success": True, "command": command}
            if message_type == "CANCEL_INTENT":
                command = self.coordinator.request_cancel(
                    strategy_id,
                    str(request.get("strategy_cancel_id") or ""),
                    str(request.get("target_strategy_order_id") or ""),
                )
                return {"type": "CANCEL_RESULT", "msg_id": message_id, "success": True, "command": command}
            if message_type == "POLL_EVENTS":
                return {
                    "type": "EVENT_BATCH",
                    "msg_id": message_id,
                    "events": self.coordinator.poll_events(strategy_id, int(request.get("limit") or 100)),
                }
            if message_type == "ACK_EVENT":
                return {
                    "type": "ACK_RESULT",
                    "msg_id": message_id,
                    "acknowledged": self.coordinator.acknowledge_event(
                        strategy_id, str(request.get("coordinator_event_id") or "")
                    ),
                }
            if message_type == "ACCOUNT_STATUS":
                return {
                    "type": "ACCOUNT_STATUS_RESULT",
                    "msg_id": message_id,
                    "status": self.coordinator.account_status(),
                }
            return {"type": "ERROR", "msg_id": message_id, "code": "UNSUPPORTED_REQUEST"}
        except CoordinatorRiskRejected:
            return {"type": "ERROR", "msg_id": message_id, "code": "RISK_REJECTED"}
        except CoordinatorConflict:
            return {"type": "ERROR", "msg_id": message_id, "code": "IDEMPOTENCY_CONFLICT"}
        except CoordinatorUnavailable:
            return {"type": "ERROR", "msg_id": message_id, "code": "ACCOUNT_NOT_READY"}
        except (CoordinatorError, TypeError, ValueError, OverflowError):
            logger.warning("coordinator strategy request rejected type=%s", message_type)
            return {"type": "ERROR", "msg_id": message_id, "code": "INVALID_REQUEST"}


__all__ = ["CoordinatorLocalServer"]
