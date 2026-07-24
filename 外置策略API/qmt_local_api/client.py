"""High-level single-account client with one reader and reliable ACKs."""

from __future__ import annotations

from collections import OrderedDict
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional, Union

from .config import ConnectionConfig
from .dispatcher import AsyncMessageDispatcher, DispatchResult
from .protocol import PROTOCOL_VERSION
from .query_broker import QueryBroker
from .transport import TradeTransport, TransportDisconnected


logger = logging.getLogger(__name__)
Number = Union[int, float]


class BridgeClient:
    """Full-performance TCP client used by :class:`LocalQmtApi`.

    One poll thread owns every socket read.  Query callers wait on a bounded
    broker, callbacks run in wire order on one bounded worker, and reliable
    messages are acknowledged only after every registered handler succeeds.
    """

    def __init__(
        self,
        config: ConnectionConfig,
        *,
        max_pending_queries: int = 128,
        dispatch_queue_size: int = 1024,
        completed_delivery_cache: int = 8192,
        transport: Optional[TradeTransport] = None,
    ) -> None:
        config.validate()
        if int(completed_delivery_cache) <= 0:
            raise ValueError("completed_delivery_cache must be positive")
        self.config = config
        self.transport = transport or TradeTransport(
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
            max_frame_bytes=config.max_frame_bytes,
        )
        self.dispatcher = AsyncMessageDispatcher(max_queue_size=dispatch_queue_size)
        self.query_broker = QueryBroker(max_pending=max_pending_queries)
        self._completed_delivery_cache = int(completed_delivery_cache)
        self._completed_deliveries: "OrderedDict[str, None]" = OrderedDict()
        self._acknowledged_deliveries: "OrderedDict[str, None]" = OrderedDict()
        self._pending_deliveries = set()
        self._delivery_lock = threading.Lock()
        self._delivery_condition = threading.Condition(self._delivery_lock)
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._identity_guard_failed = threading.Event()
        self._identity_guard_reason = ""
        self._poll_thread: Optional[threading.Thread] = None
        self._lifecycle_lock = threading.Lock()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set() and self.transport.is_connected

    @property
    def poll_thread_id(self) -> Optional[int]:
        thread = self._poll_thread
        return thread.ident if thread else None

    @property
    def identity_guard_failed(self) -> bool:
        return self._identity_guard_failed.is_set()

    @property
    def identity_guard_reason(self) -> str:
        return self._identity_guard_reason

    def on(self, msg_type: str, handler: Callable[[Dict[str, Any]], Any]) -> None:
        """Register a callback before start() so retained events are handled."""
        self.dispatcher.register(msg_type, handler)

    def off(self, msg_type: str, handler: Callable[[Dict[str, Any]], Any]) -> None:
        self.dispatcher.unregister(msg_type, handler)

    def start(self) -> bool:
        """Start dispatch and the unique poll thread; return initial readiness."""
        with self._lifecycle_lock:
            if self._poll_thread and self._poll_thread.is_alive():
                return self.is_connected
            self._stop.clear()
            self._identity_guard_failed.clear()
            self._identity_guard_reason = ""
            self.dispatcher.start(name="qmt-local-dispatch")
            initially_connected = self._connect_once(profile="startup")
            if self._identity_guard_failed.is_set():
                self.dispatcher.stop()
                return False
            self._poll_thread = threading.Thread(
                target=self._poll_loop,
                name="qmt-local-poll",
                daemon=True,
            )
            self._poll_thread.start()
            return initially_connected

    def stop(self, timeout: float = 5.0) -> None:
        with self._lifecycle_lock:
            self._stop.set()
            self._disconnect()
            thread = self._poll_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=max(0.01, float(timeout)))
        self.dispatcher.stop(timeout=timeout)
        with self._lifecycle_lock:
            if self._poll_thread is thread and (not thread or not thread.is_alive()):
                self._poll_thread = None

    close = stop

    def wait_connected(self, timeout: float = 5.0) -> bool:
        return self._connected.wait(timeout=max(0.0, float(timeout))) and self.transport.is_connected

    def wait_delivery_acknowledged(self, delivery_id: str, timeout: float = 2.0) -> bool:
        value = str(delivery_id or "")
        if not value:
            raise ValueError("delivery_id is required")
        deadline = time.monotonic() + max(0.0, float(timeout))
        with self._delivery_condition:
            while value not in self._acknowledged_deliveries:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._delivery_condition.wait(remaining)
            self._acknowledged_deliveries.move_to_end(value)
            return True

    def _connect_once(self, profile: str) -> bool:
        ok = self.transport.connect(profile=profile)
        if not ok:
            self._connected.clear()
            handshake = self.transport.last_handshake_response
            if str(handshake.get("code") or "") == "HANDSHAKE_REJECTED":
                self._identity_guard_reason = "handshake_rejected"
                self._identity_guard_failed.set()
                logger.error(
                    "local QMT handshake authentication/identity rejected endpoint=%s:%s",
                    self.config.host,
                    self.config.port,
                )
            return False
        handshake = self.transport.last_handshake_response
        try:
            protocol_version = int(handshake.get("protocol_version") or 0)
        except (TypeError, ValueError):
            protocol_version = 0
        gateway_build = str(handshake.get("build_id") or "")
        gateway_account_id = str(handshake.get("account_id") or "")
        gateway_account_name = str(handshake.get("account_name") or "")
        mismatches = []
        if protocol_version != PROTOCOL_VERSION:
            mismatches.append("protocol_version")
        if gateway_build != self.config.expected_gateway_build_id:
            mismatches.append("build_id")
        if gateway_account_id != self.config.account_id:
            mismatches.append("account_id")
        if gateway_account_name != self.config.account_name:
            mismatches.append("account_name")
        if mismatches:
            self._identity_guard_reason = ",".join(mismatches)
            self._identity_guard_failed.set()
            logger.error(
                "local QMT handshake identity mismatch endpoint=%s:%s fields=%s",
                self.config.host,
                self.config.port,
                self._identity_guard_reason,
            )
            self.transport.close()
            self._connected.clear()
            return False
        self._connected.set()
        logger.info("local QMT bridge connected endpoint=%s:%s", self.config.host, self.config.port)
        return True

    def _disconnect(self) -> None:
        self._connected.clear()
        self.query_broker.cancel_all()
        self.transport.close()

    def _poll_loop(self) -> None:
        self.transport.claim_reader()
        reconnect_delays = (0.5, 1.0, 2.0, 5.0)
        reconnect_index = 0
        try:
            while not self._stop.is_set():
                if not self.transport.is_connected:
                    self._connected.clear()
                    self.query_broker.cancel_all()
                    self.transport.close()
                    if self._identity_guard_failed.is_set() or not self.config.auto_reconnect:
                        return
                    delay = reconnect_delays[min(reconnect_index, len(reconnect_delays) - 1)]
                    if self._stop.wait(delay):
                        return
                    if self._connect_once(profile="reconnect"):
                        reconnect_index = 0
                    else:
                        if self._identity_guard_failed.is_set():
                            return
                        reconnect_index += 1
                    continue
                try:
                    message = self.transport.receive(timeout=self.config.recv_timeout)
                    if message is not None:
                        self._handle_inbound(message)
                    if not self.transport.maintain_heartbeat():
                        raise TransportDisconnected("heartbeat timed out")
                except (TransportDisconnected, OSError, ValueError, RuntimeError) as exc:
                    if not self._stop.is_set() and self.config.auto_reconnect:
                        logger.warning("local QMT poll failed; reconnecting error=%s", exc)
                    elif not self._stop.is_set():
                        logger.info("local QMT poll stopped error=%s", exc)
                    self._disconnect()
        finally:
            self.transport.release_reader()
            self._connected.clear()
            with self._lifecycle_lock:
                if self._poll_thread is threading.current_thread():
                    self._poll_thread = None

    def _handle_inbound(self, message: Dict[str, Any]) -> None:
        msg_type = str(message.get("type") or "")
        delivery_id = str(message.get("delivery_id") or "")
        if delivery_id:
            with self._delivery_lock:
                if delivery_id in self._completed_deliveries:
                    self._completed_deliveries.move_to_end(delivery_id)
                    already_completed = True
                else:
                    already_completed = False
                already_pending = delivery_id in self._pending_deliveries
            if already_completed:
                self._send_delivery_ack(delivery_id)
                return
            if already_pending:
                return

        if msg_type == "QUERY_RESPONSE":
            resolved = self.query_broker.resolve(message)
            if delivery_id:
                if resolved:
                    self._mark_completed_and_ack(delivery_id)
                else:
                    self._delivery_failed(delivery_id, "query response has no waiter")
            return

        if delivery_id:
            with self._delivery_lock:
                self._pending_deliveries.add(delivery_id)
            completion = lambda result, value=delivery_id: self._finish_delivery(value, result)
        else:
            completion = None
        if not self.dispatcher.submit(message, completion=completion):
            if delivery_id:
                self._delivery_failed(delivery_id, "dispatch queue is full")
            else:
                logger.error("dropping non-delivery message: dispatch queue is full type=%s", msg_type)

    def _finish_delivery(self, delivery_id: str, result: DispatchResult) -> None:
        if result.acknowledgeable:
            self._mark_completed_and_ack(delivery_id)
        else:
            self._delivery_failed(delivery_id, "event was unhandled or a handler failed")

    def _mark_completed_and_ack(self, delivery_id: str) -> None:
        with self._delivery_lock:
            self._pending_deliveries.discard(delivery_id)
            self._completed_deliveries[delivery_id] = None
            self._completed_deliveries.move_to_end(delivery_id)
            while len(self._completed_deliveries) > self._completed_delivery_cache:
                self._completed_deliveries.popitem(last=False)
        try:
            self._send_delivery_ack(delivery_id)
        except (TransportDisconnected, OSError):
            logger.exception("delivery ACK failed delivery_id=%s", delivery_id)
            if self._connected.is_set():
                self._disconnect()

    def _delivery_failed(self, delivery_id: str, reason: str) -> None:
        with self._delivery_lock:
            self._pending_deliveries.discard(delivery_id)
        logger.error("delivery not acknowledged delivery_id=%s reason=%s", delivery_id, reason)
        self._disconnect()

    def _send_delivery_ack(self, delivery_id: str) -> str:
        msg_id = self.send({"type": "DELIVERY_ACK", "delivery_id": delivery_id})
        with self._delivery_condition:
            self._acknowledged_deliveries[delivery_id] = None
            self._acknowledged_deliveries.move_to_end(delivery_id)
            while len(self._acknowledged_deliveries) > self._completed_delivery_cache:
                self._acknowledged_deliveries.popitem(last=False)
            self._delivery_condition.notify_all()
        return msg_id

    def send(self, message: Dict[str, Any]) -> str:
        if not self.is_connected:
            raise TransportDisconnected("identity-validated local QMT bridge is not ready")
        outbound = dict(message)
        outbound.setdefault("msg_id", self.transport.new_msg_id())
        outbound.setdefault("timestamp", time.time())
        if not outbound.get("type"):
            raise ValueError("message type is required")
        return self.transport.send_message(outbound)

    def build_order_request(
        self,
        symbol: str,
        side: str,
        quantity: Number,
        price: float,
        *,
        client_order_id: str,
        request_id: str = "",
        price_type: int = 11,
        order_type: int = 0,
        strategy_name: str = "qmt_local_api",
        order_remark: str = "",
        trace_id: str = "",
        qmt_user_order_id: str = "",
        authenticated_trader_key: str = "",
        intent_hash: str = "",
        spread: float = 0.0,
        business_order_type: str = "limit",
        credit_mode: str = "",
        intent_volume: Optional[Number] = None,
        intent_effective_price: Optional[float] = None,
        async_mode: bool = True,
    ) -> Dict[str, Any]:
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if not str(symbol).strip():
            raise ValueError("symbol is required")
        if not str(client_order_id).strip():
            raise ValueError("client_order_id is required for idempotency")
        if qmt_user_order_id and len(str(qmt_user_order_id)) > 23:
            raise ValueError("qmt_user_order_id must be at most 23 characters")
        normalized_hash = str(intent_hash or "").strip()
        upper_hash = normalized_hash.upper()
        if normalized_hash.lower() == "sha256-of-order-intent" or any(
            marker in upper_hash
            for marker in ("REPLACE_ME", "REPLACE_WITH", "PLACEHOLDER", "CHANGE_ME")
        ):
            raise ValueError("intent_hash looks like placeholder text; omit it")
        if float(quantity) <= 0:
            raise ValueError("quantity must be positive")
        if float(price) < 0:
            raise ValueError("price cannot be negative")
        effective_order_type = int(order_type) if int(order_type) > 0 else (
            23 if normalized_side == "BUY" else 24
        )
        msg_id = self.transport.new_msg_id()
        normalized_request_id = str(request_id or "").strip()
        request = {
            "type": "NEW_ASYNC" if async_mode else "NEW",
            "msg_id": msg_id,
            "request_id": normalized_request_id or msg_id,
            "protocol_version": PROTOCOL_VERSION,
            "account_id": self.config.account_id,
            "account_name": self.config.account_name,
            "symbol": str(symbol),
            "side": normalized_side,
            "quantity": quantity,
            "price": float(price),
            "order_type": effective_order_type,
            "price_type": int(price_type),
            "strategy_name": str(strategy_name),
            "order_remark": str(order_remark),
            "client_order_id": str(client_order_id),
            "trace_id": str(trace_id),
            "qmt_user_order_id": str(qmt_user_order_id),
            "authenticated_trader_key": str(authenticated_trader_key),
            "intent_volume": quantity if intent_volume is None else intent_volume,
            "spread": float(spread),
            "business_order_type": str(business_order_type),
            "credit_mode": str(credit_mode),
            "effective_price": float(price) if intent_effective_price is None else float(intent_effective_price),
            "created_at_ns": time.time_ns(),
            "timestamp": time.time(),
        }
        if normalized_hash:
            request["intent_hash"] = normalized_hash
        return request

    def send_order_async(
        self,
        symbol: str,
        side: str,
        quantity: Number,
        price: float,
        *,
        client_order_id: str,
        request_id: str = "",
        price_type: int = 11,
        order_type: int = 0,
        strategy_name: str = "qmt_local_api",
        order_remark: str = "",
        trace_id: str = "",
        qmt_user_order_id: str = "",
        authenticated_trader_key: str = "",
        intent_hash: str = "",
        spread: float = 0.0,
        business_order_type: str = "limit",
        credit_mode: str = "",
        intent_volume: Optional[Number] = None,
        intent_effective_price: Optional[float] = None,
    ) -> str:
        return self.send(self.build_order_request(
            symbol,
            side,
            quantity,
            price,
            client_order_id=client_order_id,
            request_id=request_id,
            price_type=price_type,
            order_type=order_type,
            strategy_name=strategy_name,
            order_remark=order_remark,
            trace_id=trace_id,
            qmt_user_order_id=qmt_user_order_id,
            authenticated_trader_key=authenticated_trader_key,
            intent_hash=intent_hash,
            spread=spread,
            business_order_type=business_order_type,
            credit_mode=credit_mode,
            intent_volume=intent_volume,
            intent_effective_price=intent_effective_price,
            async_mode=True,
        ))

    def send_order(self, *args: Any, **kwargs: Any) -> str:
        kwargs["async_mode"] = False
        return self.send(self.build_order_request(*args, **kwargs))

    def build_cancel_request(
        self,
        order_id: str,
        *,
        async_mode: bool = False,
        request_id: str = "",
    ) -> Dict[str, Any]:
        if not str(order_id).strip():
            raise ValueError("order_id is required")
        request = {
            "type": "CANCEL_ASYNC" if async_mode else "CANCEL",
            "account_id": self.config.account_id,
            "account_name": self.config.account_name,
            "order_id": str(order_id),
        }
        normalized_request_id = str(request_id or "").strip()
        if normalized_request_id:
            request["request_id"] = normalized_request_id
        return request

    def send_cancel(
        self,
        order_id: str,
        *,
        async_mode: bool = False,
        request_id: str = "",
    ) -> str:
        return self.send(self.build_cancel_request(
            order_id,
            async_mode=async_mode,
            request_id=request_id,
        ))

    def send_cancel_async(self, order_id: str, *, request_id: str = "") -> str:
        return self.send_cancel(
            order_id,
            async_mode=True,
            request_id=request_id,
        )

    def build_cancel_sysid_request(
        self,
        market: int,
        order_sysid: str,
        *,
        async_mode: bool = False,
        request_id: str = "",
    ) -> Dict[str, Any]:
        if not str(order_sysid).strip():
            raise ValueError("order_sysid is required")
        request = {
            "type": "CANCEL_SYSID_ASYNC" if async_mode else "CANCEL_SYSID",
            "account_id": self.config.account_id,
            "account_name": self.config.account_name,
            "market": int(market),
            "order_sysid": str(order_sysid),
        }
        normalized_request_id = str(request_id or "").strip()
        if normalized_request_id:
            request["request_id"] = normalized_request_id
        return request

    def send_cancel_sysid(
        self,
        market: int,
        order_sysid: str,
        *,
        async_mode: bool = False,
        request_id: str = "",
    ) -> str:
        return self.send(
            self.build_cancel_sysid_request(
                market,
                order_sysid,
                async_mode=async_mode,
                request_id=request_id,
            )
        )

    def send_cancel_sysid_async(
        self,
        market: int,
        order_sysid: str,
        *,
        request_id: str = "",
    ) -> str:
        return self.send_cancel_sysid(
            market,
            order_sysid,
            async_mode=True,
            request_id=request_id,
        )

    def send_subscribe(self) -> str:
        return self.send({
            "type": "SUBSCRIBE",
            "account_id": self.config.account_id,
            "account_name": self.config.account_name,
            "account_type": self.config.account_type,
        })

    def send_unsubscribe(self) -> str:
        return self.send({
            "type": "UNSUBSCRIBE",
            "account_id": self.config.account_id,
            "account_name": self.config.account_name,
        })

    def send_query(
        self,
        *,
        query_type: str = "",
        params: Optional[Dict[str, Any]] = None,
        msg_id: str = "",
    ) -> str:
        return self.send({
            "type": "QUERY",
            "msg_id": msg_id or self.transport.new_msg_id(),
            "account_id": self.config.account_id,
            "account_name": self.config.account_name,
            "query_type": str(query_type or ""),
            "params": dict(params or {}),
        })

    def query(
        self,
        query_type: str = "",
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        return self.query_broker.request(
            self.send_query,
            query_type=query_type,
            params=params,
            timeout=timeout,
        )

    def __enter__(self) -> "BridgeClient":
        if not self.start():
            raise TransportDisconnected("initial local QMT connection failed")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.stop()


__all__ = ["BridgeClient", "TransportDisconnected"]
