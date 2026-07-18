"""Validated single-account configuration for the local external API."""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import math
import re

from .protocol import MAX_FRAME_BYTES, PROTOCOL_VERSION


EXPECTED_GATEWAY_BUILD_ID = (
    "xuanling_local_qmt_gateway_20260718_low_latency_v7_post_enqueue_barrier"
)
AUTH_TOKEN_PATTERN = re.compile(r"^[0-9A-Fa-f]{64}$")


def validate_auth_token(value: object) -> str:
    """Return a valid shared token without including it in error output."""
    if not isinstance(value, str) or AUTH_TOKEN_PATTERN.fullmatch(value) is None:
        raise ValueError("auth_token must be exactly 64 hexadecimal characters")
    return value


@dataclass(frozen=True)
class ConnectionConfig:
    account_name: str
    account_id: str
    account_type: str
    auth_token: str = field(repr=False)
    host: str = "127.0.0.1"
    local_host: str = "127.0.0.1"
    port: int = 9550
    expected_gateway_build_id: str = EXPECTED_GATEWAY_BUILD_ID
    connect_timeout: float = 5.0
    recv_timeout: float = 0.2
    handshake_timeout: float = 5.0
    heartbeat_interval: float = 5.0
    heartbeat_timeout: float = 15.0
    max_frame_bytes: int = MAX_FRAME_BYTES
    auto_reconnect: bool = True

    @property
    def slot(self) -> str:
        """Compatibility label used only in thread and diagnostic names."""
        return self.account_name

    def validate(self) -> None:
        validate_auth_token(self.auth_token)
        try:
            address = ipaddress.ip_address(self.host)
        except ValueError as exc:
            raise ValueError("QMT local bind host must be a valid IPv4 address") from exc
        if address.version != 4 or not address.is_loopback:
            raise ValueError("QMT local API host must be an IPv4 loopback address")
        if self.local_host:
            try:
                local_address = ipaddress.ip_address(self.local_host)
            except ValueError as exc:
                raise ValueError("local source host must be a valid IPv4 address") from exc
            if local_address.version != 4 or not local_address.is_loopback:
                raise ValueError("local source host must be an IPv4 loopback address")
        if not 1 <= int(self.port) <= 65535:
            raise ValueError("QMT local TCP port must be in 1..65535")
        if not self.account_name.strip():
            raise ValueError("account_name is required for the PONG identity guard")
        if not self.account_id.strip():
            raise ValueError("account_id is required for the PONG identity guard")
        if self.account_type not in {"STOCK", "CREDIT"}:
            raise ValueError("account_type must be STOCK or CREDIT")
        if not self.expected_gateway_build_id.strip():
            raise ValueError("expected_gateway_build_id is required")
        if self.expected_gateway_build_id != EXPECTED_GATEWAY_BUILD_ID:
            raise ValueError("Gateway build is a fixed package contract")
        if self.max_frame_bytes != MAX_FRAME_BYTES:
            raise ValueError("max_frame_bytes must remain 10485760")
        for name, value in (
            ("connect_timeout", self.connect_timeout),
            ("recv_timeout", self.recv_timeout),
            ("handshake_timeout", self.handshake_timeout),
            ("heartbeat_interval", self.heartbeat_interval),
            ("heartbeat_timeout", self.heartbeat_timeout),
        ):
            if not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError("%s must be a finite positive number" % name)
        if self.heartbeat_timeout <= self.heartbeat_interval:
            raise ValueError("heartbeat_timeout must exceed heartbeat_interval")


__all__ = [
    "ConnectionConfig",
    "EXPECTED_GATEWAY_BUILD_ID",
    "MAX_FRAME_BYTES",
    "PROTOCOL_VERSION",
    "validate_auth_token",
]
