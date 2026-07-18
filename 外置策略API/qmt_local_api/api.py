"""Stable public API for ordinary external Windows CPython strategies."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .client import BridgeClient
from .config import ConnectionConfig
from .runtime import DEFAULT_ENV_FILE, LocalRuntimeConfig


REDACTED = "<REDACTED>"


def redact_for_output(value: Any) -> Any:
    """Return a recursive console-safe copy without account/key/raw fields."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            normalized = str(key).lower().replace("_", "")
            sensitive = (
                normalized == "raw"
                or "accountid" in normalized
                or "accountname" in normalized
                or normalized in {"authenticatedtraderkey", "writertoken", "authtoken"}
            )
            result[key] = REDACTED if sensitive else redact_for_output(item)
        return result
    if isinstance(value, list):
        return [redact_for_output(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_for_output(item) for item in value)
    return value


class LocalQmtApi(BridgeClient):
    """Single-account local QMT API loaded from the project-root `.env`."""

    def __init__(
        self,
        config: ConnectionConfig,
        *,
        query_timeout: float = 6.0,
        max_pending_queries: int = 128,
        dispatch_queue_size: int = 1024,
        completed_delivery_cache: int = 8192,
        runtime: Optional[LocalRuntimeConfig] = None,
        transport=None,
    ) -> None:
        super().__init__(
            config,
            max_pending_queries=max_pending_queries,
            dispatch_queue_size=dispatch_queue_size,
            completed_delivery_cache=completed_delivery_cache,
            transport=transport,
        )
        if float(query_timeout) <= 0:
            raise ValueError("query_timeout must be positive")
        self.default_query_timeout = float(query_timeout)
        self.runtime = runtime

    @classmethod
    def from_env(
        cls,
        env_file: str | Path = DEFAULT_ENV_FILE,
    ) -> "LocalQmtApi":
        """Create a fail-closed live client from the one project `.env`."""
        runtime = LocalRuntimeConfig.load(env_file)
        return cls(
            runtime.connection,
            query_timeout=runtime.query_timeout,
            max_pending_queries=runtime.max_pending_queries,
            dispatch_queue_size=runtime.dispatch_queue_size,
            completed_delivery_cache=runtime.completed_delivery_cache,
            runtime=runtime,
        )

    def connect(self, timeout: Optional[float] = None) -> bool:
        """Start once and optionally wait through the first reconnect attempt."""
        if self.start():
            return True
        if self.identity_guard_failed:
            return False
        wait_timeout = (
            max(self.config.connect_timeout, self.config.handshake_timeout)
            if timeout is None
            else max(0.0, float(timeout))
        )
        return self.wait_connected(wait_timeout)

    def query(
        self,
        query_type: str = "",
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        return super().query(
            query_type,
            params,
            timeout=self.default_query_timeout if timeout is None else timeout,
        )

    place_order_async = BridgeClient.send_order_async
    cancel_order_async = BridgeClient.send_cancel_async
    cancel_order_by_sysid_async = BridgeClient.send_cancel_sysid_async


__all__ = ["LocalQmtApi", "REDACTED", "redact_for_output"]
