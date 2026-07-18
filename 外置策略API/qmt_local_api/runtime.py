"""Load the external API exclusively from the project-root resolver."""

from __future__ import annotations

from dataclasses import dataclass
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Mapping, Optional

from .config import ConnectionConfig, EXPECTED_GATEWAY_BUILD_ID
from .protocol import MAX_FRAME_BYTES, PROTOCOL_VERSION


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"


def _load_project_resolver(env_file: Path) -> ModuleType:
    candidates = (
        env_file.parent / "tools" / "project_env.py",
        PROJECT_ROOT / "tools" / "project_env.py",
    )
    resolver_path = next((path for path in candidates if path.is_file()), None)
    if resolver_path is None:
        raise RuntimeError("project tools/project_env.py is required")
    spec = importlib.util.spec_from_file_location("qmt_local_project_env", resolver_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load project environment resolver")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "load_deployment", None)):
        raise RuntimeError("project resolver does not export load_deployment")
    return module


@dataclass(frozen=True)
class LocalRuntimeConfig:
    connection: ConnectionConfig
    query_timeout: float
    signal_wait_seconds: float
    signal_journal: str
    log_level: str
    max_pending_queries: int
    dispatch_queue_size: int
    completed_delivery_cache: int
    env_file: Path

    @classmethod
    def load(
        cls,
        env_file: str | Path = DEFAULT_ENV_FILE,
        *,
        allow_example: bool = False,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "LocalRuntimeConfig":
        env_path = Path(env_file).expanduser().resolve()
        resolver = _load_project_resolver(env_path)
        deployment = resolver.load_deployment(
            env_path,
            allow_example=allow_example,
            environ=environ,
        )
        raw = deployment.get("api_config")
        if not isinstance(raw, dict):
            raise ValueError("project resolver returned no api_config")
        if int(raw.get("protocol_version") or 0) != PROTOCOL_VERSION:
            raise ValueError("protocol_version must remain 2")
        if str(raw.get("expected_gateway_build_id") or "") != EXPECTED_GATEWAY_BUILD_ID:
            raise ValueError("expected Gateway build does not match this API package")
        connection = ConnectionConfig(
            account_name=str(raw.get("account_name") or "").strip(),
            account_id=str(raw.get("account_id") or "").strip(),
            account_type=str(raw.get("account_type") or "").strip().upper(),
            auth_token=str(raw.get("auth_token") or "").strip(),
            host=str(raw.get("host") or "").strip(),
            local_host=str(raw.get("local_host") or "").strip(),
            port=int(raw.get("port") or 0),
            expected_gateway_build_id=EXPECTED_GATEWAY_BUILD_ID,
            connect_timeout=float(raw.get("connect_timeout") or 0),
            recv_timeout=float(raw.get("recv_timeout") or 0),
            handshake_timeout=float(raw.get("handshake_timeout") or 0),
            heartbeat_interval=float(raw.get("heartbeat_interval") or 0),
            heartbeat_timeout=float(raw.get("heartbeat_timeout") or 0),
            max_frame_bytes=MAX_FRAME_BYTES,
            auto_reconnect=bool(raw.get("auto_reconnect")),
        )
        connection.validate()
        query_timeout = float(raw.get("query_timeout") or 0)
        signal_wait_seconds = float(raw.get("signal_wait_seconds") or 0)
        if query_timeout <= 0 or signal_wait_seconds <= 0:
            raise ValueError("query and signal wait timeouts must be positive")
        log_level = str(raw.get("log_level") or "").upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("invalid API log level")
        max_pending = int(raw.get("max_pending_queries") or 0)
        dispatch_size = int(raw.get("dispatch_queue_size") or 0)
        completed_cache = int(raw.get("completed_delivery_cache") or 0)
        if min(max_pending, dispatch_size, completed_cache) <= 0:
            raise ValueError("API queue/cache capacities must be positive")
        return cls(
            connection=connection,
            query_timeout=query_timeout,
            signal_wait_seconds=signal_wait_seconds,
            signal_journal=str(raw.get("signal_journal") or ""),
            log_level=log_level,
            max_pending_queries=max_pending,
            dispatch_queue_size=dispatch_size,
            completed_delivery_cache=completed_cache,
            env_file=env_path,
        )


__all__ = ["DEFAULT_ENV_FILE", "LocalRuntimeConfig", "PROJECT_ROOT"]
