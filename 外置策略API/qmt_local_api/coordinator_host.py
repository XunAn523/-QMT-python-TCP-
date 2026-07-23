"""Process host and strict configuration loader for the account Coordinator.

The Coordinator itself is a library.  This module gives the Windows launcher a
single long-running process that owns it, exposes only an IPv4-loopback
strategy endpoint, and provides a separately authenticated local control pipe
for status and graceful shutdown.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
from multiprocessing.connection import Client, Listener
import ntpath
import os
from pathlib import Path
import secrets
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


if __package__ in (None, ""):
    _API_ROOT = Path(__file__).resolve().parents[1]
    if str(_API_ROOT) not in sys.path:
        sys.path.insert(0, str(_API_ROOT))
    from qmt_local_api.api import LocalQmtApi
    from qmt_local_api.coordinator import AccountCoordinator, RiskLimits
    from qmt_local_api.coordinator_server import CoordinatorLocalServer
else:
    from .api import LocalQmtApi
    from .coordinator import AccountCoordinator, RiskLimits
    from .coordinator_server import CoordinatorLocalServer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_VERSION = 1
MAX_CONTROL_TOKEN_BYTES = 128


class CoordinatorHostConfigError(ValueError):
    """The external Coordinator configuration is invalid or unsafe."""


@dataclass(frozen=True)
class WorkerConfig:
    strategy_id: str
    enabled: bool
    program: Path
    arguments: tuple[str, ...]
    working_directory: Path


@dataclass(frozen=True)
class StrategyConfig:
    strategy_id: str
    auth_token: str
    enabled: bool
    priority: int
    limits: RiskLimits
    worker: WorkerConfig


@dataclass(frozen=True)
class CoordinatorHostConfig:
    path: Path
    server_host: str
    server_port: int
    max_clients: int
    state_db: Path
    account_limits: RiskLimits
    strategies: tuple[StrategyConfig, ...]

    def launch_plan(self) -> Dict[str, Any]:
        """Return a token-free plan consumed by the PowerShell launcher."""
        return {
            "version": CONFIG_VERSION,
            "server": {
                "host": self.server_host,
                "port": self.server_port,
                "max_clients": self.max_clients,
            },
            "state_db": str(self.state_db),
            "workers": [
                {
                    "strategy_id": item.worker.strategy_id,
                    "enabled": item.worker.enabled,
                    "program": str(item.worker.program),
                    "arguments": list(item.worker.arguments),
                    "working_directory": str(item.worker.working_directory),
                }
                for item in self.strategies
            ],
        }


def _json_load(path: Path) -> Dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CoordinatorHostConfigError("Coordinator config cannot be read") from exc
    try:
        decoded = json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CoordinatorHostConfigError("Coordinator config must be finite UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise CoordinatorHostConfigError("Coordinator config root must be an object")
    return decoded


def _keys(value: Mapping[str, Any], allowed: Iterable[str], label: str) -> None:
    extras = sorted(set(value) - set(allowed))
    if extras:
        raise CoordinatorHostConfigError("%s contains unsupported keys" % label)


def _required_object(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise CoordinatorHostConfigError("%s must be an object" % label)
    return dict(value)


def _required_list(value: Any, label: str) -> List[Any]:
    if not isinstance(value, list):
        raise CoordinatorHostConfigError("%s must be an array" % label)
    return list(value)


def _finite_non_negative(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CoordinatorHostConfigError("%s must be a finite non-negative number" % label) from exc
    if not math.isfinite(number) or number < 0:
        raise CoordinatorHostConfigError("%s must be a finite non-negative number" % label)
    return number


def _strict_bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise CoordinatorHostConfigError("%s must be true or false" % label)
    return bool(value)


def _root_path(project_root: Path, raw: Any, label: str, *, suffix: str = "") -> Path:
    if not isinstance(raw, (str, Path)):
        raise CoordinatorHostConfigError("%s must be a path string" % label)
    value = str(raw or "").strip()
    if not value:
        raise CoordinatorHostConfigError("%s is required" % label)
    normalized = value.replace("/", "\\")
    drive, _ = ntpath.splitdrive(normalized)
    if normalized.startswith("\\\\") or drive or normalized.startswith("\\") or ":" in normalized:
        raise CoordinatorHostConfigError("%s must be a project-root relative local path" % label)
    candidate = (project_root / Path(normalized)).resolve(strict=False)
    root = project_root.resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise CoordinatorHostConfigError("%s escaped project root" % label) from exc
    if suffix and candidate.suffix.lower() != suffix.lower():
        raise CoordinatorHostConfigError("%s must have %s suffix" % (label, suffix))
    return candidate


def _limits(value: Any, label: str) -> RiskLimits:
    raw = _required_object(value, label)
    _keys(raw, ("max_order_notional", "max_pending_notional"), label)
    if set(raw) != {"max_order_notional", "max_pending_notional"}:
        raise CoordinatorHostConfigError("%s requires both notional limits" % label)
    return RiskLimits(
        max_order_notional=_finite_non_negative(raw["max_order_notional"], label + ".max_order_notional"),
        max_pending_notional=_finite_non_negative(raw["max_pending_notional"], label + ".max_pending_notional"),
    ).validate()


def _worker(project_root: Path, strategy_id: str, value: Any) -> WorkerConfig:
    raw = _required_object(value, "strategy worker")
    _keys(raw, ("enabled", "program", "arguments", "working_directory"), "strategy worker")
    if set(raw) != {"enabled", "program", "arguments", "working_directory"}:
        raise CoordinatorHostConfigError("strategy worker requires enabled/program/arguments/working_directory")
    enabled = _strict_bool(raw["enabled"], "strategy worker.enabled")
    program = _root_path(project_root, raw["program"], "strategy worker.program", suffix=".py")
    working_directory = _root_path(project_root, raw["working_directory"], "strategy worker.working_directory")
    arguments = _required_list(raw["arguments"], "strategy worker.arguments")
    if not all(
        isinstance(item, str) and all(character not in item for character in ('\x00', '"', '\r', '\n'))
        for item in arguments
    ):
        raise CoordinatorHostConfigError(
            "strategy worker.arguments must be strings without NUL, quotes, or line breaks"
        )
    return WorkerConfig(
        strategy_id=strategy_id,
        enabled=enabled,
        program=program,
        arguments=tuple(arguments),
        working_directory=working_directory,
    )


def load_coordinator_config(
    config_path: str | Path,
    *,
    project_root: str | Path = PROJECT_ROOT,
) -> CoordinatorHostConfig:
    """Load the separate strategy configuration without reading ``.env``."""
    root = Path(project_root).expanduser().resolve()
    try:
        relative_config = os.path.relpath(Path(config_path).expanduser().resolve(), root)
    except ValueError as exc:
        raise CoordinatorHostConfigError("Coordinator config must remain on the project drive") from exc
    path = _root_path(root, relative_config, "Coordinator config")
    raw = _json_load(path)
    _keys(raw, ("version", "server", "state_db", "account_limits", "strategies"), "Coordinator config")
    if set(raw) != {"version", "server", "state_db", "account_limits", "strategies"}:
        raise CoordinatorHostConfigError("Coordinator config has missing required keys")
    if type(raw["version"]) is not int or raw["version"] != CONFIG_VERSION:
        raise CoordinatorHostConfigError("Coordinator config version must remain 1")

    server = _required_object(raw["server"], "server")
    _keys(server, ("host", "port", "max_clients"), "server")
    if set(server) != {"host", "port", "max_clients"}:
        raise CoordinatorHostConfigError("server requires host/port/max_clients")
    if server["host"] != "127.0.0.1":
        raise CoordinatorHostConfigError("Coordinator server host must be exactly 127.0.0.1")
    if type(server["port"]) is not int or not 1 <= int(server["port"]) <= 65535:
        raise CoordinatorHostConfigError("Coordinator server port must be in 1..65535")
    if type(server["max_clients"]) is not int or not 1 <= int(server["max_clients"]) <= 32:
        raise CoordinatorHostConfigError("Coordinator max_clients must be in 1..32")

    strategies: List[StrategyConfig] = []
    strategy_ids = set()
    for index, item in enumerate(_required_list(raw["strategies"], "strategies")):
        strategy = _required_object(item, "strategies[%d]" % index)
        _keys(strategy, ("strategy_id", "auth_token", "enabled", "priority", "limits", "worker"), "strategy")
        if set(strategy) != {"strategy_id", "auth_token", "enabled", "priority", "limits", "worker"}:
            raise CoordinatorHostConfigError("strategy has missing required keys")
        strategy_id = AccountCoordinator._validate_strategy_id(str(strategy["strategy_id"] or ""))
        if strategy_id in strategy_ids:
            raise CoordinatorHostConfigError("strategy_id must be unique")
        strategy_ids.add(strategy_id)
        auth_token = str(strategy["auth_token"] or "")
        if len(auth_token) < 16 or "\x00" in auth_token:
            raise CoordinatorHostConfigError("strategy auth_token must contain at least 16 non-NUL characters")
        upper_token = auth_token.upper()
        if any(marker in upper_token for marker in ("REPLACE", "PLACEHOLDER", "CHANGE_ME", "QMT_LOCAL_AUTH_TOKEN")):
            raise CoordinatorHostConfigError("strategy auth_token still looks like placeholder text")
        if type(strategy["enabled"]) is not bool:
            raise CoordinatorHostConfigError("strategy.enabled must be true or false")
        if type(strategy["priority"]) is not int or not -1000000 <= strategy["priority"] <= 1000000:
            raise CoordinatorHostConfigError("strategy.priority must be an integer in range")
        strategies.append(
            StrategyConfig(
                strategy_id=strategy_id,
                auth_token=auth_token,
                enabled=bool(strategy["enabled"]),
                priority=int(strategy["priority"]),
                limits=_limits(strategy["limits"], "strategy limits"),
                worker=_worker(root, strategy_id, strategy["worker"]),
            )
        )
    if not strategies:
        raise CoordinatorHostConfigError("at least one strategy is required")

    return CoordinatorHostConfig(
        path=path,
        server_host="127.0.0.1",
        server_port=int(server["port"]),
        max_clients=int(server["max_clients"]),
        state_db=_root_path(root, raw["state_db"], "state_db", suffix=".sqlite3"),
        account_limits=_limits(raw["account_limits"], "account_limits"),
        strategies=tuple(strategies),
    )


def _atomic_json(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".%s.%s.tmp" % (path.name, secrets.token_hex(8)))
    try:
        temporary.write_text(
            json.dumps(dict(data), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False),
            encoding="utf-8",
        )
        os.replace(str(temporary), str(path))
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_control_token(path: Path) -> bytes:
    try:
        token = path.read_bytes()
    except OSError as exc:
        raise CoordinatorHostConfigError("Coordinator control token cannot be read") from exc
    if not 16 <= len(token) <= MAX_CONTROL_TOKEN_BYTES:
        raise CoordinatorHostConfigError("Coordinator control token length is invalid")
    return token


class CoordinatorControlPipe:
    """Authenticated local named-pipe control for status and graceful stop."""

    def __init__(self, pipe_name: str, authkey: bytes, status: Any) -> None:
        if not pipe_name or "\\" in pipe_name or "/" in pipe_name:
            raise CoordinatorHostConfigError("control pipe name is invalid")
        self.address = "\\\\.\\pipe\\" + pipe_name
        self.authkey = authkey
        self.status = status
        self.shutdown_requested = threading.Event()
        self._listener: Optional[Listener] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._listener = Listener(self.address, family="AF_PIPE", authkey=self.authkey)
        self._thread = threading.Thread(target=self._run, name="qmt-coordinator-control", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self.shutdown_requested.is_set():
            try:
                connection = listener.accept()
            except (OSError, EOFError):
                return
            try:
                message = connection.recv()
                command = str((message or {}).get("command") or "").upper() if isinstance(message, dict) else ""
                if command == "STATUS":
                    connection.send({"ok": True, "status": self.status()})
                elif command == "SHUTDOWN":
                    self.shutdown_requested.set()
                    connection.send({"ok": True, "stopping": True})
                else:
                    connection.send({"ok": False, "code": "UNSUPPORTED_CONTROL"})
            except (OSError, EOFError, ValueError):
                pass
            finally:
                try:
                    connection.close()
                except (OSError, UnboundLocalError):
                    pass

    def close(self) -> None:
        listener = self._listener
        self._listener = None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass


def send_control(pipe_name: str, token_file: str | Path, command: str) -> Dict[str, Any]:
    """Invoke the Host's named pipe without putting secrets on the command line."""
    token = _read_control_token(Path(token_file).expanduser().resolve())
    address = "\\\\.\\pipe\\" + str(pipe_name)
    connection = Client(address, family="AF_PIPE", authkey=token)
    try:
        connection.send({"command": str(command or "").upper()})
        response = connection.recv()
    finally:
        connection.close()
    if not isinstance(response, dict):
        raise CoordinatorHostConfigError("Coordinator control response is invalid")
    return dict(response)


def _public_status(coordinator: AccountCoordinator, config: CoordinatorHostConfig, pipe_name: str) -> Dict[str, Any]:
    status = coordinator.account_status()
    return {
        "version": CONFIG_VERSION,
        "pid": os.getpid(),
        "coordinator_ready": not coordinator.trading_halted,
        "gateway_connected": bool(status.get("gateway_connected")),
        "trading_halted": bool(status.get("trading_halted")),
        "pending_notional": float(status.get("pending_notional") or 0.0),
        "coordinator_endpoint": "%s:%d" % (config.server_host, config.server_port),
        "control_pipe": pipe_name,
        "updated_at": time.time(),
    }


def run_host(
    env_file: str | Path,
    config_path: str | Path,
    status_file: str | Path,
    control_pipe: str,
    control_token_file: str | Path,
) -> int:
    """Run one Coordinator process until Ctrl+C or authenticated SHUTDOWN."""
    config = load_coordinator_config(config_path)
    try:
        relative_status = os.path.relpath(Path(status_file).expanduser().resolve(), PROJECT_ROOT)
    except ValueError as exc:
        raise CoordinatorHostConfigError("status_file must remain on the project drive") from exc
    status_path = _root_path(PROJECT_ROOT, relative_status, "status_file", suffix=".json")
    token = _read_control_token(Path(control_token_file).expanduser().resolve())
    coordinator: Optional[AccountCoordinator] = None
    server: Optional[CoordinatorLocalServer] = None
    control: Optional[CoordinatorControlPipe] = None
    try:
        api = LocalQmtApi.from_env(env_file)
        coordinator = AccountCoordinator(api, config.state_db, account_limits=config.account_limits)
        for strategy in config.strategies:
            coordinator.register_strategy(
                strategy.strategy_id,
                strategy.auth_token,
                enabled=strategy.enabled,
                priority=strategy.priority,
                limits=strategy.limits,
            )
        if not coordinator.start():
            _atomic_json(status_path, {
                "version": CONFIG_VERSION,
                "pid": os.getpid(),
                "coordinator_ready": False,
                "reason": "gateway_or_account_not_ready",
                "updated_at": time.time(),
            })
            return 2
        server = CoordinatorLocalServer(
            coordinator,
            host=config.server_host,
            port=config.server_port,
            max_clients=config.max_clients,
        )
        server.start()
        control = CoordinatorControlPipe(
            control_pipe,
            token,
            lambda: _public_status(coordinator, config, control_pipe),
        )
        control.start()
        _atomic_json(status_path, _public_status(coordinator, config, control_pipe))
        while not control.shutdown_requested.wait(0.5):
            _atomic_json(status_path, _public_status(coordinator, config, control_pipe))
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        _atomic_json(status_path, {
            "version": CONFIG_VERSION,
            "pid": os.getpid(),
            "coordinator_ready": False,
            "reason": type(exc).__name__,
            "updated_at": time.time(),
        })
        return 1
    finally:
        if control is not None:
            control.close()
        if server is not None:
            server.stop()
        if coordinator is not None:
            coordinator.stop()
        if status_path.exists():
            try:
                state = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                state = {"version": CONFIG_VERSION}
            state["coordinator_ready"] = False
            state["stopped_at"] = time.time()
            _atomic_json(status_path, state)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the one-account local QMT Coordinator host")
    parser.add_argument("--env-file")
    parser.add_argument("--config", required=True)
    parser.add_argument("--status-file")
    parser.add_argument("--control-pipe")
    parser.add_argument("--control-token-file")
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--send-control", choices=("STATUS", "SHUTDOWN"))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.check_config:
            print(json.dumps(load_coordinator_config(args.config).launch_plan(), ensure_ascii=False, sort_keys=True))
            return 0
        if args.send_control:
            if not args.control_pipe or not args.control_token_file:
                raise CoordinatorHostConfigError("control pipe and token file are required")
            print(json.dumps(send_control(args.control_pipe, args.control_token_file, args.send_control), ensure_ascii=False, sort_keys=True))
            return 0
        if not args.env_file or not args.status_file or not args.control_pipe or not args.control_token_file:
            raise CoordinatorHostConfigError("env-file/status-file/control-pipe/control-token-file are required")
        return run_host(
            args.env_file,
            args.config,
            args.status_file,
            args.control_pipe,
            args.control_token_file,
        )
    except CoordinatorHostConfigError as exc:
        print("Coordinator host configuration rejected: %s" % exc, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONFIG_VERSION",
    "CoordinatorHostConfig",
    "CoordinatorHostConfigError",
    "StrategyConfig",
    "WorkerConfig",
    "load_coordinator_config",
    "run_host",
    "send_control",
]
