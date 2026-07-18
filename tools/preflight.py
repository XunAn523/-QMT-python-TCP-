#!/usr/bin/env python3
"""Fail-closed validation for the one-machine Windows bridge."""

from __future__ import annotations

import argparse
import json
import ntpath
import os
import platform
import socket
import sqlite3
import struct
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Sequence

from project_env import (
    DEFAULT_ENV_FILE,
    GATEWAY_BUILD_ID,
    HELPER_BUILD_ID,
    EnvConfigError,
    materialize,
)


ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DIR = ROOT / (chr(0x7F51) + chr(0x5173))


class PreflightError(RuntimeError):
    """Raised when the local bridge is unsafe to start."""


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise PreflightError("JSON root must be an object: %s" % path)
    return value


def validate_config(config_path: Path, allow_example: bool = False) -> Dict[str, Any]:
    config = _load_json(config_path)
    errors = []
    fixed = {
        "listen_backlog": 16,
        "max_frame_bytes": 10 * 1024 * 1024,
        "tcp_idle_timeout_seconds": 60.0,
        "response_watch_interval_seconds": 0.01,
        "event_watch_interval_seconds": 0.01,
        "maintenance_interval_seconds": 60.0,
        "query_concurrency": 1,
        "expected_helper_build_id": HELPER_BUILD_ID,
        "expected_protocol_version": 2,
        "expected_command_interval_ms": 25,
    }
    for key, expected in fixed.items():
        if config.get(key) != expected:
            errors.append("%s must remain %r" % (key, expected))
    accounts = config.get("accounts")
    if not isinstance(accounts, list) or len(accounts) != 1:
        errors.append("exactly one account is required")
        accounts = []
    for account in accounts:
        if not str(account.get("name") or "").strip():
            errors.append("account name is required")
        account_id = str(account.get("account_id") or "").strip()
        if not account_id:
            errors.append("account_id is required")
        if not allow_example and account_id.startswith("REPLACE_"):
            errors.append("account_id is still an example placeholder")
        if str(account.get("account_type") or "").upper() not in ("STOCK", "CREDIT"):
            errors.append("account_type must be STOCK or CREDIT")
        if account.get("tcp_host") != "127.0.0.1":
            errors.append("gateway must listen on 127.0.0.1 only")
        try:
            port = int(account.get("tcp_port"))
        except (TypeError, ValueError):
            port = 0
        if not 1 <= port <= 65535:
            errors.append("tcp_port must be in 1..65535")
        account_fixed = {
            "poll_interval_seconds": 30.0,
            "request_timeout_seconds": 8.0,
            "query_timeout_seconds": 6.0,
            "trade_enqueue_timeout_seconds": 1.0,
            "heartbeat_stale_seconds": 2.5,
        }
        for key, expected in account_fixed.items():
            if account.get(key) != expected:
                errors.append("%s must remain %r" % (key, expected))
        validate_local_path(str(account.get("runtime_dir") or ""), "runtime_dir")
    if errors:
        raise PreflightError("configuration rejected:\n- " + "\n- ".join(errors))
    return config


def validate_local_path(value: str, label: str) -> Path:
    raw = str(value or "").strip().replace("/", "\\")
    drive, tail = ntpath.splitdrive(raw)
    if raw.startswith("\\\\") or len(drive) != 2 or drive[1] != ":" or not tail.startswith("\\"):
        raise PreflightError("%s must be an absolute local drive path, not UNC: %s" % (label, raw))
    return Path(raw)


def validate_gateway_import() -> None:
    if str(GATEWAY_DIR) not in sys.path:
        sys.path.insert(0, str(GATEWAY_DIR))
    from bigqmt_gateway_proxy import PROXY_BUILD_ID  # pylint: disable=import-outside-toplevel

    if PROXY_BUILD_ID != GATEWAY_BUILD_ID:
        raise PreflightError(
            "gateway build mismatch: expected=%s actual=%s" % (GATEWAY_BUILD_ID, PROXY_BUILD_ID)
        )


def validate_storage(runtime_dir: Path) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    temporary = runtime_dir / (".local-qmt-preflight-%s.tmp" % token)
    committed = runtime_dir / (".local-qmt-preflight-%s.ok" % token)
    database = runtime_dir / (".local-qmt-preflight-%s.sqlite3" % token)
    connection = None
    try:
        temporary.write_text("atomic-replace-check\n", encoding="utf-8")
        os.replace(str(temporary), str(committed))
        if committed.read_text(encoding="utf-8") != "atomic-replace-check\n":
            raise PreflightError("atomic replace returned different content")
        connection = sqlite3.connect(str(database), timeout=2.0)
        journal = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        connection.execute("PRAGMA synchronous=NORMAL")
        synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
        connection.execute("CREATE TABLE preflight(value INTEGER NOT NULL)")
        connection.execute("INSERT INTO preflight(value) VALUES (1)")
        connection.commit()
        if str(journal).lower() != "wal" or int(synchronous) != 1:
            raise PreflightError("runtime storage cannot provide SQLite WAL/NORMAL")
    except PreflightError:
        raise
    except Exception as exc:
        raise PreflightError("runtime storage validation failed: %s" % exc) from exc
    finally:
        if connection is not None:
            connection.close()
        for path in (
            temporary,
            committed,
            database,
            Path(str(database) + "-wal"),
            Path(str(database) + "-shm"),
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def validate_live_helper(config_path: Path) -> None:
    if str(GATEWAY_DIR) not in sys.path:
        sys.path.insert(0, str(GATEWAY_DIR))
    from bigqmt_gateway_proxy import (  # pylint: disable=import-outside-toplevel
        FileQueueHelperClient,
        helper_identity_mismatches,
        load_config,
        setup_logging,
    )

    account = load_config(config_path).accounts[0]
    health = FileQueueHelperClient(account, setup_logging(None)).health_sync()
    failures = helper_identity_mismatches(account, health)
    if failures:
        raise PreflightError("Big QMT helper identity rejected: %s" % ", ".join(failures))
    if not health.get("ready"):
        raise PreflightError(
            "Big QMT helper is not fresh/ready: state=%s heartbeat_age=%s readiness_age_ms=%s"
            % (health.get("state"), health.get("heartbeat_age_seconds"), health.get("readiness_age_ms"))
        )


def validate_deployment(config: Dict[str, Any], config_path: Path) -> None:
    if (
        platform.system() != "Windows"
        or sys.implementation.name != "cpython"
        or sys.version_info[:2] != (3, 12)
        or struct.calcsize("P") * 8 != 64
    ):
        raise PreflightError(
            "deployment requires Windows CPython 3.12 x64; current=%s %s %s-bit"
            % (platform.system(), platform.python_version(), struct.calcsize("P") * 8)
        )
    expected_prefix = os.path.normcase(os.path.realpath(str(ROOT / ".venv")))
    expected_executable = os.path.normcase(
        os.path.realpath(str(ROOT / ".venv" / "Scripts" / "python.exe"))
    )
    actual_prefix = os.path.normcase(os.path.realpath(sys.prefix))
    actual_executable = os.path.normcase(os.path.realpath(sys.executable))
    if (
        sys.prefix == sys.base_prefix
        or actual_prefix != expected_prefix
        or actual_executable != expected_executable
    ):
        raise PreflightError(
            "deployment must run with this project's .venv\\Scripts\\python.exe"
        )
    account = config["accounts"][0]
    runtime_dir = validate_local_path(account["runtime_dir"], "runtime_dir")
    validate_storage(runtime_dir)
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", int(account["tcp_port"])))
    except OSError as exc:
        raise PreflightError("cannot bind local gateway port; stop the old gateway: %s" % exc) from exc
    finally:
        probe.close()
    validate_live_helper(config_path)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the local Big QMT bridge")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--allow-example", action="store_true")
    parser.add_argument("--deployment", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        if args.config:
            config_path = args.config.resolve()
        else:
            with tempfile.TemporaryDirectory(prefix="qmt-local-preflight-") as temporary:
                summary = materialize(
                    args.env_file.resolve(),
                    allow_example=args.allow_example,
                    output_dir=Path(temporary),
                    environ={} if args.allow_example else None,
                )
                config_path = Path(summary["gateway_config_path"])
                validate_gateway_import()
                config = validate_config(config_path, args.allow_example)
                if args.deployment:
                    validate_deployment(config, config_path)
                print("preflight=ok")
                print("network=127.0.0.1:%d" % config["accounts"][0]["tcp_port"])
                print("accounts=1")
                return 0
        validate_gateway_import()
        config = validate_config(config_path, args.allow_example)
        if args.deployment:
            validate_deployment(config, config_path)
        print("preflight=ok")
        print("network=127.0.0.1:%d" % config["accounts"][0]["tcp_port"])
        print("accounts=1")
        return 0
    except (OSError, ValueError, EnvConfigError, PreflightError) as exc:
        print("preflight=failed", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
