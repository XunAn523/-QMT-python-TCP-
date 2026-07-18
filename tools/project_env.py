#!/usr/bin/env python3
"""Strictly resolve the single project .env and materialize runtime configs."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import ntpath
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
GATEWAY_BUILD_ID = "xuanling_local_qmt_gateway_20260718_low_latency_v7_post_enqueue_barrier"
HELPER_BUILD_ID = "xuanling_bigqmt_file_queue_helper_20260718_low_latency_v12_fail_closed_sibling_scan"
PROTOCOL_VERSION = 2
KEY_PATTERN = re.compile(r"^QMT_LOCAL_[A-Z0-9_]+$")
LOCAL_DRIVE = re.compile(r"^[A-Za-z]:$")
ACCOUNT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,47}$")
VENV_PYTHON_RELATIVE = r".venv\Scripts\python.exe"

BASE_KEYS = {
    "QMT_LOCAL_PYTHON_EXE",
    "QMT_LOCAL_BIND_HOST",
    "QMT_LOCAL_TCP_PORT",
    "QMT_LOCAL_AUTH_TOKEN",
    "QMT_LOCAL_RUNTIME_ROOT",
    "QMT_LOCAL_LOG_DIR",
    "QMT_LOCAL_GENERATED_DIR",
    "QMT_LOCAL_HELPER_INSTALL_ROOT",
    "QMT_LOCAL_HELPER_OUTPUT_DIR",
    "QMT_LOCAL_ACCOUNT_ENABLED",
    "QMT_LOCAL_ACCOUNT_NAME",
    "QMT_LOCAL_ACCOUNT_ID",
    "QMT_LOCAL_ACCOUNT_TYPE",
    "QMT_LOCAL_HELPER_ENABLE_TRADING",
    "QMT_LOCAL_HELPER_ENABLE_CANCEL_ORDER",
    "QMT_LOCAL_HELPER_STRATEGY_NAME",
    "QMT_LOCAL_HELPER_DEFAULT_REMARK",
}
API_KEYS = {
    "QMT_LOCAL_API_SIGNAL_JOURNAL",
    "QMT_LOCAL_API_LOG_LEVEL",
}
KNOWN_KEYS = BASE_KEYS | API_KEYS

FIXED_HELPER_SETTINGS: Dict[str, Any] = {
    "MAX_COMMANDS_PER_TICK": 4,
    "MAX_QUERIES_PER_TICK": 1,
    "COMMAND_BUDGET_MS": 15.0,
    "COMMAND_INTERVAL_MS": 25,
    "QUERY_INTERVAL_MS": 500,
    "RECONCILE_INTERVAL_SECONDS": 30,
    "MAINTENANCE_INTERVAL_SECONDS": 60,
    "HEARTBEAT_INTERVAL_SECONDS": 1,
    "READINESS_INTERVAL_MS": 100,
    "ALLOW_QMT_QUERY_DURING_TRADING": False,
    "REQUEST_GUARD_TTL_SECONDS": 604800.0,
    "MAX_FILE_AGE_SECONDS": 86400.0,
    "MAX_CLEANUP_FILES_PER_TICK": 100,
    "LOW_PRIORITY_QUIET_SECONDS": 1.0,
    "ENABLE_RUN_TIME_TIMER": True,
    "PASSORDER_QUICK_TRADE": 2,
    "QMT_ORDER_TYPE_DEFAULT": 1101,
    "QMT_USER_ORDER_ID_MAX_LENGTH": 23,
}


class EnvConfigError(ValueError):
    """Raised when the deployment environment is incomplete or unsafe."""


def parse_env_file(path: Path) -> Dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except OSError as exc:
        raise EnvConfigError("cannot read project env file %s: %s" % (path, exc)) from exc
    values: Dict[str, str] = {}
    for number, original in enumerate(lines, 1):
        line = original.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            raise EnvConfigError("%s:%d must not use export" % (path, number))
        if "=" not in line:
            raise EnvConfigError("%s:%d must use KEY=VALUE" % (path, number))
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if not KEY_PATTERN.fullmatch(key) or key not in KNOWN_KEYS:
            raise EnvConfigError("%s:%d unknown/invalid key: %s" % (path, number, key))
        if key in values:
            raise EnvConfigError("%s:%d duplicate key: %s" % (path, number, key))
        quoted = len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"')
        if quoted:
            value = value[1:-1]
        elif "#" in value:
            raise EnvConfigError("%s:%d inline comments are forbidden" % (path, number))
        if any(character in value for character in ("\x00", "\r", "\n")):
            raise EnvConfigError("%s:%d contains a control character" % (path, number))
        values[key] = value
    return values


def resolve_values(path: Path, environ: Mapping[str, str] | None = None) -> Dict[str, str]:
    values = parse_env_file(path)
    # Deployment is file-authoritative.  ``environ`` is an explicit injection
    # hook for tests/embedding; ambient process variables never silently alter
    # the selected .env file.
    if environ is not None:
        unknown = sorted(
            str(key) for key in environ
            if KEY_PATTERN.fullmatch(str(key)) and str(key) not in KNOWN_KEYS
        )
        if unknown:
            raise EnvConfigError("unknown QMT_LOCAL injected environment keys: %s" % ", ".join(unknown))
        for key in KNOWN_KEYS:
            if key in environ:
                values[key] = str(environ[key])
    missing = sorted(key for key in KNOWN_KEYS if key not in values or not values[key].strip())
    if missing:
        raise EnvConfigError("missing required project env keys: %s" % ", ".join(missing))
    return values


def parse_bool(value: str, label: str) -> bool:
    normalized = value.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise EnvConfigError("%s must be 0/1 or true/false" % label)


def ascii_value(value: str, label: str) -> str:
    normalized = value.strip()
    if not normalized or any(ord(char) < 0x20 or ord(char) > 0x7E for char in normalized):
        raise EnvConfigError("%s must be non-empty printable ASCII" % label)
    return normalized


def local_windows_path(value: str, label: str) -> str:
    normalized = ntpath.normpath(value.strip())
    if not normalized or any(ord(char) < 0x20 or ord(char) > 0x7E for char in normalized):
        raise EnvConfigError("%s must use printable ASCII characters" % label)
    drive, tail = ntpath.splitdrive(normalized)
    if normalized.startswith("\\\\") or not LOCAL_DRIVE.fullmatch(drive) or not tail.startswith("\\"):
        raise EnvConfigError("%s must be an absolute local drive path; UNC/device paths are forbidden" % label)
    if any(character in tail for character in '<>:"|?*'):
        raise EnvConfigError("%s contains a Windows-forbidden path character" % label)
    if tail == "\\":
        raise EnvConfigError("%s must not be a drive root" % label)
    return normalized


def project_python_path(value: str, label: str) -> str:
    """Resolve the one supported project-local virtual-environment interpreter."""
    normalized = ntpath.normpath(value.strip())
    if ntpath.normcase(normalized) != ntpath.normcase(VENV_PYTHON_RELATIVE):
        raise EnvConfigError(
            "%s must remain %s; run setup_venv.ps1 instead of using a system Python path"
            % (label, VENV_PYTHON_RELATIVE)
        )
    return str((ROOT / ".venv" / "Scripts" / "python.exe").resolve())


def require_disjoint_directories(paths: Mapping[str, str]) -> None:
    normalized = {
        label: ntpath.normcase(ntpath.normpath(path)).rstrip("\\")
        for label, path in paths.items()
    }
    items = sorted(normalized.items())
    for index, (left_label, left) in enumerate(items):
        for right_label, right in items[index + 1:]:
            if (
                left == right
                or left.startswith(right + "\\")
                or right.startswith(left + "\\")
            ):
                raise EnvConfigError(
                    "%s and %s must be distinct, non-nested directories"
                    % (left_label, right_label)
                )


def build_configs(values: Mapping[str, str], allow_example: bool = False) -> Dict[str, Any]:
    try:
        bind_host = str(ipaddress.IPv4Address(values["QMT_LOCAL_BIND_HOST"]))
    except ValueError as exc:
        raise EnvConfigError("QMT_LOCAL_BIND_HOST must be a valid IPv4 address") from exc
    if bind_host != "127.0.0.1":
        raise EnvConfigError("QMT_LOCAL_BIND_HOST must remain 127.0.0.1 for the one-machine bridge")

    try:
        tcp_port = int(values["QMT_LOCAL_TCP_PORT"])
    except ValueError as exc:
        raise EnvConfigError("QMT_LOCAL_TCP_PORT must be an integer") from exc
    if not 1 <= tcp_port <= 65535:
        raise EnvConfigError("QMT_LOCAL_TCP_PORT must be in 1..65535")

    auth_token = ascii_value(values["QMT_LOCAL_AUTH_TOKEN"], "QMT_LOCAL_AUTH_TOKEN")
    if not re.fullmatch(r"[0-9A-Fa-f]{64}", auth_token):
        raise EnvConfigError("QMT_LOCAL_AUTH_TOKEN must be exactly 64 hexadecimal characters")
    auth_token_sha256 = hashlib.sha256(auth_token.encode("ascii")).hexdigest()

    enabled = parse_bool(values["QMT_LOCAL_ACCOUNT_ENABLED"], "QMT_LOCAL_ACCOUNT_ENABLED")
    if not enabled and not allow_example:
        raise EnvConfigError("QMT_LOCAL_ACCOUNT_ENABLED must be true for deployment")
    if not allow_example and auth_token == ("0" * 64):
        raise EnvConfigError("QMT_LOCAL_AUTH_TOKEN is the insecure example placeholder")
    account_name = ascii_value(values["QMT_LOCAL_ACCOUNT_NAME"], "QMT_LOCAL_ACCOUNT_NAME")
    if not ACCOUNT_NAME.fullmatch(account_name):
        raise EnvConfigError("QMT_LOCAL_ACCOUNT_NAME must match [A-Za-z][A-Za-z0-9_-]{0,47}")
    account_id = ascii_value(values["QMT_LOCAL_ACCOUNT_ID"], "QMT_LOCAL_ACCOUNT_ID")
    account_type = ascii_value(values["QMT_LOCAL_ACCOUNT_TYPE"], "QMT_LOCAL_ACCOUNT_TYPE").upper()
    if account_type not in ("STOCK", "CREDIT"):
        raise EnvConfigError("QMT_LOCAL_ACCOUNT_TYPE must be STOCK or CREDIT")
    if not allow_example and (
        account_id.startswith("REPLACE_") or re.fullmatch(r"0+", account_id)
    ):
        raise EnvConfigError("QMT_LOCAL_ACCOUNT_ID is an example placeholder")

    python_exe = project_python_path(
        values["QMT_LOCAL_PYTHON_EXE"], "QMT_LOCAL_PYTHON_EXE"
    )
    runtime_root = local_windows_path(values["QMT_LOCAL_RUNTIME_ROOT"], "QMT_LOCAL_RUNTIME_ROOT")
    log_dir = local_windows_path(values["QMT_LOCAL_LOG_DIR"], "QMT_LOCAL_LOG_DIR")
    generated_dir = local_windows_path(values["QMT_LOCAL_GENERATED_DIR"], "QMT_LOCAL_GENERATED_DIR")
    helper_install_root = local_windows_path(
        values["QMT_LOCAL_HELPER_INSTALL_ROOT"], "QMT_LOCAL_HELPER_INSTALL_ROOT"
    )
    helper_output_dir = local_windows_path(
        values["QMT_LOCAL_HELPER_OUTPUT_DIR"], "QMT_LOCAL_HELPER_OUTPUT_DIR"
    )
    signal_journal = local_windows_path(
        values["QMT_LOCAL_API_SIGNAL_JOURNAL"], "QMT_LOCAL_API_SIGNAL_JOURNAL"
    )
    require_disjoint_directories({
        "QMT_LOCAL_RUNTIME_ROOT": runtime_root,
        "QMT_LOCAL_LOG_DIR": log_dir,
        "QMT_LOCAL_GENERATED_DIR": generated_dir,
        "QMT_LOCAL_HELPER_INSTALL_ROOT": helper_install_root,
        "QMT_LOCAL_HELPER_OUTPUT_DIR": helper_output_dir,
        "QMT_LOCAL_API_SIGNAL_JOURNAL parent": ntpath.dirname(signal_journal),
    })
    runtime_dir = ntpath.join(runtime_root, str(tcp_port))

    helper_settings = dict(FIXED_HELPER_SETTINGS)
    helper_settings.update({
        "ENABLE_TRADING": parse_bool(
            values["QMT_LOCAL_HELPER_ENABLE_TRADING"], "QMT_LOCAL_HELPER_ENABLE_TRADING"
        ),
        "ENABLE_CANCEL_ORDER": parse_bool(
            values["QMT_LOCAL_HELPER_ENABLE_CANCEL_ORDER"],
            "QMT_LOCAL_HELPER_ENABLE_CANCEL_ORDER",
        ),
        "STRATEGY_NAME": ascii_value(
            values["QMT_LOCAL_HELPER_STRATEGY_NAME"], "QMT_LOCAL_HELPER_STRATEGY_NAME"
        ),
        "DEFAULT_REMARK": ascii_value(
            values["QMT_LOCAL_HELPER_DEFAULT_REMARK"], "QMT_LOCAL_HELPER_DEFAULT_REMARK"
        ),
    })

    account = {
        "name": account_name,
        "account_id": account_id,
        "account_type": account_type,
        "tcp_host": bind_host,
        "tcp_port": tcp_port,
        "runtime_dir": runtime_dir,
        "poll_interval_seconds": 30.0,
        "request_timeout_seconds": 8.0,
        "query_timeout_seconds": 6.0,
        "trade_enqueue_timeout_seconds": 1.0,
        "heartbeat_stale_seconds": 2.5,
    }
    gateway = {
        "auth_token_sha256": auth_token_sha256,
        "listen_backlog": 16,
        "max_frame_bytes": 10 * 1024 * 1024,
        "tcp_idle_timeout_seconds": 60.0,
        "response_watch_interval_seconds": 0.01,
        "event_watch_interval_seconds": 0.01,
        "maintenance_interval_seconds": 60.0,
        "query_concurrency": 1,
        "expected_helper_build_id": HELPER_BUILD_ID,
        "expected_protocol_version": PROTOCOL_VERSION,
        "expected_command_interval_ms": 25,
        "accounts": [account],
    }
    qmt = {
        "helper_install_root": helper_install_root,
        "helper_settings": helper_settings,
        "accounts": [{
            "name": account_name,
            "account_id": account_id,
            "account_name": account_name,
            "account_type": account_type,
            "runtime_dir": runtime_dir,
        }],
    }
    log_level = values["QMT_LOCAL_API_LOG_LEVEL"].strip().upper()
    if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        raise EnvConfigError("QMT_LOCAL_API_LOG_LEVEL is invalid")
    api = {
        "auth_token": auth_token,
        "host": bind_host,
        "local_host": bind_host,
        "port": tcp_port,
        "account_name": account_name,
        "account_id": account_id,
        "account_type": account_type,
        "protocol_version": PROTOCOL_VERSION,
        "expected_gateway_build_id": GATEWAY_BUILD_ID,
        "connect_timeout": 5.0,
        "recv_timeout": 0.2,
        "handshake_timeout": 5.0,
        "heartbeat_interval": 5.0,
        "heartbeat_timeout": 15.0,
        "auto_reconnect": True,
        "query_timeout": 6.0,
        "signal_wait_seconds": 10.0,
        "signal_journal": signal_journal,
        "log_level": log_level,
        "max_pending_queries": 128,
        "dispatch_queue_size": 1024,
        "completed_delivery_cache": 8192,
    }
    return {
        "gateway_config": gateway,
        "qmt_config": qmt,
        "api_config": api,
        "python_exe": python_exe,
        "bind_host": bind_host,
        "tcp_port": tcp_port,
        "runtime_dir": runtime_dir,
        "log_dir": log_dir,
        "generated_dir": generated_dir,
        "helper_install_root": helper_install_root,
        "helper_output_dir": helper_output_dir,
        "account_enabled": enabled,
        "account_name": account_name,
    }


def load_deployment(
    env_file: Path,
    allow_example: bool = False,
    environ: Mapping[str, str] | None = None,
) -> Dict[str, Any]:
    return build_configs(resolve_values(env_file, environ), allow_example)


def _atomic_json(path: Path, payload: Mapping[str, Any], check: bool = False) -> None:
    data = (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode("ascii")
    if check:
        if not path.is_file() or path.read_bytes() != data:
            raise EnvConfigError("generated config is missing or stale: %s" % path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".%s.%d.tmp" % (path.name, os.getpid()))
    try:
        with temporary.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(str(temporary), str(path))
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def materialize(
    env_file: Path,
    allow_example: bool = False,
    output_dir: Path | None = None,
    check: bool = False,
    environ: Mapping[str, str] | None = None,
) -> Dict[str, Any]:
    deployment = load_deployment(env_file, allow_example, environ)
    destination = output_dir or Path(deployment["generated_dir"])
    gateway_path = destination / "gateway_config.json"
    qmt_path = destination / "qmt_config.json"
    _atomic_json(gateway_path, deployment["gateway_config"], check)
    _atomic_json(qmt_path, deployment["qmt_config"], check)
    summary = deployment_summary(deployment)
    summary.update({
        "gateway_config_path": str(gateway_path.resolve()),
        "qmt_config_path": str(qmt_path.resolve()),
        "generated_dir": str(destination),
    })
    return summary


def deployment_summary(deployment: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a redacted, no-write deployment description."""
    return {
        "python_exe": deployment["python_exe"],
        "bind_host": deployment["bind_host"],
        "tcp_port": deployment["tcp_port"],
        "runtime_dir": deployment["runtime_dir"],
        "log_dir": deployment["log_dir"],
        "generated_dir": deployment["generated_dir"],
        "helper_install_root": deployment["helper_install_root"],
        "helper_output_dir": deployment["helper_output_dir"],
        "account_name": deployment["account_name"],
        "account_count": 1,
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve the one-machine Big QMT bridge .env")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--allow-example", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--describe", action="store_true", help="validate and print paths without writing JSON")
    parser.add_argument("--ignore-process-env", action="store_true", help="offline fixture validation only")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        environ = {} if args.ignore_process_env else None
        if args.describe:
            if args.check or args.output_dir:
                raise EnvConfigError("--describe cannot be combined with --check/--output-dir")
            summary = deployment_summary(load_deployment(
                args.env_file.resolve(),
                allow_example=args.allow_example,
                environ=environ,
            ))
        else:
            summary = materialize(
                args.env_file.resolve(),
                allow_example=args.allow_example,
                output_dir=args.output_dir.resolve() if args.output_dir else None,
                check=args.check,
                environ=environ,
            )
    except (OSError, EnvConfigError) as exc:
        print("env_config=failed", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
