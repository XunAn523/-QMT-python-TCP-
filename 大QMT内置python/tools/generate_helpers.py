# -*- coding: utf-8 -*-
"""Generate deterministic single-account Big QMT helper and loader files."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import math
import ntpath
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
HELPER_TEMPLATE = ROOT / "src" / "bigqmt_file_queue_helper.py"
LOADER_TEMPLATE = ROOT / "src" / "bigqmt_loader.py"
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
PROJECT_ENV_RESOLVER = PROJECT_ROOT / "tools" / "project_env.py"
HELPER_CONFIG_MARKERS = ("# XUANLING_HELPER_CONFIG_START", "# XUANLING_HELPER_CONFIG_END")
LOADER_CONFIG_MARKERS = ("# BIGQMT_LOADER_CONFIG_START", "# BIGQMT_LOADER_CONFIG_END")
EXPECTED_BUILD_ID = "xuanling_bigqmt_file_queue_helper_20260716_low_latency_v4_identity_guard"
REQUIRED_HELPER_MARKERS = (
    "COMMAND_INTERVAL_MS = 50",
    "QUERY_INTERVAL_MS = 500",
    "READINESS_INTERVAL_MS = 100",
    "COMMAND_BUDGET_MS = 35.0",
    "def _atomic_write_json(path, payload):",
    "def _write_request_guard(request_id, state, request, response=None):",
    "def _request_identity_error(request):",
    "def call_passorder(args, context):",
    "def call_cancel(payload, context):",
    "def query_snapshot():",
    "def order_callback(ContextInfo, orderInfo):",
    "def deal_callback(ContextInfo, dealInfo):",
    "def orderError_callback(ContextInfo, orderArgs, errMsg):",
    "def bigqmt_command_timer(ContextInfo):",
    "def bigqmt_query_timer(ContextInfo):",
    "def bigqmt_readiness_timer(ContextInfo):",
    "def bigqmt_reconcile_timer(ContextInfo):",
)
FORBIDDEN_HELPER_IMPORTS = ("import socket", "import threading", "import tornado", "http.server")
FORBIDDEN_GENERATED_ENV_READS = ("os.environ.get", "os.getenv(", "os.environ[")
SAFE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,47}$")
SAFE_TEXT = re.compile(r"^[\x20-\x7e]+$")
LOCAL_DRIVE = re.compile(r"^[A-Za-z]:$")
EXAMPLE_ACCOUNT_ID = re.compile(r"^(?:0000955[0-4]|REPLACE_.*)$", re.IGNORECASE)
DEMO_TOKEN = re.compile(r"(^|[_-])demo($|[_-])", re.IGNORECASE)
HELPER_SETTING_NAMES = {
    "ENABLE_TRADING", "ENABLE_CANCEL_ORDER", "MAX_COMMANDS_PER_TICK",
    "MAX_QUERIES_PER_TICK", "COMMAND_BUDGET_MS", "COMMAND_INTERVAL_MS",
    "QUERY_INTERVAL_MS", "RECONCILE_INTERVAL_SECONDS",
    "MAINTENANCE_INTERVAL_SECONDS", "HEARTBEAT_INTERVAL_SECONDS",
    "READINESS_INTERVAL_MS", "ALLOW_QMT_QUERY_DURING_TRADING",
    "REQUEST_GUARD_TTL_SECONDS", "MAX_FILE_AGE_SECONDS",
    "MAX_CLEANUP_FILES_PER_TICK", "LOW_PRIORITY_QUIET_SECONDS",
    "ENABLE_RUN_TIME_TIMER", "STRATEGY_NAME", "DEFAULT_REMARK",
    "PASSORDER_QUICK_TRADE", "QMT_ORDER_TYPE_DEFAULT",
    "QMT_USER_ORDER_ID_MAX_LENGTH",
}
BOOL_HELPER_SETTINGS = {
    "ENABLE_TRADING", "ENABLE_CANCEL_ORDER", "ALLOW_QMT_QUERY_DURING_TRADING",
    "ENABLE_RUN_TIME_TIMER",
}
INT_HELPER_SETTINGS = {
    "MAX_COMMANDS_PER_TICK", "MAX_QUERIES_PER_TICK", "COMMAND_INTERVAL_MS",
    "QUERY_INTERVAL_MS", "RECONCILE_INTERVAL_SECONDS",
    "MAINTENANCE_INTERVAL_SECONDS", "HEARTBEAT_INTERVAL_SECONDS",
    "READINESS_INTERVAL_MS", "MAX_CLEANUP_FILES_PER_TICK",
    "PASSORDER_QUICK_TRADE", "QMT_ORDER_TYPE_DEFAULT",
    "QMT_USER_ORDER_ID_MAX_LENGTH",
}
FLOAT_HELPER_SETTINGS = {
    "COMMAND_BUDGET_MS", "REQUEST_GUARD_TTL_SECONDS", "MAX_FILE_AGE_SECONDS",
    "LOW_PRIORITY_QUIET_SECONDS",
}
TEXT_HELPER_SETTINGS = {"STRATEGY_NAME", "DEFAULT_REMARK"}
FIXED_HELPER_SETTINGS = {
    "MAX_COMMANDS_PER_TICK": 8, "MAX_QUERIES_PER_TICK": 1,
    "COMMAND_BUDGET_MS": 35.0, "COMMAND_INTERVAL_MS": 50,
    "QUERY_INTERVAL_MS": 500, "RECONCILE_INTERVAL_SECONDS": 30,
    "MAINTENANCE_INTERVAL_SECONDS": 60, "HEARTBEAT_INTERVAL_SECONDS": 1,
    "READINESS_INTERVAL_MS": 100, "ALLOW_QMT_QUERY_DURING_TRADING": False,
    "REQUEST_GUARD_TTL_SECONDS": 604800.0, "MAX_FILE_AGE_SECONDS": 86400.0,
    "MAX_CLEANUP_FILES_PER_TICK": 100, "LOW_PRIORITY_QUIET_SECONDS": 1.0,
    "ENABLE_RUN_TIME_TIMER": True, "PASSORDER_QUICK_TRADE": 2,
    "QMT_ORDER_TYPE_DEFAULT": 1101, "QMT_USER_ORDER_ID_MAX_LENGTH": 23,
}


class ConfigError(ValueError):
    pass


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _ascii_bytes(text: str, label: str) -> bytes:
    try:
        return text.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ConfigError("%s must remain ASCII-only: %s" % (label, exc)) from exc


def _python_literal(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _setting_literal(value: Any, label: str) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigError("%s must be finite" % label)
        return repr(value)
    if isinstance(value, str) and value and value.isascii():
        return _python_literal(value)
    raise ConfigError("%s must be a bool, number, or non-empty ASCII string" % label)


def _extract_block(text: str, markers: Tuple[str, str]) -> str:
    start, end = markers
    try:
        start_index = text.index(start)
        end_index = text.index(end, start_index) + len(end)
    except ValueError as exc:
        raise ConfigError("template is missing config markers: %s / %s" % markers) from exc
    return text[start_index:end_index]


def _replace_assignments(text: str, markers: Tuple[str, str], replacements: Mapping[str, str]) -> str:
    block = _extract_block(text, markers)
    tree = ast.parse(block, filename="config block")
    spans = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id in replacements:
            if target.id in spans:
                raise ConfigError("duplicate template assignment: %s" % target.id)
            spans[target.id] = (node.lineno, node.end_lineno or node.lineno)
    missing = sorted(set(replacements) - set(spans))
    if missing:
        raise ConfigError("template assignments not found: %s" % ", ".join(missing))
    by_start = {start: (name, end) for name, (start, end) in spans.items()}
    rendered = []
    lines = block.splitlines()
    number = 1
    while number <= len(lines):
        if number in by_start:
            name, end = by_start[number]
            rendered.append("%s = %s" % (name, replacements[name]))
            number = end + 1
            continue
        rendered.append(lines[number - 1])
        number += 1
    return text.replace(block, "\n".join(rendered), 1)


def _validate_python36(text: str, label: str) -> None:
    try:
        tree = ast.parse(text, filename=label, feature_version=(3, 6))
    except SyntaxError as exc:
        raise ConfigError("%s is not Python 3.6 compatible: %s" % (label, exc)) from exc
    forbidden = (ast.JoinedStr, ast.AnnAssign, ast.NamedExpr)
    if any(isinstance(node, forbidden) for node in ast.walk(tree)):
        raise ConfigError("%s uses syntax outside the Big QMT Python 3.6 subset" % label)


def validate_templates(helper_text: str, loader_text: str) -> None:
    missing = [marker for marker in REQUIRED_HELPER_MARKERS if marker not in helper_text]
    if missing:
        raise ConfigError("helper template lost low-latency contract: %s" % missing)
    forbidden = [marker for marker in FORBIDDEN_HELPER_IMPORTS if marker in helper_text]
    if forbidden:
        raise ConfigError("helper must not host network/thread services: %s" % forbidden)
    if 'BUILD_ID = "%s"' % EXPECTED_BUILD_ID not in helper_text:
        raise ConfigError("unexpected helper build id")
    _extract_block(helper_text, HELPER_CONFIG_MARKERS)
    _extract_block(loader_text, LOADER_CONFIG_MARKERS)
    _validate_python36(helper_text, "helper template")
    _validate_python36(loader_text, "loader template")
    _ascii_bytes(helper_text, "helper template")
    _ascii_bytes(loader_text, "loader template")


def _required_ascii(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("%s must be a non-empty string" % label)
    value = value.strip()
    if not SAFE_TEXT.fullmatch(value):
        raise ConfigError("%s must contain printable ASCII only" % label)
    return value


def _local_windows_path(value: str, label: str) -> str:
    normalized = ntpath.normpath(value)
    drive, tail = ntpath.splitdrive(normalized)
    if normalized.startswith("\\\\") or not LOCAL_DRIVE.fullmatch(drive) or not tail.startswith("\\"):
        raise ConfigError("%s must be an absolute local drive path; UNC/device paths are forbidden" % label)
    if any(character in tail for character in '<>:"|?*'):
        raise ConfigError("%s contains a Windows-forbidden path character" % label)
    if tail == "\\":
        raise ConfigError("%s must not be a drive root" % label)
    return normalized


def _is_example_account(account: Mapping[str, str]) -> bool:
    return bool(
        EXAMPLE_ACCOUNT_ID.fullmatch(account["account_id"])
        or DEMO_TOKEN.search(account["name"])
        or DEMO_TOKEN.search(account["account_name"])
    )


def _validate_account(raw: Any, index: int) -> Dict[str, str]:
    label = "accounts[%d]" % index
    if not isinstance(raw, dict):
        raise ConfigError("%s must be an object" % label)
    name = _required_ascii(raw.get("name"), label + ".name")
    if not SAFE_NAME.fullmatch(name):
        raise ConfigError("%s.name must match %s" % (label, SAFE_NAME.pattern))
    account_id = _required_ascii(raw.get("account_id"), label + ".account_id")
    account_name = _required_ascii(raw.get("account_name"), label + ".account_name")
    account_type = _required_ascii(raw.get("account_type"), label + ".account_type").upper()
    if account_type not in ("STOCK", "CREDIT"):
        raise ConfigError("%s.account_type must be STOCK or CREDIT" % label)
    runtime_dir = _required_ascii(raw.get("runtime_dir"), label + ".runtime_dir")
    return {
        "name": name,
        "account_id": account_id,
        "account_name": account_name,
        "account_type": account_type,
        "runtime_dir": _local_windows_path(runtime_dir, label + ".runtime_dir"),
    }


def _validate_helper_settings(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != HELPER_SETTING_NAMES:
        actual = set(raw) if isinstance(raw, dict) else set()
        raise ConfigError(
            "helper_settings keys mismatch: missing=%s extra=%s"
            % (sorted(HELPER_SETTING_NAMES - actual), sorted(actual - HELPER_SETTING_NAMES))
        )
    settings = dict(raw)
    for name, value in settings.items():
        _setting_literal(value, "helper_settings.%s" % name)
    for name in BOOL_HELPER_SETTINGS:
        if type(settings[name]) is not bool:
            raise ConfigError("helper_settings.%s must be boolean" % name)
    for name in INT_HELPER_SETTINGS:
        if type(settings[name]) is not int:
            raise ConfigError("helper_settings.%s must be an integer" % name)
    for name in FLOAT_HELPER_SETTINGS:
        if type(settings[name]) is not float:
            raise ConfigError("helper_settings.%s must be a float" % name)
    for name in TEXT_HELPER_SETTINGS:
        if not isinstance(settings[name], str):
            raise ConfigError("helper_settings.%s must be text" % name)
    for name, expected in FIXED_HELPER_SETTINGS.items():
        if settings[name] != expected:
            raise ConfigError("helper_settings.%s must remain %r" % (name, expected))
    return settings


def load_config(
    path: Path,
    allow_example: bool = False,
) -> Tuple[str, List[Dict[str, str]], Dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise ConfigError("cannot read config %s: %s" % (path, exc)) from exc
    if not isinstance(raw, dict):
        raise ConfigError("config root must be an object")
    install_root = _local_windows_path(
        _required_ascii(raw.get("helper_install_root"), "helper_install_root"),
        "helper_install_root",
    )
    helper_settings = _validate_helper_settings(raw.get("helper_settings"))
    accounts_raw = raw.get("accounts")
    if not isinstance(accounts_raw, list) or len(accounts_raw) != 1:
        raise ConfigError("single-machine qmt_config must contain exactly one account")
    accounts = [_validate_account(item, index) for index, item in enumerate(accounts_raw)]
    for field in ("name", "account_id", "runtime_dir"):
        values = [item[field].lower() for item in accounts]
        if len(values) != len(set(values)):
            raise ConfigError("duplicate account %s" % field)
    examples = [item["name"] for item in accounts if _is_example_account(item)]
    if examples and not allow_example:
        raise ConfigError(
            "example/demo account placeholders require explicit --allow-example: %s"
            % ", ".join(examples)
        )
    return install_root, accounts, helper_settings


def render_helper(
    template: str,
    account: Mapping[str, str],
    helper_settings: Mapping[str, Any],
) -> str:
    replacements = {
        "HELPER_NAME": _python_literal(account["name"]),
        "ACCOUNT_ID": _python_literal(account["account_id"]),
        "ACCOUNT_NAME": _python_literal(account["account_name"]),
        "ACCOUNT_TYPE": _python_literal(account["account_type"]),
        "RUNTIME_DIR": _python_literal(account["runtime_dir"]),
    }
    replacements.update({
        name: _setting_literal(value, "helper_settings.%s" % name)
        for name, value in helper_settings.items()
    })
    rendered = _replace_assignments(
        template,
        HELPER_CONFIG_MARKERS,
        replacements,
    )
    leaked_reads = [marker for marker in FORBIDDEN_GENERATED_ENV_READS if marker in rendered]
    if leaked_reads:
        raise ConfigError("generated helper must not read process environment: %s" % leaked_reads)
    _validate_python36(rendered, "generated helper %s" % account["name"])
    _ascii_bytes(rendered, "generated helper %s" % account["name"])
    return rendered


def render_loader(template: str, account: Mapping[str, str], installed_helper_path: str, helper_sha256: str) -> str:
    rendered = _replace_assignments(
        template,
        LOADER_CONFIG_MARKERS,
        {
            "HELPER_PATH": _python_literal(installed_helper_path),
            "EXPECTED_HELPER_NAME": _python_literal(account["name"]),
            "EXPECTED_ACCOUNT_ID": _python_literal(account["account_id"]),
            "EXPECTED_BUILD_ID": _python_literal(EXPECTED_BUILD_ID),
            "EXPECTED_SHA256": _python_literal(helper_sha256),
        },
    )
    _validate_python36(rendered, "generated loader %s" % account["name"])
    _ascii_bytes(rendered, "generated loader %s" % account["name"])
    return rendered


def _atomic_write(path: Path, data: bytes) -> None:
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


def _check_or_write(path: Path, data: bytes, check: bool) -> None:
    if check:
        try:
            actual = path.read_bytes()
        except OSError as exc:
            raise ConfigError("generated file missing: %s (%s)" % (path, exc)) from exc
        if actual != data:
            raise ConfigError("generated file is stale: %s" % path)
        return
    _atomic_write(path, data)


def generate(config_path: Path, output_root: Path, check: bool = False, allow_example: bool = False) -> Dict[str, Any]:
    helper_template = _read_text(HELPER_TEMPLATE)
    loader_template = _read_text(LOADER_TEMPLATE)
    validate_templates(helper_template, loader_template)
    install_root, accounts, helper_settings = load_config(config_path, allow_example=allow_example)
    manifest_accounts = []
    for account in accounts:
        helper_text = render_helper(helper_template, account, helper_settings)
        helper_bytes = _ascii_bytes(helper_text, "generated helper")
        helper_sha256 = hashlib.sha256(helper_bytes).hexdigest()
        installed_path = ntpath.join(install_root, account["name"], "bigqmt_file_queue_helper.py")
        loader_text = render_loader(loader_template, account, installed_path, helper_sha256)
        loader_bytes = _ascii_bytes(loader_text, "generated loader")
        account_root = output_root / account["name"]
        _check_or_write(account_root / "bigqmt_file_queue_helper.py", helper_bytes, check)
        _check_or_write(account_root / "bigqmt_loader.py", loader_bytes, check)
        manifest_accounts.append(
            {
                "name": account["name"],
                "account_id": account["account_id"],
                "account_type": account["account_type"],
                "runtime_dir": account["runtime_dir"],
                "installed_helper_path": installed_path,
                "helper_sha256": helper_sha256,
                "loader_sha256": hashlib.sha256(loader_bytes).hexdigest(),
            }
        )
    manifest = {
        "format_version": 1,
        "build_id": EXPECTED_BUILD_ID,
        "protocol_version": 2,
        "helper_template_sha256": hashlib.sha256(_ascii_bytes(helper_template, "helper template")).hexdigest(),
        "helper_settings_sha256": hashlib.sha256(
            json.dumps(helper_settings, ensure_ascii=True, sort_keys=True).encode("ascii")
        ).hexdigest(),
        "accounts": manifest_accounts,
    }
    manifest_bytes = (json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode("ascii")
    _check_or_write(output_root / "manifest.json", manifest_bytes, check)
    return manifest


def load_project_deployment(
    env_file: Path,
    allow_example: bool = False,
    ignore_process_env: bool = False,
) -> Dict[str, Any]:
    module_path = PROJECT_ENV_RESOLVER
    if not module_path.is_file():
        raise ConfigError("project env resolver not found: %s" % module_path)
    spec = importlib.util.spec_from_file_location("qmt_local_project_env", module_path)
    if spec is None or spec.loader is None:
        raise ConfigError("cannot load project env resolver: %s" % module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        return module.load_deployment(
            env_file.resolve(),
            allow_example=allow_example,
            environ={} if ignore_process_env else None,
        )
    except (OSError, ValueError) as exc:
        message = "project env rejected: %s" % exc
        reason = str(exc).lower()
        if not allow_example and (
            "placeholder" in reason
            or (
                env_file.name.lower() == ".env.example"
                and "account" in reason
            )
        ):
            message += "; use --allow-example only for offline example validation"
        raise ConfigError(message) from exc


def generate_from_env(
    env_file: Path,
    output_root: Path | None = None,
    check: bool = False,
    allow_example: bool = False,
    ignore_process_env: bool = False,
) -> Dict[str, Any]:
    deployment = load_project_deployment(env_file, allow_example, ignore_process_env)
    destination = output_root or Path(deployment["helper_output_dir"])
    output_key = ntpath.normcase(ntpath.normpath(str(destination.resolve()))).rstrip("\\")
    install_key = ntpath.normcase(
        ntpath.normpath(str(deployment["helper_install_root"]))
    ).rstrip("\\")
    if (
        output_key == install_key
        or output_key.startswith(install_key + "\\")
        or install_key.startswith(output_key + "\\")
    ):
        raise ConfigError("helper output and install roots must not overlap")
    with tempfile.TemporaryDirectory(prefix="bigqmt-env-config-") as temporary:
        config_path = Path(temporary) / "qmt.json"
        config_path.write_text(
            json.dumps(deployment["qmt_config"], ensure_ascii=True, sort_keys=True),
            encoding="ascii",
        )
        return generate(config_path, destination, check, allow_example)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the local Big QMT embedded-Python helper and loader.")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE, help="project root .env")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--allow-example", action="store_true")
    parser.add_argument("--ignore-process-env", action="store_true", help="offline fixture validation only")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        manifest = generate_from_env(
            args.env_file.resolve(),
            args.output.resolve() if args.output else None,
            check=args.check,
            allow_example=args.allow_example,
            ignore_process_env=args.ignore_process_env,
        )
        deployment = load_project_deployment(
            args.env_file.resolve(), args.allow_example, args.ignore_process_env
        )
        destination = args.output.resolve() if args.output else Path(deployment["helper_output_dir"])
    except ConfigError as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return 2
    verb = "verified" if args.check else "generated"
    print("%s %d account helper(s) in %s; build=%s" % (
        verb, len(manifest["accounts"]), destination, manifest["build_id"]
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
