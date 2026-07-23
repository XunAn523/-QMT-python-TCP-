"""Strict configuration and local-control tests for the Coordinator Host."""

from __future__ import annotations

import json
from pathlib import Path
import secrets
import sys
import tempfile
import time
import unittest


API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api.coordinator_host import (
    CoordinatorControlPipe,
    CoordinatorHostConfigError,
    load_coordinator_config,
    send_control,
)


def _configuration(**overrides):
    config = {
        "version": 1,
        "server": {"host": "127.0.0.1", "port": 9560, "max_clients": 4},
        "state_db": "runtime\\coordinator_state.sqlite3",
        "account_limits": {
            "max_order_notional": 1000000.0,
            "max_pending_notional": 1000000.0,
        },
        "strategies": [{
            "strategy_id": "alpha",
            "auth_token": "alpha-local-token-0001",
            "enabled": True,
            "priority": 100,
            "limits": {
                "max_order_notional": 100000.0,
                "max_pending_notional": 200000.0,
            },
            "worker": {
                "enabled": True,
                "program": "strategies\\alpha.py",
                "arguments": ["--mode", "paper"],
                "working_directory": ".",
            },
        }],
    }
    config.update(overrides)
    return config


class CoordinatorHostConfigTests(unittest.TestCase):
    def _write_config(self, root: Path, content):
        path = root / "coordinator_config.json"
        path.write_text(json.dumps(content), encoding="utf-8")
        return path

    def test_valid_config_has_resolved_worker_paths_and_never_exports_auth_tokens(self):
        with tempfile.TemporaryDirectory(prefix="coordinator-host-") as temporary:
            root = Path(temporary)
            config = load_coordinator_config(self._write_config(root, _configuration()), project_root=root)
            self.assertEqual(config.server_host, "127.0.0.1")
            self.assertEqual(config.strategies[0].worker.program, root / "strategies" / "alpha.py")
            self.assertEqual(config.strategies[0].worker.working_directory, root)
            self.assertNotIn("auth_token", json.dumps(config.launch_plan()))
            self.assertNotIn("alpha-local-token-0001", json.dumps(config.launch_plan()))

    def test_unsafe_listener_path_tokens_and_duplicates_are_rejected(self):
        cases = []
        remote = _configuration()
        remote["server"]["host"] = "0.0.0.0"
        cases.append(remote)
        escaped = _configuration()
        escaped["strategies"][0]["worker"]["program"] = "..\\outside.py"
        cases.append(escaped)
        alternate_stream = _configuration()
        alternate_stream["state_db"] = "runtime\\coordinator.sqlite3:alternate"
        cases.append(alternate_stream)
        placeholder = _configuration()
        placeholder["strategies"][0]["auth_token"] = "REPLACE_WITH_SECRET_12345"
        cases.append(placeholder)
        quoted_argument = _configuration()
        quoted_argument["strategies"][0]["worker"]["arguments"] = ['"unsafe"']
        cases.append(quoted_argument)
        duplicate = _configuration()
        duplicate["strategies"].append(dict(duplicate["strategies"][0]))
        cases.append(duplicate)

        with tempfile.TemporaryDirectory(prefix="coordinator-host-invalid-") as temporary:
            root = Path(temporary)
            for index, content in enumerate(cases):
                with self.subTest(index=index):
                    with self.assertRaises(CoordinatorHostConfigError):
                        load_coordinator_config(self._write_config(root, content), project_root=root)

    def test_authenticated_control_pipe_allows_status_and_shutdown(self):
        with tempfile.TemporaryDirectory(prefix="coordinator-control-") as temporary:
            token_path = Path(temporary) / "control.token"
            token_path.write_bytes(secrets.token_bytes(32))
            pipe_name = "qmt-coordinator-test-" + secrets.token_hex(8)
            pipe = CoordinatorControlPipe(pipe_name, token_path.read_bytes(), lambda: {"ready": True})
            pipe.start()
            try:
                self.assertEqual(send_control(pipe_name, token_path, "STATUS"), {"ok": True, "status": {"ready": True}})
                self.assertEqual(send_control(pipe_name, token_path, "SHUTDOWN"), {"ok": True, "stopping": True})
                self.assertTrue(pipe.shutdown_requested.wait(1.0))
            finally:
                pipe.close()


if __name__ == "__main__":
    unittest.main()
