import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_AUTH_TOKEN = "0" * 64
TEST_AUTH_TOKEN = "a" * 64
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import project_env


class ProjectEnvTest(unittest.TestCase):
    def setUp(self):
        self.example = ROOT / ".env.example"
        self.temp_dir = Path(tempfile.mkdtemp(prefix="qmt-local-env-test-"))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def write_env(self, replacements=None, append=""):
        text = self.example.read_text(encoding="utf-8")
        for old, new in (replacements or {}).items():
            text = text.replace(old, new)
        path = self.temp_dir / ".env"
        path.write_text(text + append, encoding="utf-8")
        return path

    def production_values(self):
        return {
            "QMT_LOCAL_ACCOUNT_ENABLED": "true",
            "QMT_LOCAL_ACCOUNT_ID": "TEST_ACCOUNT_9550",
            "QMT_LOCAL_AUTH_TOKEN": TEST_AUTH_TOKEN,
        }

    def test_example_is_single_account_loopback_and_fail_closed(self):
        deployment = project_env.load_deployment(
            self.example, allow_example=True, environ={}
        )
        self.assertFalse(deployment["account_enabled"])
        self.assertEqual(deployment["bind_host"], "127.0.0.1")
        self.assertEqual(deployment["tcp_port"], 9550)
        self.assertEqual(len(deployment["gateway_config"]["accounts"]), 1)
        self.assertFalse(deployment["qmt_config"]["helper_settings"]["ENABLE_TRADING"])
        self.assertFalse(deployment["qmt_config"]["helper_settings"]["ENABLE_CANCEL_ORDER"])
        with self.assertRaisesRegex(project_env.EnvConfigError, "ENABLED must be true"):
            project_env.load_deployment(self.example, environ={})

    def test_file_is_authoritative_and_explicit_injection_is_validated(self):
        previous = os.environ.get("QMT_LOCAL_TCP_PORT")
        try:
            os.environ["QMT_LOCAL_TCP_PORT"] = "10550"
            deployment = project_env.load_deployment(
                self.example, allow_example=True
            )
            self.assertEqual(deployment["tcp_port"], 9550)
        finally:
            if previous is None:
                os.environ.pop("QMT_LOCAL_TCP_PORT", None)
            else:
                os.environ["QMT_LOCAL_TCP_PORT"] = previous
        with self.assertRaisesRegex(project_env.EnvConfigError, "placeholder"):
            project_env.load_deployment(
                self.example,
                environ={
                    "QMT_LOCAL_ACCOUNT_ENABLED": "true",
                    "QMT_LOCAL_AUTH_TOKEN": TEST_AUTH_TOKEN,
                },
            )
        deployment = project_env.load_deployment(
            self.example, environ=self.production_values()
        )
        account = deployment["gateway_config"]["accounts"][0]
        self.assertEqual(account["account_id"], "TEST_ACCOUNT_9550")
        self.assertEqual(account["tcp_host"], "127.0.0.1")

    def test_non_loopback_bad_port_unc_and_unknown_key_are_rejected(self):
        cases = [
            ({"QMT_LOCAL_BIND_HOST=127.0.0.1": "QMT_LOCAL_BIND_HOST=0.0.0.0"}, "127.0.0.1"),
            ({"QMT_LOCAL_TCP_PORT=9550": "QMT_LOCAL_TCP_PORT=70000"}, "1..65535"),
            ({r"QMT_LOCAL_RUNTIME_ROOT=C:\Quant\QmtLocalBridge\runtime": r"QMT_LOCAL_RUNTIME_ROOT=\\server\share"}, "UNC"),
            ({r"QMT_LOCAL_RUNTIME_ROOT=C:\Quant\QmtLocalBridge\runtime": r"QMT_LOCAL_RUNTIME_ROOT=C:\Quant\本机桥\runtime"}, "printable ASCII"),
            ({r"QMT_LOCAL_RUNTIME_ROOT=C:\Quant\QmtLocalBridge\runtime": r"QMT_LOCAL_RUNTIME_ROOT=C:\Quant\runtime:ads"}, "forbidden path"),
            ({r"QMT_LOCAL_HELPER_OUTPUT_DIR=C:\Quant\QmtLocalBridge\helper-build": r"QMT_LOCAL_HELPER_OUTPUT_DIR=C:\Quant\QmtLocalBridge\helpers"}, "non-nested"),
        ]
        for replacements, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(project_env.EnvConfigError, message):
                    project_env.load_deployment(
                        self.write_env(replacements), allow_example=True, environ={}
                    )
        with self.assertRaisesRegex(project_env.EnvConfigError, "unknown/invalid key"):
            project_env.load_deployment(
                self.write_env(append="QMT_LOCAL_UNKNOWN=value\n"),
                allow_example=True,
                environ={},
            )
        with self.assertRaisesRegex(project_env.EnvConfigError, "ACCOUNT_NAME must match"):
            project_env.load_deployment(
                self.write_env({"QMT_LOCAL_ACCOUNT_NAME=account_main": "QMT_LOCAL_ACCOUNT_NAME=.."}),
                allow_example=True,
                environ={},
            )
        with self.assertRaisesRegex(project_env.EnvConfigError, "printable ASCII"):
            project_env.load_deployment(
                self.example,
                allow_example=True,
                environ={"QMT_LOCAL_ACCOUNT_ID": "LIVE\nINJECT"},
            )

    def test_export_duplicate_and_bad_boolean_are_rejected(self):
        with self.assertRaisesRegex(project_env.EnvConfigError, "must not use export"):
            project_env.load_deployment(
                self.write_env({"QMT_LOCAL_PYTHON_EXE": "export QMT_LOCAL_PYTHON_EXE"}),
                allow_example=True,
                environ={},
            )
        with self.assertRaisesRegex(project_env.EnvConfigError, "duplicate"):
            project_env.load_deployment(
                self.write_env(append="QMT_LOCAL_TCP_PORT=9551\n"),
                allow_example=True,
                environ={},
            )
        with self.assertRaisesRegex(project_env.EnvConfigError, "true/false"):
            project_env.load_deployment(
                self.write_env({"QMT_LOCAL_ACCOUNT_ENABLED=false": "QMT_LOCAL_ACCOUNT_ENABLED=maybe"}),
                allow_example=True,
                environ={},
            )
        with self.assertRaisesRegex(project_env.EnvConfigError, "inline comments"):
            project_env.load_deployment(
                self.write_env({"QMT_LOCAL_TCP_PORT=9550": "QMT_LOCAL_TCP_PORT=9550 # local"}),
                allow_example=True,
                environ={},
            )

    def test_performance_and_protocol_baselines_are_not_env_keys(self):
        deployment = project_env.load_deployment(
            self.example, allow_example=True, environ={}
        )
        gateway = deployment["gateway_config"]
        api = deployment["api_config"]
        helper = deployment["qmt_config"]["helper_settings"]
        self.assertEqual(gateway["max_frame_bytes"], 10 * 1024 * 1024)
        self.assertEqual(gateway["response_watch_interval_seconds"], 0.01)
        self.assertEqual(gateway["event_watch_interval_seconds"], 0.01)
        self.assertEqual(api["recv_timeout"], 0.2)
        self.assertEqual(api["dispatch_queue_size"], 1024)
        self.assertEqual(helper["COMMAND_INTERVAL_MS"], 50)
        self.assertEqual(helper["COMMAND_BUDGET_MS"], 35.0)
        keys = project_env.parse_env_file(self.example)
        self.assertFalse(any("TIMEOUT" in key or "QUEUE_SIZE" in key for key in keys))
        self.assertEqual(deployment["api_config"]["auth_token"], EXAMPLE_AUTH_TOKEN)
        self.assertNotIn(EXAMPLE_AUTH_TOKEN, json.dumps(gateway))
        self.assertRegex(gateway["auth_token_sha256"], r"^[0-9a-f]{64}$")

    def test_materialization_is_deterministic_and_summary_redacts_account_id(self):
        summary = project_env.materialize(
            self.example,
            allow_example=True,
            output_dir=self.temp_dir / "generated",
            environ={},
        )
        self.assertNotIn("REPLACE_WITH_QMT_ACCOUNT_ID", json.dumps(summary))
        self.assertNotIn(EXAMPLE_AUTH_TOKEN, json.dumps(summary))
        project_env.materialize(
            self.example,
            allow_example=True,
            output_dir=self.temp_dir / "generated",
            check=True,
            environ={},
        )
        self.assertTrue(Path(summary["gateway_config_path"]).is_file())
        self.assertTrue(Path(summary["qmt_config_path"]).is_file())


if __name__ == "__main__":
    unittest.main()
