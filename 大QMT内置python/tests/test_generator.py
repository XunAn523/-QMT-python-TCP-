import ast
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = ROOT.parent / ".env.example"
SPEC = importlib.util.spec_from_file_location("qmt_local_generator", ROOT / "tools" / "generate_helpers.py")
generator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generator)


HELPER_SETTINGS = {
    "ENABLE_TRADING": False,
    "ENABLE_CANCEL_ORDER": False,
    "MAX_COMMANDS_PER_TICK": 8,
    "MAX_QUERIES_PER_TICK": 1,
    "COMMAND_BUDGET_MS": 35.0,
    "COMMAND_INTERVAL_MS": 50,
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
    "STRATEGY_NAME": "xuanling",
    "DEFAULT_REMARK": "local_signal",
    "PASSORDER_QUICK_TRADE": 2,
    "QMT_ORDER_TYPE_DEFAULT": 1101,
    "QMT_USER_ORDER_ID_MAX_LENGTH": 23,
}


class GeneratorTest(unittest.TestCase):
    def config(self, root, accounts=None, install_root=None):
        document = {
            "helper_install_root": install_root or r"C:\Quant\QmtLocalBridge\helpers",
            "helper_settings": dict(HELPER_SETTINGS),
            "accounts": accounts or [{
                "name": "local_qmt",
                "account_id": "LIVE_ACCOUNT_001",
                "account_name": "local_qmt",
                "account_type": "STOCK",
                "runtime_dir": r"C:\Quant\QmtLocalBridge\runtime\local_qmt",
            }],
        }
        path = Path(root) / "qmt.json"
        path.write_text(json.dumps(document, ensure_ascii=True), encoding="ascii")
        return path

    def test_01_root_example_is_single_account_and_fail_closed(self):
        with self.assertRaisesRegex(generator.ConfigError, "--allow-example"):
            generator.load_project_deployment(ENV_EXAMPLE, ignore_process_env=True)
        deployment = generator.load_project_deployment(
            ENV_EXAMPLE, allow_example=True, ignore_process_env=True
        )
        accounts = deployment["qmt_config"]["accounts"]
        self.assertEqual(len(accounts), 1)
        self.assertEqual(deployment["qmt_config"]["helper_settings"]["COMMAND_INTERVAL_MS"], 50)
        self.assertFalse(deployment["qmt_config"]["helper_settings"]["ENABLE_TRADING"])

    def test_02_generated_files_are_ascii_python36_and_pinned(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-generator-") as tmp:
            output = Path(tmp) / "build"
            manifest = generator.generate(self.config(tmp), output)
            helper = output / "local_qmt" / "bigqmt_file_queue_helper.py"
            loader = output / "local_qmt" / "bigqmt_loader.py"
            helper_data, loader_data = helper.read_bytes(), loader.read_bytes()
            ast.parse(helper_data.decode("ascii"), feature_version=(3, 6))
            ast.parse(loader_data.decode("ascii"), feature_version=(3, 6))
            digest = hashlib.sha256(helper_data).hexdigest()
            self.assertEqual(manifest["accounts"][0]["helper_sha256"], digest)
            self.assertIn(('EXPECTED_SHA256 = "%s"' % digest).encode("ascii"), loader_data)
            self.assertEqual(manifest["build_id"], generator.EXPECTED_BUILD_ID)
            helper_source = helper_data.decode("ascii")
            config_block = helper_source.split(
                "# XUANLING_HELPER_CONFIG_END", 1
            )[0]
            self.assertNotIn("os.environ.get", helper_source)
            self.assertNotIn("os.getenv(", helper_source)
            self.assertIn("COMMAND_INTERVAL_MS = 50", config_block)
            self.assertIn("QUERY_INTERVAL_MS = 500", config_block)
            self.assertIn("COMMAND_BUDGET_MS = 35.0", config_block)
            self.assertIn("ENABLE_TRADING = False", config_block)

    def test_03_loader_exports_callbacks_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-loader-") as tmp:
            output = Path(tmp) / "build"
            generator.generate(self.config(tmp), output)
            helper = output / "local_qmt" / "bigqmt_file_queue_helper.py"
            loader = output / "local_qmt" / "bigqmt_loader.py"
            source = loader.read_text(encoding="ascii").replace(
                r"C:\\Quant\\QmtLocalBridge\\helpers\\local_qmt\\bigqmt_file_queue_helper.py",
                str(helper).replace("\\", "\\\\"),
            )
            namespace = {}
            with patch("builtins.print"):
                exec(compile(source, "loader.py", "exec"), namespace, namespace)
            for name in ("init", "handlebar", "order_callback", "deal_callback", "orderError_callback"):
                self.assertTrue(callable(namespace.get(name)))
            helper.write_bytes(helper.read_bytes() + b"\n# tampered\n")
            with self.assertRaisesRegex(RuntimeError, "sha256 mismatch"):
                namespace = {}
                exec(compile(source, "loader.py", "exec"), namespace, namespace)

    def test_04_check_mode_detects_stale_output(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-check-") as tmp:
            output = Path(tmp) / "build"
            config = self.config(tmp)
            generator.generate(config, output)
            generator.generate(config, output, check=True)
            loader = output / "local_qmt" / "bigqmt_loader.py"
            loader.write_bytes(loader.read_bytes() + b"# stale\n")
            with self.assertRaisesRegex(generator.ConfigError, "stale"):
                generator.generate(config, output, check=True)

    def test_05_root_example_generate_and_check_require_allow(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-example-") as tmp:
            output = Path(tmp) / "build"
            with self.assertRaisesRegex(generator.ConfigError, "--allow-example"):
                generator.generate_from_env(ENV_EXAMPLE, output, ignore_process_env=True)
            generator.generate_from_env(
                ENV_EXAMPLE, output, allow_example=True, ignore_process_env=True
            )
            generator.generate_from_env(
                ENV_EXAMPLE, output, check=True, allow_example=True,
                ignore_process_env=True,
            )

    def test_06_unc_install_and_runtime_paths_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-unc-") as tmp:
            with self.assertRaisesRegex(generator.ConfigError, "UNC/device"):
                generator.load_config(self.config(tmp, install_root=r"\\server\share\helpers"))
            accounts = [{
                "name": "local_qmt", "account_id": "LIVE_LOCAL",
                "account_name": "local_qmt", "account_type": "STOCK",
                "runtime_dir": r"\\server\share\runtime\local_qmt",
            }]
            with self.assertRaisesRegex(generator.ConfigError, "UNC/device"):
                generator.load_config(self.config(tmp, accounts=accounts))

    def test_07_single_account_ascii_and_fixed_baseline_are_enforced(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-invalid-") as tmp:
            account = {
                "name": "local_qmt", "account_id": "LIVE_LOCAL",
                "account_name": "local_qmt", "account_type": "STOCK",
                "runtime_dir": r"C:\Quant\QmtLocalBridge\runtime\local_qmt",
            }
            with self.assertRaisesRegex(generator.ConfigError, "exactly one account"):
                generator.load_config(self.config(tmp, accounts=[account, dict(account)]))
            account["account_name"] = "non-ascii-account-" + chr(0x8D26) + chr(0x6237)
            with self.assertRaisesRegex(generator.ConfigError, "printable ASCII"):
                generator.load_config(self.config(tmp, accounts=[account]))
            account["account_name"] = "local_qmt"
            path = self.config(tmp, accounts=[account])
            document = json.loads(path.read_text(encoding="ascii"))
            document["helper_settings"]["COMMAND_INTERVAL_MS"] = 100
            path.write_text(json.dumps(document, ensure_ascii=True), encoding="ascii")
            with self.assertRaisesRegex(generator.ConfigError, "must remain 50"):
                generator.load_config(path)
            with self.assertRaisesRegex(generator.ConfigError, "must not overlap"):
                generator.generate_from_env(
                    ENV_EXAMPLE,
                    Path(r"C:\Quant\QmtLocalBridge\helpers"),
                    allow_example=True,
                    ignore_process_env=True,
                )


if __name__ == "__main__":
    unittest.main()
