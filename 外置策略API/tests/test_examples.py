import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = PROJECT_ROOT / "示例"


def run_example(name, *args):
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [sys.executable, str(EXAMPLES / name), *args],
        cwd=str(PROJECT_ROOT),
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


class ExampleTests(unittest.TestCase):
    def test_protocol_demo_is_completely_offline(self):
        result = run_example("protocol_demo.py")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertFalse(payload["network_opened"])
        self.assertFalse(payload["env_loaded"])
        self.assertEqual(payload["frame_count"], 5)
        self.assertNotIn("REPLACE_WITH_QMT_ACCOUNT_ID", result.stdout)

    def test_online_examples_expose_help_without_loading_env_or_network(self):
        for name in (
            "query_account.py",
            "callback_client.py",
            "async_order.py",
            "async_cancel.py",
            "raw_tcp_query.py",
        ):
            with self.subTest(name=name):
                result = run_example(name, "--help")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout.lower())

    def test_order_and_cancel_default_to_offline_redacted_dry_run(self):
        if str(EXAMPLES) not in sys.path:
            sys.path.insert(0, str(EXAMPLES))
        import async_cancel
        import async_order

        cases = (
            (
                async_order,
                [
                    "async_order.py", "--symbol", "600000.SH", "--side", "BUY",
                    "--quantity", "100", "--price", "10.23",
                    "--client-order-id", "stable-test-order",
                ],
            ),
            (async_cancel, ["async_cancel.py", "--order-id", "QMT-ORDER-ID"]),
        )
        for module, argv in cases:
            with self.subTest(module=module.__name__):
                output = io.StringIO()
                with (
                    mock.patch.object(module, "ENV_FILE", PROJECT_ROOT / ".env.example"),
                    mock.patch.object(sys, "argv", argv),
                    mock.patch.dict(os.environ, {}, clear=True),
                    mock.patch(
                        "qmt_local_api.transport.socket.socket",
                        side_effect=AssertionError("dry-run opened a socket"),
                    ),
                    contextlib.redirect_stdout(output),
                ):
                    self.assertEqual(module.main(), 0)
                payload = json.loads(output.getvalue())
                self.assertTrue(payload["dry_run"])
                self.assertFalse(payload["network_opened"])
                self.assertNotIn("REPLACE_WITH_QMT_ACCOUNT_ID", output.getvalue())

    def test_live_examples_require_exact_confirmation_in_source(self):
        order_source = (EXAMPLES / "async_order.py").read_text(encoding="utf-8")
        cancel_source = (EXAMPLES / "async_cancel.py").read_text(encoding="utf-8")
        self.assertIn("I_UNDERSTAND_THIS_SENDS_A_LIVE_ORDER", order_source)
        self.assertIn("I_UNDERSTAND_THIS_SENDS_A_LIVE_CANCEL", cancel_source)
        self.assertIn("wait_delivery_acknowledged", order_source)
        self.assertIn("do not retry automatically", order_source)


if __name__ == "__main__":
    unittest.main()
