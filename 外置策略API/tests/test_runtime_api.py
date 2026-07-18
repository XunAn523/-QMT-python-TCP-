from pathlib import Path
import sys
import tempfile
import unittest

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api import (
    EXPECTED_GATEWAY_BUILD_ID,
    LocalQmtApi,
    LocalRuntimeConfig,
    redact_for_output,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
TEST_AUTH_TOKEN = "a" * 64


class RuntimeApiTests(unittest.TestCase):
    def test_root_example_resolves_offline_but_live_api_is_fail_closed(self):
        runtime = LocalRuntimeConfig.load(
            ENV_EXAMPLE,
            allow_example=True,
            environ={},
        )
        self.assertEqual(runtime.connection.host, "127.0.0.1")
        self.assertEqual(runtime.connection.local_host, "127.0.0.1")
        self.assertEqual(runtime.connection.port, 9550)
        self.assertEqual(runtime.connection.account_name, "account_main")
        self.assertEqual(runtime.connection.account_type, "STOCK")
        self.assertEqual(runtime.connection.auth_token, "0" * 64)
        self.assertNotIn("0" * 64, repr(runtime.connection))
        self.assertEqual(
            runtime.connection.expected_gateway_build_id,
            EXPECTED_GATEWAY_BUILD_ID,
        )
        self.assertEqual(runtime.query_timeout, 6.0)
        self.assertEqual(runtime.max_pending_queries, 128)
        self.assertEqual(runtime.dispatch_queue_size, 1024)
        self.assertEqual(runtime.completed_delivery_cache, 8192)
        with self.assertRaisesRegex(ValueError, "ENABLED"):
            LocalQmtApi.from_env(ENV_EXAMPLE)

    def test_public_api_loads_only_through_root_resolver_contract(self):
        with tempfile.TemporaryDirectory(prefix="qmt-local-api-env-") as temporary:
            text = ENV_EXAMPLE.read_text(encoding="utf-8")
            text = text.replace("QMT_LOCAL_ACCOUNT_ENABLED=false", "QMT_LOCAL_ACCOUNT_ENABLED=true")
            text = text.replace(
                "QMT_LOCAL_ACCOUNT_ID=REPLACE_WITH_QMT_ACCOUNT_ID",
                "QMT_LOCAL_ACCOUNT_ID=TEST_ACCOUNT",
            )
            text = text.replace("QMT_LOCAL_AUTH_TOKEN=" + ("0" * 64), "QMT_LOCAL_AUTH_TOKEN=" + TEST_AUTH_TOKEN)
            env_file = Path(temporary) / ".env"
            env_file.write_text(text, encoding="utf-8")
            api = LocalQmtApi.from_env(env_file)
            self.assertEqual(api.config.account_id, "TEST_ACCOUNT")
            self.assertEqual(api.config.account_name, "account_main")
            self.assertEqual(api.config.auth_token, TEST_AUTH_TOKEN)
            self.assertEqual(api.default_query_timeout, 6.0)
            self.assertIsNotNone(api.runtime)

    def test_console_redaction_is_recursive_and_non_mutating(self):
        source = {
            "account_id": "SECRET",
            "nested": [{"m_strAccountID": "SECRET2", "ok": 1}],
            "writer_token": "TOKEN",
            "auth_token": TEST_AUTH_TOKEN,
            "raw": {"account_name": "REAL"},
        }
        redacted = redact_for_output(source)
        self.assertNotIn("SECRET", repr(redacted))
        self.assertNotIn("TOKEN", repr(redacted))
        self.assertNotIn(TEST_AUTH_TOKEN, repr(redacted))
        self.assertEqual(redacted["nested"][0]["ok"], 1)
        self.assertEqual(source["account_id"], "SECRET")

    def test_runtime_rejects_missing_or_invalid_auth_token(self):
        for token in ("", "g" * 64, "a" * 63):
            with self.subTest(length=len(token)):
                with self.assertRaisesRegex(ValueError, "AUTH_TOKEN|auth_token"):
                    LocalRuntimeConfig.load(
                        ENV_EXAMPLE,
                        allow_example=True,
                        environ={"QMT_LOCAL_AUTH_TOKEN": token},
                    )

    def test_api_directory_has_no_private_env_template(self):
        api_root = Path(__file__).resolve().parents[1]
        self.assertFalse((api_root / ".env").exists())
        self.assertFalse((api_root / ".env.example").exists())
        self.assertTrue(ENV_EXAMPLE.is_file())


if __name__ == "__main__":
    unittest.main()
