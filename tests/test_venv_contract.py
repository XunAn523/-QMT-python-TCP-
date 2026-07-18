import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import preflight


class VenvDeploymentContractTest(unittest.TestCase):
    def runtime_patches(self, *, prefix, executable, base_prefix=r"C:\Python312"):
        return (
            mock.patch.object(preflight.platform, "system", return_value="Windows"),
            mock.patch.object(preflight.sys, "implementation", SimpleNamespace(name="cpython")),
            mock.patch.object(preflight.sys, "version_info", (3, 12, 0)),
            mock.patch.object(preflight.sys, "prefix", prefix),
            mock.patch.object(preflight.sys, "base_prefix", base_prefix),
            mock.patch.object(preflight.sys, "executable", executable),
            mock.patch.object(preflight.struct, "calcsize", return_value=8),
        )

    def test_deployment_rejects_system_or_foreign_python(self):
        expected_executable = str(ROOT / ".venv" / "Scripts" / "python.exe")
        cases = (
            (r"C:\Python312", r"C:\Python312\python.exe", r"C:\Python312"),
            (
                r"C:\OtherProject\.venv",
                r"C:\OtherProject\.venv\Scripts\python.exe",
                r"C:\Python312",
            ),
        )
        for prefix, executable, base_prefix in cases:
            with self.subTest(prefix=prefix):
                patches = self.runtime_patches(
                    prefix=prefix,
                    executable=executable,
                    base_prefix=base_prefix,
                )
                with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                    with self.assertRaisesRegex(
                        preflight.PreflightError,
                        r"project's \.venv",
                    ):
                        preflight.validate_deployment({"accounts": []}, Path("unused.json"))
        self.assertTrue(expected_executable.endswith(r".venv\Scripts\python.exe"))

    def test_deployment_accepts_only_project_venv_before_live_checks(self):
        expected_prefix = str(ROOT / ".venv")
        expected_executable = str(ROOT / ".venv" / "Scripts" / "python.exe")
        config = {"accounts": [{"runtime_dir": r"C:\Quant\runtime", "tcp_port": 9550}]}

        class Probe:
            def bind(self, address):
                self.address = address

            def close(self):
                return None

        patches = self.runtime_patches(
            prefix=expected_prefix,
            executable=expected_executable,
        )
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4],
            patches[5],
            patches[6],
            mock.patch.object(preflight, "validate_local_path", return_value=Path(r"C:\Quant\runtime")),
            mock.patch.object(preflight, "validate_storage"),
            mock.patch.object(preflight.socket, "socket", return_value=Probe()),
            mock.patch.object(preflight, "validate_live_helper") as live_helper,
        ):
            preflight.validate_deployment(config, Path("gateway.json"))
        live_helper.assert_called_once_with(Path("gateway.json"))


if __name__ == "__main__":
    unittest.main()
