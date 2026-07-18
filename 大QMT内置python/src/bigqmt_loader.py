#coding:gbk
"""Load one generated file-queue helper into the Big QMT strategy namespace."""

import hashlib
import os


# BIGQMT_LOADER_CONFIG_START
HELPER_PATH = r"C:\Quant\TradeBridge\helpers\account_demo\bigqmt_file_queue_helper.py"
EXPECTED_HELPER_NAME = "account_demo"
EXPECTED_ACCOUNT_ID = "00000000"
EXPECTED_BUILD_ID = "xuanling_bigqmt_file_queue_helper_20260718_low_latency_v12_fail_closed_sibling_scan"
EXPECTED_SHA256 = "REPLACED_BY_GENERATOR"
# BIGQMT_LOADER_CONFIG_END


def _load_bigqmt_helper():
    if not os.path.isfile(HELPER_PATH):
        raise RuntimeError("Big QMT helper not found: %s" % HELPER_PATH)
    with open(HELPER_PATH, "rb") as helper_file:
        source = helper_file.read()
    actual_sha256 = hashlib.sha256(source).hexdigest()
    if actual_sha256.lower() != EXPECTED_SHA256.lower():
        raise RuntimeError(
            "Big QMT helper sha256 mismatch: expected=%s actual=%s path=%s"
            % (EXPECTED_SHA256, actual_sha256, HELPER_PATH)
        )
    namespace = globals()
    exec(compile(source, HELPER_PATH, "exec"), namespace, namespace)
    if namespace.get("HELPER_NAME") != EXPECTED_HELPER_NAME:
        raise RuntimeError(
            "Big QMT helper name mismatch: expected=%s actual=%s"
            % (EXPECTED_HELPER_NAME, namespace.get("HELPER_NAME"))
        )
    if str(namespace.get("ACCOUNT_ID") or "") != EXPECTED_ACCOUNT_ID:
        raise RuntimeError(
            "Big QMT helper account mismatch: expected=%s actual=%s"
            % (EXPECTED_ACCOUNT_ID, namespace.get("ACCOUNT_ID"))
        )
    if namespace.get("BUILD_ID") != EXPECTED_BUILD_ID:
        raise RuntimeError(
            "Big QMT helper build mismatch: expected=%s actual=%s"
            % (EXPECTED_BUILD_ID, namespace.get("BUILD_ID"))
        )
    print(
        "[bigqmt_loader][%s] loaded build=%s account=%s sha256=%s path=%s"
        % (
            EXPECTED_HELPER_NAME,
            EXPECTED_BUILD_ID,
            EXPECTED_ACCOUNT_ID,
            actual_sha256,
            HELPER_PATH,
        )
    )


_load_bigqmt_helper()
