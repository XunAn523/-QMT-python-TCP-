"""Read-only account query using the single project-root `.env`."""

from __future__ import annotations

import argparse
import json
import sys

from common import ENV_FILE, EVENT_TYPES, connect_or_raise

from qmt_local_api import LocalQmtApi, redact_for_output


def parse_args():
    parser = argparse.ArgumentParser(description="query the local QMT bridge")
    parser.add_argument("--query-type", default="ACCOUNT_STATUS")
    parser.add_argument("--stock-code", default="")
    parser.add_argument("--timeout", type=float)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api = LocalQmtApi.from_env(ENV_FILE)

    def refuse_reliable_event(message):
        print(
            json.dumps(redact_for_output(message), ensure_ascii=False),
            file=sys.stderr,
        )
        raise RuntimeError("read-only query process refuses reliable event ACK")

    for event_type in EVENT_TYPES:
        api.on(event_type, refuse_reliable_event)
    try:
        connect_or_raise(api)
        params = {"stock_code": args.stock_code} if args.stock_code else {}
        response = api.query(args.query_type, params, timeout=args.timeout)
        if response is None:
            print("query timed out or connection closed", file=sys.stderr)
            return 4
        print(json.dumps(redact_for_output(response), ensure_ascii=False, indent=2))
        degraded = (
            response.get("cache_fallback") is True
            or response.get("state") == "degraded"
            or (
                isinstance(response.get("qmt_status"), dict)
                and response["qmt_status"].get("ready") is False
            )
        )
        return 5 if degraded else (0 if response.get("success") is True else 3)
    finally:
        api.stop()


if __name__ == "__main__":
    raise SystemExit(main())
