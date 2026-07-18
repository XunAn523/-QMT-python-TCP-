"""Safe asynchronous cancel example; dry-run unless explicitly confirmed."""

from __future__ import annotations

import argparse
import json
import sys
import threading

from common import (
    ENV_FILE,
    EventJournal,
    build_api,
    configure_logging,
    connect_or_raise,
    register_journal_handlers,
)

from qmt_local_api import LocalRuntimeConfig, encode_frame, redact_for_output


LIVE_CONFIRMATION = "I_UNDERSTAND_THIS_SENDS_A_LIVE_CANCEL"


def parse_args():
    parser = argparse.ArgumentParser(description="submit one asynchronous local QMT cancel")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--order-id", default="")
    target.add_argument("--order-sysid", default="")
    parser.add_argument("--market", type=int, help="required with --order-sysid")
    parser.add_argument("--request-id", default="", help="stable effect id for an explicitly safe same-key retry")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", default="")
    return parser, parser.parse_args()


def main() -> int:
    parser, args = parse_args()
    if args.live and args.confirm != LIVE_CONFIRMATION:
        parser.error("refusing live cancel: pass --confirm %s" % LIVE_CONFIRMATION)
    if args.order_sysid and args.market is None:
        parser.error("--market is required with --order-sysid")
    runtime = LocalRuntimeConfig.load(ENV_FILE, allow_example=not args.live)
    configure_logging(runtime)
    api = build_api(runtime)
    if args.order_id:
        preview = api.build_cancel_request(
            args.order_id,
            async_mode=True,
            request_id=args.request_id,
        )
    else:
        preview = api.build_cancel_sysid_request(
            args.market,
            args.order_sysid,
            async_mode=True,
            request_id=args.request_id,
        )
    if not args.live:
        print(json.dumps({
            "dry_run": True,
            "network_opened": False,
            "frame_body_bytes": len(encode_frame(preview)) - 4,
            "message": redact_for_output(preview),
            "live_requires": "--live --confirm " + LIVE_CONFIRMATION,
        }, ensure_ascii=False, indent=2))
        return 0

    result_ready = threading.Event()
    result = {}

    def capture_cancel(message):
        msg_type = str(message.get("type") or "")
        immediate_reject = (
            msg_type == "ASYNC_CANCEL"
            and str(message.get("status") or "") == "REJECTED"
        )
        if msg_type in {"ASYNC_CANCEL_RESPONSE", "ERROR"} or immediate_reject:
            result.update(message)
            result_ready.set()

    journal = EventJournal(runtime.signal_journal)
    register_journal_handlers(api, journal, capture_cancel)
    try:
        connect_or_raise(api)
        if args.order_id:
            msg_id = api.send_cancel_async(
                args.order_id,
                request_id=args.request_id,
            )
        else:
            msg_id = api.send_cancel_sysid_async(
                args.market,
                args.order_sysid,
                request_id=args.request_id,
            )
        print("cancel submitted msg_id=%s" % msg_id)
        if not result_ready.wait(runtime.signal_wait_seconds):
            print(
                "cancel helper result is UNKNOWN; inspect ORDER_UPDATE before retry",
                file=sys.stderr,
            )
            return 6
        delivery_id = str(result.get("delivery_id") or "")
        if delivery_id and not api.wait_delivery_acknowledged(
            delivery_id,
            timeout=min(5.0, max(1.0, runtime.signal_wait_seconds)),
        ):
            print(
                "cancel result was journaled but DELIVERY_ACK was not sent; "
                "inspect ORDER_UPDATE before retry",
                file=sys.stderr,
            )
            return 10
        print(json.dumps(redact_for_output(result), ensure_ascii=False, indent=2))
        if str(result.get("stage") or "") == "REJECTED":
            return 7
        if str(result.get("stage") or "") == "SUBMIT_UNKNOWN":
            return 8
        return 0
    finally:
        try:
            api.stop()
        finally:
            journal.close()


if __name__ == "__main__":
    raise SystemExit(main())
