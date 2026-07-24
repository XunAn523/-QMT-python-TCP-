"""Safe asynchronous order example; dry-run unless explicitly confirmed."""

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


LIVE_CONFIRMATION = "I_UNDERSTAND_THIS_SENDS_A_LIVE_ORDER"


def parse_args():
    parser = argparse.ArgumentParser(description="submit one asynchronous local QMT order")
    parser.add_argument("--symbol", required=True, help="for example 600000.SH")
    parser.add_argument("--side", choices=("BUY", "SELL"), required=True)
    parser.add_argument("--quantity", type=int, required=True)
    parser.add_argument("--price", type=float, required=True)
    parser.add_argument("--client-order-id", required=True, help="stable business idempotency key")
    parser.add_argument("--request-id", default="", help="stable effect id for an explicitly safe same-key retry")
    parser.add_argument("--strategy-name", default="external_strategy")
    parser.add_argument("--order-remark", default="")
    parser.add_argument("--trace-id", default="")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", default="")
    return parser, parser.parse_args()


def main() -> int:
    parser, args = parse_args()
    if args.live and args.confirm != LIVE_CONFIRMATION:
        parser.error("refusing live order: pass --confirm %s" % LIVE_CONFIRMATION)
    runtime = LocalRuntimeConfig.load(ENV_FILE, allow_example=not args.live)
    configure_logging(runtime)
    api = build_api(runtime)
    preview = api.build_order_request(
        args.symbol,
        args.side,
        args.quantity,
        args.price,
        client_order_id=args.client_order_id,
        request_id=args.request_id,
        strategy_name=args.strategy_name,
        order_remark=args.order_remark,
        trace_id=args.trace_id,
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

    def capture_submit(message):
        if str(message.get("client_order_id") or "") != args.client_order_id:
            return
        msg_type = str(message.get("type") or "")
        stage = str(message.get("stage") or "")
        status = str(message.get("status") or "")
        terminal_direct = (
            msg_type == "ASYNC_ORDER"
            and (
                status == "REJECTED"
                or stage in {"REJECTED", "SUBMIT_UNKNOWN"}
                or (
                    message.get("idempotent") is True
                    and bool(str(message.get("order_id") or ""))
                )
            )
        )
        if msg_type == "ASYNC_ORDER_RESPONSE" or terminal_direct or msg_type == "ERROR":
            result.update(message)
            result_ready.set()

    journal = EventJournal(runtime.signal_journal)
    register_journal_handlers(api, journal, capture_submit)
    try:
        connect_or_raise(api)
        msg_id = api.send_order_async(
            args.symbol,
            args.side,
            args.quantity,
            args.price,
            client_order_id=args.client_order_id,
            request_id=args.request_id,
            strategy_name=args.strategy_name,
            order_remark=args.order_remark,
            trace_id=args.trace_id,
        )
        print("submitted msg_id=%s client_order_id=%s" % (msg_id, args.client_order_id))
        if not result_ready.wait(runtime.signal_wait_seconds):
            print("submit result is UNKNOWN; do not retry automatically", file=sys.stderr)
            return 6
        delivery_id = str(result.get("delivery_id") or "")
        if delivery_id and not api.wait_delivery_acknowledged(
            delivery_id,
            timeout=min(5.0, max(1.0, runtime.signal_wait_seconds)),
        ):
            print(
                "submit result was journaled but DELIVERY_ACK was not sent; "
                "state is UNKNOWN and must not be retried automatically",
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
