"""Completely offline framing demo: no `.env`, socket, or QMT required."""

from __future__ import annotations

import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_ROOT = PROJECT_ROOT / "外置策略API"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api import FrameDecoder, encode_frame, redact_for_output


def main() -> int:
    messages = [
        {
            "type": "PING",
            "msg_id": "offline-ping-114514",
            "protocol_version": 2,
            "account_name": "account_main",
            "account_id": "REPLACE_WITH_QMT_ACCOUNT_ID",
            "auth_token": "0" * 64,
        },
        {
            "type": "QUERY",
            "msg_id": "offline-query",
            "account_name": "account_main",
            "account_id": "REPLACE_WITH_QMT_ACCOUNT_ID",
            "query_type": "ACCOUNT_STATUS",
            "params": {},
        },
        {
            "type": "NEW_ASYNC",
            "msg_id": "offline-order",
            "request_id": "offline-order",
            "protocol_version": 2,
            "account_name": "account_main",
            "account_id": "REPLACE_WITH_QMT_ACCOUNT_ID",
            "symbol": "600000.SH",
            "side": "BUY",
            "quantity": 100,
            "price": 10.23,
            "client_order_id": "stable-business-order-id",
        },
        {
            "type": "CANCEL_ASYNC",
            "msg_id": "offline-cancel",
            "account_name": "account_main",
            "account_id": "REPLACE_WITH_QMT_ACCOUNT_ID",
            "order_id": "QMT-ORDER-ID",
        },
        {
            "type": "DELIVERY_ACK",
            "msg_id": "offline-ack",
            "delivery_id": "event:offline-1",
        },
    ]
    stream = b"".join(encode_frame(message) for message in messages)
    decoder = FrameDecoder()
    decoded = []
    for offset in range(0, len(stream), 7):
        decoded.extend(decoder.feed(stream[offset:offset + 7]))
    print(json.dumps({
        "network_opened": False,
        "env_loaded": False,
        "frame_count": len(decoded),
        "messages": redact_for_output(decoded),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
