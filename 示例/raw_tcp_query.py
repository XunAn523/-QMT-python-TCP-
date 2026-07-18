"""Minimal live raw-socket query; use qmt_local_api for production strategies."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import struct
import sys
import time
import uuid


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.project_env import load_deployment


MAX_FRAME_BYTES = 10 * 1024 * 1024
SENSITIVE_FIELDS = {"accountid", "accountname", "writertoken", "authtoken", "raw"}


def reject_nonfinite(value):
    raise ValueError("non-finite JSON number is forbidden: %s" % value)


def encode_frame(message):
    body = json.dumps(
        message,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if not 0 < len(body) <= MAX_FRAME_BYTES:
        raise ValueError("frame body must be 1..10485760 bytes")
    return struct.pack(">I", len(body)) + body


def recv_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Gateway closed the TCP connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock):
    length = struct.unpack(">I", recv_exact(sock, 4))[0]
    if not 0 < length <= MAX_FRAME_BYTES:
        raise ValueError("invalid Gateway frame length: %d" % length)
    value = json.loads(
        recv_exact(sock, length).decode("utf-8"),
        parse_constant=reject_nonfinite,
    )
    if not isinstance(value, dict):
        raise ValueError("frame JSON root must be an object")
    return value


def redact_for_output(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            normalized = str(key).lower().replace("_", "")
            result[key] = (
                "<REDACTED>"
                if normalized in SENSITIVE_FIELDS
                else redact_for_output(item)
            )
        return result
    if isinstance(value, list):
        return [redact_for_output(item) for item in value]
    return value


def parse_args():
    parser = argparse.ArgumentParser(description="diagnostic raw TCP query")
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--query-type", default="ACCOUNT_STATUS")
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_deployment(args.env_file.resolve())["api_config"]
    ping_id = "raw-ping-" + uuid.uuid4().hex
    query_id = "raw-query-" + uuid.uuid4().hex
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(float(config["connect_timeout"]))
        sock.bind((str(config["local_host"]), 0))
        sock.connect((str(config["host"]), int(config["port"])))
        sock.settimeout(float(config["handshake_timeout"]))
        sock.sendall(encode_frame({
            "type": "PING",
            "msg_id": ping_id,
            "protocol_version": 2,
            "account_id": config["account_id"],
            "account_name": config["account_name"],
            "auth_token": config["auth_token"],
            "timestamp": time.time(),
        }))
        pong = recv_frame(sock)
        expected = {
            "type": "PONG",
            "msg_id": ping_id,
            "protocol_version": 2,
            "build_id": config["expected_gateway_build_id"],
            "account_id": config["account_id"],
            "account_name": config["account_name"],
        }
        mismatches = [key for key, value in expected.items() if pong.get(key) != value]
        if mismatches:
            raise RuntimeError("PONG identity mismatch: %s" % ",".join(mismatches))
        sock.sendall(encode_frame({
            "type": "QUERY",
            "msg_id": query_id,
            "account_id": config["account_id"],
            "account_name": config["account_name"],
            "query_type": args.query_type,
            "params": {},
            "timestamp": time.time(),
        }))
        while True:
            message = recv_frame(sock)
            if message.get("type") == "PING":
                sock.sendall(encode_frame({
                    "type": "PONG",
                    "msg_id": message.get("msg_id", ""),
                    "timestamp": time.time(),
                }))
                continue
            if message.get("delivery_id"):
                raise RuntimeError(
                    "reliable callback received; raw diagnostic refuses ACK, use qmt_local_api"
                )
            if message.get("msg_id") == query_id:
                safe = redact_for_output(message)
                print(json.dumps(safe, ensure_ascii=False, indent=2))
                return 0 if message.get("success") is True else 3


if __name__ == "__main__":
    raise SystemExit(main())
