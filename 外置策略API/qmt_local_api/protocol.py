"""Protocol-v2 framing shared by the local Gateway and external strategy API."""

from __future__ import annotations

import json
import struct
from typing import Any, Dict, List


HEADER = struct.Struct(">I")
MAX_FRAME_BYTES = 10 * 1024 * 1024
PROTOCOL_VERSION = 2


class ProtocolError(ValueError):
    """A frame cannot be accepted without losing stream alignment."""


def _reject_nonfinite_json(value: str) -> None:
    raise ValueError("non-finite JSON number is forbidden: %s" % value)


def encode_frame(
    message: Dict[str, Any],
    max_frame_bytes: int = MAX_FRAME_BYTES,
) -> bytes:
    """Encode one UTF-8 JSON object behind a four-byte big-endian length."""
    if not isinstance(message, dict):
        raise TypeError("message must be a dict")
    try:
        body = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError("message is not finite JSON data") from exc
    if not body or len(body) > max_frame_bytes:
        raise ProtocolError(
            "frame body length must be between 1 and %d bytes, got %d"
            % (max_frame_bytes, len(body))
        )
    return HEADER.pack(len(body)) + body


class FrameDecoder:
    """Incrementally decode fragmented and coalesced TCP frames."""

    def __init__(self, max_frame_bytes: int = MAX_FRAME_BYTES) -> None:
        if int(max_frame_bytes) <= 0:
            raise ValueError("max_frame_bytes must be positive")
        self.max_frame_bytes = int(max_frame_bytes)
        self._buffer = bytearray()

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def reset(self) -> None:
        self._buffer.clear()

    def feed(self, chunk: bytes) -> List[Dict[str, Any]]:
        if chunk:
            self._buffer.extend(chunk)
        decoded: List[Dict[str, Any]] = []
        while len(self._buffer) >= HEADER.size:
            body_size = HEADER.unpack_from(self._buffer)[0]
            if body_size <= 0 or body_size > self.max_frame_bytes:
                self.reset()
                raise ProtocolError("invalid frame body length: %d" % body_size)
            frame_size = HEADER.size + body_size
            if len(self._buffer) < frame_size:
                break
            body = bytes(self._buffer[HEADER.size:frame_size])
            del self._buffer[:frame_size]
            try:
                message = json.loads(
                    body.decode("utf-8"),
                    parse_constant=_reject_nonfinite_json,
                )
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                self.reset()
                raise ProtocolError("invalid UTF-8 JSON frame") from exc
            if not isinstance(message, dict):
                self.reset()
                raise ProtocolError("JSON frame root must be an object")
            decoded.append(message)
        return decoded
