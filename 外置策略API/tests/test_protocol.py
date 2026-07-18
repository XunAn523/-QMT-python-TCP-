import json
import math
from pathlib import Path
import struct
import sys
import unittest

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from qmt_local_api import FrameDecoder, MAX_FRAME_BYTES, ProtocolError, encode_frame


class ProtocolTests(unittest.TestCase):
    def test_four_byte_big_endian_utf8_json_and_fragmentation(self):
        message = {"type": "ORDER_UPDATE", "text": "中文", "value": 7}
        frame = encode_frame(message)
        self.assertEqual(struct.unpack(">I", frame[:4])[0], len(frame) - 4)
        self.assertEqual(json.loads(frame[4:].decode("utf-8")), message)
        decoder = FrameDecoder()
        self.assertEqual(decoder.feed(frame[:2]), [])
        self.assertEqual(decoder.feed(frame[2:9]), [])
        self.assertEqual(decoder.feed(frame[9:]), [message])

    def test_coalesced_frames_are_not_lost(self):
        first = {"type": "A", "n": 1}
        second = {"type": "B", "n": 2}
        self.assertEqual(
            FrameDecoder().feed(encode_frame(first) + encode_frame(second)),
            [first, second],
        )

    def test_frame_limit_is_fixed_at_ten_mib(self):
        self.assertEqual(MAX_FRAME_BYTES, 10 * 1024 * 1024)
        with self.assertRaises(ProtocolError):
            FrameDecoder().feed(struct.pack(">I", MAX_FRAME_BYTES + 1))

    def test_non_finite_json_is_rejected(self):
        with self.assertRaises(ProtocolError):
            encode_frame({"value": math.nan})
        body = b'{"value":NaN}'
        with self.assertRaises(ProtocolError):
            FrameDecoder().feed(struct.pack(">I", len(body)) + body)


if __name__ == "__main__":
    unittest.main()
