"""Public imports for the local single-account QMT strategy API."""

from .api import LocalQmtApi, REDACTED, redact_for_output
from .client import BridgeClient
from .config import ConnectionConfig, EXPECTED_GATEWAY_BUILD_ID
from .protocol import (
    FrameDecoder,
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    ProtocolError,
    encode_frame,
)
from .runtime import DEFAULT_ENV_FILE, LocalRuntimeConfig
from .transport import TradeTransport, TransportDisconnected


__version__ = "1.0.0"

__all__ = [
    "BridgeClient",
    "ConnectionConfig",
    "DEFAULT_ENV_FILE",
    "EXPECTED_GATEWAY_BUILD_ID",
    "FrameDecoder",
    "LocalQmtApi",
    "LocalRuntimeConfig",
    "MAX_FRAME_BYTES",
    "PROTOCOL_VERSION",
    "ProtocolError",
    "REDACTED",
    "TradeTransport",
    "TransportDisconnected",
    "encode_frame",
    "redact_for_output",
]
