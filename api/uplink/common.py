# common.py
"""Wire helpers and value types shared by the uplink client and server.

ZeroMQ frames (DEALER ⇄ ROUTER):
    [b"H1", handshake message 1]      client → server
    [b"H2", handshake message 2]      server → client
    [b"D",  NSC frame]                both ways, after the handshake
    [b"E",  {"code": ...}]            server → client, plaintext refusal (handshake only)

Inside an NSC frame: compact JSON {"t": <UPLINK_*>, ...}. Payloads are carried
as UTF-8 text ("p") when possible, base64 ("pb") otherwise.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import Any

K_HANDSHAKE1 = b"H1"
K_HANDSHAKE2 = b"H2"
K_DATA = b"D"
K_ERROR = b"E"

MAX_FRAME = 1 << 20          # 1 MiB per message
DEFAULT_PORT = 47900


class UplinkError(Exception):
    """The server rejected a message (reason is an ERR_UPLINK_* code or app text)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class UplinkUnavailable(UplinkError):
    """Not connected, or no answer in time. The message may or may not have been stored."""


def encode(msg: dict) -> bytes:
    return json.dumps(msg, separators=(",", ":"), ensure_ascii=False).encode()


def decode(raw: bytes) -> dict:
    msg = json.loads(raw.decode())
    if not isinstance(msg, dict) or "t" not in msg:
        raise ValueError("not an uplink message")
    return msg


def put_payload(msg: dict, payload: bytes | str) -> dict:
    if isinstance(payload, str):
        msg["p"] = payload
        return msg
    try:
        msg["p"] = payload.decode("utf-8")
    except UnicodeDecodeError:
        msg["pb"] = base64.b64encode(payload).decode()
    return msg


def get_payload(msg: dict) -> bytes:
    if "pb" in msg:
        return base64.b64decode(msg["pb"])
    p = msg.get("p", "")
    return p.encode("utf-8") if isinstance(p, str) else b""


@dataclass
class DataMessage:
    """Upstream data as the server application sees it."""
    peer_id: str              # authenticated sender (the connection)
    origin: str               # whose data it is: peer_id, or a peer it relays for
    msg_id: str
    topic: str
    payload: bytes
    qos: int = 1
    meta: dict[str, Any] = field(default_factory=dict)   # sender's HELLO meta

    @property
    def relayed(self) -> bool:
        return self.origin != self.peer_id


@dataclass
class Command:
    """A downstream command as the client application sees it."""
    command_id: str
    topic: str
    payload: bytes
    expires_at: float | None
    target: str | None        # None = for this peer; else a peer it relays for
