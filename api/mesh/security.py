# security.py
import base64
import hashlib
import hmac
import json
import time


def canonical(obj) -> bytes:
    """Stable JSON encoding used for every MAC and signature."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


# =================================================
# Link MAC — proves a frame came from a mesh member
# =================================================

def seal(frame: dict, key: bytes) -> bytes:
    body = dict(frame)
    body.pop("mac", None)
    body["mac"] = hmac.new(key, canonical(body), hashlib.sha256).hexdigest()
    return json.dumps(body).encode()


def unseal(raw: bytes, key: bytes, max_skew: float) -> dict:
    """Decode and verify a frame. Raises ValueError on anything suspicious."""
    try:
        frame = json.loads(raw.decode())
    except Exception:
        raise ValueError("invalid JSON")

    if not isinstance(frame, dict):
        raise ValueError("frame is not an object")

    mac = frame.pop("mac", None)
    if not isinstance(mac, str):
        raise ValueError("missing mac")

    expected = hmac.new(key, canonical(frame), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(mac, expected):
        raise ValueError("bad mac")

    ts = frame.get("t")
    if not isinstance(ts, (int, float)) or abs(time.time() - ts) > max_skew:
        raise ValueError("stale or future frame")

    return frame


# =================================================
# Origin signature — proves which gateway produced the data,
# even after it passed through a relay. Optional (Ed25519).
# =================================================

def _signed_part(origin: str, msg_id: str, topic: str, payload, ts: float) -> bytes:
    return canonical({"o": origin, "i": msg_id, "tp": topic, "p": payload, "t": ts})


class OriginSigner:
    """Signs data with this gateway's Ed25519 private key."""

    def __init__(self, private_key_pem: bytes, password: bytes | None = None):
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        self._key = load_pem_private_key(private_key_pem, password=password)

    def sign(self, origin: str, msg_id: str, topic: str, payload, ts: float) -> str:
        sig = self._key.sign(_signed_part(origin, msg_id, topic, payload, ts))
        return base64.b64encode(sig).decode()


def verify_origin(public_key_pem: bytes, origin: str, msg_id: str, topic: str,
                  payload, ts: float, signature: str) -> bool:
    """For the platform: check a relayed message really came from `origin`."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.serialization import load_pem_public_key

    key = load_pem_public_key(public_key_pem)
    try:
        key.verify(base64.b64decode(signature),
                   _signed_part(origin, msg_id, topic, payload, ts))
        return True
    except (InvalidSignature, ValueError):
        return False
