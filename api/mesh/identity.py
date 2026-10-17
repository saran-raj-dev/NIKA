# identity.py
"""
Origin proof of key possession for relay sessions.

The site mesh key only proves "this frame came from a mesh member". It says
nothing about *which* member, so an origin's long-term identity (its NSC static
X25519 key) is checked with a challenge when it opens a relay session:

    origin → relay : RELAY_OPEN      {meta, id: S_pub}
    relay  → origin: RELAY_CHALLENGE {n: nonce(32), e: E_pub}         fresh E per challenge
    origin → relay : RELAY_PROVE     {mac}
                     k   = HKDF(salt = SHA256(LABEL ‖ n), ikm = DH(S, E))[0]
                     mac = HMAC-SHA256(k, canonical({label, relay, origin, n, e, id, meta-hash}))
    relay          : recomputes with DH(E, S_pub); only S's owner could have answered

The MAC binds both node ids, the nonce, the relay's ephemeral key, the claimed
key and the RELAY_OPEN meta, so a proof can't be replayed, moved to another
relay, or reused with different meta. The ephemeral private key is thrown away
after one check.

Imports the NSC primitives lazily, so the mesh stays importable without the
`cryptography` package when identities aren't used.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os

from .security import canonical

PROOF_LABEL = "NIKA-MESH-v1/origin-proof/X25519/HMAC-SHA256"
NONCE_LEN = 32


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _transcript(*, relay_id: str, origin_id: str, nonce: str, ephemeral: str,
                identity: str, meta: dict) -> bytes:
    return canonical({
        "l": PROOF_LABEL,
        "r": relay_id,
        "o": origin_id,
        "n": nonce,
        "e": ephemeral,
        "s": identity,
        "m": hashlib.sha256(canonical(meta or {})).hexdigest(),
    })


def _mac(shared: bytes, nonce: str, transcript: bytes) -> str:
    from ..secure.channel import _hkdf2

    salt = hashlib.sha256(PROOF_LABEL.encode() + _unb64(nonce)).digest()
    key, _ = _hkdf2(salt, shared)
    return hmac.new(key, transcript, hashlib.sha256).hexdigest()


class Challenge:
    """Relay side: one outstanding challenge for one origin."""

    def __init__(self, *, relay_id: str, origin_id: str, identity: bytes, meta: dict):
        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
        from ..secure.channel import _public

        if len(identity) != 32:
            raise ValueError("identity must be a 32-byte X25519 public key")
        self.relay_id = relay_id
        self.origin_id = origin_id
        self.identity = identity
        self.meta = meta
        self._e = X25519PrivateKey.generate()
        self.nonce = _b64(os.urandom(NONCE_LEN))
        self.ephemeral = _b64(_public(self._e))

    def payload(self) -> dict:
        return {"n": self.nonce, "e": self.ephemeral}

    def verify(self, mac) -> bool:
        """True if `mac` proves possession of the claimed key. One shot."""
        from ..secure.channel import NSCError, _dh

        e, self._e = self._e, None
        if e is None or not isinstance(mac, str):
            return False
        try:
            shared = _dh(e, self.identity)
        except NSCError:
            return False
        expected = _mac(shared, self.nonce, _transcript(
            relay_id=self.relay_id, origin_id=self.origin_id, nonce=self.nonce,
            ephemeral=self.ephemeral, identity=_b64(self.identity), meta=self.meta))
        return hmac.compare_digest(mac, expected)


def prove(static, *, relay_id: str, origin_id: str, challenge: dict, meta: dict) -> str | None:
    """Origin side: answer a relay's challenge with our StaticKey. None if malformed."""
    from ..secure.channel import NSCError

    nonce, eph = challenge.get("n"), challenge.get("e")
    if not isinstance(nonce, str) or not isinstance(eph, str):
        return None
    try:
        if len(_unb64(nonce)) != NONCE_LEN:
            return None
        shared = static.dh(_unb64(eph))
    except (NSCError, ValueError):
        return None
    return _mac(shared, nonce, _transcript(
        relay_id=relay_id, origin_id=origin_id, nonce=nonce, ephemeral=eph,
        identity=static.public_b64, meta=meta))
