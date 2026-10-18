# channel.py
"""
NIKA Secure Channel (NSC), version 1.

NIKA's own end-to-end channel between two peers, built only from proven
primitives (no home-made cryptography):

    key exchange   X25519
    AEAD           ChaCha20-Poly1305
    KDF / hashing  HKDF-SHA256 / SHA-256

Handshake (two messages, the server's static key is known in advance):

    pre    : h ← H(PROTOCOL_NAME);  mix_hash(S)
    msg1 → : e,  es,  s(encrypted),  ss,  payload(encrypted: timestamp)
    msg2 ← : e,  ee,  se,               payload(encrypted: server data)
    split  : two transport keys, one per direction

Optional deployment pre-shared key (Noise IKpsk2 style, PROTOCOL_NAME_PSK):
a 32-byte secret both sides hold is mixed in after `se` in message 2, and
every `e` also feeds the key. Handshake messages then carry version byte 2
instead of 1, so a peer with a psk and a peer without one fail on the first
byte instead of on a confusing tag error.

After msg1 the server knows *which* client this is (its static key) and that
the client really holds that key; after msg2 the client knows it reached the
real server. Both sides get forward secrecy from the ephemeral keys, and the
client's identity is never sent in the clear.

Transport frame:   header(12) = version(1) ‖ epoch(1) ‖ kind(2) ‖ counter(8)
                   body       = ChaCha20-Poly1305(key[epoch], nonce(counter), plaintext, aad=header)

Counters only move forward (replays and reordering are rejected); keys roll
forward every REKEY_FRAMES frames or REKEY_SECONDS seconds.

This module knows nothing about sockets or applications: it turns bytes into
bytes. NIKA transports (the uplink, optionally the mesh) use it.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import struct
import time

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

PROTOCOL_NAME = b"NIKA-NSC-v1/IK/X25519/ChaChaPoly/SHA256"
PROTOCOL_NAME_PSK = b"NIKA-NSC-v1/IKpsk2/X25519/ChaChaPoly/SHA256"
VERSION = 1                          # transport frames, and handshakes without a psk
HANDSHAKE_VERSION_PSK = 2            # handshake messages when a psk is used
PSK_LEN = 32
KEY_LEN = 32
TAG_LEN = 16
HEADER = struct.Struct(">BBHQ")      # version, epoch, kind, counter
MAX_SKEW_S = 120                     # handshake timestamp window
REKEY_FRAMES = 1 << 20
REKEY_SECONDS = 3600
MAX_COUNTER = (1 << 64) - 1


class NSCError(Exception):
    """Anything wrong with a handshake or frame. Never reveals which check failed
    to the peer; callers just drop the message / session."""


# =================================================
# Keys
# =================================================

def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _unb64(s: str) -> bytes:
    s = s.strip()
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def encode_public_key(public_key: bytes) -> str:
    """32 raw bytes → the text form humans see (inverse of parse_public_key)."""
    if len(public_key) != KEY_LEN:
        raise ValueError("not a NIKA public key (expected 32 bytes)")
    return _b64(public_key)


def generate_psk() -> str:
    """A new random deployment pre-shared key, as text (base64url, 43 chars)."""
    return _b64(os.urandom(PSK_LEN))


def parse_psk(psk: "bytes | str | None") -> bytes | None:
    """A pre-shared key as text (base64url) or 32 raw bytes → 32 bytes. None stays None."""
    if psk is None:
        return None
    if isinstance(psk, str):
        try:
            psk = _unb64(psk)
        except Exception:
            raise ValueError("not a NIKA pre-shared key (expected base64)")
    if not isinstance(psk, (bytes, bytearray)) or len(psk) != PSK_LEN:
        raise ValueError("a NIKA pre-shared key is exactly 32 bytes")
    if not any(psk):
        raise ValueError("the all-zero pre-shared key is not allowed")
    return bytes(psk)


def fingerprint(public_key: bytes) -> str:
    """Short, human-comparable id of a public key (first 8 hex of SHA-256)."""
    return hashlib.sha256(public_key).hexdigest()[:8]


def parse_public_key(text: str) -> bytes:
    """A public key as shown to humans (base64url, 43 chars) → 32 raw bytes."""
    try:
        raw = _unb64(text)
    except Exception:
        raise ValueError("not a NIKA public key (expected base64)")
    if len(raw) != KEY_LEN:
        raise ValueError("not a NIKA public key (expected 32 bytes)")
    return raw


class StaticKey:
    """A peer's long-term X25519 identity."""

    def __init__(self, private: X25519PrivateKey):
        self._private = private
        self.public = private.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw)

    @classmethod
    def generate(cls) -> "StaticKey":
        return cls(X25519PrivateKey.generate())

    @classmethod
    def from_private_b64(cls, text: str) -> "StaticKey":
        raw = _unb64(text)
        if len(raw) != KEY_LEN:
            raise ValueError("invalid private key")
        return cls(X25519PrivateKey.from_private_bytes(raw))

    def private_b64(self) -> str:
        return _b64(self._private.private_bytes(
            serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
            serialization.NoEncryption()))

    @property
    def public_b64(self) -> str:
        return _b64(self.public)

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.public)

    def dh(self, peer_public: bytes) -> bytes:
        return _dh(self._private, peer_public)

    # ---- files (0600) ----

    def save(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(f"NIKA-NSC-KEY v1\nprivate {self.private_b64()}\npublic {self.public_b64}\n")
        os.chmod(path, 0o600)

    @classmethod
    def load(cls, path: str) -> "StaticKey":
        with open(path) as f:
            lines = f.read().split()
        if len(lines) < 4 or lines[0] != "NIKA-NSC-KEY" or "private" not in lines:
            raise ValueError(f"{path} is not a NIKA key file")
        return cls.from_private_b64(lines[lines.index("private") + 1])

    @classmethod
    def load_or_create(cls, path: str) -> "StaticKey":
        if os.path.exists(path):
            return cls.load(path)
        key = cls.generate()
        key.save(path)
        return key


# =================================================
# Symmetric state (Noise-style chaining key + transcript hash)
# =================================================

def _hkdf2(chaining_key: bytes, ikm: bytes) -> tuple[bytes, bytes]:
    prk = hmac.new(chaining_key, ikm, hashlib.sha256).digest()
    out1 = hmac.new(prk, b"\x01", hashlib.sha256).digest()
    out2 = hmac.new(prk, out1 + b"\x02", hashlib.sha256).digest()
    return out1, out2


def _hkdf3(chaining_key: bytes, ikm: bytes) -> tuple[bytes, bytes, bytes]:
    prk = hmac.new(chaining_key, ikm, hashlib.sha256).digest()
    out1 = hmac.new(prk, b"\x01", hashlib.sha256).digest()
    out2 = hmac.new(prk, out1 + b"\x02", hashlib.sha256).digest()
    out3 = hmac.new(prk, out2 + b"\x03", hashlib.sha256).digest()
    return out1, out2, out3


def _nonce(counter: int) -> bytes:
    return b"\x00\x00\x00\x00" + counter.to_bytes(8, "little")


class _Symmetric:
    def __init__(self, psk: bytes | None = None):
        self.psk = psk
        self.h = hashlib.sha256(PROTOCOL_NAME_PSK if psk else PROTOCOL_NAME).digest()
        self.ck = self.h
        self.k: bytes | None = None
        self.n = 0

    @property
    def version(self) -> int:
        return HANDSHAKE_VERSION_PSK if self.psk else VERSION

    def mix_ephemeral(self, e_pub: bytes):
        """Token `e`. In a psk handshake the ephemeral also feeds the key (Noise rule)."""
        self.mix_hash(e_pub)
        if self.psk:
            self.mix_key(e_pub)

    def mix_psk(self):
        """Token `psk` (Noise MixKeyAndHash)."""
        assert self.psk is not None
        self.ck, temp_h, self.k = _hkdf3(self.ck, self.psk)
        self.mix_hash(temp_h)
        self.n = 0

    def mix_hash(self, data: bytes):
        self.h = hashlib.sha256(self.h + data).digest()

    def mix_key(self, ikm: bytes):
        self.ck, self.k = _hkdf2(self.ck, ikm)
        self.n = 0

    def encrypt_and_hash(self, plaintext: bytes) -> bytes:
        assert self.k is not None
        ct = ChaCha20Poly1305(self.k).encrypt(_nonce(self.n), plaintext, self.h)
        self.n += 1
        self.mix_hash(ct)
        return ct

    def decrypt_and_hash(self, ciphertext: bytes) -> bytes:
        assert self.k is not None
        try:
            pt = ChaCha20Poly1305(self.k).decrypt(_nonce(self.n), ciphertext, self.h)
        except InvalidTag:
            raise NSCError("handshake authentication failed")
        self.n += 1
        self.mix_hash(ciphertext)
        return pt

    def split(self) -> tuple[bytes, bytes]:
        return _hkdf2(self.ck, b"")


def _public(private: X25519PrivateKey) -> bytes:
    return private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)


def _dh(private: X25519PrivateKey, public: bytes) -> bytes:
    try:
        shared = private.exchange(X25519PublicKey.from_public_bytes(public))
    except Exception:
        raise NSCError("bad public key")
    if shared == b"\x00" * 32:
        raise NSCError("bad public key")  # low-order point
    return shared


def _check_version(version: int, sym: _Symmetric):
    if version == sym.version:
        return
    if version == HANDSHAKE_VERSION_PSK:
        raise NSCError("peer uses a pre-shared key and we don't")
    if version == VERSION:
        raise NSCError("a pre-shared key is required")
    raise NSCError("bad handshake message")


# =================================================
# Handshake
# =================================================

class ClientHandshake:
    """The connecting side. Knows the server's public key in advance."""

    def __init__(self, static: StaticKey, server_public: bytes, payload: bytes = b"", *,
                 psk: bytes | str | None = None):
        if len(server_public) != KEY_LEN:
            raise ValueError("server public key must be 32 bytes")
        self.static = static
        self.server_public = server_public
        self.payload = payload
        self._e = X25519PrivateKey.generate()
        self._sym = _Symmetric(parse_psk(psk))
        self._sym.mix_hash(server_public)
        self._sent = False

    def write_message1(self) -> bytes:
        sym, e_pub = self._sym, _public(self._e)
        sym.mix_ephemeral(e_pub)                                       # e
        sym.mix_key(_dh(self._e, self.server_public))                 # es
        enc_static = sym.encrypt_and_hash(self.static.public)          # s
        sym.mix_key(self.static.dh(self.server_public))                # ss
        body = struct.pack(">Q", int(time.time())) + self.payload
        enc_payload = sym.encrypt_and_hash(body)
        self._sent = True
        return bytes([sym.version]) + e_pub + enc_static + enc_payload

    def read_message2(self, message: bytes) -> tuple["Session", bytes]:
        """Returns the session and the server's (decrypted) payload."""
        if not self._sent:
            raise NSCError("message 1 not sent")
        if len(message) < 1 + KEY_LEN + TAG_LEN:
            raise NSCError("bad handshake message")
        sym = self._sym
        _check_version(message[0], sym)
        f_pub = message[1:1 + KEY_LEN]
        sym.mix_ephemeral(f_pub)                                       # e
        sym.mix_key(_dh(self._e, f_pub))                               # ee
        sym.mix_key(self.static.dh(f_pub))                             # se
        if sym.psk:
            sym.mix_psk()                                              # psk
        payload = sym.decrypt_and_hash(message[1 + KEY_LEN:])
        k_c2s, k_s2c = sym.split()
        return Session(send_key=k_c2s, recv_key=k_s2c, handshake_hash=sym.h,
                       peer_public=self.server_public), payload


class ServerHandshake:
    """The listening side. Learns (and must then authorise) the client's key."""

    def __init__(self, static: StaticKey, max_skew: float = MAX_SKEW_S, *,
                 psk: bytes | str | None = None):
        self.static = static
        self.max_skew = max_skew
        self._sym = _Symmetric(parse_psk(psk))
        self._sym.mix_hash(static.public)
        self._e_remote: bytes | None = None
        self.client_public: bytes | None = None
        self.client_payload: bytes = b""
        self.client_ephemeral: bytes | None = None

    def read_message1(self, message: bytes) -> bytes:
        """Verifies message 1. Returns the client's static public key; the caller
        decides whether that key is allowed before calling write_message2()."""
        min_len = 1 + KEY_LEN + (KEY_LEN + TAG_LEN) + (8 + TAG_LEN)
        if len(message) < min_len:
            raise NSCError("bad handshake message")
        sym = self._sym
        _check_version(message[0], sym)
        e_pub = message[1:1 + KEY_LEN]
        sym.mix_ephemeral(e_pub)                                       # e
        sym.mix_key(self.static.dh(e_pub))                             # es
        off = 1 + KEY_LEN
        client_static = sym.decrypt_and_hash(message[off:off + KEY_LEN + TAG_LEN])
        off += KEY_LEN + TAG_LEN
        sym.mix_key(self.static.dh(client_static))                     # ss
        body = sym.decrypt_and_hash(message[off:])
        (ts,) = struct.unpack(">Q", body[:8])
        if abs(time.time() - ts) > self.max_skew:
            raise NSCError("handshake too old or from the future")
        self._e_remote = e_pub
        self.client_ephemeral = e_pub
        self.client_public = client_static
        self.client_payload = body[8:]
        return client_static

    def write_message2(self, payload: bytes = b"") -> tuple[bytes, "Session"]:
        if self.client_public is None or self._e_remote is None:
            raise NSCError("message 1 not read")
        sym = self._sym
        f = X25519PrivateKey.generate()
        f_pub = _public(f)
        sym.mix_ephemeral(f_pub)                                       # e
        sym.mix_key(_dh(f, self._e_remote))                            # ee
        sym.mix_key(_dh(f, self.client_public))                        # se
        if sym.psk:
            sym.mix_psk()                                              # psk
        enc_payload = sym.encrypt_and_hash(payload)
        k_c2s, k_s2c = sym.split()
        session = Session(send_key=k_s2c, recv_key=k_c2s, handshake_hash=sym.h,
                          peer_public=self.client_public)
        return bytes([sym.version]) + f_pub + enc_payload, session


# =================================================
# Transport session
# =================================================

class Session:
    """Encrypts / decrypts transport frames after a completed handshake."""

    def __init__(self, *, send_key: bytes, recv_key: bytes, handshake_hash: bytes,
                 peer_public: bytes):
        self.peer_public = peer_public
        self.handshake_hash = handshake_hash
        self._send_key, self._send_epoch, self._send_ctr = send_key, 0, 0
        self._recv_key, self._recv_epoch, self._recv_next = recv_key, 0, 0
        self._send_epoch_started = time.monotonic()

    @property
    def peer_fingerprint(self) -> str:
        return fingerprint(self.peer_public)

    @staticmethod
    def _roll(key: bytes) -> bytes:
        return _hkdf2(key, b"NIKA-NSC-v1 rekey")[0]

    def encrypt(self, plaintext: bytes, kind: int = 0) -> bytes:
        if (self._send_ctr >= REKEY_FRAMES
                or time.monotonic() - self._send_epoch_started >= REKEY_SECONDS):
            if self._send_epoch >= 255:
                raise NSCError("session exhausted; reconnect")
            self._send_key = self._roll(self._send_key)
            self._send_epoch += 1
            self._send_ctr = 0
            self._send_epoch_started = time.monotonic()
        header = HEADER.pack(VERSION, self._send_epoch, kind & 0xFFFF, self._send_ctr)
        body = ChaCha20Poly1305(self._send_key).encrypt(_nonce(self._send_ctr), plaintext, header)
        self._send_ctr += 1
        return header + body

    def decrypt(self, frame: bytes) -> tuple[int, bytes]:
        """Returns (kind, plaintext). Raises NSCError on anything wrong."""
        if len(frame) < HEADER.size + TAG_LEN:
            raise NSCError("short frame")
        version, epoch, kind, counter = HEADER.unpack(frame[:HEADER.size])
        if version != VERSION:
            raise NSCError("bad frame")

        key, next_ctr = self._recv_key, self._recv_next
        if epoch == self._recv_epoch + 1:
            key, next_ctr = self._roll(self._recv_key), 0      # peer rolled its key
        elif epoch != self._recv_epoch:
            raise NSCError("bad epoch")

        if counter < next_ctr:
            raise NSCError("replayed or reordered frame")
        try:
            plaintext = ChaCha20Poly1305(key).decrypt(_nonce(counter), frame[HEADER.size:],
                                                      frame[:HEADER.size])
        except InvalidTag:
            raise NSCError("frame authentication failed")

        # only commit state after the frame authenticated
        if epoch != self._recv_epoch:
            self._recv_key, self._recv_epoch = key, epoch
        self._recv_next = counter + 1
        return kind, plaintext
