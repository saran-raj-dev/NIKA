# NIKA Secure Channel (NSC) v1

**Package:** `nika.secure` · **Status:** v1

NSC is NIKA's own encrypted, mutually authenticated channel between two peers. The protocol (handshake shape, framing, key schedule, labels) is NIKA's. The building blocks are proven, standard primitives, because home-made ciphers are reliably broken:

| Purpose | Primitive |
|---|---|
| Key exchange | X25519 |
| Encryption + integrity | ChaCha20-Poly1305 (AEAD) |
| Key derivation, hashing | HKDF-SHA256, SHA-256 |

NSC turns bytes into bytes. It has no sockets and no application knowledge, so any NIKA transport can use it (today: the uplink).

---

## 1. Identities

Each peer has a long-term X25519 **static key**.

- **Public key:** 32 bytes, shown to humans as base64url (43 chars). Exchanged out of band, like SSH keys.
- **Fingerprint:** the first 8 hex characters of SHA-256 of the public key, for quick comparison.
- **Key file:** `NIKA-NSC-KEY v1` text, mode 0600 (`StaticKey.save()`, `load()`, `load_or_create()`).

The **server's public key is known to the client in advance** (pinned). The server learns the client's key during the handshake and decides whether to allow it.

---

## 2. Handshake

The protocol name is `NIKA-NSC-v1/IK/X25519/ChaChaPoly/SHA256`. It runs a Noise-style chaining key `ck` and transcript hash `h`, starting from `H(name)`, with the server's static key mixed into `h` first.

| Step | Client → Server (message 1) | Server → Client (message 2) |
|---|---|---|
| 1 | `e`: fresh ephemeral key, in clear | `e`: fresh ephemeral key, in clear |
| 2 | `es` = DH(e, S) → new key | `ee` = DH(e, e) → new key |
| 3 | client static key, **encrypted** | `se` = DH(e_s, C) → new key |
| 4 | `ss` = DH(C, S) → new key | payload, encrypted (server data) |
| 5 | payload, encrypted: timestamp + app data | |

Then `split()` produces two transport keys, one per direction.

Handshake messages start with a version byte: `1` for this pattern.

**What each side knows:**

- **After message 1**, the server knows **which** client this is and that it really holds that key (only its owner can compute `ss`). The timestamp must be within ±120 s.
- **After message 2**, the client knows it reached the **real** server (only its owner can produce `se`/`es`).
- **Forward secrecy:** session keys depend on ephemeral keys, so a later key leak doesn't decrypt old traffic.
- **Identity hiding:** the client's static key is never sent in clear.
- **Replays:** a replayed message 1 is rejected by the transport. The uplink server remembers recent ephemeral keys.
- **Bad keys:** low-order or invalid public keys are rejected.

---

## 2a. Optional deployment pre-shared key (IKpsk2)

A deployment can add a 32-byte secret that every peer and the server hold (`psk=`). Without it, even a peer with a registered key pair can't finish a handshake. It's off by default.

- **Protocol name:** `NIKA-NSC-v1/IKpsk2/X25519/ChaChaPoly/SHA256`, so the transcript of a psk handshake can never match a plain one.
- **Version byte:** handshake messages carry `2` instead of `1`. A peer with a psk and a peer without one fail **on the first byte** (`NSCError`, "a pre-shared key is required" / "peer uses a pre-shared key and we don't"). They don't fail on a confusing tag error.
- **Pattern** (Noise `IKpsk2`): every `e` is also mixed into the chaining key (`MixKey(e)`), and after `se` in message 2 the psk is mixed in with `MixKeyAndHash(psk)`: `ck, h', k = HKDF(ck, psk, 3)`, then `h = H(h ‖ h')`.

| | Message 1 | Message 2 |
|---|---|---|
| psk mode | `e`(+MixKey), `es`, `s`, `ss`, payload | `e`(+MixKey), `ee`, `se`, **`psk`**, payload |

- **What psk2 gives:** the server still reads message 1 without the psk (so a stranger's handshake costs it the same two DH operations as before). But nobody without the psk can read message 2 or derive the transport keys. Someone who stole a gateway's private key still can't open a session without the psk too.
- **Text form:** base64url of 32 random bytes (`generate_psk()`, `parse_psk()`). The all-zero key is refused.
- **Transport frames are unchanged** (still version `1`). The psk only changes the keys.
- **Rolling it out:** server and clients must switch together. A server with a psk refuses every plain client, and a plain server refuses every psk client.

---

## 3. Transport frames

```
header (12 bytes) = version(1) ‖ epoch(1) ‖ kind(2) ‖ counter(8)      big-endian
body              = ChaCha20-Poly1305(key[epoch], nonce = 0⁴ ‖ counter(LE), plaintext, aad = header)
```

- **The header is authenticated:** changing `kind`, `epoch` or `counter` breaks the tag.
- **Counters only move forward:** a replayed or reordered frame is rejected. State changes only after a frame authenticates, so garbage can't desynchronise a session.
- **Rekey:** every 2²⁰ frames or every hour, the sender moves to `epoch+1` with `key' = HKDF(key, "NIKA-NSC-v1 rekey")`. The receiver follows only one epoch ahead. After epoch 255, reconnect.

---

## 4. API

```python
from nika.secure import StaticKey, ClientHandshake, ServerHandshake, parse_public_key

server_key = StaticKey.load_or_create("/var/lib/app/nika/server.key")
client_key = StaticKey.load_or_create("/var/lib/app/nika/identity.key")

c = ClientHandshake(client_key, server_key.public, payload=b"hello")
s = ServerHandshake(server_key)
m1 = c.write_message1()
who = s.read_message1(m1)               # client's static public key → authorise it
m2, server_session = s.write_message2(b"welcome")
client_session, _ = c.read_message2(m2)

frame = client_session.encrypt(b"data", kind=1)
kind, data = server_session.decrypt(frame)   # raises NSCError on anything wrong
```

`NSCError` deliberately says little. Callers drop the message or session; they don't explain to the peer.

With a deployment pre-shared key, both sides pass the same `psk` (text or 32 bytes):

```python
from nika.secure import generate_psk
psk = generate_psk()                                    # store it like a password
c = ClientHandshake(client_key, server_key.public, psk=psk)
s = ServerHandshake(server_key, psk=psk)
```

Other helpers: `encode_public_key(raw)` (32 bytes → the text form, the inverse of `parse_public_key`), `PROTOCOL_NAME_PSK`.

## 5. Tests
`python -m pytest test/test_secure.py -v` (23 tests). They cover mutual authentication, identity hiding, a wrong or impostor server, tampering, stale handshakes, low-order keys, replay, epoch skips, rekeying, key files, and the pre-shared key mode (round trip, wrong psk, psk/plain mismatch on the version byte, key parsing).
