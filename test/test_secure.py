"""
NIKA Secure Channel (nika/secure) tests.

Run:  python -m pytest test/test_secure.py -v
"""
import os
import stat
import struct
import time

import pytest

from nika.secure import (
    ClientHandshake, NSCError, ServerHandshake, StaticKey, encode_public_key, fingerprint,
    generate_psk, parse_psk, parse_public_key,
)
from nika.secure import channel as ch


def handshake(client_payload=b"hello", server_payload=b"welcome"):
    server_key, client_key = StaticKey.generate(), StaticKey.generate()
    c = ClientHandshake(client_key, server_key.public, client_payload)
    s = ServerHandshake(server_key)
    m1 = c.write_message1()
    who = s.read_message1(m1)
    m2, s_sess = s.write_message2(server_payload)
    c_sess, got = c.read_message2(m2)
    return server_key, client_key, s, who, got, c_sess, s_sess


# ---- handshake ----

def test_handshake_authenticates_both_sides_and_carries_payloads():
    server_key, client_key, s, who, got, c_sess, s_sess = handshake()
    assert who == client_key.public                 # server learns the client's identity
    assert s.client_payload == b"hello"
    assert got == b"welcome"
    assert c_sess.handshake_hash == s_sess.handshake_hash
    assert c_sess.peer_public == server_key.public and s_sess.peer_public == client_key.public


def test_client_identity_is_not_sent_in_clear():
    server_key, client_key = StaticKey.generate(), StaticKey.generate()
    m1 = ClientHandshake(client_key, server_key.public).write_message1()
    assert client_key.public not in m1


def test_wrong_server_key_fails():
    real, fake, client = StaticKey.generate(), StaticKey.generate(), StaticKey.generate()
    m1 = ClientHandshake(client, fake.public).write_message1()   # client expects 'fake'
    with pytest.raises(NSCError):
        ServerHandshake(real).read_message1(m1)


def test_impostor_server_cannot_complete():
    real, impostor, client = StaticKey.generate(), StaticKey.generate(), StaticKey.generate()
    c = ClientHandshake(client, real.public)
    m1 = c.write_message1()
    s = ServerHandshake(real)
    s.read_message1(m1)
    # impostor without the real server key replays message 1 into its own handshake
    with pytest.raises(NSCError):
        ServerHandshake(impostor).read_message1(m1)
    m2, _ = s.write_message2()
    tampered = m2[:-1] + bytes([m2[-1] ^ 1])
    with pytest.raises(NSCError):
        c.read_message2(tampered)


def test_tampered_message1_rejected():
    server_key, client = StaticKey.generate(), StaticKey.generate()
    m1 = bytearray(ClientHandshake(client, server_key.public).write_message1())
    m1[40] ^= 0x01
    with pytest.raises(NSCError):
        ServerHandshake(server_key).read_message1(bytes(m1))


def test_stale_handshake_rejected(monkeypatch):
    server_key, client = StaticKey.generate(), StaticKey.generate()
    real_time = time.time
    monkeypatch.setattr(ch.time, "time", lambda: real_time() - 600)
    m1 = ClientHandshake(client, server_key.public).write_message1()
    monkeypatch.setattr(ch.time, "time", real_time)
    with pytest.raises(NSCError):
        ServerHandshake(server_key).read_message1(m1)


def test_low_order_ephemeral_rejected():
    server_key = StaticKey.generate()
    bogus = bytes([1]) + b"\x00" * 32 + b"\x00" * 100
    with pytest.raises(NSCError):
        ServerHandshake(server_key).read_message1(bogus)


def test_forward_secrecy_new_keys_every_session():
    _, _, _, _, _, c1, _ = handshake()
    _, _, _, _, _, c2, _ = handshake()
    assert c1._send_key != c2._send_key


# ---- transport ----

def test_frames_round_trip_both_ways():
    *_, c_sess, s_sess = handshake()
    for i in range(5):
        k, pt = s_sess.decrypt(c_sess.encrypt(f"up {i}".encode(), kind=7))
        assert (k, pt) == (7, f"up {i}".encode())
        k, pt = c_sess.decrypt(s_sess.encrypt(b"down", kind=2))
        assert (k, pt) == (2, b"down")


def test_replayed_frame_rejected():
    *_, c_sess, s_sess = handshake()
    f = c_sess.encrypt(b"once")
    s_sess.decrypt(f)
    with pytest.raises(NSCError):
        s_sess.decrypt(f)


def test_tampered_frame_and_header_rejected():
    *_, c_sess, s_sess = handshake()
    f = bytearray(c_sess.encrypt(b"payload", kind=1))
    f[-3] ^= 1
    with pytest.raises(NSCError):
        s_sess.decrypt(bytes(f))
    g = bytearray(c_sess.encrypt(b"payload", kind=1))
    g[3] ^= 1   # kind field in the header is authenticated too
    with pytest.raises(NSCError):
        s_sess.decrypt(bytes(g))


def test_failed_frame_does_not_advance_state():
    *_, c_sess, s_sess = handshake()
    good = c_sess.encrypt(b"ok")
    bad = bytearray(good)
    bad[-1] ^= 1
    with pytest.raises(NSCError):
        s_sess.decrypt(bytes(bad))
    assert s_sess.decrypt(good) == (0, b"ok")


def test_frames_from_another_session_rejected():
    *_, c1, _ = handshake()
    *_, _, s2 = handshake()
    with pytest.raises(NSCError):
        s2.decrypt(c1.encrypt(b"cross"))


def test_rekey_by_frame_count(monkeypatch):
    monkeypatch.setattr(ch, "REKEY_FRAMES", 3)
    *_, c_sess, s_sess = handshake()
    for i in range(10):
        assert s_sess.decrypt(c_sess.encrypt(str(i).encode()))[1] == str(i).encode()
    assert c_sess._send_epoch >= 3 and s_sess._recv_epoch == c_sess._send_epoch


def test_epoch_skip_rejected(monkeypatch):
    monkeypatch.setattr(ch, "REKEY_FRAMES", 1)
    *_, c_sess, s_sess = handshake()
    c_sess.encrypt(b"a")
    c_sess.encrypt(b"b")           # receiver never saw epoch 1
    f = c_sess.encrypt(b"c")       # epoch 2
    with pytest.raises(NSCError):
        s_sess.decrypt(f)


# ---- keys ----

def test_key_file_round_trip_and_permissions(tmp_path):
    p = tmp_path / "id" / "identity.key"
    k = StaticKey.load_or_create(str(p))
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    again = StaticKey.load_or_create(str(p))
    assert again.public == k.public
    assert parse_public_key(k.public_b64) == k.public
    assert fingerprint(k.public) == k.fingerprint and len(k.fingerprint) == 8


def test_parse_public_key_rejects_garbage():
    for bad in ("", "abc", "!!!!", "A" * 60):
        with pytest.raises(ValueError):
            parse_public_key(bad)


def test_encode_public_key_round_trips():
    k = StaticKey.generate()
    assert encode_public_key(k.public) == k.public_b64
    assert parse_public_key(encode_public_key(k.public)) == k.public
    with pytest.raises(ValueError):
        encode_public_key(b"short")


# ---- pre-shared key (IKpsk2) ----

def psk_handshake(client_psk, server_psk):
    server_key, client_key = StaticKey.generate(), StaticKey.generate()
    c = ClientHandshake(client_key, server_key.public, b"hello", psk=client_psk)
    s = ServerHandshake(server_key, psk=server_psk)
    m1 = c.write_message1()
    who = s.read_message1(m1)
    m2, s_sess = s.write_message2(b"welcome")
    c_sess, got = c.read_message2(m2)
    return m1, m2, who, client_key, got, c_sess, s_sess


def test_psk_handshake_round_trip_and_transport():
    psk = generate_psk()
    m1, m2, who, client_key, got, c_sess, s_sess = psk_handshake(psk, psk)
    assert m1[0] == ch.HANDSHAKE_VERSION_PSK and m2[0] == ch.HANDSHAKE_VERSION_PSK
    assert who == client_key.public and got == b"welcome"
    assert c_sess.handshake_hash == s_sess.handshake_hash
    assert s_sess.decrypt(c_sess.encrypt(b"up"))[1] == b"up"
    assert c_sess.decrypt(s_sess.encrypt(b"down"))[1] == b"down"
    # transport frames are unchanged: still version 1
    assert c_sess.encrypt(b"x")[0] == ch.VERSION


def test_psk_changes_every_key():
    *_, plain_c, _ = handshake()
    psk = generate_psk()
    *_, a, _ = psk_handshake(psk, psk)
    assert a.handshake_hash != plain_c.handshake_hash
    # the protocol name is part of the transcript, so the two modes can't be confused
    assert ch.PROTOCOL_NAME_PSK != ch.PROTOCOL_NAME


def test_wrong_psk_cannot_complete_the_handshake():
    server_key, client_key = StaticKey.generate(), StaticKey.generate()
    c = ClientHandshake(client_key, server_key.public, psk=generate_psk())
    s = ServerHandshake(server_key, psk=generate_psk())
    assert s.read_message1(c.write_message1()) == client_key.public   # psk2: checked in message 2
    m2, _ = s.write_message2(b"welcome")
    with pytest.raises(NSCError):
        c.read_message2(m2)


def test_psk_mismatch_fails_on_the_version_byte():
    server_key, client_key = StaticKey.generate(), StaticKey.generate()
    m1 = ClientHandshake(client_key, server_key.public, psk=generate_psk()).write_message1()
    with pytest.raises(NSCError, match="pre-shared key"):
        ServerHandshake(server_key).read_message1(m1)               # server has none

    m1 = ClientHandshake(client_key, server_key.public).write_message1()
    with pytest.raises(NSCError, match="pre-shared key is required"):
        ServerHandshake(server_key, psk=generate_psk()).read_message1(m1)

    # a psk client refuses a plain message 2 too
    psk = generate_psk()
    c = ClientHandshake(client_key, server_key.public, psk=psk)
    plain = ClientHandshake(client_key, server_key.public)
    s = ServerHandshake(server_key)
    s.read_message1(plain.write_message1())
    c.write_message1()
    m2, _ = s.write_message2()
    with pytest.raises(NSCError, match="pre-shared key"):
        c.read_message2(m2)


def test_parse_psk():
    text = generate_psk()
    raw = parse_psk(text)
    assert len(raw) == 32 and parse_psk(raw) == raw and parse_psk(None) is None
    assert generate_psk() != text
    for bad in ("", "abc", "!!!!", b"\x01" * 31, b"\x01" * 33, b"\x00" * 32, 42):
        with pytest.raises(ValueError):
            parse_psk(bad)
    with pytest.raises(ValueError):
        ClientHandshake(StaticKey.generate(), StaticKey.generate().public, psk="nope")
