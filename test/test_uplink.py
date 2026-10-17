"""
NIKA uplink (api/uplink) tests: real client/server over localhost TCP.

Run:  python -m pytest test/test_uplink.py -v
"""
import asyncio
import socket
import time

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio

from api.secure import ClientHandshake, StaticKey
from api.uplink import (
    CONNECTED, REFUSED, NikaUplinkClient, NikaUplinkServer, UplinkError, UplinkUnavailable,
)

pytestmark = pytest.mark.asyncio


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def wait_for(cond, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return False


class Platform:
    """A tiny app around NikaUplinkServer: knows some keys, stores data in a list."""

    def __init__(self, port=None, key=None):
        self.port = port or free_port()
        self.key = key or StaticKey.generate()
        self.known: dict[bytes, str] = {}
        self.stored = []
        self.reject_with: str | None = None
        self.raise_on_data = False
        self.allow_routes = True
        self.routes, self.sessions, self.cmd_acks = [], [], []
        self.server = None

    def register(self, key: StaticKey, peer_id: str):
        self.known[key.public] = peer_id

    async def start(self, **kw):
        async def authorize(pub):
            return self.known.get(pub)

        async def on_data(dm):
            if self.raise_on_data:
                raise RuntimeError("db down")
            if self.reject_with:
                return self.reject_with
            self.stored.append(dm)

        async def on_route(relay, origin, active):
            self.routes.append((relay, origin, active))
            return self.allow_routes

        async def on_session(peer, active, meta):
            self.sessions.append((peer, active, meta))

        async def on_ack(peer, cid, status, detail):
            self.cmd_acks.append((peer, cid, status))

        self.server = NikaUplinkServer(self.key, bind=f"tcp://127.0.0.1:{self.port}",
                                       authorize=authorize, on_data=on_data, on_route=on_route,
                                       on_session=on_session, on_command_ack=on_ack, **kw)
        await self.server.start()

    async def stop(self):
        await self.server.stop()


@pytest_asyncio.fixture
async def platform():
    p = Platform()
    await p.start()
    yield p
    await p.stop()


clients = []


def make_client(platform, key=None, **kw):
    key = key or StaticKey.generate()
    c = NikaUplinkClient(key, platform.key.public, endpoint=f"tcp://127.0.0.1:{platform.port}",
                         heartbeat=0.3, timeout=2.0, handshake_timeout=1.0, ack_timeout=3.0,
                         max_backoff=0.5, **kw)
    clients.append(c)
    return key, c


@pytest_asyncio.fixture(autouse=True)
async def _cleanup():
    yield
    while clients:
        await clients.pop().stop()


# ---- connect + data ----

async def test_connects_and_publishes_with_ack(platform):
    key, c = make_client(platform, meta={"base_topic": "site/gw/b"})
    platform.register(key, "gw-b")
    await c.start()
    assert await c.wait_connected(3)
    assert c.peer_id == "gw-b"
    await c.publish("site/gw/b/data/AHU-1", '{"t": 21.5}', msg_id="m1")

    dm = platform.stored[0]
    assert (dm.peer_id, dm.origin, dm.msg_id, dm.topic) == ("gw-b", "gw-b", "m1", "site/gw/b/data/AHU-1")
    assert dm.payload == b'{"t": 21.5}' and not dm.relayed
    assert dm.meta == {"base_topic": "site/gw/b"}
    assert await wait_for(lambda: ("gw-b", True, {"base_topic": "site/gw/b"}) in platform.sessions)


async def test_binary_payload_round_trips(platform):
    key, c = make_client(platform)
    platform.register(key, "gw-b")
    await c.start()
    assert await c.wait_connected(3)
    await c.publish("t", b"\x00\xff\x10")
    assert platform.stored[0].payload == b"\x00\xff\x10"


async def test_unknown_key_is_refused(platform):
    _, c = make_client(platform)
    await c.start()
    assert await wait_for(lambda: c.state == REFUSED, 4)
    assert c.last_error == "UPLINK_NOT_AUTHORISED"
    with pytest.raises(UplinkUnavailable):
        await c.publish("t", "x")


async def test_wrong_server_key_never_connects(platform):
    key = StaticKey.generate()
    platform.register(key, "gw-b")
    c = NikaUplinkClient(key, StaticKey.generate().public, endpoint=f"tcp://127.0.0.1:{platform.port}",
                         handshake_timeout=0.5, max_backoff=0.5)
    clients.append(c)
    await c.start()
    assert not await c.wait_connected(1.5)
    assert platform.stored == [] and platform.server.metrics["handshakes_rejected"] >= 1


async def test_application_rejection_and_failure(platform):
    key, c = make_client(platform)
    platform.register(key, "gw-b")
    await c.start()
    assert await c.wait_connected(3)

    platform.reject_with = "PARSE_ERROR"
    with pytest.raises(UplinkError) as e:
        await c.publish("t", "x")
    assert e.value.reason == "PARSE_ERROR"

    platform.reject_with, platform.raise_on_data = None, True
    with pytest.raises(UplinkError) as e:
        await c.publish("t", "x")
    assert e.value.reason == "UPLINK_STORE_FAILED"


async def test_order_is_preserved(platform):
    key, c = make_client(platform)
    platform.register(key, "gw-b")
    await c.start()
    assert await c.wait_connected(3)
    await asyncio.gather(*(c.publish("t", str(i), msg_id=f"m{i}") for i in range(50)))
    assert [d.msg_id for d in platform.stored] == [f"m{i}" for i in range(50)]


# ---- relaying ----

async def test_relayed_origin_requires_an_allowed_route(platform):
    key_a, a = make_client(platform)
    platform.register(key_a, "gw-a")
    await a.start()
    assert await a.wait_connected(3)

    with pytest.raises(UplinkError) as e:
        await a.publish("site/gw/b/data/x", "1", origin="gw-b")
    assert e.value.reason == "UPLINK_ORIGIN_DENIED"

    assert await a.announce_route("gw-b", True)
    await a.publish("site/gw/b/data/x", "2", origin="gw-b", msg_id="r1")
    dm = platform.stored[-1]
    assert (dm.peer_id, dm.origin, dm.relayed) == ("gw-a", "gw-b", True)
    assert platform.server.route_of("gw-b") == "gw-a"

    await a.announce_route("gw-b", False)
    assert await wait_for(lambda: platform.server.route_of("gw-b") is None)


async def test_platform_can_refuse_a_route(platform):
    key_a, a = make_client(platform)
    platform.register(key_a, "gw-a")
    platform.allow_routes = False
    await a.start()
    assert await a.wait_connected(3)
    assert await a.announce_route("gw-b", True) is False
    assert platform.server.route_of("gw-b") is None


# ---- commands ----

async def test_command_direct_with_dedupe_and_expiry(platform):
    got = []

    async def on_cmd(cmd):
        got.append((cmd.command_id, cmd.topic, cmd.payload, cmd.target))
        return "accepted"

    key, c = make_client(platform, on_command=on_cmd)
    platform.register(key, "gw-b")
    await c.start()
    assert await c.wait_connected(3)

    srv = platform.server
    assert await srv.send_command("gw-b", "site/gw/b/cmd", '{"v": 1}', command_id="c1") == "direct"
    assert await wait_for(lambda: ("gw-b", "c1", "accepted") in platform.cmd_acks)
    await srv.send_command("gw-b", "site/gw/b/cmd", '{"v": 1}', command_id="c1")
    assert await wait_for(lambda: ("gw-b", "c1", "duplicate") in platform.cmd_acks)
    await srv.send_command("gw-b", "site/gw/b/cmd", "{}", command_id="c2", expires_at=time.time() - 1)
    assert await wait_for(lambda: ("gw-b", "c2", "expired") in platform.cmd_acks)

    assert got == [("c1", "site/gw/b/cmd", b'{"v": 1}', None)]


async def test_command_reaches_relayed_peer_through_its_relay(platform):
    got = []

    async def on_cmd(cmd):
        got.append((cmd.command_id, cmd.target))
        return "forwarded"

    key_a, a = make_client(platform, on_command=on_cmd)
    platform.register(key_a, "gw-a")
    await a.start()
    assert await a.wait_connected(3)

    assert await platform.server.send_command("gw-b", "t", "{}") is None   # unreachable
    await a.announce_route("gw-b", True)
    assert await platform.server.send_command("gw-b", "t", "{}", command_id="cb") == "gw-a"
    assert await wait_for(lambda: got == [("cb", "gw-b")])
    assert await wait_for(lambda: ("gw-a", "cb", "forwarded") in platform.cmd_acks)


# ---- resilience ----

async def test_reconnects_after_server_restart_and_replays_routes():
    p = Platform()
    await p.start()
    key, c = make_client(p)
    p.register(key, "gw-a")
    await c.start()
    assert await c.wait_connected(3)
    await c.announce_route("gw-b", True)
    await p.stop()

    assert await wait_for(lambda: c.state != CONNECTED, 5)
    with pytest.raises(UplinkUnavailable):
        await c.publish("t", "x")

    p2 = Platform(port=p.port, key=p.key)
    p2.register(key, "gw-a")
    await p2.start()
    try:
        assert await wait_for(lambda: c.connected, 6)
        await c.publish("t", "after", msg_id="m2")
        assert p2.stored[-1].msg_id == "m2"
        assert await wait_for(lambda: p2.server.route_of("gw-b") == "gw-a")
    finally:
        await p2.stop()


async def test_new_session_replaces_old_one(platform):
    key = StaticKey.generate()
    platform.register(key, "gw-b")
    _, c1 = make_client(platform, key=key)
    await c1.start()
    assert await c1.wait_connected(3)
    _, c2 = make_client(platform, key=key)
    await c2.start()
    assert await c2.wait_connected(3)
    assert len(platform.server.sessions()) == 1
    await c2.publish("t", "x")


async def test_silent_peer_is_dropped(platform):
    key, c = make_client(platform)
    platform.register(key, "gw-b")
    platform.server.session_timeout = 0.6
    await c.start()
    assert await c.wait_connected(3)
    c._stopping = True          # freeze heartbeats without saying bye
    c._task.cancel()
    assert await wait_for(lambda: ("gw-b", False) in [(s[0], s[1]) for s in platform.sessions], 4)


async def test_replayed_handshake_does_not_open_a_session(platform):
    key = StaticKey.generate()
    platform.register(key, "gw-b")
    ctx = zmq.asyncio.Context()
    m1 = ClientHandshake(key, platform.key.public).write_message1()
    answers = []
    for _ in range(2):
        s = ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://127.0.0.1:{platform.port}")
        await s.send_multipart([b"H1", m1])
        answers.append(await s.poll(800))
        s.close(0)
    ctx.term()
    assert answers[0] and not answers[1]


async def test_server_can_disconnect_a_peer(platform):
    key, c = make_client(platform)
    platform.register(key, "gw-b")
    await c.start()
    assert await c.wait_connected(3)
    platform.known.clear()                       # key revoked
    assert await platform.server.disconnect("gw-b")
    assert await wait_for(lambda: c.state == REFUSED, 6)
    assert platform.server.route_of("gw-b") is None


async def test_concurrent_route_announcements_share_one_request(platform):
    key_a, a = make_client(platform)
    platform.register(key_a, "gw-a")
    await a.start()
    assert await a.wait_connected(3)
    results = await asyncio.gather(*(a.announce_route("gw-b", True) for _ in range(10)))
    assert results == [True] * 10                     # none "superseded" by another
    assert [r for r in platform.routes if r[2]] == [("gw-a", "gw-b", True)]   # announced once
    await asyncio.gather(*(a.publish("t", str(i), origin="gw-b") for i in range(10)))


# ---- pre-shared key ----

async def test_psk_client_connects_to_psk_server():
    from api.secure import generate_psk
    psk = generate_psk()
    p = Platform()
    await p.start(psk=psk)
    try:
        key, c = make_client(p, psk=psk)
        p.register(key, "gw-b")
        await c.start()
        assert await c.wait_connected(3)
        await c.publish("t", "with psk", msg_id="m1")
        assert p.stored[-1].payload == b"with psk"
    finally:
        await p.stop()


async def test_psk_mismatch_never_connects():
    from api.secure import generate_psk
    p = Platform()
    await p.start(psk=generate_psk())
    try:
        key, plain = make_client(p)                         # no psk at all
        _, wrong = make_client(p, key=key, psk=generate_psk())   # a different psk
        p.register(key, "gw-b")
        await plain.start()
        await wrong.start()
        assert not await plain.wait_connected(1.5)
        assert not wrong.connected
        assert p.stored == [] and p.server.sessions() == [] and p.sessions == []
        assert p.server.metrics["handshakes_rejected"] >= 1     # the plain one, on its first byte
        assert "pre-shared key" in (plain.last_error or "")
    finally:
        await p.stop()

    # and a psk client can't talk to a server without one
    p2 = Platform()
    await p2.start()
    try:
        key, c = make_client(p2, psk=generate_psk())
        p2.register(key, "gw-b")
        await c.start()
        assert not await c.wait_connected(1.5)
        assert p2.stored == [] and p2.server.metrics["handshakes_rejected"] >= 1
    finally:
        await p2.stop()


async def test_stolen_key_without_psk_cannot_replace_the_live_session():
    """psk2: a holder of a gateway's identity.key but not the psk gets through message 1,
    but the server only switches sessions once the new one proves it finished the handshake."""
    from api.secure import generate_psk
    psk = generate_psk()
    p = Platform()
    await p.start(psk=psk)
    try:
        key, real = make_client(p, psk=psk)
        p.register(key, "gw-b")
        await real.start()
        assert await real.wait_connected(3)

        ctx = zmq.asyncio.Context()
        s = ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://127.0.0.1:{p.port}")
        hs = ClientHandshake(key, p.key.public, psk=generate_psk())
        await s.send_multipart([b"H1", hs.write_message1()])
        assert await s.poll(1000)
        kind, m2 = await s.recv_multipart()
        assert kind == b"H2"
        with pytest.raises(Exception):
            hs.read_message2(m2)                      # can't derive the session keys
        await s.send_multipart([b"D", b"\x01" * 40])   # garbage can't confirm the session
        await asyncio.sleep(0.3)
        s.close(0)
        ctx.term()

        assert [x["peer_id"] for x in p.server.sessions()] == ["gw-b"]
        assert real.connected
        await real.publish("t", "still mine", msg_id="m-real")
        assert p.stored[-1].msg_id == "m-real"
        assert ("gw-b", False) not in [(x[0], x[1]) for x in p.sessions]
    finally:
        await p.stop()


async def test_bad_psk_is_rejected_at_construction():
    with pytest.raises(ValueError):
        NikaUplinkServer(StaticKey.generate(), authorize=None, on_data=None, psk="too-short")
    with pytest.raises(ValueError):
        NikaUplinkClient(StaticKey.generate(), StaticKey.generate().public,
                         endpoint="tcp://127.0.0.1:1", psk=b"\x00" * 32)
