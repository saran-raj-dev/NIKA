"""
Mesh tests. Every node runs on 127.0.0.1 with its own ports and finds the
others through static (unicast) beacons.

Run:  python -m pytest test/test_mesh.py -v
"""
import socket
import threading
import time

import pytest

from api.mesh import (
    ACCEPT, DUPLICATE, EXPIRED, DIRECT, RELAYED, SEEKING,
    CommandGuard, MeshConfig, MeshNode, NoRelayAvailable,
)
from api.mesh.security import seal, unseal

KEY = b"test-mesh-key-0123456789"


# =================================================
# Helpers
# =================================================

def free_port(kind=socket.SOCK_STREAM) -> int:
    s = socket.socket(socket.AF_INET, kind)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_for(cond, timeout=5.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


class Mesh:
    """Builds fully-connected test meshes and tears them down."""

    def __init__(self):
        self.nodes: list[MeshNode] = []

    def build(self, *ids, key=KEY, **overrides):
        ports = {i: (free_port(), free_port(socket.SOCK_DGRAM)) for i in ids}
        nodes = []
        for i in ids:
            cfg = dict(
                node_id=i,
                mesh_key=key,
                bind_host="127.0.0.1",
                mesh_port=ports[i][0],
                discovery_port=ports[i][1],
                broadcast_addrs=[],
                static_peers=[f"127.0.0.1:{ports[o][1]}" for o in ids if o != i],
                beacon_interval=0.2,
                peer_timeout=1.0,
                keepalive_interval=0.3,
                relay_timeout=1.2,
                open_timeout=1.0,
                recover_hold=0.5,
                ack_timeout=0.5,
                downlink_retry=0.3,
            )
            cfg.update(overrides.get(i, {}))
            node = MeshNode(MeshConfig(**cfg))
            nodes.append(node)
            self.nodes.append(node)
        return nodes

    def start(self, *nodes):
        for n in nodes:
            n.start()

    def close(self):
        for n in self.nodes:
            n.stop()


@pytest.fixture
def mesh():
    m = Mesh()
    yield m
    m.close()


def auto_relay(node, published=None, fail=False):
    """Make `node` a relay whose 'cloud publish' always succeeds (or fails)."""
    published = published if published is not None else []

    def handler(req):
        if fail:
            req.fail()
            return
        published.append((req.origin, req.msg_id, req.topic, req.payload))
        req.ack()

    node.on_uplink_publish(handler)
    return published


def collect_deliveries(node):
    results = {}
    node.on_delivery(lambda mid, ok, reason: results.__setitem__(mid, (ok, reason)))
    return results


def go_relayed(origin, timeout=5.0):
    origin.set_uplink(False)
    assert wait_for(lambda: origin.state == RELAYED, timeout), f"state={origin.state}"


# =================================================
# Discovery
# =================================================

def test_peers_discover_each_other(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    mesh.start(a, b)

    assert wait_for(lambda: [p["node_id"] for p in a.peers()] == ["sg-B"])
    assert wait_for(lambda: [p["node_id"] for p in b.peers()] == ["sg-A"])
    assert b.peers()[0]["uplink"] is True


def test_node_with_wrong_key_is_ignored(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-B": {"mesh_key": b"some-other-key-xxxxxxxx"}})
    mesh.start(a, b)

    time.sleep(1.0)
    assert a.peers() == []
    assert a.metrics["frames_rejected"] > 0


def test_peer_disappears_after_timeout(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    mesh.start(a, b)
    assert wait_for(lambda: len(a.peers()) == 1)

    b.stop()
    assert wait_for(lambda: a.peers() == [], timeout=3)


# =================================================
# Upstream relay
# =================================================

def test_origin_relays_data_through_neighbour(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    published = auto_relay(a)
    sessions = []
    a.on_session(lambda origin, active: sessions.append((origin, active)))
    results = collect_deliveries(b)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)

    go_relayed(b)
    assert b.relay_id == "sg-A"
    assert wait_for(lambda: sessions == [("sg-B", True)])

    b.send("stargate/gateway/sg-B/data/AHU-1", {"t": 13.2}, msg_id="sg-B:1")
    assert wait_for(lambda: results.get("sg-B:1") == (True, None))
    assert published == [("sg-B", "sg-B:1", "stargate/gateway/sg-B/data/AHU-1", {"t": 13.2})]


def test_send_while_direct_raises(mesh):
    (a,) = mesh.build("sg-A")
    mesh.start(a)
    with pytest.raises(NoRelayAvailable):
        a.send("t", {})


def test_relay_does_not_publish_same_message_twice(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    published = auto_relay(a)
    results = collect_deliveries(b)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    b.send("t", {"v": 1}, msg_id="sg-B:7")
    assert wait_for(lambda: "sg-B:7" in results)
    b.send("t", {"v": 1}, msg_id="sg-B:7")  # e.g. outbox replays the same row
    time.sleep(0.5)

    assert len(published) == 1


def test_failed_uplink_publish_is_retried_then_reported(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-B": {"max_attempts": 2}})
    auto_relay(a, fail=True)
    results = collect_deliveries(b)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    b.send("t", {}, msg_id="m1")
    assert wait_for(lambda: "m1" in results)
    ok, reason = results["m1"]
    assert ok is False and reason == "RELAY_UPLINK_FAILED"
    assert a.metrics["relay_msgs_failed"] == 2


def test_alarms_go_before_telemetry(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-B": {"max_inflight": 1}})
    order = []
    gate = threading.Event()

    def slow_publish(req):
        order.append(req.msg_id)
        threading.Thread(target=lambda: (gate.wait(2), req.ack()), daemon=True).start()

    a.on_uplink_publish(slow_publish)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    b.send("t", {}, msg_id="tele-1", priority=5)   # occupies the single in-flight slot
    assert wait_for(lambda: order == ["tele-1"])
    b.send("t", {}, msg_id="tele-2", priority=5)
    b.send("t", {}, msg_id="alarm", priority=0)
    time.sleep(0.1)
    gate.set()

    assert wait_for(lambda: len(order) == 3)
    assert order == ["tele-1", "alarm", "tele-2"]


def test_rate_limit_spares_priority_zero(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-A": {"relay_rate_limit": 2.0}})
    published = auto_relay(a)
    results = collect_deliveries(b)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    for i in range(5):
        b.send("t", {}, msg_id=f"tele-{i}", priority=5)
    b.send("t", {}, msg_id="alarm", priority=0)

    assert wait_for(lambda: "alarm" in results, timeout=2)
    assert results["alarm"] == (True, None)
    # telemetry is slowed down, not lost
    assert wait_for(lambda: len(published) == 6, timeout=6)


# =================================================
# Commands (downlink)
# =================================================

def test_command_reaches_origin_once(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    auto_relay(a)
    executed, statuses = [], []
    b.on_command(lambda cmd: executed.append((cmd.command_id, cmd.payload, cmd.relay_id)))
    a.on_command_result(lambda origin, cid, status: statuses.append((origin, cid, status)))
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)
    assert wait_for(lambda: a.sessions() == ["sg-B"])

    cmd = {"point": "AHU-1.sp", "value": 22}
    assert a.forward_command("sg-B", "stargate/gateway/sg-B/cmd", cmd,
                             command_id="c1", expires_at=time.time() + 30)
    assert wait_for(lambda: len(statuses) == 1)
    a.forward_command("sg-B", "stargate/gateway/sg-B/cmd", cmd,
                      command_id="c1", expires_at=time.time() + 30)
    assert wait_for(lambda: len(statuses) == 2)

    assert executed == [("c1", cmd, "sg-A")]
    assert statuses == [("sg-B", "c1", "delivered"), ("sg-B", "c1", "duplicate")]


def test_expired_command_is_not_executed(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    auto_relay(a)
    executed, statuses = [], []
    b.on_command(lambda cmd: executed.append(cmd))
    a.on_command_result(lambda origin, cid, status: statuses.append(status))
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)
    assert wait_for(lambda: a.sessions() == ["sg-B"])

    # already expired on arrival: the relay gives up without delivering
    a.forward_command("sg-B", "x", {}, command_id="old", expires_at=time.time() - 1)
    assert wait_for(lambda: statuses == ["undelivered"])
    assert executed == []


def test_forward_command_without_session(mesh):
    (a,) = mesh.build("sg-A")
    mesh.start(a)
    assert a.forward_command("sg-B", "x", {}, command_id="c") is False


def test_direct_and_relayed_paths_share_one_guard(mesh):
    """During handover the same command can arrive over MQTT and through the relay."""
    a, b = mesh.build("sg-A", "sg-B")
    auto_relay(a)
    executed, statuses = [], []
    b.on_command(lambda cmd: executed.append(cmd.command_id))
    a.on_command_result(lambda origin, cid, status: statuses.append(status))
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)
    assert wait_for(lambda: a.sessions() == ["sg-B"])

    assert b.commands.check("c9", time.time() + 30) == ACCEPT  # arrived directly over MQTT
    a.forward_command("sg-B", "x", {}, command_id="c9", expires_at=time.time() + 30)

    assert wait_for(lambda: statuses == ["duplicate"])
    assert executed == []


# =================================================
# Failover & recovery
# =================================================

def test_fails_over_when_relay_loses_uplink(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C")
    auto_relay(a)
    auto_relay(c)
    mesh.start(a, b, c)
    assert wait_for(lambda: len(b.peers()) == 2)

    go_relayed(b)
    first = b.relay_id
    other = c if first == "sg-A" else a
    relay = a if first == "sg-A" else c

    relay.set_uplink(False)
    assert wait_for(lambda: b.state == RELAYED and b.relay_id == other.cfg.node_id, timeout=6)
    assert b.metrics["failovers"] >= 1


def test_fails_over_when_relay_dies(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C")
    auto_relay(a)
    auto_relay(c)
    results = collect_deliveries(b)
    mesh.start(a, b, c)
    assert wait_for(lambda: len(b.peers()) == 2)
    go_relayed(b)

    first = b.relay_id
    (a if first == "sg-A" else c).stop()

    assert wait_for(lambda: b.state == RELAYED and b.relay_id != first, timeout=6)
    b.send("t", {}, msg_id="after")
    assert wait_for(lambda: results.get("after") == (True, None))


def test_no_relay_available_returns_messages(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-B": {"queue_timeout": 0.5}})
    auto_relay(a)
    results = collect_deliveries(b)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    a.set_uplink(False)  # the only relay goes dark
    assert wait_for(lambda: b.state == SEEKING, timeout=4)
    with pytest.raises(NoRelayAvailable):
        b.send("t", {})


def test_relay_refuses_when_full(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C", **{"sg-A": {"relay_capacity": 1}})
    auto_relay(a)
    mesh.start(a, b, c)
    assert wait_for(lambda: len(b.peers()) == 2 and len(c.peers()) == 2)

    b.set_uplink(False)
    c.set_uplink(False)  # both stranded, only A can relay, and only for one
    assert wait_for(lambda: RELAYED in (b.state, c.state), timeout=4)
    time.sleep(1.0)

    assert sorted([b.state, c.state]) == [RELAYED, SEEKING]
    assert len(a.sessions()) == 1


def test_recovery_waits_for_stable_uplink(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-B": {"recover_hold": 1.0}})
    auto_relay(a)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    b.set_uplink(True)
    time.sleep(0.3)
    b.set_uplink(False)  # flapped before the hold expired
    time.sleep(1.2)
    assert b.state == RELAYED and a.sessions() == ["sg-B"]

    b.set_uplink(True)
    assert wait_for(lambda: b.state == DIRECT, timeout=3)
    assert wait_for(lambda: a.sessions() == [])


def test_session_expires_if_origin_goes_silent(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-A": {"session_timeout": 0.8}})
    auto_relay(a)
    ended = []
    a.on_session(lambda origin, active: None if active else ended.append(origin))
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    # simulate B vanishing without RELAY_CLOSE (power cut)
    b._running = False
    b._thread.join(2)
    assert wait_for(lambda: ended == ["sg-B"], timeout=3)


# =================================================
# Units
# =================================================

def test_command_guard():
    g = CommandGuard()
    assert g.check("a", time.time() + 10) == ACCEPT
    assert g.check("a", time.time() + 10) == DUPLICATE
    assert g.check("b", time.time() - 1) == EXPIRED
    assert g.check("b", time.time() + 10) == DUPLICATE
    assert g.check("c") == ACCEPT


def test_seal_detects_tampering_and_staleness():
    frame = {"type": "X", "t": time.time(), "payload": {"v": 1}}
    raw = seal(frame, KEY)
    assert unseal(raw, KEY, 60)["payload"] == {"v": 1}

    with pytest.raises(ValueError):
        unseal(raw.replace(b'"v": 1', b'"v": 2'), KEY, 60)
    with pytest.raises(ValueError):
        unseal(raw, b"wrong-key-wrong-key-wrong", 60)
    with pytest.raises(ValueError):
        unseal(seal({"type": "X", "t": time.time() - 600}, KEY), KEY, 60)


def test_origin_signature_roundtrip():
    pytest.importorskip("cryptography")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from api.mesh import OriginSigner, verify_origin

    key = Ed25519PrivateKey.generate()
    priv = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())
    pub = key.public_key().public_bytes(serialization.Encoding.PEM,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)

    sig = OriginSigner(priv).sign("sg-B", "m1", "t", {"v": 1}, 123.0)
    assert verify_origin(pub, "sg-B", "m1", "t", {"v": 1}, 123.0, sig)
    assert not verify_origin(pub, "sg-A", "m1", "t", {"v": 1}, 123.0, sig)
    assert not verify_origin(pub, "sg-B", "m1", "t", {"v": 2}, 123.0, sig)


def test_signature_travels_through_relay(mesh):
    pytest.importorskip("cryptography")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from api.mesh import OriginSigner, verify_origin

    key = Ed25519PrivateKey.generate()
    priv = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())
    pub = key.public_key().public_bytes(serialization.Encoding.PEM,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)

    a, b = mesh.build("sg-A", "sg-B")
    b.signer = OriginSigner(priv)
    seen = []

    def publish(req):
        seen.append(req)
        req.ack()

    a.on_uplink_publish(publish)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    b.send("t", {"v": 1}, msg_id="m1")
    assert wait_for(lambda: len(seen) == 1)
    r = seen[0]
    assert verify_origin(pub, r.origin, r.msg_id, r.topic, r.payload, r.ts, r.signature)


def test_command_guard_forgets_after_ttl():
    g = CommandGuard(ttl=0.2)
    assert g.check("x") == ACCEPT
    assert g.check("x") == DUPLICATE
    time.sleep(0.3)
    assert g.check("x") == ACCEPT


def test_session_meta_reaches_relay(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    auto_relay(a)
    seen = []
    a.on_session(lambda origin, active: seen.append((active, a.session_meta(origin))))
    b.set_session_meta({"topics": ["stargate/gateway/sg-B/cmd"]})
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)
    assert wait_for(lambda: seen == [(True, {"topics": ["stargate/gateway/sg-B/cmd"]})])

    b.set_uplink(True)
    # still readable inside the closing callback, gone afterwards
    assert wait_for(lambda: len(seen) == 2)
    assert seen[1] == (False, {"topics": ["stargate/gateway/sg-B/cmd"]})
    assert a.session_meta("sg-B") == {}


def test_preferred_relay_wins(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C", **{"sg-B": {"preferred_relays": ["sg-C"]}})
    auto_relay(a)
    auto_relay(c)
    mesh.start(a, b, c)
    assert wait_for(lambda: len(b.peers()) == 2)
    go_relayed(b)
    assert b.relay_id == "sg-C"


def test_revoke_sends_origin_elsewhere_and_cools_down(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C",
                         **{"sg-B": {"preferred_relays": ["sg-A"], "relay_cooldown": 30.0}})
    auto_relay(a)
    auto_relay(c)
    mesh.start(a, b, c)
    assert wait_for(lambda: len(b.peers()) == 2)
    go_relayed(b)
    assert b.relay_id == "sg-A"

    a.revoke("sg-B", "RELAY_ACL_DENIED")
    assert wait_for(lambda: b.state == RELAYED and b.relay_id == "sg-C", timeout=4)
    assert a.sessions() == []


def test_only_relay_in_cooldown_is_not_hammered(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-B": {"relay_cooldown": 30.0}})
    auto_relay(a)
    opened = []
    a.on_session(lambda origin, active: opened.append(active))
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    a.revoke("sg-B", "RELAY_ACL_DENIED")
    assert wait_for(lambda: b.state == SEEKING)
    time.sleep(1.5)
    assert b.state == SEEKING
    assert opened.count(True) == 1   # no reconnect storm while cooling down


# =================================================
# Channels (several independent uplinks per node)
# =================================================

def test_named_channels_replace_the_implicit_default(mesh):
    (a,) = mesh.build("sg-A")
    mesh.start(a)
    a.set_uplink(True, "mqtt")
    a.set_uplink(True, "nika")
    assert wait_for(lambda: a.uplinks() == {"mqtt": True, "nika": True})


def test_relay_carries_only_the_channel_that_is_down(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    seen = []

    def publish(req):
        seen.append((req.msg_id, req.channel))
        req.ack()

    a.on_uplink_publish(publish)
    results = collect_deliveries(b)
    mesh.start(a, b)
    for n in (a, b):
        n.set_uplink(True, "mqtt")
        n.set_uplink(True, "nika")
    assert wait_for(lambda: len(b.peers()) == 1 and b.peers()[0]["uplinks"] == {"mqtt": True, "nika": True})

    b.set_uplink(False, "nika")           # NIKA down, MQTT still fine
    assert wait_for(lambda: b.state == RELAYED)
    b.send("t", "{}", msg_id="n1", channel="nika")
    assert wait_for(lambda: results.get("n1") == (True, None))
    assert seen == [("n1", "nika")]


def test_relay_choice_prefers_full_channel_coverage(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C", **{"sg-B": {"preferred_relays": ["sg-A"]}})
    auto_relay(a)
    auto_relay(c)
    mesh.start(a, b, c)
    for n in (a, b, c):
        n.set_uplink(True, "mqtt")
        n.set_uplink(True, "nika")
    a.set_uplink(False, "nika")           # A can't do NIKA right now
    assert wait_for(lambda: len(b.peers()) == 2)
    assert wait_for(lambda: any(p["node_id"] == "sg-A" and not p["uplinks"]["nika"] for p in b.peers()))

    b.set_uplink(False, "nika")
    assert wait_for(lambda: b.state == RELAYED)
    assert b.relay_id == "sg-C"           # preferred A lacks the channel B needs


def test_no_relay_with_the_needed_channel(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    auto_relay(a)
    mesh.start(a, b)
    a.set_uplink(True, "mqtt")
    a.set_uplink(False, "nika")
    b.set_uplink(True, "mqtt")
    b.set_uplink(False, "nika")
    time.sleep(1.5)
    assert b.state == SEEKING


def test_relay_losing_the_needed_channel_triggers_failover(mesh):
    a, b, c = mesh.build("sg-A", "sg-B", "sg-C", **{"sg-B": {"preferred_relays": ["sg-A"]}})
    auto_relay(a)
    auto_relay(c)
    mesh.start(a, b, c)
    for n in (a, b, c):
        n.set_uplink(True, "mqtt")
        n.set_uplink(True, "nika")
    assert wait_for(lambda: len(b.peers()) == 2)
    b.set_uplink(False, "nika")
    assert wait_for(lambda: b.state == RELAYED and b.relay_id == "sg-A")

    a.set_uplink(False, "nika")           # A keeps MQTT, loses NIKA
    assert wait_for(lambda: b.state == RELAYED and b.relay_id == "sg-C", timeout=6)
    assert a.uplinks() == {"mqtt": True, "nika": False}


def test_relay_app_failure_on_one_channel_keeps_the_relay(mesh):
    """The relay app can't deliver one message on a channel (e.g. its uplink just
    blipped): the origin gets that message back, but keeps the relay."""
    a, b = mesh.build("sg-A", "sg-B")

    def flaky(req):
        if req.msg_id == "m-bad":
            req.fail("RELAY_NO_UPLINK")
        else:
            req.ack()

    a.on_uplink_publish(flaky)
    results = collect_deliveries(b)
    mesh.start(a, b)
    for n in (a, b):
        n.set_uplink(True, "mqtt")
        n.set_uplink(True, "nika")
    assert wait_for(lambda: len(b.peers()) == 1 and b.peers()[0]["uplinks"].get("nika"))
    b.set_uplink(False, "nika")
    assert wait_for(lambda: b.state == RELAYED)

    b.send("t", "{}", msg_id="m-bad", channel="nika")
    assert wait_for(lambda: results.get("m-bad") == (False, "RELAY_NO_UPLINK"))
    b.send("t", "{}", msg_id="m-ok", channel="nika")
    assert wait_for(lambda: results.get("m-ok") == (True, None))
    assert b.state == RELAYED and b.relay_id == "sg-A" and b.metrics["failovers"] == 0


# =================================================
# Origin identity: proof of key possession
# =================================================

from api.mesh import identity as mesh_identity
from api.secure import StaticKey


class _Impostor:
    """Claims someone else's public key but only holds its own private key."""

    def __init__(self, victim: StaticKey):
        self._own = StaticKey.generate()
        self.public_b64 = victim.public_b64

    def dh(self, peer_public):
        return self._own.dh(peer_public)


def _relay_port(node):
    return node.cfg.mesh_port


def _raw_frame(sender, msg_type, payload, key=KEY):
    from api.envelope import envelope
    frame = envelope(msg_type=msg_type, payload=payload, sender=sender)
    frame["t"] = time.time()
    return seal(frame, key)


def test_origin_proves_its_identity_to_the_relay(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    key = StaticKey.generate()
    auto_relay(a)
    seen = []
    a.on_session(lambda origin, active: seen.append((origin, active, a.session_identity(origin))))
    b.set_identity(key)
    b.set_session_meta({"nika": {"server": "abcd1234"}})
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)

    assert a.session_identity("sg-B") == key.public
    assert a.session_meta("sg-B") == {"nika": {"server": "abcd1234"}}
    assert seen == [("sg-B", True, key.public)]       # readable inside on_session
    assert a.metrics["identity_proofs_ok"] == 1 and a.metrics["identity_proofs_failed"] == 0

    b.set_uplink(True)
    assert wait_for(lambda: len(seen) == 2)
    assert seen[1] == ("sg-B", False, key.public)     # still readable while closing
    assert a.session_identity("sg-B") is None


def test_origin_without_identity_is_carried_but_unproven(mesh):
    a, b = mesh.build("sg-A", "sg-B")
    auto_relay(a)
    b.set_session_meta({"nika": {"public_key": StaticKey.generate().public_b64}})  # just a claim
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)
    assert a.sessions() == ["sg-B"]
    assert a.session_identity("sg-B") is None


def test_impostor_claiming_another_gateways_key_is_refused(mesh):
    a, x = mesh.build("sg-A", "sg-X", **{"sg-X": {"relay_cooldown": 30.0}})
    victim = StaticKey.generate()
    auto_relay(a)
    opened = []
    a.on_session(lambda origin, active: opened.append(origin))
    x.set_identity(_Impostor(victim))                  # has the mesh key, not victim's private key
    mesh.start(a, x)
    assert wait_for(lambda: len(x.peers()) == 1)
    x.set_uplink(False)

    assert wait_for(lambda: a.metrics["identity_proofs_failed"] >= 1)
    time.sleep(0.5)
    assert x.state == SEEKING
    assert opened == [] and a.sessions() == [] and a.session_identity("sg-X") is None


def test_relay_can_require_a_proven_identity(mesh):
    a, b = mesh.build("sg-A", "sg-B", **{"sg-A": {"require_identity": True},
                                        "sg-B": {"relay_cooldown": 0.5}})
    auto_relay(a)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    b.set_uplink(False)
    time.sleep(1.0)
    assert b.state == SEEKING and a.sessions() == []   # refused: RELAY_IDENTITY_REQUIRED

    key = StaticKey.generate()
    b.set_identity(key)
    assert wait_for(lambda: b.state == RELAYED, 4)
    assert a.session_identity("sg-B") == key.public


def test_takeover_without_proof_does_not_inherit_the_identity(mesh):
    """Someone with the mesh key re-opens sg-B's session under its node id (ROUTER
    handover) without proving the key: the relay ends the proven session first."""
    import zmq

    a, b = mesh.build("sg-A", "sg-B")
    key = StaticKey.generate()
    auto_relay(a)
    events = []
    a.on_session(lambda origin, active: events.append((active, a.session_identity(origin))))
    b.set_identity(key)
    mesh.start(a, b)
    assert wait_for(lambda: len(b.peers()) == 1)
    go_relayed(b)
    assert a.session_identity("sg-B") == key.public

    ctx = zmq.Context()
    s = ctx.socket(zmq.DEALER)
    s.setsockopt(zmq.LINGER, 0)
    s.setsockopt_string(zmq.IDENTITY, "sg-B")
    s.connect(f"tcp://127.0.0.1:{_relay_port(a)}")
    try:
        time.sleep(0.2)
        s.send(_raw_frame("sg-B", "RELAY_OPEN", {"meta": {"evil": True}}))
        assert wait_for(lambda: events[-1:] == [(True, None)], 3), events
        assert (False, key.public) in events
        assert a.session_identity("sg-B") is None
    finally:
        s.close(0)
        ctx.term()


def test_forwarded_challenge_is_useless_to_another_relay():
    """Unit: a proof binds relay id, origin id, the challenge and the meta."""
    key = StaticKey.generate()
    meta = {"topics": ["a"]}

    def challenge(relay="sg-A", origin="sg-B"):
        return mesh_identity.Challenge(relay_id=relay, origin_id=origin, identity=key.public, meta=meta)

    def answer(ch, relay="sg-A", origin="sg-B", m=meta):
        return mesh_identity.prove(key, relay_id=relay, origin_id=origin, challenge=ch.payload(), meta=m)

    ch = challenge()
    assert ch.verify(answer(ch)) is True
    assert ch.verify(answer(ch)) is False                          # one shot

    ch = challenge()
    assert not ch.verify(answer(ch, relay="sg-EVIL"))              # proof made for another relay
    ch = challenge()
    assert not ch.verify(answer(ch, origin="sg-C"))                # for another node id
    ch = challenge()
    assert not ch.verify(answer(ch, m={"topics": ["b"]}))          # with different meta
    old = challenge()
    ch = challenge()
    assert not ch.verify(answer(old))                              # replayed from an older challenge
    ch = challenge()
    assert not ch.verify(None) and not challenge().verify("00" * 32)

    imp = _Impostor(key)
    ch = challenge()
    assert not ch.verify(mesh_identity.prove(imp, relay_id="sg-A", origin_id="sg-B",
                                             challenge=ch.payload(), meta=meta))


def test_malformed_challenges_and_claims():
    key = StaticKey.generate()
    for bad in ({}, {"n": 1, "e": 2}, {"n": "AAAA", "e": "AAAA"},
                {"n": mesh_identity._b64(b"\x01" * 32), "e": mesh_identity._b64(b"\x00" * 32)}):
        assert mesh_identity.prove(key, relay_id="r", origin_id="o", challenge=bad, meta={}) is None
    with pytest.raises(ValueError):
        mesh_identity.Challenge(relay_id="r", origin_id="o", identity=b"short", meta={})
    # a low-order "public key" can never be proven
    ch = mesh_identity.Challenge(relay_id="r", origin_id="o", identity=b"\x00" * 32, meta={})
    assert ch.verify("00" * 32) is False
