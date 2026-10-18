# mesh_demo.py
"""
Two gateways, A and B, on one machine. B loses its uplink, relays through A,
receives a writeback through A, then recovers.

Run from the repo root:
    python -m examples.mesh_demo
"""
import logging
import threading
import time

from nika.mesh import MeshConfig, MeshNode

KEY = b"site-17-mesh-key-change-me"


class FakeCloud:
    """Stands in for the MQTT broker + platform."""

    def publish(self, via: str, topic: str, payload, relayed_for: str | None = None):
        tag = f" (relayed by {via} for {relayed_for})" if relayed_for else ""
        print(f"  ☁  {topic} {payload}{tag}")


def make_node(node_id, mesh_port, disc_port, peer_disc_port):
    return MeshNode(MeshConfig(
        node_id=node_id,
        mesh_key=KEY,
        bind_host="127.0.0.1",
        mesh_port=mesh_port,
        discovery_port=disc_port,
        broadcast_addrs=[],                       # localhost demo: no broadcast
        static_peers=[f"127.0.0.1:{peer_disc_port}"],
        beacon_interval=0.5,
        peer_timeout=2.0,
        recover_hold=2.0,
    ))


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cloud = FakeCloud()

    a = make_node("sg-A", 47811, 47810, 47820)
    b = make_node("sg-B", 47821, 47820, 47810)

    # ---- A: relay role ----
    def a_publish(req):
        cloud.publish("sg-A", req.topic, req.payload, relayed_for=req.origin)
        req.ack()  # in Stargate: ack after the MQTT PUBACK

    a.on_uplink_publish(a_publish)
    a.on_session(lambda origin, active: print(
        f"  A: {'subscribing to' if active else 'dropping'} {origin}'s command topics"))
    a.on_command_result(lambda origin, cid, status: print(
        f"  A: command {cid} -> {origin}: {status}"))

    # ---- B: origin role ----
    delivered = threading.Event()

    def b_command(cmd):
        print(f"  B: executing {cmd.topic} {cmd.payload}")
        # the ack is ordinary upstream data, so it travels back through A too
        b.send(f"stargate/gateway/sg-B/ack/cmd",
               {"command_id": cmd.command_id, "ok": True}, priority=1)

    b.on_command(b_command)
    b.on_delivery(lambda msg_id, ok, reason: delivered.set() if ok else print(
        f"  B: {msg_id} not delivered ({reason}) — stays in outbox"))
    b.on_state(lambda old, new, relay: print(f"  B: {old} -> {new}" +
                                             (f" via {relay}" if relay else "")))

    a.start()
    b.start()

    print("\n== Both gateways online, discovering each other ==")
    time.sleep(1.5)
    print(f"  B sees peers: {[p['node_id'] for p in b.peers()]}")

    print("\n== 14:14 B's cable is cut ==")
    b.set_uplink(False)
    for _ in range(50):
        if b.state == "relayed":
            break
        time.sleep(0.1)

    print("\n== B keeps publishing telemetry through A ==")
    for i in range(3):
        b.send("stargate/gateway/sg-B/data/AHU-1", {"supply_temp": 13.0 + i * 0.1},
               msg_id=f"sg-B:{4821 + i}")
        time.sleep(0.2)
    delivered.wait(2)

    print("\n== 14:20 Operator sends a writeback to B; A receives it on B's topic ==")
    a.forward_command("sg-B", "stargate/gateway/sg-B/cmd",
                      {"point": "AHU-1.setpoint", "value": 22.0},
                      command_id="cmd-991", expires_at=time.time() + 60)
    time.sleep(1)

    print("\n== The broker redelivers the same command (QoS 1) ==")
    a.forward_command("sg-B", "stargate/gateway/sg-B/cmd",
                      {"point": "AHU-1.setpoint", "value": 22.0},
                      command_id="cmd-991", expires_at=time.time() + 60)
    time.sleep(1)

    print("\n== 14:41 Cable repaired ==")
    b.set_uplink(True)
    time.sleep(3)
    print(f"  B state: {b.state}, A sessions: {a.sessions()}")

    b.stop()
    a.stop()


if __name__ == "__main__":
    main()
