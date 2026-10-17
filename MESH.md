# NIKA Mesh — Gateway Relay Specification

**Status:** Experimental (v0.2)
**Package:** `api.mesh`
**Transport:** UDP (discovery) + ZeroMQ ROUTER/DEALER (sessions)

---

## 1. Purpose

Gateways on the same LAN each have their own uplink to the cloud (today MQTT).
When **one** gateway loses its uplink, it sends its upstream traffic through a
healthy neighbour, and cloud commands reach it the same way, until its own uplink
returns.

The mesh never talks to the cloud. The application does that (e.g. Stargate's
MQTT service). NIKA moves messages between gateways and makes the guarantees
listed in section 6.

> The mesh only helps when the outage is **per gateway** (cable, NIC, SIM, port).
> If the whole site loses internet, no neighbour can relay: the application's
> own outbox is the safety net.

---

## 2. Roles

Every node is both, all the time:

| Role | When | Does |
|---|---|---|
| **Origin** | its own uplink is down | finds a relay, sends data through it, receives commands from it |
| **Relay** | its own uplink is up | carries data for up to `relay_capacity` origins, forwards their commands |

Relaying is **single-hop**. A node only accepts relay sessions while its own
uplink is up, so traffic never chains A→B→C and can't loop.

---

## 3. Origin state machine

```
            set_uplink(False)
  DIRECT ───────────────────────▶ SEEKING ◀──────────────┐
    ▲                               │  RELAY_OPEN         │ relay lost uplink,
    │                               │  accepted           │ died, revoked, or
    │                               ▼                     │ stopped answering
    │   uplink stable for       RELAYED ──────────────────┘
    └── recover_hold ─────────── (sends RELAY_CLOSE)
```

- **Relay choice:** among alive peers with `uplink && relay && load < capacity`,
  pick by rendezvous hash of `(origin, peer)`. Different origins prefer different
  relays, so ten stranded gateways spread out instead of piling onto one. The
  choice is kept until it fails.
- **Preferred relays:** `preferred_relays` (operator-chosen node IDs) are tried
  first, in order, whenever they are eligible. Otherwise the hash decides.
- **Cooldown:** a relay that failed, refused or revoked us is skipped for
  `relay_cooldown`, even when it is the only candidate. Retrying it early would
  hot-loop against a relay that keeps refusing (e.g. its broker rejects our
  topics). The exception is `RELAY_NO_SESSION` (usually a relay restart),
  which is re-opened straight away.
- **Recovery hold:** the uplink must stay up for `recover_hold` before the session
  closes. Flapping links don't churn the relay's proxy subscriptions.

---

## 4. Messages

All frames are NIKA envelopes (`nika`, `type`, `id`, `ts`, `sender`, `payload`)
plus `t` (unix time) and `mac`.

| Type | Direction | Payload |
|---|---|---|
| `MESH_BEACON` | UDP broadcast / unicast | `port, host, uplink, relay, load, capacity` |
| `RELAY_OPEN` | origin → relay | `meta` (app-defined dict, e.g. `{"topics": [...]}` to proxy) |
| `RELAY_OPEN_ACK` | relay → origin | `accepted, reason?` |
| `RELAY_PING` / `RELAY_PONG` | origin ↔ relay | `uplink` (on pong) |
| `RELAY_DATA` | origin → relay | `msg_id, seq, topic, payload, qos, priority, ts, sig?` |
| `RELAY_DATA_ACK` | relay → origin | `msg_id, ok, reason?` |
| `RELAY_DOWNLINK` | relay → origin | `command_id, topic, payload, expires_at?` |
| `RELAY_DOWNLINK_ACK` | origin → relay | `command_id, status` (`accepted` / `duplicate` / `expired` / `rejected`) |
| `RELAY_CLOSE` | origin → relay | — (my uplink is back) |
| `RELAY_REVOKE` | relay → origin | `reason` (relay lost uplink, shutting down, unknown session, or app-initiated via `revoke()`) |

Error codes: `RELAY_CAPACITY`, `RELAY_NO_UPLINK`, `RELAY_NOT_ALLOWED`,
`RELAY_NO_SESSION`, `RELAY_RATE_LIMITED`, `RELAY_UPLINK_FAILED`,
`RELAY_ACL_DENIED` (the relay's upstream refuses the origin's topics),
`RELAY_WRONG_BROKER` (relay and origin use different upstreams).

---

## 5. Data path

```
Origin app ──send()──▶ Origin node ──RELAY_DATA──▶ Relay node ──on_uplink_publish(req)──▶ Relay app ──MQTT──▶ cloud
Origin app ◀─on_delivery(ok)── Origin node ◀──RELAY_DATA_ACK── Relay node ◀────── req.ack() (after PUBACK) ─┘
```

- The relay sends `RELAY_DATA_ACK ok` **only after the relay app calls
  `req.ack()`**. The app should call it once its MQTT publish is confirmed.
- **Priority:** lower number goes first. `priority=0` (alarms, fire) jumps the
  queue and bypasses the relay's per-origin rate limit.
- **Retries:** no ACK within `ack_timeout` means a resend, up to `max_attempts`.
  Rate-limited messages back off and don't count as attempts.
- **Failover:** in-flight messages are re-queued for the next relay.

## 6. Guarantees

| Behaviour | Guarantee |
|---|---|
| Upstream data | **At-least-once to the cloud**, with a stable `msg_id`. Duplicates are possible across failover and recovery, so consumers dedupe on `msg_id` |
| Same relay, lost ACK | Not published twice (the relay remembers published `msg_id`s for 5 min) |
| Delivery outcome | Every `send()` gets exactly one `on_delivery(msg_id, ok, reason)` |
| Commands | **At-most-once execution** per `command_id` within `command_dedupe_ttl` (shared `CommandGuard`). Never executed after `expires_at` |
| Ordering | Per-priority FIFO per origin. No ordering across failover |
| Persistence | None. Messages that fail (`NO_RELAY`, `UNDELIVERED`, `UPLINK_RESTORED`, `STOPPED`) go back to the app, which keeps them in its outbox |

## 7. Security

- **Link MAC:** every frame, beacons included, carries HMAC-SHA256 over the
  canonical JSON using the site-wide `mesh_key`. Frames with a bad MAC,
  timestamps beyond `max_clock_skew`, or a repeated frame `id` are dropped.
- **Identity binding:** the ZeroMQ identity must equal the envelope's `sender`.
  Origins only accept frames from the relay they chose.
- **Allow-list:** `allowed_peers` limits which node IDs may join.
- **Origin signature (optional):** give the node an `OriginSigner` (Ed25519) and
  every message is signed over `(origin, msg_id, topic, payload, ts)`. The relay
  passes `req.signature` and `req.ts` upstream, and the platform checks them with
  `verify_origin()`. A compromised relay can then drop data but can't forge it.
- **Not provided:** encryption on the LAN. Frames are authenticated, not
  confidential. Run the mesh on a trusted VLAN, or add CURVE later.

---

## 8. Application contract

```python
from api.mesh import MeshConfig, MeshNode, NoRelayAvailable

node = MeshNode(MeshConfig(node_id="sg-B", mesh_key=SITE_KEY))

# ---- relay role ----
node.on_uplink_publish(lambda req: publish_to_mqtt(req))   # then req.ack() / req.fail()
node.on_session(lambda origin, active: proxy_subscribe(origin) if active else proxy_unsubscribe(origin))
node.session_meta(origin)          # what the origin sent in RELAY_OPEN (readable inside on_session)
node.on_command_result(lambda origin, cid, status: ...)
# when a command arrives on a proxied topic:
node.forward_command(origin, topic, payload, command_id=cid, expires_at=exp)
# when we can't serve a neighbour after all (e.g. our broker refuses its topics):
node.revoke(origin, "RELAY_ACL_DENIED")   # it fails over and cools us down

# ---- origin role ----
node.on_command(lambda cmd: execute(cmd))                  # already deduped + expiry-checked
node.on_delivery(lambda msg_id, ok, reason: mark_outbox(msg_id, ok))
node.on_state(lambda old, new, relay: ...)
node.set_session_meta({"topics": my_command_topics})      # sent with every RELAY_OPEN
node.set_uplink(mqtt_connected)                           # call on every connect/disconnect
node.send(topic, payload, priority=5, msg_id=outbox_row_id)  # only while state == RELAYED

# commands arriving DIRECTLY over MQTT must use the same guard:
if node.commands.check(cid, exp) == "accepted": execute(...)

node.start()
```

Callbacks run on the mesh thread. Don't block in them. In an asyncio app, use
`loop.call_soon_threadsafe(...)`.

---

## 9. Configuration

See `api/mesh/config.py`. The main settings:

| Setting | Default | Notes |
|---|---|---|
| `mesh_port` | 47801 | TCP, relay sessions |
| `discovery_port` | 47800 | UDP beacons |
| `broadcast_addrs` | `255.255.255.255` | use the subnet broadcast on multi-NIC boxes |
| `static_peers` | `[]` | `host:udp_port`, for networks that block broadcast |
| `relay_capacity` | 4 | origins carried at once |
| `relay_rate_limit` | 200/s | per origin, priority 0 exempt |
| `relay_timeout` | 6 s | origin fails over after this long without PONG |
| `recover_hold` | 5 s | uplink must be stable before going direct |
| `queue_timeout` | 30 s | queued messages given back if no relay appears |
| `relay_cooldown` | 30 s | a relay that failed, refused or revoked us is skipped this long |
| `preferred_relays` | `[]` | node IDs to try first, in order |

Docker: the nodes need `network_mode: host` (Stargate already uses it) for
broadcast discovery.

---

## 10. Demo and tests

```bash
python -m examples.mesh_demo
python -m pytest test/test_mesh.py -v
```
