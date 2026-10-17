# NIKA Protocol

**Version:** 1.0  
**Status:** Internal Production  
**Transport:** ZeroMQ  

NIKA is a **live-first messaging protocol** designed for internal systems
requiring low latency, high fan-out, and honest delivery guarantees.

> **Using NIKA with Stargate and ATLAS (or another platform)?** Start with the guide:
> [`docs/NIKA_GUIDE.md`](docs/NIKA_GUIDE.md), or the interactive version [`docs/nika_guide.html`](docs/nika_guide.html)
> (with screenshots, a pairing wizard and flow simulators). It has a user part and a developer part.

---

## Core Models

NIKA defines three independent interaction models, plus a gateway mesh and a platform uplink.

---

## 1. REQ / RES (Control Plane)

Used for:
- Commands
- Queries
- Health checks
- Metadata

Properties:
- Authenticated
- Idempotent
- Deterministic
- Explicit errors

No streaming data is allowed here.

---

## 2. PUB / SUB (Event Plane)

Used for:
- Alerts
- Notifications
- System events

Properties:
- Fire-and-forget
- Best-effort delivery
- No replay
- No persistence

Subscribers may miss messages.

---

## 3. STREAMS (Data Plane)

Used for:
- Telemetry
- Sensors
- Dashboards
- Live metrics

Properties:
- Live-only
- Sequence numbers
- Drop-tolerant
- Per-client flow control

No replay or persistence is guaranteed.

---

## 4. MESH (Gateway Relay)

Used for:
- Keeping a gateway online when its own uplink fails
- Relaying its data and cloud commands through a healthy LAN neighbour

Properties:
- Single-hop, automatic failover and recovery
- At-least-once upstream with stable message ids
- At-most-once, expiry-checked command execution
- HMAC-authenticated frames, optional Ed25519 origin signatures

See `MESH.md`.

---

## 5. UPLINK (Peer ⇄ Platform)

Used for:
- A gateway sending data to a platform, and receiving commands back
- Relaying a neighbour's traffic (with the mesh) when its own uplink is down

Properties:
- Encrypted, mutually authenticated with the NIKA Secure Channel (NSC)
- An ack means the platform stored it; explicit rejections, no silent drops
- Commands de-duplicated and expiry-checked; routed via a relay when needed

See `UPLINK.md` and `SECURE.md`.

---

## Authentication

Authentication happens once during `CLIENT_JOIN`.
All subsequent messages are trusted for that connection.

---

## Explicit Non-Goals

- Guaranteed delivery
- Persistence
- Replay
- Global ordering
- Exactly-once semantics
- Public internet exposure

---

## Contract Rule

If a behavior is not explicitly documented, it is **not guaranteed**.

This README is a high-level entry point.
See `PROTOCOL.md` for the full specification.
