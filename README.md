# NIKA Protocol

**Version:** 1.0  
**Status:** Internal Production  
**Transport:** ZeroMQ  

NIKA is a **live-first messaging protocol** designed for internal systems
requiring low latency, high fan-out, and honest delivery guarantees.

---

## Core Models

NIKA defines three independent interaction models, plus a gateway mesh.

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
