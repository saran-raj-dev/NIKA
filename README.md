# NIKA

**NIKA** is a **live-first internal streaming protocol** designed for systems where *what is happening now* matters more than *what happened before*.

It is built for **trusted environments**, **low-latency telemetry**, and **real-time internal platforms**, with one strict rule:

> **If something is not guaranteed, NIKA does not pretend it is.**

---

## Why NIKA Exists

Most messaging systems fall into one of these traps:

1. They promise delivery guarantees that collapse under scale  
2. They hide loss, forcing clients to assume incorrect state  

NIKA avoids both by being **explicit, minimal, and honest**.

- Loss is acceptable  
- State is explicit  
- Liveness is prioritized  
- Clients participate in correctness  

---

## Core Principles

- **Live-first semantics** — data represents *now*
- **Explicit non-guarantees** — no hidden reliability
- **Strict separation** — control, events, and streams never overlap
- **Client responsibility** — gap detection and tolerance
- **Simple protocol** — JSON messages, deterministic behavior

---

## The Three Interaction Models

NIKA is intentionally built around **three independent planes**.  
Each plane has a **single responsibility** and **clear guarantees**.

---

## 1. REQ / RES — Control Plane

### Purpose
Used for **commands and queries**:

- Metadata
- Configuration
- Health checks
- Diagnostics
- Administrative control

### Properties
- Authenticated
- Deterministic
- Idempotent
- Explicit success or failure

### Example Request
```json
{
  "type": "REQUEST",
  "id": "req-uuid",
  "nika": "1.0",
  "payload": {
    "command": "GET_STATUS",
    "args": {}
  }
}
```

### Guarantees
- At-most-once execution per request ID
- Safe retries
- Deterministic responses

### Non-Guarantees
- No streaming
- No long-running jobs
- No implicit state mutation

---

## 2. PUB / SUB — Event Plane

### Purpose
Used for **broadcast notifications**:

- System events
- Alerts
- Informational signals

### Properties
- Fire-and-forget
- Best-effort delivery
- Fan-out semantics

### Guarantees
- Messages may reach zero or more subscribers

### Explicit Non-Guarantees
- No delivery guarantee
- No persistence
- No replay
- No ordering

> PUB/SUB is a firehose, not a database.

---

## 3. STREAMS — Data Plane

### Purpose
Used for **high-frequency live data**:

- Telemetry
- Sensor data
- Real-time dashboards

### Properties
- Live-only
- Drop-tolerant
- Rate-limited
- Per-client flow control

### Example Stream Message
```json
{
  "type": "STREAM",
  "payload": {
    "stream": "numbers",
    "seq": 1024,
    "data": {}
  }
}
```

### Client Responsibilities
- Track sequence numbers
- Detect gaps
- Tolerate drops
- Report stream health

### Explicit Non-Guarantees
- No replay
- No persistence
- No ordering across clients
- No state reconstruction

> Streams optimize for *now*, not *then*.

---

## Authentication & Authorization

- Authentication occurs once during `CLIENT_JOIN`
- API-key based
- Identity bound to connection
- Stream access controlled by server-side ACLs

Unauthenticated messages are rejected immediately.

---

## Limits & Guardrails

NIKA enforces operational limits to protect the system:

- Maximum concurrent clients
- Maximum streams per client
- Request idempotency cache (TTL-based)
- Per-client stream throttling

Limits are implementation-defined and deployment-specific.

---

## Intended Use Cases

### Supported
- Internal platforms
- IoT and telemetry systems
- Live monitoring dashboards
- Trusted services

### Not Supported
- Public internet exposure
- Untrusted clients
- Guaranteed delivery workloads
- Compliance-critical systems

---

## Repository Layout

```
nika/
├── api/
│   ├── server.py
│   ├── client.py
│   ├── protocol.py
│   ├── envelope.py
│   └── auth.py
├── test/
│   └── nika_full_test.py
├── PROTOCOL.md
└── README.md
```

---

## Running the Server

```bash
python -m api.server
```

## Running the Full Test Suite

```bash
python -m test.nika_full_test
```

---

## Contract

> **If a behavior is not explicitly documented, it is NOT guaranteed.**

See `PROTOCOL.md` for the authoritative specification.

---

## Final Note

NIKA is not trying to be universal.

It is trying to be **fast, honest, and predictable**.

That is its strength.
