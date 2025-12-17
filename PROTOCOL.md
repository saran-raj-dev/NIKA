# NIKA Protocol Specification — v0.1

**Status:** Production (Internal)  
**Protocol Version:** 1.0  
**Transport:** ZeroMQ (implementation detail)  
**Delivery Model:** Live-first, best-effort  

---

## 1. Overview

NIKA is a **live-first messaging and streaming protocol** designed for internal systems that need:

- Low-latency communication
- Clear separation of concerns
- Predictable behavior under load
- Honest delivery semantics

NIKA explicitly prefers **liveness over reliability**.  
If data is late, it is dropped. If a client is slow, it falls behind.

---

## 2. Core Principles

- **Live-first:** Messages represent *now*, not history
- **Loss-tolerant:** Dropped data is expected and visible
- **Explicit state:** State is never reconstructed from streams
- **Separation of planes:** Control, Events, and Streams never overlap
- **No hidden guarantees:** Only documented behavior is guaranteed

---

## 3. Interaction Models

NIKA defines **three independent interaction models**.

Each model solves a different problem and must not be mixed.

---

## 3.1 REQ / RES — Control Plane

### Purpose

- Commands
- Queries
- Configuration
- Health & diagnostics
- Metadata

### Characteristics

- Authenticated
- Deterministic
- Idempotent
- Versioned
- Explicit success or failure

### Request Format

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

### Success Response

```json
{
  "type": "RESPONSE",
  "id": "req-uuid",
  "payload": {
    "ok": true,
    "result": {}
  }
}
```

### Error Response

```json
{
  "type": "ERROR",
  "id": "req-uuid",
  "payload": {
    "ok": false,
    "error": {
      "code": "INVALID_COMMAND",
      "message": "Command not allowed"
    }
  }
}
```

### Guarantees

- At-most-once execution per request ID
- Safe retries (idempotency enforced server-side)
- Deterministic responses
- No side effects on duplicate requests

### Explicit Non-Guarantees

- No streaming data
- No background jobs
- No implicit state mutation

---

## 3.2 PUB / SUB — Event Plane

### Purpose

- Broadcast notifications
- System-wide signals
- Non-critical updates

### Characteristics

- Fire-and-forget
- Fan-out semantics
- Best-effort delivery

### Message Format

```json
{
  "type": "EVENT",
  "payload": {
    "topic": "alerts",
    "data": {}
  }
}
```

### Guarantees

- Messages may be delivered to zero or more subscribers

### Explicit Non-Guarantees

- No delivery guarantee
- No persistence
- No replay
- No ordering guarantee
- No state reconstruction

> PUB/SUB is a firehose, not a database.

---

## 3.3 STREAMS — Data Plane

### Purpose

- High-frequency telemetry
- Live metrics
- Sensor data
- Real-time dashboards

### Characteristics

- Live-only
- Per-stream rate limits
- Per-client throttling
- Drop-tolerant

### Stream Message Format

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

### Guarantees

- Messages represent current data only
- Best-effort delivery
- Independent per-client flow control

### Client Responsibilities

- Track sequence numbers
- Detect gaps
- Tolerate loss
- Never assume completeness

### Explicit Non-Guarantees

- No replay
- No persistence
- No global ordering
- No state reconstruction

> Streams optimize for **now**, not **then**.

---

## 4. Authentication

- Authentication occurs once via `CLIENT_JOIN`
- API-key based
- Identity is bound to the connection
- No per-message authentication

### CLIENT_JOIN Example

```json
{
  "type": "CLIENT_JOIN",
  "id": "join-uuid",
  "nika": "1.0",
  "auth": {
    "api_key": "nika-dev-key-123"
  }
}
```

Unauthenticated messages are rejected.

---

## 5. Authorization

- Enforced server-side
- Stream access controlled via ACLs
- Unauthorized stream requests are rejected explicitly

---

## 6. Versioning

- Protocol version declared via `nika`
- Unsupported versions are rejected
- No silent downgrade

---

## 7. Error Model

All errors follow a single schema:

```json
{
  "ok": false,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable explanation"
  }
}
```

Errors are explicit and deterministic.

---

## 8. Limits & Guardrails

Implementations enforce:

- Maximum concurrent clients
- Maximum streams per client
- Request idempotency cache with TTL
- Throttling and drop control

Limits are deployment-specific.

---

## 9. Explicit Non-Goals (v0.1)

The following are intentionally out of scope:

- Guaranteed delivery
- Persistence
- Replay
- Exactly-once semantics
- Global ordering
- Multi-node clustering
- Public internet exposure

---

## 10. Intended Usage

### Supported

- Internal services
- IoT & telemetry pipelines
- Dashboards
- Monitoring systems

### Not Supported

- Untrusted public clients
- Compliance-critical workloads
- Historical data systems

---

## 11. Contract Statement

> If a behavior is not explicitly documented here, it is **not guaranteed**.

This document is the authoritative contract for **NIKA v0.1**.

---

**End of Specification**
