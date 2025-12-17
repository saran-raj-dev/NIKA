# NIKA Protocol Specification — v0.1

**Status:** Production (Internal)  
**Version:** `1.0`  
**Transport:** ZeroMQ (implementation detail)  
**Delivery Model:** Live-first, best-effort

---

## 1. Overview

NIKA is a **live-first streaming protocol** designed for internal systems that require:

- Low-latency data delivery
- High fan-out
- Clear separation between **control**, **events**, and **streams**
- Honest delivery guarantees (no hidden reliability promises)

NIKA explicitly prioritizes **liveness over completeness**.

---

## 2. Core Principles

- **Live-first:** Data represents what is happening *now*
- **Loss is acceptable:** Dropped messages are expected and handled
- **State is explicit:** System state is never inferred from streams
- **Separation of concerns:** Control, events, and streams are independent
- **No silent guarantees:** Only documented guarantees exist

---

## 3. Interaction Models

NIKA defines **three interaction models** with **non-overlapping responsibilities**.

---

## 3.1 REQ / RES — Control Plane

### Purpose

- Commands
- Queries
- Metadata
- Configuration
- Health & diagnostics

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
- Safe retries (idempotent)
- Deterministic responses
- No side effects on duplicate requests

### Non-Guarantees

- No long-running jobs
- No streaming data
- No implicit state mutation

---

## 3.2 PUB / SUB — Event Plane

### Purpose

- Broadcast notifications
- Lightweight system events
- Non-critical updates

### Characteristics

- Best-effort delivery
- Fire-and-forget
- Fan-out semantics

### Guarantees

- Messages may be delivered to zero or more subscribers

### Explicit Non-Guarantees

- No delivery guarantee
- No persistence
- No replay
- No ordering guarantee
- No state reconstruction

> **PUB/SUB is a firehose, not a database.**

---

## 3.3 STREAMS — Data Plane

### Purpose

- High-frequency live data
- Telemetry
- Sensor streams
- Real-time dashboards

### Characteristics

- Live-only
- Per-stream rate limits
- Per-client adaptive throttling
- Drop-tolerant
- Client health feedback

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
- Detect and tolerate gaps
- Report drops via `STREAM_STATS`
- Never assume completeness

### Explicit Non-Guarantees

- No replay
- No persistence
- No ordering across clients
- No state reconstruction

> **Streams optimize for “now”, not “then”.**

---

## 4. Authentication

- Authentication occurs **once** during `CLIENT_JOIN`
- API-key based
- Identity bound to connection
- No per-message secrets

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

After successful authentication:

- All control, event, and stream operations are implicitly authorized
- Unauthenticated messages are rejected

---

## 5. Authorization

- Authorization is enforced server-side
- Stream access may be restricted via ACLs
- Unauthorized stream subscriptions are rejected

---

## 6. Versioning

- Protocol version is declared via the `nika` field
- Server rejects unsupported versions
- No silent downgrade or compatibility fallback

---

## 7. Error Model

All errors follow a **single, consistent schema**.

```json
{
  "ok": false,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable explanation"
  }
}
```

Errors are **explicit**, **deterministic**, and **non-recoverable unless stated**.

---

## 8. Limits & Guardrails

NIKA enforces operational limits to prevent misuse:

- Maximum concurrent clients
- Maximum streams per client
- Request idempotency cache with TTL
- Server-side throttling and drop control

Limits are implementation-defined and may vary by deployment.

---

## 9. Explicit Non-Goals (v0.1)

The following are **intentionally out of scope**:

- Guaranteed delivery
- Persistence
- Replay
- Exactly-once semantics
- Global ordering
- Multi-node clustering
- Public internet exposure

---

## 10. Production Scope

### Supported

- Internal services
- Trusted clients
- IoT & telemetry systems
- Dashboards & monitoring

### Not Supported

- Untrusted public clients
- Internet-facing APIs
- Compliance-critical workloads

---

## 11. Contract Statement

> **If a behavior is not explicitly documented here, it is NOT guaranteed.**

This document is the **authoritative contract** for NIKA v0.1.

---

**End of Specification**
