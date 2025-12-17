# NIKA Security Model — v0.1

**Threat Model:** Trusted internal clients  
**Exposure Level:** Private network only

---

## 1. Security Philosophy

NIKA follows a **minimal, explicit security model**:

- Clear trust boundaries
- No hidden protections
- No false guarantees

Security is enforced at **connection and authorization time**, not per-message.

---

## 2. Authentication

### Mechanism
- API key based
- Per-connection authentication
- Occurs during CLIENT_JOIN

```json
{
  "type": "CLIENT_JOIN",
  "auth": {
    "api_key": "nika-dev-key-123"
  }
}
```

### Properties
- One-time authentication
- Identity bound to connection
- No token refresh
- No session cookies

---

## 3. Authorization

### Control Plane
- Only whitelisted commands allowed
- Unknown commands rejected

### Stream Plane
- Stream access controlled via ACLs
- Unauthorized stream access rejected explicitly

```python
STREAM_ACL = {
    "numbers": {"client-a", "ops"}
}
```

---

## 4. Transport Security

### Current State
- No encryption at protocol level
- Relies on network isolation

### Required
- Run behind:
  - VPN
  - Private VPC
  - Encrypted tunnel
  - Or localhost-only access

> NIKA must NOT be exposed directly to the public internet.

---

## 5. Replay & Injection

### Replay
- REQ/RES idempotency prevents duplicate execution
- Streams intentionally do not protect against replay

### Injection
- Strict JSON parsing
- Command whitelist
- No dynamic execution

---

## 6. Denial of Service Protections

Implemented:
- Client limits
- Stream limits
- Request cache TTL
- Heartbeat-based cleanup

Not implemented (by design):
- Rate limiting
- CAPTCHA
- IP blocking

---

## 7. Logging & Auditing

Currently:
- Operational logs only
- No audit trail
- No request persistence

Future (v0.2+):
- Structured logs
- Security events
- Access audit hooks

---

## 8. Explicit Non-Goals

NIKA does NOT provide:
- End-to-end encryption
- Zero-trust security
- Multi-tenant isolation
- Compliance guarantees (SOC2, ISO, HIPAA)

---

## 9. Responsibility Model

| Layer | Responsibility |
|-----|---------------|
| Network | Operator |
| Auth keys | Operator |
| ACLs | Operator |
| Client behavior | Client |
| Data integrity | Application |

---

## 10. Security Contract

> If a security feature is not explicitly documented here, it does not exist.

---

**End of Security Model**
