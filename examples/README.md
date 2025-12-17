# NIKA Examples

This directory contains **minimal, focused examples** demonstrating how to use
NIKA’s three interaction models:

1. **REQ / RES** — Control Plane  
2. **PUB / SUB** — Event Plane  
3. **STREAMS** — Data Plane  

Each example is intentionally simple and runnable.

---

## Directory Structure

```
examples/
├── req_res_server.py
├── req_res_client.py
│
├── pub_sub_server.py
├── pub_sub_client.py
│
├── stream_server.py
├── stream_client.py
├── stream_client_pure.py
└── README.md
```

---

## 1. REQ / RES — Control Plane

Used for commands, queries, health checks, and configuration.

### Run
```bash
python -m examples.req_res_server
python -m examples.req_res_client
```

Guarantees:
- Deterministic
- Idempotent
- Explicit success or error

---

## 2. PUB / SUB — Event Plane

Used for broadcast notifications and alerts.

### Run
```bash
python -m examples.pub_sub_server
python -m examples.pub_sub_client
```

Notes:
- Fire-and-forget
- No delivery guarantee
- No replay

---

## 3. STREAMS — Data Plane

Used for live telemetry and real-time data.

### Run
```bash
python -m examples.stream_server
python -m examples.stream_client
```

Or pure data-plane client:
```bash
python -m examples.stream_client_pure
```

Streams are **live-only** and **drop-tolerant**.

---

## Rule of Thumb

REQ/RES → control  
PUB/SUB → notifications  
STREAMS → live data  

Never mix responsibilities unless you understand why.
