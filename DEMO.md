# NIKA Demo & Usage Guide

This document shows **how to use NIKA in a real project**, end-to-end.

You can follow this guide even if you have **never seen NIKA before**.

---

## 1. What is NIKA?

NIKA is a **live-first internal protocol** for:

- Control commands (REQ / RES)
- Broadcast events (PUB / SUB)
- High-frequency live data (STREAMS)

It is **not**:
- A database
- A message queue
- A replay system

Think of NIKA as:
> *“WebSockets + strict rules + honest guarantees.”*

---

## 2. When should you use NIKA?

Use NIKA if you need:

✅ Real-time dashboards  
✅ Telemetry / sensor data  
✅ Internal services communication  
✅ Low latency, high fan-out  

Do NOT use NIKA if you need:

❌ Guaranteed delivery  
❌ Message replay  
❌ Long-term storage  
❌ Public internet exposure  

---

## 3. Architecture Overview

```
+------------+        REQ / RES        +------------+
|            | <--------------------> |            |
|   Client   |                        |   Server   |
|            | ---- PUB / SUB ---->   |            |
|            | ---- STREAMS ------>   |            |
+------------+                        +------------+
```

- **REQ / RES** → Commands & queries
- **PUB / SUB** → Events & notifications
- **STREAMS** → Live data only

---

## 4. Starting the Server

Minimal server startup:

```python
from nika.server import NikaServer

server = NikaServer(
    control_addr="tcp://127.0.0.1:5555",
    event_addr="tcp://127.0.0.1:5556",
)

server.start()
```

That’s it.
No config files. No magic.

---

## 5. Creating a Client

```python
from nika.client import NikaClient

client = NikaClient(
    api_key="nika-dev-key-123"
)
```

Authentication happens **once** during `CLIENT_JOIN`.

---

## 6. REQ / RES — Control Commands

### Example: Ping the server

```python
client.request("PING")
```

### Example: Get server status

```python
client.get_status()
```

### Handling responses

```python
def on_response(msg):
    print("RESPONSE:", msg)

def on_error(msg):
    print("ERROR:", msg)

client.on_response(on_response)
client.on_error(on_error)
```

Guarantees:
- Idempotent
- Deterministic
- Safe retries

---

## 7. PUB / SUB — Events

### Subscribe to events

```python
client.subscribe("alerts")
```

### Handle events

```python
def on_event(topic, data):
    print(f"EVENT {topic}: {data}")

client.on_event(on_event)
```

Events are:
- Best-effort
- Fire-and-forget
- Non-critical

---

## 8. STREAMS — Live Data

### Start a stream

```python
client.start_stream("numbers")
```

### Handle stream data

```python
def on_stream(stream, data):
    print(f"{stream} -> {data}")

client.on_event(on_stream)
```

### Stop a stream

```python
client.stop_stream("numbers")
```

Important:
- Streams are **live-only**
- Drops are expected
- Gaps must be tolerated

---

## 9. Handling Drops (Recommended)

NIKA provides sequence numbers.

You should:

```python
if seq != last_seq + 1:
    print("Dropped data detected")
```

Never assume completeness.

---

## 10. Shutdown Cleanly

```python
client.close()
```

This:
- Sends `CLIENT_LEAVE`
- Cleans up server state
- Closes sockets safely

---

## 11. Common Patterns

### Dashboards
- Use STREAMS
- Ignore drops
- Show latest values only

### Alerts
- Use PUB / SUB
- No retries

### Control APIs
- Use REQ / RES
- Idempotent commands

---

## 12. Mental Model (Very Important)

If you remember only one thing:

> **NIKA delivers “now”, not “history”.**

If you need history:
- Store it yourself
- Query a database
- Do not ask NIKA to do it


---

**End of Demo Guide**
