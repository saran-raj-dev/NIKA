# NIKA — 5‑Minute Quickstart 🚀

This guide gets you **from zero to streaming data** with NIKA in under 5 minutes.

> Assumption: You already cloned the repo and are on branch `NIKA-V0.1`

---

## 1️⃣ Prerequisites

- Python **3.10+**
- ZeroMQ installed
- Local machine (NIKA v0.1 is **not** internet-facing)

Install dependencies:

```bash
pip install pyzmq
```

---

## 2️⃣ Start the Server (1 minute)

Create `run_server.py`:

```python
from nika.server import NikaServer

if __name__ == "__main__":
    server = NikaServer()
    server.start()
```

Run it:

```bash
python run_server.py
```

You should see:

```text
NIKA server started
Control: tcp://127.0.0.1:5555
Events : tcp://127.0.0.1:5556
```

Server is now live ✅

---

## 3️⃣ Create a Client (1 minute)

Create `quick_client.py`:

```python
from nika.client import NikaClient
import threading
import time

client = NikaClient(api_key="nika-dev-key-123")

def on_response(msg):
    print("[RESPONSE]", msg)

def on_event(topic, data):
    print("[EVENT]", topic, data)

client.on_response(on_response)
client.on_event(on_event)

threading.Thread(target=client.start, daemon=True).start()
time.sleep(1)
```

Run it:

```bash
python quick_client.py
```

You should see:

```text
[RESPONSE] CLIENT_JOIN
```

Client authenticated ✅

---

## 4️⃣ Use REQ / RES (Control Plane)

Add this to `quick_client.py`:

```python
client.request("PING")
client.request("GET_STATUS")
client.request("GET_STREAMS")
```

Expected output:

```text
[RESPONSE] pong
[RESPONSE] status
[RESPONSE] streams
```

✔ Deterministic  
✔ Idempotent  
✔ Safe to retry  

---

## 5️⃣ Use PUB / SUB (Events)

Subscribe to an event topic:

```python
client.subscribe("alerts")
```

From the server side (anywhere):

```python
server.event_socket.send_multipart([
    b"alerts",
    b'{"type":"EVENT","payload":{"topic":"alerts","data":{"msg":"hello"}}}'
])
```

Client receives:

```text
[EVENT] alerts {'msg': 'hello'}
```

✔ Fire-and-forget  
✔ No delivery guarantees  

---

## 6️⃣ Use STREAMS (Live Data)

Start a stream:

```python
client.start_stream("numbers")
```

Publish stream data on the server:

```python
seq = 0
while True:
    seq += 1
    server.publish_stream("numbers", seq, {"value": seq})
    time.sleep(0.1)
```

Client output:

```text
numbers {'value': 42}
numbers {'value': 43}
```

✔ Live-only  
✔ Drops allowed  
✔ Sequence gaps expected  

---

## 7️⃣ Handle Drops (Important!)

NIKA **does not guarantee delivery**.

Client example:

```python
def on_event(topic, data):
    print("[LIVE]", topic, data)
```

Never assume:
- completeness
- replay
- ordering across clients

---

## 8️⃣ Shutdown Cleanly

```python
client.close()
```

Server auto-cleans inactive clients.

---

## ✅ Mental Model (Read This)

- **REQ / RES** → commands & queries
- **PUB / SUB** → notifications
- **STREAMS** → live data only

If you need:
- persistence ❌
- replay ❌
- guaranteed delivery ❌

NIKA is not the right tool.

---

## 🎯 You’re Ready

You now know:
- how to start NIKA
- how to talk to it
- how to stream data correctly

---

**End of Quickstart**
