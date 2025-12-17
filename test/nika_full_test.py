"""
NIKA v0.1 — Full Pre-Production Test Script

Tests:
- Auth
- REQ / RES
- PUB / SUB
- STREAMS
- ACLs
- Limits
"""

import time
import threading
from api.client import NikaClient

API_KEY = "nika-dev-key-123"
STREAM_OK = "numbers"
STREAM_DENIED = "metrics"
EVENT_TOPIC = "alerts"


# ----------------------------
# Helpers
# ----------------------------

def banner(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ----------------------------
# Event handler
# ----------------------------

def on_event(topic, data):
    print(f"[EVENT] {topic} -> {data}")


def on_response(msg):
    print("[RESPONSE]", msg)


def on_error(msg):
    print("[ERROR]", msg)


# ----------------------------
# Start client
# ----------------------------

banner("1. START CLIENT (AUTH TEST)")

client = NikaClient(
    api_key=API_KEY,
)

client.on_event(on_event)
client.on_response(on_response)
client.on_error(on_error)

client_thread = threading.Thread(target=client.start, daemon=True)
client_thread.start()

time.sleep(2)


# ----------------------------
# REQ / RES TESTS
# ----------------------------

banner("2. REQ / RES — VALID COMMANDS")

client.request("PING")
client.request("GET_STATUS")
client.request("GET_STREAMS")

time.sleep(1)


banner("3. REQ / RES — INVALID COMMAND")

client.request("DROP_DATABASE")

time.sleep(1)


banner("4. REQ / RES — IDEMPOTENCY")

# same request ID twice
from uuid import uuid4
req_id = f"req-test-{uuid4()}"

client._send(
    "REQUEST",
    payload={"command": "GET_STATUS"},
    auth=None,
)
client._send(
    "REQUEST",
    payload={"command": "GET_STATUS"},
    auth=None,
)

time.sleep(1)


# ----------------------------
# PUB / SUB TESTS
# ----------------------------

banner("5. PUB / SUB")

client.subscribe(EVENT_TOPIC)
print("[INFO] Subscribed to events")

print("[INFO] Ask server to publish events manually now")
print("      (server.publish('alerts', {...}))")

time.sleep(3)


# ----------------------------
# STREAM TESTS
# ----------------------------

banner("6. STREAM — AUTHORIZED")

client.start_stream(STREAM_OK)

time.sleep(3)


banner("7. STREAM — UNAUTHORIZED")

client.start_stream(STREAM_DENIED)

time.sleep(2)


banner("8. STREAM — MAX STREAM LIMIT")

for i in range(10):
    client.start_stream(f"extra_stream_{i}")
    time.sleep(0.2)


# ----------------------------
# DROP / THROTTLING TEST
# ----------------------------

banner("9. STREAM — DROP SIMULATION")

def slow_handler(topic, data):
    time.sleep(0.2)
    print(f"[SLOW STREAM] {topic} -> {data}")

client.on_event(slow_handler)

print("[INFO] Let stream run slow for throttling")
time.sleep(5)

client.on_event(on_event)


# ----------------------------
# METRICS
# ----------------------------

banner("10. METRICS")

client.get_status()
time.sleep(1)


# ----------------------------
# CLEANUP
# ----------------------------

banner("11. CLEANUP")

client.close()
print("Client closed")

time.sleep(1)

banner("TEST COMPLETE")
