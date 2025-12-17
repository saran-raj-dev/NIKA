"""
NIKA v0.1 — REQ / RES Client Example

This example shows:
- How a client authenticates
- How to send REQ / RES commands
- How to receive responses and errors
- How idempotency works naturally

Run this AFTER starting req_res_server.py
"""

import time
import threading
from api.client import NikaClient

API_KEY = "nika-dev-key-123"


# ------------------------------------
# Response handlers
# ------------------------------------

def on_response(msg: dict):
    print("\n[RESPONSE RECEIVED]")
    print(msg)


def on_error(msg: dict):
    print("\n[ERROR RECEIVED]")
    print(msg)


def main():
    print("\nStarting NIKA REQ / RES client...\n")

    # ------------------------------------
    # Create client (AUTH happens here)
    # ------------------------------------
    client = NikaClient(api_key=API_KEY)

    client.on_response(on_response)
    client.on_error(on_error)

    # ------------------------------------
    # Start client loop in background
    # ------------------------------------
    t = threading.Thread(target=client.start, daemon=True)
    t.start()

    # Give time for CLIENT_JOIN handshake
    time.sleep(1)

    # ------------------------------------
    # Send valid commands
    # ------------------------------------
    print("\nSending PING")
    client.request("PING")

    time.sleep(0.5)

    print("\nRequesting server status")
    client.request("GET_STATUS")

    time.sleep(0.5)

    print("\nRequesting available streams")
    client.request("GET_STREAMS")

    time.sleep(0.5)

    # ------------------------------------
    # Send invalid command (will be rejected)
    # ------------------------------------
    print("\nSending INVALID command")
    client.request("DROP_DATABASE")

    # ------------------------------------
    # Shutdown
    # ------------------------------------
    time.sleep(1)
    print("\nShutting down client\n")
    client.close()


if __name__ == "__main__":
    main()
