"""
NIKA PUB / SUB — Server Example (Correct)

This example shows:
- How to publish broadcast events
- Using NIKA's event socket directly
- No delivery guarantees (fire-and-forget)
"""

import time
import random
import json

from nika.server import NikaServer
from nika.envelope import envelope
from nika.protocol import EVENT


def main():
    server = NikaServer()

    print("\nPUB / SUB server is running.")
    print("Publishing events to topic: alerts")
    print("Press Ctrl+C to stop.\n")

    counter = 0

    try:
        while True:
            event_payload = {
                "id": counter,
                "level": random.choice(["INFO", "WARN", "ERROR"]),
                "message": "This is a broadcast event",
            }

            msg = envelope(
                msg_type=EVENT,
                sender="server",
                payload={
                    "topic": "alerts",
                    "data": event_payload,
                },
            )

            # 🔥 Fire-and-forget publish
            server.event_socket.send_multipart([
                b"alerts",
                json.dumps(msg).encode(),
            ])

            print(f"[PUBLISHED] alerts -> {event_payload}")

            counter += 1
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
