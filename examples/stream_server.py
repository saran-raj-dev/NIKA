"""
NIKA STREAM — Server Example (Full)

- Runs full control plane
- Responds to heartbeats
- Allows STREAM_START
- Publishes live stream data
"""

import time
import json
import random
import threading

from nika.server import NikaServer
from nika.envelope import envelope
from nika.protocol import STREAM


STREAM_NAME = "numbers"
RATE_PER_SEC = 2


def stream_loop(server: NikaServer):
    seq = 0
    interval = 1 / RATE_PER_SEC

    while True:
        payload = {
            "stream": STREAM_NAME,
            "seq": seq,
            "data": {
                "value": random.randint(0, 100)
            },
        }

        msg = envelope(
            msg_type=STREAM,
            sender="server",
            payload=payload,
        )

        server.event_socket.send_multipart([
            STREAM_NAME.encode(),
            json.dumps(msg).encode(),
        ])

        print(f"[STREAM] {STREAM_NAME} seq={seq}")

        seq += 1
        time.sleep(interval)


def main():
    server = NikaServer()

    # 🔥 Start control plane (REQ/RES, heartbeat, auth)
    threading.Thread(target=server.start, daemon=True).start()

    print("\nSTREAM server running (FULL)")
    print(f"Streaming '{STREAM_NAME}' at {RATE_PER_SEC} msg/sec\n")

    stream_loop(server)


if __name__ == "__main__":
    main()
