"""
NIKA PUB / SUB — Client Example (Pure)

No control plane
No auth
No heartbeat
"""

import zmq
import json

EVENT_ADDR = "tcp://127.0.0.1:5556"
TOPIC = "alerts"


def main():
    print("\nStarting NIKA PUB / SUB client...\n")

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(EVENT_ADDR)

    socket.setsockopt_string(zmq.SUBSCRIBE, TOPIC)

    print(f"Subscribed to topic: {TOPIC}\n")

    try:
        while True:
            topic, raw = socket.recv_multipart()
            event = json.loads(raw.decode())

            payload = event["payload"]
            print(f"[EVENT RECEIVED] {payload['topic']} -> {payload['data']}")

    except KeyboardInterrupt:
        print("\nClient stopped.")

    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()
