"""
NIKA STREAM — Client Example (Pure)

- SUB socket only
- No control plane
- No heartbeat
- No auth
- Live data only
"""

import zmq
import json

EVENT_ADDR = "tcp://127.0.0.1:5556"
STREAM = "numbers"


def main():
    print("\nStarting PURE stream client\n")

    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(EVENT_ADDR)
    sock.setsockopt_string(zmq.SUBSCRIBE, STREAM)

    try:
        while True:
            _, raw = sock.recv_multipart()
            msg = json.loads(raw.decode())
            payload = msg["payload"]

            print(
                f"[STREAM] {payload['stream']} "
                f"seq={payload['seq']} "
                f"value={payload['data']['value']}"
            )

    except KeyboardInterrupt:
        print("\nClient stopped")

    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    main()
