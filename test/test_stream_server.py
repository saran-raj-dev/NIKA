from api.server import NikaServer
import time
import threading
import random

server = NikaServer()

def stream_loop():
    seq = 0
    while True:
        server.publish_stream(
            "numbers",
            seq,
            {"value": random.randint(1, 100)}
        )
        seq += 1
        time.sleep(1)

threading.Thread(target=stream_loop, daemon=True).start()
server.start()
