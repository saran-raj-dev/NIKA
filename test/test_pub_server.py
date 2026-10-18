import time
import threading
from nika.server import NikaServer

server = NikaServer()

print("PUB server running...")

def publish_loop():
    i = 0
    while True:
        server.publish(
            topic="alerts",
            payload={"level": "info", "count": i}
        )
        i += 1
        time.sleep(1)

threading.Thread(target=publish_loop, daemon=True).start()

server.start()   # <-- REQUIRED
