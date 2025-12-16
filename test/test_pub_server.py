from api.server import NikaServer
import time
import threading
import random

server = NikaServer()

print("PUB SERVER started (random numbers)")

def publish_loop():
    counter = 0
    while True:
        payload = {
            "seq": counter,
            "value": random.randint(1, 100)
        }

        print("SERVER publishing:", payload)
        server.publish("numbers", payload)

        counter += 1
        time.sleep(1)

# Start publisher in background
threading.Thread(target=publish_loop, daemon=True).start()

# Keep control plane alive
server.start()
