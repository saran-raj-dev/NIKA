import time
from api.server import NikaServer

server = NikaServer()

def echo_handler(payload):
    return {"echo": payload}

server.on_request(echo_handler)

print("Server running...")

# ---- simple producer loop ----
seq = 0
def producer_loop():
    global seq
    while True:
        server.publish_stream(
            stream="numbers",
            seq=seq,
            payload={"value": seq}
        )
        seq += 1
        time.sleep(0.05)  # 20 msg/sec (limit is 10)

# run producer in background thread
import threading
threading.Thread(target=producer_loop, daemon=True).start()

server.start()
