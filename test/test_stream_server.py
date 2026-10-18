import time
import threading
from nika.server import NikaServer

server = NikaServer()
print("STREAM server running...")

seq = 0

def produce():
    global seq
    while True:
        server.publish_stream(
            stream="numbers",
            seq=seq,
            payload={"value": seq}
        )
        seq += 1
        time.sleep(0.5)  # 20/sec (server limits to 10)

threading.Thread(target=produce, daemon=True).start()
server.start()
