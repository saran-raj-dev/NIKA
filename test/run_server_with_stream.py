# test/run_server_with_stream.py
import time
import threading
from api.server import NikaServer

server = NikaServer()
print("NIKA server running with stream producer...")

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
        time.sleep(0.2)  # 20/sec (limit = 10)

threading.Thread(target=produce, daemon=True).start()
server.start()
