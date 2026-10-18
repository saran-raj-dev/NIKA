# test/test_metrics_client.py
import time
import threading
from nika.client import NikaClient

def on_event(stream, data):
    print(f"[STREAM] {stream} → {data}")

def on_response(msg):
    payload = msg["payload"]
    if "metrics" in payload:
        print("\n=== METRICS ===")
        print(payload["metrics"])
        print(payload["streams"])
        print("================\n")

client = NikaClient()
client.on_event(on_event)
client.on_response(on_response)

client.start_stream("numbers")

def poll_metrics():
    while True:
        time.sleep(3)
        client.get_metrics()

threading.Thread(target=poll_metrics, daemon=True).start()
client.start()
