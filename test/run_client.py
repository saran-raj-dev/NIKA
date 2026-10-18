from nika.client import NikaClient

def on_event(stream, data):
    print(f"[DATA] {stream}: {data}")

def on_response(msg):
    print("[RESPONSE]", msg["payload"])

client = NikaClient()
client.on_event(on_event)
client.on_response(on_response)

client.start_stream("numbers")
client.start()
