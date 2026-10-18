from nika.client import NikaClient

def on_event(stream, data):
    print(f"[STREAM] {stream} → {data}")

client = NikaClient()
client.on_event(on_event)

client.start_stream("numbers")
client.start()
