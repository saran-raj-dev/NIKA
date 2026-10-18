from nika.client import NikaClient

def on_event(topic, data):
    print(f"[EVENT] {topic} → {data}")

client = NikaClient()
client.on_event(on_event)

client.subscribe("alerts")
client.start()
