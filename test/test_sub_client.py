from api.client import NikaClient

client = NikaClient()

def on_event(topic, payload):
    print(f"CLIENT received [{topic}]:", payload)

def on_response(msg):
    print("CLIENT CONTROL:", msg)

client.on_event(on_event)
client.on_response(on_response)

print("CLIENT subscribing to numbers")
client.subscribe("numbers")

client.start()
