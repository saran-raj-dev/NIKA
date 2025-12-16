from api.client import NikaClient

client = NikaClient()

def on_stream(stream, payload):
    print(f"STREAM [{stream}]:", payload)

client.on_event(on_stream)

client.start_stream("numbers")
client.start()
