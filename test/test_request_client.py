from api.client import NikaClient

client = NikaClient()

def on_response(msg):
    print("CLIENT got response:", msg)

client.on_response(on_response)

client.request({"hello": "request-response test"})
client.start()
