from api.client import NikaClient
import time

def on_response(msg):
    print("[RESPONSE]", msg["payload"])

client = NikaClient()
client.on_response(on_response)

client.request({"cmd": "ping"})
client.start()
