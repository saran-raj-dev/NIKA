from api.server import NikaServer

server = NikaServer()

def echo(payload):
    return {
        "received": payload,
        "status": "ok"
    }

server.on_request(echo)

print("REQ/RES server running...")
server.start()
