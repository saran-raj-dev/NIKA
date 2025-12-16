from api.server import NikaServer

server = NikaServer()

def handle_request(payload):
    print("SERVER received request:", payload)
    return {"echo": payload}

server.on_request(handle_request)
server.start()
