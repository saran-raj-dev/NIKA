# sse_adapter.py
import json
import threading
import queue
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from api.client import NikaClient

app = FastAPI()

# -------------------------------------------------
# Thread-safe queue to move data from NIKA → HTTP
# -------------------------------------------------
event_queue: queue.Queue = queue.Queue()

# -------------------------------------------------
# Internal NIKA client
# -------------------------------------------------
nika = NikaClient()

def on_stream(stream: str, payload: dict):
    event_queue.put({
        "stream": stream,
        "payload": payload
    })

nika.on_event(on_stream)

def start_nika():
    # Start the stream you want to expose
    nika.start_stream("numbers")
    nika.start()

threading.Thread(target=start_nika, daemon=True).start()

# -------------------------------------------------
# SSE endpoint
# -------------------------------------------------
@app.get("/stream/{stream_name}")
def stream(stream_name: str):
    def event_generator():
        while True:
            event = event_queue.get()
            if event["stream"] == stream_name:
                yield f"data: {json.dumps(event['payload'])}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )
