#client.py
import zmq
import json
import uuid
import time
from typing import Callable, Optional
from .envelope import envelope
from .protocol import *

class NikaClient:
    def __init__(
        self,
        control_addr: str = "tcp://127.0.0.1:5555",
        event_addr: str = "tcp://127.0.0.1:5556",
        heartbeat_interval: int = 3,
        heartbeat_timeout: int = 10,
    ):
        self.context = zmq.Context()
        self.last_seq: dict[str, int] = {}

        # Control socket
        self.control_socket = self.context.socket(zmq.DEALER)
        self.client_id = f"client-{uuid.uuid4()}"
        self.control_socket.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.control_socket.connect(control_addr)

        # Event socket
        self.event_socket = self.context.socket(zmq.SUB)
        self.event_socket.connect(event_addr)

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.event_socket, zmq.POLLIN)

        self.event_handler: Optional[Callable[[str, dict], None]] = None
        self.response_handler: Optional[Callable[[dict], None]] = None
        self.error_handler: Optional[Callable[[dict], None]] = None

        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.last_ping_time = 0.0
        self.last_pong_time = time.time()

        print(f"NIKA client started ({self.client_id})")

    # -----------------------
    # Public API
    # -----------------------

    def request(self, payload: dict):
        self._send(REQUEST, payload)

    def subscribe(self, topic: str):
        self.event_socket.setsockopt_string(zmq.SUBSCRIBE, topic)


    def start_stream(self, stream: str):
        self._send(STREAM_START, {"stream": stream})


    def stop_stream(self, stream: str):
        self._send(STREAM_STOP, {"stream": stream})
        
    def resume_stream(self, stream: str):
        last = self.last_seq.get(stream)
        self._send(
            RESUME_STREAM,
            {
                "stream": stream,
                "last_seq": last,
            }
        )

    def on_event(self, handler: Callable[[str, dict], None]):
        self.event_handler = handler

    def on_response(self, handler: Callable[[dict], None]):
        self.response_handler = handler

    def on_error(self, handler: Callable[[dict], None]):
        self.error_handler = handler
        
    def get_metrics(self):
        self._send(METRICS)

    # -----------------------
    # Main loop
    # -----------------------

    def start(self):
        while True:
            now = time.time()

            if now - self.last_ping_time >= self.heartbeat_interval:
                self._send(PING)
                self.last_ping_time = now

            sockets = dict(self.poller.poll(timeout=1000))

            if self.control_socket in sockets:
                self._handle_control()

            if self.event_socket in sockets:
                self._handle_event()

            if time.time() - self.last_pong_time > self.heartbeat_timeout:
                raise RuntimeError("Server heartbeat lost")

    # -----------------------
    # Internal helpers
    # -----------------------

    def _send(self, msg_type: str, payload: dict | None = None, msg_id: str | None = None):
        msg = envelope(
            msg_type=msg_type,
            payload=payload,
            msg_id=msg_id,
            sender="client",
        )
        self.control_socket.send(json.dumps(msg).encode())

    def _handle_control(self):
        msg = json.loads(self.control_socket.recv().decode())

        msg_type = msg["type"]
        payload = msg["payload"]

        if msg_type == PONG:
            self.last_pong_time = time.time()
            return

        if msg_type == RESPONSE:
            action = payload.get("action")

            if action == STREAM_START:
                stream = payload.get("topic")
                if stream:
                    self.event_socket.setsockopt_string(
                        zmq.SUBSCRIBE,
                        f"stream:{stream}"
                    )

            if action == STREAM_STOP:
                stream = payload.get("topic")
                if stream:
                    self.event_socket.setsockopt_string(
                        zmq.UNSUBSCRIBE,
                        f"stream:{stream}"
                    )

            if action == RESUME_STREAM:
                print(
                    f"[INFO] Resumed stream '{payload['stream']}' "
                    f"from seq {payload.get('resumed_from')}"
                )

            if self.response_handler:
                self.response_handler(msg)

        elif msg_type == ERROR and self.error_handler:
            self.error_handler(msg)

    def _handle_event(self):
        _, raw = self.event_socket.recv_multipart()
        event = json.loads(raw.decode())

        if not self.event_handler:
            return

        payload = event["payload"]

        if event["type"] == STREAM:
            stream = payload["stream"]
            seq = payload["seq"]

            last = self.last_seq.get(stream)
            if last is not None and seq != last + 1:
                print(
                    f"[WARN] Stream '{stream}' dropped data "
                    f"(expected {last + 1}, got {seq})"
                )

            self.last_seq[stream] = seq
            self.event_handler(stream, payload["data"])

        else:
            self.event_handler(payload["topic"], payload["data"])
