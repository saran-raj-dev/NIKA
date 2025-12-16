import zmq
import json
import uuid
import time
from typing import Callable, Optional
from .protocol import (
    REQUEST,
    RESPONSE,
    ERROR,
    SUBSCRIBE,
    UNSUBSCRIBE,
    PING,
    PONG,
)

class NikaClient:
    def __init__(
        self,
        control_addr: str = "tcp://127.0.0.1:5555",
        event_addr: str = "tcp://127.0.0.1:5556",
        heartbeat_interval: int = 3,
        heartbeat_timeout: int = 10,
    ):
        # ---------------------------
        # ZeroMQ setup
        # ---------------------------
        self.context = zmq.Context()

        # Control socket (DEALER)
        self.control_socket = self.context.socket(zmq.DEALER)
        self.client_id = f"client-{uuid.uuid4()}"
        self.control_socket.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.control_socket.connect(control_addr)

        # Event socket (SUB)
        self.event_socket = self.context.socket(zmq.SUB)
        self.event_socket.connect(event_addr)

        # Poller
        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.event_socket, zmq.POLLIN)

        # ---------------------------
        # User callbacks
        # ---------------------------
        self.event_handler: Optional[Callable[[str, dict], None]] = None
        self.response_handler: Optional[Callable[[dict], None]] = None
        self.error_handler: Optional[Callable[[dict], None]] = None

        # ---------------------------
        # Heartbeat state
        # ---------------------------
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.last_ping_time = 0.0
        self.last_pong_time = time.time()

        print(f"NIKA client started ({self.client_id})")

    # ======================================================
    # Public API
    # ======================================================

    def request(self, payload: dict) -> str:
        msg_id = str(uuid.uuid4())
        self._send_control({
            "type": REQUEST,
            "id": msg_id,
            "payload": payload,
        })
        return msg_id

    def subscribe(self, topic: str):
        self._send_control({
            "type": SUBSCRIBE,
            "id": str(uuid.uuid4()),
            "topic": topic,
        })

    def unsubscribe(self, topic: str):
        self._send_control({
            "type": UNSUBSCRIBE,
            "id": str(uuid.uuid4()),
            "topic": topic,
        })
        self.event_socket.setsockopt_string(zmq.UNSUBSCRIBE, topic)

    def on_event(self, handler: Callable[[str, dict], None]):
        self.event_handler = handler

    def on_response(self, handler: Callable[[dict], None]):
        self.response_handler = handler

    def on_error(self, handler: Callable[[dict], None]):
        self.error_handler = handler

    # ======================================================
    # Main loop
    # ======================================================

    def start(self):
        while True:
            now = time.time()

            # ---------------------------
            # Heartbeat
            # ---------------------------
            if now - self.last_ping_time >= self.heartbeat_interval:
                self._send_control({
                    "type": PING,
                    "id": str(uuid.uuid4()),
                })
                self.last_ping_time = now

            # ---------------------------
            # Poll sockets
            # ---------------------------
            sockets = dict(self.poller.poll(timeout=1000))

            if self.control_socket in sockets:
                self._handle_control()

            if self.event_socket in sockets:
                self._handle_event()

            # ---------------------------
            # Heartbeat timeout detection
            # ---------------------------
            if time.time() - self.last_pong_time > self.heartbeat_timeout:
                raise RuntimeError("Server heartbeat lost")

    # ======================================================
    # Internal helpers
    # ======================================================

    def _send_control(self, message: dict):
        self.control_socket.send(json.dumps(message).encode())

    def _handle_control(self):
        msg = json.loads(self.control_socket.recv().decode())
        msg_type = msg.get("type")
        payload = msg.get("payload", {})

        if msg_type == PONG:
            self.last_pong_time = time.time()
            return

        if msg_type == RESPONSE:
            # ---- protocol state handling ----
            action = payload.get("action")
            topic = payload.get("topic")

            if action == SUBSCRIBE and topic:
                self.event_socket.setsockopt_string(zmq.SUBSCRIBE, topic)

            # ---- user callback ----
            if self.response_handler:
                self.response_handler(msg)
            return

        if msg_type == ERROR:
            if self.error_handler:
                self.error_handler(msg)
            return

    def _handle_event(self):
        topic, raw = self.event_socket.recv_multipart()
        event = json.loads(raw.decode())

        if self.event_handler:
            self.event_handler(event.get("topic"), event.get("payload"))
