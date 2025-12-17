# server.py
import zmq
import json
import time
from typing import Callable
from collections import defaultdict

from .envelope import envelope
from .protocol import *


class NikaServer:
    def __init__(
        self,
        control_addr: str = "tcp://127.0.0.1:5555",
        event_addr: str = "tcp://127.0.0.1:5556",
        client_timeout: int = 15,
    ):
        self.context = zmq.Context()
        
        self.stream_limits = {
            # stream_name: max_msgs_per_second
            "numbers": 10,
        }

        self.stream_last_sent: dict[str, float] = {}

        # -----------------------
        # Control plane (REQ/REP)
        # -----------------------
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.bind(control_addr)

        # -----------------------
        # Data plane (PUB/SUB)
        # -----------------------
        self.event_socket = self.context.socket(zmq.PUB)
        self.event_socket.bind(event_addr)

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)

        # -----------------------
        # Handlers & state
        # -----------------------
        self.request_handler: Callable[[dict], dict] | None = None

        # client_id (bytes) -> set(stream names)
        self.client_streams: dict[bytes, set[str]] = defaultdict(set)

        # client_id (bytes) -> last seen timestamp
        self.last_seen: dict[bytes, float] = {}

        self.client_timeout = client_timeout

        print("NIKA server started")
        print(f"Control: {control_addr}")
        print(f"Events : {event_addr}")

        # ZMQ slow-joiner mitigation
        time.sleep(1)

    # -------------------------------------------------
    # Public API
    # -------------------------------------------------

    def on_request(self, handler: Callable[[dict], dict]):
        """Register request handler."""
        self.request_handler = handler

    def publish(self, topic: str, payload: dict):
        """Publish a normal event."""
        msg = envelope(
            msg_type=EVENT,
            sender="server",
            payload={
                "topic": topic,
                "data": payload,
            },
        )

        self.event_socket.send_multipart([
            topic.encode(),
            json.dumps(msg).encode(),
        ])

    # -------------------------------------------------
    # Main loop
    # -------------------------------------------------

    def start(self):
        while True:
            sockets = dict(self.poller.poll(timeout=100))

            # -----------------------
            # Handle control messages
            # -----------------------
            if self.control_socket in sockets:
                client_id, raw = self.control_socket.recv_multipart()
                self.last_seen[client_id] = time.time()

                msg = json.loads(raw.decode())
                msg_type = msg.get("type")
                msg_id = msg.get("id")

                # Protocol version check
                if msg.get("nika") != "1.0":
                    self._error(client_id, msg_id, "Unsupported NIKA version")
                    continue

                if msg_type == REQUEST:
                    self._handle_request(client_id, msg_id, msg)

                elif msg_type == PING:
                    self._pong(client_id, msg_id)

                elif msg_type == STREAM_START:
                    self._handle_stream_start(client_id, msg_id, msg)

                elif msg_type == STREAM_STOP:
                    self._handle_stream_stop(client_id, msg_id, msg)
                    
                elif msg_type == RESUME_STREAM:
                    self._handle_resume(client_id, msg_id, msg)

                else:
                    self._error(client_id, msg_id, "Unknown message type")

            # -----------------------
            # Cleanup dead clients
            # -----------------------
            now = time.time()
            for cid, ts in list(self.last_seen.items()):
                if now - ts > self.client_timeout:
                    self.client_streams.pop(cid, None)
                    self.last_seen.pop(cid, None)

    # -------------------------------------------------
    # Internal handlers
    # -------------------------------------------------

    def _handle_request(self, client_id, msg_id, msg):
        if not self.request_handler:
            self._error(client_id, msg_id, "No request handler registered")
            return

        try:
            result = self.request_handler(msg.get("payload", {}))
            response = envelope(
                msg_type=RESPONSE,
                msg_id=msg_id,
                sender="server",
                payload=result,
            )

        except Exception as e:
            response = envelope(
                msg_type=ERROR,
                msg_id=msg_id,
                sender="server",
                payload={
                    "code": ERR_INVALID_PAYLOAD,
                    "message": str(e),
                },
            )

        self.control_socket.send_multipart([
            client_id,
            json.dumps(response).encode(),
        ])

    def _handle_stream_start(self, client_id, msg_id, msg):
        stream = msg.get("payload", {}).get("stream")
        if not stream:
            self._error(client_id, msg_id, "Missing stream name")
            return

        self.client_streams[client_id].add(stream)

        self._ack(client_id, msg_id, STREAM_START, stream)

    def _handle_stream_stop(self, client_id, msg_id, msg):
        stream = msg.get("payload", {}).get("stream")
        if not stream:
            self._error(client_id, msg_id, "Missing stream name")
            return

        self.client_streams[client_id].discard(stream)

        self._ack(client_id, msg_id, STREAM_STOP, stream)
        
    def _handle_resume(self, client_id, msg_id, msg):
        payload = msg.get("payload", {})
        stream = payload.get("stream")
        last_seq = payload.get("last_seq")

        if not stream:
            self._error(client_id, msg_id, "Missing stream name")
            return

        # Register stream again
        self.client_streams[client_id].add(stream)

        # Acknowledge resume (explicit gap)
        self.control_socket.send_multipart([
            client_id,
            json.dumps(
                envelope(
                    msg_type=RESPONSE,
                    msg_id=msg_id,
                    sender="server",
                    payload={
                        "action": RESUME_STREAM,
                        "stream": stream,
                        "resumed_from": last_seq,
                        "note": "Resume is live-only; gaps may exist",
                    },
                )
            ).encode()
        ])

    def _ack(self, client_id, msg_id, action, topic=None):
        payload = {"action": action}
        if topic:
            payload["topic"] = topic

        self.control_socket.send_multipart([
            client_id,
            json.dumps(
                envelope(
                    msg_type=RESPONSE,
                    msg_id=msg_id,
                    sender="server",
                    payload=payload,
                )
            ).encode(),
        ])

    def _pong(self, client_id, msg_id):
        self.control_socket.send_multipart([
            client_id,
            json.dumps(
                envelope(
                    msg_type=PONG,
                    msg_id=msg_id,
                    sender="server",
                )
            ).encode(),
        ])

    def _error(self, client_id, msg_id, reason):
        self.control_socket.send_multipart([
            client_id,
            json.dumps(
                envelope(
                    msg_type=ERROR,
                    msg_id=msg_id,
                    sender="server",
                    payload={
                        "code": ERR_UNKNOWN_MESSAGE,
                        "message": reason,
                    },
                )
            ).encode(),
        ])

    # -------------------------------------------------
    # Streaming publisher
    # -------------------------------------------------

    def publish_stream(self, stream: str, seq: int, payload: dict):
        # Publish only if at least one client wants this stream
        if not any(stream in s for s in self.client_streams.values()):
            return

        now = time.time()
        limit = self.stream_limits.get(stream)

        if limit:
            last = self.stream_last_sent.get(stream, 0)
            if now - last < (1 / limit):
                return  # silent drop

            self.stream_last_sent[stream] = now

        msg = envelope(
            msg_type=STREAM,
            sender="server",
            payload={
                "stream": stream,
                "seq": seq,
                "data": payload,
            },
        )

        self.event_socket.send_multipart([
            f"stream:{stream}".encode(),
            json.dumps(msg).encode(),
        ])
