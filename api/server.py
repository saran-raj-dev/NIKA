# nika/server.py

import zmq
import json
import time
from typing import Callable, Dict
from .protocol import *

class NikaServer:
    def __init__(
        self,
        control_addr: str = "tcp://127.0.0.1:5555",
        event_addr: str = "tcp://127.0.0.1:5556"
    ):
        self.context = zmq.Context()

        # Control plane
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.bind(control_addr)

        # Data plane
        self.event_socket = self.context.socket(zmq.PUB)
        self.event_socket.bind(event_addr)

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)

        # User-defined handlers
        self.request_handler = None

        # ✅ REQUIRED: stream registry
        self.active_streams: set[str] = set()

        print("NIKA server started")
        print(f"Control: {control_addr}")
        print(f"Events : {event_addr}")

        # ZMQ slow joiner fix
        time.sleep(1)


    # -----------------------
    # Public API
    # -----------------------

    def on_request(self, handler: Callable[[dict], dict]):
        """
        Register request handler.
        Handler receives payload and returns payload.
        """
        self.request_handler = handler

    def publish(self, topic: str, payload: dict):
        """
        Publish an event to subscribers.
        """
        event = {
            "type": EVENT,
            "topic": topic,
            "payload": payload
        }

        self.event_socket.send_multipart([
            topic.encode(),
            json.dumps(event).encode()
        ])

    # -----------------------
    # Main loop
    # -----------------------

    def start(self):
        while True:
            sockets = dict(self.poller.poll(timeout=100))

            if self.control_socket in sockets:
                client_id, raw = self.control_socket.recv_multipart()
                msg = json.loads(raw.decode())

                msg_type = msg.get("type")
                msg_id = msg.get("id")

                if msg_type == REQUEST:
                    self._handle_request(client_id, msg_id, msg)

                elif msg_type in (SUBSCRIBE, UNSUBSCRIBE):
                    self._ack(
                        client_id,
                        msg_id,
                        msg_type,
                        topic=msg.get("topic")
                    )


                elif msg_type == PING:
                    self._pong(client_id, msg_id)
                
                elif msg_type == STREAM_START:
                    stream = msg.get("stream")
                    if stream:
                        self.active_streams.add(stream)
                        self._ack(client_id, msg_id, STREAM_START, stream)

                elif msg_type == STREAM_STOP:
                    stream = msg.get("stream")
                    if stream:
                        self.active_streams.discard(stream)
                        self._ack(client_id, msg_id, STREAM_STOP, stream)


                else:
                    self._error(client_id, msg_id, "Unknown message type")

    # -----------------------
    # Internal handlers
    # -----------------------

    def _handle_request(self, client_id, msg_id, msg):
        if not self.request_handler:
            self._error(client_id, msg_id, "No request handler registered")
            return

        try:
            result = self.request_handler(msg.get("payload", {}))

            response = {
                "type": RESPONSE,
                "id": msg_id,
                "payload": result
            }

        except Exception as e:
            response = {
                "type": ERROR,
                "id": msg_id,
                "payload": {"error": str(e)}
            }

        self.control_socket.send_multipart([
            client_id,
            json.dumps(response).encode()
        ])

    def _ack(self, client_id, msg_id, action, topic=None):
        payload = {"action": action}

        if topic:
            payload["topic"] = topic

        self.control_socket.send_multipart([
            client_id,
            json.dumps({
                "type": RESPONSE,
                "id": msg_id,
                "payload": payload
            }).encode()
        ])

    def _pong(self, client_id, msg_id):
        self.control_socket.send_multipart([
            client_id,
            json.dumps({
                "type": PONG,
                "id": msg_id
            }).encode()
        ])

    def _error(self, client_id, msg_id, reason):
        self.control_socket.send_multipart([
            client_id,
            json.dumps({
                "type": ERROR,
                "id": msg_id,
                "payload": {"error": reason}
            }).encode()
        ])
        
    def publish_stream(self, stream: str, seq: int, payload: dict):
        if stream not in self.active_streams:
            return

        msg = {
            "type": STREAM,
            "stream": stream,
            "seq": seq,
            "payload": payload
        }

        self.event_socket.send_multipart([
            f"stream:{stream}".encode(),
            json.dumps(msg).encode()
        ])

