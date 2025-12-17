# client.py
import zmq
import json
import uuid
import time
import threading
from typing import Callable, Optional

from .envelope import envelope
from .protocol import *


class NikaClient:
    def __init__(
        self,
        control_addr: str = "tcp://127.0.0.1:5555",
        event_addr: str = "tcp://127.0.0.1:5556",
        api_key: str | None = None,
        heartbeat_interval: int = 3,
        heartbeat_timeout: int = 10,
        stream_stats_interval: int = 5,
    ):
        if not api_key:
            raise ValueError("api_key is required for NIKA client authentication")

        self.api_key = api_key
        self.context = zmq.Context()

        # -----------------------
        # Stream state
        # -----------------------
        self.last_seq: dict[str, int] = {}
        self.drop_count: dict[str, int] = {}

        self.stream_stats_interval = stream_stats_interval
        self._running = True

        # -----------------------
        # Control socket
        # -----------------------
        self.control_socket = self.context.socket(zmq.DEALER)
        self.client_id = f"client-{uuid.uuid4()}"
        self.control_socket.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.control_socket.connect(control_addr)

        # -----------------------
        # Event socket
        # -----------------------
        self.event_socket = self.context.socket(zmq.SUB)
        self.event_socket.connect(event_addr)

        # -----------------------
        # Poller
        # -----------------------
        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.event_socket, zmq.POLLIN)

        # -----------------------
        # Handlers
        # -----------------------
        self.event_handler: Optional[Callable[[str, dict], None]] = None
        self.response_handler: Optional[Callable[[dict], None]] = None
        self.error_handler: Optional[Callable[[dict], None]] = None

        # -----------------------
        # Heartbeat
        # -----------------------
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.last_ping_time = 0.0
        self.last_pong_time = time.time()

        # -----------------------
        # Join server (AUTH)
        # -----------------------
        self._send(
            CLIENT_JOIN,
            auth={"api_key": self.api_key},
        )

        print(f"NIKA client started ({self.client_id})")

    # =================================================
    # Public API — REQ / RES
    # =================================================

    def request(self, command: str, args: dict | None = None):
        """
        Send a REQ/RES command.
        Idempotency is handled by the server using msg_id.
        """
        self._send(
            REQUEST,
            payload={
                "command": command,
                "args": args or {},
            },
        )

    def get_status(self):
        self.request("GET_STATUS")

    def get_streams(self):
        self.request("GET_STREAMS")

    # =================================================
    # Streaming API
    # =================================================

    def subscribe(self, topic: str):
        self.event_socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def start_stream(self, stream: str):
        self._send(STREAM_START, {"stream": stream})

    def stop_stream(self, stream: str):
        self._send(STREAM_STOP, {"stream": stream})
        self.last_seq.pop(stream, None)
        self.drop_count.pop(stream, None)

    def resume_stream(self, stream: str):
        self._send(
            RESUME_STREAM,
            {
                "stream": stream,
                "last_seq": self.last_seq.get(stream),
            },
        )

    def get_metrics(self):
        self._send(METRICS)

    # =================================================
    # Handlers
    # =================================================

    def on_event(self, handler: Callable[[str, dict], None]):
        self.event_handler = handler

    def on_response(self, handler: Callable[[dict], None]):
        self.response_handler = handler

    def on_error(self, handler: Callable[[dict], None]):
        self.error_handler = handler

    # =================================================
    # Lifecycle
    # =================================================

    def close(self):
        self._running = False
        time.sleep(0.1)
        try:
            self._send(CLIENT_LEAVE)
            self.control_socket.poll(100)
        finally:
            self.control_socket.close(0)
            self.event_socket.close(0)
            self.context.term()

    # =================================================
    # Main loop
    # =================================================

    def start(self):
        stats_thread = threading.Thread(
            target=self._report_stream_stats_loop,
            daemon=True,
        )
        stats_thread.start()

        try:
            while self._running:
                now = time.time()

                # ---- Heartbeat ----
                if now - self.last_ping_time >= self.heartbeat_interval:
                    self._send(PING)
                    self.last_ping_time = now

                sockets = dict(self.poller.poll(timeout=1000))

                if self.control_socket in sockets and self._running:
                    self._handle_control()

                if self.event_socket in sockets:
                    self._handle_event()

                if time.time() - self.last_pong_time > self.heartbeat_timeout:
                    raise RuntimeError("Server heartbeat lost")

        except KeyboardInterrupt:
            print("Client shutting down...")
            self.close()

    # =================================================
    # Internal helpers
    # =================================================

    def _send(
        self,
        msg_type: str,
        payload: dict | None = None,
        auth: dict | None = None,
    ):
        """
        Internal send helper.
        NOTE:
        - msg_id is generated by envelope()
        - protocol version is set by envelope()
        """
        msg = envelope(
            msg_type=msg_type,
            payload=payload,
            sender="client",
        )

        if auth:
            msg["auth"] = auth

        try:
            self.control_socket.send(json.dumps(msg).encode(), zmq.NOBLOCK)
        except zmq.ZMQError:
            pass  # socket closing or unavailable

    def _handle_control(self):
        try:
            msg = json.loads(self.control_socket.recv().decode())
        except zmq.ZMQError:
            return

        msg_type = msg["type"]

        if msg_type == PONG:
            self.last_pong_time = time.time()
            return

        if msg_type == RESPONSE:
            if self.response_handler:
                self.response_handler(msg)
            return

        if msg_type == ERROR:
            if self.error_handler:
                self.error_handler(msg)
            else:
                print("[ERROR]", msg)

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
                dropped = max(seq - last - 1, 0)
                self.drop_count[stream] = self.drop_count.get(stream, 0) + dropped
                print(
                    f"[WARN] Stream '{stream}' dropped data "
                    f"(expected {last + 1}, got {seq})"
                )

            self.last_seq[stream] = seq
            self.event_handler(stream, payload["data"])
        else:
            self.event_handler(payload["topic"], payload["data"])

    def _report_stream_stats_loop(self):
        while self._running:
            time.sleep(self.stream_stats_interval)

            if not self.last_seq:
                continue

            for stream, last_seq in list(self.last_seq.items()):
                self._send(
                    STREAM_STATS,
                    {
                        "stream": stream,
                        "last_seq": last_seq,
                        "drops": self.drop_count.get(stream, 0),
                    },
                )
                self.drop_count[stream] = 0
