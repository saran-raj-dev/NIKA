# server.py
import zmq
import json
import time
from collections import defaultdict

from .envelope import envelope
from .protocol import *
from .auth import AuthManager


class NikaServer:
    def __init__(
        self,
        control_addr: str = "tcp://127.0.0.1:5555",
        event_addr: str = "tcp://127.0.0.1:5556",
        client_timeout: int = 15,
        stream_health_timeout: int = 20,
    ):
        self.context = zmq.Context()

        # -----------------------
        # Auth
        # -----------------------
        self.auth = AuthManager()

        # -----------------------
        # Limits & ACLs
        # -----------------------
        self.MAX_CLIENTS = 50
        self.MAX_STREAMS_PER_CLIENT = 5

        self.REQ_CACHE_TTL = 60  # seconds
        self._req_cache: dict[str, dict] = {}
        self._req_cache_time: dict[str, float] = {}

        # stream -> allowed logical client ids
        self.STREAM_ACL = {
            "numbers": {"client-a", "ops"},
        }

        # -----------------------
        # Stream config
        # -----------------------
        self.stream_limits = {
            "numbers": 10,  # msgs/sec
        }

        self.stream_last_sent: dict[tuple[bytes, str], float] = {}
        self.client_throttle: dict[bytes, dict[str, dict]] = defaultdict(dict)

        # -----------------------
        # Metrics
        # -----------------------
        self.metrics = {
            "requests_total": 0,
            "events_published": 0,
            "stream_sent": 0,
            "stream_dropped": 0,
            "clients_active": 0,
        }

        # -----------------------
        # REQ / RES commands (whitelist)
        # -----------------------
        self.command_handlers = {
            "PING": self._cmd_ping,
            "GET_STREAMS": self._cmd_get_streams,
            "GET_STATUS": self._cmd_get_status,
        }

        # -----------------------
        # Sockets
        # -----------------------
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.bind(control_addr)

        self.event_socket = self.context.socket(zmq.PUB)
        self.event_socket.bind(event_addr)

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)

        # -----------------------
        # State
        # -----------------------
        self.client_streams: dict[bytes, set[str]] = defaultdict(set)
        self.last_seen: dict[bytes, float] = {}
        self.connected_clients: set[bytes] = set()
        self.stream_health: dict[bytes, dict[str, dict]] = defaultdict(dict)

        self.client_timeout = client_timeout
        self.stream_health_timeout = stream_health_timeout

        print("NIKA server started")
        print(f"Control: {control_addr}")
        print(f"Events : {event_addr}")

        time.sleep(1)  # slow joiner mitigation

    # =================================================
    # Main loop
    # =================================================

    def start(self):
        while True:
            sockets = dict(self.poller.poll(timeout=100))
            now = time.time()

            # ---- Cleanup expired REQ cache ----
            for req_id, ts in list(self._req_cache_time.items()):
                if now - ts > self.REQ_CACHE_TTL:
                    self._req_cache.pop(req_id, None)
                    self._req_cache_time.pop(req_id, None)

            if self.control_socket in sockets:
                client_id, raw = self.control_socket.recv_multipart()
                self.last_seen[client_id] = now

                try:
                    msg = json.loads(raw.decode())
                except Exception:
                    self._error(client_id, None, "Invalid JSON")
                    continue

                msg_type = msg.get("type")
                msg_id = msg.get("id")

                # ---- Protocol version ----
                if msg.get("nika") != "1.0":
                    self._error(client_id, msg_id, "Unsupported NIKA version")
                    continue

                # ---- Auth gate ----
                if msg_type != CLIENT_JOIN:
                    if not self.auth.is_authenticated(client_id):
                        self._error(client_id, msg_id, "Client not authenticated")
                        continue

                # ---- Dispatch ----
                if msg_type == CLIENT_JOIN:
                    self._handle_client_join(client_id, msg_id, msg)
                elif msg_type == CLIENT_LEAVE:
                    self._ack(client_id, msg_id, CLIENT_LEAVE)
                    self._remove_client(client_id)
                elif msg_type == REQUEST:
                    self._handle_request(client_id, msg_id, msg)
                elif msg_type == PING:
                    self._pong(client_id, msg_id)
                elif msg_type == STREAM_START:
                    self._handle_stream_start(client_id, msg_id, msg)
                elif msg_type == STREAM_STOP:
                    self._handle_stream_stop(client_id, msg_id, msg)
                elif msg_type == RESUME_STREAM:
                    self._handle_resume(client_id, msg_id, msg)
                elif msg_type == STREAM_STATS:
                    self._handle_stream_stats(client_id, msg)
                elif msg_type == METRICS:
                    self._send_metrics(client_id, msg_id)
                else:
                    self._error(client_id, msg_id, "Unknown message type")

            # ---- Dead client cleanup ----
            for cid, ts in list(self.last_seen.items()):
                if now - ts > self.client_timeout:
                    self._remove_client(cid)

            self.metrics["clients_active"] = len(self.connected_clients)

    # =================================================
    # REQ / RES
    # =================================================

    def _handle_request(self, client_id, msg_id, msg):
        self.metrics["requests_total"] += 1

        if not msg_id:
            self._req_error(client_id, None, "MISSING_ID", "Request id required")
            return

        # ---- Idempotency ----
        if msg_id in self._req_cache:
            self.control_socket.send_multipart([
                client_id,
                json.dumps(self._req_cache[msg_id]).encode(),
            ])
            return

        payload = msg.get("payload") or {}
        command = payload.get("command")
        args = payload.get("args", {})

        if not command:
            self._req_error(client_id, msg_id, "MISSING_COMMAND", "command is required")
            return

        handler = self.command_handlers.get(command)
        if not handler:
            self._req_error(client_id, msg_id, "INVALID_COMMAND", command)
            return

        try:
            result = handler(client_id, args)
            response = envelope(
                msg_type=RESPONSE,
                msg_id=msg_id,
                sender="server",
                payload={"ok": True, "result": result},
            )
        except Exception as e:
            response = envelope(
                msg_type=ERROR,
                msg_id=msg_id,
                sender="server",
                payload={
                    "ok": False,
                    "error": {"code": "EXECUTION_ERROR", "message": str(e)},
                },
            )

        self._req_cache[msg_id] = response
        self._req_cache_time[msg_id] = time.time()

        self.control_socket.send_multipart([
            client_id,
            json.dumps(response).encode(),
        ])

    # =================================================
    # Command handlers
    # =================================================

    def _cmd_ping(self, client_id, args):
        return {"pong": True, "ts": time.time()}

    def _cmd_get_streams(self, client_id, args):
        streams = set(self.stream_limits.keys())
        streams |= {s for subs in self.client_streams.values() for s in subs}
        return {"streams": sorted(streams)}

    def _cmd_get_status(self, client_id, args):
        return {
            "clients_active": len(self.connected_clients),
            "metrics": self.metrics,
        }

    # =================================================
    # Streaming
    # =================================================

    def _handle_stream_start(self, client_id, msg_id, msg):
        stream = msg.get("payload", {}).get("stream")
        if not stream:
            self._error(client_id, msg_id, "Missing stream")
            return

        if len(self.client_streams[client_id]) >= self.MAX_STREAMS_PER_CLIENT:
            self._error(client_id, msg_id, "Stream limit exceeded")
            return

        ctx = self.auth.get_context(client_id)
        logical_id = ctx["client_id"]

        allowed = self.STREAM_ACL.get(stream)
        if allowed is None:
            self._error(client_id, msg_id, "Stream not allowed")
            return
        if logical_id not in allowed:
            self._error(client_id, msg_id, "Access denied for stream")
            return

        self.client_streams[client_id].add(stream)
        self._ack(client_id, msg_id, STREAM_START, stream)

    def _handle_stream_stop(self, client_id, msg_id, msg):
        stream = msg.get("payload", {}).get("stream")
        if not stream:
            self._error(client_id, msg_id, "Missing stream")
            return

        self.client_streams[client_id].discard(stream)
        self._ack(client_id, msg_id, STREAM_STOP, stream)

    def _handle_resume(self, client_id, msg_id, msg):
        stream = msg.get("payload", {}).get("stream")
        if not stream:
            self._error(client_id, msg_id, "Missing stream")
            return

        self.client_streams[client_id].add(stream)
        self._ack(client_id, msg_id, RESUME_STREAM, stream)

    # =================================================
    # Client lifecycle
    # =================================================

    def _handle_client_join(self, client_id, msg_id, msg):
        if len(self.connected_clients) >= self.MAX_CLIENTS:
            self._error(client_id, msg_id, "Server at capacity")
            return

        ok, reason = self.auth.authenticate(client_id, msg)
        if not ok:
            self._error(client_id, msg_id, reason)
            return

        self.connected_clients.add(client_id)
        self.last_seen[client_id] = time.time()
        self._ack(client_id, msg_id, CLIENT_JOIN)

    def _remove_client(self, client_id):
        self.auth.remove(client_id)
        self.connected_clients.discard(client_id)
        self.client_streams.pop(client_id, None)
        self.last_seen.pop(client_id, None)
        self.stream_health.pop(client_id, None)
        self.client_throttle.pop(client_id, None)

        for key in list(self.stream_last_sent):
            if key[0] == client_id:
                self.stream_last_sent.pop(key)

    # =================================================
    # Low-level helpers
    # =================================================

    def _send_metrics(self, client_id, msg_id):
        self.control_socket.send_multipart([
            client_id,
            json.dumps(envelope(
                msg_type=RESPONSE,
                msg_id=msg_id,
                sender="server",
                payload={"metrics": self.metrics},
            )).encode(),
        ])

    def _ack(self, client_id, msg_id, action, topic=None):
        payload = {"action": action}
        if topic:
            payload["topic"] = topic

        self.control_socket.send_multipart([
            client_id,
            json.dumps(envelope(
                msg_type=RESPONSE,
                msg_id=msg_id,
                sender="server",
                payload=payload,
            )).encode(),
        ])

    def _pong(self, client_id, msg_id):
        self.control_socket.send_multipart([
            client_id,
            json.dumps(envelope(
                msg_type=PONG,
                msg_id=msg_id,
                sender="server",
            )).encode(),
        ])

    def _req_error(self, client_id, msg_id, code, message):
        self.control_socket.send_multipart([
            client_id,
            json.dumps(envelope(
                msg_type=ERROR,
                msg_id=msg_id,
                sender="server",
                payload={
                    "ok": False,
                    "error": {"code": code, "message": message},
                },
            )).encode(),
        ])

    def _error(self, client_id, msg_id, reason):
        self._req_error(client_id, msg_id, "ERROR", reason)
