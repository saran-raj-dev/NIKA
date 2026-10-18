# server.py
"""
NIKA uplink server (platform side).

Peers connect, prove their identity with the NIKA Secure Channel, and stream
topic + payload messages. The application decides everything specific to it
through callbacks:

    authorize(public_key) -> peer_id | None       who may connect
    on_data(DataMessage)  -> None/True | str      store it; a str rejects it
    on_route(relay_id, origin, active) -> bool    may relay carry origin?
    on_session(peer_id, active, meta)             connection came / went
    on_command_ack(peer_id, command_id, status, detail)

Every UPLINK_DATA is acknowledged only after on_data returned, so an ack means
"the application has it". Messages from one peer are handled in order.

Optional `psk`: a deployment-wide pre-shared key (NSC IKpsk2). Every client must
then hold the same key; handshakes without it are dropped.

A new session only becomes the peer's session (replacing an older one) once
its first encrypted frame authenticates, i.e. once the client has shown it
completed the handshake, not merely sent a valid message 1.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import zmq
import zmq.asyncio

from ..protocol import *
from ..secure import NSCError, ServerHandshake, Session, StaticKey, fingerprint, parse_psk
from .common import (
    K_DATA, K_ERROR, K_HANDSHAKE1, K_HANDSHAKE2, MAX_FRAME, DataMessage,
    decode, encode, get_payload, put_payload,
)

log = logging.getLogger("nika.uplink.server")

Authorize = Callable[[bytes], Awaitable[str | None]]
OnData = Callable[[DataMessage], Awaitable[bool | str | None]]


@dataclass
class _Conn:
    ident: bytes
    session: Session
    peer_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.monotonic)
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=10_000))
    worker: asyncio.Task | None = None
    routes: set[str] = field(default_factory=set)
    confirmed: bool = False       # first frame authenticated: the session is live
    msgs_in: int = 0
    msgs_rejected: int = 0


class NikaUplinkServer:
    def __init__(
        self,
        key: StaticKey,
        *,
        bind: str = "tcp://0.0.0.0:47900",
        authorize: Authorize,
        on_data: OnData,
        on_route: Callable[[str, str, bool], Awaitable[bool]] | None = None,
        on_session: Callable[[str, bool, dict], Awaitable[None]] | None = None,
        on_command_ack: Callable[[str, str, str, str | None], Awaitable[None]] | None = None,
        session_timeout: float = 30.0,
        max_skew: float = 120.0,
        psk: bytes | str | None = None,
    ):
        self.key = key
        self._psk = parse_psk(psk)                     # validate now, not on the first connect
        self.bind = bind
        self._authorize = authorize
        self._on_data = on_data
        self._on_route = on_route
        self._on_session = on_session
        self._on_command_ack = on_command_ack
        self.session_timeout = session_timeout
        self.max_skew = max_skew
        self.endpoint: str | None = None               # actual bound endpoint, after start()

        self._ctx: zmq.asyncio.Context | None = None
        self._sock: zmq.asyncio.Socket | None = None
        self._tasks: list[asyncio.Task] = []
        self._send_lock = asyncio.Lock()
        self._conns: dict[bytes, _Conn] = {}          # zmq identity -> connection
        self._by_peer: dict[str, _Conn] = {}          # peer_id -> current connection
        self._routes: dict[str, str] = {}             # origin -> relay peer_id
        self._seen_ephemeral: dict[bytes, float] = {}
        self.metrics = {"handshakes": 0, "handshakes_rejected": 0, "frames_rejected": 0,
                        "data_in": 0, "data_rejected": 0, "commands_sent": 0}

    # =================================================
    # Lifecycle
    # =================================================

    async def start(self):
        self._ctx = zmq.asyncio.Context()
        self._sock = self._ctx.socket(zmq.ROUTER)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self._sock.setsockopt(zmq.MAXMSGSIZE, MAX_FRAME + 4096)
        self._sock.bind(self.bind)                     # "tcp://host:*" picks a free port
        self.endpoint = self._sock.getsockopt_string(zmq.LAST_ENDPOINT)
        self._tasks = [asyncio.create_task(self._recv_loop(), name="nika-uplink-recv"),
                       asyncio.create_task(self._reaper(), name="nika-uplink-reaper")]
        log.info("nika uplink server on %s (key %s%s)", self.endpoint, self.key.fingerprint,
                 ", pre-shared key required" if self._psk else "")

    async def stop(self):
        for conn in list(self._conns.values()):
            await self._send_session(conn, {"t": UPLINK_BYE})
            await self._drop(conn, "server stopping")
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._tasks = []
        if self._sock is not None:
            self._sock.close(0)
        if self._ctx is not None:
            self._ctx.term()
        self._sock = self._ctx = None

    # =================================================
    # Public API
    # =================================================

    def sessions(self) -> list[dict]:
        now = time.monotonic()
        return [
            {
                "peer_id": c.peer_id,
                "fingerprint": c.session.peer_fingerprint,
                "connected_at": c.connected_at,
                "idle_s": round(now - c.last_seen, 1),
                "meta": dict(c.meta),
                "relaying_for": sorted(c.routes),
                "msgs_in": c.msgs_in,
                "msgs_rejected": c.msgs_rejected,
            }
            for c in self._by_peer.values()
        ]

    async def disconnect(self, peer_id: str, reason: str = "closed by server") -> bool:
        """End a peer's session (e.g. its key was revoked). It may reconnect, and is then
        authorised again from scratch."""
        conn = self._by_peer.get(peer_id)
        if conn is None:
            return False
        await self._send_session(conn, {"t": UPLINK_BYE})
        await self._drop(conn, reason)
        return True

    def route_of(self, peer_id: str) -> str | None:
        """'direct', the relay's peer_id, or None if unreachable."""
        if peer_id in self._by_peer:
            return "direct"
        relay = self._routes.get(peer_id)
        return relay if relay in self._by_peer else None

    async def send_command(self, peer_id: str, topic: str, payload: bytes | str, *,
                           command_id: str | None = None,
                           expires_at: float | None = None) -> str | None:
        """Send a command to a peer, directly or through the relay that carries it.
        Returns the route used ('direct' or the relay's peer_id), or None if the
        peer can't be reached right now. The outcome arrives via on_command_ack."""
        command_id = command_id or str(uuid.uuid4())
        msg = put_payload({"t": UPLINK_CMD, "command_id": command_id, "topic": topic,
                           "expires_at": expires_at}, payload)
        conn = self._by_peer.get(peer_id)
        route = "direct"
        if conn is None:
            relay = self._routes.get(peer_id)
            conn = self._by_peer.get(relay) if relay else None
            if conn is None:
                return None
            msg["target"] = peer_id
            route = relay
        if not await self._send_session(conn, msg):
            return None
        self.metrics["commands_sent"] += 1
        return route

    # =================================================
    # Receive
    # =================================================

    async def _recv_loop(self):
        assert self._sock is not None
        while True:
            try:
                frames = await self._sock.recv_multipart()
            except asyncio.CancelledError:
                raise
            except zmq.ZMQError as e:
                log.warning("uplink recv error: %s", e)
                continue
            if len(frames) != 3:
                continue
            ident, kind, data = frames
            try:
                if kind == K_HANDSHAKE1:
                    await self._handle_handshake(ident, data)
                elif kind == K_DATA:
                    await self._handle_frame(ident, data)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("uplink: error handling frame")

    async def _handle_handshake(self, ident: bytes, data: bytes):
        hs = ServerHandshake(self.key, max_skew=self.max_skew, psk=self._psk)
        try:
            client_key = hs.read_message1(data)
        except NSCError as e:
            self.metrics["handshakes_rejected"] += 1
            log.info("uplink handshake rejected: %s", e)
            return  # say nothing to an unauthenticated stranger

        # a replayed message 1 must not create (and so replace) a session
        now = time.monotonic()
        eph = hs.client_ephemeral or b""
        if eph in self._seen_ephemeral:
            self.metrics["handshakes_rejected"] += 1
            return
        self._seen_ephemeral[eph] = now + 2 * self.max_skew

        peer_id = await self._authorize(client_key)
        if not peer_id:
            self.metrics["handshakes_rejected"] += 1
            log.warning("uplink: unknown key %s refused", fingerprint(client_key))
            await self._raw_send(ident, K_ERROR, encode({"code": ERR_UPLINK_NOT_AUTHORISED}))
            return

        reply, session = hs.write_message2(encode({"server": self.key.fingerprint}))
        existing = self._conns.get(ident)
        if existing is not None:
            await self._drop(existing, "re-handshake on the same connection")
        # pending until its first frame authenticates (see _confirm)
        self._conns[ident] = _Conn(ident=ident, session=session, peer_id=peer_id)
        self.metrics["handshakes"] += 1
        await self._raw_send(ident, K_HANDSHAKE2, reply)

    async def _confirm(self, conn: _Conn):
        """The client completed the handshake: make this the peer's session."""
        conn.confirmed = True
        old = self._by_peer.get(conn.peer_id)
        if old is not None and old is not conn:
            await self._drop(old, "replaced by a new session")
        self._by_peer[conn.peer_id] = conn
        conn.worker = asyncio.create_task(self._worker(conn), name=f"nika-uplink-{conn.peer_id}")
        log.info("uplink: %s connected (%s)", conn.peer_id, conn.session.peer_fingerprint)

    async def _handle_frame(self, ident: bytes, data: bytes):
        conn = self._conns.get(ident)
        if conn is None:
            return
        try:
            _, plaintext = conn.session.decrypt(data)
            msg = decode(plaintext)
        except (NSCError, ValueError) as e:
            self.metrics["frames_rejected"] += 1
            log.warning("uplink: bad frame from %s: %s", conn.peer_id, e)
            return
        conn.last_seen = time.monotonic()
        if not conn.confirmed:
            await self._confirm(conn)
        t = msg.get("t")

        if t == UPLINK_PING:
            await self._send_session(conn, {"t": UPLINK_PONG})
        elif t == UPLINK_HELLO:
            conn.meta = msg.get("meta") if isinstance(msg.get("meta"), dict) else {}
            await self._send_session(conn, {"t": UPLINK_WELCOME, "peer_id": conn.peer_id})
            await self._call(self._on_session, conn.peer_id, True, dict(conn.meta))
        elif t in (UPLINK_DATA, UPLINK_ROUTE):
            try:
                conn.queue.put_nowait(msg)       # handled in order by the worker
            except asyncio.QueueFull:
                if t == UPLINK_DATA:
                    await self._send_session(conn, {"t": UPLINK_ACK, "msg_id": msg.get("msg_id"),
                                                    "ok": False, "reason": ERR_UPLINK_STORE_FAILED})
        elif t == UPLINK_CMD_ACK:
            await self._call(self._on_command_ack, conn.peer_id, str(msg.get("command_id")),
                             str(msg.get("status")), msg.get("detail"))
        elif t == UPLINK_BYE:
            await self._drop(conn, "peer said bye")

    async def _worker(self, conn: _Conn):
        while True:
            msg = await conn.queue.get()
            if msg.get("t") == UPLINK_DATA:
                await self._process_data(conn, msg)
            else:
                await self._process_route(conn, msg)

    async def _process_data(self, conn: _Conn, msg: dict):
        msg_id = str(msg.get("msg_id") or "")
        topic = msg.get("topic")
        if not msg_id or not isinstance(topic, str) or not topic:
            await self._send_session(conn, {"t": UPLINK_ACK, "msg_id": msg_id, "ok": False,
                                            "reason": ERR_UPLINK_BAD_MESSAGE})
            return
        origin = msg.get("origin") or conn.peer_id
        if origin != conn.peer_id and origin not in conn.routes:
            reason = ERR_UPLINK_ORIGIN_DENIED       # must announce (and be allowed) the route first
        else:
            dm = DataMessage(peer_id=conn.peer_id, origin=str(origin), msg_id=msg_id, topic=topic,
                             payload=get_payload(msg), qos=int(msg.get("qos", 1)), meta=dict(conn.meta))
            try:
                result = await self._on_data(dm)
                reason = result if isinstance(result, str) else (None if result in (None, True) else ERR_UPLINK_STORE_FAILED)
            except Exception:
                log.exception("uplink: on_data failed")
                reason = ERR_UPLINK_STORE_FAILED
        conn.msgs_in += 1
        self.metrics["data_in"] += 1
        if reason:
            conn.msgs_rejected += 1
            self.metrics["data_rejected"] += 1
        await self._send_session(conn, {"t": UPLINK_ACK, "msg_id": msg_id, "ok": reason is None,
                                        **({"reason": reason} if reason else {})})

    async def _process_route(self, conn: _Conn, msg: dict):
        origin = msg.get("origin")
        active = bool(msg.get("active"))
        if not isinstance(origin, str) or not origin or origin == conn.peer_id:
            return
        if active:
            allowed = True
            if self._on_route is not None:
                try:
                    allowed = bool(await self._on_route(conn.peer_id, origin, True))
                except Exception:
                    log.exception("uplink: on_route failed")
                    allowed = False
            if allowed:
                conn.routes.add(origin)
                self._routes[origin] = conn.peer_id
            await self._send_session(conn, {"t": UPLINK_ACK, "msg_id": f"route:{origin}",
                                            "ok": allowed,
                                            **({} if allowed else {"reason": ERR_UPLINK_ORIGIN_DENIED})})
        else:
            conn.routes.discard(origin)
            if self._routes.get(origin) == conn.peer_id:
                del self._routes[origin]
            await self._call(self._on_route, conn.peer_id, origin, False)

    # =================================================
    # Housekeeping
    # =================================================

    async def _reaper(self):
        while True:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            for conn in list(self._conns.values()):
                if now - conn.last_seen > self.session_timeout:
                    await self._drop(conn, "timed out")
            for eph, exp in list(self._seen_ephemeral.items()):
                if exp < now:
                    del self._seen_ephemeral[eph]

    async def _drop(self, conn: _Conn, why: str):
        if self._conns.pop(conn.ident, None) is None:
            return
        if conn.worker is not None:
            conn.worker.cancel()
        for origin in list(conn.routes):
            if self._routes.get(origin) == conn.peer_id:
                del self._routes[origin]
            await self._call(self._on_route, conn.peer_id, origin, False)
        if conn.peer_id and self._by_peer.get(conn.peer_id) is conn:
            del self._by_peer[conn.peer_id]
            await self._call(self._on_session, conn.peer_id, False, dict(conn.meta))
        log.info("uplink: %s disconnected (%s)", conn.peer_id, why)

    # =================================================
    # Send
    # =================================================

    async def _send_session(self, conn: _Conn, msg: dict) -> bool:
        try:
            frame = conn.session.encrypt(encode(msg))
        except NSCError:
            await self._drop(conn, "session exhausted")
            return False
        return await self._raw_send(conn.ident, K_DATA, frame)

    async def _raw_send(self, ident: bytes, kind: bytes, data: bytes) -> bool:
        if self._sock is None:
            return False
        try:
            async with self._send_lock:
                await self._sock.send_multipart([ident, kind, data], flags=zmq.NOBLOCK)
            return True
        except zmq.ZMQError as e:
            log.debug("uplink send to %r failed: %s", ident, e)
            return False

    async def _call(self, fn, *args):
        if fn is None:
            return None
        try:
            return await fn(*args)
        except Exception:
            log.exception("uplink callback %s failed", getattr(fn, "__name__", fn))
            return None
