# client.py
"""
NIKA uplink client (peer side).

Keeps one authenticated, encrypted session to a NIKA uplink server and
reconnects forever with backoff. The application:

    await client.publish(topic, payload, msg_id=..., origin=None)
        → returns when the server acknowledged it (stored), raises UplinkError
          if the server rejected it, UplinkUnavailable if not connected/timed out
    await client.announce_route(origin, True/False)
        → "I carry traffic for origin"; replayed automatically after reconnects
    on_command(Command) -> status str
        → handle a downstream command (already de-duplicated and expiry-checked)

Optional `psk`: the deployment pre-shared key, if the server requires one
(NSC IKpsk2). It must match the server's exactly.

Nothing here knows what topics or payloads mean.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from typing import Any, Awaitable, Callable

import zmq
import zmq.asyncio

from ..mesh.commands import ACCEPT, CommandGuard
from ..protocol import *
from ..secure import ClientHandshake, NSCError, Session, StaticKey, fingerprint, parse_psk
from .common import (
    K_DATA, K_ERROR, K_HANDSHAKE1, K_HANDSHAKE2, MAX_FRAME, Command, UplinkError,
    UplinkUnavailable, decode, encode, get_payload, put_payload,
)

log = logging.getLogger("nika.uplink.client")

# connection states
DISCONNECTED = "disconnected"
CONNECTING = "connecting"
CONNECTED = "connected"
REFUSED = "refused"          # the server does not know our key


class NikaUplinkClient:
    def __init__(
        self,
        key: StaticKey,
        server_public: bytes,
        *,
        endpoint: str,
        meta: dict[str, Any] | None = None,
        on_command: Callable[[Command], Awaitable[str | None]] | None = None,
        on_state: Callable[[str, str | None], Awaitable[None] | None] | None = None,
        command_guard: CommandGuard | None = None,
        heartbeat: float = 5.0,
        timeout: float = 15.0,
        handshake_timeout: float = 5.0,
        ack_timeout: float = 15.0,
        max_backoff: float = 30.0,
        psk: bytes | str | None = None,
    ):
        self.key = key
        self.server_public = server_public
        self._psk = parse_psk(psk)
        self.endpoint = endpoint
        self.meta = dict(meta or {})
        self._on_command = on_command
        self._on_state = on_state
        self.commands = command_guard or CommandGuard()
        self.heartbeat = heartbeat
        self.timeout = timeout
        self.handshake_timeout = handshake_timeout
        self.ack_timeout = ack_timeout
        self.max_backoff = max_backoff

        self.state = DISCONNECTED
        self.last_error: str | None = None
        self.connected_since: float | None = None
        self.peer_id: str | None = None

        self._ctx: zmq.asyncio.Context | None = None
        self._sock: zmq.asyncio.Socket | None = None
        self._session: Session | None = None
        self._task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}
        self._routes: set[str] = set()
        self._route_inflight: dict[str, asyncio.Task] = {}   # origin -> pending announcement
        self._stopping = False
        self._ready = asyncio.Event()
        self._last_rx = 0.0
        self.metrics = {"connects": 0, "published": 0, "acked": 0, "rejected": 0,
                        "commands": 0, "frames_rejected": 0}

    @property
    def server_fingerprint(self) -> str:
        return fingerprint(self.server_public)

    @property
    def connected(self) -> bool:
        return self.state == CONNECTED

    # =================================================
    # Lifecycle
    # =================================================

    async def start(self):
        if self._task is None or self._task.done():
            self._stopping = False
            self._ctx = zmq.asyncio.Context()
            self._task = asyncio.create_task(self._run(), name="nika-uplink-client")

    async def stop(self):
        self._stopping = True
        if self._session is not None:
            with contextlib.suppress(Exception):
                await self._send({"t": UPLINK_BYE})
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        self._close_socket()
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        self._fail_pending(ERR_UPLINK_UNAVAILABLE)
        await self._set_state(DISCONNECTED, None)

    async def wait_connected(self, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def set_meta(self, meta: dict[str, Any]):
        """Sent in HELLO on the next (re)connect."""
        self.meta = dict(meta or {})

    # =================================================
    # Public API
    # =================================================

    async def publish(self, topic: str, payload: bytes | str, *, msg_id: str | None = None,
                      origin: str | None = None, qos: int = 1,
                      timeout: float | None = None) -> str:
        """Send and wait for the server's ack. Returns msg_id."""
        if not self.connected:
            raise UplinkUnavailable(ERR_UPLINK_UNAVAILABLE)
        msg_id = msg_id or str(uuid.uuid4())
        msg = put_payload({"t": UPLINK_DATA, "msg_id": msg_id, "topic": topic, "qos": qos}, payload)
        if origin:
            msg["origin"] = origin
        await self._request(msg_id, msg, timeout)
        return msg_id

    async def announce_route(self, origin: str, active: bool, *, timeout: float | None = None) -> bool:
        """Tell the server we carry (or stopped carrying) origin's traffic.
        Returns False if the server refused the route."""
        if active:
            self._routes.add(origin)
        else:
            self._routes.discard(origin)
        if not self.connected:
            return active is False   # replayed on reconnect
        msg = {"t": UPLINK_ROUTE, "origin": origin, "active": active}
        if not active:
            await self._send(msg)
            return True
        # Many messages for a new origin can arrive at once; they all wait on ONE
        # announcement instead of superseding each other's.
        task = self._route_inflight.get(origin)
        if task is None or task.done():
            task = asyncio.ensure_future(self._announce(origin, msg, timeout))
            self._route_inflight[origin] = task
        return await asyncio.shield(task)

    async def _announce(self, origin: str, msg: dict, timeout: float | None) -> bool:
        try:
            await self._request(f"route:{origin}", msg, timeout)
            return True
        except UplinkUnavailable:
            raise
        except UplinkError:
            self._routes.discard(origin)
            return False
        finally:
            if self._route_inflight.get(origin) is asyncio.current_task():
                self._route_inflight.pop(origin, None)

    # =================================================
    # Connection loop
    # =================================================

    async def _run(self):
        backoff = 1.0
        while not self._stopping:
            await self._set_state(CONNECTING, self.last_error)
            refused = False
            try:
                await self._connect_once()
                backoff = 1.0
                await self._serve()
            except asyncio.CancelledError:
                raise
            except _Refused as e:
                refused = True
                await self._set_state(REFUSED, str(e))
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"[:300]
                log.info("uplink: %s", self.last_error)
            finally:
                self._ready.clear()
                self._session = None
                self.connected_since = None
                self._close_socket()
                self._fail_pending(ERR_UPLINK_UNAVAILABLE)
            if self._stopping:
                break
            if not refused:
                await self._set_state(DISCONNECTED, self.last_error)
            await asyncio.sleep(self.max_backoff if refused else backoff)
            backoff = min(backoff * 2, self.max_backoff)

    async def _connect_once(self):
        assert self._ctx is not None
        self._close_socket()
        sock = self._ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.MAXMSGSIZE, MAX_FRAME + 4096)
        sock.setsockopt(zmq.IMMEDIATE, 1)
        sock.connect(self.endpoint)
        self._sock = sock

        hs = ClientHandshake(self.key, self.server_public, psk=self._psk)
        deadline = time.monotonic() + self.handshake_timeout
        try:
            # IMMEDIATE: this waits until a TCP connection exists, so bound it
            await asyncio.wait_for(sock.send_multipart([K_HANDSHAKE1, hs.write_message1()]),
                                   self.handshake_timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"cannot reach {self.endpoint}")
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not await sock.poll(remaining * 1000):
                # a server drops bad handshakes silently, wrong psk included
                raise TimeoutError(f"no handshake answer from {self.endpoint} "
                                   f"(check the platform key and the pre-shared key setting)")
            frames = await sock.recv_multipart()
            if len(frames) != 2:
                continue
            kind, data = frames
            if kind == K_ERROR:
                code = decode_error(data)
                self.last_error = code
                raise _Refused(code)
            if kind == K_HANDSHAKE2:
                try:
                    self._session, _ = hs.read_message2(data)
                except NSCError as e:
                    raise ConnectionError(f"server failed authentication ({e})")
                break

        self._last_rx = time.monotonic()
        await self._send({"t": UPLINK_HELLO, "meta": self.meta})
        # wait for WELCOME
        while not self._ready.is_set():
            remaining = deadline + self.handshake_timeout - time.monotonic()
            if remaining <= 0 or not await sock.poll(remaining * 1000):
                raise TimeoutError("no welcome from server")
            await self._handle_frames(await sock.recv_multipart())

    async def _serve(self):
        assert self._sock is not None
        last_ping = 0.0
        while not self._stopping:
            now = time.monotonic()
            if now - last_ping >= self.heartbeat:
                await self._send({"t": UPLINK_PING})
                last_ping = now
            if now - self._last_rx > self.timeout:
                raise ConnectionError("server stopped answering")
            if await self._sock.poll(500):
                await self._handle_frames(await self._sock.recv_multipart())

    async def _handle_frames(self, frames: list[bytes]):
        if len(frames) != 2:
            return
        kind, data = frames
        if kind == K_ERROR:
            raise _Refused(decode_error(data))
        if kind != K_DATA or self._session is None:
            return
        try:
            _, plaintext = self._session.decrypt(data)
            msg = decode(plaintext)
        except (NSCError, ValueError):
            self.metrics["frames_rejected"] += 1
            return
        self._last_rx = time.monotonic()
        t = msg.get("t")

        if t == UPLINK_WELCOME:
            self.peer_id = msg.get("peer_id")
            self.connected_since = time.time()
            self.metrics["connects"] += 1
            self.last_error = None
            await self._set_state(CONNECTED, None)
            self._ready.set()
            for origin in sorted(self._routes):
                asyncio.create_task(self._replay_route(origin))
        elif t == UPLINK_ACK:
            fut = self._pending.pop(str(msg.get("msg_id")), None)
            if fut is not None and not fut.done():
                if msg.get("ok"):
                    self.metrics["acked"] += 1
                    fut.set_result(None)
                else:
                    self.metrics["rejected"] += 1
                    fut.set_exception(UplinkError(str(msg.get("reason") or "rejected")))
        elif t == UPLINK_CMD:
            asyncio.create_task(self._handle_command(msg))
        elif t == UPLINK_BYE:
            raise ConnectionError("server closed the session")

    async def _replay_route(self, origin: str):
        with contextlib.suppress(Exception):
            await self.announce_route(origin, True)

    async def _handle_command(self, msg: dict):
        cid = str(msg.get("command_id") or "")
        if not cid:
            return
        self.metrics["commands"] += 1
        cmd = Command(command_id=cid, topic=str(msg.get("topic") or ""), payload=get_payload(msg),
                      expires_at=msg.get("expires_at"), target=msg.get("target"))
        detail = None
        if cmd.target:
            # for a peer we relay for: it de-duplicates itself
            status = "rejected" if self._on_command is None else None
        else:
            status = self.commands.check(cid, cmd.expires_at)
            if status == ACCEPT and self._on_command is None:
                status = "rejected"
        if status in (None, ACCEPT):
            try:
                result = await self._on_command(cmd)
                status = result or ACCEPT
            except Exception as e:
                log.exception("uplink: command handler failed")
                status, detail = "failed", f"{type(e).__name__}: {e}"[:300]
        with contextlib.suppress(Exception):
            await self._send({"t": UPLINK_CMD_ACK, "command_id": cid, "status": status,
                              **({"detail": detail} if detail else {})})

    # =================================================
    # Plumbing
    # =================================================

    async def _request(self, key: str, msg: dict, timeout: float | None):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        old = self._pending.pop(key, None)
        if old is not None and not old.done():
            old.set_exception(UplinkUnavailable("superseded"))
        self._pending[key] = fut
        if not await self._send(msg):
            self._pending.pop(key, None)
            raise UplinkUnavailable(ERR_UPLINK_UNAVAILABLE)
        if msg.get("t") == UPLINK_DATA:
            self.metrics["published"] += 1
        try:
            await asyncio.wait_for(asyncio.shield(fut), timeout or self.ack_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(key, None)
            raise UplinkUnavailable("no ack in time")

    async def _send(self, msg: dict) -> bool:
        if self._sock is None or self._session is None:
            return False
        try:
            async with self._send_lock:
                frame = self._session.encrypt(encode(msg))
                await self._sock.send_multipart([K_DATA, frame], flags=zmq.NOBLOCK)
            return True
        except (zmq.ZMQError, NSCError) as e:
            log.debug("uplink send failed: %s", e)
            return False

    def _close_socket(self):
        if self._sock is not None:
            self._sock.close(0)
            self._sock = None

    def _fail_pending(self, reason: str):
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(UplinkUnavailable(reason))

    async def _set_state(self, state: str, error: str | None):
        changed = state != self.state
        self.state = state
        if changed and self._on_state is not None:
            try:
                r = self._on_state(state, error)
                if asyncio.iscoroutine(r):
                    await r
            except Exception:
                log.exception("uplink on_state failed")


class _Refused(Exception):
    pass


def decode_error(data: bytes) -> str:
    try:
        return str(decode_raw(data).get("code") or "refused")
    except Exception:
        return "refused"


def decode_raw(data: bytes) -> dict:
    import json
    v = json.loads(data.decode())
    return v if isinstance(v, dict) else {}
