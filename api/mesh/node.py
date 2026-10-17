# node.py
"""
NIKA mesh node.

Every gateway runs one MeshNode. It plays two roles at once:

- Origin: when its own uplink is down, it finds a healthy neighbour on the LAN,
  opens a relay session, and sends its upstream traffic through it. Commands
  from the cloud come back the same way.
- Relay: it carries upstream traffic for neighbours whose uplink is down, and
  forwards cloud commands to them.

The node never talks to the cloud itself. The application (e.g. Stargate's MQTT
service) does that through the callbacks below. All sockets live on one
background thread; callbacks run on that thread, so hand work off to your own
event loop instead of blocking in them.
"""
import base64
import hashlib
import heapq
import itertools
import json
import logging
import queue
import socket
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable

import zmq

from ..envelope import envelope, NIKA_VERSION
from ..protocol import *
from .commands import CommandGuard, ACCEPT, DUPLICATE
from .config import MeshConfig
from .identity import Challenge, prove
from .security import seal, unseal

log = logging.getLogger("nika.mesh")

# ---- Origin states ----
# Uplink channels: a node may have several independent uplinks (e.g. two
# platform protocols). Callers that don't name one use DEFAULT_CHANNEL.
DEFAULT_CHANNEL = "default"

DIRECT = "direct"     # own uplink is fine
SEEKING = "seeking"   # uplink down, looking for a relay
RELAYED = "relayed"   # uplink down, traffic going through a relay

# ---- Delivery failure reasons reported to on_delivery ----
NO_RELAY = "NO_RELAY"
BACKPRESSURE = "BACKPRESSURE"
UNDELIVERED = "UNDELIVERED"
UPLINK_RESTORED = "UPLINK_RESTORED"
STOPPED = "STOPPED"

MAX_QUEUE = 10_000
UPLINK_REQUEST_TIMEOUT = 30.0
DOWNLINK_DEFAULT_TTL = 30.0
PUBLISHED_TTL = 300.0
PUBLISHED_MAX = 50_000
CHALLENGE_TTL = 10.0          # relay: an unanswered identity challenge is dropped after this
MIN_CHALLENGES = 16           # relay: outstanding challenges allowed (or 4 x relay_capacity)


class NoRelayAvailable(RuntimeError):
    pass


# =================================================
# Public value types
# =================================================

@dataclass
class UplinkRequest:
    """A neighbour's message this node should publish upstream on its behalf."""

    origin: str
    msg_id: str
    seq: int
    topic: str
    payload: Any
    qos: int
    priority: int
    ts: float
    signature: str | None
    channel: str = DEFAULT_CHANNEL
    _node: "MeshNode" = field(repr=False, default=None)

    def ack(self):
        """Call once the message is published (e.g. MQTT PUBACK received)."""
        self._node._post(("uplink_result", self.origin, self.msg_id, True, None))

    def fail(self, reason: str = ERR_RELAY_UPLINK_FAILED):
        self._node._post(("uplink_result", self.origin, self.msg_id, False, reason))


@dataclass
class Command:
    """A cloud command that reached this node through a relay."""

    command_id: str
    topic: str
    payload: Any
    expires_at: float | None
    relay_id: str


@dataclass
class _Peer:
    node_id: str
    endpoint: str
    uplink: bool
    relay: bool
    load: int
    capacity: int
    last_seen: float
    uplinks: dict = field(default_factory=dict)   # channel -> up

    def channel_up(self, channel: str) -> bool:
        if self.uplinks:
            return bool(self.uplinks.get(channel))
        return self.uplink and channel == DEFAULT_CHANNEL   # peer without channels


@dataclass
class _Session:
    origin: str
    last_seen: float
    tokens: float
    tokens_at: float


@dataclass
class _Outgoing:
    msg_id: str
    seq: int
    topic: str
    payload: Any
    qos: int
    priority: int
    ts: float
    signature: str | None
    channel: str = DEFAULT_CHANNEL
    queued_at: float = 0.0
    attempts: int = 0
    not_before: float = 0.0
    sent_at: float = 0.0


@dataclass
class _Pending:
    """Relay side: a RELAY_OPEN waiting for its identity proof."""
    challenge: Challenge
    deadline: float


@dataclass
class _Downlink:
    origin: str
    command_id: str
    frame: dict
    expires_at: float
    next_try: float


# =================================================
# MeshNode
# =================================================

class MeshNode:
    def __init__(self, config: MeshConfig, *, signer=None, identity=None):
        """`identity`: optional long-term key (an `api.secure.StaticKey`). Origins
        that have one prove it to relays; relays expose the verified key through
        session_identity(origin)."""
        self.cfg = config
        self.signer = signer
        self._identity = identity
        self.commands = CommandGuard(ttl=config.command_dedupe_ttl)

        # ---- Callbacks ----
        self._on_uplink_publish: Callable[[UplinkRequest], None] | None = None
        self._on_session: Callable[[str, bool], None] | None = None
        self._on_command_result: Callable[[str, str, str], None] | None = None
        self._on_command: Callable[[Command], None] | None = None
        self._on_delivery: Callable[[str, bool, str | None], None] | None = None
        self._on_state: Callable[[str, str, str | None], None] | None = None

        # ---- Shared state (read from any thread) ----
        self._lock = threading.Lock()
        self._peers: dict[str, _Peer] = {}
        self._sessions: dict[str, _Session] = {}
        self._state = DIRECT
        self._relay_id: str | None = None
        self._uplinks: dict[str, bool] = {DEFAULT_CHANNEL: True}
        self._default_explicit = False
        self._meta: dict = {}                       # sent to relays in RELAY_OPEN
        self._session_meta: dict[str, dict] = {}    # relay side: origin -> its meta
        self._session_identity: dict[str, bytes | None] = {}   # relay side: origin -> proven key
        self.metrics = {
            "beacons_sent": 0,
            "beacons_received": 0,
            "frames_rejected": 0,
            "relay_msgs_in": 0,
            "relay_msgs_published": 0,
            "relay_msgs_failed": 0,
            "origin_msgs_sent": 0,
            "origin_msgs_acked": 0,
            "origin_msgs_failed": 0,
            "commands_forwarded": 0,
            "commands_received": 0,
            "failovers": 0,
            "identity_proofs_ok": 0,
            "identity_proofs_failed": 0,
        }

        # ---- Loop-thread-only state ----
        self._inbox: queue.Queue = queue.Queue()
        self._running = False
        self._thread: threading.Thread | None = None
        self._seq = itertools.count(1)
        self._seen_frames: dict[str, float] = {}

        # origin side
        self._dealer: zmq.Socket | None = None
        self._opening: tuple[str, float] | None = None
        self._opening_meta: dict = {}               # meta as sent in the current RELAY_OPEN
        self._last_pong = 0.0
        self._last_ping = 0.0
        self._recover_at: float | None = None
        self._cooldown: dict[str, float] = {}
        self._outq: list[tuple[int, int, _Outgoing]] = []
        self._inflight: dict[str, _Outgoing] = {}

        # relay side
        self._pending_uplink: dict[tuple[str, str], tuple[float, str]] = {}   # -> (started, channel)
        self._published: OrderedDict[tuple[str, str], float] = OrderedDict()
        self._downlinks: dict[str, _Downlink] = {}
        self._challenges: dict[str, _Pending] = {}

        self._last_beacon = 0.0

    # =================================================
    # Callback registration
    # =================================================

    def on_uplink_publish(self, fn: Callable[[UplinkRequest], None]):
        """Relay: publish a neighbour's message upstream, then call req.ack() / req.fail()."""
        self._on_uplink_publish = fn

    def on_session(self, fn: Callable[[str, bool], None]):
        """Relay: a neighbour started (True) or stopped (False) relaying through us.
        Subscribe / unsubscribe its command topics here."""
        self._on_session = fn

    def on_command_result(self, fn: Callable[[str, str, str], None]):
        """Relay: (origin, command_id, status) for a forwarded command."""
        self._on_command_result = fn

    def on_command(self, fn: Callable[[Command], None]):
        """Origin: a cloud command arrived via the relay (already deduped and expiry-checked)."""
        self._on_command = fn

    def on_delivery(self, fn: Callable[[str, bool, str | None], None]):
        """Origin: (msg_id, ok, reason) for every message passed to send()."""
        self._on_delivery = fn

    def on_state(self, fn: Callable[[str, str, str | None], None]):
        """Origin: (old_state, new_state, relay_id)."""
        self._on_state = fn

    # =================================================
    # Public API (thread-safe)
    # =================================================

    @property
    def state(self) -> str:
        return self._state

    @property
    def relay_id(self) -> str | None:
        return self._relay_id

    def peers(self) -> list[dict]:
        now = time.monotonic()
        with self._lock:
            return [
                {
                    "node_id": p.node_id,
                    "endpoint": p.endpoint,
                    "uplink": p.uplink,
                    "uplinks": dict(p.uplinks) if p.uplinks else {DEFAULT_CHANNEL: p.uplink},
                    "relay": p.relay,
                    "load": p.load,
                    "capacity": p.capacity,
                    "age": round(now - p.last_seen, 2),
                }
                for p in self._peers.values()
                if now - p.last_seen <= self.cfg.peer_timeout
            ]

    def sessions(self) -> list[str]:
        with self._lock:
            return sorted(self._sessions)

    def set_session_meta(self, meta: dict):
        """Origin: details a relay needs to serve us (e.g. which command topics to
        proxy). Sent with every RELAY_OPEN; set it before the uplink goes down."""
        self._meta = dict(meta or {})

    def session_meta(self, origin: str) -> dict:
        """Relay: the meta an origin sent when it opened its session. Self-declared:
        don't treat anything in it as proof of who the origin is."""
        with self._lock:
            return dict(self._session_meta.get(origin) or {})

    def set_identity(self, identity):
        """Origin: the long-term key (an `api.secure.StaticKey`, or None) to prove to
        relays. Used from the next RELAY_OPEN on."""
        self._identity = identity

    def session_identity(self, origin: str) -> bytes | None:
        """Relay: the 32-byte public key the origin PROVED it holds when it opened its
        session, or None if it didn't prove one. This, not anything in
        session_meta(), is what may name the origin to a platform."""
        with self._lock:
            return self._session_identity.get(origin)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, name=f"nika-mesh-{self.cfg.node_id}",
                                        daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0):
        if not self._running:
            return
        self._post(("stop",))
        self._thread.join(timeout)

    def set_uplink(self, up: bool, channel: str = DEFAULT_CHANNEL):
        """Tell the node whether this gateway can reach the cloud by itself on
        `channel`. A node with several channels seeks a relay while any of them
        is down, and relays for others while any of them is up."""
        self._post(("uplink", channel, bool(up)))

    def uplinks(self) -> dict[str, bool]:
        return dict(self._uplinks)

    def send(self, topic: str, payload: Any, *, qos: int = 1, priority: int = 5,
             msg_id: str | None = None, channel: str = DEFAULT_CHANNEL) -> str:
        """
        Origin: queue a message for the relay. Lower priority number = sent first;
        priority 0 (alarms) also bypasses the relay's rate limit.
        Outcome arrives via on_delivery(msg_id, ok, reason).
        """
        if self._state != RELAYED:
            raise NoRelayAvailable(f"mesh state is {self._state}")
        msg_id = msg_id or f"{self.cfg.node_id}:{uuid.uuid4()}"
        self._post(("send", msg_id, topic, payload, int(qos), int(priority), channel))
        return msg_id

    def forward_command(self, origin: str, topic: str, payload: Any, *, command_id: str,
                        expires_at: float | None = None) -> bool:
        """
        Relay: pass a cloud command to a neighbour we are relaying for.
        Returns False if there is no session for that neighbour.
        Outcome arrives via on_command_result(origin, command_id, status).
        """
        with self._lock:
            if origin not in self._sessions:
                return False
        self._post(("forward", origin, topic, payload, command_id, expires_at))
        return True

    def revoke(self, origin: str, reason: str = ERR_RELAY_NOT_ALLOWED):
        """Relay: end a neighbour's session (e.g. our broker refuses its topics).
        The origin fails over and won't pick us again for relay_cooldown."""
        self._post(("revoke", origin, reason))

    # =================================================
    # Thread plumbing
    # =================================================

    def _post(self, item):
        self._inbox.put(item)
        try:
            self._wake_w.send(b"\0")
        except (BlockingIOError, OSError, AttributeError):
            pass

    def _run(self):
        self._ctx = zmq.Context()
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.LINGER, 0)
        self._router.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self._router.setsockopt(zmq.ROUTER_HANDOVER, 1)
        self._router.bind(f"tcp://{self.cfg.bind_host}:{self.cfg.mesh_port}")

        self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            self._udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._udp.bind((self.cfg.bind_host, self.cfg.discovery_port))
        self._udp.setblocking(False)

        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)

        self._poller = zmq.Poller()
        self._poller.register(self._router, zmq.POLLIN)
        self._poller.register(self._udp.fileno(), zmq.POLLIN)
        self._poller.register(self._wake_r.fileno(), zmq.POLLIN)

        log.info("mesh node %s up (tcp %s, udp %s)", self.cfg.node_id,
                 self.cfg.mesh_port, self.cfg.discovery_port)

        try:
            while self._running:
                events = dict(self._poller.poll(timeout=100))

                if self._wake_r.fileno() in events:
                    try:
                        while self._wake_r.recv(4096):
                            pass
                    except BlockingIOError:
                        pass
                self._drain_inbox()

                if self._router in events:
                    self._recv_router()
                if self._dealer is not None and self._dealer in events:
                    self._recv_dealer()
                if self._udp.fileno() in events:
                    self._recv_udp()

                self._tick()
        except Exception:
            log.exception("mesh loop crashed")
        finally:
            self._shutdown()

    def _shutdown(self):
        # tell everyone we're leaving
        if self._state == RELAYED:
            self._dealer_send(RELAY_CLOSE, {})
        for origin in list(self._sessions):
            self._router_send(origin, RELAY_REVOKE, {"reason": "SHUTDOWN"})
            self._end_session(origin)
        for item in list(self._inflight.values()) + [o for _, _, o in self._outq]:
            self._deliver(item.msg_id, False, STOPPED)
        self._inflight.clear()
        self._outq.clear()

        self._close_dealer(linger=200)  # let RELAY_CLOSE get out
        self._router.close(200)
        self._udp.close()
        self._wake_r.close()
        self._wake_w.close()
        self._ctx.term()
        self._running = False
        log.info("mesh node %s stopped", self.cfg.node_id)

    def _drain_inbox(self):
        while True:
            try:
                item = self._inbox.get_nowait()
            except queue.Empty:
                return

            kind = item[0]
            if kind == "stop":
                self._running = False
            elif kind == "uplink":
                self._handle_uplink_change(item[1], item[2])
            elif kind == "send":
                self._enqueue(*item[1:])
            elif kind == "forward":
                self._start_downlink(*item[1:])
            elif kind == "uplink_result":
                self._finish_uplink(*item[1:])
            elif kind == "revoke":
                _, origin, reason = item
                self._challenges.pop(origin, None)
                if origin in self._sessions:
                    self._router_send(origin, RELAY_REVOKE, {"reason": reason})
                    self._end_session(origin)

    # =================================================
    # Framing
    # =================================================

    def _seal(self, msg_type: str, payload: dict) -> bytes:
        frame = envelope(msg_type=msg_type, payload=payload, sender=self.cfg.node_id)
        frame["t"] = time.time()
        return seal(frame, self.cfg.mesh_key)

    def _open(self, raw: bytes) -> dict | None:
        try:
            frame = unseal(raw, self.cfg.mesh_key, self.cfg.max_clock_skew)
        except ValueError as e:
            self.metrics["frames_rejected"] += 1
            log.debug("rejected frame: %s", e)
            return None

        if frame.get("nika") != NIKA_VERSION:
            self.metrics["frames_rejected"] += 1
            return None

        sender = frame.get("sender")
        if self.cfg.allowed_peers is not None and sender not in self.cfg.allowed_peers:
            self.metrics["frames_rejected"] += 1
            return None

        # replay protection inside the clock-skew window
        fid = frame.get("id")
        if fid in self._seen_frames:
            self.metrics["frames_rejected"] += 1
            return None
        self._seen_frames[fid] = time.monotonic() + 2 * self.cfg.max_clock_skew

        return frame

    def _router_send(self, origin: str, msg_type: str, payload: dict) -> bool:
        try:
            self._router.send_multipart([origin.encode(), self._seal(msg_type, payload)],
                                        zmq.NOBLOCK)
            return True
        except zmq.ZMQError as e:
            log.debug("router send to %s failed: %s", origin, e)
            return False

    def _dealer_send(self, msg_type: str, payload: dict) -> bool:
        if self._dealer is None:
            return False
        try:
            self._dealer.send(self._seal(msg_type, payload), zmq.NOBLOCK)
            return True
        except zmq.ZMQError as e:
            log.debug("dealer send failed: %s", e)
            return False

    # =================================================
    # Discovery
    # =================================================

    def _send_beacon(self):
        raw = self._seal(MESH_BEACON, {
            "port": self.cfg.mesh_port,
            "host": self.cfg.advertise_host,
            "uplink": self._any_up(),
            "uplinks": dict(self._uplinks),
            "relay": self.cfg.relay_enabled,
            "load": len(self._sessions),
            "capacity": self.cfg.relay_capacity,
        })
        targets = [(a, self.cfg.discovery_port) for a in self.cfg.broadcast_addrs]
        for peer in self.cfg.static_peers:
            host, _, port = peer.rpartition(":")
            targets.append((host, int(port)))

        for addr in targets:
            try:
                self._udp.sendto(raw, addr)
            except OSError as e:
                log.debug("beacon to %s failed: %s", addr, e)
        self.metrics["beacons_sent"] += 1

    def _recv_udp(self):
        while True:
            try:
                raw, (src_ip, _) = self._udp.recvfrom(65535)
            except (BlockingIOError, OSError):
                return

            frame = self._open(raw)
            if not frame or frame.get("type") != MESH_BEACON:
                continue
            sender = frame["sender"]
            if sender == self.cfg.node_id:
                continue

            p = frame.get("payload") or {}
            peer = _Peer(
                node_id=sender,
                endpoint=f"tcp://{p.get('host') or src_ip}:{int(p.get('port', 0))}",
                uplink=bool(p.get("uplink")),
                uplinks={str(k): bool(v) for k, v in (p.get("uplinks") or {}).items()}
                if isinstance(p.get("uplinks"), dict) else {},
                relay=bool(p.get("relay")),
                load=int(p.get("load", 0)),
                capacity=int(p.get("capacity", 0)),
                last_seen=time.monotonic(),
            )
            with self._lock:
                is_new = sender not in self._peers
                self._peers[sender] = peer
            if is_new:
                log.info("peer %s discovered at %s", sender, peer.endpoint)
            self.metrics["beacons_received"] += 1

    def _alive_peer(self, node_id: str) -> _Peer | None:
        with self._lock:
            p = self._peers.get(node_id)
        if p and time.monotonic() - p.last_seen <= self.cfg.peer_timeout:
            return p
        return None

    # =================================================
    # Timers
    # =================================================

    def _tick(self):
        now = time.monotonic()

        if now - self._last_beacon >= self.cfg.beacon_interval:
            self._send_beacon()
            self._last_beacon = now

        for fid, exp in list(self._seen_frames.items()):
            if exp < now:
                del self._seen_frames[fid]

        self._tick_origin(now)
        self._tick_relay(now)

    # =================================================
    # ORIGIN SIDE
    # =================================================

    def _all_up(self) -> bool:
        """Origin view: can we reach the cloud on every channel ourselves?"""
        return all(self._uplinks.values())

    def _any_up(self) -> bool:
        """Relay view: can we carry anything for others?"""
        return any(self._uplinks.values())

    def _needed(self) -> list[str]:
        """Channels we currently need a relay for. While recovering (everything
        back up, waiting out recover_hold) that's all our channels: the relay
        keeps carrying until we close the session."""
        down = [ch for ch, up in self._uplinks.items() if not up]
        return down or list(self._uplinks) or [DEFAULT_CHANNEL]

    def _set_state(self, new: str, relay_id: str | None = None):
        old = self._state
        self._state = new
        self._relay_id = relay_id
        if old != new:
            log.info("%s: %s -> %s%s", self.cfg.node_id, old, new,
                     f" via {relay_id}" if relay_id else "")
            self._call(self._on_state, old, new, relay_id)

    def _handle_uplink_change(self, channel: str, up: bool):
        # A node that names its own channels never meant the implicit default one.
        if channel != DEFAULT_CHANNEL and not self._default_explicit:
            self._uplinks.pop(DEFAULT_CHANNEL, None)
        if channel == DEFAULT_CHANNEL:
            self._default_explicit = True

        was_all, was_any = self._all_up(), self._any_up()
        if self._uplinks.get(channel) == up:
            return
        self._uplinks[channel] = up
        now_all, now_any = self._all_up(), self._any_up()

        if now_all and not was_all:
            if self._state == SEEKING:
                self._abort_opening()
                self._set_state(DIRECT)
                self._fail_all(UPLINK_RESTORED)
            elif self._state == RELAYED:
                # don't flap: wait for the uplink to prove itself
                self._recover_at = time.monotonic() + self.cfg.recover_hold
            # stop carrying others? no — a healthy uplink is exactly what relays need
        elif not now_all:
            self._recover_at = None
            if self._state == DIRECT:
                self._set_state(SEEKING)

        if was_any and not now_any:
            # a relay with no uplink at all is useless to its origins
            for origin in list(self._sessions):
                self._router_send(origin, RELAY_REVOKE, {"reason": ERR_RELAY_NO_UPLINK})
                self._end_session(origin)

    def _tick_origin(self, now: float):
        if self._state == SEEKING:
            if self._opening:
                peer_id, deadline = self._opening
                if now > deadline:
                    log.warning("%s: relay %s did not answer RELAY_OPEN",
                                self.cfg.node_id, peer_id)
                    self._cooldown[peer_id] = now + self.cfg.relay_cooldown
                    self._abort_opening()
            if not self._opening:
                self._try_open(now)
            self._expire_queued(now)
            return

        if self._state != RELAYED:
            return

        # recovered?
        if self._recover_at and now >= self._recover_at and self._all_up():
            self._dealer_send(RELAY_CLOSE, {})
            self._close_dealer()
            self._recover_at = None
            self._set_state(DIRECT)
            self._fail_all(UPLINK_RESTORED)
            return

        # relay still healthy?
        peer = self._alive_peer(self._relay_id)
        reason = None
        if now - self._last_pong > self.cfg.relay_timeout:
            reason = "keepalive timeout"
        elif peer is None:
            reason = "beacons stopped"
        elif not any(peer.channel_up(ch) for ch in self._needed()):
            reason = "relay lost its uplink"
        if reason:
            self._failover(reason, cooldown=True)
            return

        if now - self._last_ping >= self.cfg.keepalive_interval:
            self._dealer_send(RELAY_PING, {})
            self._last_ping = now

        self._pump(now)

    def _pick_relay(self, now: float) -> _Peer | None:
        with self._lock:
            peers = list(self._peers.values())

        needed = self._needed()
        candidates = [
            p for p in peers
            if now - p.last_seen <= self.cfg.peer_timeout
            and p.relay and p.load < p.capacity
            and any(p.channel_up(ch) for ch in needed)
        ]
        if not candidates:
            return None
        full = [p for p in candidates if all(p.channel_up(ch) for ch in needed)]
        candidates = full or candidates

        # A relay that failed or refused us waits out relay_cooldown. Retrying it
        # early would hot-loop against one that keeps revoking (e.g. its broker
        # refuses our topics).
        pool = [p for p in candidates if self._cooldown.get(p.node_id, 0) <= now]
        if not pool:
            return None

        # operator-chosen relays win, in the order given
        by_id = {p.node_id: p for p in pool}
        for pid in self.cfg.preferred_relays:
            if pid in by_id:
                return by_id[pid]

        # rendezvous hashing: each origin prefers a different relay, so ten
        # stranded gateways spread out instead of piling onto one neighbour
        def score(p: _Peer) -> int:
            h = hashlib.sha256(f"{self.cfg.node_id}|{p.node_id}".encode()).digest()
            return int.from_bytes(h[:8], "big")

        return max(pool, key=score)

    def _try_open(self, now: float):
        peer = self._pick_relay(now)
        if not peer:
            return

        self._close_dealer()
        self._dealer = self._ctx.socket(zmq.DEALER)
        self._dealer.setsockopt(zmq.LINGER, 0)
        self._dealer.setsockopt_string(zmq.IDENTITY, self.cfg.node_id)
        self._dealer.connect(peer.endpoint)
        self._poller.register(self._dealer, zmq.POLLIN)

        self._opening = (peer.node_id, now + self.cfg.open_timeout)
        # the relay sees the meta after a JSON round trip; prove over that same form
        self._opening_meta = json.loads(json.dumps(self._meta))
        payload = {"meta": self._opening_meta}
        if self._identity is not None:
            payload["id"] = self._identity.public_b64       # proved on RELAY_CHALLENGE
        self._dealer_send(RELAY_OPEN, payload)
        log.info("%s: asking %s to relay", self.cfg.node_id, peer.node_id)

    def _abort_opening(self):
        self._opening = None
        self._close_dealer()

    def _close_dealer(self, linger: int = 0):
        if self._dealer is not None:
            try:
                self._poller.unregister(self._dealer)
            except KeyError:
                pass
            self._dealer.close(linger)
            self._dealer = None

    def _failover(self, reason: str, *, cooldown: bool):
        log.warning("%s: leaving relay %s (%s)", self.cfg.node_id, self._relay_id, reason)
        if cooldown and self._relay_id:
            self._cooldown[self._relay_id] = time.monotonic() + self.cfg.relay_cooldown
        self._close_dealer()
        self.metrics["failovers"] += 1

        # whatever was in flight goes back in the queue for the next relay
        for item in self._inflight.values():
            item.sent_at = 0.0
            heapq.heappush(self._outq, (item.priority, item.seq, item))
        self._inflight.clear()

        if self._all_up():
            self._set_state(DIRECT)
            self._fail_all(UPLINK_RESTORED)
        else:
            self._set_state(SEEKING)

    def _recv_dealer(self):
        while self._dealer is not None:
            try:
                raw = self._dealer.recv(zmq.NOBLOCK)
            except zmq.Again:
                return

            frame = self._open(raw)
            if not frame:
                continue

            expected = self._relay_id or (self._opening[0] if self._opening else None)
            if frame.get("sender") != expected:
                self.metrics["frames_rejected"] += 1
                continue

            t = frame.get("type")
            p = frame.get("payload") or {}

            if t == RELAY_OPEN_ACK:
                self._handle_open_ack(p)
            elif t == RELAY_CHALLENGE:
                self._handle_challenge(p)
            elif t == RELAY_PONG:
                self._last_pong = time.monotonic()
            elif t == RELAY_DATA_ACK:
                self._handle_data_ack(p)
            elif t == RELAY_DOWNLINK:
                self._handle_downlink(p)
            elif t == RELAY_REVOKE:
                if self._state == RELAYED:
                    # NO_SESSION usually means the relay restarted; it's worth
                    # asking it again straight away
                    self._failover(f"revoked: {p.get('reason')}",
                                   cooldown=p.get("reason") != ERR_RELAY_NO_SESSION)

    def _handle_challenge(self, p: dict):
        if not self._opening or self._state != SEEKING or self._identity is None:
            return
        mac = prove(self._identity, relay_id=self._opening[0], origin_id=self.cfg.node_id,
                    challenge=p, meta=self._opening_meta)
        if mac is not None:
            self._dealer_send(RELAY_PROVE, {"mac": mac})

    def _handle_open_ack(self, p: dict):
        if not self._opening or self._state != SEEKING:
            return
        peer_id, _ = self._opening
        self._opening = None

        if not p.get("accepted"):
            log.warning("%s: %s refused to relay (%s)", self.cfg.node_id, peer_id,
                        p.get("reason"))
            self._cooldown[peer_id] = time.monotonic() + self.cfg.relay_cooldown
            self._close_dealer()
            return

        now = time.monotonic()
        self._last_pong = now
        self._last_ping = now
        self._set_state(RELAYED, peer_id)
        self._pump(now)

    # ---- outgoing data ----

    def _enqueue(self, msg_id, topic, payload, qos, priority, channel=DEFAULT_CHANNEL):
        if self._state != RELAYED and self._all_up():
            self._deliver(msg_id, False, NO_RELAY)
            return
        if len(self._outq) + len(self._inflight) >= MAX_QUEUE:
            self._deliver(msg_id, False, BACKPRESSURE)
            return

        seq = next(self._seq)
        ts = time.time()
        sig = None
        if self.signer is not None:
            sig = self.signer.sign(self.cfg.node_id, msg_id, topic, payload, ts)

        item = _Outgoing(msg_id, seq, topic, payload, qos, priority, ts, sig, channel,
                         queued_at=time.monotonic())
        heapq.heappush(self._outq, (priority, seq, item))
        if self._state == RELAYED:
            self._pump(time.monotonic())

    def _pump(self, now: float):
        # retry timed-out messages
        for msg_id, item in list(self._inflight.items()):
            if now - item.sent_at > self.cfg.ack_timeout:
                del self._inflight[msg_id]
                if item.attempts >= self.cfg.max_attempts:
                    self._deliver(msg_id, False, UNDELIVERED)
                else:
                    heapq.heappush(self._outq, (item.priority, item.seq, item))

        deferred = []
        while self._outq and len(self._inflight) < self.cfg.max_inflight:
            prio, seq, item = heapq.heappop(self._outq)
            if item.not_before > now:
                deferred.append((prio, seq, item))
                continue
            item.attempts += 1
            item.sent_at = now
            self._inflight[item.msg_id] = item
            self._dealer_send(RELAY_DATA, {
                "msg_id": item.msg_id,
                "seq": item.seq,
                "topic": item.topic,
                "payload": item.payload,
                "qos": item.qos,
                "priority": item.priority,
                "ch": item.channel,
                "ts": item.ts,
                "sig": item.signature,
            })
            self.metrics["origin_msgs_sent"] += 1
        for d in deferred:
            heapq.heappush(self._outq, d)

    def _handle_data_ack(self, p: dict):
        msg_id = p.get("msg_id")
        item = self._inflight.pop(msg_id, None)
        if item is None:
            return

        if p.get("ok"):
            self.metrics["origin_msgs_acked"] += 1
            self._deliver(msg_id, True, None)
            return

        reason = p.get("reason")
        if reason == ERR_RELAY_RATE_LIMITED:
            item.attempts -= 1  # not the message's fault
            item.not_before = time.monotonic() + 0.5
            heapq.heappush(self._outq, (item.priority, item.seq, item))
        elif reason == ERR_RELAY_NO_UPLINK and p.get("ch") and self._relay_has_any_needed():
            # the relay can't carry THIS channel right now; the rest still works
            self._deliver(msg_id, False, ERR_RELAY_NO_UPLINK)
        elif reason in (ERR_RELAY_NO_SESSION, ERR_RELAY_NO_UPLINK):
            item.attempts -= 1
            heapq.heappush(self._outq, (item.priority, item.seq, item))
            if self._state == RELAYED:
                self._failover(f"data rejected: {reason}",
                               cooldown=reason == ERR_RELAY_NO_UPLINK)
        elif item.attempts >= self.cfg.max_attempts:
            self._deliver(msg_id, False, reason or UNDELIVERED)
        else:
            heapq.heappush(self._outq, (item.priority, item.seq, item))

    def _relay_has_any_needed(self) -> bool:
        peer = self._alive_peer(self._relay_id) if self._relay_id else None
        return bool(peer) and any(peer.channel_up(ch) for ch in self._needed())

    def _expire_queued(self, now: float):
        """While no relay is available, don't hold messages longer than queue_timeout."""
        keep = []
        for entry in self._outq:
            if now - entry[2].queued_at > self.cfg.queue_timeout:
                self._deliver(entry[2].msg_id, False, NO_RELAY)
            else:
                keep.append(entry)
        if len(keep) != len(self._outq):
            heapq.heapify(keep)
            self._outq = keep

    def _fail_all(self, reason: str):
        """Hand every queued message back to the app (it will publish direct / keep in outbox)."""
        for item in list(self._inflight.values()) + [o for _, _, o in self._outq]:
            self._deliver(item.msg_id, False, reason)
        self._inflight.clear()
        self._outq.clear()

    def _deliver(self, msg_id: str, ok: bool, reason: str | None):
        if not ok:
            self.metrics["origin_msgs_failed"] += 1
        self._call(self._on_delivery, msg_id, ok, reason)

    # ---- incoming commands ----

    def _handle_downlink(self, p: dict):
        cid = p.get("command_id")
        if not cid:
            return

        if self._on_command is None:
            status = "rejected"
        else:
            status = self.commands.check(cid, p.get("expires_at"))
            if status == ACCEPT:
                self.metrics["commands_received"] += 1
                self._call(self._on_command, Command(
                    command_id=cid,
                    topic=p.get("topic"),
                    payload=p.get("payload"),
                    expires_at=p.get("expires_at"),
                    relay_id=self._relay_id,
                ))

        self._dealer_send(RELAY_DOWNLINK_ACK, {"command_id": cid, "status": status})

    # =================================================
    # RELAY SIDE
    # =================================================

    def _recv_router(self):
        while True:
            try:
                ident, raw = self._router.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            except ValueError:
                continue

            frame = self._open(raw)
            if not frame:
                continue

            origin = ident.decode(errors="replace")
            if frame.get("sender") != origin:
                self.metrics["frames_rejected"] += 1
                continue

            t = frame.get("type")
            p = frame.get("payload") or {}

            if t == RELAY_OPEN:
                meta = p.get("meta")
                self._handle_open(origin, meta if isinstance(meta, dict) else {}, p.get("id"))
            elif t == RELAY_PROVE:
                self._handle_prove(origin, p.get("mac"))
            elif t == RELAY_PING:
                if self._touch(origin):
                    self._router_send(origin, RELAY_PONG, {"uplink": self._any_up()})
                else:
                    self._router_send(origin, RELAY_REVOKE, {"reason": ERR_RELAY_NO_SESSION})
            elif t == RELAY_DATA:
                self._handle_data(origin, p)
            elif t == RELAY_CLOSE:
                log.info("%s: %s is back on its own uplink", self.cfg.node_id, origin)
                self._end_session(origin)
            elif t == RELAY_DOWNLINK_ACK:
                self._handle_downlink_ack(origin, p)

    def _open_refusal(self, origin: str) -> str | None:
        if not self.cfg.relay_enabled:
            return ERR_RELAY_NOT_ALLOWED
        if not self._any_up():
            return ERR_RELAY_NO_UPLINK
        if origin not in self._sessions and len(self._sessions) >= self.cfg.relay_capacity:
            return ERR_RELAY_CAPACITY
        return None

    def _refuse_open(self, origin: str, reason: str):
        self._router_send(origin, RELAY_OPEN_ACK, {"accepted": False, "reason": reason})

    def _handle_open(self, origin: str, meta: dict, claim=None):
        self._challenges.pop(origin, None)          # a new RELAY_OPEN replaces an old attempt
        reason = self._open_refusal(origin)
        if reason:
            return self._refuse_open(origin, reason)

        if claim is None:
            if self.cfg.require_identity:
                return self._refuse_open(origin, ERR_RELAY_IDENTITY_REQUIRED)
            return self._accept_session(origin, meta, None)

        # the origin claims a long-term key: make it prove it before accepting
        try:
            if not isinstance(claim, str):
                raise ValueError
            identity = base64.urlsafe_b64decode(claim + "=" * (-len(claim) % 4))
            challenge = Challenge(relay_id=self.cfg.node_id, origin_id=origin,
                                  identity=identity, meta=meta)
        except Exception:
            self.metrics["identity_proofs_failed"] += 1
            return self._refuse_open(origin, ERR_RELAY_IDENTITY)
        if len(self._challenges) >= max(MIN_CHALLENGES, 4 * self.cfg.relay_capacity):
            return self._refuse_open(origin, ERR_RELAY_CAPACITY)
        self._challenges[origin] = _Pending(challenge, time.monotonic() + CHALLENGE_TTL)
        self._router_send(origin, RELAY_CHALLENGE, challenge.payload())

    def _handle_prove(self, origin: str, mac):
        pending = self._challenges.pop(origin, None)
        if pending is None or time.monotonic() > pending.deadline:
            return
        challenge = pending.challenge
        if not challenge.verify(mac):
            self.metrics["identity_proofs_failed"] += 1
            log.warning("%s: %s failed to prove its identity key", self.cfg.node_id, origin)
            return self._refuse_open(origin, ERR_RELAY_IDENTITY)
        self.metrics["identity_proofs_ok"] += 1
        reason = self._open_refusal(origin)         # things may have changed meanwhile
        if reason:
            return self._refuse_open(origin, reason)
        self._accept_session(origin, challenge.meta, challenge.identity)

    def _accept_session(self, origin: str, meta: dict, identity: bytes | None):
        # A session whose proven identity changes is a different session to the
        # app (it may have mapped the old key to this origin): end it first.
        if origin in self._sessions and self._session_identity.get(origin) != identity:
            log.warning("%s: %s re-opened with a different identity", self.cfg.node_id, origin)
            self._end_session(origin)

        now = time.monotonic()
        with self._lock:
            is_new = origin not in self._sessions
            self._sessions[origin] = _Session(origin, now, self.cfg.relay_rate_limit, now)
            self._session_meta[origin] = meta
            self._session_identity[origin] = identity

        self._router_send(origin, RELAY_OPEN_ACK, {"accepted": True})
        if is_new:
            log.info("%s: now relaying for %s%s", self.cfg.node_id, origin,
                     " (identity proven)" if identity else "")
            self._call(self._on_session, origin, True)

    def _touch(self, origin: str) -> bool:
        s = self._sessions.get(origin)
        if s:
            s.last_seen = time.monotonic()
        return s is not None

    def _end_session(self, origin: str):
        with self._lock:
            existed = self._sessions.pop(origin, None) is not None
        for cid, d in list(self._downlinks.items()):
            if d.origin == origin:
                del self._downlinks[cid]
                self._call(self._on_command_result, origin, cid, "no_session")
        if existed:
            self._call(self._on_session, origin, False)  # meta + identity still readable here
        with self._lock:
            self._session_meta.pop(origin, None)
            self._session_identity.pop(origin, None)

    def _handle_data(self, origin: str, p: dict):
        msg_id = p.get("msg_id")
        if not msg_id or not p.get("topic"):
            return

        channel = str(p.get("ch") or DEFAULT_CHANNEL)

        def nack(reason):
            self._router_send(origin, RELAY_DATA_ACK,
                              {"msg_id": msg_id, "ok": False, "reason": reason, "ch": channel})

        if not self._touch(origin):
            return nack(ERR_RELAY_NO_SESSION)
        if not self._uplinks.get(channel):
            return nack(ERR_RELAY_NO_UPLINK)

        key = (origin, msg_id)
        if key in self._published:
            # our earlier ACK was lost; don't publish twice
            self._router_send(origin, RELAY_DATA_ACK, {"msg_id": msg_id, "ok": True})
            return
        if key in self._pending_uplink:
            return  # still publishing; the ACK will follow

        priority = int(p.get("priority", 5))
        if priority > 0 and not self._take_token(self._sessions[origin]):
            return nack(ERR_RELAY_RATE_LIMITED)

        if self._on_uplink_publish is None:
            return nack(ERR_RELAY_UPLINK_FAILED)

        self.metrics["relay_msgs_in"] += 1
        self._pending_uplink[key] = (time.monotonic(), channel)
        self._call(self._on_uplink_publish, UplinkRequest(
            origin=origin,
            msg_id=msg_id,
            seq=int(p.get("seq", 0)),
            topic=p["topic"],
            payload=p.get("payload"),
            qos=int(p.get("qos", 1)),
            priority=priority,
            ts=float(p.get("ts", 0)),
            signature=p.get("sig"),
            channel=channel,
            _node=self,
        ))

    def _take_token(self, s: _Session) -> bool:
        now = time.monotonic()
        rate = self.cfg.relay_rate_limit
        s.tokens = min(rate, s.tokens + (now - s.tokens_at) * rate)
        s.tokens_at = now
        if s.tokens >= 1:
            s.tokens -= 1
            return True
        return False

    def _finish_uplink(self, origin: str, msg_id: str, ok: bool, reason: str | None):
        key = (origin, msg_id)
        pending = self._pending_uplink.pop(key, None)
        if pending is None:
            return
        channel = pending[1]

        if ok:
            self.metrics["relay_msgs_published"] += 1
            self._published[key] = time.monotonic()
            if len(self._published) > PUBLISHED_MAX:
                self._published.popitem(last=False)
            self._router_send(origin, RELAY_DATA_ACK, {"msg_id": msg_id, "ok": True})
        else:
            self.metrics["relay_msgs_failed"] += 1
            # carry the channel: "this channel failed", not "this relay is gone"
            self._router_send(origin, RELAY_DATA_ACK, {
                "msg_id": msg_id, "ok": False, "reason": reason or ERR_RELAY_UPLINK_FAILED, "ch": channel,
            })

    # ---- commands to origins ----

    def _start_downlink(self, origin, topic, payload, command_id, expires_at):
        if origin not in self._sessions:
            self._call(self._on_command_result, origin, command_id, "no_session")
            return

        wall_expiry = expires_at or (time.time() + DOWNLINK_DEFAULT_TTL)
        d = _Downlink(
            origin=origin,
            command_id=command_id,
            frame={
                "command_id": command_id,
                "topic": topic,
                "payload": payload,
                "expires_at": expires_at,
            },
            expires_at=time.monotonic() + max(0.0, wall_expiry - time.time()),
            next_try=0.0,
        )
        self._downlinks[command_id] = d
        self.metrics["commands_forwarded"] += 1
        self._send_downlink(d, time.monotonic())

    def _send_downlink(self, d: _Downlink, now: float):
        d.next_try = now + self.cfg.downlink_retry
        self._router_send(d.origin, RELAY_DOWNLINK, d.frame)

    def _handle_downlink_ack(self, origin: str, p: dict):
        self._touch(origin)
        cid = p.get("command_id")
        d = self._downlinks.get(cid)
        if d is None or d.origin != origin:
            return
        del self._downlinks[cid]
        status = p.get("status")
        if status == ACCEPT:
            status = "delivered"
        # "duplicate": the origin already ran this command_id; it was not run again
        self._call(self._on_command_result, origin, cid, status)

    def _tick_relay(self, now: float):
        for origin, pending in list(self._challenges.items()):
            if now > pending.deadline:
                del self._challenges[origin]

        for origin, s in list(self._sessions.items()):
            if now - s.last_seen > self.cfg.session_timeout:
                log.warning("%s: session with %s timed out", self.cfg.node_id, origin)
                self._end_session(origin)

        for key, (started, _ch) in list(self._pending_uplink.items()):
            if now - started > UPLINK_REQUEST_TIMEOUT:
                del self._pending_uplink[key]  # the origin will retry

        for cid, d in list(self._downlinks.items()):
            if now > d.expires_at:
                del self._downlinks[cid]
                self._call(self._on_command_result, d.origin, cid, "undelivered")
            elif now >= d.next_try:
                self._send_downlink(d, now)

        while self._published:
            key, ts = next(iter(self._published.items()))
            if now - ts <= PUBLISHED_TTL:
                break
            self._published.popitem(last=False)

    # =================================================
    # Helpers
    # =================================================

    def _call(self, fn, *args):
        if fn is None:
            return
        try:
            fn(*args)
        except Exception:
            log.exception("mesh callback %s failed", getattr(fn, "__name__", fn))
