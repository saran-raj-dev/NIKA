# files.py
"""
NIKA uplink file transfer: send files of any size (GBs) over the uplink
session, in either direction, with resume.

    sender ── FILE_OFFER {fid, name, size, sha256, meta} ──▶ receiver
           ◀─ FILE_ACCEPT {fid, ok, offset | reason} ──────  (offset > 0: resume)
           ── FILE_CHUNK {fid, off, d} × N ─────────────▶   appended to <path>.part
           ◀─ FILE_ACK {fid, off} ──────────────────────    bytes held so far
           ◀─ FILE_DONE {fid, ok | reason} ─────────────    sha256 checked, renamed
    either ── FILE_CANCEL {fid, reason} ─────────────────▶

Chunks are 256 KiB with a window of 8 in flight, so a slow receiver slows the
sender down instead of filling memory. If the session drops, the sender offers
the same fid again once it is back and the receiver answers with the bytes it
already holds, so the transfer continues where it stopped (also across a
receiver restart: the .part file is the state). A transfer is complete only
when the receiver has checked the SHA-256 of the whole file.

The engine is transport-agnostic: NikaUplinkClient and NikaUplinkServer each
own one and feed it the FILE_* messages of their sessions.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import logging
import os
import re
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from ..protocol import (
    UPLINK_FILE_ACCEPT, UPLINK_FILE_ACK, UPLINK_FILE_CANCEL, UPLINK_FILE_CHUNK, UPLINK_FILE_DONE,
    UPLINK_FILE_OFFER,
)
from .common import UplinkError, UplinkUnavailable

log = logging.getLogger("nika.uplink.files")

FILE_CHUNK = 256 * 1024          # raw bytes per chunk (≈ 350 KB base64, well under a frame)
FILE_WINDOW = 8                  # chunks in flight
FILE_MESSAGES = frozenset({UPLINK_FILE_OFFER, UPLINK_FILE_ACCEPT, UPLINK_FILE_CHUNK,
                           UPLINK_FILE_ACK, UPLINK_FILE_DONE, UPLINK_FILE_CANCEL})
INCOMING_TTL_S = 24 * 3600       # an interrupted incoming transfer is kept this long for resume
COMPLETED_KEEP = 1000            # finished transfers remembered, so a repeated offer is not re-sent

ERR_FILE_REJECTED = "FILE_REJECTED"
ERR_FILE_NO_HANDLER = "FILE_NO_HANDLER"
ERR_FILE_BAD_OFFER = "FILE_BAD_OFFER"
ERR_FILE_CHECKSUM = "FILE_CHECKSUM_MISMATCH"
ERR_FILE_WRITE = "FILE_WRITE_FAILED"
ERR_FILE_CANCELLED = "FILE_CANCELLED"
ERR_FILE_UNKNOWN = "FILE_UNKNOWN_TRANSFER"

_FID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,96}$")

Send = Callable[[dict], Awaitable[bool]]


class FileError(UplinkError):
    """The receiver refused or failed the transfer (reason: an ERR_FILE_* code or app text)."""


@dataclass
class FileOffer:
    """What a receiver is asked to accept."""
    fid: str
    name: str
    size: int
    sha256: str
    meta: dict[str, Any]
    peer_id: str | None          # the other side (server side: the peer; client side: None)


@dataclass
class ReceivedFile:
    """A complete, checksum-verified file on the receiver's disk."""
    fid: str
    name: str
    size: int
    sha256: str
    meta: dict[str, Any]
    peer_id: str | None
    path: str


@dataclass
class FileResult:
    fid: str
    size: int
    sha256: str
    resumed_from: int            # bytes the receiver already had on the last (re)offer
    attempts: int
    seconds: float


# Receiver app callbacks:
#   accept(offer) -> path to store the file at | None (refuse) | "REASON" via FileError
#   on_file(received) -> None              called once the file is verified and in place
Accept = Callable[[FileOffer], Awaitable[str | None]]
OnFile = Callable[[ReceivedFile], Awaitable[None]]
Progress = Callable[[int, int], Any]


@dataclass
class _Outgoing:
    fid: str
    peer_id: str | None = None
    acked: int = 0
    accept: asyncio.Future | None = None
    done: asyncio.Future | None = None
    progress: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: str | None = None


@dataclass
class _Incoming:
    key: tuple
    offer: FileOffer
    path: str
    held: int
    fd: int | None = None
    touched: float = field(default_factory=time.monotonic)
    finishing: bool = False


def _quiet(fut: asyncio.Future) -> None:
    if not fut.cancelled():
        fut.exception()


def sha256_of(path: str, *, block: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(block)
            if not b:
                return h.hexdigest()
            h.update(b)


class FileEngine:
    def __init__(self, *, accept: Accept | None = None, on_file: OnFile | None = None,
                 chunk: int = FILE_CHUNK, window: int = FILE_WINDOW):
        self._accept = accept
        self._on_file = on_file
        self.chunk = int(chunk)
        self.window = int(window)
        self._out: dict[str, _Outgoing] = {}
        self._in: dict[tuple, _Incoming] = {}
        self._tasks: set[asyncio.Task] = set()
        # (peer, fid) -> (size, sha256) of verified files: if FILE_DONE was lost
        # with the session, the re-offer is answered "complete" without a resend
        # and without calling on_file twice.
        self._completed: "OrderedDict[tuple, tuple[int, str]]" = OrderedDict()
        self.metrics = {"files_sent": 0, "files_received": 0, "bytes_sent": 0, "bytes_received": 0,
                        "files_failed": 0}

    def set_handlers(self, *, accept: Accept | None = None, on_file: OnFile | None = None) -> None:
        if accept is not None:
            self._accept = accept
        if on_file is not None:
            self._on_file = on_file

    # =================================================
    # Sending
    # =================================================

    async def send_file(
        self,
        path: str,
        *,
        send: Send,
        wait_ready: Callable[[float], Awaitable[bool]],
        name: str | None = None,
        meta: dict[str, Any] | None = None,
        fid: str | None = None,
        sha256: str | None = None,
        peer_id: str | None = None,
        ack_timeout: float = 30.0,
        resume_timeout: float = 300.0,
        progress: Progress | None = None,
    ) -> FileResult:
        """Send a file and return once the receiver has verified it.

        If the link drops, waits up to resume_timeout for it to come back and
        continues from what the receiver holds. Raises FileError when the
        receiver refuses or fails it, UplinkUnavailable when the link stays down.
        Calling again later with the same fid resumes too."""
        fid = fid or uuid.uuid4().hex
        if not _FID_RE.match(fid):
            raise ValueError("fid: 1-96 characters of A-Z a-z 0-9 . _ : -")
        size = os.path.getsize(path)
        digest = sha256 or await asyncio.to_thread(sha256_of, path)
        name = name or os.path.basename(path)
        offer = {"t": UPLINK_FILE_OFFER, "fid": fid, "name": name, "size": size, "sha256": digest,
                 "meta": dict(meta or {})}
        if fid in self._out:
            raise ValueError(f"transfer {fid} is already running")
        st = self._out[fid] = _Outgoing(fid, peer_id=peer_id)
        started = time.monotonic()
        attempts = 0
        resumed_from = 0
        try:
            while True:
                attempts += 1
                if not await wait_ready(resume_timeout):
                    raise UplinkUnavailable("link down; transfer can be resumed with the same fid")
                loop = asyncio.get_running_loop()
                st.accept, st.done = loop.create_future(), loop.create_future()
                for fut in (st.accept, st.done):   # link_lost() may fail one nobody awaits yet
                    fut.add_done_callback(_quiet)
                try:
                    if not await send(offer):
                        raise UplinkUnavailable("send failed")
                    ans = await asyncio.wait_for(asyncio.shield(st.accept), ack_timeout)
                    if not ans.get("ok"):
                        raise FileError(str(ans.get("reason") or ERR_FILE_REJECTED))
                    off = int(ans.get("offset") or 0)
                    if not 0 <= off <= size:
                        raise FileError(ERR_FILE_BAD_OFFER)
                    resumed_from = off
                    st.acked = off
                    await self._stream(path, fid, size, off, st, send, ack_timeout, progress)
                    res = await asyncio.wait_for(asyncio.shield(st.done), ack_timeout)
                    if not res.get("ok"):
                        raise FileError(str(res.get("reason") or ERR_FILE_WRITE))
                    self.metrics["files_sent"] += 1
                    return FileResult(fid=fid, size=size, sha256=digest, resumed_from=resumed_from,
                                      attempts=attempts, seconds=round(time.monotonic() - started, 3))
                except FileError:
                    raise
                except (UplinkUnavailable, asyncio.TimeoutError, ConnectionError) as e:
                    if st.cancelled:
                        raise FileError(st.cancelled)
                    log.info("file %s interrupted at %d/%d (%s); resuming", fid, st.acked, size,
                             type(e).__name__)
                    if time.monotonic() - started > resume_timeout and st.acked == resumed_from:
                        raise UplinkUnavailable("no progress; transfer can be resumed with the same fid")
                    await asyncio.sleep(0.2)
        except BaseException:
            self.metrics["files_failed"] += 1
            raise
        finally:
            self._out.pop(fid, None)

    async def _stream(self, path, fid, size, off, st: _Outgoing, send: Send, ack_timeout, progress):
        nxt = off
        with open(path, "rb") as f:
            while st.acked < size:
                if st.cancelled:
                    raise FileError(st.cancelled)
                while nxt < size and nxt - st.acked < self.window * self.chunk:
                    f.seek(nxt)
                    data = await asyncio.to_thread(f.read, self.chunk)
                    if not data:
                        raise FileError("file shrank while sending")
                    if not await send({"t": UPLINK_FILE_CHUNK, "fid": fid, "off": nxt,
                                       "d": base64.b64encode(data).decode()}):
                        raise UplinkUnavailable("send failed")
                    nxt += len(data)
                    self.metrics["bytes_sent"] += len(data)
                if st.acked >= size:
                    break
                st.progress.clear()
                before = st.acked
                await asyncio.wait_for(st.progress.wait(), ack_timeout)
                if st.acked < before:            # receiver went back (lost its .part): resend from there
                    nxt = st.acked
                if progress is not None and st.acked != before:
                    with contextlib.suppress(Exception):
                        r = progress(st.acked, size)
                        if asyncio.iscoroutine(r):
                            await r

    def link_lost(self, peer_id: str | None = None) -> None:
        """A session dropped (server side: that peer's): wake its senders so they
        resume after the reconnect, and close its half-written files."""
        for st in self._out.values():
            if st.peer_id != peer_id:
                continue
            for fut in (st.accept, st.done):
                if fut is not None and not fut.done():
                    fut.set_exception(UplinkUnavailable("session lost"))
            st.progress.set()
        for inc in self._in.values():
            if inc.offer.peer_id == peer_id:
                self._close(inc)

    # =================================================
    # Receiving (and the sender's answers)
    # =================================================

    async def handle(self, msg: dict, send: Send, peer_id: str | None = None) -> None:
        """Feed one FILE_* message of a session."""
        t = msg.get("t")
        fid = str(msg.get("fid") or "")
        if t == UPLINK_FILE_ACCEPT:
            st = self._out.get(fid)
            if st and st.accept is not None and not st.accept.done():
                st.accept.set_result(msg)
        elif t == UPLINK_FILE_ACK:
            st = self._out.get(fid)
            if st is not None:
                st.acked = int(msg.get("off") or 0)
                st.progress.set()
        elif t == UPLINK_FILE_DONE:
            st = self._out.get(fid)
            if st and st.done is not None and not st.done.done():
                st.done.set_result(msg)
        elif t == UPLINK_FILE_CANCEL:
            st = self._out.get(fid)
            if st is not None:
                st.cancelled = str(msg.get("reason") or ERR_FILE_CANCELLED)
                st.progress.set()
                for fut in (st.accept, st.done):
                    if fut is not None and not fut.done():
                        fut.set_exception(FileError(st.cancelled))
            inc = self._in.pop((peer_id, fid), None)
            if inc is not None:
                self._discard(inc)
        elif t == UPLINK_FILE_OFFER:
            await self._on_offer(msg, send, peer_id)
        elif t == UPLINK_FILE_CHUNK:
            await self._on_chunk(msg, send, peer_id)

    async def _on_offer(self, msg: dict, send: Send, peer_id):
        fid = str(msg.get("fid") or "")
        try:
            size = int(msg.get("size"))
            digest = str(msg.get("sha256") or "").lower()
            name = str(msg.get("name") or "")
            meta = msg.get("meta") if isinstance(msg.get("meta"), dict) else {}
            if not _FID_RE.match(fid) or size < 0 or not re.fullmatch(r"[0-9a-f]{64}", digest) or not name:
                raise ValueError
        except (TypeError, ValueError):
            await send({"t": UPLINK_FILE_ACCEPT, "fid": fid, "ok": False, "reason": ERR_FILE_BAD_OFFER})
            return
        self._expire()
        key = (peer_id, fid)
        if self._completed.get(key) == (size, digest):
            await send({"t": UPLINK_FILE_ACCEPT, "fid": fid, "ok": True, "offset": size})
            await send({"t": UPLINK_FILE_DONE, "fid": fid, "ok": True})
            return
        inc = self._in.get(key)
        if inc is not None and (inc.offer.size != size or inc.offer.sha256 != digest):
            self._discard(inc)                     # same fid, different file: start over
            self._in.pop(key, None)
            inc = None
        if inc is not None:
            inc.held = self._part_size(inc)        # the disk is the truth (a write may have landed late)
        if inc is None:
            offer = FileOffer(fid=fid, name=name, size=size, sha256=digest, meta=meta, peer_id=peer_id)
            if self._accept is None:
                await send({"t": UPLINK_FILE_ACCEPT, "fid": fid, "ok": False, "reason": ERR_FILE_NO_HANDLER})
                return
            try:
                path = await self._accept(offer)
            except FileError as e:
                path, reason = None, e.reason
            except Exception:
                log.exception("file accept callback failed")
                path, reason = None, ERR_FILE_REJECTED
            else:
                reason = ERR_FILE_REJECTED
            if not path:
                await send({"t": UPLINK_FILE_ACCEPT, "fid": fid, "ok": False, "reason": reason})
                return
            inc = _Incoming(key=key, offer=offer, path=str(path), held=0)
            inc.held = self._part_size(inc)        # left by an earlier attempt, even before a restart
            self._in[key] = inc
        inc.touched = time.monotonic()
        await send({"t": UPLINK_FILE_ACCEPT, "fid": fid, "ok": True, "offset": inc.held})
        if inc.held == size:
            self._spawn_finish(inc, send)

    def _spawn_finish(self, inc: _Incoming, send: Send):
        """Checking a GB file's SHA-256 takes seconds: off the message path."""
        inc.finishing = True
        t = asyncio.create_task(self._finish(inc, send), name=f"nika-file-finish-{inc.offer.fid}")
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _on_chunk(self, msg: dict, send: Send, peer_id):
        fid = str(msg.get("fid") or "")
        inc = self._in.get((peer_id, fid))
        if inc is None:
            await send({"t": UPLINK_FILE_CANCEL, "fid": fid, "reason": ERR_FILE_UNKNOWN})
            return
        if inc.finishing:
            return
        inc.touched = time.monotonic()
        off = int(msg.get("off") or 0)
        if off != inc.held:                        # duplicate or a gap: say what we hold
            await send({"t": UPLINK_FILE_ACK, "fid": fid, "off": inc.held})
            return
        try:
            data = base64.b64decode(msg.get("d") or "", validate=True)
            if inc.held + len(data) > inc.offer.size:
                raise ValueError("more data than offered")
            if inc.fd is None:
                os.makedirs(os.path.dirname(os.path.abspath(inc.path)), exist_ok=True)
                inc.fd = os.open(inc.path + ".part", os.O_WRONLY | os.O_CREAT, 0o600)
            # positional: the bytes at an offset are always the file's bytes there, so
            # a write that lands after the session dropped cannot misplace data
            n = await asyncio.to_thread(os.pwrite, inc.fd, data, off)
            if n != len(data):
                raise OSError(f"short write ({n} of {len(data)})")
        except Exception as e:
            log.warning("file %s: write failed: %s", fid, e)
            self._in.pop(inc.key, None)
            self._discard(inc)
            await send({"t": UPLINK_FILE_CANCEL, "fid": fid, "reason": ERR_FILE_WRITE})
            return
        inc.held += len(data)
        self.metrics["bytes_received"] += len(data)
        await send({"t": UPLINK_FILE_ACK, "fid": fid, "off": inc.held})
        if inc.held == inc.offer.size:
            self._spawn_finish(inc, send)

    async def _finish(self, inc: _Incoming, send: Send):
        self._close(inc)
        part = inc.path + ".part"
        self._in.pop(inc.key, None)
        try:
            if not os.path.exists(part):           # empty file: no chunk ever created it
                os.makedirs(os.path.dirname(os.path.abspath(part)), exist_ok=True)
                open(part, "ab").close()
            ok = (await asyncio.to_thread(sha256_of, part)) == inc.offer.sha256
            if ok:
                os.replace(part, inc.path)
        except OSError as e:
            log.warning("file %s: finishing failed: %s", inc.offer.fid, e)
            self._discard(inc)
            self.metrics["files_failed"] += 1
            await send({"t": UPLINK_FILE_DONE, "fid": inc.offer.fid, "ok": False, "reason": ERR_FILE_WRITE})
            return
        if not ok:
            self._discard(inc)
            self.metrics["files_failed"] += 1
            await send({"t": UPLINK_FILE_DONE, "fid": inc.offer.fid, "ok": False, "reason": ERR_FILE_CHECKSUM})
            return
        self._completed[inc.key] = (inc.offer.size, inc.offer.sha256)
        while len(self._completed) > COMPLETED_KEEP:
            self._completed.popitem(last=False)
        self.metrics["files_received"] += 1
        received = ReceivedFile(fid=inc.offer.fid, name=inc.offer.name, size=inc.offer.size,
                                sha256=inc.offer.sha256, meta=inc.offer.meta, peer_id=inc.offer.peer_id,
                                path=inc.path)
        if self._on_file is not None:
            try:
                await self._on_file(received)
            except Exception:
                log.exception("file on_file callback failed")
        await send({"t": UPLINK_FILE_DONE, "fid": inc.offer.fid, "ok": True})

    def incoming(self) -> list[dict]:
        return [{"fid": i.offer.fid, "name": i.offer.name, "peer_id": i.offer.peer_id,
                 "size": i.offer.size, "held": i.held} for i in self._in.values()]

    def outgoing(self) -> list[dict]:
        return [{"fid": o.fid, "acked": o.acked} for o in self._out.values()]

    # -------------------------------------------------- housekeeping

    @staticmethod
    def _close(inc: _Incoming):
        if inc.fd is not None:
            with contextlib.suppress(OSError):
                os.close(inc.fd)
            inc.fd = None

    @staticmethod
    def _part_size(inc: _Incoming) -> int:
        """Bytes of this transfer already on disk (0 if none, or if the .part is
        bigger than the file: then it is someone else's and starts over)."""
        part = inc.path + ".part"
        try:
            held = os.path.getsize(part)
        except OSError:
            return 0
        if held > inc.offer.size:
            with contextlib.suppress(OSError):
                os.remove(part)
            return 0
        return held

    def _discard(self, inc: _Incoming):
        self._close(inc)
        with contextlib.suppress(OSError):
            os.remove(inc.path + ".part")

    def _expire(self):
        now = time.monotonic()
        for key, inc in list(self._in.items()):
            if now - inc.touched > INCOMING_TTL_S:
                self._in.pop(key, None)
                self._discard(inc)
