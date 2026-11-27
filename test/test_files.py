"""
NIKA uplink file transfer (nika/uplink/files.py): real client/server over localhost.

Run:  python -m pytest test/test_files.py -v
"""
import asyncio
import hashlib
import os
import socket
import time

import pytest
import pytest_asyncio

from nika.secure import StaticKey
from nika.uplink import FileError, NikaUplinkClient, NikaUplinkServer

pytestmark = pytest.mark.asyncio


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def wait_for(cond, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return False


def make_file(path, size):
    with open(path, "wb") as f:
        f.write(os.urandom(size))
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


class Rig:
    def __init__(self, tmp):
        self.tmp = tmp
        self.port = free_port()
        self.skey = StaticKey.generate()
        self.ckey = StaticKey.generate()
        self.server_got, self.client_got = [], []
        self.refuse = False
        self.server = self.client = None

    async def start_server(self, **kw):
        async def authorize(pub):
            return "gw-1" if pub == self.ckey.public else None

        async def on_data(dm):
            return None

        async def accept(offer):
            if self.refuse:
                return None
            return str(self.tmp / "server" / offer.peer_id / offer.name)

        async def on_file(rf):
            self.server_got.append(rf)

        self.server = NikaUplinkServer(self.skey, bind=f"tcp://127.0.0.1:{self.port}", authorize=authorize,
                                       on_data=on_data, on_file_offer=accept, on_file=on_file, **kw)
        await self.server.start()

    async def start_client(self):
        async def accept(offer):
            return str(self.tmp / "client" / offer.name)

        async def on_file(rf):
            self.client_got.append(rf)

        self.client = NikaUplinkClient(self.ckey, self.skey.public, endpoint=f"tcp://127.0.0.1:{self.port}",
                                       on_file_offer=accept, on_file=on_file, heartbeat=0.5, timeout=3.0)
        await self.client.start()
        assert await self.client.wait_connected(5)

    async def stop(self):
        if self.client:
            await self.client.stop()
        if self.server:
            await self.server.stop()


@pytest_asyncio.fixture
async def rig(tmp_path):
    r = Rig(tmp_path)
    await r.start_server()
    await r.start_client()
    yield r
    await r.stop()


async def test_client_sends_a_file_to_the_platform(rig, tmp_path):
    src = tmp_path / "export.csv"
    digest = make_file(src, 5 * 1024 * 1024 + 123)          # several windows, odd tail
    res = await rig.client.send_file(str(src), meta={"kind": "csv_export", "export_id": "e1"})
    assert res.sha256 == digest and res.size == src.stat().st_size and res.resumed_from == 0
    (rf,) = rig.server_got
    assert rf.peer_id == "gw-1" and rf.meta == {"kind": "csv_export", "export_id": "e1"}
    assert hashlib.sha256(open(rf.path, "rb").read()).hexdigest() == digest
    assert not os.path.exists(rf.path + ".part")


async def test_platform_sends_a_file_to_a_peer(rig, tmp_path):
    src = tmp_path / "import.csv"
    digest = make_file(src, 700 * 1024)
    res = await rig.server.send_file("gw-1", str(src), name="points.csv", meta={"kind": "csv_import"})
    assert res.sha256 == digest
    (rf,) = rig.client_got
    assert rf.name == "points.csv" and rf.meta["kind"] == "csv_import"
    assert hashlib.sha256(open(rf.path, "rb").read()).hexdigest() == digest


async def test_empty_file(rig, tmp_path):
    src = tmp_path / "empty.txt"
    src.write_bytes(b"")
    await rig.client.send_file(str(src))
    assert rig.server_got and os.path.getsize(rig.server_got[0].path) == 0


async def test_refused_offer_raises(rig, tmp_path):
    rig.refuse = True
    src = tmp_path / "x.bin"
    make_file(src, 1000)
    with pytest.raises(FileError) as e:
        await rig.client.send_file(str(src))
    assert e.value.reason == "FILE_REJECTED"


async def test_wrong_checksum_is_refused_and_nothing_kept(rig, tmp_path):
    src = tmp_path / "x.bin"
    make_file(src, 300 * 1024)
    with pytest.raises(FileError) as e:
        await rig.client.send_file(str(src), sha256="0" * 64)
    assert e.value.reason == "FILE_CHECKSUM_MISMATCH"
    assert rig.server_got == []
    assert not os.path.exists(tmp_path / "server" / "gw-1" / "x.bin")
    assert not os.path.exists(tmp_path / "server" / "gw-1" / "x.bin.part")


async def test_transfer_resumes_after_the_session_drops(rig, tmp_path):
    src = tmp_path / "backup.tar"
    digest = make_file(src, 8 * 1024 * 1024)
    cut = []

    async def progress(done, total):
        # mid-way: cut the session (the sender waits here, so at most one
        # window is past this point when the session goes)
        if done >= 2 * 1024 * 1024 and not cut:
            cut.append(done)
            await rig.server.disconnect("gw-1", "test")

    res = await rig.client.send_file(str(src), progress=progress, ack_timeout=5, resume_timeout=20)
    assert res.attempts >= 2 and res.resumed_from > 0 and res.sha256 == digest
    (rf,) = rig.server_got
    assert hashlib.sha256(open(rf.path, "rb").read()).hexdigest() == digest


async def test_resume_survives_a_receiver_restart(tmp_path):
    """The .part file is the state: a new server process continues from it."""
    r = Rig(tmp_path)
    await r.start_server()
    await r.start_client()
    try:
        src = tmp_path / "big.bin"
        digest = make_file(src, 6 * 1024 * 1024)
        hit = asyncio.Event()

        async def progress(done, total):
            if done >= 2 * 1024 * 1024 and not hit.is_set():
                hit.set()

        task = asyncio.create_task(r.client.send_file(str(src), fid="restart-1", progress=progress,
                                                      ack_timeout=3, resume_timeout=30))
        await asyncio.wait_for(hit.wait(), 10)
        await r.server.stop()                                # receiver goes away mid-transfer
        held = os.path.getsize(tmp_path / "server" / "gw-1" / "big.bin.part")
        assert held >= 2 * 1024 * 1024
        await r.start_server()                               # same port, same key, empty memory
        res = await asyncio.wait_for(task, 40)
        assert res.resumed_from >= held - 8 * 256 * 1024 and res.sha256 == digest
        assert r.server_got and hashlib.sha256(open(r.server_got[0].path, "rb").read()).hexdigest() == digest
    finally:
        await r.stop()


async def test_bad_fid_is_rejected_before_sending(rig, tmp_path):
    src = tmp_path / "x.bin"
    make_file(src, 10)
    with pytest.raises(ValueError):
        await rig.client.send_file(str(src), fid="../etc/passwd")


async def test_lost_done_does_not_resend_or_deliver_twice(rig, tmp_path):
    """The session drops after the receiver verified the file but before the
    sender heard FILE_DONE: the re-offer is answered 'complete'."""
    src = tmp_path / "x.bin"
    digest = make_file(src, 600 * 1024)
    await rig.client.send_file(str(src), fid="same-1")
    sent_before = rig.client.files.metrics["bytes_sent"]
    res = await rig.client.send_file(str(src), fid="same-1")        # e.g. the retry after a lost DONE
    assert res.resumed_from == res.size and res.sha256 == digest
    assert rig.client.files.metrics["bytes_sent"] == sent_before    # nothing re-sent
    assert len(rig.server_got) == 1                                   # delivered once


async def test_same_fid_new_content_starts_over(rig, tmp_path):
    src = tmp_path / "x.bin"
    make_file(src, 300 * 1024)
    await rig.client.send_file(str(src), fid="reuse-1")
    digest = make_file(src, 400 * 1024)
    res = await rig.client.send_file(str(src), fid="reuse-1")
    assert res.resumed_from == 0 and len(rig.server_got) == 2
    assert hashlib.sha256(open(rig.server_got[1].path, "rb").read()).hexdigest() == digest


async def test_relayed_peer_gets_a_clear_error(rig, tmp_path):
    rig.server._routes["gw-2"] = "gw-1"                    # gw-2 is carried by gw-1 over the mesh
    src = tmp_path / "x.bin"
    make_file(src, 10)
    with pytest.raises(FileError) as e:
        await rig.server.send_file("gw-2", str(src))
    assert "relayed" in e.value.reason


async def test_unreachable_peer_times_out(rig, tmp_path):
    from nika.uplink import UplinkUnavailable
    src = tmp_path / "x.bin"
    make_file(src, 10)
    with pytest.raises(UplinkUnavailable):
        await rig.server.send_file("gw-9", str(src), resume_timeout=0.5)


async def test_no_handler_refuses(tmp_path):
    r = Rig(tmp_path)
    await r.start_server()
    r.server.files._accept = None
    await r.start_client()
    try:
        src = tmp_path / "x.bin"
        make_file(src, 10)
        with pytest.raises(FileError) as e:
            await r.client.send_file(str(src))
        assert e.value.reason == "FILE_NO_HANDLER"
    finally:
        await r.stop()


async def test_two_files_at_once_and_data_still_flows(rig, tmp_path):
    a, b = tmp_path / "a.bin", tmp_path / "b.bin"
    da, db = make_file(a, 3 * 1024 * 1024), make_file(b, 2 * 1024 * 1024)
    ra, rb, mid = await asyncio.gather(rig.client.send_file(str(a)), rig.client.send_file(str(b)),
                                       rig.client.publish("t/x", "data during files"))
    assert {ra.sha256, rb.sha256} == {da, db} and len(rig.server_got) == 2
