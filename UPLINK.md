# NIKA Uplink: Peer ⇄ Platform

**Package:** `nika.uplink` · **Transport:** ZeroMQ DEALER ⇄ ROUTER over TCP · **Security:** [NSC](SECURE.md) · **Default port:** 47900

The uplink is how a NIKA peer (e.g. a gateway) talks to a NIKA platform: upstream data, downstream commands, and relaying for neighbours, over one encrypted, authenticated session. It's generic: it carries **topic + payload** and never interprets them. Platforms plug in through callbacks.

---

## 1. Model

```
peer ──NSC session──▶ platform           upstream: UPLINK_DATA → UPLINK_ACK (after the app stored it)
peer ◀────────────── platform            downstream: UPLINK_CMD → UPLINK_CMD_ACK
relay ─UPLINK_ROUTE(origin)─▶ platform   "I carry origin"; its data and commands then go via the relay
```

- **Acks mean stored:** the server acks only **after** the application's `on_data` returned, so an ack means "the platform has it". A rejection is explicit, with a reason. Nothing is dropped silently.
- **Order:** messages from one peer are processed in order.
- **One session per peer:** a new session for the same peer replaces the old one, but only once its first encrypted frame authenticates. A valid message 1 alone (e.g. from someone holding a stolen key but not the pre-shared key) can't knock out the live session.
- **Optional pre-shared key:** `NikaUplinkServer(..., psk=...)` and `NikaUplinkClient(..., psk=...)` turn on NSC's IKpsk2 mode ([SECURE.md §2a](SECURE.md)). Both must hold the same key. A mismatch never connects: the server drops the handshake without a reply, and the client keeps retrying with `last_error` pointing at the pre-shared key setting.
- **Liveness:** the client pings every few seconds. The server drops a silent session after `session_timeout` (30 s), along with its routes.

## 2. Wire format

ZeroMQ multipart frames:

| Frame | Direction | Content |
|---|---|---|
| `H1` + handshake message 1 | client → server | NSC |
| `H2` + handshake message 2 | server → client | NSC |
| `D` + NSC frame | both | encrypted JSON message |
| `E` + `{"code": …}` | server → client | plaintext refusal (only `UPLINK_NOT_AUTHORISED`) |

Inside `D`, the JSON carries `{"t": TYPE, ...}`. Payloads are UTF-8 text in `p`, or base64 in `pb`. The maximum message size is 1 MiB.

| Type | Direction | Fields |
|---|---|---|
| `UPLINK_HELLO` / `UPLINK_WELCOME` | c→s / s→c | `meta` (app-defined) / `peer_id` |
| `UPLINK_DATA` / `UPLINK_ACK` | c→s / s→c | `msg_id, topic, p\|pb, qos, origin?` / `msg_id, ok, reason?` |
| `UPLINK_ROUTE` | c→s | `origin, active` (acked as `msg_id = "route:<origin>"`) |
| `UPLINK_CMD` / `UPLINK_CMD_ACK` | s→c / c→s | `command_id, topic, p\|pb, expires_at?, target?` / `command_id, status, detail?` |
| `UPLINK_PING` / `UPLINK_PONG` / `UPLINK_BYE` | both | — |
| `UPLINK_FILE_*` | both | file transfer, see §5 |

Errors: `UPLINK_NOT_AUTHORISED`, `UPLINK_ORIGIN_DENIED`, `UPLINK_STORE_FAILED`, `UPLINK_UNAVAILABLE`, `UPLINK_BAD_MESSAGE`, or any reason string the application returns.

## 3. Relaying (with the [mesh](MESH.md))

1. A relay announces `UPLINK_ROUTE {origin, active: true}`. The platform's `on_route(relay, origin, True)` decides whether to allow it.
2. If allowed, the relay may send `UPLINK_DATA` with `origin=<that peer>`. Without an allowed route: `UPLINK_ORIGIN_DENIED`.
3. `send_command(origin, …)` reaches the origin directly if it's connected, otherwise through the relay that announced it, with `target=<origin>`.
4. Routes are replayed automatically after a reconnect, and dropped when the relay's session ends.
5. Concurrent `announce_route()` calls for the same origin share **one** request (a burst of relayed messages for a new origin waits on a single announcement).

## 4. Commands

The client de-duplicates commands by `command_id` and refuses expired ones before the application sees them. Statuses are `accepted`, `duplicate`, `expired`, `rejected`, `failed`, or whatever the app returns. A platform can share one `CommandGuard` between the uplink and the mesh, so a command that arrives both ways runs once. Commands with a `target` (for a peer this client relays for) are passed straight to the app, which forwards them; the target de-duplicates them itself.

## 5. Files (any size, both directions)

For files bigger than one message (exports, logs, backups, packages: up to many GB). It runs **inside the same NSC session**, so there is no second port, login or certificate, and the same peer identity and pre-shared key apply.

```
sender ── FILE_OFFER  {fid, name, size, sha256, meta} ─▶ receiver   app: accept(offer) -> path | None
       ◀─ FILE_ACCEPT {fid, ok, offset | reason} ──────             offset > 0: resume
       ── FILE_CHUNK  {fid, off, d} × N ──────────────▶             written at off into <path>.part
       ◀─ FILE_ACK    {fid, off} ─────────────────────              bytes held so far
       ◀─ FILE_DONE   {fid, ok | reason} ─────────────              sha256 checked, renamed, app: on_file()
either ── FILE_CANCEL {fid, reason} ──────────────────▶
```

| Field | Meaning |
|---|---|
| `fid` | Transfer id chosen by the sender, `[A-Za-z0-9._:-]{1,96}`. Re-sending with the same `fid` resumes. |
| `meta` | App-defined JSON (e.g. `{"kind": "csv_export", "export_id": "e1"}`). NIKA never reads it. |
| `d` | Base64 of up to 256 KiB raw (≈ 350 KB: well under the 1 MiB message limit). |

**Design decisions**

- **Flow control:** at most 8 chunks (2 MiB) are unacknowledged. A slow disk or link slows the sender; memory stays flat whatever the file size.
- **Resume, not restart:** if the session drops, the sender waits for the link (`resume_timeout`, default 300 s) and offers the same `fid` again. The receiver answers with the bytes it holds, and the sender continues from there. The `.part` file on disk *is* the state, so this also works after the receiver restarts.
- **Positional writes:** each chunk is written at its own offset (`pwrite`), and the resume point is always read from the `.part` size on disk. A write still landing after its session dropped can't put bytes in the wrong place.
- **Done means verified:** `FILE_DONE ok` is sent only after the receiver hashed the whole file and it matched the offered SHA-256. A mismatch deletes the `.part` and fails with `FILE_CHECKSUM_MISMATCH`. Hashing runs off the message path, so a multi-GB check doesn't stall the session.
- **Exactly-once delivery to the app:** the receiver remembers the last 1000 completed `(peer, fid)` with their size and SHA-256. If `FILE_DONE` was lost with the session, the re-offer is answered "complete" at once: nothing is re-sent and `on_file` isn't called twice.
- **The app decides storage:** `accept(offer)` returns the path to write to, or `None` to refuse (`FILE_REJECTED`; raise `FileError("REASON")` for a specific reason). No handler means `FILE_NO_HANDLER`. NIKA never picks paths from the offered name.
- **Files share the session's order:** file messages are handled in order with the peer's data. Data and commands keep flowing during a transfer (one window per file).
- **Direct sessions only:** a peer that's relayed through the mesh can't receive or send files (`send_file` raises `FileError`). Relays carry small messages; GB files would starve them.
- **Interrupted transfers expire:** an unfinished incoming transfer is dropped, with its `.part`, after 24 h without activity.

Errors: `FILE_REJECTED`, `FILE_NO_HANDLER`, `FILE_BAD_OFFER`, `FILE_CHECKSUM_MISMATCH`, `FILE_WRITE_FAILED`, `FILE_CANCELLED`, `FILE_UNKNOWN_TRANSFER`.

## 6. API

```python
from nika.secure import StaticKey
from nika.uplink import NikaUplinkServer, NikaUplinkClient, UplinkError, UplinkUnavailable

# platform
server = NikaUplinkServer(server_key, bind="tcp://0.0.0.0:47900",   # psk=... (optional)
    authorize=lambda pub: lookup_peer(pub),              # async → peer_id | None
    on_data=lambda dm: store(dm),                        # async → None | "REASON"
    on_route=lambda relay, origin, active: allowed(...), # async → bool
    on_session=..., on_command_ack=...)
await server.start()
await server.send_command("gw-b", "site/gw-b/cmd", '{"v": 1}', expires_at=time.time() + 60)
server.sessions(); server.route_of("gw-b")               # 'direct' | relay peer_id | None
# files: NikaUplinkServer(..., on_file_offer=accept, on_file=got)   accept(FileOffer) -> path | None
res = await server.send_file("gw-b", "/data/import.csv", meta={"kind": "csv_import"})  # FileResult

# peer
client = NikaUplinkClient(my_key, server_public, endpoint="tcp://platform:47900",   # psk=... (optional)
                          meta={...}, on_command=handle)  # async (Command) → status
await client.start()
await client.publish("site/gw-b/data/x", payload, msg_id="row-42")   # returns when stored
await client.announce_route("gw-c", True)
res = await client.send_file("/var/backups/db.tar.gz", meta={"kind": "backup"})   # returns when verified
# NikaUplinkClient(..., on_file_offer=accept, on_file=got) to receive files from the platform
```

`publish` raises `UplinkError(reason)` when the platform rejects a message, and `UplinkUnavailable` when not connected or no ack arrived in time (the message may or may not have been stored, so retry it; platforms should de-duplicate on `msg_id` or content). Client states: `disconnected`, `connecting`, `connected`, `refused` (the server doesn't know our key; retried slowly).

## 7. Tests
`python -m pytest test/test_files.py -v`: both directions, empty file, refusal, wrong checksum, resume after a dropped session, resume after a receiver restart, lost `FILE_DONE`, same `fid` with new content, relayed and unreachable peers, two files at once with data.

`python -m pytest test/test_uplink.py -v` (20 tests, real sockets on localhost), including the pre-shared key: psk round trip, mismatches in both directions, and a stolen key without the psk failing to replace the live session.
