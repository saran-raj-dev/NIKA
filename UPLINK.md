# NIKA Uplink: Peer ⇄ Platform

**Package:** `api.uplink` · **Transport:** ZeroMQ DEALER ⇄ ROUTER over TCP · **Security:** [NSC](SECURE.md) · **Default port:** 47900

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

Errors: `UPLINK_NOT_AUTHORISED`, `UPLINK_ORIGIN_DENIED`, `UPLINK_STORE_FAILED`, `UPLINK_UNAVAILABLE`, `UPLINK_BAD_MESSAGE`, or any reason string the application returns.

## 3. Relaying (with the [mesh](MESH.md))

1. A relay announces `UPLINK_ROUTE {origin, active: true}`. The platform's `on_route(relay, origin, True)` decides whether to allow it.
2. If allowed, the relay may send `UPLINK_DATA` with `origin=<that peer>`. Without an allowed route: `UPLINK_ORIGIN_DENIED`.
3. `send_command(origin, …)` reaches the origin directly if it's connected, otherwise through the relay that announced it, with `target=<origin>`.
4. Routes are replayed automatically after a reconnect, and dropped when the relay's session ends.
5. Concurrent `announce_route()` calls for the same origin share **one** request (a burst of relayed messages for a new origin waits on a single announcement).

## 4. Commands

The client de-duplicates commands by `command_id` and refuses expired ones before the application sees them. Statuses are `accepted`, `duplicate`, `expired`, `rejected`, `failed`, or whatever the app returns. A platform can share one `CommandGuard` between the uplink and the mesh, so a command that arrives both ways runs once. Commands with a `target` (for a peer this client relays for) are passed straight to the app, which forwards them; the target de-duplicates them itself.

## 5. API

```python
from api.secure import StaticKey
from api.uplink import NikaUplinkServer, NikaUplinkClient, UplinkError, UplinkUnavailable

# platform
server = NikaUplinkServer(server_key, bind="tcp://0.0.0.0:47900",   # psk=... (optional)
    authorize=lambda pub: lookup_peer(pub),              # async → peer_id | None
    on_data=lambda dm: store(dm),                        # async → None | "REASON"
    on_route=lambda relay, origin, active: allowed(...), # async → bool
    on_session=..., on_command_ack=...)
await server.start()
await server.send_command("gw-b", "site/gw-b/cmd", '{"v": 1}', expires_at=time.time() + 60)
server.sessions(); server.route_of("gw-b")               # 'direct' | relay peer_id | None

# peer
client = NikaUplinkClient(my_key, server_public, endpoint="tcp://platform:47900",   # psk=... (optional)
                          meta={...}, on_command=handle)  # async (Command) → status
await client.start()
await client.publish("site/gw-b/data/x", payload, msg_id="row-42")   # returns when stored
await client.announce_route("gw-c", True)
```

`publish` raises `UplinkError(reason)` when the platform rejects a message, and `UplinkUnavailable` when not connected or no ack arrived in time (the message may or may not have been stored, so retry it; platforms should de-duplicate on `msg_id` or content). Client states: `disconnected`, `connecting`, `connected`, `refused` (the server doesn't know our key; retried slowly).

## 6. Tests
`python -m pytest test/test_uplink.py -v` (20 tests, real sockets on localhost), including the pre-shared key: psk round trip, mismatches in both directions, and a stolen key without the psk failing to replace the live session.
