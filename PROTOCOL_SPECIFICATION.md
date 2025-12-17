📜 NIKA Protocol — Version 1.0 Specification

Status: Stable
Version: 1.0
Transport: ZeroMQ
Pattern: Control Plane (ROUTER/DEALER) + Data Plane (PUB/SUB)

1. Overview

NIKA is a lightweight, event-driven streaming protocol designed for:

Real-time data delivery

Backpressure awareness

Explicit failure semantics

Adapter-friendly consumption (SSE, WebSocket, etc.)

NIKA prioritizes honesty over illusion:

It does not promise exactly-once delivery

It does not hide data loss

It makes system behavior observable and debuggable

2. Architecture
+-------------+        Control Plane        +-------------+
|   Client    | <------------------------> |   Server    |
| (DEALER)    |        (ROUTER)             |             |
+-------------+                             +-------------+
        |
        |          Data Plane (PUB/SUB)
        v
+-------------+ <------------------------> +-------------+
|   Client    |        Streams / Events    |   Server    |
|   (SUB)     |                            |   (PUB)     |
+-------------+                            +-------------+

Planes
Plane	Purpose
Control	Requests, heartbeats, subscriptions, resume
Data	High-throughput event & stream delivery
3. Envelope Format (Mandatory)

All messages MUST follow this envelope.

{
  "nika": "1.0",
  "type": "STREAM",
  "id": "uuid",
  "ts": "ISO-8601 timestamp",
  "sender": "client | server",
  "payload": {}
}

Field Definitions
Field	Description
nika	Protocol version
type	Message type
id	Correlation ID
ts	UTC timestamp
sender	Sender identity
payload	Message-specific data
4. Message Types
Core

REQUEST

RESPONSE

ERROR

Heartbeat

PING

PONG

Streaming

STREAM_START

STREAM_STOP

STREAM

RESUME_STREAM

5. Streaming Semantics
STREAM Message
{
  "type": "STREAM",
  "payload": {
    "stream": "numbers",
    "seq": 42,
    "data": { ... }
  }
}

Guarantees

seq is monotonically increasing

Messages MAY be dropped under backpressure

Order is preserved per stream

Non-Guarantees

No replay

No exactly-once

No persistence (v1.0)

6. Backpressure Model
Server Behavior

Server enforces rate limits

When exceeded, messages are silently dropped

Server NEVER blocks

Client Behavior

Client detects loss via sequence gaps

Loss is observable and loggable

Application decides recovery strategy

Backpressure is explicit, not hidden.

7. Reconnect & Resume
Client → Server
{
  "type": "RESUME_STREAM",
  "payload": {
    "stream": "numbers",
    "last_seq": 41
  }
}

Server → Client
{
  "type": "RESPONSE",
  "payload": {
    "action": "RESUME_STREAM",
    "stream": "numbers",
    "resumed_from": 41,
    "note": "Resume is live-only; gaps may exist"
  }
}

Semantics

Resume is live-only

Gaps are acknowledged explicitly

No historical replay is implied

8. Error Handling
{
  "type": "ERROR",
  "payload": {
    "code": "ERR_INVALID_PAYLOAD",
    "message": "Missing stream name"
  }
}

Common Error Codes

ERR_UNKNOWN_MESSAGE

ERR_INVALID_PAYLOAD

ERR_NO_HANDLER

9. Client Lifecycle

Connect

Send PING

Start streams

Receive STREAM

Detect gaps (if any)

Resume if needed

Disconnect / timeout cleanup

10. Server Guarantees

✔ Never blocks
✔ Never lies about delivery
✔ Never hides data loss
✔ Cleans up dead clients
✔ Enforces rate limits

11. What NIKA Is (and Is Not)
NIKA IS

A real-time streaming protocol

Backpressure-aware

Adapter-friendly

Honest by design

NIKA IS NOT

A message broker

A persistence layer

A Kafka replacement

Exactly-once delivery system

12. Comparison
Feature	NIKA	SSE	MQTT	Kafka
Backpressure visibility	✅	❌	❌	✅
Resume support	✅	❌	⚠️	✅
Persistent storage	❌	❌	❌	✅
Lightweight	✅	✅	⚠️	❌
13. Versioning Policy

Backward compatible changes → minor version

Breaking changes → major version

Envelope version always required

14. Status

NIKA v1.0 is complete and stable.

Future versions may add:

Persistence

Auth

Stream history

QoS tiers