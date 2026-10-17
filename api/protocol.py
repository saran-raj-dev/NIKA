# protocol.py

# ---- Core message types ----
REQUEST = "REQUEST"
RESPONSE = "RESPONSE"
ERROR = "ERROR"

# ---- Pub/Sub ----
SUBSCRIBE = "SUBSCRIBE"
UNSUBSCRIBE = "UNSUBSCRIBE"
EVENT = "EVENT"

# ---- Heartbeat ----
PING = "PING"
PONG = "PONG"

# ---- Streaming ----
STREAM_START = "STREAM_START"
STREAM_STOP = "STREAM_STOP"
STREAM = "STREAM"
RESUME_STREAM = "RESUME_STREAM"
STREAM_STATS = "STREAM_STATS"

# ---- Error codes ----
ERR_UNKNOWN_MESSAGE = "UNKNOWN_MESSAGE"
ERR_NO_HANDLER = "NO_HANDLER"
ERR_INVALID_PAYLOAD = "INVALID_PAYLOAD"

# ---------------------
METRICS = "METRICS"

# ---- Client lifecycle ----
CLIENT_JOIN = "CLIENT_JOIN"
CLIENT_LEAVE = "CLIENT_LEAVE"

# ---- Mesh (see MESH.md) ----
MESH_BEACON = "MESH_BEACON"              # UDP: peer presence + uplink health

RELAY_OPEN = "RELAY_OPEN"                # origin -> relay: carry my traffic
RELAY_OPEN_ACK = "RELAY_OPEN_ACK"        # relay -> origin: accepted / rejected
RELAY_CLOSE = "RELAY_CLOSE"              # origin -> relay: my uplink is back
RELAY_REVOKE = "RELAY_REVOKE"            # relay -> origin: I can no longer relay
RELAY_PING = "RELAY_PING"                # origin -> relay: session keepalive
RELAY_PONG = "RELAY_PONG"

RELAY_DATA = "RELAY_DATA"                # origin -> relay: publish this upstream
RELAY_DATA_ACK = "RELAY_DATA_ACK"        # relay -> origin: published / failed

RELAY_DOWNLINK = "RELAY_DOWNLINK"        # relay -> origin: command from the cloud
RELAY_DOWNLINK_ACK = "RELAY_DOWNLINK_ACK"  # origin -> relay: delivered / duplicate / expired

# ---- Mesh error codes ----
ERR_RELAY_CAPACITY = "RELAY_CAPACITY"
ERR_RELAY_NO_UPLINK = "RELAY_NO_UPLINK"
ERR_RELAY_NOT_ALLOWED = "RELAY_NOT_ALLOWED"
ERR_RELAY_NO_SESSION = "RELAY_NO_SESSION"
ERR_RELAY_RATE_LIMITED = "RELAY_RATE_LIMITED"
ERR_RELAY_UPLINK_FAILED = "RELAY_UPLINK_FAILED"