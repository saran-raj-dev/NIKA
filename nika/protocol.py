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
RELAY_CHALLENGE = "RELAY_CHALLENGE"      # relay -> origin: prove the identity you claimed
RELAY_PROVE = "RELAY_PROVE"              # origin -> relay: proof of key possession
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
ERR_RELAY_ACL_DENIED = "RELAY_ACL_DENIED"        # relay's broker refuses the origin's topics
ERR_RELAY_WRONG_BROKER = "RELAY_WRONG_BROKER"    # relay and origin use different brokers
ERR_RELAY_IDENTITY = "RELAY_IDENTITY_FAILED"     # identity claim malformed or proof wrong
ERR_RELAY_IDENTITY_REQUIRED = "RELAY_IDENTITY_REQUIRED"  # relay only carries proven identities

# ---- Uplink (see UPLINK.md) ----
UPLINK_HELLO = "UPLINK_HELLO"            # client -> server: who I am (app-defined meta)
UPLINK_WELCOME = "UPLINK_WELCOME"        # server -> client: session accepted
UPLINK_DATA = "UPLINK_DATA"              # client -> server: topic + payload (own or relayed origin)
UPLINK_ACK = "UPLINK_ACK"                # server -> client: accepted / rejected (after the app stored it)
UPLINK_CMD = "UPLINK_CMD"                # server -> client: command for this peer or a relayed origin
UPLINK_CMD_ACK = "UPLINK_CMD_ACK"        # client -> server: accepted / duplicate / expired / rejected
UPLINK_ROUTE = "UPLINK_ROUTE"            # client -> server: I carry (or stop carrying) origin X
UPLINK_PING = "UPLINK_PING"
UPLINK_PONG = "UPLINK_PONG"
UPLINK_BYE = "UPLINK_BYE"                # either side: session ends

# ---- Uplink error codes ----
ERR_UPLINK_NOT_AUTHORISED = "UPLINK_NOT_AUTHORISED"    # unknown client key
ERR_UPLINK_ORIGIN_DENIED = "UPLINK_ORIGIN_DENIED"      # may not carry data for that origin
ERR_UPLINK_STORE_FAILED = "UPLINK_STORE_FAILED"        # server could not store it; retry later
ERR_UPLINK_UNAVAILABLE = "UPLINK_UNAVAILABLE"          # not connected / timed out
ERR_UPLINK_BAD_MESSAGE = "UPLINK_BAD_MESSAGE"
