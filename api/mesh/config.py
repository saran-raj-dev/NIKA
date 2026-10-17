# config.py
from dataclasses import dataclass, field


@dataclass
class MeshConfig:
    """
    Settings for one mesh node.

    Every node on a site shares `mesh_key`; frames without a valid MAC are
    dropped. Timers are in seconds.
    """

    node_id: str
    mesh_key: bytes

    # ---- Transport ----
    bind_host: str = "0.0.0.0"
    mesh_port: int = 47801               # ZeroMQ ROUTER for relay sessions
    advertise_host: str | None = None    # None -> peers use the beacon's source IP

    # ---- Discovery ----
    discovery_port: int = 47800          # UDP beacons
    broadcast_addrs: list[str] = field(default_factory=lambda: ["255.255.255.255"])
    static_peers: list[str] = field(default_factory=list)  # "host:udp_port"
    allowed_peers: set[str] | None = None  # None -> any node holding mesh_key
    beacon_interval: float = 1.0
    peer_timeout: float = 3.5

    # ---- Relay (serving others) ----
    relay_enabled: bool = True
    relay_capacity: int = 4              # concurrent origins this node will carry
    relay_rate_limit: float = 200.0      # msgs/sec per origin; priority 0 bypasses
    session_timeout: float = 10.0        # drop an origin that goes quiet

    # ---- Origin (being carried) ----
    keepalive_interval: float = 2.0
    relay_timeout: float = 6.0           # no PONG for this long -> fail over
    open_timeout: float = 3.0
    recover_hold: float = 5.0            # uplink must stay up this long before RELAY_CLOSE
    relay_cooldown: float = 30.0         # don't reselect a relay that just failed us
    preferred_relays: list[str] = field(default_factory=list)  # tried first, in order
    ack_timeout: float = 3.0
    max_attempts: int = 3
    max_inflight: int = 256
    queue_timeout: float = 30.0          # give a message back if no relay carries it in time

    # ---- Commands ----
    command_dedupe_ttl: float = 3600.0
    downlink_retry: float = 1.0

    # ---- Security ----
    max_clock_skew: float = 120.0        # reject frames older/newer than this

    def __post_init__(self):
        if not self.node_id:
            raise ValueError("node_id is required")
        if isinstance(self.mesh_key, str):
            self.mesh_key = self.mesh_key.encode()
        if len(self.mesh_key) < 16:
            raise ValueError("mesh_key must be at least 16 bytes")
