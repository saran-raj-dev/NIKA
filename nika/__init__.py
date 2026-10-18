"""NIKA: secure channel (NSC), gateway-platform uplink and LAN mesh relay over ZeroMQ.

    from nika.secure import StaticKey                 # NSC keys and handshake
    from nika.uplink import NikaUplinkClient, NikaUplinkServer
    from nika.mesh import MeshNode, MeshConfig

Subpackages are imported on demand, so `import nika` pulls in nothing heavy.
"""

__version__ = "1.0.0"
