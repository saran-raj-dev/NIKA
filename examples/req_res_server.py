"""
NIKA v0.1 — REQ / RES Server Example

This example shows:
- How to start a NIKA server
- How REQ / RES commands are handled
- How command whitelisting works
- Where business logic belongs

IMPORTANT:
- REQ / RES is the CONTROL PLANE
- Use it for commands, queries, metadata
- NOT for streaming or events
"""

from api.server import NikaServer


def main():
    # ------------------------------------
    # Create server
    # ------------------------------------
    server = NikaServer()

    print("\nREQ / RES server is running.")
    print("Allowed commands:")
    print("  - PING")
    print("  - GET_STATUS")
    print("  - GET_STREAMS")
    print("\nWaiting for clients...\n")

    # ------------------------------------
    # Start blocking server loop
    # ------------------------------------
    server.start()


if __name__ == "__main__":
    main()
