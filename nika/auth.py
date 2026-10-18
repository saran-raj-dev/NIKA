# auth.py
import time


class AuthManager:
    """
    Unified authentication manager for NIKA.
    Auth happens once during CLIENT_JOIN.
    """

    def __init__(self):
        # api_key -> identity
        self.valid_keys = {
            "nika-dev-key-123": {
                "client_id": "client-a",
                "role": "internal",
            },
            "nika-ops-key": {
                "client_id": "ops",
                "role": "admin",
            },
        }

        # zmq_client_id (bytes) -> auth context
        self._authenticated: dict[bytes, dict] = {}

    def authenticate(self, zmq_client_id: bytes, msg: dict):
        auth = msg.get("auth", {})
        api_key = auth.get("api_key")

        if not api_key:
            return False, "Missing API key"

        if api_key not in self.valid_keys:
            return False, "Invalid API key"

        identity = self.valid_keys[api_key]

        ctx = {
            "client_id": identity["client_id"],
            "role": identity["role"],
            "authenticated_at": time.time(),
        }

        self._authenticated[zmq_client_id] = ctx
        return True, ctx

    def is_authenticated(self, zmq_client_id: bytes) -> bool:
        return zmq_client_id in self._authenticated

    def get_context(self, zmq_client_id: bytes):
        return self._authenticated.get(zmq_client_id)

    def remove(self, zmq_client_id: bytes):
        self._authenticated.pop(zmq_client_id, None)
