# commands.py
import threading
import time

ACCEPT = "accepted"
DUPLICATE = "duplicate"
EXPIRED = "expired"


class CommandGuard:
    """
    Makes sure a command (writeback, config, schedule) runs at most once,
    and never after it expires.

    The SAME guard must see commands arriving directly over MQTT and those
    arriving through a relay — during handover both paths can deliver the
    same command_id.
    """

    def __init__(self, ttl: float = 3600.0):
        self.ttl = ttl
        self._seen: dict[str, float] = {}
        self._lock = threading.Lock()

    def check(self, command_id: str, expires_at: float | None = None) -> str:
        now = time.time()

        with self._lock:
            self._prune(now)

            if command_id in self._seen:
                return DUPLICATE

            if expires_at is not None and now > expires_at:
                # remembered too, so a late retry is also reported as expired
                self._seen[command_id] = now
                return EXPIRED

            self._seen[command_id] = now
            return ACCEPT

    def _prune(self, now: float):
        if len(self._seen) < 1024:
            return
        cutoff = now - self.ttl
        for cid, ts in list(self._seen.items()):
            if ts < cutoff:
                del self._seen[cid]
