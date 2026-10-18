# envelope.py
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

NIKA_VERSION = "1.0"

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def envelope(
    *,
    msg_type: str,
    payload: Dict[str, Any] | None = None,
    msg_id: str | None = None,
    sender: str,
) -> Dict[str, Any]:
    return {
        "nika": NIKA_VERSION,
        "type": msg_type,
        "id": msg_id or str(uuid.uuid4()),
        "ts": now_iso(),
        "sender": sender,
        "payload": payload or {},
    }
