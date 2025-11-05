# queuectl/models.py
from datetime import datetime

# ISO timestamp like 2025-11-05T08:30:00Z
ISO = "%Y-%m-%dT%H:%M:%SZ"

def now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.utcnow().strftime(ISO)
