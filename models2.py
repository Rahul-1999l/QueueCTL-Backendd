from datetime import datetime
ISO = "%Y-%m-%dT%H:%M:%SZ"
def now_iso() -> str:
    return datetime.utcnow().strftime(ISO)
