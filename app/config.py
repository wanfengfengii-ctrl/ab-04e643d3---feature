"""Application configuration loaded from environment variables.

Everything is overridable through the environment so tests and the
one-shot ``verify`` service can point at the same Dockerized deployment.
"""
from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Database connection
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = _int("DB_PORT", 5432)
DB_USER = os.environ.get("DB_USER", "eventsvc")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "eventsvc")
DB_NAME = os.environ.get("DB_NAME", "eventsvc")

# HTTP
API_PORT = _int("API_PORT", 8080)

# Feature / sizing limits
MAX_EVENTS_PER_BATCH = _int("MAX_EVENTS_PER_BATCH", 100)
MAX_BATCH_BYTES = _int("MAX_BATCH_BYTES", 1_048_576)  # 1 MiB canonical payload
MAX_EVENT_PAYLOAD_BYTES = _int("MAX_EVENT_PAYLOAD_BYTES", 65_536)
MAX_NAME_LENGTH = _int("MAX_NAME_LENGTH", 128)

# Retention / snapshot defaults
DEFAULT_SNAPSHOT_TTL_SECONDS = _int("DEFAULT_SNAPSHOT_TTL_SECONDS", 3600)
MAX_SNAPSHOT_TTL_SECONDS = _int("MAX_SNAPSHOT_TTL_SECONDS", 7 * 24 * 3600)
SNAPSHOT_TTL_GRACE_SECONDS = _int("SNAPSHOT_TTL_GRACE_SECONDS", 0)

# When there are no consumer groups, retention may reclaim data older than
# this position horizon. "reject" (default) means never delete without a
# group; "delete" uses RETENTION_NO_GROUP_HORIZON_SECONDS.
RETENTION_NO_GROUP_POLICY = os.environ.get("RETENTION_NO_GROUP_POLICY", "reject")
RETENTION_NO_GROUP_HORIZON_SECONDS = _int(
    "RETENTION_NO_GROUP_HORIZON_SECONDS", 86400
)

# Idle consumer groups are automatically invalidated (stop protecting data)
# after this many seconds of no ack activity. 0 disables idle expiry.
CONSUMER_GROUP_IDLE_TTL_SECONDS = _int("CONSUMER_GROUP_IDLE_TTL_SECONDS", 0)

# Deterministic fault injection. MUST be off in production.
# Accepts a comma separated list of fault points to arm:
#   crash-after-commit  : exit hard after tx commit is durable but before HTTP response
#   crash-during-reclaim: exit hard in the middle of a retention sweep
FAULT_POINTS = {
    p.strip() for p in os.environ.get("FAULT_POINTS", "").split(",") if p.strip()
}
# Token a client must echo to trigger a fault armed above (extra guard).
FAULT_TOKEN = os.environ.get("FAULT_TOKEN", "")

# Deterministic clock override for acceptance tests. Empty (default)
# disallows the X-Now header entirely; server UTC is used otherwise.
CLOCK_OVERRIDE_TOKEN = os.environ.get("CLOCK_OVERRIDE_TOKEN", "")


def dsn() -> str:
    return (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
