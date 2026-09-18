"""PostgreSQL connection pool and idempotent schema initialization.

All cross-request concurrency control lives in the database (row locks,
partial unique indexes and a single global sequence); API processes hold
no authoritative in-memory state and any healthy instance can serve any
request.
"""
from __future__ import annotations

import asyncio

import asyncpg

from . import config

# Database-wide advisory lock used to give commits a global completion
# order equal to commitPosition order, and to serialize snapshot creation,
# processor claims/completions and retention sweeps against that order.
# Works across every API instance.
COMMIT_ORDER_LOCK = 0x45564E54  # arbitrary constant

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL
);

-- Single global, immutable, gap-tolerant commit ordering. Values are
-- allocated inside the committing transaction and never reused.
CREATE SEQUENCE IF NOT EXISTS commit_position_seq AS BIGINT;

CREATE TABLE IF NOT EXISTS producers (
    name        TEXT PRIMARY KEY,
    epoch       BIGINT NOT NULL CHECK (epoch > 0),
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    producer                TEXT NOT NULL,
    tx_id                   TEXT NOT NULL,
    epoch                   BIGINT NOT NULL,
    status                  TEXT NOT NULL CHECK (status IN ('open','committed','aborted','fenced')),
    first_sequence          BIGINT,
    event_count             INTEGER,
    batch_canonical         TEXT,
    batch_bytes             BIGINT,
    commit_position         BIGINT,
    committed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (producer, tx_id),
    UNIQUE (commit_position)
);

-- At most one *open* transaction per (producer, epoch). Fenced, aborted
-- and committed transactions are terminal and do not block new ones.
CREATE UNIQUE INDEX IF NOT EXISTS ux_open_tx_per_epoch
    ON transactions (producer, epoch)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS ix_tx_committed_seq
    ON transactions (producer, (first_sequence + event_count))
    WHERE status = 'committed';

CREATE TABLE IF NOT EXISTS events (
    commit_position BIGINT NOT NULL,
    ordinal         INTEGER NOT NULL,
    producer        TEXT NOT NULL,
    tx_id           TEXT NOT NULL,
    seq             BIGINT NOT NULL,
    stream          TEXT NOT NULL,
    event_key       TEXT NOT NULL,
    payload         JSONB NOT NULL,
    PRIMARY KEY (commit_position, ordinal),
    -- Whole-envelope integrity: events cannot outlive their transaction
    -- record. Deletions must target complete envelopes (application does
    -- events-first inside one transaction).
    CONSTRAINT fk_events_tx FOREIGN KEY (commit_position)
        REFERENCES transactions (commit_position) ON DELETE RESTRICT
);

-- Partial index covering FK joins / envelope lookups without a second
-- unique constraint (the transactions UNIQUE covers the referenced side).
CREATE INDEX IF NOT EXISTS ix_events_stream_pos
    ON events (stream, commit_position);

CREATE TABLE IF NOT EXISTS snapshots (
    id              UUID PRIMARY KEY,
    streams         TEXT[] NOT NULL,
    high_watermark  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_groups (
    name          TEXT PRIMARY KEY,
    ack_position  BIGINT NOT NULL DEFAULT 0 CHECK (ack_position >= 0),
    created_at    TIMESTAMPTZ NOT NULL,
    last_ack_at   TIMESTAMPTZ
);

-- Persistent derived processors. Semantics-affecting configuration is
-- immutable after creation (enforced by the API; only status, checkpoint
-- and lease columns ever change).
CREATE TABLE IF NOT EXISTS processors (
    id              TEXT PRIMARY KEY,
    input_streams   TEXT[] NOT NULL,
    output_streams  TEXT[] NOT NULL,
    batch_size      INTEGER NOT NULL CHECK (batch_size BETWEEN 1 AND 100),
    lease_seconds   INTEGER NOT NULL,
    start_position  BIGINT NOT NULL CHECK (start_position >= 0),
    status          TEXT NOT NULL CHECK (status IN ('active','paused','deleted')),
    -- Everything up to checkpoint_position has been processed durably
    -- (zero-output conclusions included).
    checkpoint_position BIGINT NOT NULL DEFAULT 0 CHECK (checkpoint_position >= 0),
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    -- The single outstanding work unit (NULL while idle). generation
    -- increases monotonically on every claim/takeover.
    lease_id        UUID,
    generation      BIGINT NOT NULL DEFAULT 0,
    lease_from      BIGINT,
    lease_through   BIGINT,
    lease_expires_at TIMESTAMPTZ,
    -- Normalized digest (sha256 hex) of the claimed source range contents.
    source_digest   TEXT
);

-- Completion idempotency: every accepted result is keyed by the client
-- supplied resultId, recording the exact work identity and canonical
-- content, so retries return the original outcome.
CREATE TABLE IF NOT EXISTS processor_results (
    processor_id    TEXT NOT NULL,
    result_id       TEXT NOT NULL,
    generation      BIGINT NOT NULL,
    from_position   BIGINT NOT NULL,
    through_position BIGINT NOT NULL,
    source_digest   TEXT NOT NULL,
    content_digest  TEXT NOT NULL,
    event_count     INTEGER NOT NULL,
    derived_commit_position BIGINT,
    completed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (processor_id, result_id)
);

INSERT INTO settings (key, value)
VALUES ('schema_version', '2')
ON CONFLICT (key) DO UPDATE SET value = '2'
WHERE settings.value = '1';
"""

_pool: asyncpg.Pool | None = None


async def create_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        last_err: Exception | None = None
        for attempt in range(60):
            try:
                pool = await asyncpg.create_pool(
                    host=config.DB_HOST,
                    port=config.DB_PORT,
                    user=config.DB_USER,
                    password=config.DB_PASSWORD,
                    database=config.DB_NAME,
                    min_size=2,
                    max_size=20,
                    command_timeout=30,
                )
                break
            except (OSError, asyncpg.PostgresError) as exc:  # db may still start
                last_err = exc
                await asyncio.sleep(1)
        else:  # pragma: no cover - startup path
            raise RuntimeError(f"database unreachable: {last_err}")

        async with pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)
        _pool = pool
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def pool() -> asyncpg.Pool:
    assert _pool is not None, "pool not initialized"
    return _pool


async def readiness() -> bool:
    try:
        p = await create_pool()
        async with p.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception:
        return False
