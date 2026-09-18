"""Versioned HTTP JSON API for the multi-stream experimental event service.

All endpoints are served under ``/v1``. Durable state lives exclusively in
PostgreSQL; this process is stateless and may be killed at any moment.
Concurrency control uses database row locks, a single global sequence and
partial unique indexes -- never in-process mutexes.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import config, db
from .validation import (
    EPOCH_MAX,
    EPOCH_MIN,
    PAGE_SIZE_MAX,
    PAGE_SIZE_MIN,
    POSITION_MAX,
    POSITION_MIN,
    SEQUENCE_MAX,
    SEQUENCE_MIN,
    TTL_MIN,
    ValidationError,
    canonical_json,
    parse_json_strict,
    require_int,
    require_name,
    require_object,
    require_string_list,
    validate_payload,
)

log = logging.getLogger("eventsvc")
UTC = timezone.utc

# Database-wide advisory lock used to give commits a global completion
# order equal to commitPosition order, and to serialize snapshot creation
# and retention sweeps against that order. Works across every API instance.
COMMIT_ORDER_LOCK = 0x45564E54  # arbitrary constant


# ---------------------------------------------------------------------------
# Time (server UTC, deterministically injectable for tests)
# ---------------------------------------------------------------------------


def current_time(request: Request) -> datetime:
    override = request.headers.get("x-now")
    if override is not None:
        if not config.CLOCK_OVERRIDE_TOKEN:
            raise ApiError(400, "CLOCK_OVERRIDE_DISABLED", "clock override is not enabled")
        supplied = request.headers.get("x-now-token", "")
        if not hmac.compare_digest(supplied, config.CLOCK_OVERRIDE_TOKEN):
            raise ApiError(403, "CLOCK_OVERRIDE_FORBIDDEN", "invalid clock override token")
        try:
            ts = float(override)
        except ValueError as exc:
            raise ApiError(400, "INVALID_TIME", "X-Now must be POSIX seconds") from exc
        if not (0 <= ts <= 253402300800):
            raise ApiError(400, "INVALID_TIME", "X-Now out of range")
        return datetime.fromtimestamp(ts, tz=UTC)
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Error model
# ---------------------------------------------------------------------------


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def error_body(code: str, message: str, details: dict | None = None) -> dict:
    body: dict[str, Any] = {"error": {"code": code, "message": message}}
    if details:
        body["error"]["details"] = details
    return body


# ---------------------------------------------------------------------------
# Opaque, tamper-evident cursor (key persisted in DB, shared by all instances)
# ---------------------------------------------------------------------------

_CURSOR_KEY: bytes = b""


async def ensure_cursor_key(conn: asyncpg.Connection) -> None:
    global _CURSOR_KEY
    row = await conn.fetchrow("SELECT value FROM settings WHERE key = 'cursor_key'")
    if row is None:
        key = base64.urlsafe_b64encode(os.urandom(32)).decode()
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('cursor_key', $1) "
            "ON CONFLICT (key) DO NOTHING",
            key,
        )
        row = await conn.fetchrow("SELECT value FROM settings WHERE key = 'cursor_key'")
    _CURSOR_KEY = row["value"].encode()


def encode_cursor(payload: dict) -> str:
    raw = canonical_json(payload).encode("utf-8")
    sig = hmac.new(_CURSOR_KEY, raw, hashlib.sha256).digest()  # fixed 32 bytes
    # Fixed-length suffix: the HMAC is exactly 32 bytes and may itself
    # contain any byte (including '.'), so never split on a separator.
    return base64.urlsafe_b64encode(raw + sig).decode("ascii")


def decode_cursor(token: str) -> dict:
    try:
        blob = base64.urlsafe_b64decode(token.encode("ascii"))
        if len(blob) <= 32:
            raise ValueError("token too short")
        raw, sig = blob[:-32], blob[-32:]
    except Exception as exc:
        raise ApiError(400, "CURSOR_INVALID", "cursor is malformed") from exc
    expected = hmac.new(_CURSOR_KEY, raw, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise ApiError(400, "CURSOR_INVALID", "cursor signature mismatch")
    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise ApiError(400, "CURSOR_INVALID", "cursor payload unreadable") from exc
    required = {"snapshotId", "streams", "nextPosition", "v"}
    if not required.issubset(payload) or not isinstance(payload["streams"], list):
        raise ApiError(400, "CURSOR_INVALID", "cursor missing fields")
    if not payload["streams"] or not all(isinstance(s, str) for s in payload["streams"]):
        raise ApiError(400, "CURSOR_INVALID", "cursor stream set invalid")
    if not isinstance(payload["nextPosition"], int) or payload["nextPosition"] < 0:
        raise ApiError(400, "CURSOR_INVALID", "cursor position invalid")
    return payload


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def out_tx(row: asyncpg.Record) -> dict:
    return {
        "producer": row["producer"],
        "txId": row["tx_id"],
        "epoch": row["epoch"],
        "status": row["status"],
        "firstSequence": row["first_sequence"],
        "eventCount": row["event_count"],
        "commitPosition": row["commit_position"],
        "committedAt": row["committed_at"].isoformat() if row["committed_at"] else None,
    }


async def body_json(request: Request) -> Any:
    raw = await request.body()
    try:
        return parse_json_strict(raw)
    except ValidationError as exc:
        raise ApiError(400, "INVALID_JSON", exc.message)


async def get_producer(conn: asyncpg.Connection, producer: str) -> asyncpg.Record:
    row = await conn.fetchrow("SELECT * FROM producers WHERE name = $1", producer)
    if row is None:
        raise ApiError(404, "PRODUCER_NOT_FOUND", f"producer {producer!r} does not exist")
    return row


def check_current_epoch(request_epoch: int, producer_row: asyncpg.Record) -> None:
    if request_epoch < producer_row["epoch"]:
        raise ApiError(
            409, "FENCED",
            f"epoch {request_epoch} is fenced; current epoch is {producer_row['epoch']}",
            {"currentEpoch": producer_row["epoch"], "requestEpoch": request_epoch},
        )
    if request_epoch > producer_row["epoch"]:
        raise ApiError(
            409, "EPOCH_NOT_CURRENT",
            f"epoch {request_epoch} has not been registered; "
            f"current epoch is {producer_row['epoch']}",
            {"currentEpoch": producer_row["epoch"], "requestEpoch": request_epoch},
        )


def parse_query_int(request: Request, name: str, default: int, minimum: int, maximum: int) -> int:
    raw = request.query_params.get(name)
    if raw is None:
        return default
    # strict: no decimals/exponents/bools sneaking through
    if isinstance(raw, str) and not raw.lstrip("-").isdigit():
        raise ApiError(400, "VALIDATION_ERROR", f"{name} must be an integer", {"field": name})
    return require_int(int(raw), name, minimum, maximum)


async def expire_idle_groups(conn: asyncpg.Connection, now: datetime) -> None:
    """Durable idle invalidation: recomputed from timestamps, no in-memory state."""
    if config.CONSUMER_GROUP_IDLE_TTL_SECONDS > 0:
        cutoff = now - timedelta(seconds=config.CONSUMER_GROUP_IDLE_TTL_SECONDS)
        await conn.execute(
            "DELETE FROM consumer_groups "
            "WHERE last_ack_at IS NOT NULL AND last_ack_at < $1",
            cutoff,
        )


async def earliest_available(conn: asyncpg.Connection) -> int:
    return await conn.fetchval("SELECT COALESCE(MIN(commit_position), 0) FROM events")


def canonicalize_events(events_raw: Any) -> tuple[list[dict], str, int]:
    if not isinstance(events_raw, list) or not (
        1 <= len(events_raw) <= config.MAX_EVENTS_PER_BATCH
    ):
        raise ValidationError(
            f"events must contain 1-{config.MAX_EVENTS_PER_BATCH} items", "events"
        )
    events: list[dict] = []
    total_bytes = 0
    for i, ev in enumerate(events_raw):
        if not isinstance(ev, dict):
            raise ValidationError(f"events[{i}] must be an object", f"events[{i}]")
        stream = require_name(ev.get("stream"), f"events[{i}].stream")
        event_key = require_name(ev.get("key"), f"events[{i}].key")
        if "payload" not in ev:
            raise ValidationError(f"events[{i}].payload is required", f"events[{i}].payload")
        payload_text = validate_payload(ev["payload"], f"events[{i}].payload")
        nbytes = len(payload_text.encode("utf-8"))
        if nbytes > config.MAX_EVENT_PAYLOAD_BYTES:
            raise ApiError(
                413, "BATCH_TOO_LARGE",
                f"events[{i}].payload exceeds {config.MAX_EVENT_PAYLOAD_BYTES} bytes",
            )
        total_bytes += nbytes
        if total_bytes > config.MAX_BATCH_BYTES:
            raise ApiError(
                413, "BATCH_TOO_LARGE",
                f"batch exceeds {config.MAX_BATCH_BYTES} canonical bytes",
                {"maxBatchBytes": config.MAX_BATCH_BYTES},
            )
        events.append({"stream": stream, "key": event_key, "payload": payload_text})
    # Fingerprint of the *content* only; producer/txId/epoch bind the row and
    # firstSequence is positional state.
    fingerprint = [
        [e["stream"], e["key"], json.loads(e["payload"])] for e in events
    ]
    return events, canonical_json({"events": fingerprint}), total_bytes


async def read_page(
    conn: asyncpg.Connection,
    streams: list[str],
    start: int,
    high_watermark: int,
    page_size: int,
) -> tuple[list[int], list[dict]]:
    """Return up to page_size whole transaction envelopes in (start, hw]."""
    pos_rows = await conn.fetch(
        "SELECT DISTINCT commit_position FROM events "
        "WHERE stream = ANY($1::text[]) AND commit_position > $2 "
        "AND commit_position <= $3 ORDER BY commit_position LIMIT $4",
        streams, start, high_watermark, page_size,
    )
    positions = [r["commit_position"] for r in pos_rows]
    if not positions:
        return [], []

    rows = await conn.fetch(
        "SELECT e.commit_position AS cp, e.ordinal, e.producer, e.tx_id, e.seq, "
        "       e.stream, e.event_key, e.payload, "
        "       t.epoch, t.first_sequence, t.committed_at "
        "FROM events e JOIN transactions t ON t.commit_position = e.commit_position "
        "WHERE e.commit_position = ANY($1::bigint[]) AND e.stream = ANY($2::text[]) "
        "ORDER BY e.commit_position, e.ordinal",
        positions, streams,
    )
    grouped: dict[int, dict] = {}
    envelopes: list[dict] = []
    for r in rows:
        env = grouped.get(r["cp"])
        if env is None:
            env = {
                "commitPosition": r["cp"],
                "producer": r["producer"],
                "txId": r["tx_id"],
                "epoch": r["epoch"],
                "firstSequence": r["first_sequence"],
                "committedAt": r["committed_at"].isoformat(),
                "events": [],
            }
            grouped[r["cp"]] = env
            envelopes.append(env)
        env["events"].append({
            "stream": r["stream"],
            "key": r["event_key"],
            "sequence": r["seq"],
            "payload": json.loads(r["payload"]),
        })
    return positions, envelopes


# ---------------------------------------------------------------------------
# Application wiring
# ---------------------------------------------------------------------------

app = FastAPI(title="Experiment Event Service", version="1.0.0", docs_url=None, redoc_url=None)


@app.on_event("startup")
async def _startup() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    pool = await db.create_pool()
    async with pool.acquire() as conn:
        await ensure_cursor_key(conn)


@app.on_event("shutdown")
async def _shutdown() -> None:
    await db.close_pool()


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status,
                        content=error_body(exc.code, exc.message, exc.details))


@app.exception_handler(ValidationError)
async def validation_handler(request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content=error_body(
            "VALIDATION_ERROR", exc.message, {"field": exc.field} if exc.field else None
        ),
    )


@app.get("/health/live")
async def live() -> dict:
    return {"status": "alive", "time": datetime.now(UTC).isoformat()}


@app.get("/health/ready")
async def ready() -> JSONResponse:
    ok = await db.readiness()
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ready" if ok else "degraded",
                 "database": "up" if ok else "down"},
    )


@app.get("/v1/config")
async def get_config() -> dict:
    return {
        "maxEventsPerBatch": config.MAX_EVENTS_PER_BATCH,
        "maxBatchBytes": config.MAX_BATCH_BYTES,
        "maxEventPayloadBytes": config.MAX_EVENT_PAYLOAD_BYTES,
        "defaultSnapshotTtlSeconds": config.DEFAULT_SNAPSHOT_TTL_SECONDS,
        "maxSnapshotTtlSeconds": config.MAX_SNAPSHOT_TTL_SECONDS,
        "retentionNoGroupPolicy": config.RETENTION_NO_GROUP_POLICY,
        "retentionNoGroupHorizonSeconds": config.RETENTION_NO_GROUP_HORIZON_SECONDS,
        "consumerGroupIdleTtlSeconds": config.CONSUMER_GROUP_IDLE_TTL_SECONDS,
        "faultInjectionEnabled": bool(config.FAULT_POINTS),
    }


# ---------------------------------------------------------------------------
# Producer sessions (monotonic epoch fencing)
# ---------------------------------------------------------------------------


@app.post("/v1/producers")
async def create_producer(request: Request) -> JSONResponse:
    obj = require_object(await body_json(request))
    name = require_name(obj.get("name"), "name")
    epoch = require_int(obj.get("epoch"), "epoch", EPOCH_MIN, EPOCH_MAX)
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM producers WHERE name = $1 FOR UPDATE", name
            )
            if row is None:
                await conn.execute(
                    "INSERT INTO producers (name, epoch, created_at, updated_at) "
                    "VALUES ($1, $2, $3, $3)",
                    name, epoch, now,
                )
                status, current = "created", epoch
            else:
                current = row["epoch"]
                if epoch < current:
                    raise ApiError(
                        409, "EPOCH_NOT_CURRENT",
                        f"epoch {epoch} is lower than current epoch {current}",
                        {"currentEpoch": current, "requestEpoch": epoch},
                    )
                if epoch == current:
                    status = "already_current"
                else:
                    await conn.execute(
                        "UPDATE producers SET epoch = $2, updated_at = $3 WHERE name = $1",
                        name, epoch, now,
                    )
                    # Lower-epoch open transactions lose commit eligibility now.
                    await conn.execute(
                        "UPDATE transactions SET status = 'fenced', updated_at = $3 "
                        "WHERE producer = $1 AND status = 'open' AND epoch < $2",
                        name, epoch, now,
                    )
                    status, current = "fenced_previous", epoch
    return JSONResponse(
        status_code=201 if status == "created" else 200,
        content={"producer": name, "epoch": current, "status": status},
    )


@app.get("/v1/producers/{name}")
async def get_producer_session(name: str) -> JSONResponse:
    name = require_name(name, "name")
    async with db.pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM producers WHERE name = $1", name)
    if row is None:
        raise ApiError(404, "PRODUCER_NOT_FOUND", f"producer {name!r} does not exist")
    return JSONResponse(status_code=200, content={
        "producer": row["name"], "epoch": row["epoch"],
        "createdAt": row["created_at"].isoformat(),
        "updatedAt": row["updated_at"].isoformat(),
    })


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


@app.post("/v1/transactions")
async def create_transaction(request: Request) -> JSONResponse:
    obj = require_object(await body_json(request))
    producer = require_name(obj.get("producer"), "producer")
    tx_id = require_name(obj.get("txId"), "txId")
    epoch = require_int(obj.get("epoch"), "epoch", EPOCH_MIN, EPOCH_MAX)
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            prow = await get_producer(conn, producer)
            check_current_epoch(epoch, prow)
            existing = await conn.fetchrow(
                "SELECT * FROM transactions WHERE producer = $1 AND tx_id = $2",
                producer, tx_id,
            )
            if existing is not None:
                if existing["epoch"] != epoch:
                    raise ApiError(
                        409, "TX_ID_REUSED",
                        f"txId {tx_id!r} already exists under epoch {existing['epoch']}",
                        {"existingEpoch": existing["epoch"],
                         "existingStatus": existing["status"]},
                    )
                return JSONResponse(status_code=200, content=out_tx(existing))
            try:
                await conn.execute(
                    "INSERT INTO transactions "
                    "(producer, tx_id, epoch, status, created_at, updated_at) "
                    "VALUES ($1,$2,$3,'open',$4,$4)",
                    producer, tx_id, epoch, now,
                )
            except asyncpg.UniqueViolationError as exc:
                raise ApiError(
                    409, "OPEN_TRANSACTION_EXISTS",
                    f"producer {producer!r} already has an open transaction in epoch {epoch}",
                ) from exc
    return JSONResponse(status_code=201, content={
        "producer": producer, "txId": tx_id, "epoch": epoch, "status": "open",
        "firstSequence": None, "eventCount": 0,
        "commitPosition": None, "committedAt": None,
    })


@app.put("/v1/transactions/{producer}/{tx_id}/batch")
async def write_batch(producer: str, tx_id: str, request: Request) -> JSONResponse:
    producer = require_name(producer, "producer")
    tx_id = require_name(tx_id, "txId")
    obj = require_object(await body_json(request))
    epoch = require_int(obj.get("epoch"), "epoch", EPOCH_MIN, EPOCH_MAX)
    first_sequence = require_int(
        obj.get("firstSequence"), "firstSequence", SEQUENCE_MIN, SEQUENCE_MAX
    )
    _, canonical, total_bytes = canonicalize_events(obj.get("events"))
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            prow = await get_producer(conn, producer)
            tx = await conn.fetchrow(
                "SELECT * FROM transactions WHERE producer=$1 AND tx_id=$2 FOR UPDATE",
                producer, tx_id,
            )
            if tx is None:
                raise ApiError(404, "TRANSACTION_NOT_FOUND", "create the transaction first")

            if tx["status"] == "fenced":
                # Old-epoch writes after fencing: stable FENCED, never overwrite.
                raise ApiError(
                    409, "FENCED",
                    "transaction belongs to a fenced epoch; writes are rejected",
                    {"currentEpoch": prow["epoch"], "requestEpoch": epoch},
                )
            if tx["status"] in ("committed", "aborted"):
                # Identity first: an exact retry of the stored batch (same
                # canonical content AND same firstSequence) returns the same
                # transaction; any difference at all is TX_ID_REUSED.
                if (tx["batch_canonical"] == canonical
                        and tx["first_sequence"] == first_sequence):
                    return JSONResponse(status_code=200, content=out_tx(tx))
                raise ApiError(
                    409, "TX_ID_REUSED",
                    f"transaction is already {tx['status']} with different content",
                    {"status": tx["status"]},
                )

            # open
            check_current_epoch(epoch, prow)
            if tx["batch_canonical"] is not None:
                if tx["batch_canonical"] == canonical and tx["first_sequence"] == first_sequence:
                    return JSONResponse(status_code=200, content=out_tx(tx))
                raise ApiError(409, "TX_ID_REUSED", "txId already carries a different batch")

            # The next admissible firstSequence is one past the final event
            # of the latest committed batch. first + count is already the
            # "next" sequence; with no commits the first sequence is 1.
            next_seq = await conn.fetchval(
                "SELECT COALESCE(MAX(first_sequence + event_count), 1) "
                "FROM transactions WHERE producer=$1 AND status='committed'",
                producer,
            )
            if first_sequence != next_seq:
                raise ApiError(
                    409, "INVALID_FIRST_SEQUENCE",
                    f"firstSequence must be {next_seq}",
                    {"expectedFirstSequence": next_seq,
                     "lastCommittedSequence": next_seq - 1},
                )
            event_count = len(json.loads(canonical)["events"])
            await conn.execute(
                "UPDATE transactions SET first_sequence=$3, event_count=$4, "
                "batch_canonical=$5, batch_bytes=$6, updated_at=$7 "
                "WHERE producer=$1 AND tx_id=$2",
                producer, tx_id, first_sequence, event_count,
                canonical, total_bytes, now,
            )
    return JSONResponse(status_code=200, content={
        "producer": producer, "txId": tx_id, "epoch": epoch, "status": "open",
        "firstSequence": first_sequence, "eventCount": event_count,
        "commitPosition": None, "committedAt": None,
    })


@app.post("/v1/transactions/{producer}/{tx_id}/commit")
async def commit_transaction(producer: str, tx_id: str, request: Request) -> JSONResponse:
    producer = require_name(producer, "producer")
    tx_id = require_name(tx_id, "txId")
    obj = require_object(await body_json(request))
    epoch = require_int(obj.get("epoch"), "epoch", EPOCH_MIN, EPOCH_MAX)
    fault_token = obj.get("faultToken")
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            prow = await get_producer(conn, producer)
            tx = await conn.fetchrow(
                "SELECT * FROM transactions WHERE producer=$1 AND tx_id=$2 FOR UPDATE",
                producer, tx_id,
            )
            if tx is None:
                raise ApiError(404, "TRANSACTION_NOT_FOUND", "transaction does not exist")

            # Identity first: terminal results are always recognizable by
            # txId even after a newer epoch took over. A committed retry
            # must never be misreported as FENCED.
            if tx["status"] == "committed":
                return JSONResponse(status_code=200, content=out_tx(tx))
            if tx["status"] == "aborted":
                raise ApiError(
                    409, "TERMINAL_CONFLICT", "transaction already aborted",
                    {"status": "aborted", "commitPosition": None},
                )
            if tx["status"] == "fenced":
                raise ApiError(
                    409, "FENCED",
                    "transaction was fenced by a newer epoch before commit",
                    {"currentEpoch": prow["epoch"], "requestEpoch": epoch},
                )

            if tx["batch_canonical"] is None:
                raise ApiError(409, "BATCH_NOT_WRITTEN", "write the batch before committing")
            check_current_epoch(epoch, prow)

            # Global serialization point: snapshot watermark acquisition and
            # retention sweeps take the same lock, making the global order
            # linearizable across every API process.
            await conn.fetchval("SELECT pg_advisory_xact_lock($1)", COMMIT_ORDER_LOCK)

            commit_pos = await conn.fetchval("SELECT nextval('commit_position_seq')")
            # Publish the envelope's position first so the events FK (which
            # references transactions.commit_position) is satisfiable; all of
            # this is still one atomic transaction.
            await conn.execute(
                "UPDATE transactions SET status='committed', commit_position=$3, "
                "committed_at=$4, updated_at=$4 WHERE producer=$1 AND tx_id=$2",
                producer, tx_id, commit_pos, now,
            )
            batch_doc = json.loads(tx["batch_canonical"])
            rows = []
            for ordinal, ev in enumerate(batch_doc["events"]):
                stream, key, payload = ev
                rows.append((
                    commit_pos, ordinal, producer, tx_id,
                    tx["first_sequence"] + ordinal,
                    stream, key,
                    json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                ))
            await conn.executemany(
                "INSERT INTO events "
                "(commit_position, ordinal, producer, tx_id, seq, stream, event_key, payload) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",
                rows,
            )
        # COMMIT has returned: durable. The HTTP response has not been sent.

    if "crash-after-commit" in config.FAULT_POINTS and (
        not config.FAULT_TOKEN or fault_token == config.FAULT_TOKEN
    ):
        log.error("FAULT crash-after-commit firing at commitPosition=%s", commit_pos)
        os._exit(137)

    return JSONResponse(status_code=200, content={
        "producer": producer, "txId": tx_id, "epoch": tx["epoch"],
        "status": "committed", "firstSequence": tx["first_sequence"],
        "eventCount": tx["event_count"], "commitPosition": commit_pos,
        "committedAt": now.isoformat(),
    })


@app.post("/v1/transactions/{producer}/{tx_id}/abort")
async def abort_transaction(producer: str, tx_id: str, request: Request) -> JSONResponse:
    producer = require_name(producer, "producer")
    tx_id = require_name(tx_id, "txId")
    obj = require_object(await body_json(request))
    epoch = require_int(obj.get("epoch"), "epoch", EPOCH_MIN, EPOCH_MAX)
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            prow = await get_producer(conn, producer)
            tx = await conn.fetchrow(
                "SELECT * FROM transactions WHERE producer=$1 AND tx_id=$2 FOR UPDATE",
                producer, tx_id,
            )
            if tx is None:
                raise ApiError(404, "TRANSACTION_NOT_FOUND", "transaction does not exist")
            if tx["status"] == "aborted":
                return JSONResponse(status_code=200, content=out_tx(tx))
            if tx["status"] == "committed":
                raise ApiError(
                    409, "TERMINAL_CONFLICT",
                    "transaction already committed; abort lost the race",
                    {"status": "committed", "commitPosition": tx["commit_position"]},
                )
            if tx["status"] == "fenced":
                # Fenced is itself terminal: the abort is acknowledged
                # idempotently, but the status stays "fenced" so a stale
                # agent's first-time commit can never be anything other
                # than the stable FENCED error.
                return JSONResponse(status_code=200, content=out_tx(tx))

            check_current_epoch(epoch, prow)
            await conn.execute(
                "UPDATE transactions SET status='aborted', updated_at=$3 "
                "WHERE producer=$1 AND tx_id=$2",
                producer, tx_id, now,
            )
            tx = await conn.fetchrow(
                "SELECT * FROM transactions WHERE producer=$1 AND tx_id=$2",
                producer, tx_id,
            )
    return JSONResponse(status_code=200, content=out_tx(tx))


@app.get("/v1/transactions/{producer}/{tx_id}")
async def get_transaction(producer: str, tx_id: str) -> JSONResponse:
    producer = require_name(producer, "producer")
    tx_id = require_name(tx_id, "txId")
    async with db.pool().acquire() as conn:
        tx = await conn.fetchrow(
            "SELECT * FROM transactions WHERE producer=$1 AND tx_id=$2",
            producer, tx_id,
        )
    if tx is None:
        raise ApiError(404, "TRANSACTION_NOT_FOUND", "transaction does not exist")
    return JSONResponse(status_code=200, content=out_tx(tx))


# ---------------------------------------------------------------------------
# Read snapshots and cursor pagination
# ---------------------------------------------------------------------------


async def load_snapshot(conn: asyncpg.Connection, snapshot_id: str, now: datetime):
    try:
        sid = uuid.UUID(snapshot_id)
    except ValueError as exc:
        raise ApiError(400, "CURSOR_INVALID", "snapshot id is not a UUID") from exc
    snap = await conn.fetchrow("SELECT * FROM snapshots WHERE id=$1", sid)
    if snap is None or now >= snap["expires_at"]:
        earliest = await earliest_available(conn)
        raise ApiError(
            410, "CURSOR_EXPIRED",
            "snapshot does not exist or its TTL has expired",
            {
                "earliestAvailablePosition": earliest,
                "snapshotExpiresAt": snap["expires_at"].isoformat() if snap else None,
                "createSnapshot": {
                    "method": "POST",
                    "path": "/v1/snapshots",
                    "body": {"streams": ["<stream-name>"],
                             "ttlSeconds": config.DEFAULT_SNAPSHOT_TTL_SECONDS,
                             "pageSize": 100},
                },
            },
        )
    return snap


def build_cursor(snapshot_id: str, streams: list[str], next_position: int) -> str:
    return encode_cursor({
        "v": 1, "snapshotId": snapshot_id,
        "streams": streams, "nextPosition": next_position,
    })


@app.delete("/v1/snapshots/{snapshot_id}")
async def delete_snapshot(snapshot_id: str) -> JSONResponse:
    """Admin: explicitly release a snapshot before its TTL expires."""
    try:
        sid = uuid.UUID(snapshot_id)
    except ValueError:
        return JSONResponse(status_code=404, content={"snapshotId": snapshot_id,
                                                       "deleted": False})
    async with db.pool().acquire() as conn:
        result = await conn.execute("DELETE FROM snapshots WHERE id=$1", sid)
    deleted = int(result.split()[-1])
    return JSONResponse(status_code=200 if deleted else 404, content={
        "snapshotId": snapshot_id, "deleted": bool(deleted),
    })


@app.post("/v1/snapshots")
async def create_snapshot(request: Request) -> JSONResponse:
    obj = require_object(await body_json(request))
    streams = require_string_list(obj.get("streams"), "streams")
    ttl = require_int(
        obj.get("ttlSeconds", config.DEFAULT_SNAPSHOT_TTL_SECONDS),
        "ttlSeconds", TTL_MIN, config.MAX_SNAPSHOT_TTL_SECONDS,
    )
    page_size = require_int(
        obj.get("pageSize", 100), "pageSize", PAGE_SIZE_MIN, PAGE_SIZE_MAX
    )
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            # Same global lock as commits: the immutable high watermark can
            # never race a concurrent commit into ambiguity. MAX over
            # committed envelopes while holding the commit-ordering lock is
            # exactly "everything durable so far".
            await conn.fetchval("SELECT pg_advisory_xact_lock($1)", COMMIT_ORDER_LOCK)
            high_watermark = await conn.fetchval(
                "SELECT COALESCE(MAX(commit_position), 0) FROM transactions "
                "WHERE status = 'committed'"
            )
            sid = uuid.uuid4()
            expires = now + timedelta(seconds=ttl)
            await conn.execute(
                "INSERT INTO snapshots (id, streams, high_watermark, created_at, expires_at) "
                "VALUES ($1,$2,$3,$4,$5)",
                sid, streams, high_watermark, now, expires,
            )
            positions, envelopes = await read_page(conn, streams, 0, high_watermark, page_size)
        next_pos = positions[-1] if positions else 0
        has_more = len(positions) == page_size and next_pos < high_watermark
        cursor = build_cursor(str(sid), streams, next_pos)
    return JSONResponse(status_code=201, content={
        "snapshotId": str(sid),
        "streams": streams,
        "highWatermark": high_watermark,
        "createdAt": now.isoformat(),
        "expiresAt": expires.isoformat(),
        "pageSize": page_size,
        "page": {
            "transactions": envelopes,
            "transactionCount": len(envelopes),
            "nextCursor": cursor,
            "hasMore": has_more,
        },
    })


@app.get("/v1/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str, request: Request) -> JSONResponse:
    now = current_time(request)
    try:
        sid = uuid.UUID(snapshot_id)
    except ValueError as exc:
        raise ApiError(404, "SNAPSHOT_NOT_FOUND", "snapshot does not exist") from exc
    async with db.pool().acquire() as conn:
        snap = await conn.fetchrow("SELECT * FROM snapshots WHERE id=$1", sid)
        if snap is None:
            raise ApiError(404, "SNAPSHOT_NOT_FOUND", "snapshot does not exist")
        expired = now >= snap["expires_at"]
    return JSONResponse(status_code=200, content={
        "snapshotId": str(snap["id"]),
        "streams": list(snap["streams"]),
        "highWatermark": snap["high_watermark"],
        "createdAt": snap["created_at"].isoformat(),
        "expiresAt": snap["expires_at"].isoformat(),
        "expired": expired,
    })


@app.get("/v1/streams/read")
async def read_streams(request: Request) -> JSONResponse:
    token = request.query_params.get("cursor")
    if not token:
        raise ApiError(400, "CURSOR_INVALID", "cursor query parameter is required")
    page_size = parse_query_int(request, "pageSize", 100, PAGE_SIZE_MIN, PAGE_SIZE_MAX)
    now = current_time(request)
    payload = decode_cursor(token)

    async with db.pool().acquire() as conn:
        snap = await load_snapshot(conn, payload["snapshotId"], now)
        start = payload["nextPosition"]
        positions, envelopes = await read_page(
            conn, payload["streams"], start, snap["high_watermark"], page_size
        )
        next_pos = positions[-1] if positions else start
        has_more = len(positions) == page_size and next_pos < snap["high_watermark"]
        new_cursor = build_cursor(payload["snapshotId"], payload["streams"], next_pos)
    return JSONResponse(status_code=200, content={
        "snapshotId": payload["snapshotId"],
        "streams": payload["streams"],
        "highWatermark": snap["high_watermark"],
        "page": {
            "transactions": envelopes,
            "transactionCount": len(envelopes),
            "nextCursor": new_cursor,
            "hasMore": has_more,
        },
    })


# ---------------------------------------------------------------------------
# Consumer groups (monotonic ack positions)
# ---------------------------------------------------------------------------


@app.get("/v1/consumer-groups")
async def list_consumer_groups(request: Request) -> JSONResponse:
    """Admin: enumerate groups. Removing a group is an explicit safety
    reduction -- it stops protecting history on the next reclaim."""
    now = current_time(request)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await expire_idle_groups(conn, now)
            rows = await conn.fetch(
                "SELECT name, ack_position, created_at, last_ack_at "
                "FROM consumer_groups ORDER BY name"
            )
    return JSONResponse(status_code=200, content={
        "consumerGroups": [
            {
                "name": r["name"],
                "ackPosition": r["ack_position"],
                "createdAt": r["created_at"].isoformat(),
                "lastAckAt": r["last_ack_at"].isoformat() if r["last_ack_at"] else None,
            }
            for r in rows
        ],
    })


@app.delete("/v1/consumer-groups/{name}")
async def delete_consumer_group(name: str) -> JSONResponse:
    name = require_name(name, "name")
    async with db.pool().acquire() as conn:
        result = await conn.execute(
            "DELETE FROM consumer_groups WHERE name=$1", name
        )
    # asyncpg returns "DELETE <count>"
    deleted = int(result.split()[-1])
    return JSONResponse(status_code=200 if deleted else 404, content={
        "name": name, "deleted": bool(deleted),
    })


@app.post("/v1/consumer-groups")
async def create_consumer_group(request: Request) -> JSONResponse:
    obj = require_object(await body_json(request))
    name = require_name(obj.get("name"), "name")
    now = current_time(request)
    async with db.pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM consumer_groups WHERE name=$1", name
        )
        if row is not None:
            return JSONResponse(status_code=200, content={
                "name": name, "ackPosition": row["ack_position"],
                "createdAt": row["created_at"].isoformat(),
                "lastAckAt": row["last_ack_at"].isoformat() if row["last_ack_at"] else None,
                "status": "already_exists",
            })
        await conn.execute(
            "INSERT INTO consumer_groups (name, ack_position, created_at) VALUES ($1,0,$2)",
            name, now,
        )
    return JSONResponse(status_code=201, content={
        "name": name, "ackPosition": 0,
        "createdAt": now.isoformat(), "lastAckAt": None, "status": "created",
    })


@app.get("/v1/consumer-groups/{name}")
async def get_consumer_group(name: str) -> JSONResponse:
    name = require_name(name, "name")
    async with db.pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM consumer_groups WHERE name=$1", name)
    if row is None:
        raise ApiError(404, "CONSUMER_GROUP_NOT_FOUND", "no such consumer group")
    return JSONResponse(status_code=200, content={
        "name": name, "ackPosition": row["ack_position"],
        "createdAt": row["created_at"].isoformat(),
        "lastAckAt": row["last_ack_at"].isoformat() if row["last_ack_at"] else None,
    })


@app.post("/v1/consumer-groups/{name}/ack")
async def ack_consumer_group(name: str, request: Request) -> JSONResponse:
    name = require_name(name, "name")
    obj = require_object(await body_json(request))
    position = require_int(obj.get("position"), "position", POSITION_MIN, POSITION_MAX)
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            # Pessimistic lock so concurrent acks are serializable.
            row = await conn.fetchrow(
                "SELECT * FROM consumer_groups WHERE name=$1 FOR UPDATE", name
            )
            if row is None:
                raise ApiError(404, "CONSUMER_GROUP_NOT_FOUND",
                               "create the consumer group before acknowledging")
            current = row["ack_position"]
            if position < current:
                raise ApiError(
                    409, "ACK_POSITION_REGRESSED",
                    "ack position can only move forward",
                    {"currentPosition": current, "requestedPosition": position},
                )
            if position == current:
                return JSONResponse(status_code=200, content={
                    "name": name, "ackPosition": current,
                    "lastAckAt": row["last_ack_at"].isoformat() if row["last_ack_at"] else None,
                    "status": "idempotent",
                })
            await conn.execute(
                "UPDATE consumer_groups SET ack_position=$2, last_ack_at=$3 WHERE name=$1",
                name, position, now,
            )
    return JSONResponse(status_code=200, content={
        "name": name, "ackPosition": position,
        "lastAckAt": now.isoformat(), "status": "advanced",
    })


# ---------------------------------------------------------------------------
# Retention / historical reclamation
# ---------------------------------------------------------------------------


@app.post("/v1/retention/reclaim")
async def reclaim(request: Request) -> JSONResponse:
    obj = await body_json(request)
    obj = obj if isinstance(obj, dict) else {}
    fault_token = obj.get("faultToken")
    now = current_time(request)

    # Phase 1 (its own committed transaction): durably reap expired
    # snapshots and idle-invalidated consumer groups. This must persist even
    # if phase 2 later refuses (e.g. no groups left -> RETENTION_NOTHING);
    # folding both into one transaction would roll the expiry back.
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            expired_rows = await conn.fetch(
                "SELECT id FROM snapshots WHERE expires_at <= $1", now
            )
            await conn.execute("DELETE FROM snapshots WHERE expires_at <= $1", now)
            await expire_idle_groups(conn, now)

        # Phase 2: the reclaim decision and whole-envelope deletions.
        async with conn.transaction():
            # Serialize against in-flight commits and snapshot creation.
            await conn.fetchval("SELECT pg_advisory_xact_lock($1)", COMMIT_ORDER_LOCK)

            # Reclaimable positions must satisfy BOTH:
            #   p < ackFloor         (below EVERY active group's ack), and
            #   p > snapshotCeiling  (beyond EVERY live snapshot's need)
            group_count = await conn.fetchval("SELECT COUNT(*) FROM consumer_groups")
            no_group_delete = group_count == 0 and (
                config.RETENTION_NO_GROUP_POLICY == "delete"
            )
            if group_count == 0 and not no_group_delete:
                raise ApiError(
                    409, "RETENTION_NOTHING",
                    "no consumer groups exist; retention disabled by "
                    "RETENTION_NO_GROUP_POLICY=reject",
                    {"policy": config.RETENTION_NO_GROUP_POLICY},
                )

            snap_hw_max = await conn.fetchval(
                "SELECT MAX(high_watermark) FROM snapshots"
            )  # NULL when no live snapshot

            if no_group_delete:
                # Time-based eligibility (every surviving snapshot still
                # protects envelopes at/below its high watermark).
                horizon = now - timedelta(
                    seconds=config.RETENTION_NO_GROUP_HORIZON_SECONDS
                )
                victim_rows = await conn.fetch(
                    "SELECT commit_position FROM transactions "
                    "WHERE status='committed' AND committed_at <= $1 "
                    "AND ($2::bigint IS NULL OR commit_position > $2) "
                    "ORDER BY commit_position FOR UPDATE",
                    horizon, snap_hw_max,
                )
                ack_floor = None
            else:
                ack_floor = await conn.fetchval(
                    "SELECT COALESCE(MIN(ack_position), 0) FROM consumer_groups"
                )
                victim_rows = await conn.fetch(
                    "SELECT commit_position FROM transactions "
                    "WHERE status='committed' AND commit_position < $1 "
                    "AND ($2::bigint IS NULL OR commit_position > $2) "
                    "ORDER BY commit_position FOR UPDATE",
                    ack_floor, snap_hw_max,
                )
            victims = [r["commit_position"] for r in victim_rows]

            if "crash-during-reclaim" in config.FAULT_POINTS and (
                not config.FAULT_TOKEN or fault_token == config.FAULT_TOKEN
            ):
                # Deterministic mid-sweep crash. Everything above is in this
                # transaction; hard exit rolls it all back atomically.
                log.error("FAULT crash-during-reclaim firing: %d candidate envelopes",
                          len(victims))
                os._exit(137)

            deleted_events = 0
            if victims:
                deleted_events = await conn.fetchval(
                    "SELECT COUNT(*) FROM events WHERE commit_position = ANY($1::bigint[])",
                    victims,
                )
                # Whole envelopes only -- never individual events. Events are
                # deleted before the tx rows (FK ON DELETE RESTRICT) in the
                # same atomic transaction.
                await conn.execute(
                    "DELETE FROM events WHERE commit_position = ANY($1::bigint[])",
                    victims,
                )
                await conn.execute(
                    "DELETE FROM transactions WHERE commit_position = ANY($1::bigint[])",
                    victims,
                )

            earliest = await earliest_available(conn)

    return JSONResponse(status_code=200, content={
        "reclaimedTransactionCount": len(victims),
        "reclaimedEventCount": deleted_events,
        "positions": victims,
        "ackFloorPosition": ack_floor,
        "snapshotWatermarkCeiling": snap_hw_max,
        "expiredSnapshotCount": len(expired_rows),
        "earliestAvailablePosition": earliest,
        "reclaimedAt": now.isoformat(),
    })


@app.get("/v1/retention/status")
async def retention_status(request: Request) -> JSONResponse:
    now = current_time(request)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await expire_idle_groups(conn, now)
            group_count = await conn.fetchval("SELECT COUNT(*) FROM consumer_groups")
            min_ack = await conn.fetchval(
                "SELECT COALESCE(MIN(ack_position), -1) FROM consumer_groups"
            )
            snap = await conn.fetchrow(
                "SELECT COUNT(*) AS n, COALESCE(MAX(high_watermark), 0) AS hw_max "
                "FROM snapshots WHERE expires_at > $1",
                now,
            )
            earliest = await earliest_available(conn)
            latest = await conn.fetchval(
                "SELECT COALESCE(MAX(commit_position), 0) FROM events"
            )
    return JSONResponse(status_code=200, content={
        "earliestAvailablePosition": earliest,
        "latestPosition": latest,
        "activeConsumerGroupCount": group_count,
        "minAckPosition": min_ack if min_ack >= 0 else None,
        "liveSnapshotCount": snap["n"],
        "snapshotWatermarkCeiling": snap["hw_max"] if snap["n"] else None,
        "noGroupPolicy": config.RETENTION_NO_GROUP_POLICY,
        "asOf": now.isoformat(),
    })
