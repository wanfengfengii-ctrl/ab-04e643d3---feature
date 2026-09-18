"""Persistent derived processors (versioned HTTP JSON API under /v1).

A processor reads whole committed source transaction envelopes from a fixed
set of input streams, in strict ``commitPosition`` order, and an analysis
worker atomically turns one claimed range into zero or more derived events
on declared output streams plus an advanced checkpoint.

Every concurrency guarantee is provided by PostgreSQL -- a per-processor
transaction-scoped advisory lock (keyed by ``hashtext(id)``, identical on
every API process), ``FOR UPDATE`` row locks, the global commit-order
advisory lock and a primary-keyed result table. No in-process mutex is used,
so any healthy API instance sharing the database can claim, renew, take
over, complete and retry work.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from . import config, db
from .common import ApiError, body_json, canonicalize_events, current_time
from .validation import (
    POSITION_MAX,
    ValidationError,
    canonical_json,
    require_int,
    require_name,
    require_object,
    require_string_list,
)

UTC = timezone.utc
log = logging.getLogger("eventsvc.processors")

# First key of the two-integer advisory lock used to serialize claim /
# renew / complete for a single processor across every API process.
PROCESSOR_LOCK_NS = 0x50524F43  # "PROC"

router = APIRouter()


# ---------------------------------------------------------------------------
# Serialization / reading helpers
# ---------------------------------------------------------------------------


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


async def _high_watermark(conn: asyncpg.Connection) -> int:
    return await conn.fetchval(
        "SELECT COALESCE(MAX(commit_position), 0) FROM transactions "
        "WHERE status = 'committed'"
    )


async def _read_source(
    conn: asyncpg.Connection,
    streams: list[str],
    start: int,
    end: int,
    limit: int,
) -> tuple[list[int], list[dict]]:
    """Whole envelopes touching ``streams`` with positions in (start, end].

    A source transaction enters the result at most once even if it hits
    several configured streams; only the event subset on configured streams
    is returned.
    """
    pos_rows = await conn.fetch(
        "SELECT DISTINCT commit_position FROM events "
        "WHERE stream = ANY($1::text[]) AND commit_position > $2 "
        "AND commit_position <= $3 ORDER BY commit_position LIMIT $4",
        streams, start, end, limit,
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


def _source_digest(input_streams: list[str], envelopes: list[dict]) -> str:
    """Stable canonical summary of the claimed source range.

    Covers the configured stream set and every returned envelope's identity
    plus its filtered, canonical event subset.
    """
    doc = {
        "streams": list(input_streams),
        "transactions": [
            {
                "commitPosition": e["commitPosition"],
                "producer": e["producer"],
                "txId": e["txId"],
                "epoch": e["epoch"],
                "firstSequence": e["firstSequence"],
                "events": [
                    [ev["stream"], ev["key"], ev["payload"]] for ev in e["events"]
                ],
            }
            for e in envelopes
        ],
    }
    return hashlib.sha256(canonical_json(doc).encode("utf-8")).hexdigest()


async def _lock_processor(conn: asyncpg.Connection, processor_id: str) -> None:
    await conn.fetchval(
        "SELECT pg_advisory_xact_lock($1::int, hashtext($2)::int)",
        PROCESSOR_LOCK_NS, processor_id,
    )


async def _get_processor(
    conn: asyncpg.Connection, processor_id: str, for_update: bool = False
) -> asyncpg.Record:
    sql = "SELECT * FROM processors WHERE id = $1"
    if for_update:
        sql += " FOR UPDATE"
    row = await conn.fetchrow(sql, processor_id)
    if row is None or row["status"] == "deleted":
        raise ApiError(404, "PROCESSOR_NOT_FOUND",
                       f"processor {processor_id!r} does not exist")
    return row


def _work_view(row: asyncpg.Record, now: datetime) -> dict | None:
    if row["lease_id"] is None:
        return None
    return {
        "leaseId": str(row["lease_id"]),
        "generation": row["generation"],
        "fromPosition": row["lease_from"],
        "throughPosition": row["lease_through"],
        "expiresAt": _iso(row["lease_expires_at"]),
        "expired": now >= row["lease_expires_at"],
        "sourceDigest": row["source_digest"],
    }


def _processor_view(row: asyncpg.Record, now: datetime) -> dict:
    return {
        "id": row["id"],
        "status": row["status"],
        "inputStreams": list(row["input_streams"]),
        "outputStreams": list(row["output_streams"]),
        "batchSize": row["batch_size"],
        "leaseSeconds": row["lease_seconds"],
        "startPosition": row["start_position"],
        "checkpointPosition": row["checkpoint_position"],
        "createdAt": _iso(row["created_at"]),
        "updatedAt": _iso(row["updated_at"]),
        "currentWork": _work_view(row, now),
    }


def _completed_view(row: asyncpg.Record) -> dict:
    return {
        "processorId": row["processor_id"],
        "resultId": row["result_id"],
        "generation": row["generation"],
        "fromPosition": row["from_position"],
        "throughPosition": row["through_position"],
        "commitPosition": row["derived_commit_position"],
        "eventCount": row["event_count"],
        "status": "completed",
    }


def _parse_lease_id(value: Any) -> uuid.UUID:
    if not isinstance(value, str):
        raise ValidationError("leaseId must be a UUID string", "leaseId")
    try:
        return uuid.UUID(value)
    except (ValueError, TypeError) as exc:
        raise ValidationError("leaseId must be a UUID string", "leaseId") from exc


def _canonicalize_derived(
    raw_events: Any, output_streams: list[str]
) -> tuple[list[dict], str, int]:
    """Validate 0..100 derived events against declared outputs and limits.

    Membership is checked against the persisted output stream set; naming,
    JSON canonicalization and size limits are exactly those applied to
    ordinary producer batches.
    """
    if raw_events is None:
        raw_events = []
    if not isinstance(raw_events, list):
        raise ValidationError("events must be an array", "events")
    if len(raw_events) > config.PROCESSOR_MAX_EVENTS:
        raise ValidationError(
            f"events may contain at most {config.PROCESSOR_MAX_EVENTS} items",
            "events",
        )
    if not raw_events:
        return [], canonical_json({"events": []}), 0
    events, _fingerprint, total_bytes = canonicalize_events(raw_events)
    outputs = set(output_streams)
    for i, ev in enumerate(events):
        if ev["stream"] not in outputs:
            raise ValidationError(
                f"events[{i}].stream {ev['stream']!r} is not declared in "
                "outputStreams",
                f"events[{i}].stream",
            )
    fingerprint = [
        [e["stream"], e["key"], json.loads(e["payload"])] for e in events
    ]
    return events, canonical_json({"events": fingerprint}), total_bytes


# ---------------------------------------------------------------------------
# Processor lifecycle
# ---------------------------------------------------------------------------


@router.post("/v1/processors")
async def create_processor(request: Request) -> JSONResponse:
    obj = require_object(await body_json(request))
    pid = require_name(obj.get("id"), "id")
    input_streams = require_string_list(
        obj.get("inputStreams"), "inputStreams", config.PROCESSOR_MAX_STREAMS
    )
    output_streams = require_string_list(
        obj.get("outputStreams"), "outputStreams", config.PROCESSOR_MAX_STREAMS
    )
    batch_size = require_int(
        obj.get("batchSize"), "batchSize", 1, config.PROCESSOR_MAX_BATCH_SIZE
    )
    lease_seconds = require_int(
        obj.get("leaseSeconds"), "leaseSeconds",
        config.PROCESSOR_LEASE_MIN_SECONDS, config.PROCESSOR_LEASE_MAX_SECONDS,
    )
    start_position = require_int(
        obj.get("startPosition"), "startPosition", 0, POSITION_MAX
    )
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            # Serialize against producer commits / retention so the history
            # window check is one linearizable point.
            await conn.fetchval("SELECT pg_advisory_xact_lock($1)",
                                db.COMMIT_ORDER_LOCK)
            earliest = await conn.fetchval(
                "SELECT COALESCE(MIN(commit_position), 0) FROM events"
            )
            latest = await _high_watermark(conn)
            if not (earliest <= start_position <= latest):
                raise ApiError(
                    409, "START_POSITION_OUT_OF_RANGE",
                    "startPosition must lie within the currently available history",
                    {"earliestAvailablePosition": earliest,
                     "latestPosition": latest,
                     "requestedPosition": start_position},
                )
            try:
                await conn.execute(
                    "INSERT INTO processors (id, input_streams, output_streams, "
                    "batch_size, lease_seconds, start_position, status, "
                    "checkpoint_position, generation, created_at, updated_at) "
                    "VALUES ($1,$2,$3,$4,$5,$6,'active',$7,0,$8,$8)",
                    pid, input_streams, output_streams, batch_size,
                    lease_seconds, start_position, start_position, now,
                )
            except asyncpg.UniqueViolationError as exc:
                raise ApiError(409, "PROCESSOR_ID_REUSED",
                               f"processor {pid!r} already exists") from exc
            row = await conn.fetchrow("SELECT * FROM processors WHERE id=$1", pid)
    return JSONResponse(status_code=201, content=_processor_view(row, now))


@router.get("/v1/processors")
async def list_processors(request: Request) -> JSONResponse:
    now = current_time(request)
    async with db.pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM processors WHERE status <> 'deleted' ORDER BY id"
        )
    return JSONResponse(status_code=200,
                        content={"processors": [_processor_view(r, now) for r in rows]})


@router.get("/v1/processors/{processor_id}")
async def get_processor(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    now = current_time(request)
    async with db.pool().acquire() as conn:
        row = await _get_processor(conn, processor_id)
    return JSONResponse(status_code=200, content=_processor_view(row, now))


@router.post("/v1/processors/{processor_id}/pause")
async def pause_processor(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    now = current_time(request)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await _lock_processor(conn, processor_id)
            row = await _get_processor(conn, processor_id, for_update=True)
            if row["status"] == "active":
                await conn.execute(
                    "UPDATE processors SET status='paused', updated_at=$2 WHERE id=$1",
                    processor_id, now,
                )
                row = await conn.fetchrow(
                    "SELECT * FROM processors WHERE id=$1", processor_id
                )
    # Pausing only forbids new claims; an outstanding lease stays valid and
    # the checkpoint keeps protecting history.
    return JSONResponse(status_code=200, content=_processor_view(row, now))


@router.post("/v1/processors/{processor_id}/resume")
async def resume_processor(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    now = current_time(request)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await _lock_processor(conn, processor_id)
            row = await _get_processor(conn, processor_id, for_update=True)
            if row["status"] == "paused":
                await conn.execute(
                    "UPDATE processors SET status='active', updated_at=$2 WHERE id=$1",
                    processor_id, now,
                )
                row = await conn.fetchrow(
                    "SELECT * FROM processors WHERE id=$1", processor_id
                )
    return JSONResponse(status_code=200, content=_processor_view(row, now))


@router.delete("/v1/processors/{processor_id}")
async def delete_processor(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    now = current_time(request)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await _lock_processor(conn, processor_id)
            row = await conn.fetchrow(
                "SELECT * FROM processors WHERE id=$1 FOR UPDATE", processor_id
            )
            if row is None or row["status"] == "deleted":
                raise ApiError(404, "PROCESSOR_NOT_FOUND",
                               f"processor {processor_id!r} does not exist")
            # Explicitly abandon history protection and revoke any outstanding
            # lease. Already committed derived envelopes are ordinary global
            # log entries and are never deleted.
            await conn.execute(
                "UPDATE processors SET status='deleted', lease_id=NULL, "
                "lease_from=NULL, lease_through=NULL, lease_expires_at=NULL, "
                "source_digest=NULL, updated_at=$2 WHERE id=$1",
                processor_id, now,
            )
    return JSONResponse(status_code=200,
                        content={"id": processor_id, "deleted": True})


# ---------------------------------------------------------------------------
# Claim / renew / complete
# ---------------------------------------------------------------------------


@router.post("/v1/processors/{processor_id}/claims")
async def claim_work(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await _lock_processor(conn, processor_id)
            row = await _get_processor(conn, processor_id, for_update=True)
            if row["status"] == "paused":
                raise ApiError(409, "PROCESSOR_PAUSED",
                               "processor is paused; new claims are forbidden")

            # Serialize against producer commits and retention sweeps so the
            # claimed range and the protection decision are one linearizable
            # point for every API process.
            await conn.fetchval("SELECT pg_advisory_xact_lock($1)",
                                db.COMMIT_ORDER_LOCK)

            generation = row["generation"] + 1
            checkpoint = row["checkpoint_position"]
            if row["lease_id"] is not None:
                if now < row["lease_expires_at"]:
                    raise ApiError(
                        409, "WORK_ALREADY_CLAIMED",
                        "this processor already has an outstanding work unit",
                        {"leaseId": str(row["lease_id"]),
                         "generation": row["generation"],
                         "expiresAt": _iso(row["lease_expires_at"])},
                    )
                # Lease expired: another executor takes over the EXACT same,
                # immutable range. Read bounded by the stored throughPosition
                # and verify the canonical digest still matches; retention can
                # never have removed this range because the checkpoint pins it.
                takeover_through = row["lease_through"]
                positions, envelopes = await _read_source(
                    conn, list(row["input_streams"]),
                    checkpoint, takeover_through, row["batch_size"],
                )
                digest = _source_digest(list(row["input_streams"]), envelopes)
                if not positions or positions[-1] != takeover_through \
                        or digest != row["source_digest"]:
                    raise ApiError(
                        500, "WORK_RANGE_INVALID",
                        "stored work range no longer matches source history",
                    )
                through = takeover_through
            else:
                high_watermark = await _high_watermark(conn)
                positions, envelopes = await _read_source(
                    conn, list(row["input_streams"]),
                    checkpoint, high_watermark, row["batch_size"],
                )
                if not positions:
                    return JSONResponse(status_code=200, content={
                        "processorId": processor_id,
                        "status": "no_work",
                        "checkpointPosition": checkpoint,
                    })
                through = positions[-1]
                digest = _source_digest(list(row["input_streams"]), envelopes)
            lease_id = uuid.uuid4()
            expires = now + timedelta(seconds=row["lease_seconds"])
            await conn.execute(
                "UPDATE processors SET lease_id=$2, generation=$3, lease_from=$4, "
                "lease_through=$5, lease_expires_at=$6, source_digest=$7, "
                "updated_at=$8 WHERE id=$1",
                processor_id, lease_id, generation, checkpoint, through,
                expires, digest, now,
            )
            response = {
                "processorId": processor_id,
                "status": "claimed",
                "leaseId": str(lease_id),
                "generation": generation,
                "fromPosition": checkpoint,
                "throughPosition": through,
                "expiresAt": expires.isoformat(),
                "leaseSeconds": row["lease_seconds"],
                "sourceDigest": digest,
                "checkpointPosition": checkpoint,
                "transactionCount": len(envelopes),
                "transactions": envelopes,
            }
    return JSONResponse(status_code=200, content=response)


@router.post("/v1/processors/{processor_id}/renew")
async def renew_lease(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    obj = require_object(await body_json(request))
    lease_id = _parse_lease_id(obj.get("leaseId"))
    generation = require_int(obj.get("generation"), "generation", 1, POSITION_MAX)
    now = current_time(request)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await _lock_processor(conn, processor_id)
            row = await _get_processor(conn, processor_id, for_update=True)

            # Any mismatch (stale generation, unknown lease, work finished or
            # taken over, deleted processor) is a stable fence.
            if (row["lease_id"] is None
                    or row["lease_id"] != lease_id
                    or row["generation"] != generation):
                raise ApiError(
                    409, "LEASE_FENCED", "lease is no longer current",
                    {"currentGeneration": row["generation"]},
                )
            # Boundary rule: now >= expiresAt means the lease is dead.
            if now >= row["lease_expires_at"]:
                raise ApiError(
                    409, "LEASE_FENCED", "lease has expired",
                    {"currentGeneration": row["generation"],
                     "expiresAt": _iso(row["lease_expires_at"])},
                )
            expires = now + timedelta(seconds=row["lease_seconds"])
            await conn.execute(
                "UPDATE processors SET lease_expires_at=$2, updated_at=$3 "
                "WHERE id=$1",
                processor_id, expires, now,
            )
            view = {
                "processorId": processor_id,
                "status": "renewed",
                "leaseId": str(lease_id),
                "generation": generation,
                "fromPosition": row["lease_from"],
                "throughPosition": row["lease_through"],
                "expiresAt": expires.isoformat(),
            }
    return JSONResponse(status_code=200, content=view)


@router.post("/v1/processors/{processor_id}/complete")
async def complete_work(processor_id: str, request: Request) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    obj = require_object(await body_json(request))
    result_id = require_name(obj.get("resultId"), "resultId")
    lease_id = _parse_lease_id(obj.get("leaseId"))
    generation = require_int(obj.get("generation"), "generation", 1, POSITION_MAX)
    source_digest = obj.get("sourceDigest")
    if not isinstance(source_digest, str) or len(source_digest) != 64:
        raise ValidationError("sourceDigest must be the 64-char claim digest",
                              "sourceDigest")
    if "events" not in obj:
        raise ValidationError("events is required (use [] for zero output)",
                              "events")
    raw_events = obj["events"]
    fault_token = obj.get("faultToken")
    now = current_time(request)

    async with db.pool().acquire() as conn:
        result_row: asyncpg.Record | None = None
        async with conn.transaction():
            await _lock_processor(conn, processor_id)
            row = await conn.fetchrow(
                "SELECT * FROM processors WHERE id=$1 FOR UPDATE", processor_id
            )
            if row is None:
                raise ApiError(404, "PROCESSOR_NOT_FOUND",
                               f"processor {processor_id!r} does not exist")

            # Canonicalize against the persisted (immutable) output set while
            # holding the processor lock; a bad batch can never reach the
            # result/lease checks or write anything.
            events, content_canonical, total_bytes = _canonicalize_derived(
                raw_events, list(row["output_streams"])
            )
            content_digest = hashlib.sha256(
                content_canonical.encode("utf-8")
            ).hexdigest()

            # Idempotency has the HIGHEST priority: a previously accepted
            # completion is recognizable by resultId even after lease expiry,
            # a higher-generation takeover, processor pause or crash/restart.
            existing = await conn.fetchrow(
                "SELECT * FROM processor_results "
                "WHERE processor_id=$1 AND result_id=$2",
                processor_id, result_id,
            )
            if existing is not None:
                same_work = (
                    existing["generation"] == generation
                    and existing["source_digest"] == source_digest
                    and existing["content_digest"] == content_digest
                )
                if same_work:
                    result_row = existing
                else:
                    raise ApiError(
                        409, "RESULT_ID_REUSED",
                        "resultId was already completed for different work or "
                        "with different content",
                        {"existingGeneration": existing["generation"],
                         "existingThroughPosition": existing["through_position"]},
                    )
            else:
                if row["status"] == "deleted":
                    raise ApiError(404, "PROCESSOR_NOT_FOUND",
                                   "processor has been deleted")
                # Lease validation: exact identity, strictly live lease.
                if (row["lease_id"] is None
                        or row["lease_id"] != lease_id
                        or row["generation"] != generation):
                    raise ApiError(
                        409, "LEASE_FENCED",
                        "lease is no longer current; work may have been taken over",
                        {"currentGeneration": row["generation"]},
                    )
                if now >= row["lease_expires_at"]:
                    raise ApiError(
                        409, "LEASE_FENCED", "lease has expired",
                        {"currentGeneration": row["generation"],
                         "expiresAt": _iso(row["lease_expires_at"])},
                    )
                if source_digest != row["source_digest"]:
                    raise ApiError(
                        409, "SOURCE_DIGEST_MISMATCH",
                        "sourceDigest does not match the claimed work",
                        {"expectedDigest": row["source_digest"]},
                    )

                from_position = row["lease_from"]
                through_position = row["lease_through"]
                derived_position: int | None = None

                # Always serialize against producer commits and retention
                # sweeps, even for zero output: the checkpoint advance and
                # (for non-empty output) the derived envelope commit must
                # become visible to reclaim as one indivisible step.
                await conn.fetchval(
                    "SELECT pg_advisory_xact_lock($1)", db.COMMIT_ORDER_LOCK
                )

                if events:
                    # Derived output is an ordinary committed envelope in the
                    # global log: same global lock, same sequence, same tables,
                    # same read model, same snapshot/stream/whole-envelope
                    # retention rules. No second log exists.
                    await conn.fetchval(
                        "SELECT pg_advisory_xact_lock($1)", db.COMMIT_ORDER_LOCK
                    )
                    derived_position = await conn.fetchval(
                        "SELECT nextval('commit_position_seq')"
                    )
                    derived_tx = "derived-" + uuid.uuid4().hex
                    await conn.execute(
                        "INSERT INTO transactions (producer, tx_id, epoch, status, "
                        "first_sequence, event_count, batch_canonical, batch_bytes, "
                        "commit_position, committed_at, created_at, updated_at) "
                        "VALUES ($1,$2,1,'committed',$3,$4,$5,$6,$7,$8,$8,$8)",
                        processor_id, derived_tx, 1, len(events),
                        content_canonical, total_bytes, derived_position, now,
                    )
                    out_rows = []
                    for ordinal, ev in enumerate(
                        json.loads(content_canonical)["events"]
                    ):
                        stream, key, payload = ev
                        out_rows.append((
                            derived_position, ordinal, processor_id, derived_tx,
                            ordinal + 1, stream, key,
                            json.dumps(payload, separators=(",", ":"),
                                       ensure_ascii=False),
                        ))
                    await conn.executemany(
                        "INSERT INTO events (commit_position, ordinal, producer, "
                        "tx_id, seq, stream, event_key, payload) "
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",
                        out_rows,
                    )

                # Result dedup record, checkpoint advance and lease release
                # are all part of this same atomic transaction -- including
                # the zero-output path.
                await conn.execute(
                    "INSERT INTO processor_results (processor_id, result_id, "
                    "generation, from_position, through_position, source_digest, "
                    "content_digest, event_count, derived_commit_position, "
                    "completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    processor_id, result_id, generation, from_position,
                    through_position, source_digest, content_digest, len(events),
                    derived_position, now,
                )
                await conn.execute(
                    "UPDATE processors SET checkpoint_position=$2, lease_id=NULL, "
                    "lease_from=NULL, lease_through=NULL, lease_expires_at=NULL, "
                    "source_digest=NULL, updated_at=$3 WHERE id=$1",
                    processor_id, through_position, now,
                )
                result_row = await conn.fetchrow(
                    "SELECT * FROM processor_results "
                    "WHERE processor_id=$1 AND result_id=$2",
                    processor_id, result_id,
                )
        # COMMIT has returned: derived envelope (if any), zero-output result
        # and checkpoint advance are all durable together. The HTTP response
        # has not been sent yet.

    if "crash-after-processor-complete" in config.FAULT_POINTS and (
        not config.FAULT_TOKEN or fault_token == config.FAULT_TOKEN
    ):
        log.error(
            "FAULT crash-after-processor-complete firing processor=%s result=%s",
            processor_id, result_id,
        )
        os._exit(137)

    return JSONResponse(status_code=200, content=_completed_view(result_row))


@router.get("/v1/processors/{processor_id}/results/{result_id}")
async def get_result(processor_id: str, result_id: str) -> JSONResponse:
    processor_id = require_name(processor_id, "processorId")
    result_id = require_name(result_id, "resultId")
    async with db.pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM processor_results "
            "WHERE processor_id=$1 AND result_id=$2",
            processor_id, result_id,
        )
    if row is None:
        raise ApiError(404, "RESULT_NOT_FOUND",
                       "no completed result with this resultId")
    return JSONResponse(status_code=200, content=_completed_view(row))
