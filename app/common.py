"""Shared HTTP/error helpers with no dependency on the FastAPI application.

Keeping these out of ``app.main`` lets feature routers (e.g. derived
processors) reuse them without a circular import.
"""
from __future__ import annotations

import hmac
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import Request

from . import config
from .validation import (
    ValidationError,
    parse_json_strict,
    require_name,
    validate_payload,
)

UTC = timezone.utc


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


async def body_json(request: Request) -> Any:
    raw = await request.body()
    try:
        return parse_json_strict(raw)
    except ValidationError as exc:
        raise ApiError(400, "INVALID_JSON", exc.message)


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
    from .validation import canonical_json
    return events, canonical_json({"events": fingerprint}), total_bytes
