"""Strict input validation and deterministic JSON canonicalization.

Protocol contract:

* Names (producer, transaction, consumer group, stream, event key) use a
  restricted ASCII subset: 1-128 chars, first char ``[A-Za-z0-9]``,
  remaining chars ``[A-Za-z0-9._-]``.
* Protocol integers (epoch, sequences, positions, page sizes, counts)
  must be JSON integers: booleans, decimals, exponent notation and
  out-of-range values are rejected.
* Event payloads are arbitrary finite JSON. Duplicate object keys,
  NaN/Infinity are rejected. Canonical form uses sorted object keys,
  compact separators, UTF-8 and shortest-roundtrip float rendering.
"""
from __future__ import annotations

import json
import re
from typing import Any

NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._\-]{0,127}\Z")

# Protocol integer bounds
EPOCH_MIN, EPOCH_MAX = 1, 2**63 - 1
SEQUENCE_MIN, SEQUENCE_MAX = 1, 2**63 - 1
POSITION_MIN, POSITION_MAX = 0, 2**63 - 1
COUNT_MIN, COUNT_MAX = 1, 100
PAGE_SIZE_MIN, PAGE_SIZE_MAX = 1, 1000
TTL_MIN, TTL_MAX = 1, 7 * 24 * 3600


class ValidationError(Exception):
    def __init__(self, message: str, field: str | None = None):
        super().__init__(message)
        self.message = message
        self.field = field


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ValidationError(f"duplicate object key: {key!r}")
        seen.add(key)
        obj[key] = value
    return obj


def _reject_constant(value: str) -> Any:
    raise ValidationError(f"non-finite number is not allowed: {value}")


def parse_json_strict(raw: bytes | str) -> Any:
    """Parse JSON, rejecting duplicate keys and NaN/Infinity."""
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValidationError("request body is not valid UTF-8") from exc
    else:
        text = raw
    try:
        return json.loads(
            text,
            object_pairs_hook=_object_pairs,
            parse_constant=_reject_constant,
        )
    except ValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise ValidationError(f"request body is not valid JSON: {exc.msg}")


def require_object(value: Any, field: str = "body") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{field} must be a JSON object", field)
    return value


def require_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not NAME_RE.match(value):
        raise ValidationError(
            f"{field} must be 1-128 chars of [A-Za-z0-9._-], starting with "
            "an ASCII letter or digit",
            field,
        )
    return value


def require_int(
    value: Any,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    # bool is a subclass of int in Python; reject it explicitly.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} must be an integer", field)
    if minimum is not None and value < minimum:
        raise ValidationError(f"{field} must be >= {minimum}", field)
    if maximum is not None and value > maximum:
        raise ValidationError(f"{field} must be <= {maximum}", field)
    return value


def require_str(value: Any, field: str, max_length: int = 256) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{field} must be a non-empty string", field)
    if len(value) > max_length:
        raise ValidationError(f"{field} must be <= {max_length} characters", field)
    return value


def require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{field} must be a boolean", field)
    return value


def require_string_list(value: Any, field: str, max_items: int = 100) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValidationError(f"{field} must be a non-empty array of strings", field)
    if len(value) > max_items:
        raise ValidationError(f"{field} may contain at most {max_items} items", field)
    out: list[str] = []
    seen: set[str] = set()
    for i, item in enumerate(value):
        name = require_name(item, f"{field}[{i}]")
        if name in seen:
            raise ValidationError(f"{field}[{i}] is duplicated: {name}", field)
        seen.add(name)
        out.append(name)
    return out


def canonical_json(value: Any) -> str:
    """Deterministic serialization. Input already passed strict parsing."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def validate_payload(value: Any, field: str = "payload") -> str:
    """Validate an event payload and return its canonical UTF-8 text."""
    try:
        text = canonical_json(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field} is not canonicalizable JSON: {exc}", field)
    return text
