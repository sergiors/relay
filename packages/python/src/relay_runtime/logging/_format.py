"""JSON rendering for Relay's structured log records.

The default handler uses this formatter. It only reads attributes already on the
record, so a record created inside an invocation keeps its correlation fields
when an application hands it to a ``QueueHandler`` and formats it in another
thread. Relay's formatter never initializes OpenTelemetry or opens a transport.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

__all__ = ["JsonFormatter"]

_MAX_DEPTH = 16
_FALLBACK_MESSAGE = "log record could not be serialized"
_EXCEPTION_FORMATTER = logging.Formatter()

_STRING_METADATA = (
    "app",
    "handler",
    "request_id",
    "trace_id",
    "span_id",
)
_INT_METADATA = ("attempt",)


class JsonFormatter(logging.Formatter):
    """Render one record as a compact single-line JSON object.

    The exact key set is documented in the package README. Application values
    are converted to a finite, JSON-native representation; a value outside the
    supported set is replaced by a marker instead of failing the record.
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            payload = _payload(record)
            return _dumps(payload)
        except Exception as exc:
            # One malformed record must not break the handler chain or replace
            # the application's error with a logging error. BaseException is
            # deliberately not caught: process-control signals still propagate.
            return _fallback_line(record, exc)


def _payload(record: logging.LogRecord) -> dict[str, object]:
    payload: dict[str, object] = {
        "timestamp": datetime.fromtimestamp(record.created, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "level": record.levelname,
        "logger": record.name,
        "message": record.getMessage(),
        "location": f"{record.module}.{record.funcName}:{record.lineno}",
    }

    service = getattr(record, "service", None)
    payload["service"] = service if isinstance(service, str) else record.name

    for name in _STRING_METADATA:
        value = getattr(record, name, None)
        if isinstance(value, str):
            payload[name] = value
    for name in _INT_METADATA:
        value = getattr(record, name, None)
        if isinstance(value, int) and not isinstance(value, bool):
            payload[name] = value

    cold_start = getattr(record, "cold_start", None)
    if isinstance(cold_start, bool):
        payload["cold_start"] = cold_start

    attributes = getattr(record, "attributes", None)
    if isinstance(attributes, Mapping):
        converted_attributes = _json_value(attributes)
        if not isinstance(converted_attributes, dict) or converted_attributes:
            payload["attributes"] = converted_attributes

    exception = _exception_payload(record)
    if exception is not None:
        payload["exception"] = exception
    if record.stack_info:
        payload["stack_info"] = record.stack_info

    return _as_json_object(payload)


def _exception_payload(record: logging.LogRecord) -> dict[str, object] | None:
    exc_info = record.exc_info
    if exc_info is not None and exc_info[0] is not None:
        exc = exc_info[1]
        stacktrace = _EXCEPTION_FORMATTER.formatException(exc_info)
        return {
            "type": type(exc).__qualname__,
            "message": str(exc),
            "stacktrace": stacktrace,
        }
    if record.exc_text:
        return {"stacktrace": record.exc_text}
    return None


def _as_json_object(value: dict[str, object]) -> dict[str, object]:
    converted = _json_value(value)
    # _json_value returns a mapping for the dict handed to it.
    return converted if isinstance(converted, dict) else {}


def _json_value(value: object, depth: int = 0, seen: set[int] | None = None) -> object:
    """Convert one value to the documented finite JSON-native representation."""
    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else {"$unsupported": "float"}

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else {"$unsupported": "Decimal"}

    active = set() if seen is None else seen

    if isinstance(value, Mapping):
        if depth >= _MAX_DEPTH:
            return {"$truncated": True}

        if id(value) in active:
            return {"$circular": True}

        active.add(id(value))

        try:
            result: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    # JSON object keys are strings. A mixed-key mapping is not
                    # converted with str() because that can collide and leak.
                    return {"$unsupported": type(value).__qualname__}
                result[key] = _json_value(item, depth + 1, active)

            return result
        finally:
            active.discard(id(value))

    if isinstance(value, (list, tuple)):
        if depth >= _MAX_DEPTH:
            return {"$truncated": True}

        if id(value) in active:
            return {"$circular": True}

        active.add(id(value))

        try:
            return [_json_value(item, depth + 1, active) for item in value]
        finally:
            active.discard(id(value))

    return {"$unsupported": type(value).__qualname__}


def _dumps(payload: dict[str, object]) -> str:
    # ensure_ascii keeps the line writable on an ASCII-only stderr; allow_nan is
    # false because _json_value already replaced non-finite floats.
    return json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def _fallback_line(record: logging.LogRecord, exc: Exception) -> str:
    try:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname if isinstance(record.levelname, str) else "ERROR",
            "logger": record.name if isinstance(record.name, str) else "relay.logging",
            "message": _FALLBACK_MESSAGE,
            "error": type(exc).__qualname__,
        }
        service = getattr(record, "service", None)
        payload["service"] = (
            service if isinstance(service, str) and service else payload["logger"]
        )

        if isinstance(record.module, str) and isinstance(record.funcName, str):
            payload["location"] = f"{record.module}.{record.funcName}:{record.lineno}"

        return _dumps(payload)
    except Exception:
        return (
            '{"level":"ERROR","logger":"relay.logging",'
            '"message":"log record could not be serialized"}'
        )
