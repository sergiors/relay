"""The per-invocation ``HandlerContext`` Relay passes as a handler's second
argument.

Embedded beside the bootstrap and written to ``/relay/_relay_context.py`` in
the runtime image.
"""

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class HandlerContext:
    """Metadata for exactly one handler invocation.

    Built fresh from the request frame's "context" object for every call and
    never shared or mutated across invocations. The deadline is an absolute,
    timezone-aware instant, so get_remaining_time_ms() is independent of the
    process's warm lifetime.
    """

    request_id: str
    attempt: int
    max_attempts: int
    deadline: datetime
    message_id: str | None
    trace_id: str | None
    app: str
    handler: str

    def get_remaining_time_ms(self) -> int:
        """Milliseconds until the deadline, clamped at zero once it passes."""
        remaining = self.deadline - datetime.now(UTC)
        return max(0, int(remaining.total_seconds() * 1000))


# Go's handlerContextFrame always emits these fields (MessageID and TraceID are
# omitempty). A context that omits one is not a complete invocation identity.
REQUIRED_CONTEXT_FIELDS = (
    "requestId",
    "attempt",
    "maxAttempts",
    "deadline",
    "app",
    "handler",
)


def parse_deadline(value):
    """Parse the protocol's ISO-8601 deadline into an aware UTC datetime.

    Go emits RFC 3339 with an optional fractional part of up to nanosecond
    precision and a trailing "Z". Older Pythons' fromisoformat accepts neither,
    so normalize both here instead of depending on the interpreter version.
    """
    if not isinstance(value, str):
        raise ValueError("context.deadline must be an ISO-8601 string")

    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"

    # datetime stores microseconds. Padding or truncating to six digits also
    # covers older Pythons that accept only 3 or 6 fractional digits.
    head, dot, tail = text.partition(".")
    if dot:
        digits = 0
        while digits < len(tail) and tail[digits].isdigit():
            digits += 1
        if digits:
            fraction = (tail[:digits][:6] + "000000")[:6]
            text = head + "." + fraction + tail[digits:]

    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed


def build_handler_context(raw):
    """Build a fresh HandlerContext from one request frame's context object.

    The required identity fields must be present and correctly typed; the
    optional source message and trace IDs may be absent, but when present they
    must be strings. A malformed context fails only this invocation (ok:false),
    so the container stays reusable.
    """
    if not isinstance(raw, dict):
        raise ValueError("request frame is missing the context object")

    missing = [name for name in REQUIRED_CONTEXT_FIELDS if name not in raw]
    if missing:
        raise ValueError("context is missing required field(s): " + ", ".join(missing))

    for name in ("requestId", "deadline", "app", "handler"):
        if not isinstance(raw[name], str):
            raise ValueError(f"context.{name} must be a string")

    for name in ("attempt", "maxAttempts"):
        value = raw[name]
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"context.{name} must be an integer")

    message_id = raw.get("messageId")
    if "messageId" in raw and not isinstance(message_id, str):
        raise ValueError("context.messageId must be a string when present")

    trace_id = raw.get("traceId")
    if "traceId" in raw and not isinstance(trace_id, str):
        raise ValueError("context.traceId must be a string when present")

    return HandlerContext(
        request_id=raw["requestId"],
        attempt=raw["attempt"],
        max_attempts=raw["maxAttempts"],
        deadline=parse_deadline(raw["deadline"]),
        message_id=message_id,
        trace_id=trace_id,
        app=raw["app"],
        handler=raw["handler"],
    )
