"""Relay's Python function bootstrap.

One execution container per function STAYS ALIVE between invocations and serves
Relay's authenticated, length-prefixed JSON protocol over TCP on
0.0.0.0:$RELAY_EXECUTION_PORT (7000). Relay owns exactly one long-lived
connection per container and exchanges one invocation/result at a time over it.
Application stdout/stderr stay native container logs: no protocol traffic uses
them, and Relay neither reads nor forwards them.

Wire format (both directions):
  4-byte big-endian unsigned payload length, then that many UTF-8 JSON bytes.
  There is no newline or sentinel. Handshake frames are capped at 4 KiB, invoke
  requests at 8 MiB, and results at 1 MiB. A zero or oversized length is
  rejected before allocation; reads are exact and writes complete.

Handshake (once per connection, before any invocation):
  Relay    -> {"type":"hello","version":1,"nonce":"<64 lowercase hex>"}
  bootstrap-> {"type":"auth","version":1,"mac":"<64 lowercase hex>",
               "nonce":"<64 lowercase hex>"}
  Relay    -> {"type":"ready","version":1,"mac":"<64 lowercase hex>"}
  Both MACs are HMAC-SHA256 keyed by the ASCII bytes of RELAY_EXECUTION_SECRET
  (64 lowercase hex chars): auth signs "relay-bootstrap-auth-v1:" plus the
  Relay nonce, ready signs "relay-client-auth-v1:" plus the bootstrap nonce.
  The bootstrap sends auth only once it is ready to read invocations and
  verifies ready with hmac.compare_digest before accepting an invoke frame.
  Unauthenticated peers are closed after a short deadline without running a
  handler; the listener stays available for Relay.

Invocation:
  Relay    -> {"type":"invoke","id":"<id>","handler":"mod.func",
               "event":<raw JSON>,"env":{"K":"V"},
               "trace":{"traceparent":"...","tracestate":"...",
                        "baggage":"..."},
               "context":{"requestId":"<id>","attempt":1,
                          "maxAttempts":3,"deadline":"<ISO-8601>",
                          "messageId":"<optional>","traceId":"<optional>",
                          "app":"<app>","handler":"mod.func"}}
  bootstrap-> {"type":"result","id":"<same id>","ok":true}
              {"type":"result","id":"<same id>","ok":false,"error":"..."}
  The id is echoed exactly. "trace" is omitted when Relay has no trace context;
  "context" is always present. Error text is bounded so the serialized result
  stays within the 1 MiB response cap (and never exceeds 64 KiB), with
  truncation on a Unicode code-point boundary.

Process-model notes (intended semantics, not a bug):
  - Handler errors (Exceptions, SystemExit) fail THAT invocation (ok:false,
    stderr log) and the loop continues: the container is kept healthy.
  - A malformed frame, wrong frame type, bad authentication, or EOF after the
    channel is authenticated closes that connection. The bootstrap then exits
    cleanly so Relay detects the loss and discards the container.
  - The whole process owns ONE event loop, created once by asyncio.run() (with
    zuvloop's loop factory when the image was built with PYTHON_ZUVLOOP for an
    app that serves event or schedule handlers) and alive until the container
    exits. Requests are read and processed on that loop, one at a time (see
    serve_invocations). Sync handlers are called inline on the loop; an async
    handler (or any awaitable the handler returns) is awaited on that SAME
    loop. Module-level asyncio resources (a lock, a queue, an HTTP client
    session) therefore bind to one stable loop and stay usable across
    invocations; Relay never runs a per-invocation loop, so a warm resource is
    not "attached to a different event loop" on its second use.
  - sys.modules caches imported modules, so module-level state (a cache dict,
    a client, a connection) persists across invocations. Per-request env is
    applied EXACTLY per invocation: a key absent from the new frame is restored
    to its pre-Relay baseline (or removed) and unrelated OS/system/container
    variables are preserved (see apply_env). RELAY_HANDLER is reserved and
    overwritten every invocation, and the channel secret is never applied.
  - The optional "trace" object carries the W3C trace context Relay injected for
    this invocation. It is extracted and attached ONLY around the one handler
    call and always detached afterwards, so one invocation's context never leaks
    into the next despite the warm interpreter (see invoke()).
  - Every request frame carries a "context" object. The bootstrap builds a
    fresh HandlerContext for that invocation (never module or global mutable
    state) and passes it as the handler's second argument. Its deadline is an
    absolute, timezone-aware instant, and get_remaining_time_ms() reports the
    time left, clamped at zero.

Lifetime caveat: reuse is a warm-process optimization, never a durability or
lifetime guarantee. A container is discarded on a timeout, protocol error,
channel loss, or image change, and the process exits when its authenticated
connection ends; module state and any loop-bound resource created by a handler
are released with it.

Trace propagation:
  The runtime image installs the OpenTelemetry API (see the Python engine's
  managed install). The bootstrap extracts the incoming carrier with the global
  W3C propagator and attaches it for the handler call, so a user-installed OTel
  SDK resolves the SAME global context and creates child spans under Relay's
  invocation span. The trace context stays ambient. The invocation metadata is
  not: every handler call receives a fresh HandlerContext as its second
  argument, so the signature is func(event, context). The import is guarded so
  a bare interpreter without the API simply skips propagation instead of
  failing.
"""

import asyncio
import hashlib
import hmac
import importlib
import inspect
import json
import os
import secrets
import struct
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

# Build-time event-loop selector. The Python engine replaces this exact line
# (with `import zuvloop` and `ZUVLOOP_ENABLED = True`) when the worker-global
# PYTHON_ZUVLOOP setting applies to this image: the app declares at least one
# event or schedule handler and the image installed the pinned zuvloop package.
# A disabled image keeps this line verbatim, never imports zuvloop, and runs the
# standard asyncio loop. The import/loop initialization deliberately has no
# fallback: if an enabled image cannot import the package or create the loop,
# the container must fail startup rather than silently run a different loop.
ZUVLOOP_ENABLED = False

# OpenTelemetry API: installed into the runtime image next to uv. Import is
# guarded so running the bootstrap outside the image (or in a minimal test
# harness) degrades to no propagation rather than crashing the container.
try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate as otel_propagate
except ImportError:  # pragma: no cover - the runtime image always ships the API
    otel_context = None
    otel_propagate = None

# The function sources live in /app; put it on sys.path so imports resolve
# regardless of where this bootstrap script is located.
sys.path.insert(0, "/app")

# Relay's execution-channel contract (see internal/runtime/protocol.go in the
# Relay source). The limits bound the JSON payload only, excluding the 4-byte
# length header.
EXECUTION_PORT_ENV = "RELAY_EXECUTION_PORT"
EXECUTION_SECRET_ENV = "RELAY_EXECUTION_SECRET"
PROTOCOL_VERSION = 1
MAX_HANDSHAKE_BYTES = 4 * 1024
MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 1 * 1024 * 1024
MAX_ERROR_BYTES = 64 * 1024
HANDSHAKE_TIMEOUT_SECONDS = 2.0
MAX_CONCURRENT_HANDSHAKES = 8
BOOTSTRAP_AUTH_CONTEXT = "relay-bootstrap-auth-v1:"
CLIENT_AUTH_CONTEXT = "relay-client-auth-v1:"


class ProtocolError(Exception):
    """A fatal violation of the authenticated execution-channel protocol."""


class ConnectionAdmission:
    """Process-local admission for the container's single execution channel.

    Only one authenticated connection is served. Handshakes are bounded rather
    than queued: a sibling container cannot leave unbounded tasks or sockets
    waiting behind the warm authenticated session, and once Relay is
    authenticated every additional connection is closed immediately. The
    counters are only mutated on the event loop, with no awaits between the
    admission check and its update.
    """

    __slots__ = ("handshakes", "authenticated")

    def __init__(self):
        self.handshakes = 0
        self.authenticated = False


def _is_lower_hex_64(value):
    """Whether value is the protocol's 64-character lowercase hex form."""
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(ch in "0123456789abcdef" for ch in value)


def _is_protocol_version(value):
    """Whether value is the integer protocol version. JSON true compares equal
    to 1 in Python but is not a version and must be rejected."""
    return type(value) is int and value == PROTOCOL_VERSION


def _load_execution_config():
    """Read and validate Relay's channel configuration, then remove the secret
    from this process's environment before any handler module can be imported.

    Both values are Relay-owned and fail closed: a missing or malformed port or
    secret means the container must not start. The secret is the ASCII HMAC key
    and is never logged or framed.
    """
    secret = os.environ.get(EXECUTION_SECRET_ENV)
    port_text = os.environ.get(EXECUTION_PORT_ENV)
    os.environ.pop(EXECUTION_SECRET_ENV, None)
    if not _is_lower_hex_64(secret):
        raise RuntimeError("invalid Relay execution configuration")
    if (
        not isinstance(port_text, str)
        or not port_text.isascii()
        or not port_text.isdigit()
        or not 1 <= int(port_text) <= 65535
    ):
        raise RuntimeError("invalid Relay execution configuration")
    return secret, int(port_text)


EXECUTION_SECRET, EXECUTION_PORT = _load_execution_config()


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


def _parse_deadline(value):
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
        raise ValueError(
            "context is missing required field(s): " + ", ".join(missing)
        )
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
        deadline=_parse_deadline(raw["deadline"]),
        message_id=message_id,
        trace_id=trace_id,
        app=raw["app"],
        handler=raw["handler"],
    )


# User print() output is the container's native log, not protocol data. Keep the
# text layer line-buffered so it appears live while the container stays warm.
try:
    sys.stdout.reconfigure(line_buffering=True)
except (AttributeError, ValueError):
    pass

# Relay-managed invocation env: exact per-request application without wiping the
# process's OS/system/container variables.
#
# The bootstrap is a long-lived, reused process, so per-request env keys would
# otherwise linger from an earlier invocation. Instead of blindly writing every
# frame value into os.environ (which leaked a removed/rotated key into later
# invocations), the bootstrap tracks only the keys Relay itself applies:
#   _applied  — the keys currently owned by Relay in os.environ
#   _baseline — for each owned key, the value os.environ had BEFORE Relay first
#               overrode it, or _MISSING when it did not exist then
# On each request the env is applied EXACTLY: a previously applied key absent
# from the new frame is restored to its pre-Relay baseline (or deleted), a
# present key is set to the new value (its baseline captured once, on first
# application), and every unrelated variable is left untouched. Cleanup runs at
# the top of each request, before the next env is applied, so it is robust even
# when the previous handler raised or called SystemExit. The channel secret is
# never applied or restored, so a frame can never reintroduce it.
_MISSING = object()
_applied = set()
_baseline = {}


def apply_env(env):
    """Apply one request frame's env to os.environ exactly, without wiping
    unrelated process variables. A key Relay applied in an earlier invocation
    that this frame no longer carries is restored to its pre-Relay value (or
    removed), so a rotated or removed env/secret never leaks forward; a key that
    is present is set to its new value. Only keys Relay applies are tracked."""
    incoming = {}
    if isinstance(env, dict):
        for key, value in env.items():
            name = str(key)
            if name == EXECUTION_SECRET_ENV:
                continue
            incoming[name] = str(value)
    os.environ.pop(EXECUTION_SECRET_ENV, None)
    for key in list(_applied):
        if key not in incoming:
            prior = _baseline.pop(key, _MISSING)
            if prior is _MISSING:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prior
            _applied.discard(key)
    for key, value in incoming.items():
        if key not in _applied:
            _baseline[key] = os.environ.get(key, _MISSING)
        os.environ[key] = value
        _applied.add(key)


def _dumps(value):
    """Serialize one frame payload without incidental whitespace."""
    return json.dumps(value, separators=(",", ":"))


def _json_escaped_bytes(ch):
    """Number of UTF-8 bytes one Unicode character contributes inside a JSON
    string (its escaped form, without the surrounding quotes). JSON escaping is
    per-character, so summing these is exactly the error field's serialized
    contribution."""
    return len(json.dumps(ch)[1:-1].encode("utf-8"))


def _bounded_error(req_id, message):
    """Return an error string such that the whole serialized result payload
    fits in MAX_RESPONSE_BYTES and the error itself stays within MAX_ERROR_BYTES,
    while always remaining valid UTF-8.

    The budget is the smaller of the diagnostic cap and the response cap minus
    the exact size of the envelope with an empty error. The message is then
    accumulated one Unicode character at a time (including any JSON escaping of
    quotes, backslashes, and control characters), so the payload is bounded
    precisely, a multibyte character is never cut mid-byte, and the result always
    round-trips through json.dumps (the emitted frame is parseable).
    """
    overhead = len(
        _dumps({"type": "result", "id": req_id, "ok": False, "error": ""}).encode(
            "utf-8"
        )
    )
    budget = min(MAX_ERROR_BYTES, MAX_RESPONSE_BYTES - overhead)
    if budget <= 0:
        return ""
    parts = []
    used = 0
    for ch in message:
        n = _json_escaped_bytes(ch)
        if used + n > budget:
            break
        parts.append(ch)
        used += n
    return "".join(parts)


def _result_payload(req_id, ok, error=None):
    """Build one result payload, bounding the error and enforcing the response
    cap. A result that cannot fit is a protocol failure: Relay would reject an
    oversized frame anyway."""
    if ok:
        frame = {"type": "result", "id": req_id, "ok": True}
    else:
        frame = {
            "type": "result",
            "id": req_id,
            "ok": False,
            "error": _bounded_error(req_id, "" if error is None else str(error)),
        }
    payload = _dumps(frame).encode("utf-8")
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ProtocolError("result payload exceeds the response frame limit")
    return payload


async def _read_frame(reader, max_bytes):
    """Read one length-prefixed frame, rejecting zero and oversized lengths
    before allocating, and requiring the complete payload."""
    header = await reader.readexactly(4)
    (size,) = struct.unpack(">I", header)
    if size == 0:
        raise ProtocolError("empty frame")
    if size > max_bytes:
        raise ProtocolError(
            f"frame of {size} bytes exceeds the {max_bytes}-byte limit"
        )
    return await reader.readexactly(size)


async def _write_frame(writer, payload, max_bytes):
    """Write one length-prefixed frame completely, then wait for the transport
    to drain its buffer."""
    if not payload:
        raise ProtocolError("empty frame")
    if len(payload) > max_bytes:
        raise ProtocolError(
            f"frame of {len(payload)} bytes exceeds the {max_bytes}-byte limit"
        )
    writer.write(struct.pack(">I", len(payload)) + payload)
    await writer.drain()


def _reject_json_constant(value):
    """Reject JSON extensions Python accepts but the wire contract (and
    JSON.parse) does not: NaN, Infinity, and -Infinity."""
    raise ValueError(f"non-standard JSON constant {value}")


def _decode_json(payload):
    """Decode one UTF-8 JSON frame payload, rejecting malformed input and
    non-standard constants before any handler can run."""
    try:
        return json.loads(payload.decode("utf-8"), parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError(f"invalid JSON frame: {exc}") from exc


def _challenge_mac(secret, auth_context, nonce):
    """Compute one handshake MAC in the protocol's lowercase hex form. The key
    is the ASCII secret and the message is the direction-specific context prefix
    plus the nonce exactly as sent."""
    message = (auth_context + nonce).encode("ascii")
    return hmac.new(secret.encode("ascii"), message, hashlib.sha256).hexdigest()


async def handshake(reader, writer, secret):
    """Authenticate Relay and leave the connection ready for invoke frames.

    The bootstrap answers Relay's hello with its own nonce and MAC, then requires
    Relay to prove knowledge of the secret over the bootstrap nonce before any
    handler can run. Both MAC checks use constant-time comparison. Raise
    ProtocolError (or an I/O error) on any mismatch or unexpected frame.
    """
    hello = _decode_json(await _read_frame(reader, MAX_HANDSHAKE_BYTES))
    if (
        not isinstance(hello, dict)
        or hello.get("type") != "hello"
        or not _is_protocol_version(hello.get("version"))
    ):
        raise ProtocolError("unexpected bootstrap hello frame")
    relay_nonce = hello.get("nonce")
    if not _is_lower_hex_64(relay_nonce):
        raise ProtocolError("malformed Relay nonce")

    bootstrap_nonce = secrets.token_hex(32)
    auth = {
        "type": "auth",
        "version": PROTOCOL_VERSION,
        "mac": _challenge_mac(secret, BOOTSTRAP_AUTH_CONTEXT, relay_nonce),
        "nonce": bootstrap_nonce,
    }
    await _write_frame(writer, _dumps(auth).encode("utf-8"), MAX_HANDSHAKE_BYTES)

    ready = _decode_json(await _read_frame(reader, MAX_HANDSHAKE_BYTES))
    if (
        not isinstance(ready, dict)
        or ready.get("type") != "ready"
        or not _is_protocol_version(ready.get("version"))
    ):
        raise ProtocolError("unexpected Relay ready frame")
    mac = ready.get("mac")
    if not _is_lower_hex_64(mac):
        raise ProtocolError("Relay authentication failed")
    expected = _challenge_mac(secret, CLIENT_AUTH_CONTEXT, bootstrap_nonce)
    if not hmac.compare_digest(mac, expected):
        raise ProtocolError("Relay authentication failed")


def extract_context(trace):
    """Extracts the invocation's W3C trace context and attaches it, returning
    the detach token (or None when there is nothing to attach). The caller MUST
    detach in a finally: this is the per-invocation boundary that keeps a warm
    container's ambient context from leaking across invocations, on success and
    on failure alike."""
    if (
        otel_context is None
        or otel_propagate is None
        or not isinstance(trace, dict)
        or not trace
    ):
        return None
    # A dict carrier is exactly what the JSON "trace" object decodes to, so the
    # global W3C propagator (TraceContext + Baggage) reads it directly.
    parent = otel_propagate.extract(trace)
    return otel_context.attach(parent)


def detach_context(token):
    """Detaches a context attached by extract_context. Safe on None and never
    raises: a detach failure must not turn a healthy invocation into a
    protocol failure."""
    if token is None or otel_context is None:
        return
    try:
        otel_context.detach(token)
    except Exception:
        pass


async def invoke(handler, event, context_frame, trace=None):
    """Runs one handler on the process-lifetime loop. Handler/user errors are
    converted to an error string here; the caller reports them as an ok:false
    result and keeps the container reusable.

    The import, the handler lookup, a sync handler's call, and the await of an
    async handler all happen on the SAME loop (the one serve_invocations runs
    on), so a module-level asyncio resource reused by a later invocation is still
    bound to a live loop.

    A fresh HandlerContext is built from context_frame for this call and passed
    as the handler's second argument. The invocation's trace context (if any) is
    attached for the single handler call and detached in a finally, so the warm
    interpreter's ambient context is restored whatever the handler does (return,
    raise, or exit)."""
    module_name, _, func_name = handler.rpartition(".")
    if not module_name or not func_name:
        return f"invalid handler {handler!r}"

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        return f"failed to import module {module_name!r}: {exc}"

    func = getattr(module, func_name, None)
    if func is None:
        return f"module {module_name!r} has no function {func_name!r}"

    try:
        context = build_handler_context(context_frame)
    except (TypeError, ValueError) as exc:
        return f"invalid handler context: {exc}"

    token = extract_context(trace)
    try:
        result = func(event, context)
        if inspect.isawaitable(result):
            await result  # on the process-lifetime loop, never a fresh one
    except SystemExit as exc:
        code = exc.code
        if code is None or code is True:
            return f"handler {handler!r} exited"
        return f"handler {handler!r} exited with code {code}"
    except asyncio.CancelledError:
        # Task cancellation is control flow, not a handler failure: re-raise so
        # asyncio.run can shut the connection loop down and join its tasks.
        raise
    except BaseException as exc:
        # BaseException: SystemExit is handled above, KeyboardInterrupt and
        # anything the handler raises is a failed INVOCATION, not a fatality.
        return f"handler {handler!r} failed: {exc}"
    finally:
        detach_context(token)
    return None


def _parse_invoke_request(request):
    """Validate one invoke frame and return its routing/execution fields.

    The frame is Relay's complete invocation contract: a non-empty id, a string
    handler, an optional object/null env, a present event of any JSON value, an
    optional trace object, and a context object with every required identity
    field. A structural violation is a protocol error (the channel closes); it
    never runs a handler. The required context fields are validated here rather
    than defaulted, so a malformed identity is never fabricated from a partial
    frame.
    """
    if not isinstance(request, dict):
        raise ProtocolError("request frame is not a JSON object")
    if request.get("type") != "invoke":
        raise ProtocolError("unexpected execution-channel frame type")

    req_id = request.get("id")
    if not isinstance(req_id, str) or not req_id:
        raise ProtocolError("invocation id must be a non-empty string")
    handler = request.get("handler")
    if not isinstance(handler, str):
        raise ProtocolError("invocation handler must be a string")
    env = request.get("env")
    if env is not None and not isinstance(env, dict):
        raise ProtocolError("request env must be an object or null")
    if "event" not in request:
        raise ProtocolError("invocation frame is missing the event")
    trace = request.get("trace")
    if "trace" in request and not isinstance(trace, dict):
        raise ProtocolError("trace carrier must be an object")

    context = request.get("context")
    if not isinstance(context, dict):
        raise ProtocolError("request context must be an object")
    missing = [name for name in REQUIRED_CONTEXT_FIELDS if name not in context]
    if missing:
        raise ProtocolError(
            "request context is missing required field(s): " + ", ".join(missing)
        )
    for name in ("requestId", "deadline", "app", "handler"):
        if not isinstance(context[name], str):
            raise ProtocolError(f"context.{name} must be a string")
    for name in ("attempt", "maxAttempts"):
        value = context[name]
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProtocolError(f"context.{name} must be an integer")
    for name in ("messageId", "traceId"):
        if name in context and not isinstance(context[name], str):
            raise ProtocolError(f"context.{name} must be a string when present")
    if context["handler"] != handler:
        raise ProtocolError("context.handler does not match the invocation handler")

    # Reject an id that cannot fit any result envelope before a handler runs, so
    # an impossible response is a protocol failure with no execution side effect.
    if (
        len(
            _dumps(
                {"type": "result", "id": req_id, "ok": False, "error": ""}
            ).encode("utf-8")
        )
        > MAX_RESPONSE_BYTES
    ):
        raise ProtocolError("invocation id exceeds the response limit")

    return req_id, handler, env, context, trace


async def serve_invocations(reader, writer):
    """Serve invoke/result exchanges sequentially on one authenticated
    connection.

    Relay serializes warm-container use, so there is never more than one
    invocation in flight. The handler runs on this process's one event loop;
    reading awaits the connection, never a worker thread. Any protocol or
    transport failure propagates so the channel is closed and the process exits.
    """
    while True:
        request = _decode_json(await _read_frame(reader, MAX_REQUEST_BYTES))
        req_id, handler, env, context, trace = _parse_invoke_request(request)

        # Per-request environment: applied EXACTLY (see apply_env). Keys this
        # invocation no longer carries are restored to their pre-Relay baseline
        # (or removed), so a rotated/removed value never leaks forward; every
        # unrelated OS/system/container variable is preserved. A null or absent
        # env is empty.
        apply_env(env)
        os.environ["RELAY_HANDLER"] = handler

        error = await invoke(
            handler,
            request.get("event"),
            context,
            trace,
        )

        if error is not None:
            print(f"handler {handler!r} failed: {error}", file=sys.stderr)

        await _write_frame(
            writer, _result_payload(req_id, error is None, error), MAX_RESPONSE_BYTES
        )


# Admission is checked before any await: while Relay's authenticated session is
# active, an extra connection is closed immediately instead of being left queued
# behind the long-lived handler. Handshakes are bounded for the same reason; an
# unauthorized peer can only occupy a slot until HANDSHAKE_TIMEOUT_SECONDS.
async def _handle_connection(reader, writer, admission, shutdown):
    try:
        if (
            admission.authenticated
            or admission.handshakes >= MAX_CONCURRENT_HANDSHAKES
        ):
            return

        admission.handshakes += 1
        try:
            try:
                await asyncio.wait_for(
                    handshake(reader, writer, EXECUTION_SECRET),
                    timeout=HANDSHAKE_TIMEOUT_SECONDS,
                )
            except (
                asyncio.TimeoutError,
                asyncio.IncompleteReadError,
                ProtocolError,
                OSError,
            ):
                # Reject the peer without running a handler and keep the
                # listener available for Relay. Log no frame or MAC values.
                print("execution channel: handshake rejected", file=sys.stderr)
                return
        finally:
            admission.handshakes -= 1

        # No await since the handshake completed: this claim is atomic on the
        # event loop, so at most one authenticated session can ever be served.
        if admission.authenticated or shutdown.is_set():
            return
        admission.authenticated = True
        try:
            try:
                await serve_invocations(reader, writer)
            except asyncio.IncompleteReadError:
                # Relay closed the channel: a clean, expected end.
                pass
            except ProtocolError as exc:
                print(f"execution channel: {exc}", file=sys.stderr)
            except OSError as exc:
                print(f"execution channel: transport failure: {exc}", file=sys.stderr)
            except Exception as exc:
                print(f"execution channel: internal failure: {exc}", file=sys.stderr)
            finally:
                shutdown.set()
        finally:
            admission.authenticated = False
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def runtime_loop():
    """Bind the execution listener and run until the authenticated channel ends.

    The listener starts before any handler module is imported, so Relay's
    handshake is also the container's readiness signal. Relay and the workload
    share a private container network; this port is never published."""
    admission = ConnectionAdmission()
    shutdown = asyncio.Event()

    async def handle_connection(reader, writer):
        await _handle_connection(reader, writer, admission, shutdown)

    server = await asyncio.start_server(
        handle_connection, host="0.0.0.0", port=EXECUTION_PORT
    )
    async with server:
        await shutdown.wait()


def main():
    # ONE loop for the whole process lifetime: module-level asyncio resources
    # stay bound to it across invocations instead of being orphaned by a
    # per-invocation loop. The loop type is selected once, here at process
    # startup. The enabled branch requires the module installed by the engine's
    # managed build step; an import or loop-initialization failure propagates
    # and fails container startup, never falling back to the standard loop.
    if ZUVLOOP_ENABLED:
        asyncio.run(runtime_loop(), loop_factory=zuvloop.new_event_loop)
    else:
        asyncio.run(runtime_loop())


if __name__ == "__main__":
    main()
