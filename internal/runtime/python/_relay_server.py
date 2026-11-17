"""The authenticated execution server: listener, admission, and exchanges.

Embedded beside the bootstrap and written to ``/relay/_relay_server.py`` in the
runtime image.

Relay owns exactly one long-lived connection per container and exchanges one
invocation/result at a time over it. A malformed frame, wrong frame type, bad
authentication, or EOF after the channel is authenticated closes that connection
and the bootstrap exits cleanly, so Relay detects the loss and discards the
container. The listener starts before any handler module is imported, so Relay's
handshake is also the container's readiness signal. Relay and the workload share
a private container network; this port is never published.
"""

import asyncio
import sys

import _relay_environment as environment
import _relay_lifecycle as lifecycle
import _relay_protocol as protocol
from _relay_config import ExecutionConfig
from _relay_context import REQUIRED_CONTEXT_FIELDS
from _relay_invocation import invoke

HANDSHAKE_TIMEOUT_SECONDS = 2.0
MAX_CONCURRENT_HANDSHAKES = 8


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


def validate_invoke_request(request):
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
        raise protocol.ProtocolError("request frame is not a JSON object")
    if request.get("type") != "invoke":
        raise protocol.ProtocolError("unexpected execution-channel frame type")

    req_id = request.get("id")
    if not isinstance(req_id, str) or not req_id:
        raise protocol.ProtocolError("invocation id must be a non-empty string")
    handler = request.get("handler")
    if not isinstance(handler, str):
        raise protocol.ProtocolError("invocation handler must be a string")
    env = request.get("env")
    if env is not None and not isinstance(env, dict):
        raise protocol.ProtocolError("request env must be an object or null")
    if "event" not in request:
        raise protocol.ProtocolError("invocation frame is missing the event")
    trace = request.get("trace")
    if "trace" in request and not isinstance(trace, dict):
        raise protocol.ProtocolError("trace carrier must be an object")

    context = request.get("context")
    if not isinstance(context, dict):
        raise protocol.ProtocolError("request context must be an object")
    missing = [name for name in REQUIRED_CONTEXT_FIELDS if name not in context]
    if missing:
        raise protocol.ProtocolError(
            "request context is missing required field(s): " + ", ".join(missing)
        )
    for name in ("requestId", "deadline", "app", "handler"):
        if not isinstance(context[name], str):
            raise protocol.ProtocolError(f"context.{name} must be a string")
    for name in ("attempt", "maxAttempts"):
        value = context[name]
        if not isinstance(value, int) or isinstance(value, bool):
            raise protocol.ProtocolError(f"context.{name} must be an integer")
    for name in ("messageId", "traceId"):
        if name in context and not isinstance(context[name], str):
            raise protocol.ProtocolError(
                f"context.{name} must be a string when present"
            )
    if context["handler"] != handler:
        raise protocol.ProtocolError(
            "context.handler does not match the invocation handler"
        )

    # Reject an id that cannot fit any result envelope before a handler runs, so
    # an impossible response is a protocol failure with no execution side effect.
    if (
        len(
            protocol.dumps(
                {"type": "result", "id": req_id, "ok": False, "error": ""}
            ).encode("utf-8")
        )
        > protocol.MAX_RESPONSE_BYTES
    ):
        raise protocol.ProtocolError("invocation id exceeds the response limit")

    return req_id, handler, env, context, trace


async def serve_invocations(reader, writer):
    """Serve invoke/result exchanges sequentially on one authenticated
    connection.

    Relay serializes warm-container use, so there is never more than one
    invocation in flight. The handler runs on this process's one event loop;
    reading awaits the connection, never a worker thread. Any protocol or
    transport failure propagates so the channel is closed and the process exits.

    START is written once the frame is accepted, before its environment or
    handler runs; END is written only after the invocation completes. A
    cancellation raised by the handler propagates past END, so an interrupted
    invocation is not reported as completed.
    """
    while True:
        request = protocol.decode_json(
            await protocol.read_frame(reader, protocol.MAX_REQUEST_BYTES)
        )
        req_id, handler, env, context, trace = validate_invoke_request(request)

        lifecycle.start_invocation(context["requestId"])
        environment.apply_invocation_env(env, handler)

        error = await invoke(
            handler,
            request.get("event"),
            context,
            trace,
        )
        lifecycle.end_invocation(context["requestId"])

        if error is not None:
            print(f"handler {handler!r} failed: {error}", file=sys.stderr)

        await protocol.write_frame(
            writer,
            protocol.result_payload(req_id, error is None, error),
            protocol.MAX_RESPONSE_BYTES,
        )


# Admission is checked before any await: while Relay's authenticated session is
# active, an extra connection is closed immediately instead of being left queued
# behind the long-lived handler. Handshakes are bounded for the same reason; an
# unauthorized peer can only occupy a slot until HANDSHAKE_TIMEOUT_SECONDS.
async def _handle_connection(reader, writer, admission, shutdown, secret):
    try:
        if admission.authenticated or admission.handshakes >= MAX_CONCURRENT_HANDSHAKES:
            return

        admission.handshakes += 1
        try:
            try:
                await asyncio.wait_for(
                    protocol.handshake(reader, writer, secret),
                    timeout=HANDSHAKE_TIMEOUT_SECONDS,
                )
            except (
                asyncio.TimeoutError,
                asyncio.IncompleteReadError,
                protocol.ProtocolError,
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
            except protocol.ProtocolError as exc:
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


async def runtime_loop(config: ExecutionConfig):
    """Bind the execution listener and run until the authenticated channel ends.

    The listener starts before any handler module is imported, so Relay's
    handshake is also the container's readiness signal.
    """
    admission = ConnectionAdmission()
    shutdown = asyncio.Event()

    async def handle_connection(reader, writer):
        await _handle_connection(reader, writer, admission, shutdown, config.secret)

    server = await asyncio.start_server(
        handle_connection, host="0.0.0.0", port=config.port
    )
    async with server:
        await shutdown.wait()
