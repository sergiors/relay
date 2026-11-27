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
import importlib
import sys
import time

import _relay_environment as environment
import _relay_lifecycle as lifecycle
import _relay_memory as memory
import _relay_protocol as protocol
import _relay_report as report
import _relay_telemetry as telemetry
from _relay_config import ExecutionConfig
from _relay_context import REQUIRED_CONTEXT_FIELDS
from _relay_invocation import invoke

HANDSHAKE_TIMEOUT_SECONDS = 2.0
MAX_CONCURRENT_HANDSHAKES = 8

# The worker allows this process up to 10s after its authenticated channel
# closes before it kills the container (executionShutdownFlushGrace in the
# runtime package, capped to half the remaining manager budget). Shutdown keeps
# the managed tracer provider active while application container dependencies
# close, and only then spends the provider's own bounded flush. This budget
# bounds the wait for that cleanup; together with
# _relay_telemetry.SHUTDOWN_TIMEOUT_SECONDS it stays below the worker grace
# with room for interpreter exit. Both are named constants so the Go tests can
# pin the combined budget. A synchronous, blocking, or cancellation-resistant
# cleanup cannot be stopped safely from here: the budget bounds only this wait,
# and such a callback can still delay exit until the worker kills the
# container.
DEPENDENCY_CLEANUP_TIMEOUT_SECONDS = 1.0


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
    invocation is not reported as completed. A completed invocation (success or
    handled failure) is followed by one REPORT line carrying its monotonic
    duration and memory sample.
    """
    while True:
        request = protocol.decode_json(
            await protocol.read_frame(reader, protocol.MAX_REQUEST_BYTES)
        )
        req_id, handler, env, context, trace = validate_invoke_request(request)
        request_id = context["requestId"]

        lifecycle.start_invocation(request_id)
        started_at = time.perf_counter()
        environment.apply_invocation_env(env, handler)
        # The effective application environment is in place, so this is the
        # earliest point a managed provider sees the process's real OTel
        # configuration. It runs before the handler module is imported, so
        # import-time instrumentation exports under this invocation's trace.
        telemetry.ensure_started()
        error = await invoke(
            handler,
            request.get("event"),
            context,
            trace,
        )
        duration_ms = (time.perf_counter() - started_at) * 1000.0

        memory_usage = memory.sample_memory()
        lifecycle.end_invocation(request_id)
        report.report_invocation(request_id, duration_ms, memory_usage)

        if error is not None:
            # ``invoke`` already formatted the complete operator-visible text;
            # the same string is mirrored here and returned in the result.
            print(error, file=sys.stderr)

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


async def _shutdown_optional_dependencies():
    """Best-effort cleanup for the optional Python SDK's container resources.

    The SDK is not a runtime dependency, so an import failure simply means the
    app did not use it. Cleanup runs on the container-lifetime loop before
    asyncio.run closes it, while the managed tracer provider is still active, so
    spans and records it produces still export; an individual provider failure
    is reported and never prevents the remaining cleanup or the provider flush.

    The wait is bounded by DEPENDENCY_CLEANUP_TIMEOUT_SECONDS: cleanup that is
    still running then is cancelled and abandoned so the provider flush and
    process exit still fit inside the worker's drain grace. Cancelling cannot
    stop a synchronous, blocking, or cancellation-resistant cleanup, which can
    still delay exit until the worker kills the container.
    """
    # A handler that never imported the SDK cannot own container dependencies.
    # Do not import it merely to discover that there is nothing to clean up.
    if "relay_runtime" not in sys.modules:
        return

    try:
        module = importlib.import_module("relay_runtime.dependencies")
    except ImportError:
        return
    except Exception as exc:
        print(
            f"execution channel: dependency cleanup unavailable: {exc}",
            file=sys.stderr,
        )
        return

    shutdown_cleanup = getattr(module, "_shutdown_container_dependencies", None)
    if shutdown_cleanup is None:
        return

    cleanup = asyncio.create_task(shutdown_cleanup())
    try:
        await asyncio.wait({cleanup}, timeout=DEPENDENCY_CLEANUP_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        # Cancellation is control flow: stop the abandoned cleanup best effort
        # and let the caller's finally still attempt the bounded provider flush.
        cleanup.cancel()
        cleanup.add_done_callback(_consume_abandoned_cleanup)
        raise

    if not cleanup.done():
        cleanup.cancel()
        cleanup.add_done_callback(_consume_abandoned_cleanup)
        print(
            "execution channel: dependency cleanup abandoned after "
            f"{DEPENDENCY_CLEANUP_TIMEOUT_SECONDS:g}s",
            file=sys.stderr,
        )
        return

    try:
        errors = cleanup.result()
    except asyncio.CancelledError:
        # The cleanup task itself was cancelled; it is already finished, so
        # there is nothing left to report and the caller keeps its own control
        # flow.
        return
    except BaseException as exc:
        errors = [exc]

    for error in errors:
        print(
            f"execution channel: dependency cleanup failed: {error}",
            file=sys.stderr,
        )


def _consume_abandoned_cleanup(task: asyncio.Task) -> None:
    try:
        task.exception()
    except BaseException:
        pass


async def runtime_loop(config: ExecutionConfig):
    """Bind the execution listener and run until the authenticated channel ends.

    The listener starts before any handler module is imported, so Relay's
    handshake is also the container's readiness signal. Container-scoped SDK
    dependencies are closed after the channel ends, while the container's event
    loop that created them is still running and the managed OpenTelemetry
    provider is still active; that cleanup is waited for at most
    DEPENDENCY_CLEANUP_TIMEOUT_SECONDS. The provider is flushed afterwards,
    before the loop exits. Cleanup is best-effort but ordered: a cancelled or
    abandoned cleanup still leaves the provider one bounded shutdown attempt.
    """
    admission = ConnectionAdmission()
    shutdown = asyncio.Event()

    async def handle_connection(reader, writer):
        await _handle_connection(reader, writer, admission, shutdown, config.secret)

    server = await asyncio.start_server(
        handle_connection, host="0.0.0.0", port=config.port
    )
    try:
        async with server:
            await shutdown.wait()
    finally:
        try:
            await _shutdown_optional_dependencies()
        finally:
            telemetry.shutdown()
