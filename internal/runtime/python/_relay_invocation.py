"""One handler call on the process-lifetime event loop.

Embedded beside the bootstrap and written to ``/relay/_relay_invocation.py`` in
the runtime image.

Process-model notes (intended semantics, not a bug):

  - The whole process owns ONE event loop, created once by ``asyncio.run()`` in
    the bootstrap and alive until the container exits. Sync handlers are called
    inline on the loop; an async handler (or any awaitable the handler returns)
    is awaited on that SAME loop. Module-level asyncio resources (a lock, a
    queue, an HTTP client session) therefore bind to one stable loop and stay
    usable across invocations: Relay never runs a per-invocation loop, so a warm
    resource is not "attached to a different event loop" on its second use.
  - Handler errors (Exceptions, SystemExit) fail THAT invocation (ok:false,
    stderr log) and the loop continues: the container is kept healthy. Task
    cancellation is control flow, not a handler failure, and is re-raised so the
    connection loop can shut down and join its tasks.
  - A fresh HandlerContext is built from the frame for this call and passed as
    the handler's second argument, so the signature is ``func(event, context)``.
    The trace context is attached before the handler module is imported and
    detached only after the whole attempt (module import, context build, call,
    and dependency cleanup), so import-time instrumentation joins Relay's
    invocation trace and one invocation's context never leaks into the next
    despite the warm interpreter.
  - The shared logging context is bound before the trace context and before the
    handler module is imported, then expired and restored in the outer finally.
    Import-time, call-time, and dependency-cleanup log records therefore carry
    this invocation's identity, and a warm container never attributes a later
    record to an earlier invocation. A child task that inherited the context
    after the invocation ended sees it expired instead of stale.
"""

import asyncio
import inspect

import _relay_handlers as handlers
import _relay_logging_context as logging_context
import _relay_tracing as tracing
from _relay_context import build_handler_context

# Text for an error whose own rendering fails. Deliberately generic: reporting
# must not inspect arbitrary exception attributes to describe the failure.
_UNPRINTABLE_ERROR = "an unprintable error"


async def invoke(handler, event, context_frame, trace=None):
    """Run one handler on the process-lifetime loop.

    Handler and resolution errors are converted to ONE operator-visible string
    here. The caller both logs that string and returns it as the ok:false
    result, so the text is formatted exactly once. Return None on success.
    """
    log_token = logging_context.begin(context_frame)
    try:
        trace_token = tracing.attach_invocation_context(trace)
        try:
            try:
                func = handlers.load(handler)
            except handlers.HandlerLoadError as exc:
                return _handler_failure(handler, _error_text(exc))

            try:
                context = build_handler_context(context_frame)
            except (TypeError, ValueError) as exc:
                return _handler_failure(
                    handler,
                    f"invalid handler context: {_error_text(exc)}",
                )

            try:
                result = func(event, context)
                if inspect.isawaitable(result):
                    await result  # on the process-lifetime loop, never a fresh one
            except SystemExit as exc:
                return _exit_message(handler, exc.code)
            except asyncio.CancelledError:
                # Task cancellation is control flow, not a handler failure:
                # re-raise so asyncio.run can shut the connection loop down and
                # join its tasks.
                raise
            except BaseException as exc:
                # BaseException: SystemExit is handled above, KeyboardInterrupt
                # and anything the handler raises is a failed INVOCATION, not a
                # fatality.
                return _handler_failure(handler, _error_text(exc))

            return None
        finally:
            tracing.detach_invocation_context(trace_token)
    finally:
        logging_context.end(log_token)


def _handler_failure(handler, detail):
    """Format every handler, import, or context failure the same way."""
    return f"handler {handler!r} failed: {detail}"


def _error_text(exc):
    """Render one error for the operator and the result payload.

    Best effort: a custom ``__str__`` (or an argument's) that raises must not
    escape the invocation boundary and turn a reported failure into a broken
    channel. ``BaseException`` is covered because a hostile renderer can raise
    any of it; the fallback is fixed text.
    """
    try:
        return str(exc)
    except BaseException:
        return _UNPRINTABLE_ERROR


def _exit_message(handler, code):
    """Describe a ``SystemExit`` without letting its code's text escape."""
    if code is None or code is True:
        return f"handler {handler!r} exited"
    try:
        rendered = str(code)
    except BaseException:
        return f"handler {handler!r} exited"
    return f"handler {handler!r} exited with code {rendered}"
