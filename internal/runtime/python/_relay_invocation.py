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
    The trace context is attached for the single handler call and detached in a
    finally, so one invocation's context never leaks into the next despite the
    warm interpreter.
"""

import asyncio
import inspect

import _relay_handlers as handlers
import _relay_tracing as tracing
from _relay_context import build_handler_context


async def invoke(handler, event, context_frame, trace=None):
    """Run one handler on the process-lifetime loop.

    Handler and resolution errors are converted to an error string here; the
    caller reports them as an ok:false result and keeps the container reusable.
    Return None on success.
    """
    try:
        func = handlers.load(handler)
    except handlers.HandlerLoadError as exc:
        return str(exc)

    try:
        context = build_handler_context(context_frame)
    except (TypeError, ValueError) as exc:
        return f"invalid handler context: {exc}"

    token = tracing.attach_invocation_context(trace)
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
        tracing.detach_invocation_context(token)

    return None
