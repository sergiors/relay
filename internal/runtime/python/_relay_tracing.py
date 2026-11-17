"""Per-invocation OpenTelemetry trace-context attachment.

Embedded beside the bootstrap and written to ``/relay/_relay_tracing.py`` in
the runtime image.

The runtime image installs the OpenTelemetry API (see the Python engine's
managed install). Relay's incoming W3C trace context is extracted with the
global propagator and attached for the handler call only, so a user-installed
OTel SDK resolves the SAME global context and creates child spans under Relay's
invocation span. The trace context stays ambient; the invocation metadata does
not (see ``_relay_invocation``), so every handler has the signature
``func(event, context)``.

The import is guarded so a bare interpreter without the API degrades to no
propagation instead of failing.
"""

try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate as otel_propagate
except ImportError:  # pragma: no cover - the runtime image always ships the API
    otel_context = None
    otel_propagate = None


def attach_invocation_context(trace):
    """Extract the invocation's W3C trace context and attach it, returning the
    detach token (or None when there is nothing to attach).

    The caller MUST detach in a finally: this is the per-invocation boundary
    that keeps a warm container's ambient context from leaking across
    invocations, on success and on failure alike.
    """
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


def detach_invocation_context(token):
    """Detach a context attached by attach_invocation_context. Safe on None and
    never raises: a detach failure must not turn a healthy invocation into a
    protocol failure."""
    if token is None or otel_context is None:
        return

    try:
        otel_context.detach(token)
    except Exception:
        pass
