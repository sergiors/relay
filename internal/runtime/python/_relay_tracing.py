"""Per-invocation OpenTelemetry trace-context attachment.

Embedded beside the bootstrap and written to ``/relay/_relay_tracing.py`` in
the runtime image.

Relay's carrier is parsed with Relay's own W3C TraceContext and Baggage
propagators, not with the process's global propagator. An application that
installed a different global propagator (or none at all) therefore still gets
Relay's incoming trace context, and Relay never installs or changes the
application's propagator. The managed tracer provider itself is configured in
``_relay_telemetry`` and registered globally before the handler module is
imported, so spans created while importing the module already export.

An invocation without a carrier attaches an empty root context rather than
inheriting unrelated ambient context.

The invocation context stays ambient for the whole handler attempt — module
loading, the call, and dependency cleanup — and is detached in a finally, so a
warm container's context never leaks into the next invocation. The invocation
metadata does not live in the context (see ``_relay_invocation``), so every
handler has the signature ``func(event, context)``.

The import is guarded so a bare interpreter without the API degrades to no
propagation instead of failing.
"""

try:
    from opentelemetry import context as otel_context
except ImportError:  # pragma: no cover - the runtime image always ships the API
    otel_context = None

if otel_context is None:
    _TRACE_CONTEXT = None
    _BAGGAGE = None
else:
    try:
        from opentelemetry.baggage.propagation import W3CBaggagePropagator
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )
    except ImportError:  # pragma: no cover - the runtime image ships propagators
        _TRACE_CONTEXT = None
        _BAGGAGE = None
    else:
        _TRACE_CONTEXT = TraceContextTextMapPropagator()
        _BAGGAGE = W3CBaggagePropagator()


def attach_invocation_context(trace):
    """Extract and attach the invocation context, returning its detach token.

    The caller MUST detach in a finally: this is the per-invocation boundary
    that keeps a warm container's ambient context from leaking across
    invocations, on success and on failure alike. Extraction is best-effort:
    an absent carrier or trace extraction failure attaches an empty root context,
    and malformed baggage cannot discard an otherwise valid trace identity.
    None is returned when the API is unavailable or attachment fails.
    """
    if otel_context is None:
        return None
    if not isinstance(trace, dict) or not trace:
        return _attach_root_context()
    if _TRACE_CONTEXT is None or _BAGGAGE is None:
        return _attach_root_context()
    # A dict carrier is exactly what the JSON "trace" object decodes to, so the
    # explicit W3C propagators read it directly.
    try:
        parent = _TRACE_CONTEXT.extract(trace)
    except Exception:
        return _attach_root_context()
    try:
        parent = _BAGGAGE.extract(trace, parent)
    except Exception:
        pass
    try:
        return otel_context.attach(parent)
    except Exception:
        return _attach_root_context()


def _attach_root_context():
    try:
        return otel_context.attach(otel_context.Context())
    except Exception:
        return None


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
