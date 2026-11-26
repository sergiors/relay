"""Relay's opt-in tracing helpers.

Import :class:`Tracer` from this module. It is deliberately not re-exported
from :mod:`relay_runtime`, and the OpenTelemetry API is only required when this
module is imported.
"""

try:
    from opentelemetry import trace as _trace_api
except ImportError:
    raise ImportError(
        "Install relay-runtime[tracing] to use relay_runtime.tracing."
    ) from None

from ._tracer import Tracer

__all__ = ["Tracer"]
