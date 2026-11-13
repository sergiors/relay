"""Public handler interfaces for Python applications running on Relay.

The initial release exposes ``HandlerContext``, a Protocol describing the
context object Relay passes to each handler. Relay's Python bootstrap
constructs the concrete per-invocation value.
"""

from datetime import datetime
from typing import Protocol

__all__ = ["HandlerContext"]


class HandlerContext(Protocol):
    """Metadata Relay supplies for exactly one handler invocation.

    A handler receives a fresh instance as its second argument. Implementations
    are structural: any object with these attributes and method satisfies the
    contract, and handlers should treat the values as read-only.
    """

    @property
    def request_id(self) -> str:
        """Stable identity for one logical invocation.

        Retries, redeliveries, and worker restarts reuse it for the same invocation.
        State-free executions such as manual invocations and dead-letter replays get
        a fresh identity.
        """
        ...

    @property
    def attempt(self) -> int:
        """1-based attempt number, starting at 1 for the initial execution."""
        ...

    @property
    def max_attempts(self) -> int:
        """Configured total attempt budget (initial attempt plus retries).

        A configuration change can leave :attr:`attempt` above this value, so it is
        not an upper bound for the current attempt.
        """
        ...

    @property
    def deadline(self) -> datetime:
        """Absolute, timezone-aware instant when the invocation is cancelled."""
        ...

    @property
    def message_id(self) -> str | None:
        """Source stream message identity, or ``None`` when no source exists."""
        ...

    @property
    def trace_id(self) -> str | None:
        """W3C trace ID for the invocation, or ``None`` when tracing is absent."""
        ...

    @property
    def app(self) -> str:
        """Name of the app that owns the handler."""
        ...

    @property
    def handler(self) -> str:
        """Handler identity, in the same form used by the app manifest."""
        ...

    def get_remaining_time_ms(self) -> int:
        """Return milliseconds left before :attr:`deadline`, clamped at zero."""
        ...
