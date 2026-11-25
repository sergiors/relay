"""Shared helpers for the dependency injection test suite."""

import asyncio
import inspect
from typing import Any


class InvocationContext:
    """HandlerContext-shaped stub standing in for Relay's per-invocation
    context when a bound handler is exercised without the bootstrap."""

    def __init__(self, request_id: str = "req-di") -> None:
        self.request_id = request_id
        self.attempt = 1
        self.max_attempts = 3

    def get_remaining_time_ms(self) -> int:
        return 30_000


async def _settle(awaitable: Any) -> Any:
    return await awaitable


def run_handler(bound: Any, event: Any, context: Any) -> Any:
    """Invoke bound(event, context) the way Relay does and settle the result.

    bind may answer with an awaitable whenever the handler or its dependency
    graph is asynchronous; Relay's invocation loop awaits exactly that, so the
    helper settles any awaitable on a fresh loop for synchronous test code.
    """
    result = bound(event, context)
    if inspect.isawaitable(result):
        return asyncio.run(_settle(result))
    return result
