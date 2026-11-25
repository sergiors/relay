"""functools.wraps decorator composition around the @bind handler decorator.

Relay calls the outermost callable with exactly ``(event, context)`` and
awaits its result, so both supported orderings must keep dependency
resolution, the decorator's own bookkeeping, the wrapped handler's metadata,
and the published two-argument signature intact.
"""

import functools
import inspect
from typing import Any

from relay_runtime import Depends, bind

from di_support import InvocationContext, run_handler


def provide_activity() -> str:
    return "ping"


def activity_tracker() -> tuple[Any, list[str]]:
    """Return a functools.wraps decorator and its per-test activity log.

    The decorator brackets execution on both sides: when the wrapped callable
    answers with an awaitable (the bound handler always does), the trailing
    entry is recorded only after that awaitable settles.
    """

    events: list[str] = []

    def track_activity(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            events.append("before")
            result = func(*args, **kwargs)
            if not inspect.isawaitable(result):
                events.append("after")
                return result

            async def settle():
                try:
                    return await result
                finally:
                    events.append("after")

            return settle()

        return wrapper

    return track_activity, events


def test_bind_outside_a_wraps_decorator_resolves_and_tracks_activity() -> None:
    # given a wraps-based activity decorator around a dependency handler
    track_activity, events = activity_tracker()

    @bind
    @track_activity
    def greeting(event, context, activity: str = Depends(provide_activity)) -> str:
        """Handle a greeting."""
        events.append("handler")
        return f"{event['msg']}:{activity}"

    # when Relay invokes it with exactly two arguments
    result = run_handler(greeting, {"msg": "hi"}, InvocationContext())

    # then the dependency resolved inside the decorated body and the
    # decorator bracketed the handler execution
    assert result == "hi:ping"
    assert events == ["before", "handler", "after"]

    # and the wrapped handler's metadata survives both layers
    assert greeting.__name__ == "greeting"
    assert greeting.__doc__ == "Handle a greeting."

    # and the published signature is Relay's two-argument contract only
    assert list(inspect.signature(greeting).parameters) == ["event", "context"]


def test_wraps_decorator_outside_bind_keeps_the_two_argument_contract() -> None:
    # given a wraps-based activity decorator applied over @bind
    track_activity, events = activity_tracker()

    @track_activity
    @bind
    def search(event, context, activity: str = Depends(provide_activity)) -> str:
        """Search things."""
        events.append("handler")
        return f"{event['q']}:{activity}"

    # when Relay invokes the composed callable with exactly two arguments
    result = run_handler(search, {"q": "x"}, InvocationContext())

    # then the dependency resolved through both layers and the decorator
    # bracketed the full asynchronous execution
    assert result == "x:ping"
    assert events == ["before", "handler", "after"]

    # and the metadata stays relevant through both wraps layers
    assert search.__name__ == "search"
    assert search.__doc__ == "Search things."

    # and the outer wraps preserves the bound handler's published signature
    assert list(inspect.signature(search).parameters) == ["event", "context"]
