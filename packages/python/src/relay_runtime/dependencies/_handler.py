"""The public ``@bind`` decorator and bound-handler invocation."""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar, overload

from relay_runtime.typing import HandlerContext

from ._execution import call
from ._graph import _CallablePlan, _GraphCompiler
from ._model import _DependencyError, _ProviderCallable
from ._override import _current_override
from ._state import CONTAINER_STATE, InvocationState

_R = TypeVar("_R")

_LOGGER = logging.getLogger("relay_runtime.dependencies")

__all__ = ["bind"]


@overload
def bind(
    func: Callable[..., Coroutine[Any, Any, _R]], /
) -> Callable[[Any, HandlerContext], Coroutine[Any, Any, _R]]: ...


@overload
def bind(
    func: Callable[..., Awaitable[_R]], /
) -> Callable[[Any, HandlerContext], Coroutine[Any, Any, _R]]: ...


@overload
def bind(
    func: Callable[..., _R], /
) -> Callable[[Any, HandlerContext], Coroutine[Any, Any, _R]]: ...


@overload
def bind(
    func: None = None, /
) -> Callable[
    [Callable[..., _R]], Callable[[Any, HandlerContext], Coroutine[Any, Any, _R]]
]: ...


def bind(func: _ProviderCallable | None = None, /) -> Any:
    """Decorate a handler so its ``Depends(...)`` parameters are resolved.

    Relay still calls the decorated handler with exactly ``(event, context)``.
    The wrapper is async because dependency providers may await; Relay already
    awaits a handler's awaitable result. The original function metadata is
    preserved through :func:`functools.wraps`, and the dependency graph is
    compiled once when the decorator is applied.
    """

    if func is None:
        return bind

    if not callable(func):
        raise TypeError(f"bind() expects a callable handler, got {func!r}")

    if getattr(func, "__relay_di_bound__", False):
        raise _DependencyError("handler is already decorated with @bind")

    graph = _HandlerGraph(func)

    @functools.wraps(func)
    async def bound(event: Any, context: HandlerContext) -> Any:
        return await graph.invoke(event, context)

    setattr(bound, "__relay_di_bound__", True)
    # functools.wraps exposes the original signature through __wrapped__. Relay's
    # callable contract is still exactly (event, context); publish that signature
    # without touching the wrapped function's own metadata.
    setattr(
        bound,
        "__signature__",
        inspect.Signature(
            parameters=(
                inspect.Parameter(
                    "event",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Any,
                ),
                inspect.Parameter(
                    "context",
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=HandlerContext,
                ),
            ),
            return_annotation=Any,
        ),
    )
    return bound


class _HandlerGraph:
    def __init__(self, handler: _ProviderCallable) -> None:
        self._plan: _CallablePlan = _GraphCompiler().compile_handler(handler)

    async def invoke(self, event: Any, context: HandlerContext) -> Any:
        scope = _current_override()
        if scope is None:
            plan = self._plan
            state = InvocationState(event, context, CONTAINER_STATE)
        else:
            plan = scope.plan_for(self._plan)
            state = InvocationState(
                event,
                context,
                scope.container,
                is_closed=lambda: scope.closed,
            )

        try:
            outcome = await call(plan, state)
        except BaseException as exc:
            errors, cancelled = await state.close(exc)
            for error in errors:
                _LOGGER.error(
                    "dependency cleanup failed after handler error",
                    exc_info=(type(error), error, error.__traceback__),
                )
            if isinstance(exc, asyncio.CancelledError):
                raise
            if cancelled:
                raise asyncio.CancelledError from exc
            raise

        errors, cancelled = await state.close()
        if errors:
            for error in errors[1:]:
                _LOGGER.error(
                    "additional dependency cleanup failure",
                    exc_info=(type(error), error, error.__traceback__),
                )
            raise errors[0]
        if cancelled:
            raise asyncio.CancelledError
        return outcome.value
