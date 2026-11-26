"""Invoke compiled callables and materialize dependency values."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Protocol

from ._cleanup import _Finalizer, start_async_generator, start_generator
from ._graph import _CallablePlan, _CompiledNode
from ._model import _DependencyError, _Injection


class _Resolver(Protocol):
    """State that can supply an invocation's special values and dependencies."""

    event: Any
    context: Any

    async def resolve(self, node: _CompiledNode) -> Any: ...


@dataclass(frozen=True)
class _CallOutcome:
    value: Any
    awaited: bool


@dataclass(frozen=True)
class _Produced:
    value: Any
    finalizer: _Finalizer | None
    async_created: bool


async def call(plan: _CallablePlan, state: _Resolver) -> _CallOutcome:
    positional: list[Any] = []
    keywords: dict[str, Any] = {}

    for parameter in plan.parameters:
        if parameter.injection is _Injection.EVENT:
            value = state.event
        elif parameter.injection is _Injection.CONTEXT:
            value = state.context
        elif parameter.injection is _Injection.DEPENDENCY:
            dependency = parameter.dependency
            if dependency is None:  # pragma: no cover - compiler invariant
                raise _DependencyError(
                    f"{plan.name} parameter {parameter.name!r}: "
                    "missing compiled dependency"
                )
            value = await state.resolve(dependency)
        else:
            value = parameter.default

        if parameter.positional:
            positional.append(value)
        else:
            keywords[parameter.name] = value

    result = plan.func(*positional, **keywords)
    if inspect.isawaitable(result):
        return _CallOutcome(await result, True)
    return _CallOutcome(result, False)


async def produce(
    node: _CompiledNode,
    state: _Resolver,
    *,
    reject_async_generator: bool = False,
) -> _Produced:
    try:
        outcome = await call(node.plan, state)
    except BaseException as exc:
        _note_dependency_failure(node, exc)
        raise

    result = outcome.value
    try:
        if inspect.isasyncgen(result):
            if reject_async_generator:
                await result.aclose()
                raise _DependencyError(
                    f"dependency {node.plan.name!r} produced an async "
                    "generator; a synchronous override_dependencies context "
                    "cannot start or clean it up. Use 'async with "
                    "override_dependencies(...)'."
                )
            value, finalizer = await start_async_generator(node.name, result)
            return _Produced(value, finalizer, True)
        if inspect.isgenerator(result):
            value, finalizer = start_generator(node.name, result)
            return _Produced(value, finalizer, outcome.awaited)
    except BaseException as exc:
        _note_dependency_failure(node, exc)
        raise

    return _Produced(result, None, outcome.awaited)


def _note_dependency_failure(node: _CompiledNode, exc: BaseException) -> None:
    if not isinstance(exc, asyncio.CancelledError):
        exc.add_note(f"while resolving dependency {node.name!r}")
