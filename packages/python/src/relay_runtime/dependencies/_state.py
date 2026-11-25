"""Invocation-scoped and container-scoped dependency state."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from relay_runtime.typing import HandlerContext

from ._cleanup import _Finalizer, _FinalizerStack, _run_cleanups
from ._execution import _Resolver, produce
from ._graph import _CompiledNode
from ._model import _DependencyError, _ProviderCallable, _Scope


@dataclass(frozen=True)
class _ContainerEntry:
    value: Any
    loop: asyncio.AbstractEventLoop | None


class ContainerState:
    """Warm-container cache, provider identities, resources, and shutdown.

    Relay serializes invocations in one process on one event loop, so the
    state needs no lock. It deliberately owns every module-global container
    concern: cached values, the loop each asynchronous value was created on,
    strong provider references that keep identity cache keys stable, the LIFO
    cleanup stack, and the shutdown transition.
    """

    def __init__(self) -> None:
        self._providers: dict[int, _ProviderCallable] = {}
        self._entries: dict[int, _ContainerEntry] = {}
        self._finalizers: list[_Finalizer] = []
        self._shutting_down = False

    async def resolve(self, node: _CompiledNode, invocation: _Resolver) -> Any:
        key = id(node.dependency)
        entry = self._entries.get(key)
        if entry is not None:
            self._check_loop(node, entry)
            return entry.value

        produced = await produce(node, invocation)
        # Only asynchronous creation can bind a value to a loop; sync values
        # remain usable from any loop that drives the same warm container.
        loop = asyncio.get_running_loop() if produced.async_created else None

        # Keep the provider alive for as long as its cached entry exists so the
        # identity cache key cannot be reused by an unrelated callable.
        self._providers[key] = node.dependency
        self._entries[key] = _ContainerEntry(value=produced.value, loop=loop)
        if produced.finalizer is not None:
            self._finalizers.append(produced.finalizer)
        return produced.value

    def _check_loop(self, node: _CompiledNode, entry: _ContainerEntry) -> None:
        if entry.loop is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop is not entry.loop:
            raise _DependencyError(
                f"container-scoped dependency {node.name!r} was created on a "
                "different event loop; Relay container-scoped values are bound to "
                "the container lifetime loop"
            )

    async def shutdown(self) -> list[BaseException]:
        if self._shutting_down:
            return []
        self._shutting_down = True

        finalizers, self._finalizers = self._finalizers, []
        self._entries.clear()
        self._providers.clear()

        try:
            try:
                errors, cancelled = await _run_cleanups(finalizers)
            except asyncio.CancelledError:
                return [
                    _DependencyError("container dependency cleanup was cancelled")
                ]
            except BaseException as exc:
                return [
                    _DependencyError(f"container dependency cleanup failed: {exc}")
                ]
        finally:
            self._shutting_down = False

        if cancelled:
            errors.append(
                _DependencyError("container dependency cleanup was cancelled")
            )
        return errors


class InvocationState:
    """Per-invocation event/context values, cache, and cleanup stack."""

    def __init__(
        self,
        event: Any,
        context: HandlerContext,
        container: ContainerState,
    ) -> None:
        self.event = event
        self.context = context
        self._container = container
        self._cache: dict[_CompiledNode, Any] = {}
        self._finalizers = _FinalizerStack()

    async def resolve(self, node: _CompiledNode) -> Any:
        if node.scope is _Scope.CONTAINER:
            return await self._container.resolve(node, self)

        if node.use_cache and node in self._cache:
            return self._cache[node]

        produced = await produce(node, self)
        if node.use_cache:
            self._cache[node] = produced.value
        if produced.finalizer is not None:
            self._finalizers.append(produced.finalizer)
        return produced.value

    async def close(
        self, active_exception: BaseException | None = None
    ) -> tuple[list[BaseException], bool]:
        return await self._finalizers.close(active_exception)


CONTAINER_STATE = ContainerState()
