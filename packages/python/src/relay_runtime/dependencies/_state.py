"""Invocation-scoped and container-scoped dependency state."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NoReturn

from relay_runtime.typing import HandlerContext

from ._cleanup import (
    _Finalizer,
    _FinalizerStack,
    _run_cleanups,
    _run_sync_cleanups,
)
from ._execution import _Resolver, produce
from ._graph import _CleanupKind, _CompiledNode
from ._model import _DependencyError, _ProviderCallable, _Scope

_LOGGER = logging.getLogger("relay_runtime.dependencies")


@dataclass(frozen=True)
class _ContainerEntry:
    value: Any
    loop: asyncio.AbstractEventLoop | None


class ContainerState:
    """Container cache, provider identities, resources, and shutdown.

    Relay's warm container owns one instance. An override context owns an
    isolated instance with the same behavior. Relay serializes invocations in
    one process on one event loop, so the state needs no lock. It owns cached
    values, the loop each asynchronous value was created on, strong provider
    references that keep identity cache keys stable, the LIFO cleanup stack,
    and the shutdown transition.
    """

    def __init__(
        self,
        *,
        reject_async_cleanup: bool = False,
        is_closed: Callable[[], bool] | None = None,
    ) -> None:
        self._providers: dict[int, _ProviderCallable] = {}
        self._entries: dict[int, _ContainerEntry] = {}
        self._finalizers: list[_Finalizer] = []
        self._shutting_down = False
        self._reject_async_cleanup = reject_async_cleanup
        self._is_closed = is_closed

    async def resolve(self, node: _CompiledNode, invocation: _Resolver) -> Any:
        self._ensure_open()

        if self._reject_async_cleanup and node.cleanup is _CleanupKind.ASYNC:
            raise _DependencyError(
                f"container-scoped dependency {node.plan.name!r} is an async "
                "generator; a synchronous override_dependencies context cannot "
                "clean it up. Use 'async with override_dependencies(...)'."
            )

        key = id(node.dependency)
        entry = self._entries.get(key)
        if entry is not None:
            self._check_loop(node, entry)
            return entry.value

        produced = await produce(
            node,
            invocation,
            reject_async_generator=self._reject_async_cleanup,
        )
        if self._is_closed is not None and self._is_closed():
            await self._close_and_raise(
                produced.finalizer,
                "override_dependencies context has exited; its container "
                "dependencies cannot be resolved",
            )

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

    def _ensure_open(self) -> None:
        if self._is_closed is not None and self._is_closed():
            raise _DependencyError(
                "override_dependencies context has exited; its container "
                "dependencies cannot be resolved"
            )

    async def _close_and_raise(
        self, finalizer: _Finalizer | None, message: str
    ) -> NoReturn:
        if finalizer is not None:
            errors, cancelled = await _run_cleanups([finalizer])
            for error in errors:
                _LOGGER.error(
                    "container dependency cleanup failed while rejecting it",
                    exc_info=(type(error), error, error.__traceback__),
                )
            if cancelled:
                raise asyncio.CancelledError

        raise _DependencyError(message)

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
                return [_DependencyError("container dependency cleanup was cancelled")]
            except BaseException as exc:
                return [_DependencyError(f"container dependency cleanup failed: {exc}")]
        finally:
            self._shutting_down = False

        if cancelled:
            errors.append(
                _DependencyError("container dependency cleanup was cancelled")
            )

        return errors

    def shutdown_sync(self) -> list[BaseException]:
        """Close synchronous container resources without an event loop."""

        if self._shutting_down:
            return []
        self._shutting_down = True

        finalizers, self._finalizers = self._finalizers, []
        self._entries.clear()
        self._providers.clear()

        try:
            return _run_sync_cleanups(finalizers)
        finally:
            self._shutting_down = False


class InvocationState:
    """Per-invocation event/context values, cache, and cleanup stack."""

    def __init__(
        self,
        event: Any,
        context: HandlerContext,
        container: ContainerState,
        *,
        is_closed: Callable[[], bool] | None = None,
    ) -> None:
        self.event = event
        self.context = context
        self._container = container
        self._is_closed = is_closed
        self._cache: dict[_CompiledNode, Any] = {}
        self._finalizers = _FinalizerStack()

    async def resolve(self, node: _CompiledNode) -> Any:
        self._ensure_open()

        if node.scope is _Scope.CONTAINER:
            return await self._container.resolve(node, self)

        if node.use_cache and node in self._cache:
            return self._cache[node]

        produced = await produce(node, self)
        if produced.finalizer is not None:
            self._finalizers.append(produced.finalizer)
        self._ensure_open()
        if node.use_cache:
            self._cache[node] = produced.value
        return produced.value

    def _ensure_open(self) -> None:
        if self._is_closed is not None and self._is_closed():
            raise _DependencyError(
                "override_dependencies context has exited; dependencies cannot "
                "be resolved after the context manager exits"
            )

    async def close(
        self, active_exception: BaseException | None = None
    ) -> tuple[list[BaseException], bool]:
        return await self._finalizers.close(active_exception)


CONTAINER_STATE = ContainerState()
