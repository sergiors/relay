"""Context-scoped dependency overrides.

Overrides are matched by provider identity and applied only at ``Depends(...)``
references. Each active context owns an isolated container cache and its
resources, so entering or leaving a context never mutates the global container
state or another context's cache.
"""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Mapping
from types import TracebackType
from typing import Literal

from ._graph import _CallablePlan, _GraphCompiler
from ._model import _DependencyError, _ProviderCallable
from ._state import ContainerState

_LOGGER = logging.getLogger("relay_runtime.dependencies")

_OverrideTable = dict[int, tuple[_ProviderCallable, _ProviderCallable]]


class _OverrideScope:
    """The effective overrides, container cache, and compiled plans."""

    def __init__(self, overrides: _OverrideTable, *, sync_only: bool) -> None:
        self._overrides = overrides
        self.closed = False
        self._plans: dict[int, tuple[_CallablePlan, _CallablePlan]] = {}
        self.container = ContainerState(
            reject_async_cleanup=sync_only,
            is_closed=lambda: self.closed,
        )

    def replacement_for(self, provider: _ProviderCallable) -> _ProviderCallable | None:
        entry = self._overrides.get(id(provider))
        if entry is None:
            return None

        original, replacement = entry
        if original is not provider:
            return None
        if replacement is provider:
            return None
        return replacement

    def plan_for(self, plan: _CallablePlan) -> _CallablePlan:
        self.ensure_open()
        key = id(plan)
        cached = self._plans.get(key)
        if cached is not None and cached[0] is plan:
            return cached[1]

        compiled = _GraphCompiler(self).compile_handler(plan.func)
        self._plans[key] = (plan, compiled)
        return compiled

    def ensure_open(self) -> None:
        if self.closed:
            raise _DependencyError(
                "override_dependencies context has exited; dependency "
                "overrides cannot be used after the context manager exits"
            )

    def close_sync(self) -> list[BaseException]:
        if self.closed:
            return []
        self.closed = True
        try:
            return self.container.shutdown_sync()
        finally:
            self._plans.clear()

    async def close_async(self) -> list[BaseException]:
        if self.closed:
            return []
        self.closed = True
        try:
            return await self.container.shutdown()
        finally:
            self._plans.clear()


_ACTIVE_OVERRIDE: contextvars.ContextVar[_OverrideScope | None] = (
    contextvars.ContextVar("relay_runtime_dependency_overrides", default=None)
)


def _current_override() -> _OverrideScope | None:
    return _ACTIVE_OVERRIDE.get()


class _OverrideContext:
    """A reusable sync/async context manager for dependency overrides."""

    def __init__(
        self, overrides: Mapping[_ProviderCallable, _ProviderCallable]
    ) -> None:
        self._requested = overrides
        self._scope: _OverrideScope | None = None
        self._token: contextvars.Token[_OverrideScope | None] | None = None

    def __enter__(self) -> _OverrideContext:
        return self._enter(sync_only=True)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        scope, token = self._detach()
        try:
            errors = scope.close_sync()
        finally:
            _ACTIVE_OVERRIDE.reset(token)
            self._scope = None
            self._token = None
        _report_cleanup_errors(errors, exc)
        return False

    async def __aenter__(self) -> _OverrideContext:
        return self._enter(sync_only=False)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        scope, token = self._detach()
        try:
            errors = await scope.close_async()
        finally:
            _ACTIVE_OVERRIDE.reset(token)
            self._scope = None
            self._token = None
        _report_cleanup_errors(errors, exc)
        return False

    def _enter(self, *, sync_only: bool) -> _OverrideContext:
        if self._token is not None:
            raise RuntimeError(
                "override_dependencies context manager is already active"
            )
        if not isinstance(self._requested, Mapping):
            raise TypeError(
                "override_dependencies() expects a mapping of provider callables"
            )

        inherited = _ACTIVE_OVERRIDE.get()
        if inherited is not None and inherited.closed:
            raise _DependencyError(
                "override_dependencies cannot be entered from a task whose "
                "parent context has already exited"
            )

        requested = _snapshot_overrides(self._requested)
        scope = _OverrideScope(
            _merge_overrides(inherited, requested),
            sync_only=sync_only,
        )
        token = _ACTIVE_OVERRIDE.set(scope)
        self._scope = scope
        self._token = token
        return self

    def _detach(
        self,
    ) -> tuple[_OverrideScope, contextvars.Token[_OverrideScope | None]]:
        scope = self._scope
        token = self._token
        if scope is None or token is None:
            raise RuntimeError("override_dependencies context manager was not entered")
        return scope, token


def _snapshot_overrides(
    overrides: Mapping[_ProviderCallable, _ProviderCallable],
) -> _OverrideTable:
    table: _OverrideTable = {}
    for original, replacement in overrides.items():
        if not callable(original):
            raise TypeError(
                "override_dependencies() keys must be provider callables, "
                f"got {original!r}"
            )

        if not callable(replacement):
            raise TypeError(
                "override_dependencies() values must be provider callables, "
                f"got {replacement!r}"
            )

        if replacement is original:
            continue

        table[id(original)] = (original, replacement)

    return table


def _merge_overrides(
    inherited: _OverrideScope | None, requested: _OverrideTable
) -> _OverrideTable:
    merged: _OverrideTable = {} if inherited is None else dict(inherited._overrides)
    merged.update(requested)
    return merged


def _report_cleanup_errors(
    errors: list[BaseException], active: BaseException | None
) -> None:
    if not errors:
        return

    if active is not None:
        for error in errors:
            _LOGGER.error(
                "override_dependencies cleanup failed after an active error",
                exc_info=(type(error), error, error.__traceback__),
            )
        return

    for error in errors[1:]:
        _LOGGER.error(
            "additional override_dependencies cleanup failure",
            exc_info=(type(error), error, error.__traceback__),
        )
    raise errors[0]


def override_dependencies(
    overrides: Mapping[_ProviderCallable, _ProviderCallable],
) -> _OverrideContext:
    """Override providers for the current context.

    ``overrides`` maps each original provider callable to a replacement
    callable. Keys are matched by identity, the mapping is snapshotted when the
    context is entered, and the replacement inherits the original declaration's
    scope and ``use_cache`` setting. Use the result as either ``with`` or
    ``async with``.

    Only dependencies resolved through ``@bind`` and ``Depends(...)`` are
    affected. The mapping is applied at each original declaration; a
    replacement provider's own dependencies are also resolved under the same
    overrides, but the replacement root is not replaced again. Mapping a
    provider to itself is a no-op.
    """

    return _OverrideContext(overrides)
