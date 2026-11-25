"""Dependency declaration vocabulary shared by the injection modules."""

from __future__ import annotations

import enum
from collections.abc import Callable
from typing import Any, Literal, TypeAlias


class _DependencyError(RuntimeError):
    """A dependency declaration, resolution, or cleanup failed."""


class _Scope(str, enum.Enum):
    """Lifetime of a dependency-provided value."""

    INVOCATION = "invocation"
    CONTAINER = "container"


class _Injection(enum.Enum):
    """How one compiled parameter receives its value."""

    EVENT = "event"
    CONTEXT = "context"
    DEPENDENCY = "dependency"
    DEFAULT = "default"


_RESERVED_PARAMETERS = frozenset({"event", "context"})

_ProviderCallable: TypeAlias = Callable[..., Any]


def _coerce_scope(value: object) -> _Scope:
    if isinstance(value, _Scope):
        return value

    if isinstance(value, str):
        if value == _Scope.INVOCATION.value:
            return _Scope.INVOCATION
        if value == _Scope.CONTAINER.value:
            return _Scope.CONTAINER

    raise _DependencyError(
        f"unknown dependency scope {value!r}; use 'invocation' or 'container'"
    )


def _callable_name(func: _ProviderCallable) -> str:
    name = getattr(func, "__qualname__", None) or getattr(func, "__name__", None)
    if isinstance(name, str) and name:
        return name
    return repr(func)


class _Depends:
    """The marker object created by :func:`Depends`.

    ``scope`` controls the lifetime of the produced value. ``use_cache=False``
    asks for a fresh value every time this exact dependency is requested and
    makes the requesting invocation own its cleanup. The provider is identified
    by the callable object itself, so use a module-level function when a value
    must be shared by more than one handler.
    """

    __slots__ = ("dependency", "scope", "use_cache")

    def __init__(
        self,
        dependency: _ProviderCallable,
        *,
        scope: Literal["invocation", "container"] | _Scope = "invocation",
        use_cache: bool = True,
    ) -> None:
        if not callable(dependency):
            raise TypeError(
                f"Depends() expects a callable dependency, got {dependency!r}"
            )

        if not isinstance(use_cache, bool):
            raise TypeError("Depends(use_cache=...) must be a bool")

        self.dependency = dependency
        self.scope = _coerce_scope(scope)
        self.use_cache = use_cache

        if self.scope is _Scope.CONTAINER and not self.use_cache:
            raise _DependencyError(
                "Depends(scope='container', use_cache=False) is not supported; "
                "container-scoped dependencies are shared. Use "
                "scope='invocation' for uncached values."
            )

    def __repr__(self) -> str:
        return (
            f"Depends({self.dependency!r}, scope={self.scope.value!r}, "
            f"use_cache={self.use_cache!r})"
        )


def Depends(
    dependency: _ProviderCallable,
    *,
    scope: Literal["invocation", "container"] = "invocation",
    use_cache: bool = True,
) -> Any:
    """Mark a handler or provider parameter as dependency-injected.

    The return value is ``Any`` for the type checker so a parameter keeps its
    declared annotation (for example ``db: Database = Depends(get_db)``) while
    Relay injects the provider's value. ``scope`` selects ``"invocation"`` or
    ``"container"`` lifetime; ``use_cache=False`` disables sharing and is valid
    only for invocation scope.
    """
    return _Depends(dependency, scope=scope, use_cache=use_cache)
