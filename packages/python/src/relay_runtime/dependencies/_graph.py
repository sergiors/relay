"""Compile handler and provider signatures into immutable dependency graphs."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, ForwardRef, get_type_hints

from ._model import (
    _RESERVED_PARAMETERS,
    _DependencyError,
    _Depends,
    _Injection,
    _ProviderCallable,
    _Scope,
    _callable_name,
)


@dataclass(frozen=True)
class _Parameter:
    """One argument to supply when the compiled callable is invoked."""

    name: str
    injection: _Injection
    positional: bool
    dependency: _CompiledNode | None = field(default=None, repr=False)
    default: Any = None


@dataclass(frozen=True)
class _CallablePlan:
    """A validated callable together with its parameter resolutions."""

    func: _ProviderCallable
    name: str
    parameters: tuple[_Parameter, ...]


@dataclass(frozen=True, eq=False)
class _CompiledNode:
    """A provider node in a compiled dependency graph.

    Equality and hashing stay identity-based so a node can key the invocation
    cache without comparing recursively nested plans.
    """

    dependency: _ProviderCallable
    name: str
    scope: _Scope
    use_cache: bool
    plan: _CallablePlan = field(repr=False)


def _annotation_depends(
    parameter: inspect.Parameter, owner: str, annotation: Any
) -> _Depends | None:
    if isinstance(annotation, _Depends):
        return annotation

    metadata = getattr(annotation, "__metadata__", ())
    if not isinstance(metadata, tuple):
        return None

    markers = [item for item in metadata if isinstance(item, _Depends)]
    if len(markers) > 1:
        raise _DependencyError(
            f"{owner} parameter {parameter.name!r}: Annotated metadata declares "
            "more than one Depends(...)"
        )

    return markers[0] if markers else None


def _resolved_annotations(
    func: _ProviderCallable, signature: inspect.Signature
) -> tuple[dict[str, Any], Exception | None]:
    # Annotated Depends(...) may be postponed (PEP 563). Resolve annotations
    # when possible and keep the failure so a parameter that needs its
    # annotation for DI can fail with an actionable message.
    try:
        return get_type_hints(func, include_extras=True), None
    except Exception as exc:
        # Fall back per parameter so an unrelated unresolved annotation does
        # not hide a resolvable DI marker or a default Depends(...) declaration.
        resolved: dict[str, Any] = {}
        try:
            # functools.wraps copies annotations but not __globals__; postponed
            # strings belong to the innermost function's module namespace.
            target = inspect.unwrap(func)
        except ValueError:
            target = func
        target = getattr(target, "__func__", target)
        globalns = getattr(target, "__globals__", None)
        if not isinstance(globalns, dict):
            globalns = None

        for parameter in signature.parameters.values():
            annotation = parameter.annotation
            if annotation is inspect.Parameter.empty:
                continue

            if not isinstance(annotation, str):
                if isinstance(annotation, ForwardRef):
                    continue
                resolved[parameter.name] = annotation
                continue

            if globalns is None:
                continue

            try:
                resolved[parameter.name] = eval(annotation, globalns)
            except Exception:
                continue

        return resolved, exc


class _GraphCompiler:
    """Compile one handler's graph, reusing each provider by declaration."""

    def __init__(self) -> None:
        self._completed: dict[tuple[int, _Scope, bool], _CompiledNode] = {}
        self._active: list[tuple[tuple[int, _Scope, bool], str]] = []

    def compile_handler(self, handler: _ProviderCallable) -> _CallablePlan:
        return self._plan_callable(
            handler,
            _callable_name(handler),
            _Scope.INVOCATION,
            reserved_allowed=True,
            is_handler=True,
        )

    def _node(
        self, dependency: _ProviderCallable, scope: _Scope, use_cache: bool
    ) -> _CompiledNode:
        key = (id(dependency), scope, use_cache)
        existing = self._completed.get(key)
        if existing is not None:
            return existing

        name = _callable_name(dependency)
        if any(active_key == key for active_key, _ in self._active):
            path = " -> ".join([active_name for _, active_name in self._active] + [name])
            raise _DependencyError(f"dependency cycle detected: {path}")

        self._active.append((key, name))
        try:
            plan = self._plan_callable(
                dependency,
                name,
                scope,
                reserved_allowed=scope is _Scope.INVOCATION,
                is_handler=False,
            )
        finally:
            self._active.pop()

        node = _CompiledNode(
            dependency=dependency,
            name=name,
            scope=scope,
            use_cache=use_cache,
            plan=plan,
        )
        self._completed[key] = node
        return node

    def _plan_callable(
        self,
        func: _ProviderCallable,
        name: str,
        scope: _Scope,
        *,
        reserved_allowed: bool,
        is_handler: bool,
    ) -> _CallablePlan:
        try:
            signature = inspect.signature(func)
        except (TypeError, ValueError) as exc:
            raise _DependencyError(f"{name} is not inspectable: {exc}") from exc

        if is_handler and (
            inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func)
        ):
            raise _DependencyError(
                f"handler {name!r} must return a value, not yield; "
                "use a generator provider through Depends(...) instead"
            )

        annotations, annotation_error = _resolved_annotations(func, signature)
        parameters, seen_event, seen_context = self._plan_parameters(
            name,
            scope,
            signature,
            annotations,
            annotation_error,
            reserved_allowed=reserved_allowed,
        )

        if is_handler and (seen_event != 1 or seen_context != 1):
            raise _DependencyError(
                f"handler {name!r} must declare exactly one 'event' "
                "parameter and one 'context' parameter"
            )

        return _CallablePlan(func=func, name=name, parameters=tuple(parameters))

    def _plan_parameters(
        self,
        name: str,
        scope: _Scope,
        signature: inspect.Signature,
        annotations: dict[str, Any],
        annotation_error: Exception | None,
        *,
        reserved_allowed: bool,
    ) -> tuple[list[_Parameter], int, int]:
        parameters: list[_Parameter] = []
        seen_event = 0
        seen_context = 0

        for parameter in signature.parameters.values():
            if parameter.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                raise _DependencyError(
                    f"{name} parameter {parameter.name!r}: "
                    "*args and **kwargs are not supported in "
                    "dependency-injected callables"
                )

            positional = parameter.kind is inspect.Parameter.POSITIONAL_ONLY

            if parameter.name in _RESERVED_PARAMETERS:
                if not reserved_allowed:
                    raise _DependencyError(
                        f"{name} parameter {parameter.name!r}: container-scoped "
                        "dependencies cannot use the reserved "
                        f"{parameter.name!r} parameter"
                    )
                _reject_reserved_override(parameter, name, annotations)
                if parameter.name == "event":
                    seen_event += 1
                    injection = _Injection.EVENT
                else:
                    seen_context += 1
                    injection = _Injection.CONTEXT
                parameters.append(
                    _Parameter(
                        name=parameter.name,
                        injection=injection,
                        positional=positional,
                    )
                )
                continue

            depends = (
                parameter.default if isinstance(parameter.default, _Depends) else None
            )
            raw_annotation = parameter.annotation
            annotation = annotations.get(parameter.name, raw_annotation)
            if (
                depends is None
                and annotation_error is not None
                and parameter.name not in annotations
                and parameter.default is inspect.Parameter.empty
                and isinstance(raw_annotation, (str, ForwardRef))
            ):
                raise _DependencyError(
                    f"{name} parameter {parameter.name!r}: cannot resolve its "
                    "annotation to check for Annotated[..., Depends(...)]: "
                    f"{annotation_error}. Declare the dependency as a "
                    "Depends(...) default or make the annotation resolvable."
                ) from annotation_error

            annotated = _annotation_depends(parameter, name, annotation)
            if depends is not None and annotated is not None:
                raise _DependencyError(
                    f"{name} parameter {parameter.name!r}: declare Depends(...) "
                    "either as the default or in Annotated, not both"
                )
            if (
                annotated is not None
                and parameter.default is not inspect.Parameter.empty
            ):
                raise _DependencyError(
                    f"{name} parameter {parameter.name!r}: an Annotated "
                    "Depends(...) cannot also have a default value"
                )
            if depends is None:
                depends = annotated

            if depends is not None:
                child = self._node(depends.dependency, depends.scope, depends.use_cache)
                if scope is _Scope.CONTAINER and child.scope is not _Scope.CONTAINER:
                    raise _DependencyError(
                        f"container-scoped dependency {name!r} cannot depend on "
                        f"invocation-scoped dependency {child.name!r}"
                    )
                parameters.append(
                    _Parameter(
                        name=parameter.name,
                        injection=_Injection.DEPENDENCY,
                        positional=positional,
                        dependency=child,
                    )
                )
                continue

            if parameter.default is inspect.Parameter.empty:
                raise _DependencyError(
                    f"{name} parameter {parameter.name!r} must have a "
                    "Depends(...) default, an Annotated Depends(...), or an "
                    "ordinary default"
                )

            # Non-positional parameters with a plain default keep it in the
            # function itself; only positional-only arguments must be supplied.
            if positional:
                parameters.append(
                    _Parameter(
                        name=parameter.name,
                        injection=_Injection.DEFAULT,
                        positional=True,
                        default=parameter.default,
                    )
                )

        return parameters, seen_event, seen_context


def _reject_reserved_override(
    parameter: inspect.Parameter, owner: str, annotations: dict[str, Any]
) -> None:
    if isinstance(parameter.default, _Depends):
        raise _DependencyError(
            f"{owner} parameter {parameter.name!r}: the reserved "
            "parameter cannot use Depends(...)"
        )
    if parameter.default is not inspect.Parameter.empty:
        raise _DependencyError(
            f"{owner} parameter {parameter.name!r}: the reserved "
            "parameter cannot have a default"
        )

    annotation = annotations.get(parameter.name, parameter.annotation)
    if _annotation_depends(parameter, owner, annotation) is not None:
        raise _DependencyError(
            f"{owner} parameter {parameter.name!r}: the reserved "
            "parameter cannot use Depends(...)"
        )
