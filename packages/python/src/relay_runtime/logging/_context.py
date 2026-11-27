"""Fallback invocation-logging context for standalone SDK use.

Relay's runtime image ships ``/relay/_relay_logging_context.py`` and the
bootstrap imports it before application code. When ``relay_runtime.logging``
runs inside Relay, :mod:`relay_runtime.logging._bridge` uses that shared module
instead of this fallback, so the bootstrap and the SDK always observe one state.

This module mirrors the runtime module's small surface for environments without
the Relay runtime (unit tests and local use). Keep the two implementations in
sync; the runtime copy is authoritative.
"""

from __future__ import annotations

import contextvars
import itertools
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypeIs

__all__ = [
    "Invocation",
    "append_keys",
    "begin",
    "bind_fields",
    "end",
    "remove_keys",
    "reset_fields",
    "snapshot",
]

_EMPTY_FIELDS: Mapping[str, object] = MappingProxyType({})
_EMPTY_APPENDED: Mapping[object, Mapping[str, object]] = MappingProxyType({})
_EMPTY_LAYERS: Mapping[object, _Layer] = MappingProxyType({})

@dataclass(frozen=True, slots=True)
class Invocation:
    """Immutable logging identity for one accepted invocation."""

    app: str
    handler: str
    request_id: str
    attempt: int
    trace_id: str | None
    cold_start: bool


class _Lease:
    """Mutable lifetime marker shared by every copy of one context state."""

    __slots__ = ("active",)

    def __init__(self) -> None:
        self.active = True


@dataclass(frozen=True, slots=True)
class _Layer:
    """One temporary field layer.

    The identity is an increasing id rather than the object: removing a layer
    below the top rebuilds the layers above it, and a handle must still match
    the rebuilt layer it created.
    """

    id: int
    fields: Mapping[str, object]
    parent: _Layer | None


@dataclass(frozen=True, slots=True)
class _State:
    invocation: Invocation | None
    lease: _Lease | None
    appended: Mapping[object, Mapping[str, object]]
    temporary: Mapping[object, _Layer]


@dataclass(frozen=True, slots=True)
class _Handle:
    token: contextvars.Token[_State]
    lease: _Lease


@dataclass(frozen=True, slots=True)
class _Scope:
    """Handle for one temporary layer, bound to its owning invocation."""

    lease: _Lease | None
    scope: object
    layer: _Layer


_EMPTY = _State(
    invocation=None,
    lease=None,
    appended=_EMPTY_APPENDED,
    temporary=_EMPTY_LAYERS,
)
_CURRENT: contextvars.ContextVar[_State] = contextvars.ContextVar(
    "relay_logging_context", default=_EMPTY
)
_INVOCATIONS = itertools.count(1)
_LAYERS = itertools.count(1)


def begin(frame: object) -> _Handle | None:
    """Bind logging metadata from one accepted invocation's context frame."""
    invocation = _invocation_from_frame(frame)
    if invocation is None:
        return None

    lease = _Lease()
    state = _State(
        invocation=invocation,
        lease=lease,
        appended=_EMPTY_APPENDED,
        temporary=_EMPTY_LAYERS,
    )
    return _Handle(token=_CURRENT.set(state), lease=lease)


def end(handle: object | None) -> None:
    """Expire the invocation and restore the previous logging context."""
    if not isinstance(handle, _Handle):
        return

    handle.lease.active = False
    try:
        _CURRENT.reset(handle.token)
    except (ValueError, RuntimeError):
        _CURRENT.set(_EMPTY)


def snapshot(scope: object | None = None) -> tuple[
    Invocation | None, Mapping[str, object], Mapping[str, object], bool
]:
    """Return invocation, appended fields, and one logger's temporary fields."""
    state = _CURRENT.get()
    if state.lease is not None and not state.lease.active:
        return None, _EMPTY_FIELDS, _EMPTY_FIELDS, True
    appended = _EMPTY_FIELDS if scope is None else state.appended.get(
        scope, _EMPTY_FIELDS
    )
    temporary = _EMPTY_FIELDS if scope is None else _merged_fields(
        state.temporary.get(scope)
    )
    return state.invocation, appended, temporary, False


def append_keys(scope: object, fields: Mapping[str, object]) -> None:
    """Append fields to a copy-on-write context-local mapping."""
    state = _CURRENT.get()
    appended = dict(state.appended)
    merged = MappingProxyType({**appended.get(scope, _EMPTY_FIELDS), **fields})
    appended[scope] = merged
    _CURRENT.set(
        _State(
            invocation=state.invocation,
            lease=state.lease,
            appended=MappingProxyType(appended),
            temporary=state.temporary,
        )
    )


def remove_keys(scope: object, keys: tuple[str, ...]) -> None:
    """Remove fields from the current context-local appended mapping."""
    state = _CURRENT.get()
    removed = set(keys)
    appended = dict(state.appended)
    remaining = {
        key: value
        for key, value in appended.get(scope, _EMPTY_FIELDS).items()
        if key not in removed
    }
    if remaining:
        appended[scope] = MappingProxyType(remaining)
    else:
        appended.pop(scope, None)
    _CURRENT.set(
        _State(
            invocation=state.invocation,
            lease=state.lease,
            appended=MappingProxyType(appended),
            temporary=state.temporary,
        )
    )


def bind_fields(scope: object, fields: Mapping[str, object]) -> _Scope:
    """Layer fields into one logger's context, returning a reset handle."""
    state = _CURRENT.get()
    layer = _Layer(
        id=next(_LAYERS),
        fields=MappingProxyType(dict(fields)),
        parent=state.temporary.get(scope),
    )
    temporary = dict(state.temporary)
    temporary[scope] = layer
    _CURRENT.set(
        _State(
            invocation=state.invocation,
            lease=state.lease,
            appended=state.appended,
            temporary=MappingProxyType(temporary),
        )
    )
    return _Scope(lease=state.lease, scope=scope, layer=layer)


def reset_fields(handle: object) -> None:
    """Remove exactly the layer one :func:`bind_fields` call added.

    Safe after the invocation that owned the state ended, and safe for a handle
    whose scope was already reset or replaced: the current state must still
    carry the same lease and that layer, otherwise the call is a no-op. Only
    temporary fields are touched, so appended and removed keys, other scopes,
    and a later invocation keep their current values.
    """
    if not isinstance(handle, _Scope):
        return

    state = _CURRENT.get()
    if state.lease is not handle.lease:
        return
    top = state.temporary.get(handle.scope)
    if top is None:
        return
    owned, remaining = _without_layer(top, handle.layer)
    if not owned:
        return

    temporary = dict(state.temporary)
    if remaining is None:
        temporary.pop(handle.scope, None)
    else:
        temporary[handle.scope] = remaining
    _CURRENT.set(
        _State(
            invocation=state.invocation,
            lease=state.lease,
            appended=state.appended,
            temporary=MappingProxyType(temporary),
        )
    )


def _merged_fields(layer: _Layer | None) -> Mapping[str, object]:
    """Merge one logger's layer chain, oldest first, into one mapping."""
    if layer is None:
        return _EMPTY_FIELDS
    if layer.parent is None:
        return layer.fields

    layers: list[Mapping[str, object]] = []
    while layer is not None:
        layers.append(layer.fields)
        layer = layer.parent
    merged: dict[str, object] = {}
    for fields in reversed(layers):
        merged.update(fields)
    return MappingProxyType(merged)


def _without_layer(top: _Layer, target: _Layer) -> tuple[bool, _Layer | None]:
    """Return ``(owned, chain)`` with ``target`` unlinked from ``top``.

    ``chain`` is the new top layer, or ``None`` when it became empty. ``owned``
    is false when the target is not part of this chain, so the caller must not
    modify the state.
    """
    layers: list[_Layer] = []
    layer: _Layer | None = top
    while layer is not None and layer.id != target.id:
        layers.append(layer)
        layer = layer.parent
    if layer is None:
        return False, None

    remaining = layer.parent
    for entry in reversed(layers):
        remaining = _Layer(id=entry.id, fields=entry.fields, parent=remaining)
    return True, remaining


def _invocation_from_frame(frame: object) -> Invocation | None:
    if not isinstance(frame, Mapping):
        return None

    app = frame.get("app")
    handler = frame.get("handler")
    request_id = frame.get("requestId")
    attempt = frame.get("attempt")
    if not (isinstance(app, str) and isinstance(handler, str)):
        return None
    if not isinstance(request_id, str):
        return None
    if not _is_int(attempt):
        return None

    trace_id = frame.get("traceId")
    if not _is_valid_trace_id(trace_id):
        trace_id = None

    return Invocation(
        app=app,
        handler=handler,
        request_id=request_id,
        attempt=attempt,
        trace_id=trace_id,
        cold_start=next(_INVOCATIONS) == 1,
    )


def _is_int(value: object) -> TypeIs[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_valid_trace_id(value: object) -> TypeIs[str]:
    return (
        isinstance(value, str)
        and len(value) == 32
        and value != "0" * 32
        and all(character in "0123456789abcdef" for character in value)
    )
