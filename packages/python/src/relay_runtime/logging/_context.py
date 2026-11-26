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
_EMPTY_CONTEXT_FIELDS: Mapping[object, Mapping[str, object]] = MappingProxyType({})


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
class _State:
    invocation: Invocation | None
    lease: _Lease | None
    appended: Mapping[object, Mapping[str, object]]
    temporary: Mapping[object, Mapping[str, object]]


@dataclass(frozen=True, slots=True)
class _Handle:
    token: contextvars.Token[_State]
    lease: _Lease


_EMPTY = _State(
    invocation=None,
    lease=None,
    appended=_EMPTY_APPENDED,
    temporary=_EMPTY_CONTEXT_FIELDS,
)
_CURRENT: contextvars.ContextVar[_State] = contextvars.ContextVar(
    "relay_logging_context", default=_EMPTY
)
_INVOCATIONS = itertools.count(1)


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
        temporary=_EMPTY_CONTEXT_FIELDS,
    )
    return _Handle(token=_CURRENT.set(state), lease=lease)


def end(handle: object | None) -> None:
    """Expire the invocation and restore the previous logging context."""
    if not isinstance(handle, _Handle):
        return

    handle.lease.active = False
    try:
        _CURRENT.reset(handle.token)
    except ValueError, RuntimeError:
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
    temporary = _EMPTY_FIELDS if scope is None else state.temporary.get(
        scope, _EMPTY_FIELDS
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


def bind_fields(
    scope: object, fields: Mapping[str, object]
) -> contextvars.Token[_State]:
    """Layer fields into one logger's context, returning a reset token."""
    state = _CURRENT.get()
    temporary = dict(state.temporary)
    merged = MappingProxyType({**temporary.get(scope, _EMPTY_FIELDS), **fields})
    temporary[scope] = merged
    return _CURRENT.set(
        _State(
            invocation=state.invocation,
            lease=state.lease,
            appended=state.appended,
            temporary=MappingProxyType(temporary),
        )
    )


def reset_fields(token: contextvars.Token[_State]) -> None:
    """Restore the state captured before a :func:`bind_fields` call."""
    try:
        _CURRENT.reset(token)
    except ValueError, RuntimeError:
        pass


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
