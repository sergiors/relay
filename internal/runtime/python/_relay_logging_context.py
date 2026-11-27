"""Per-invocation logging metadata shared by the bootstrap and the Python SDK.

Embedded beside the bootstrap and written to ``/relay/_relay_logging_context.py``
in the runtime image.

The bootstrap imports this module before ``/app`` joins ``sys.path``. The
optional ``relay_runtime.logging`` SDK imports the same top-level module name,
so both resolve ONE module object and share one ``ContextVar``. An application
can therefore use the SDK without any registration step, and an application that
never imports the SDK still gets import-time logging metadata because the
invocation runner, not the handler, owns this state.

The bound metadata is immutable. :func:`begin` builds a fresh snapshot from a
validated request frame; :func:`end` first expires the snapshot's lease and then
restores the previous value. A child task that copied the context keeps the same
lease object, so it can no longer emit stale invocation metadata after the
invocation completed, even though its own ``ContextVar`` copy still exists.

Temporary fields are copy-on-write layers on the same state: :func:`bind_fields`
adds one layer and returns its handle, and :func:`reset_fields` removes exactly
that layer. It never restores a whole earlier state, so fields appended or
removed inside the scope, other loggers' layers, and a later invocation are
preserved; a handle whose state no longer owns its layer is ignored. The layers
inherit and expire with the invocation that owns them.
"""

import contextvars
import itertools
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

_EMPTY_FIELDS: Mapping[str, object] = MappingProxyType({})
_EMPTY_APPENDED: Mapping[object, Mapping[str, object]] = MappingProxyType({})
_EMPTY_LAYERS: Mapping[object, "_Layer"] = MappingProxyType({})


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
    parent: "_Layer | None"


@dataclass(frozen=True, slots=True)
class _State:
    invocation: Invocation | None
    lease: _Lease | None
    appended: Mapping[object, Mapping[str, object]]
    temporary: Mapping[object, "_Layer"]


@dataclass(frozen=True, slots=True)
class _Handle:
    token: contextvars.Token
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

# Relay serves invocations serially, but a counter is cheap and avoids relying on
# that scheduling detail. The first bound invocation is the process's cold
# start, including an invocation that later fails to import or run its handler.
_INVOCATIONS = itertools.count(1)
_LAYERS = itertools.count(1)


def begin(frame):
    """Bind logging metadata from one accepted invocation's context frame.

    Returns an opaque handle for :func:`end`, or ``None`` when the frame does
    not carry the required identity fields. The runtime only calls this with a
    frame its server already validated; the lenient check keeps a direct call
    from replacing a valid context with partial metadata.
    """
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
    token = _CURRENT.set(state)
    return _Handle(token=token, lease=lease)


def end(handle):
    """Expire the invocation and restore the previous logging context.

    Safe on ``None`` and never raises: a restore failure must not turn a
    healthy invocation into a protocol failure. The lease is expired BEFORE the
    reset so a child task holding an inherited copy observes the expiry even if
    the reset itself is not observable in that task.
    """
    if handle is None:
        return

    try:
        lease = handle.lease
        token = handle.token
    except AttributeError:
        return

    lease.active = False
    try:
        _CURRENT.reset(token)
    except (ValueError, RuntimeError):
        _CURRENT.set(_EMPTY)


def snapshot(scope=None):
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


def append_keys(scope, fields):
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


def remove_keys(scope, keys):
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


def bind_fields(scope, fields):
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


def reset_fields(handle):
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


def _merged_fields(layer):
    """Merge one logger's layer chain, oldest first, into one mapping."""
    if layer is None:
        return _EMPTY_FIELDS
    if layer.parent is None:
        return layer.fields

    layers = []
    while layer is not None:
        layers.append(layer.fields)
        layer = layer.parent
    merged = {}
    for fields in reversed(layers):
        merged.update(fields)
    return MappingProxyType(merged)


def _without_layer(top, target):
    """Return ``(owned, chain)`` with ``target`` unlinked from ``top``.

    ``chain`` is the new top layer, or ``None`` when it became empty. ``owned``
    is false when the target is not part of this chain, so the caller must not
    modify the state.
    """
    layers = []
    layer = top
    while layer is not None and layer.id != target.id:
        layers.append(layer)
        layer = layer.parent
    if layer is None:
        return False, None

    remaining = layer.parent
    for entry in reversed(layers):
        remaining = _Layer(id=entry.id, fields=entry.fields, parent=remaining)
    return True, remaining


def _invocation_from_frame(frame):
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


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_valid_trace_id(value):
    return (
        isinstance(value, str)
        and len(value) == 32
        and value != "0" * 32
        and all(character in "0123456789abcdef" for character in value)
    )
