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

Temporary fields are copy-on-write snapshots layered onto the same state:
:func:`bind_fields` creates a new immutable state, and :func:`reset_fields`
restores the token. They inherit and expire with the invocation that owns them.
"""

import contextvars
import itertools
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

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
    token: contextvars.Token
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

# Relay serves invocations serially, but a counter is cheap and avoids relying on
# that scheduling detail. The first bound invocation is the process's cold
# start, including an invocation that later fails to import or run its handler.
_INVOCATIONS = itertools.count(1)


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
        temporary=_EMPTY_CONTEXT_FIELDS,
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
    temporary = _EMPTY_FIELDS if scope is None else state.temporary.get(
        scope, _EMPTY_FIELDS
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


def reset_fields(token):
    """Restore the state captured before a :func:`bind_fields` call.

    Safe to call after the invocation that owned the state ended; the token is
    then from a different context and the restore is ignored.
    """
    try:
        _CURRENT.reset(token)
    except (ValueError, RuntimeError):
        pass


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
