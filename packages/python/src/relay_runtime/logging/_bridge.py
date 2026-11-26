"""Select the process-wide invocation-logging state implementation.

Inside Relay's runtime image ``/relay`` is on ``sys.path`` and the bootstrap
imports ``_relay_logging_context`` before application code, so importing it here
resolves the SAME module object the runtime already bound. Outside that runtime
(for unit tests and local use) the SDK's fallback module provides the identical
surface with no shared bootstrap state.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any, Protocol, cast

from ._context import (
    Invocation,
)
from ._context import (
    append_keys as _fallback_append_keys,
)
from ._context import (
    bind_fields as _fallback_bind_fields,
)
from ._context import (
    remove_keys as _fallback_remove_keys,
)
from ._context import (
    reset_fields as _fallback_reset_fields,
)
from ._context import (
    snapshot as _fallback_snapshot,
)


class _Bridge(Protocol):
    def snapshot(
        self,
        scope: object | None = None,
    ) -> tuple[
        Invocation | None, Mapping[str, object], Mapping[str, object], bool
    ]: ...

    def append_keys(self, scope: object, fields: Mapping[str, object]) -> None: ...

    def remove_keys(self, scope: object, keys: tuple[str, ...]) -> None: ...

    def bind_fields(self, scope: object, fields: Mapping[str, object]) -> object: ...

    def reset_fields(self, token: object) -> None: ...


class _FallbackBridge:
    def snapshot(
        self,
        scope: object | None = None,
    ) -> tuple[
        Invocation | None, Mapping[str, object], Mapping[str, object], bool
    ]:
        return _fallback_snapshot(scope)

    def append_keys(self, scope: object, fields: Mapping[str, object]) -> None:
        _fallback_append_keys(scope, fields)

    def remove_keys(self, scope: object, keys: tuple[str, ...]) -> None:
        _fallback_remove_keys(scope, keys)

    def bind_fields(self, scope: object, fields: Mapping[str, object]) -> object:
        return _fallback_bind_fields(scope, fields)

    def reset_fields(self, token: object) -> None:
        _fallback_reset_fields(cast(Any, token))


def _load_shared() -> object | None:
    # ImportError (including ModuleNotFoundError) is the documented "no Relay
    # runtime" signal. Any other failure in the shared module is a runtime bug
    # and must not silently split the bootstrap's state from the SDK's.
    try:
        return importlib.import_module("_relay_logging_context")
    except ImportError:
        return None


_SHARED = _load_shared()
BRIDGE: _Bridge = cast(_Bridge, _SHARED) if _SHARED is not None else _FallbackBridge()
