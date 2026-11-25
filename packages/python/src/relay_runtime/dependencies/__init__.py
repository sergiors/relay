"""Optional dependency injection for Relay Python handlers.

The feature is opt-in: a handler that does not use :func:`bind` keeps the plain
``handler(event, context)`` contract and never imports this package. A bound
handler is still called with exactly those two arguments; the wrapper resolves
its declared dependencies before calling the original function.

Dependency providers are ordinary callables referenced by ``Depends``. Relay
never infers a dependency from a parameter name or type. The only parameters
injected by name are the two reserved invocation parameters, ``event`` and
``context``, and only on invocation-scoped callables. A container-scoped
provider may not reach either reserved parameter or an invocation-scoped
dependency.

Values live in two scopes:

``invocation``
    Created at most once per handler call when cached, then cleaned up when the
    call finishes (including failure and cancellation).

``container``
    Created at most once per warm container and shared by later invocations.
    Relay's runtime closes these values when its event loop is shutting down.

``use_cache=False`` disables sharing: the provider is called for every request
for it and its value belongs to the requesting invocation. It is valid only for
invocation-scoped dependencies.

Provider callables may return a value, an awaitable, a generator, or an async
generator. Generator providers yield exactly one value; the remainder of the
generator is the cleanup path and runs when the scope ends. Cleanups run in
reverse creation order and are attempted even when the handler fails or is
cancelled.
"""

from __future__ import annotations

from ._handler import bind
from ._model import Depends
from ._state import CONTAINER_STATE

__all__ = ["Depends", "bind"]


async def _shutdown_container_dependencies() -> list[BaseException]:
    return await CONTAINER_STATE.shutdown()
