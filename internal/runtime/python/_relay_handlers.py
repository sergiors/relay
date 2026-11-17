"""Resolution of a ``"module.function"`` handler reference.

Embedded beside the bootstrap and written to ``/relay/_relay_handlers.py`` in
the runtime image.

Imports happen on the invocation's event loop and ``sys.modules`` caches them,
so a handler module is imported once per warm container and its module-level
state persists across invocations. A resolution failure is reported to the
caller as an invocation failure; it never kills the process.
"""

import importlib


class HandlerLoadError(Exception):
    """A handler reference that does not resolve to a callable."""


def load(handler):
    """Return the callable referenced by handler's ``"module.function"`` form.

    Raise HandlerLoadError with the operator-visible reason when the reference
    is malformed, the module cannot be imported, or the attribute is missing.
    """
    module_name, _, func_name = handler.rpartition(".")
    if not module_name or not func_name:
        raise HandlerLoadError(f"invalid handler {handler!r}")

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise HandlerLoadError(
            f"failed to import module {module_name!r}: {exc}"
        ) from exc

    func = getattr(module, func_name, None)
    if func is None:
        raise HandlerLoadError(f"module {module_name!r} has no function {func_name!r}")

    return func
