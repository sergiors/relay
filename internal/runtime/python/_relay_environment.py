"""Exact per-invocation application of Relay-managed environment variables.

Embedded beside the bootstrap and written to ``/relay/_relay_environment.py``
in the runtime image.

Relay applies a request's env to the warm process before each handler call.
Blindly writing every frame value into ``os.environ`` would leak a removed or
rotated key into later invocations and wiping the environment would drop the
container's own OS/system variables, so only the keys Relay itself applies are
tracked:

  ``_applied``  — the keys currently owned by Relay in ``os.environ``
  ``_baseline`` — for each owned key, the value ``os.environ`` had BEFORE Relay
                  first overrode it, or ``_MISSING`` when it did not exist then

On each request the env is applied EXACTLY: a previously applied key absent from
the new frame is restored to its pre-Relay baseline (or deleted), a present key
is set to the new value (its baseline captured once, on first application), and
every unrelated variable is left untouched. Cleanup runs at the top of each
request, before the next env is applied, so it is robust even when the previous
handler raised or called ``SystemExit``. The channel secret is never applied or
restored, so a frame can never reintroduce it.
"""

import os

from _relay_config import EXECUTION_SECRET_ENV

RELAY_HANDLER_ENV = "RELAY_HANDLER"

_MISSING = object()
_applied = set()
_baseline = {}


def apply_invocation_env(env, handler):
    """Apply one request frame's env exactly, then reserve RELAY_HANDLER for the
    handler being invoked. A null or absent env is empty.

    A key Relay applied in an earlier invocation that this frame no longer
    carries is restored to its pre-Relay value (or removed), so a rotated or
    removed env/secret never leaks forward; a key that is present is set to its
    new value. Only keys Relay applies are tracked.
    """
    incoming = {}
    if isinstance(env, dict):
        for key, value in env.items():
            name = str(key)
            if name == EXECUTION_SECRET_ENV:
                continue
            incoming[name] = str(value)

    os.environ.pop(EXECUTION_SECRET_ENV, None)

    for key in list(_applied):
        if key not in incoming:
            prior = _baseline.pop(key, _MISSING)
            if prior is _MISSING:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prior
            _applied.discard(key)

    for key, value in incoming.items():
        if key not in _applied:
            _baseline[key] = os.environ.get(key, _MISSING)
        os.environ[key] = value
        _applied.add(key)

    os.environ[RELAY_HANDLER_ENV] = handler
