"""Relay's Python function bootstrap.

This is the container entrypoint (``python -u /relay/bootstrap.py``). It selects
the event loop once for the whole process, reports the container's startup
metadata, and then serves Relay's authenticated invocation channel until Relay
closes it. The implementation is split into internal ``_relay_*`` modules that
are embedded and deployed beside this file under ``/relay`` (never a HOT_RELOAD
source mount), so application files cannot shadow them:

  _relay_config       Relay-owned channel port and secret
  _relay_protocol     framing, JSON, authentication handshake, result envelopes
  _relay_context      the per-invocation HandlerContext Relay passes to handlers
  _relay_environment  exact per-invocation environment application
  _relay_tracing      per-invocation OpenTelemetry context attach/detach
  _relay_handlers     "module.function" handler resolution
  _relay_invocation   one handler call on the process-lifetime loop
  _relay_lifecycle    INIT_START/START/END lines on the native stdout
  _relay_memory       once-per-process cgroup memory discovery and sampling
  _relay_report       per-invocation duration and memory REPORT line
  _relay_server       listener, connection admission, invoke/result exchange

Application stdout/stderr stay native container logs: no protocol traffic uses
them, and Relay neither reads nor forwards them. Lifecycle lines are written
directly to the process's native stdout as plain single lines.

Lifetime caveat: reuse is a warm-process optimization, never a durability or
lifetime guarantee. A container is discarded on a timeout, protocol error,
channel loss, or image change, and the process exits when its authenticated
connection ends; module state and any loop-bound resource created by a handler
are released with it.
"""

import asyncio
import logging
import sys

import _relay_lifecycle as lifecycle
import _relay_memory as memory
from _relay_config import load_execution_config
from _relay_server import runtime_loop

# Configure a default logging format before importing application modules.
# Keep Python's default WARNING level. Applications can change the level
# using logging.getLogger().setLevel() or replace the configuration explicitly.
logging.basicConfig(format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

# The function sources live in /app; put it on sys.path so imports resolve
# regardless of where this bootstrap script is located. It is inserted after the
# runtime modules are imported so application files can never shadow them.
sys.path.insert(0, "/app")

# User print() output is the container's native log, not protocol data. Keep the
# text layer line-buffered so it appears live while the container stays warm.
try:
    sys.stdout.reconfigure(line_buffering=True)
except (AttributeError, ValueError):
    pass

# Build-time event-loop selector. The Python engine replaces this exact line
# (with `import zuvloop` and `ZUVLOOP_ENABLED = True`) when the worker-global
# PYTHON_ZUVLOOP setting applies to this image: the app declares at least one
# event or schedule handler and the image installed the pinned zuvloop package.
# A disabled image keeps this line verbatim, never imports zuvloop, and runs the
# standard asyncio loop. The import/loop initialization deliberately has no
# fallback: if an enabled image cannot import the package or create the loop,
# the container must fail startup rather than silently run a different loop.
ZUVLOOP_ENABLED = False


def main():
    # One loop for the whole process lifetime: module-level asyncio resources
    # stay bound to it across invocations instead of being orphaned by a
    # per-invocation loop. Startup metadata is reported once, before the
    # channel exists; a missing or malformed configuration fails startup
    # before any handler can be imported.
    lifecycle.initialize()
    memory.initialize()
    config = load_execution_config()

    # The enabled branch requires the module installed by the engine's managed
    # build step; an import or loop-initialization failure propagates and fails
    # container startup, never falling back to the standard loop.
    if ZUVLOOP_ENABLED:
        asyncio.run(runtime_loop(config), loop_factory=zuvloop.new_event_loop)
    else:
        asyncio.run(runtime_loop(config))


if __name__ == "__main__":
    main()
