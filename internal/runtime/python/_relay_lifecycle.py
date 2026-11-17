"""Lifecycle lines on the container's native stdout.

Embedded beside the bootstrap and written to ``/relay/_relay_lifecycle.py`` in
the runtime image.

Relay reports process startup and invocation boundaries as plain single-line
records on stdout, which is where the container's native logs are collected:

  INIT_START Runtime: <runtime> BaseImage: <base_image>
  START RequestId: <id>
  END RequestId: <id>

INIT_START is emitted once per process from the static runtime metadata Go
writes to /relay/runtime-metadata.json. START precedes an accepted handler
invocation. END follows an invocation that completed, including one whose
failure was caught and reported, and is never written for cancellation,
interruption, or forced termination.

Every write is best-effort: lifecycle output must never affect invocation
results or startup, and it never captures, buffers, or forwards application
output.
"""

import json
import sys

RUNTIME_METADATA_PATH = "/relay/runtime-metadata.json"

# The process's native stdout, captured before any application code runs so a
# handler that rebinds sys.stdout cannot swallow or redirect lifecycle lines.
# Writes are immediate and flushed; this is not a buffering or forwarding layer
# between the application and its output.
_STDOUT = sys.stdout


def _write(line):
    try:
        _STDOUT.write(line + "\n")
        _STDOUT.flush()
    except Exception:
        # Lifecycle output is diagnostic only: a broken stream must not fail
        # the process or an invocation.
        pass


def initialize():
    """Emit INIT_START once per process from the runtime metadata file.

    The file is static and written at image build time. A missing, unreadable,
    or malformed file simply produces no INIT_START line; Relay's startup and
    invocations are unaffected.
    """
    try:
        with open(RUNTIME_METADATA_PATH, encoding="utf-8") as fh:
            metadata = json.load(fh)
    except (OSError, ValueError):
        return

    if not isinstance(metadata, dict):
        return

    runtime = metadata.get("runtime")
    base_image = metadata.get("base_image")
    if not isinstance(runtime, str) or not isinstance(base_image, str):
        return

    _write(f"INIT_START Runtime: {runtime} BaseImage: {base_image}")


def start_invocation(request_id):
    """Emit START for an accepted invocation, before its handler runs."""
    _write(f"START RequestId: {request_id}")


def end_invocation(request_id):
    """Emit END for a completed invocation, including a caught failure."""
    _write(f"END RequestId: {request_id}")
