"""Relay's Python function bootstrap.

Since Relay 1.x one execution container per function STAYS ALIVE between
invocations and serves a persistent, line-delimited JSON protocol on
stdin/stdout (see internal/runtime/protocol.go in the Relay source):

  Relay -> stdin:  {"id":"<hex>","handler":"mod.func","event":<raw>, "env":{"K":"V"}}
  stdout:          @@RELAY@@{"id":"<id>","ok":true}
                   @@RELAY@@{"id":"<id>","ok":false,"error":"..."}

Everything that is not a sentinel-prefixed response line is user output and is
proxied by Relay to the function-output sink as-is.

Process-model notes (intended semantics, not a bug):
  - Handler errors (Exceptions, SystemExit) fail THAT invocation (ok:false,
    stderr log) and the loop continues: the container is kept healthy.
  - A malformed request line is a fatal protocol failure: the process exits
    nonzero so Relay discards the container.
  - EOF on stdin means Relay is done with the container; exit 0 cleanly.
  - sys.modules caches imported modules, so module-level state (a cache dict,
    a client, a connection) persists across invocations. Per-request env values
    are applied to os.environ and likewise persist across invocations: a value
    set for one invocation remains visible to later ones unless overwritten.
"""

import asyncio
import importlib
import inspect
import json
import os
import sys

# Must match the Relay-side constant @@RELAY@@ (internal/runtime/protocol.go);
# the tests hardcode it because importing a Go constant from Python is not a
# thing.
# The function sources live in /app; put it on sys.path so imports resolve
# regardless of where this bootstrap script is located.
sys.path.insert(0, "/app")

SENTINEL = "@@RELAY@@"

# Response error strings are operator log text only; bound them. The cap keeps
# the whole response frame well under Relay's 4 KiB stdout line cap, so a frame
# is never split mid-line by the demuxer.
MAX_ERROR_BYTES = 3 * 1024

# User print() output must reach Relay live (and BEFORE the protocol response
# of the invocation that printed it): force line buffering on the text layer.
try:
    sys.stdout.reconfigure(line_buffering=True)
except (AttributeError, ValueError):
    pass


def respond(req_id, ok, error=None):
    # Flush pending user-layer output first so the byte order on stdout keeps
    # user print lines ahead of the protocol frame.
    try:
        sys.stdout.flush()
    except Exception:
        pass
    frame = {"id": req_id, "ok": ok}
    if error is not None:
        frame["error"] = str(error)[:MAX_ERROR_BYTES]
    sys.stdout.buffer.write(SENTINEL.encode() + json.dumps(frame).encode() + b"\n")
    sys.stdout.buffer.flush()


def invoke(handler, event):
    """Runs one handler. Raises SystemExit-free exceptions up to the caller for
    protocol failure only; handler/user errors are converted here."""
    module_name, _, func_name = handler.rpartition(".")
    if not module_name or not func_name:
        return f"invalid handler {handler!r}"

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        return f"failed to import module {module_name!r}: {exc}"

    func = getattr(module, func_name, None)
    if func is None:
        return f"module {module_name!r} has no function {func_name!r}"

    try:
        result = func(event)
        if inspect.isawaitable(result):
            asyncio.run(result)  # a fresh loop per invocation
    except SystemExit as exc:
        code = exc.code
        if code is None or code is True:
            return f"handler {handler!r} exited"
        return f"handler {handler!r} exited with code {code}"
    except BaseException as exc:
        # BaseException: SystemExit is hand led above, KeyboardInterrupt and
        # anything the handler raises is a failed INVOCATION, not a fatality.
        return f"handler {handler!r} failed: {exc}"
    return None


def main():
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            # Relay closed stdin (container discard or shutdown): exit cleanly.
            break
        try:
            req = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            # Malformed request frame: a fatal protocol failure. There is no
            # recoverable id (the line does not parse), so respond nothing and
            # terminate so Relay discards the container.
            print(f"invalid request frame: {exc}", file=sys.stderr)
            sys.exit(1)

        req_id = req.get("id", "")
        handler = req.get("handler", "")

        # Per-request environment: set/overwrite. Values persist for later
        # invocations unless overwritten (documented process-global behavior).
        for key, value in (req.get("env") or {}).items():
            os.environ[str(key)] = str(value)
        os.environ["RELAY_HANDLER"] = str(handler)

        try:
            error = invoke(handler, req.get("event"))
        except SystemExit as exc:
            sys.exit(1 if exc.code not in (None, 0, True) else 0)

        if error is not None:
            print(f"handler {handler!r} failed: {error}", file=sys.stderr)

        respond(req_id, error is None, error)


if __name__ == "__main__":
    main()
