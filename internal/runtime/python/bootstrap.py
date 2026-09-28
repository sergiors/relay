"""Relay's Python function bootstrap.

One execution container per function STAYS ALIVE between invocations and serves
a persistent, line-delimited JSON protocol on stdin/stdout (see
internal/runtime/protocol.go in the Relay source):

  Relay -> stdin:  {"id":"<hex>","handler":"mod.func","event":<raw>,
                    "env":{"K":"V"},
                    "trace":{"traceparent":"...","tracestate":"...","baggage":"..."}}
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
    a client, a connection) persists across invocations. Per-request env is
    applied EXACTLY per invocation: a key absent from the new frame is restored
    to its pre-Relay baseline (or removed) and unrelated OS/system/container
    variables are preserved (see apply_env). RELAY_HANDLER is reserved and
    overwritten every invocation.
  - The optional "trace" object carries the W3C trace context Relay injected for
    this invocation. It is extracted and attached ONLY around the one handler
    call and always detached afterwards, so one invocation's context never leaks
    into the next despite the warm interpreter (see invoke()).

Trace propagation:
  The runtime image installs the OpenTelemetry API (see the Python engine's
  managed install). The bootstrap extracts the incoming carrier with the global
  W3C propagator and attaches it for the handler call, so a user-installed OTel
  SDK resolves the SAME global context and creates child spans under Relay's
  invocation span. The handler signature stays func(event): the context is
  ambient, never an argument. The import is guarded so a bare interpreter
  without the API simply skips propagation instead of failing.
"""

import asyncio
import importlib
import inspect
import json
import os
import sys

# OpenTelemetry API: installed into the runtime image next to uv. Import is
# guarded so running the bootstrap outside the image (or in a minimal test
# harness) degrades to no propagation rather than crashing the container.
try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate as otel_propagate
except ImportError:  # pragma: no cover - the runtime image always ships the API
    otel_context = None
    otel_propagate = None

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

# Relay-managed invocation env: exact per-request application without wiping the
# process's OS/system/container variables.
#
# The bootstrap is a long-lived, reused process, so per-request env keys would
# otherwise linger from an earlier invocation. Instead of blindly writing every
# frame value into os.environ (which leaked a removed/rotated key into later
# invocations), the bootstrap tracks only the keys Relay itself applies:
#   _applied  — the keys currently owned by Relay in os.environ
#   _baseline — for each owned key, the value os.environ had BEFORE Relay first
#               overrode it, or _MISSING when it did not exist then
# On each request the env is applied EXACTLY: a previously applied key absent
# from the new frame is restored to its pre-Relay baseline (or deleted), a
# present key is set to the new value (its baseline captured once, on first
# application), and every unrelated variable is left untouched. Cleanup runs at
# the top of each request, before the next env is applied, so it is robust even
# when the previous handler raised or called SystemExit.
_MISSING = object()
_applied = set()
_baseline = {}


def apply_env(env):
    """Apply one request frame's env to os.environ exactly, without wiping
    unrelated process variables. A key Relay applied in an earlier invocation
    that this frame no longer carries is restored to its pre-Relay value (or
    removed), so a rotated or removed env/secret never leaks forward; a key that
    is present is set to its new value. Only keys Relay applies are tracked."""
    incoming = {}
    if isinstance(env, dict):
        for key, value in env.items():
            incoming[str(key)] = str(value)
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


def extract_context(trace):
    """Extracts the invocation's W3C trace context and attaches it, returning
    the detach token (or None when there is nothing to attach). The caller MUST
    detach in a finally: this is the per-invocation boundary that keeps a warm
    container's ambient context from leaking across invocations, on success and
    on failure alike."""
    if otel_context is None or otel_propagate is None or not isinstance(trace, dict) or not trace:
        return None
    # A dict carrier is exactly what the JSON "trace" object decodes to, so the
    # global W3C propagator (TraceContext + Baggage) reads it directly.
    parent = otel_propagate.extract(trace)
    return otel_context.attach(parent)


def detach_context(token):
    """Detaches a context attached by extract_context. Safe on None and never
    raises: a detach failure must not turn a healthy invocation into a
    protocol failure."""
    if token is None or otel_context is None:
        return
    try:
        otel_context.detach(token)
    except Exception:
        pass


def invoke(handler, event, trace=None):
    """Runs one handler. Raises SystemExit-free exceptions up to the caller for
    protocol failure only; handler/user errors are converted here.

    The invocation's trace context (if any) is attached for the single handler
    call and detached in a finally, so the warm interpreter's ambient context is
    restored whatever the handler does (return, raise, or exit)."""
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

    token = extract_context(trace)
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
    finally:
        detach_context(token)
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

        # Per-request environment: applied EXACTLY (see apply_env). Keys this
        # invocation no longer carries are restored to their pre-Relay baseline
        # (or removed), so a rotated/removed value never leaks forward; every
        # unrelated OS/system/container variable is preserved.
        apply_env(req.get("env"))
        os.environ["RELAY_HANDLER"] = str(handler)

        try:
            error = invoke(handler, req.get("event"), req.get("trace"))
        except SystemExit as exc:
            sys.exit(1 if exc.code not in (None, 0, True) else 0)

        if error is not None:
            print(f"handler {handler!r} failed: {error}", file=sys.stderr)

        respond(req_id, error is None, error)


if __name__ == "__main__":
    main()
