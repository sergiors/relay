import asyncio
import importlib
import inspect
import json
import os
import sys

# The function sources live in /app; put it on sys.path so imports resolve
# regardless of where this bootstrap script is located.
sys.path.insert(0, "/app")


def main():
    handler = os.environ.get("RELAY_HANDLER", "")
    if not handler:
        print("RELAY_HANDLER is not set", file=sys.stderr)
        sys.exit(1)

    module_name, _, func_name = handler.rpartition(".")
    if not module_name or not func_name:
        print("invalid RELAY_HANDLER %r" % handler, file=sys.stderr)
        sys.exit(1)

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        print("failed to import module %r: %s" % (module_name, exc), file=sys.stderr)
        sys.exit(1)

    func = getattr(module, func_name, None)
    if func is None:
        print("module %r has no function %r" % (module_name, func_name), file=sys.stderr)
        sys.exit(1)

    try:
        event = json.load(sys.stdin)
    except Exception as exc:
        print("failed to read event from stdin: %s" % exc, file=sys.stderr)
        sys.exit(1)

    try:
        result = func(event)
        if inspect.isawaitable(result):
            asyncio.run(result)
    except Exception as exc:
        print("handler %r failed: %s" % (handler, exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
