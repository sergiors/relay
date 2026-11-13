# relay-runtime

The official Python library for developing Relay applications and using Relay's
public handler interfaces.

The package is optional for Relay's execution. Add it as a normal application
dependency when your handlers import it. Its initial release publishes the
`HandlerContext` structural Protocol; Relay's Python bootstrap constructs the
actual context object for each invocation and passes it to the handler.

## Install

With uv:

```sh
uv add relay-runtime
```

Or with pip:

```sh
pip install relay-runtime
```

The library supports Python 3.14 and newer.

## Handler example

Relay calls a handler with the event as its first argument and the context as
its second:

```python
from relay_runtime.typing import HandlerContext


async def handler(event: dict, context: HandlerContext) -> None:
    print(
        f"request {context.request_id} "
        f"has {context.get_remaining_time_ms()}ms left"
    )
```

`HandlerContext` is a public `typing.Protocol`, not the concrete context
implementation. Relay's Python bootstrap creates and supplies that actual
per-invocation object. The current release provides `HandlerContext`; later
releases may add application-development utilities.

## Contract

`HandlerContext` provides:

| Member                    | Type          | Meaning                                       |
| ------------------------- | ------------- | --------------------------------------------- |
| `request_id`              | `str`         | Stable identity for one logical invocation    |
| `attempt`                 | `int`         | 1-based attempt number                        |
| `max_attempts`            | `int`         | Configured total attempt budget               |
| `deadline`                | `datetime`    | Absolute, timezone-aware cancellation instant |
| `message_id`              | `str \| None` | Source message identity when one exists       |
| `trace_id`                | `str \| None` | W3C trace ID when tracing is enabled          |
| `app`                     | `str`         | Owning app name                               |
| `handler`                 | `str`         | Handler identity                              |
| `get_remaining_time_ms()` | `int`         | Milliseconds left, clamped at zero            |

`max_attempts` is the configured budget, not an upper bound on `attempt`: a
configuration change can leave a claimed attempt above the current budget.

The package version is independent of the Relay release version. Future
releases may add exports, and existing members keep their documented meaning.

## Development

The package is managed with [uv](https://docs.astral.sh/uv/) and uses a `src`
layout with the `uv_build` backend.

```sh
cd packages/python
uv lock
uv sync --locked
uv run mypy
uv build
uv run pytest
```

`uv.lock` is committed so installs and checks resolve the same versions. The
`dev` dependency group contains mypy and pytest only; neither is required by
handlers or by the package at runtime.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
