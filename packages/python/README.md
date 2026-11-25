# relay-runtime

The official Python library for developing Relay applications and using Relay's
public handler interfaces.

The package is optional for Relay's execution. Add it as a normal application
dependency when your handlers import it.

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
per-invocation object.

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

## Dependency injection

Dependency injection is optional. A handler that does not use it keeps the
plain `handler(event, context)` form and does not need to import the library.
Mark a handler with `@bind` and default the parameters you want Relay to
resolve to `Depends(...)`:

```python
from collections.abc import AsyncIterator

from relay_runtime import Depends, bind
from relay_runtime.typing import HandlerContext


class Settings:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url


async def get_settings() -> Settings:
    return Settings("postgres://localhost/app")


class Connection:
    async def close(self) -> None:
        ...


async def get_connection(
    settings: Settings = Depends(get_settings),
) -> AsyncIterator[Connection]:
    connection = Connection()
    try:
        yield connection
    finally:
        await connection.close()


@bind
async def handler(
    event: dict[str, object],
    context: HandlerContext,
    settings: Settings = Depends(get_settings, scope="container"),
    connection: Connection = Depends(get_connection),
) -> None:
    ...
```

Relay still calls the decorated handler with exactly `(event, context)`; the
wrapper resolves the remaining parameters and awaits the original result. A
decorated handler is always async, even when the original function is sync.

### Decorators

`@bind` composes with an ordinary decorator that uses `functools.wraps` and
forwards the call:

```python
import functools
from collections.abc import Awaitable, Callable
from typing import Any

from relay_runtime import Depends, bind
from relay_runtime.typing import HandlerContext


def track_activity(
    func: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        # tracing/metrics here
        return await func(*args, **kwargs)

    return wrapper


async def get_activity_log() -> object:
    return object()


@bind
@track_activity
async def handler(
    event: dict[str, object],
    context: HandlerContext,
    activity_log: object = Depends(get_activity_log),
) -> None:
    ...
```

In this order `@bind` sees the signature preserved by `functools.wraps` and
resolves `activity_log` before calling `track_activity`. A `functools.wraps`
decorator may also be placed outside `@bind`; it then wraps the already-bound
`(event, context)` callable. Both orders work when the ordinary decorator
forwards arguments and preserves the signature. Relay resolves dependencies
only at the `@bind` layer; it does not inject into a third-party decorator. A
decorator that changes the handler signature or does not forward the call is
unsupported.

### Provider parameters

Invocation-scoped providers receive the special values only through parameters
named exactly `event` and `context`:

```python
def request_label(event, context) -> str:
    return f"{context.request_id}:{event['kind']}"
```

No other parameter name or type annotation is inferred. Every other required
provider parameter must use `Depends(...)` or have an ordinary default.
Container-scoped providers cannot use `event` or `context`.

### Rules

- `event` and `context` are reserved parameter names. A handler must declare
  exactly one of each, and they receive the invocation values. They cannot use
  `Depends(...)` or a default.
- Every other handler and provider parameter must have a `Depends(...)` default
  or an ordinary default. Relay never infers a dependency from a type
  annotation or from a parameter name.
- `Depends(provider, *, scope="invocation", use_cache=True)` references a
  callable provider directly. The only supported scopes are `"invocation"` and
  `"container"`. No registration step is required.
- Provider parameters are resolved the same way. Invocation-scoped providers
  may also use the reserved `event` and `context` parameters. Container-scoped
  providers may not use them and may not depend on an invocation-scoped
  dependency.
- `Annotated[T, Depends(provider)]` is accepted in addition to a default. Do
  not combine both forms on one parameter. If a parameter's annotation cannot
  be resolved when it is needed to find `Depends(...)`, relay reports that
  parameter and the resolution error instead of failing later with a generic
  missing-dependency message.
- `*args` and `**kwargs` are rejected. Dependency cycles and invalid signatures
  fail when the handler is decorated.
- `use_cache=False` is valid only with `scope="invocation"`. Combining it with
  `scope="container"` is rejected when `Depends(...)` is declared.

### Scopes and cleanup

- `invocation`: created at most once per handler call when cached, shared only
  within that call, and closed when the call finishes.
- `container`: created at most once per warm container, shared across calls and
  bound handlers, and closed when Relay's event loop is shutting down.

`use_cache=False` calls the provider each time that dependency is requested and
gives the value to the requesting invocation. Use it for cheap or intentionally
fresh values.

Within one dependency graph, cached declarations of the same provider in the
same scope share one cached value. Each uncached declaration is a separate
resolution path: it bypasses that cached slot, creates a fresh value every time
it is requested, and its cleanup belongs to the invocation. Declaring the same
provider in both modes does not make their values shared; the uncached path
neither observes nor replaces the cached value.

Providers may be sync or async functions, generators, or async generators. A
generator provider yields exactly one value; code after the `yield` is its
cleanup path. On success, invocation-scoped generators are resumed so that code
after the `yield` runs and must terminate; a second yield fails the invocation.
On handler or dependency-resolution failure, the active exception is thrown
into each invocation generator (`throw`/`athrow`) before it is closed, so
familiar `except` cleanup runs. If a provider catches or suppresses that
exception, the original handler/dependency failure still propagates; cleanup
failures are reported and never replace it. The same applies when the
invocation is cancelled. Cleanup runs in reverse creation order and every
resource is attempted. Container-scoped dependencies use the same normal
teardown and close when Relay's long-lived event loop exits, without replaying
an older invocation's error.

### Integration example

The following app uses `pymongo` and `httpx` as application dependencies; they
are not dependencies of `relay-runtime`. The Mongo client is container-scoped,
while the HTTP client and course catalog are invocation-scoped:

```python
from collections.abc import AsyncIterator
from typing import Any

import httpx
from pymongo import AsyncMongoClient

from relay_runtime import Depends, bind
from relay_runtime.typing import HandlerContext


class CourseCatalog:
    def __init__(
        self, mongo: AsyncMongoClient, http: httpx.AsyncClient
    ) -> None:
        self._courses = mongo.get_database("school").courses
        self._http = http

    async def notify(
        self, course_id: str, event: dict[str, Any], request_id: str
    ) -> None:
        course = await self._courses.find_one({"_id": course_id})
        if course is None:
            return
        await self._http.post(
            "/course-events",
            json={
                "course": str(course["_id"]),
                "event": event,
                "requestId": request_id,
            },
        )


async def get_mongo() -> AsyncIterator[AsyncMongoClient]:
    client = AsyncMongoClient("mongodb://localhost:27017")
    try:
        yield client
    finally:
        await client.close()


async def get_courses(
    mongo: AsyncMongoClient = Depends(get_mongo, scope="container"),
) -> AsyncIterator[CourseCatalog]:
    async with httpx.AsyncClient(base_url="https://courses.internal") as http:
        yield CourseCatalog(mongo, http)


@bind
async def handler(
    event: dict[str, Any],
    context: HandlerContext,
    courses: CourseCatalog = Depends(get_courses),
) -> None:
    await courses.notify(event["course_id"], event, context.request_id)
```

`get_mongo` opens once per warm container. `get_courses` and its HTTP client are
created for each invocation; their cleanup runs after the handler, and the Mongo
client closes when the container shuts down. The handler completes its response
through the injected HTTP client; Relay uses handler completion for success or
failure, not the Python return value.

### Errors and plain handlers

Dependency declaration, resolution, and cleanup failures use Relay's normal
handler failure path: the invocation fails through the usual error reporting and
Relay's configured retry policy applies. There is no separate dependency retry
mechanism. A cleanup failure during an already-failing invocation is reported
without replacing the original handler or dependency error.

`Depends(...)` on a handler is resolved only when that handler is decorated with
`@bind`. Without `@bind`, Relay executes the ordinary handler exactly as before
with `(event, context)` and does not resolve dependency markers; a parameter
whose default is `Depends(...)` simply retains that default value.

## Limitations

- Container-scoped values live in one warm container's Python process and event
  loop. They are not shared across containers, replicas, or nodes and are lost
  when the container is discarded. A value created by an async provider belongs
  to the event loop that created it; using it from a different loop fails rather
  than sharing a loop-bound resource.
- Container cache initialization relies on Relay's serialized invocation model.
  Concurrent bound-handler calls outside Relay are not coordinated.
- A provider is identified by the callable object used in `Depends(...)`. Use
  module-level callables when the same container-scoped value must be shared by
  more than one handler.
- Cleanup is best-effort. A hard timeout, container kill, or process crash does
  not run generator finalizers.
- Dependency injection does not change Relay's at-least-once delivery contract;
  handlers should still be idempotent.

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
