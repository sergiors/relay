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

## Structured logging

`relay_runtime.logging` provides an optional structured logger. Import `Logger`
and the public `JsonFormatter` from this module; neither is re-exported from
`relay_runtime`:

```python
from relay_runtime.logging import Logger

log = Logger(service="users")


async def handler(event: dict[str, object], context) -> None:
    log.info("User created", extra={"user_id": event["user_id"]})
```

The default handler writes compact JSON lines to stderr. The logger does not
propagate to the root logger, so it does not duplicate output through the
bootstrap's existing `logging.basicConfig`. Relay's lifecycle lines remain on
stdout. `Logger()` works outside Relay with the standalone service identity.
New SDK loggers default to `INFO`, so informational calls are emitted without
changing the root logger. An existing named logger's non-`NOTSET` level is
preserved unless `level=` is explicitly supplied.

### Identity and configuration

The public constructor is:

```python
Logger(
    service: str | None = None,
    level: str | int | None = None,
    *,
    logger_handler: logging.Handler | None = None,
    logger_formatter: logging.Formatter | None = None,
)
```

When `service` is omitted, identity is resolved once, in this order: a non-empty
`OTEL_SERVICE_NAME`, the active Relay app identity (when constructed during an
invocation), then `"standalone"`. An explicit non-empty `service` takes
precedence. The stdlib logger name is `relay_runtime.<service>`; separate
services therefore have separate names, handlers, and levels.

Repeated construction for the same service reuses its stdlib logger and never
adds a duplicate SDK handler. A supplied `logger_handler` replaces the SDK
handler for that service. Its existing formatter is retained unless
`logger_formatter` is also supplied, in which case that formatter is set on the
supplied handler. The SDK does not close application handlers. A custom handler
can replace an SDK handler installed by an earlier construction. Once a custom
handler is selected, later constructions without `logger_handler` keep using
it. A later explicit `logger_handler` replaces the previously selected handler
for that service; the old handler is detached, not closed. An explicit
`logger_formatter` updates the selected handler; omitting it preserves the
handler's formatter. Supplying `level` sets the shared stdlib logger's level;
omitting it preserves a preconfigured level or defaults a new `NOTSET` logger
to `INFO`.

`logger.name` and `logger.service` expose the resolved values. `addHandler` and
`removeHandler` use standard logging behavior; removing a handler only detaches
it and never closes it. The SDK isolates ordinary `Exception`s raised by a
handler, continues dispatching to healthy handlers, and does not emit recursive
diagnostic logs. `KeyboardInterrupt`, `SystemExit`, and cancellation are not
swallowed. This per-logger dispatch behavior applies to handlers attached via
`addHandler` as well as the configured destination.

`addHandler` adds a destination alongside the default JSON stderr handler:

```python
import logging
import sys

from relay_runtime.logging import Logger

log = Logger(service="users")
console = logging.StreamHandler(sys.stdout)
console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
log.addHandler(console)
log.info("User created", extra={"user_id": "u-42"})
```

The record goes to both stderr and stdout. Use `logger_handler=` at
construction when the custom handler should replace the SDK destination.

### Calls and context fields

`debug`, `info`, `warning`, `error`, `exception`, `critical`, and `log` accept
standard logging arguments: `%` formatting arguments, `extra` mapping,
`exc_info`, `stack_info`, and `stacklevel`. Keyword arguments are not treated as
application fields; put those fields in `extra`:

```python
log.info("request %s completed", request_id, extra={"duration_ms": 28})

try:
    process()
except ValueError:
    log.exception("processing failed", extra={"phase": "validate"})
```

Fields added with `extra` are shallow-copied into a new mapping at record
creation. They appear on the public `LogRecord.attributes` mapping and are
available individually on the record to custom formatters. Nested mutable values
remain references; changing a nested list or dictionary before a queued listener
formats the record may change its later representation. Standard `LogRecord`
field collisions raise `KeyError` as in Python logging. Relay-owned fields such as `service`,
`request_id`, `trace_id`, `location`, and `attributes` are reserved, as are the
logging option names `extra`, `exc_info`, `stack_info`, and `stacklevel`;
attempting to replace them raises `ValueError`. Framework metadata always wins.
`message_id` and `max_attempts` are not automatically populated and may be used
as application attributes.

Appended fields can be set and removed with `append_keys(**fields)` and
`remove_keys(names)`. `remove_keys` accepts an iterable such as a list; separate
string arguments are also supported. These fields are scoped to the named
stdlib logger identity: Logger instances for the same service share them, while
different services remain isolated. `append_context_keys(**fields)` adds a
temporary layer scoped to the same logger identity and current context. It
returns a token-restoring scope that can be used with `with` when the fields
should apply only inside a block:

```python
log.append_keys(component="payments")

with log.append_context_keys(operation="authorize"):
    log.info("starting")
```

Each operation is copy-on-write and context-local, so concurrent tasks do not
mutate shared mappings. If the same name is present at multiple levels,
precedence is `extra` then temporary `append_context_keys` values, then appended
values. For example, `log.remove_keys(["user_id", "organization_id"])` removes
both appended values. Leaving the `with` block restores the previous temporary
layer. Invocation-scoped fields reset when an invocation ends; appending keys
outside Relay remains local to the current context until changed. Context
inherited by a child task expires when its invocation ends.

### JSON record

The default `JsonFormatter` emits one JSON object per line. It always includes
`timestamp`, `level`, `service`, `message`, `logger`, and `location`. Timestamp
uses UTC RFC 3339 with millisecond precision and is derived from
`LogRecord.created`. `location` is `module.function:line`, resolved using normal
`stacklevel` rules. Invocation metadata (`app`, `handler`, `request_id`,
`attempt`, and `cold_start`) appears when present.

Valid active OpenTelemetry spans add `trace_id` and `span_id`. Application
fields are nested in `attributes`; the empty object is omitted. An
exception is emitted as
`exception: {"type": ..., "message": ..., "stacktrace": ...}`.
`stack_info=True` adds `stack_info`.

The following record is formatted across multiple lines for readability:

```json
{
  "timestamp": "2026-04-02T12:30:15.123Z",
  "level": "INFO",
  "service": "checkout",
  "message": "order accepted",
  "logger": "relay_runtime.checkout",
  "location": "handler.process:18",
  "attributes": {
    "order_id": "o-42"
  }
}
```

The SDK captures framework metadata and a shallow copy of application field
mappings when the record is created. Nested mutable values are not deep-copied,
so mutations before a `QueueHandler` listener formats the record can affect
their representation. Supported JSON values include primitives, finite
numbers, lists, tuples, string-keyed mappings, dates/datetimes, UUIDs, and
Decimals. Unsupported, circular, overly deep, or non-finite values become
explicit marker objects. If serialization itself fails, a safe one-line
fallback is emitted and the application call continues.

On CPython, the standard `QueueHandler.prepare` formats and copies a record,
then clears `exc_info` and `exc_text`; traceback text is rendered into the
message before enqueueing. A listener using `JsonFormatter` may therefore lack
the structured `exception` object for queued records. The captured context and
`attributes` mapping still travel with the record. Preserve exception details
with a custom `QueueHandler.prepare` implementation if the listener needs the
structured exception schema.

Logging handlers run synchronously by default. A slow or blocking handler can
block its caller or an asyncio event loop and cause an invocation to exceed its
configured timeout; logging does not extend the timeout. For slow destinations,
the standard library `QueueHandler` and `QueueListener` can move sink work to a
listener thread:

```python
import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

from relay_runtime.logging import Logger

queue: Queue[logging.LogRecord] = Queue(maxsize=1000)
listener = QueueListener(queue, logging.StreamHandler())
listener.start()
log = Logger(service="users", logger_handler=QueueHandler(queue))

try:
    log.info("User created", extra={"user_id": "u-42"})
finally:
    listener.stop()
```

Choose a bounded queue size and an explicit overflow policy. Standard
`QueueHandler` enqueue is non-blocking; when a bounded queue is full, its error
handling may report or drop the record. A custom blocking/retrying policy can
stall the invocation. The application owns the listener lifecycle: stop it
during graceful shutdown after producers finish, and ensure the queue can accept
the stop signal; `stop()` waits for the listener and may itself block on a stuck
sink. A timeout, container kill, or process crash may terminate the listener
before queued records are written. Logging is not guaranteed delivery.

Messages, exception tracebacks, and application attributes may contain secrets.
Relay cannot reliably sanitize or redact them; avoid logging credentials, tokens,
or other sensitive values.

`JsonFormatter` can be used directly or supplied as `logger_formatter`. A custom
formatter receives an ordinary `LogRecord` with the standard members, framework
metadata, and `record.attributes`:

```python
import logging

from relay_runtime.logging import JsonFormatter, Logger

log = Logger(
    "checkout",
    level=logging.INFO,
    logger_formatter=JsonFormatter(),
)

log.info("captured", extra={"order_id": "o-42"})
```

### Relay invocation and OpenTelemetry context

Relay binds invocation logging context before importing the handler module and
expires it after handler execution and dependency cleanup, including failure
and cancellation paths. `cold_start` is true on the first accepted invocation,
even if importing or running that handler fails. A child task may finish after
its parent invocation, but its inherited invocation and temporary context are
expired and cannot leak into later invocations. Records created during an
invocation keep their captured metadata when formatted later. An expired child
context also suppresses span lookup and trace correlation even when it still
inherits the invocation's OpenTelemetry context.

When the optional OpenTelemetry API is available, a valid active span is read at
record creation in non-expired contexts both inside and outside Relay. The logger
only reads the API: it does not install a provider, create spans, or export logs.
The invocation trace ID is used only when there is no active valid span, so
unrelated span and invocation IDs are never paired. Span discovery and
application-field serialization are skipped when the log level is disabled.
Relay collects application stderr as the container's native log; the SDK does
not implement a Tracer, dependency integration, or log transport.

Applications can create spans with the native OpenTelemetry API; log records
inside a valid active span automatically include its trace and span IDs:

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("create-user"):
    log.info("User created", extra={"user_id": "u-42"})
```

OpenTelemetry remains optional and is not added as a `relay-runtime`
dependency. The SDK reads active span context but does not expose or initialize
a `Tracer` API.

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
import time
from collections.abc import Awaitable, Callable
from typing import Any

from relay_runtime import Depends, bind
from relay_runtime.typing import HandlerContext


def measure_duration(
    func: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return await func(*args, **kwargs)
        finally:
            print(f"{func.__name__}: {time.perf_counter() - start:.3f}s")

    return wrapper


def get_currency() -> str:
    return "USD"


@bind
@measure_duration
async def handler(
    event: dict[str, object],
    context: HandlerContext,
    currency: str = Depends(get_currency),
) -> None:
    print(f"Order {event['order_id']} uses {currency}")
```

In this order `@bind` sees the signature preserved by `functools.wraps` and
resolves `currency` before calling `measure_duration`. A `functools.wraps`
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

### Overriding dependencies

`relay_runtime.dependencies.override_dependencies` temporarily replaces
providers for the current context. It is available only from the
`relay_runtime.dependencies` module; the top-level `relay_runtime` package does
not export it.

A test can replace the production email provider with a fake and call the bound
handler inside the context:

```python
import asyncio
from typing import Protocol

from relay_runtime import Depends, bind
from relay_runtime.dependencies import override_dependencies
from relay_runtime.typing import HandlerContext


class EmailSender(Protocol):
    def send(self, address: str, subject: str) -> None: ...


def get_email_sender() -> EmailSender:
    ...  # return the production SES-backed sender


@bind
async def handler(
    event: dict[str, object],
    context: HandlerContext,
    email: EmailSender = Depends(get_email_sender),
) -> None:
    email.send(str(event["address"]), "Welcome")


class FakeEmailSender:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def send(self, address: str, subject: str) -> None:
        self.sent.append((address, subject))


def test_handler_sends_welcome_email(context: HandlerContext) -> None:
    fake = FakeEmailSender()
    with override_dependencies({get_email_sender: lambda: fake}):
        asyncio.run(handler({"address": "user@example.com"}, context))
    assert fake.sent == [("user@example.com", "Welcome")]
```

`context` is the invocation context Relay supplies; a unit test can pass a stub
that implements `HandlerContext`.

Keys are matched by provider identity, not equality. The mapping is snapshotted
and validated when the context is entered; later changes to the mapping object
do not affect an active context. Mapping a provider to itself is a no-op.
For parametrized provider factories, use the same cached provider object in the
handler declaration and override mapping; for example, cache
`get_table("users")` with `functools.cache` if it is constructed at both sites.
Nested contexts compose: an inner context inherits the outer overrides and its
entries take precedence. Leaving the inner context restores the outer mapping.

Overrides apply only where a `Depends(...)` declaration references the original
provider. The replacement inherits that declaration's `scope` and `use_cache`.
The replacement's own dependencies are resolved with the same overrides, but
the replacement root is not replaced again. An override chain is therefore not
followed recursively, and a replacement that depends on the provider it replaces
is reported as a dependency cycle. Container and invocation caches are keyed by
the original declaration, so two different originals mapped to one replacement
remain separate cache entries and own separate resource instances. The effective
graph is compiled lazily the first time a bound handler runs under the context;
replacement signatures, cycles, and scope rules are validated then.

Each active context owns an isolated container cache. Container-scoped values
created inside the context are not shared with the global warm container or with
another context, and their managed resources are closed when that context exits.
Nested contexts each own their own cache; an inner context does not see the
outer context's container values, and leaving the inner context closes only the
resources it created.

Use `with` for synchronous code and `async with` when container-scoped async
generator providers may be involved:

- `with` closes synchronous generator container resources without starting an
  event loop. A container-scoped async generator is rejected before it is
  started, including when another callable returns it indirectly; the error
  asks for `async with`.
- `async with` closes both synchronous and asynchronous container resources.
- Invocation-scoped resources are always cleaned up inside the handler call, so
  either form works for them.

Container-scoped async generator replacements require `async with` so their
cleanup path can run:

```python
from collections.abc import AsyncIterator

from relay_runtime import Depends, bind
from relay_runtime.dependencies import override_dependencies
from relay_runtime.typing import HandlerContext


class Bucket:
    async def put(self, key: str, body: bytes) -> None: ...
    async def close(self) -> None: ...


async def get_bucket() -> AsyncIterator[Bucket]:
    bucket = Bucket()
    try:
        yield bucket
    finally:
        await bucket.close()


@bind
async def upload(
    event: dict[str, object],
    context: HandlerContext,
    bucket: Bucket = Depends(get_bucket, scope="container"),
) -> None:
    await bucket.put(str(event["key"]), b"...")


async def get_fake_bucket() -> AsyncIterator[Bucket]:
    bucket = Bucket()
    try:
        yield bucket
    finally:
        await bucket.close()


async def test_upload_uses_fake_bucket(context: HandlerContext) -> None:
    async with override_dependencies({get_bucket: get_fake_bucket}):
        await upload({"key": "report.csv"}, context)
```

`get_fake_bucket` is a container-scoped async generator, so its resource is
started and closed by `async with`. A synchronous `with` rejects that
replacement before the generator starts and reports that `async with` is
required.

If cleanup fails on exit, the error is raised when no other exception is active;
the context variable is restored first. If an exception is already propagating,
cleanup failures are logged and the original exception is preserved.
Invocation-scoped resources are cleaned up inside the handler on success,
failure, and cancellation; container-scoped resources are cleaned up when their
owning override context exits.

The active context is stored in a `contextvars.ContextVar`. It is visible in the
task that entered the context and in child tasks created while it is active.
Contexts are independent across tasks. A child task that outlives the context
and tries to resolve a dependency after the owner exits fails instead of using
the closed container. That inherited task context also cannot enter a new
override context after its parent has exited; create longer-lived tasks outside
the override context. Enter and exit the context in the same task; do not move a
context manager object to another task or thread.

Overrides affect only dependency resolution performed by `@bind` and
`Depends(...)`. They do not monkeypatch providers, change direct calls to a
provider, or alter container values that were resolved outside dependency
injection. Relay's at-least-once delivery contract is unchanged.

### Integration example

The following app uses `pymongo` and `httpx` as application dependencies; they
are not dependencies of `relay-runtime`. The Mongo client is container-scoped,
while the HTTP client is invocation-scoped.

The handler processes user registration events and sends a welcome email
through an external notification service:

```python
import os
from collections.abc import AsyncIterator
from typing import Annotated, Any

import httpx
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from relay_runtime import Depends, bind
from relay_runtime.typing import HandlerContext


async def get_mongo() -> AsyncIterator[AsyncMongoClient]:
    async with AsyncMongoClient(os.environ["MONGO_DSN"]) as client:
        yield client


def get_users(
    mongo: Annotated[AsyncMongoClient, Depends(get_mongo, scope="container")],
) -> AsyncCollection:
    return mongo.get_database("app").get_collection("users")


async def get_http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=os.environ["NOTIFICATIONS_API_URL"],
        timeout=10.0,
    ) as client:
        yield client


@bind
async def handler(
    event: dict[str, Any],
    context: HandlerContext,
    users: Annotated[AsyncCollection, Depends(get_users)],
    http: Annotated[httpx.AsyncClient, Depends(get_http_client)],
) -> None:
    user = await users.find_one({"_id": event["user_id"]})
    if user is None:
        return

    response = await http.post(
        "/notifications/email",
        json={
            "recipient": user["email"],
            "template": "welcome",
            "data": {"name": user["name"]},
        },
        headers={"Idempotency-Key": context.request_id},
    )
    response.raise_for_status()
```

`get_mongo` creates a client once per warm container. `get_users` resolves
the collection using the shared Mongo client. The HTTP client is created
for each invocation and closed after the handler finishes. The Mongo client
closes when the container shuts down.

The notification API is assumed to honor the idempotency key, allowing
retries of the same logical invocation without sending duplicate emails.

Relay uses handler completion for success or failure, not the Python return
value.

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
