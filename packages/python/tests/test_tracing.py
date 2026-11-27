"""Contract tests for relay_runtime.tracing's Tracer facade.

The suite runs against the real OpenTelemetry API and the in-memory SDK
exporter installed through the dev group (``uv run --extra tracing pytest``).
Provider registration is process-global and one-shot, so the session-scoped
fixture performs it exactly once and no test resets OpenTelemetry's private
state. Two subprocess tests pin the stories that need a clean interpreter:
importing without OpenTelemetry installed, and a Tracer created before the
process registers its provider. The module-level TRACER is constructed before
the fixture registers the provider, so every recorded span also proves the
proxy tracer resolves through a later registration.
"""

import asyncio
import functools
import inspect
import io
import json
import logging
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode

from relay_runtime import Depends, bind
from relay_runtime.logging import JsonFormatter, Logger
from relay_runtime.tracing import Tracer

from di_support import InvocationContext, run_handler

SCOPE_NAME = "relay.tracing.tests"

# Constructed at module import, before the session fixture registers the
# provider: every span below also exercises proxy-tracer resolution.
TRACER = Tracer(SCOPE_NAME)
_EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="session")
def _provider() -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(_EXPORTER))
    trace.set_tracer_provider(provider)
    return provider


@pytest.fixture()
def exporter(_provider: TracerProvider) -> InMemorySpanExporter:
    _EXPORTER.clear()
    return _EXPORTER


def _finished(exporter: InMemorySpanExporter) -> tuple[ReadableSpan, ...]:
    return exporter.get_finished_spans()


def _only_span(exporter: InMemorySpanExporter) -> ReadableSpan:
    exported = _finished(exporter)
    assert len(exported) == 1, f"exported {len(exported)} spans, want exactly 1: {exported!r}"
    return exported[0]


def _span_named(exporter: InMemorySpanExporter, name: str) -> ReadableSpan:
    matches = [span for span in _finished(exporter) if span.name == name]
    assert len(matches) == 1, f"spans named {name!r} = {len(matches)}, want exactly 1"
    return matches[0]


def _span_blob(span: ReadableSpan) -> str:
    """Render every user-visible part of an exported span for leak checks."""
    return json.dumps(
        {
            "description": span.status.description,
            "events": [[event.name, dict(event.attributes)] for event in span.events],
            "attributes": dict(span.attributes or {}),
        },
        default=str,
    )


def _run_subprocess(script: str) -> subprocess.CompletedProcess[str]:
    src = Path(__file__).resolve().parents[1] / "src"
    env = dict(os.environ, PYTHONPATH=str(src))
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


# --- optional import -------------------------------------------------------


def test_tracing_import_fails_without_opentelemetry_and_base_imports_stay_clean() -> None:
    # given an interpreter where every opentelemetry import is blocked, so the
    # optional-extra boundary can be observed deterministically
    script = """
import sys

sys.modules["opentelemetry"] = None

# The base package and its other submodules stay importable without the API.
import relay_runtime
import relay_runtime.dependencies
import relay_runtime.logging
import relay_runtime.typing

assert "Tracer" not in relay_runtime.__all__, relay_runtime.__all__
try:
    from relay_runtime import Tracer  # noqa: F401
except ImportError:
    pass
else:
    raise AssertionError("Tracer must not be re-exported from relay_runtime")

# when the tracing submodule is imported
try:
    import relay_runtime.tracing  # noqa: F401
except ImportError as exc:
    # then the documented message is raised with no chained cause
    assert str(exc) == (
        "Install relay-runtime[tracing] to use relay_runtime.tracing."
    ), str(exc)
    assert exc.__cause__ is None and exc.__suppress_context__, exc
else:
    raise AssertionError("relay_runtime.tracing must require the optional API")

print("optional-import-ok")
"""
    result = _run_subprocess(script)
    assert result.returncode == 0, (
        f"subprocess failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "optional-import-ok" in result.stdout


def test_tracer_is_not_reexported_from_the_base_package() -> None:
    import relay_runtime

    assert "Tracer" not in relay_runtime.__all__
    with pytest.raises(ImportError, match="cannot import name 'Tracer'"):
        from relay_runtime import Tracer as _Tracer  # type: ignore[attr-defined]


# --- provider ownership and no-op behavior --------------------------------


def test_tracer_before_provider_is_noop_until_one_time_registration() -> None:
    # given a clean interpreter with no provider registered, when a Tracer is
    # created and used before any registration
    script = """
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from relay_runtime.tracing import Tracer

assert type(trace.get_tracer_provider()).__name__ == "ProxyTracerProvider"

TRACER = Tracer("early.scope")

# then spans are native but non-recording, and neither construction nor use
# installs a provider of its own
with TRACER.span("early.noop") as span:
    assert not span.is_recording(), span
    assert not span.get_span_context().is_valid, span
assert type(trace.get_tracer_provider()).__name__ == "ProxyTracerProvider"

# when the process registers its provider exactly once, after the Tracer exists
exporter = InMemorySpanExporter()
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(exporter))
trace.set_tracer_provider(provider)

# then the pre-existing Tracer resolves through the proxy and records under
# the name it was constructed with
with TRACER.span("early.recorded") as span:
    assert span.is_recording(), span
    span.set_attribute("early", True)
assert type(trace.get_tracer_provider()).__name__ == "TracerProvider"

spans = exporter.get_finished_spans()
assert [s.name for s in spans] == ["early.recorded"], spans
assert spans[0].instrumentation_scope.name == "early.scope"
assert dict(spans[0].attributes or {}) == {"early": True}
print("early-tracer-ok")
"""
    result = _run_subprocess(script)
    assert result.returncode == 0, (
        f"subprocess failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "early-tracer-ok" in result.stdout


# --- capture_method: decorator forms, metadata, names ----------------------


@pytest.mark.parametrize(
    ("form", "expected_name"),
    [
        ("direct", "test_capture_method_forms_record_spans.<locals>.direct_op"),
        ("empty", "test_capture_method_forms_record_spans.<locals>.empty_op"),
        ("configured", "orders.find"),
    ],
)
def test_capture_method_forms_record_spans(
    exporter: InMemorySpanExporter, form: str, expected_name: str
) -> None:
    # given the three documented decorator forms
    if form == "direct":

        @TRACER.capture_method
        def direct_op(x: int) -> int:
            return x

        target = direct_op
    elif form == "empty":

        @TRACER.capture_method()
        def empty_op(x: int) -> int:
            return x

        target = empty_op
    else:

        @TRACER.capture_method(name="orders.find")
        def configured_op(x: int) -> int:
            return x

        target = configured_op

    # when the decorated callable runs
    assert target(7) == 7

    # then exactly one span is exported under the default qualname or the
    # configured name, through the Tracer's instrumentation scope, with no
    # status and no ambient parent
    span = _only_span(exporter)
    assert span.name == expected_name
    assert span.instrumentation_scope is not None
    assert span.instrumentation_scope.name == SCOPE_NAME
    assert span.status.status_code == StatusCode.UNSET
    assert span.parent is None
    assert span.events == ()


def test_sync_call_preserves_return_value_signature_and_metadata(
    exporter: InMemorySpanExporter,
) -> None:
    # given a documented function with a signature worth preserving
    def original(a: int, b: str = "x") -> str:
        """Original doc."""
        return f"{a}{b}"

    wrapped = TRACER.capture_method(original)

    # when it is called through the wrapper
    assert wrapped(1, b="y") == "1y"

    # then the result is unchanged and functools.wraps kept the metadata and
    # the introspectable signature
    assert wrapped.__name__ == "original"
    assert wrapped.__qualname__ == original.__qualname__
    assert wrapped.__doc__ == "Original doc."
    assert wrapped.__wrapped__ is original
    assert inspect.signature(wrapped) == inspect.signature(original)
    assert _span_named(exporter, original.__qualname__)


def test_async_function_stays_async_and_records_after_await(
    exporter: InMemorySpanExporter,
) -> None:
    # given an async function decorated with the empty form
    observed: dict[str, int] = {}

    @TRACER.capture_method()
    async def fetch(user: str) -> dict[str, str]:
        """Fetch a user."""
        ctx = trace.get_current_span().get_span_context()
        observed["span_id"] = ctx.span_id
        await asyncio.sleep(0)
        return {"user": user}

    # then the wrapper is still a coroutine function with the original metadata
    assert inspect.iscoroutinefunction(fetch)
    assert fetch.__doc__ == "Fetch a user."

    # and no span exists until the coroutine actually runs
    pending = fetch("a")
    assert len(_finished(exporter)) == 0

    # when it is awaited, exactly one span covers the body and carries the
    # span id the body observed
    assert asyncio.run(pending) == {"user": "a"}
    span = _only_span(exporter)
    assert span.name == "test_async_function_stays_async_and_records_after_await.<locals>.fetch"
    assert span.context.span_id == observed["span_id"]
    assert span.status.status_code == StatusCode.UNSET


# --- exceptions and privacy -----------------------------------------------


def test_exception_is_reraised_unchanged_and_marks_generic_error(
    exporter: InMemorySpanExporter,
) -> None:
    # given a decorated call that raises an ordinary Exception with a
    # sensitive message
    failure = ValueError("sensitive-value-77")

    @TRACER.capture_method
    def fail() -> None:
        raise failure

    # when the wrapper sees the exception
    with pytest.raises(ValueError) as caught:
        fail()

    # then the very same exception instance propagates unchanged
    assert caught.value is failure
    assert caught.value.__traceback__ is not None

    # and the span carries the generic ERROR status: no description, no
    # exception event, and the message appears nowhere on the span
    span = _only_span(exporter)
    assert span.status.status_code == StatusCode.ERROR
    assert not span.status.description
    assert span.events == ()
    assert "sensitive-value-77" not in _span_blob(span)


def test_record_exception_opt_in_adds_the_native_exception_event(
    exporter: InMemorySpanExporter,
) -> None:
    # given the configured form with record_exception=True
    @TRACER.capture_method(name="orders.find", record_exception=True)
    def find() -> None:
        raise KeyError("missing-order")

    with pytest.raises(KeyError):
        find()

    # then the span still carries only the generic status while one native
    # exception event records the type, message, and traceback
    span = _span_named(exporter, "orders.find")
    assert span.status.status_code == StatusCode.ERROR
    assert not span.status.description
    (event,) = span.events
    assert event.name == "exception"
    assert "KeyError" in str(event.attributes["exception.type"])
    assert "missing-order" in str(event.attributes["exception.message"])
    assert "Traceback" in str(event.attributes["exception.stacktrace"])


@pytest.mark.parametrize("entry", ["decorated", "manual"])
@pytest.mark.parametrize(
    "signal",
    [KeyboardInterrupt, SystemExit, asyncio.CancelledError],
    ids=["keyboard-interrupt", "system-exit", "cancelled"],
)
def test_control_flow_signals_propagate_without_status_or_event(
    exporter: InMemorySpanExporter, entry: str, signal: type[BaseException]
) -> None:
    # given a BaseException-only control signal raised inside a span, through
    # either entry point
    if entry == "decorated":

        @TRACER.capture_method
        def interrupted() -> None:
            raise signal()

        with pytest.raises(signal):
            interrupted()
    else:
        with pytest.raises(signal):
            with TRACER.span("control.flow"):
                raise signal()

    # then the signal propagates and the span ends with no status change and
    # no exception event
    span = _only_span(exporter)
    assert span.status.status_code == StatusCode.UNSET
    assert span.events == ()


# --- validation ------------------------------------------------------------


def test_constructor_and_capture_method_validate_their_arguments() -> None:
    with pytest.raises(TypeError, match="name must be a string"):
        Tracer(42)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="name must be a string"):
        TRACER.span(42)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="capture_method expects a callable"):
        TRACER.capture_method(42)  # type: ignore[arg-type]

    class PlainClass:
        pass

    with pytest.raises(TypeError, match="cannot decorate a class"):
        TRACER.capture_method(PlainClass)

    with pytest.raises(TypeError, match="name must be a string or None"):
        TRACER.capture_method(lambda: None, name=42)  # type: ignore[arg-type]


# --- generators and async generators --------------------------------------


@pytest.mark.parametrize(
    "case",
    [
        "direct-generator",
        "wraps-generator",
        "wraps-generator-delegating",
        "direct-async-generator",
        "wraps-async-generator",
        "generator-callable-instance",
    ],
)
def test_generator_functions_are_rejected_at_decoration_time(
    exporter: InMemorySpanExporter, case: str
) -> None:
    # given generator and async-generator callables in every supported shape:
    # directly, behind functools.wraps metadata, and as a callable instance
    def generator_function():
        yield "x"

    async def async_generator_function():
        yield "x"

    if case == "direct-generator":
        target = generator_function
    elif case == "wraps-generator":

        @functools.wraps(generator_function)
        def delegating() -> None:
            return None

        target = delegating
    elif case == "wraps-generator-delegating":

        @functools.wraps(generator_function)
        def generator_wrapper():
            yield from generator_function()

        target = generator_wrapper
    elif case == "direct-async-generator":
        target = async_generator_function
    elif case == "wraps-async-generator":

        @functools.wraps(async_generator_function)
        def async_wrapper():
            return async_generator_function()

        target = async_wrapper
    else:

        class GeneratorCallable:
            def __call__(self):
                yield "x"

        target = GeneratorCallable()

    # when decoration happens
    # then it is rejected with the documented rationale, before any span could
    # end before iteration began
    with pytest.raises(TypeError, match="generator or async-generator"):
        TRACER.capture_method(target)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="generator or async-generator"):
        TRACER.capture_method()(target)  # type: ignore[arg-type]
    assert len(_finished(exporter)) == 0


# --- methods, nesting, composition ----------------------------------------


def test_instance_class_and_static_methods_record_spans(
    exporter: InMemorySpanExporter,
) -> None:
    # given the three method shapes, with classmethod and staticmethod applied
    # outside capture_method so the descriptors survive wrapping
    class Service:
        @TRACER.capture_method
        def instance_op(self, value: int) -> int:
            """Instance op."""
            return value + 1

        @classmethod
        @TRACER.capture_method
        def class_op(cls, value: int) -> int:
            return value + len(cls.__name__)

        @staticmethod
        @TRACER.capture_method
        def static_op(value: int) -> int:
            return value + 3

    service = Service()

    # when each shape is invoked through its natural call syntax
    assert service.instance_op(1) == 2
    assert Service.class_op(1) == 1 + len("Service")
    assert service.static_op(1) == 4

    # then each call produced one span under the method's qualname
    names = {span.name for span in _finished(exporter)}
    assert len(_finished(exporter)) == 3
    for suffix in ("Service.instance_op", "Service.class_op", "Service.static_op"):
        assert sum(name.endswith(suffix) for name in names) == 1, names
    assert service.instance_op.__name__ == "instance_op"
    assert service.instance_op.__doc__ == "Instance op."


def test_nested_function_and_wraps_decorators_compose(
    exporter: InMemorySpanExporter,
) -> None:
    # given a nested function and an ordinary wraps-based decorator stacked on
    # capture_method in both orders
    def outer() -> str:
        @TRACER.capture_method
        def inner() -> str:
            return "in"

        return inner()

    def tag(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    @TRACER.capture_method
    @tag
    def tagged_first(x: int) -> int:
        """First order."""
        return x

    @tag
    @TRACER.capture_method
    def tagged_second(x: int) -> int:
        """Second order."""
        return x

    # when every composition runs
    assert outer() == "in"
    assert tagged_first(1) == 1
    assert tagged_second(2) == 2

    # then one span per call is exported, the nested function keeps its full
    # qualname, and the ordinary decorator preserved the metadata
    exported = _finished(exporter)
    assert [span.name for span in exported] == [
        "test_nested_function_and_wraps_decorators_compose.<locals>.outer.<locals>.inner",
        "test_nested_function_and_wraps_decorators_compose.<locals>.tagged_first",
        "test_nested_function_and_wraps_decorators_compose.<locals>.tagged_second",
    ]
    assert tagged_first.__name__ == "tagged_first"
    assert tagged_second.__doc__ == "Second order."


def test_repeated_calls_record_independent_spans(
    exporter: InMemorySpanExporter,
) -> None:
    # given a decorated callable invoked repeatedly
    @TRACER.capture_method
    def tick(seed: int) -> int:
        return seed

    assert [tick(i) for i in range(3)] == [0, 1, 2]

    # then each call exported its own span: one shared name, three distinct
    # span ids and trace ids, no parent, no status
    exported = _finished(exporter)
    assert len(exported) == 3
    assert len({span.name for span in exported}) == 1
    assert len({span.context.span_id for span in exported}) == 3
    assert len({span.context.trace_id for span in exported}) == 3
    assert all(span.parent is None for span in exported)
    assert all(span.status.status_code == StatusCode.UNSET for span in exported)


# --- callable objects and awaitable-returning callables --------------------


def test_callable_instance_is_treated_as_synchronous(
    exporter: InMemorySpanExporter,
) -> None:
    # given a callable instance whose __call__ is a coroutine function
    class Loader:
        async def __call__(self) -> str:
            await asyncio.sleep(0)
            return "loaded"

    load = TRACER.capture_method(Loader())

    # when it is called, the documented sync treatment applies: the span ends
    # when the coroutine object is created, before the body runs
    pending = load()
    span = _only_span(exporter)
    assert span.name.endswith(".Loader")
    assert span.status.status_code == StatusCode.UNSET

    # and awaiting later does not add or reopen any span
    assert asyncio.run(pending) == "loaded"
    assert len(_finished(exporter)) == 1


def test_sync_function_returning_an_awaitable_is_traced_until_it_returns(
    exporter: InMemorySpanExporter,
) -> None:
    # given a sync function that answers with a coroutine
    async def fetch_work() -> str:
        await asyncio.sleep(0)
        return "fetched"

    @TRACER.capture_method
    def launch() -> object:
        return fetch_work()

    # when it is called, the documented sync treatment traces only until return
    pending = launch()
    span = _only_span(exporter)
    assert span.status.status_code == StatusCode.UNSET

    # and the awaited body runs after the span already ended
    assert asyncio.run(pending) == "fetched"
    assert len(_finished(exporter)) == 1


# --- manual spans ----------------------------------------------------------


def test_manual_span_yields_the_native_span_with_attributes_and_kind(
    exporter: InMemorySpanExporter,
) -> None:
    # when a manual span is opened with constructor attributes and a kind
    with TRACER.span(
        "users.export",
        attributes={"user.count": 42},
        kind=SpanKind.SERVER,
    ) as span:
        # then the yielded object is the native Span, it is recording and
        # current, and native APIs keep working inside the block
        assert isinstance(span, trace.Span)
        assert span.is_recording()
        assert trace.get_current_span() is span
        span.set_attribute("export.format", "csv")

    # and the exported span carries both the constructor and the late
    # attribute, the requested kind, the Tracer's scope, and no status
    exported = _span_named(exporter, "users.export")
    assert isinstance(exported.attributes["user.count"], int)
    assert exported.attributes["user.count"] == 42
    assert exported.attributes["export.format"] == "csv"
    assert exported.kind == SpanKind.SERVER
    assert exported.status.status_code == StatusCode.UNSET
    assert exported.instrumentation_scope is not None
    assert exported.instrumentation_scope.name == SCOPE_NAME
    assert exported.parent is None


def test_manual_span_nests_both_directions_and_restores_the_context(
    exporter: InMemorySpanExporter,
) -> None:
    native = trace.get_tracer("native.test")

    # direction one: native instrumentation inside a Tracer span
    with TRACER.span("manual.root") as manual_root:
        root_id = manual_root.get_span_context().span_id
        with native.start_as_current_span("native.under_manual") as native_inner:
            assert native_inner.parent.span_id == root_id
            native_inner.set_attribute("native", True)
        # the context is restored to the manual span after the native block
        assert trace.get_current_span() is manual_root
        with TRACER.span("manual.under_manual") as manual_inner:
            assert manual_inner.parent.span_id == root_id

    # direction two: a Tracer span inside a native parent
    with native.start_as_current_span("native.root") as native_root:
        root_id = native_root.get_span_context().span_id
        with TRACER.span("tracer.under_native") as child:
            assert child.parent.span_id == root_id

    # then the ambient context is fully restored after every block
    assert not trace.get_current_span().get_span_context().is_valid

    # and the exported tree mirrors the live parenting on one trace
    exported = {span.name: span for span in _finished(exporter)}
    assert set(exported) == {
        "manual.root",
        "native.under_manual",
        "manual.under_manual",
        "native.root",
        "tracer.under_native",
    }
    trace_ids = {span.context.trace_id for span in exported.values()}
    assert len(trace_ids) == 2, "one trace per root"
    manual_trace = exported["manual.root"].context.trace_id
    for name in ("native.under_manual", "manual.under_manual"):
        assert exported[name].parent.span_id == exported["manual.root"].context.span_id
        assert exported[name].context.trace_id == manual_trace
    assert exported["tracer.under_native"].parent.span_id == (
        exported["native.root"].context.span_id
    )
    assert exported["tracer.under_native"].context.trace_id == (
        exported["native.root"].context.trace_id
    )
    assert dict(exported["native.under_manual"].attributes or {}) == {"native": True}
    assert exported["manual.root"].parent is None


def test_manual_span_exception_sets_generic_error_without_event(
    exporter: InMemorySpanExporter,
) -> None:
    # when an ordinary Exception leaves a manual span
    with pytest.raises(RuntimeError, match="manual-sensitive"):
        with TRACER.span("export.failed"):
            raise RuntimeError("manual-sensitive")

    # then only the generic status is set: no description, no event, and the
    # message appears nowhere on the span
    span = _span_named(exporter, "export.failed")
    assert span.status.status_code == StatusCode.ERROR
    assert not span.status.description
    assert span.events == ()
    assert "manual-sensitive" not in _span_blob(span)


def test_manual_span_record_exception_opt_in_adds_the_native_event(
    exporter: InMemorySpanExporter,
) -> None:
    # when record_exception=True is set on a manual span that raises
    with pytest.raises(LookupError):
        with TRACER.span("export.failed", record_exception=True):
            raise LookupError("optin-lookup")

    # then one native exception event records the message alongside the
    # generic status
    span = _span_named(exporter, "export.failed")
    assert span.status.status_code == StatusCode.ERROR
    (event,) = span.events
    assert event.name == "exception"
    assert "optin-lookup" in str(event.attributes["exception.message"])


# --- concurrency -----------------------------------------------------------


def test_concurrent_calls_keep_independent_contexts(
    exporter: InMemorySpanExporter,
) -> None:
    async def scenario() -> tuple[int, list[tuple[int, int]]]:
        with TRACER.span("batch.root") as root:
            root_id = root.get_span_context().span_id

            @TRACER.capture_method
            async def task(n: int) -> tuple[int, int]:
                # each task must observe its OWN span, before and after any
                # suspension point
                before = trace.get_current_span().get_span_context().span_id
                await asyncio.sleep(0.01)
                after = trace.get_current_span().get_span_context().span_id
                return before, after

            return root_id, list(await asyncio.gather(task(1), task(2)))

    root_id, results = asyncio.run(scenario())

    # then both calls saw their own stable span id: no context bleed between
    # the concurrently gathered tasks
    (first_before, first_after), (second_before, second_after) = results
    assert first_before == first_after
    assert second_before == second_after
    assert first_before != second_before

    # and both exported spans are children of the shared root on one trace,
    # with ids matching what each task observed
    task_spans = [
        span for span in _finished(exporter) if span.name.endswith(".task")
    ]
    assert len(task_spans) == 2
    assert all(span.parent.span_id == root_id for span in task_spans)
    assert len({span.context.trace_id for span in task_spans}) == 1
    assert {span.context.span_id for span in task_spans} == {
        first_before,
        second_before,
    }
    root_span = _span_named(exporter, "batch.root")
    assert root_span.parent is None


# --- Logger correlation ----------------------------------------------------


def test_logger_records_carry_the_active_span_ids(
    exporter: InMemorySpanExporter,
) -> None:
    # given a Logger writing JSON records to a private buffer
    buffer = io.StringIO()
    log = Logger(
        f"tracer-corr-{uuid.uuid4().hex[:8]}",
        "INFO",
        logger_handler=logging.StreamHandler(buffer),
        logger_formatter=JsonFormatter(),
    )
    observed: dict[str, str] = {}

    @TRACER.capture_method
    def traced_method() -> None:
        ctx = trace.get_current_span().get_span_context()
        observed["captured_trace"] = format(ctx.trace_id, "032x")
        observed["captured_span"] = format(ctx.span_id, "016x")
        log.info("captured-record")
        with TRACER.span("manual.inner") as inner:
            ictx = inner.get_span_context()
            observed["manual_trace"] = format(ictx.trace_id, "032x")
            observed["manual_span"] = format(ictx.span_id, "016x")
            log.info("manual-record")

    # when records are emitted inside a parent manual span and inside the
    # captured method's own span
    with TRACER.span("parent.manual") as parent:
        pctx = parent.get_span_context()
        observed["parent_trace"] = format(pctx.trace_id, "032x")
        observed["parent_span"] = format(pctx.span_id, "016x")
        log.info("parent-record")
        traced_method()

    # then each record carries exactly the trace and span that were active
    # where it was logged
    records = {}
    for line in buffer.getvalue().splitlines():
        if line.startswith("{"):
            record = json.loads(line)
            records[record["message"]] = record
    for message, span_key in (
        ("parent-record", "parent"),
        ("captured-record", "captured"),
        ("manual-record", "manual"),
    ):
        record = records[message]
        assert record["trace_id"] == observed[f"{span_key}_trace"]
        assert record["span_id"] == observed[f"{span_key}_span"]

    # and every logged pair references an actually exported span, with the
    # expected parent chain
    exported_by_id = {
        format(span.context.span_id, "016x"): span
        for span in _finished(exporter)
    }
    parent_span = exported_by_id[observed["parent_span"]]
    captured_span = exported_by_id[observed["captured_span"]]
    manual_span = exported_by_id[observed["manual_span"]]
    assert parent_span.name == "parent.manual"
    assert captured_span.name.endswith("traced_method")
    assert manual_span.name == "manual.inner"
    assert captured_span.parent.span_id == parent_span.context.span_id
    assert manual_span.parent.span_id == captured_span.context.span_id
    assert records["captured-record"]["trace_id"] == format(
        captured_span.context.trace_id, "032x"
    )


# --- composition with dependency injection --------------------------------


def test_bind_outside_capture_method_resolves_dependencies_before_the_span(
    exporter: InMemorySpanExporter,
) -> None:
    # given a dependency provider that records whether a span was active when
    # it resolved
    resolution: list[bool] = []

    def provide_limit() -> int:
        resolution.append(trace.get_current_span().get_span_context().is_valid)
        return 7

    @bind
    @TRACER.capture_method
    def handler(
        event: dict[str, int],
        context: object,
        limit: int = Depends(provide_limit),
    ) -> int:
        """Handle arithmetic."""
        return event["n"] + limit

    # when Relay invokes it with exactly two arguments
    result = run_handler(handler, {"n": 1}, InvocationContext())

    # then dependency resolution happened BEFORE the span started, the handler
    # body ran inside one span, and DI and the published contract are intact
    assert result == 8
    assert resolution == [False]
    span = _span_named(exporter, handler.__qualname__)
    assert span.status.status_code == StatusCode.UNSET
    assert list(inspect.signature(handler).parameters) == ["event", "context"]
    assert handler.__doc__ == "Handle arithmetic."


def test_capture_method_outside_bind_spans_resolution_and_body(
    exporter: InMemorySpanExporter,
) -> None:
    # given a dependency provider that records the active span id it observed
    resolution: list[str] = []

    def provide_limit() -> int:
        ctx = trace.get_current_span().get_span_context()
        resolution.append(format(ctx.span_id, "016x") if ctx.is_valid else "")
        return 5

    @TRACER.capture_method
    @bind
    def handler(
        event: dict[str, int],
        context: object,
        limit: int = Depends(provide_limit),
    ) -> int:
        """Handle arithmetic."""
        return event["n"] + limit

    # when Relay invokes it with exactly two arguments
    result = run_handler(handler, {"n": 2}, InvocationContext())

    # then dependency resolution ran INSIDE the span, DI still injected, and
    # the published two-argument contract survived both layers
    assert result == 7
    span = _span_named(exporter, handler.__qualname__)
    assert resolution == [format(span.context.span_id, "016x")]
    assert span.status.status_code == StatusCode.UNSET
    assert list(inspect.signature(handler).parameters) == ["event", "context"]
    assert handler.__doc__ == "Handle arithmetic."


# --- correlation without an active span ------------------------------------


def test_records_without_an_active_span_omit_trace_correlation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # given the API is importable (this suite runs with it) but no span is
    # current: the default context and an explicit empty context
    service = f"no-span-{uuid.uuid4().hex[:8]}"
    buffer = io.StringIO()
    log = Logger(
        service,
        "INFO",
        logger_handler=logging.StreamHandler(buffer),
        logger_formatter=JsonFormatter(),
    )

    log.info("no-span-record")
    token = context_api.attach(Context())
    try:
        log.info("root-context-record")
    finally:
        context_api.detach(token)

    records = {}
    for line in buffer.getvalue().splitlines():
        if line.startswith("{"):
            record = json.loads(line)
            records[record["message"]] = record
    for message in ("no-span-record", "root-context-record"):
        record = records[message]
        assert "trace_id" not in record, f"{message} must not pair a span: {record!r}"
        assert "span_id" not in record, f"{message} must not pair a span: {record!r}"


# --- hostile provider failures ---------------------------------------------


class _HostileSpan:
    """A provider span whose telemetry operations all raise."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def get_span_context(self) -> object:
        return trace.INVALID_SPAN_CONTEXT

    def is_recording(self) -> bool:
        return False

    def set_status(self, status: object) -> None:
        self._calls.append("set_status")
        raise RuntimeError("hostile-set-status")

    def record_exception(self, exception: object) -> None:
        self._calls.append("record_exception")
        raise RuntimeError("hostile-record-exception")

    def end(self) -> None:
        self._calls.append("end")
        raise RuntimeError("hostile-end")


class _HostileTracer:
    """A native tracer whose spans always fail their telemetry operations."""

    def __init__(self) -> None:
        self.names: list[str] = []
        self.calls: list[str] = []

    def start_span(self, name: str, **kwargs: object) -> _HostileSpan:
        self.names.append(name)
        return _HostileSpan(self.calls)


def _hostile_tracer(monkeypatch: pytest.MonkeyPatch) -> _HostileTracer:
    hostile = _HostileTracer()
    monkeypatch.setattr(trace, "get_tracer", lambda *args, **kwargs: hostile)
    return hostile


@pytest.mark.parametrize("entry", ["manual", "decorated"])
def test_provider_failures_never_replace_the_block_outcome(
    monkeypatch: pytest.MonkeyPatch, entry: str
) -> None:
    # given a provider whose span operations all raise, wired in where the
    # facade resolves its native tracer
    hostile = _hostile_tracer(monkeypatch)
    tracer = Tracer("hostile.provider")

    # when the block succeeds and when it fails, through either entry point
    if entry == "manual":

        def run_success() -> str:
            with tracer.span("hostile.success"):
                return "block-result"

        assert run_success() == "block-result"
        failure = ValueError("application-failure")
        with pytest.raises(ValueError) as caught:
            with tracer.span("hostile.error", record_exception=True):
                raise failure
        # then the application's own result and exception survive unchanged:
        # no provider failure escapes or replaces them
        assert caught.value is failure
    else:

        @tracer.capture_method
        def succeed() -> str:
            return "decorated-result"

        assert succeed() == "decorated-result"
        with pytest.raises(KeyError, match="application-key") as caught:

            @tracer.capture_method
            def fail() -> None:
                raise KeyError("application-key")

            fail()
        assert isinstance(caught.value, KeyError)

    # and every failing provider call was attempted best effort, not skipped
    assert "set_status" in hostile.calls
    assert "end" in hostile.calls
    if entry == "manual":
        assert "record_exception" in hostile.calls


def test_a_failing_context_detach_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given a context detach that restores the context and then fails
    hostile = _hostile_tracer(monkeypatch)
    tracer = Tracer("hostile.detach")
    real_detach = context_api.detach
    attempts: list[str] = []

    def failing_detach(token: object) -> None:
        real_detach(token)
        attempts.append("detach")
        raise RuntimeError("hostile-detach")

    monkeypatch.setattr(context_api, "detach", failing_detach)

    # when a span block completes normally
    with tracer.span("hostile.detach-span"):
        pass

    # then the detach failure is swallowed after restoring the context
    assert attempts == ["detach"]
    assert hostile.calls == ["end"]
    assert not trace.get_current_span().get_span_context().is_valid
