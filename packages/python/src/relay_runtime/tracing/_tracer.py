"""Thin tracing helpers over the official OpenTelemetry API."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import ParamSpec, TypeVar, cast, overload

from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from opentelemetry.util.types import AttributeValue

__all__ = ["Tracer"]

_P = ParamSpec("_P")
_R = TypeVar("_R")


@contextmanager
def _span_scope(
    native_tracer: trace.Tracer,
    name: str,
    *,
    attributes: Mapping[str, AttributeValue] | None,
    kind: SpanKind,
    record_exception: bool,
) -> Iterator[Span]:
    with native_tracer.start_as_current_span(
        name,
        kind=kind,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            yield span
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR))
            if record_exception:
                span.record_exception(exc)
            raise


class Tracer:
    """Create spans through the process's active OpenTelemetry provider.

    ``name`` is the instrumentation scope reported to the provider, not a
    service name. The facade does not parse or override OpenTelemetry
    environment configuration. Provider lookup is delegated to the native API,
    which may lazily resolve a provider from its environment configuration. The
    tracer does not install or shut down a provider.
    """

    __slots__ = ("_tracer",)

    def __init__(self, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        self._tracer = trace.get_tracer(name)

    @overload
    def capture_method(
        self,
        func: Callable[_P, _R],
        /,
        *,
        name: str | None = None,
        record_exception: bool = False,
    ) -> Callable[_P, _R]: ...

    @overload
    def capture_method(
        self,
        func: None = None,
        /,
        *,
        name: str | None = None,
        record_exception: bool = False,
    ) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]: ...

    def capture_method(
        self,
        func: Callable[..., object] | None = None,
        /,
        *,
        name: str | None = None,
        record_exception: bool = False,
    ) -> (
        Callable[..., object]
        | Callable[[Callable[..., object]], Callable[..., object]]
    ):
        """Decorate a callable so each call runs inside a span.

        Supports ``@capture_method``, ``@capture_method()``, and the configured
        keyword form. Synchronous callables stay synchronous and async
        functions stay async. Generator functions are rejected at decoration
        time because their span would end before the body is iterated.
        """
        if func is not None:
            return self._decorate(
                func, name=name, record_exception=record_exception
            )

        def decorator(inner: Callable[..., object]) -> Callable[..., object]:
            return self._decorate(
                inner, name=name, record_exception=record_exception
            )

        return decorator

    def span(
        self,
        name: str,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        record_exception: bool = False,
    ) -> AbstractContextManager[Span]:
        """Start a native span as the current span for a ``with`` block.

        Use an ordinary ``with`` in synchronous and asynchronous code; the
        native OpenTelemetry context manager is not asynchronous. An ordinary
        ``Exception`` marks the span as failed without a description and is
        re-raised. Process-control exceptions and cancellation propagate
        without an exception event or status change.
        """
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        return _span_scope(
            self._tracer,
            name,
            attributes=attributes,
            kind=kind,
            record_exception=record_exception,
        )

    def _decorate(
        self,
        func: Callable[_P, _R],
        *,
        name: str | None,
        record_exception: bool,
    ) -> Callable[_P, _R]:
        if not callable(func):
            raise TypeError("capture_method expects a callable")
        if inspect.isclass(func):
            raise TypeError(
                "capture_method cannot decorate a class; decorate its methods "
                "or a callable instance instead"
            )
        if _is_generator_callable(func):
            raise TypeError(
                "capture_method cannot decorate generator or async-generator "
                "functions because the span would end before iteration; wrap "
                "the iteration in tracer.span(...) instead"
            )
        if name is not None and not isinstance(name, str):
            raise TypeError("name must be a string or None")

        span_name = name if name is not None else _default_span_name(func)
        if inspect.iscoroutinefunction(func):
            coroutine_func = cast(Callable[_P, Awaitable[_R]], func)

            @functools.wraps(func)
            async def async_wrapper(
                *args: _P.args, **kwargs: _P.kwargs
            ) -> _R:
                with _span_scope(
                    self._tracer,
                    span_name,
                    attributes=None,
                    kind=SpanKind.INTERNAL,
                    record_exception=record_exception,
                ):
                    return await coroutine_func(*args, **kwargs)

            return cast(Callable[_P, _R], async_wrapper)

        @functools.wraps(func)
        def sync_wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            with _span_scope(
                self._tracer,
                span_name,
                attributes=None,
                kind=SpanKind.INTERNAL,
                record_exception=record_exception,
            ):
                return func(*args, **kwargs)

        return sync_wrapper


def _is_generator_callable(func: object) -> bool:
    if _is_generator_function(func):
        return True
    call = getattr(func, "__call__", None)
    return call is not None and _is_generator_function(call)


def _is_generator_function(func: object) -> bool:
    if not callable(func):
        return False
    if inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func):
        return True
    try:
        unwrapped = inspect.unwrap(func)
    except ValueError:
        return False
    return inspect.isgeneratorfunction(unwrapped) or inspect.isasyncgenfunction(
        unwrapped
    )


def _default_span_name(func: object) -> str:
    if isinstance(func, functools.partial):
        return _default_span_name(func.func)
    qualname = getattr(func, "__qualname__", None)
    if isinstance(qualname, str) and qualname:
        return qualname
    return type(func).__qualname__
