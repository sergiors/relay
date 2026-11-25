"""Generator finalizers and the LIFO cleanup stack used by both scopes.

Context-manager stacks are not used directly. Their default exit semantics
allow a generator to suppress or replace an active exception, and do not shield
an in-flight asynchronous cleanup from cancellation. Adapting them to preserve
the original invocation outcome and continue cleanup would still require custom
LIFO handling. This stack also enforces the single-yield provider contract.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncGenerator, Awaitable, Generator
from dataclasses import dataclass
from typing import Any, Protocol

from ._model import _DependencyError

_LOGGER = logging.getLogger("relay_runtime.dependencies")


class _Finalizer(Protocol):
    """A started generator provider awaiting its cleanup path."""

    name: str

    def close(self) -> Awaitable[Any] | Any: ...

    def throw(self, exception: BaseException) -> Awaitable[Any] | Any: ...

    def resume(self) -> Awaitable[Any] | Any: ...


class _SyncGeneratorFinalizer:
    __slots__ = ("name", "_generator")

    def __init__(self, name: str, generator: Generator[Any, Any, Any]) -> None:
        self.name = name
        self._generator = generator

    def close(self) -> None:
        self._generator.close()

    def throw(self, exception: BaseException) -> Any:
        return self._generator.throw(exception)

    def resume(self) -> Any:
        return next(self._generator)


class _AsyncGeneratorFinalizer:
    __slots__ = ("name", "_generator")

    def __init__(self, name: str, generator: AsyncGenerator[Any, Any]) -> None:
        self.name = name
        self._generator = generator

    def close(self) -> Awaitable[Any]:
        return self._generator.aclose()

    def throw(self, exception: BaseException) -> Awaitable[Any]:
        return self._generator.athrow(exception)

    def resume(self) -> Awaitable[Any]:
        return self._generator.__anext__()


def start_generator(
    name: str, generator: Generator[Any, Any, Any]
) -> tuple[Any, _Finalizer]:
    finalizer = _SyncGeneratorFinalizer(name, generator)
    try:
        value = next(generator)
    except StopIteration as exc:
        raise _DependencyError(
            f"dependency {name!r} generator did not yield a value"
        ) from exc
    except BaseException:
        try:
            finalizer.close()
        except BaseException as exc:
            _log_cleanup_failure(name, exc)
        raise
    return value, finalizer


async def start_async_generator(
    name: str, generator: AsyncGenerator[Any, Any]
) -> tuple[Any, _Finalizer]:
    finalizer = _AsyncGeneratorFinalizer(name, generator)
    try:
        value = await generator.__anext__()
    except StopAsyncIteration as exc:
        raise _DependencyError(
            f"dependency {name!r} async generator did not yield a value"
        ) from exc
    except BaseException as original:
        await _close_quietly(finalizer, original)
        raise
    return value, finalizer


async def _close_quietly(
    finalizer: _Finalizer, original: BaseException | None = None
) -> None:
    try:
        errors, _ = await _force_close(finalizer)
    except BaseException as exc:
        _log_cleanup_failure(finalizer.name, exc)
        return

    for error in errors:
        cause = error.__cause__
        if cause is original:
            continue
        _log_cleanup_failure(finalizer.name, cause if cause is not None else error)


def _log_cleanup_failure(name: str, exc: BaseException) -> None:
    _LOGGER.error(
        "dependency generator cleanup failed while reporting another error",
        extra={"dependency": name},
        exc_info=(type(exc), exc, exc.__traceback__),
    )


@dataclass(frozen=True)
class _AwaitOutcome:
    error: BaseException | None
    cancelled: bool


async def _capture_cleanup(awaitable: Awaitable[Any]) -> BaseException | None:
    try:
        await awaitable
        return None
    except BaseException as exc:
        return exc


async def _await_cleanup(awaitable: Awaitable[Any]) -> _AwaitOutcome:
    task = asyncio.ensure_future(_capture_cleanup(awaitable))
    cancelled = False

    # A cancelled invocation must still finish its resource cleanup. Shield the
    # cleanup task and keep waiting after each CancelledError, then let the
    # caller re-raise cancellation once every resource has been attempted.
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True

    try:
        error = task.result()
    except asyncio.CancelledError as exc:
        return _AwaitOutcome(exc, True)
    return _AwaitOutcome(error, cancelled)


def _cleanup_failure(name: str, exc: BaseException) -> BaseException:
    error = _DependencyError(f"dependency {name!r} cleanup failed: {exc}")
    error.__cause__ = exc
    return error


def _yielded_after_failure(name: str) -> BaseException:
    return _DependencyError(
        f"dependency {name!r} yielded after an invocation failure"
    )


def _exactly_one_yield_failure(name: str) -> BaseException:
    return _DependencyError(
        f"dependency {name!r} must yield exactly once; "
        "it yielded again during cleanup"
    )


async def _force_close(
    finalizer: _Finalizer,
) -> tuple[list[BaseException], bool]:
    try:
        result = finalizer.close()
    except BaseException as exc:
        return [_cleanup_failure(finalizer.name, exc)], False

    if not inspect.isawaitable(result):
        return [], False

    outcome = await _await_cleanup(result)
    if outcome.error is not None:
        return [_cleanup_failure(finalizer.name, outcome.error)], outcome.cancelled
    return [], outcome.cancelled


async def _finalize_successfully(
    finalizer: _Finalizer,
) -> tuple[list[BaseException], bool]:
    # A suspended generator's normal teardown is the code after its single
    # yield, not GeneratorExit from close(). Resume it and require termination.
    try:
        result = finalizer.resume()
    except (StopIteration, StopAsyncIteration):
        return [], False
    except BaseException as exc:
        return [_cleanup_failure(finalizer.name, exc)], False

    if not inspect.isawaitable(result):
        close_errors, close_cancelled = await _force_close(finalizer)
        return (
            [_exactly_one_yield_failure(finalizer.name), *close_errors],
            close_cancelled,
        )

    outcome = await _await_cleanup(result)
    if outcome.error is not None:
        if isinstance(outcome.error, (StopIteration, StopAsyncIteration)):
            return [], outcome.cancelled
        return [_cleanup_failure(finalizer.name, outcome.error)], outcome.cancelled

    close_errors, close_cancelled = await _force_close(finalizer)
    return (
        [_exactly_one_yield_failure(finalizer.name), *close_errors],
        outcome.cancelled or close_cancelled,
    )


async def _throw_finalizer(
    finalizer: _Finalizer, active: BaseException
) -> tuple[list[BaseException], bool]:
    try:
        result = finalizer.throw(active)
    except (StopIteration, StopAsyncIteration):
        return [], False
    except BaseException as exc:
        if exc is active:
            return [], False
        return [_cleanup_failure(finalizer.name, exc)], False

    if not inspect.isawaitable(result):
        close_errors, close_cancelled = await _force_close(finalizer)
        return (
            [_yielded_after_failure(finalizer.name), *close_errors],
            close_cancelled,
        )

    outcome = await _await_cleanup(result)
    if outcome.error is not None:
        if outcome.error is active:
            return [], outcome.cancelled
        if isinstance(outcome.error, (StopIteration, StopAsyncIteration)):
            return [], outcome.cancelled
        return [_cleanup_failure(finalizer.name, outcome.error)], outcome.cancelled

    close_errors, close_cancelled = await _force_close(finalizer)
    return (
        [_yielded_after_failure(finalizer.name), *close_errors],
        outcome.cancelled or close_cancelled,
    )


async def _run_cleanups(
    finalizers: list[_Finalizer],
    active_exception: BaseException | None = None,
) -> tuple[list[BaseException], bool]:
    errors: list[BaseException] = []
    cancelled = False

    for finalizer in reversed(finalizers):
        try:
            if active_exception is None:
                found, inner_cancelled = await _finalize_successfully(finalizer)
            else:
                found, inner_cancelled = await _throw_finalizer(
                    finalizer, active_exception
                )
        except asyncio.CancelledError:
            cancelled = True
            continue
        except BaseException as exc:
            found = [_cleanup_failure(finalizer.name, exc)]
            inner_cancelled = False

        errors.extend(found)
        cancelled = cancelled or inner_cancelled

    return errors, cancelled


class _FinalizerStack:
    """Invocation-owned LIFO stack of started generator providers."""

    def __init__(self) -> None:
        self._finalizers: list[_Finalizer] = []

    def append(self, finalizer: _Finalizer) -> None:
        self._finalizers.append(finalizer)

    async def close(
        self, active_exception: BaseException | None = None
    ) -> tuple[list[BaseException], bool]:
        finalizers, self._finalizers = self._finalizers, []
        try:
            return await _run_cleanups(finalizers, active_exception)
        except asyncio.CancelledError:
            return [], True
        except BaseException as exc:
            return [_DependencyError(f"dependency cleanup failed: {exc}")], False
