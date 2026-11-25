"""Resource lifecycle: generator providers, teardown order, failure safety."""

import asyncio

import pytest

from relay_runtime import Depends, bind

from di_support import InvocationContext, run_handler


def test_invocation_scoped_resource_closes_after_every_invocation() -> None:
    # given an invocation-scoped generator provider
    events: list[str] = []

    def provide_conn():
        events.append("open")
        yield "conn"
        events.append("close")

    @bind
    def handler(event, context, conn: str = Depends(provide_conn)) -> str:
        events.append(f"run:{conn}")
        return conn

    context = InvocationContext()

    # when two invocations run
    assert run_handler(handler, {}, context) == "conn"
    # then the resource was open during the run and closed right after
    assert events == ["open", "run:conn", "close"]

    # and the next invocation opens a fresh resource
    assert run_handler(handler, {}, context) == "conn"
    assert events == ["open", "run:conn", "close", "open", "run:conn", "close"]


def test_container_scoped_resource_stays_open_across_invocations() -> None:
    # given a container-scoped generator provider
    events: list[str] = []

    def provide_conn():
        events.append("open")
        yield "conn"
        events.append("close")

    @bind
    def handler(event, context, conn: str = Depends(provide_conn, scope="container")):
        events.append(f"run:{conn}")
        return conn

    context = InvocationContext()

    # when two warm invocations run
    assert run_handler(handler, {}, context) == "conn"
    assert run_handler(handler, {}, context) == "conn"

    # then the resource opened once and outlived both invocations;
    # its teardown belongs to graceful runtime shutdown (see the bootstrap
    # integration coverage)
    assert events == ["open", "run:conn", "run:conn"]


def test_cleanups_run_in_reverse_initialization_order() -> None:
    # given three independent invocation-scoped resources
    events: list[str] = []

    def first():
        events.append("first-open")
        yield "1"
        events.append("first-close")

    def second():
        events.append("second-open")
        yield "2"
        events.append("second-close")

    def third():
        events.append("third-open")
        yield "3"
        events.append("third-close")

    @bind
    def handler(
        event,
        context,
        one=Depends(first),
        two=Depends(second),
        three=Depends(third),
    ) -> None:
        events.append("handler")

    # when the handler runs
    run_handler(handler, {}, InvocationContext())

    # then cleanup unwinds in the opposite order of initialization
    assert events == [
        "first-open",
        "second-open",
        "third-open",
        "handler",
        "third-close",
        "second-close",
        "first-close",
    ]


def test_nested_resource_closes_after_its_dependent() -> None:
    # given a repository provider built on a database provider
    events: list[str] = []

    def provide_db():
        events.append("db-open")
        yield "db"
        events.append("db-close")

    def provide_repo(db: str = Depends(provide_db)):
        events.append("repo-open")
        yield "repo"
        events.append("repo-close")

    @bind
    def handler(
        event,
        context,
        repo=Depends(provide_repo),
        db=Depends(provide_db),
    ) -> None:
        events.append("handler")

    # when the handler runs against the shared database instance
    run_handler(handler, {}, InvocationContext())

    # then the dependent closes before its own dependency
    assert events == [
        "db-open",
        "repo-open",
        "handler",
        "repo-close",
        "db-close",
    ]


def test_cleanup_runs_when_the_handler_fails() -> None:
    # given an invocation-scoped resource guarding its teardown
    events: list[str] = []

    def provide_conn():
        events.append("open")
        try:
            yield "conn"
        finally:
            events.append("close")

    # given a handler that raises
    @bind
    def handler(event, context, conn: str = Depends(provide_conn)) -> None:
        raise ValueError("handler-boom")

    # when the invocation runs
    with pytest.raises(ValueError, match="handler-boom"):
        run_handler(handler, {}, InvocationContext())

    # then the resource still closed and the original error survived
    assert events == ["open", "close"]


def test_cleanup_runs_when_resolution_fails_midway() -> None:
    # given two initialized resources followed by a failing provider
    events: list[str] = []

    def first():
        events.append("first-open")
        try:
            yield "1"
        finally:
            events.append("first-close")

    def second():
        events.append("second-open")
        try:
            yield "2"
        finally:
            events.append("second-close")

    def failing() -> str:
        raise ValueError("init-boom")

    @bind
    def handler(
        event,
        context,
        a=Depends(first),
        b=Depends(second),
        c=Depends(failing),
    ) -> None:
        events.append("handler")

    # when resolution fails on the third dependency
    with pytest.raises(ValueError, match="init-boom"):
        run_handler(handler, {}, InvocationContext())

    # then the already-initialized resources unwound in reverse order
    # and the handler never ran
    assert events == [
        "first-open",
        "second-open",
        "second-close",
        "first-close",
    ]


def test_original_exception_survives_a_failing_cleanup() -> None:
    # given a resource whose teardown itself fails
    def provide_conn():
        try:
            yield "conn"
        finally:
            raise RuntimeError("cleanup-boom")

    # given a handler that raises before cleanup starts
    @bind
    def handler(event, context, conn: str = Depends(provide_conn)) -> None:
        raise ValueError("handler-boom")

    # when both failures happen in one invocation
    # then the handler's original exception is what the caller sees
    with pytest.raises(ValueError, match="handler-boom"):
        run_handler(handler, {}, InvocationContext())


def test_async_generator_provider_closes_after_the_invocation() -> None:
    # given an async generator provider
    events: list[str] = []

    async def provide_conn():
        events.append("open")
        yield "conn"
        events.append("close")

    @bind
    def handler(event, context, conn: str = Depends(provide_conn)) -> str:
        events.append(f"run:{conn}")
        return conn

    # when the invocation is run and settled
    result = run_handler(handler, {}, InvocationContext())

    # then the async resource opened, served, and closed in order
    assert result == "conn"
    assert events == ["open", "run:conn", "close"]


def test_async_handler_cleanup_runs_after_the_await() -> None:
    # given an async handler with a sync resource
    events: list[str] = []

    def provide_conn():
        events.append("open")
        yield "conn"
        events.append("close")

    @bind
    async def handler(event, context, conn: str = Depends(provide_conn)) -> None:
        await asyncio.sleep(0)
        events.append("handler")

    # when the handler is invoked and awaited
    run_handler(handler, {}, InvocationContext())

    # then cleanup ran only after the awaited body finished
    assert events == ["open", "handler", "close"]


def test_sync_generator_yielding_twice_fails_the_invocation() -> None:
    # given a provider that yields a second value
    def bad_provider():
        yield "one"
        yield "two"

    @bind
    def handler(event, context, value=Depends(bad_provider)) -> None:
        pass

    # when the invocation runs
    # then the invocation fails with the stable dependency error naming the
    # provider and the exactly-once yield rule
    with pytest.raises(
        RuntimeError, match="bad_provider' must yield exactly once"
    ):
        run_handler(handler, {}, InvocationContext())


def test_async_generator_yielding_twice_fails_the_invocation() -> None:
    # given an async provider that yields a second value
    async def bad_provider():
        yield "one"
        yield "two"

    @bind
    def handler(event, context, value=Depends(bad_provider)) -> None:
        pass

    # when the invocation runs
    # then the invocation fails with the stable dependency error naming the
    # provider and the exactly-once yield rule
    with pytest.raises(
        RuntimeError, match="bad_provider' must yield exactly once"
    ):
        run_handler(handler, {}, InvocationContext())


def test_sync_generator_yielding_nothing_fails_the_invocation() -> None:
    # given a provider that is a generator but yields no value
    def empty_provider():
        yield from ()

    @bind
    def handler(event, context, value=Depends(empty_provider)) -> None:
        pass

    # when the invocation runs
    # then the missing value fails it with a diagnosable error
    with pytest.raises(RuntimeError, match="did not yield"):
        run_handler(handler, {}, InvocationContext())


def test_async_generator_yielding_nothing_fails_the_invocation() -> None:
    # given an async provider that yields no value
    async def empty_provider():
        if False:  # pragma: no cover - makes this an async generator
            yield "never"

    @bind
    def handler(event, context, value=Depends(empty_provider)) -> None:
        pass

    # when the invocation runs
    # then the missing value fails it
    with pytest.raises(RuntimeError, match="did not yield"):
        run_handler(handler, {}, InvocationContext())


def test_sync_generator_sees_the_handler_failure_and_original_propagates() -> None:
    # given an invocation-scoped resource observing what fails it
    events: list[str] = []

    def provide_conn():
        events.append("open")
        try:
            yield "conn"
        except ValueError as exc:
            events.append(f"seen:{exc}")

    # given a handler that raises
    @bind
    def handler(event, context, conn: str = Depends(provide_conn)) -> None:
        raise ValueError("handler-boom")

    # when the invocation fails
    with pytest.raises(ValueError, match="handler-boom"):
        run_handler(handler, {}, InvocationContext())

    # then the generator's except block observed the failure, and the
    # provider suppressing it did not change the invocation's outcome
    assert events == ["open", "seen:handler-boom"]


def test_async_generator_sees_the_handler_failure_and_original_propagates() -> None:
    # given an async resource observing what fails it
    events: list[str] = []

    async def provide_conn():
        events.append("open")
        try:
            yield "conn"
        except ValueError as exc:
            events.append(f"seen:{exc}")

    # given an async handler that raises
    @bind
    async def handler(event, context, conn: str = Depends(provide_conn)) -> None:
        await asyncio.sleep(0)
        raise ValueError("async-boom")

    # when the invocation fails
    with pytest.raises(ValueError, match="async-boom"):
        run_handler(handler, {}, InvocationContext())

    # then the async generator observed the failure and the original
    # exception still propagates
    assert events == ["open", "seen:async-boom"]


def test_initialized_generator_sees_the_dependency_failure() -> None:
    # given an initialized resource followed by a provider that fails
    events: list[str] = []

    def provide_conn():
        events.append("open")
        try:
            yield "conn"
        except RuntimeError as exc:
            events.append(f"seen:{exc}")

    def failing() -> str:
        raise RuntimeError("init-boom")

    @bind
    def handler(
        event,
        context,
        conn=Depends(provide_conn),
        other=Depends(failing),
    ) -> None:
        events.append("handler")

    # when dependency resolution fails after the resource was created
    with pytest.raises(RuntimeError, match="init-boom"):
        run_handler(handler, {}, InvocationContext())

    # then the resource's except block saw the dependency failure, the
    # handler never ran, and the original error is what propagated
    assert events == ["open", "seen:init-boom"]


def test_suppressed_provider_error_does_not_swallow_the_original() -> None:
    # given a resource that deliberately swallows the failure thrown into it
    events: list[str] = []

    def provide_conn():
        try:
            yield "conn"
        except ValueError:
            events.append("suppressed")
            return

    # given a handler that raises
    @bind
    def handler(event, context, conn=Depends(provide_conn)) -> None:
        raise ValueError("handler-boom")

    # when the provider suppresses the handler failure during cleanup
    with pytest.raises(ValueError, match="handler-boom"):
        run_handler(handler, {}, InvocationContext())

    # then the suppression happened but the original failure still decides
    # the invocation outcome
    assert events == ["suppressed"]


def test_failing_async_cleanup_preserves_the_handler_exception() -> None:
    # given an async resource whose except block fails while unwinding
    async def provide_conn():
        try:
            yield "conn"
        except ValueError:
            raise RuntimeError("cleanup-boom")

    # given an async handler that raises
    @bind
    async def handler(event, context, conn=Depends(provide_conn)) -> None:
        raise ValueError("handler-boom")

    # when both the handler and the cleanup fail in one invocation
    # then the handler's original exception is what the caller sees
    with pytest.raises(ValueError, match="handler-boom"):
        run_handler(handler, {}, InvocationContext())


def test_cancellation_during_resolution_cleans_initialized_resources() -> None:
    # given an initialized resource and a provider that never settles
    events: list[str] = []
    slow_started = asyncio.Event()

    def provide_conn():
        events.append("open")
        try:
            yield "conn"
        finally:
            events.append("close")

    async def provide_slow():
        events.append("slow-start")
        slow_started.set()
        await asyncio.Event().wait()  # never completes
        return "never"

    @bind
    def handler(
        event,
        context,
        conn=Depends(provide_conn),
        slow=Depends(provide_slow),
    ) -> None:
        events.append("handler")

    async def scenario() -> None:
        # when the invocation is cancelled while a dependency is resolving
        task = asyncio.ensure_future(handler({}, InvocationContext()))
        await slow_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    # then the handler never ran, the cancellation propagated, and the
    # already-initialized resource still completed its cleanup
    assert events == ["open", "slow-start", "close"]


def test_cancellation_during_resource_initialization_closes_it() -> None:
    # given a resource cancelled before it produced its first yield
    events: list[str] = []
    resource_starting = asyncio.Event()

    async def provide_partial():
        events.append("partial-open")
        resource_starting.set()
        try:
            await asyncio.Event().wait()  # never completes
            yield "never"  # pragma: no cover - cancelled first
        finally:
            events.append("partial-close")

    @bind
    def handler(event, context, resource=Depends(provide_partial)) -> None:
        events.append("handler")

    async def scenario() -> None:
        # when the invocation is cancelled during the resource's own
        # initialization
        task = asyncio.ensure_future(handler({}, InvocationContext()))
        await resource_starting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    # then the half-initialized resource was closed and the handler never ran
    assert events == ["partial-open", "partial-close"]
