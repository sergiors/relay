"""Scope and cache semantics: invocation cache, container cache, use_cache."""

import asyncio

import pytest

from relay_runtime import Depends, bind

from di_support import InvocationContext, run_handler


def test_dependencies_are_cached_within_a_single_invocation() -> None:
    # given a provider recorded per execution
    calls: list[str] = []

    def provide() -> object:
        calls.append("call")
        return object()

    # given a handler requesting the same provider twice
    @bind
    def handler(event, context, a=Depends(provide), b=Depends(provide)) -> bool:
        return a is b

    # when one invocation runs
    same_within = run_handler(handler, {}, InvocationContext())

    # then both sites share one instance created once
    assert same_within is True
    assert calls == ["call"]

    # and a later invocation starts from a fresh instance
    run_handler(handler, {}, InvocationContext())
    assert calls == ["call", "call"]


def test_explicit_invocation_scope_matches_the_default() -> None:
    # given the default and the explicit invocation scope side by side
    calls: list[str] = []

    def provide() -> object:
        calls.append("call")
        return object()

    @bind
    def handler(
        event,
        context,
        implicit=Depends(provide),
        explicit=Depends(provide, scope="invocation"),
    ) -> bool:
        return implicit is explicit

    # when one invocation runs
    same_instance = run_handler(handler, {}, InvocationContext())

    # then the explicit invocation scope hits the default cache
    assert same_instance is True
    assert calls == ["call"]


def test_shared_provider_in_a_nested_graph_resolves_once() -> None:
    # given a leaf provider used both directly and through a dependent provider
    calls: list[str] = []

    def provide_inner() -> object:
        calls.append("call")
        return object()

    def provide_outer(inner=Depends(provide_inner)) -> object:
        return inner

    @bind
    def handler(
        event,
        context,
        outer=Depends(provide_outer),
        inner=Depends(provide_inner),
    ) -> bool:
        return outer is inner

    # when one invocation runs
    shared = run_handler(handler, {}, InvocationContext())

    # then the nested graph and the direct site see the same cached instance
    assert shared is True
    assert calls == ["call"]


def test_container_scope_is_shared_across_invocations_and_handlers() -> None:
    # given a container-scoped provider counted per execution
    calls: list[str] = []

    def provide_shared() -> object:
        calls.append("call")
        return object()

    # given two separately bound handlers using it
    @bind
    def first(event, context, dep=Depends(provide_shared, scope="container")):
        return dep

    @bind
    def second(event, context, dep=Depends(provide_shared, scope="container")):
        return dep

    # when both handlers run across warm invocations
    a1 = run_handler(first, {}, InvocationContext())
    a2 = run_handler(first, {}, InvocationContext())
    b = run_handler(second, {}, InvocationContext())

    # then one container-wide instance serves them all
    assert a1 is a2
    assert a1 is b
    assert calls == ["call"]


def test_cache_identity_includes_the_scope() -> None:
    # given one provider requested at both scopes
    calls: list[str] = []

    def provide() -> object:
        calls.append("call")
        return object()

    @bind
    def handler(
        event,
        context,
        per_invocation=Depends(provide),
        per_container=Depends(provide, scope="container"),
    ):
        return per_invocation, per_container

    # when a first invocation runs
    context = InvocationContext()
    local1, shared1 = run_handler(handler, {}, context)
    # then each scope executed the provider into its own cache entry
    assert local1 is not shared1
    assert calls == ["call", "call"]

    # when a second invocation runs
    local2, shared2 = run_handler(handler, {}, context)
    # then only the invocation-scoped instance is refreshed
    assert local2 is not local1
    assert shared2 is shared1
    assert calls == ["call", "call", "call"]


def test_use_cache_false_bypasses_the_invocation_cache() -> None:
    # given cached and uncached requests for the same provider
    calls: list[str] = []

    def provide() -> object:
        calls.append("call")
        return object()

    @bind
    def handler(
        event,
        context,
        cached=Depends(provide),
        cached_again=Depends(provide),
        fresh=Depends(provide, use_cache=False),
        fresh_again=Depends(provide, use_cache=False),
    ):
        return cached, cached_again, fresh, fresh_again

    # when one invocation runs
    cached, cached_again, fresh, fresh_again = run_handler(
        handler, {}, InvocationContext()
    )

    # then cached sites share one instance
    assert cached is cached_again
    # and uncached sites neither read nor write that instance
    assert fresh is not cached
    assert fresh is not fresh_again
    # one cached resolution plus one resolution per uncached site
    assert calls == ["call", "call", "call"]


def test_failed_initialization_is_not_cached() -> None:
    # given a provider that fails its first execution
    calls: list[str] = []

    def flaky() -> str:
        calls.append("call")
        if len(calls) == 1:
            raise RuntimeError("first call fails")
        return "ready"

    @bind
    def handler(event, context, value: str = Depends(flaky)) -> str:
        return value

    context = InvocationContext()

    # then the invocation fails with the provider's original error
    with pytest.raises(RuntimeError, match="first call fails"):
        run_handler(handler, {}, context)

    # and a retry executes the provider again instead of replaying the failure
    assert run_handler(handler, {}, context) == "ready"
    assert len(calls) == 2


def test_cancelled_initialization_is_not_cached() -> None:
    # given a provider cancelled on its first execution
    calls: list[str] = []

    async def flaky() -> str:
        calls.append("call")
        if len(calls) == 1:
            raise asyncio.CancelledError
        return "ready"

    @bind
    def handler(event, context, value: str = Depends(flaky)) -> str:
        return value

    context = InvocationContext()

    # then the cancellation propagates instead of becoming a result
    with pytest.raises(asyncio.CancelledError):
        run_handler(handler, {}, context)

    # and the next invocation resolves the provider again
    assert run_handler(handler, {}, context) == "ready"
    assert len(calls) == 2


@pytest.mark.parametrize("value", [None, 0, "", False, [], {}])
def test_falsey_provider_results_are_cached_within_the_invocation(value) -> None:
    # given a provider whose result is falsey
    calls: list[str] = []

    def provide() -> object:
        calls.append("call")
        return value

    # given two cached sites requesting it
    @bind
    def handler(event, context, a=Depends(provide), b=Depends(provide)):
        return a, b

    context = InvocationContext()

    # when one invocation runs
    first = run_handler(handler, {}, context)

    # then the falsey result is produced once and served to both sites -
    # a falsey value must never read as a cache miss
    assert calls == ["call"]
    assert first == (value, value)

    # and the next invocation re-resolves as any other invocation-scoped value
    second = run_handler(handler, {}, context)
    assert second == (value, value)
    assert len(calls) == 2


def test_mixed_cached_and_uncached_sites_stay_independent() -> None:
    # given one provider requested through cached and uncached declarations,
    # including from inside dependent providers
    calls: list[str] = []

    def provide() -> object:
        calls.append("call")
        return object()

    def provide_via_a(value=Depends(provide, use_cache=False)) -> object:
        return value

    def provide_via_b(value=Depends(provide, use_cache=False)) -> object:
        return value

    @bind
    def handler(
        event,
        context,
        shared=Depends(provide),
        shared_again=Depends(provide),
        via_a=Depends(provide_via_a),
        via_b=Depends(provide_via_b),
    ):
        return shared, shared_again, via_a, via_b

    context = InvocationContext()

    # when one invocation runs
    shared, shared_again, via_a, via_b = run_handler(handler, {}, context)

    # then the cached pair shares one instance despite the uncached
    # resolutions happening in the same invocation
    assert shared is shared_again
    # and every uncached path got its own instance without touching the cache
    assert via_a is not via_b
    assert via_a is not shared
    assert via_b is not shared
    assert calls == ["call", "call", "call"]

    # and a later invocation refreshes the cached pair while the uncached
    # paths stay fresh as always
    shared2, shared_again2, via_a2, via_b2 = run_handler(handler, {}, context)
    assert shared2 is shared_again2
    assert shared2 is not shared
    assert via_a2 is not via_a
    assert via_b2 is not via_b
    assert len(calls) == 6


def test_awaited_container_value_is_rejected_on_another_event_loop() -> None:
    # given a container-scoped provider whose value arrives through an await
    calls: list[str] = []

    async def provide_shared() -> str:
        calls.append("call")
        return "shared"

    @bind
    async def handler(
        event, context, value: str = Depends(provide_shared, scope="container")
    ) -> str:
        return value

    # when an invocation settles the provider on its own event loop
    assert run_handler(handler, {}, InvocationContext()) == "shared"
    assert calls == ["call"]

    # then a later invocation on a fresh event loop is rejected with the
    # documented cross-loop error instead of sharing the loop-bound value
    with pytest.raises(
        RuntimeError,
        match="provide_shared' was created on a different event loop",
    ):
        run_handler(handler, {}, InvocationContext())

    # and the rejected request neither re-ran nor replaced the cached value
    assert calls == ["call"]


def test_container_shutdown_releases_the_cached_value() -> None:
    # given a container-scoped provider cached across warm invocations
    calls: list[str] = []

    def provide_shared() -> object:
        calls.append("call")
        return object()

    @bind
    def handler(
        event, context, value=Depends(provide_shared, scope="container")
    ) -> object:
        return value

    first = run_handler(handler, {}, InvocationContext())
    second = run_handler(handler, {}, InvocationContext())
    # then one creation served both warm invocations
    assert second is first
    assert calls == ["call"]

    # when the warm container shuts down through the same seam the bootstrap
    # drives when its event loop exits
    from relay_runtime.dependencies import _shutdown_container_dependencies

    asyncio.run(_shutdown_container_dependencies())

    # then the released cache no longer replays the value: the next creation
    # runs the provider again on the live loop
    third = run_handler(handler, {}, InvocationContext())
    assert third is not first
    assert calls == ["call", "call"]
