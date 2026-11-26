"""Context-scoped dependency overrides.

Overrides substitute providers at ``Depends(...)`` references without mutating
the compiled handler graph. The tests pin identity matching, nesting, inherited
scope/cache settings, container isolation, the sync/async lifecycle split,
replacement diagnostics, and cache behavior for falsey and failing providers.
"""

import asyncio
import functools
import inspect

import pytest

from relay_runtime import Depends, bind
from relay_runtime.dependencies import override_dependencies

from di_support import InvocationContext, run_handler


def test_override_is_exported_only_from_the_dependencies_package() -> None:
    # given the package and its dependencies module
    import relay_runtime
    from relay_runtime import dependencies

    # then the override entry point is public on the DI module only
    assert dependencies.override_dependencies is override_dependencies
    assert not hasattr(relay_runtime, "override_dependencies")


def test_override_replaces_a_provider_only_inside_the_context() -> None:
    # given an original and replacement provider
    def provide_original() -> str:
        return "original"

    def provide_replacement() -> str:
        return "replacement"

    @bind
    def handler(event, context, value: str = Depends(provide_original)) -> str:
        return value

    # then the original graph is used outside the context
    assert run_handler(handler, {}, InvocationContext()) == "original"

    # when the context overrides the original declaration
    with override_dependencies({provide_original: provide_replacement}):
        assert run_handler(handler, {}, InvocationContext()) == "replacement"

    # then the original graph is unchanged after the context exits
    assert run_handler(handler, {}, InvocationContext()) == "original"


def test_identity_replacement_is_a_no_op() -> None:
    # given a mapping that replaces a provider with itself
    def provide_value() -> str:
        return "value"

    @bind
    def handler(event, context, value: str = Depends(provide_value)) -> str:
        return value

    # when the identity mapping is active
    with override_dependencies({provide_value: provide_value}):
        # then the provider still runs as declared
        assert run_handler(handler, {}, InvocationContext()) == "value"


def test_nested_contexts_compose_and_snapshot_the_mapping() -> None:
    # given original, outer, and inner replacements
    def provide_original() -> str:
        return "original"

    def provide_outer() -> str:
        return "outer"

    def provide_inner() -> str:
        return "inner"

    def provide_changed() -> str:
        return "changed"

    @bind
    def handler(event, context, value: str = Depends(provide_original)) -> str:
        return value

    mapping = {provide_original: provide_outer}

    with override_dependencies(mapping):
        # then the entry snapshot is not affected by later mutation
        mapping[provide_original] = provide_changed
        assert run_handler(handler, {}, InvocationContext()) == "outer"

        # and an inner context takes precedence over the inherited override
        with override_dependencies({provide_original: provide_inner}):
            assert run_handler(handler, {}, InvocationContext()) == "inner"

        # and leaving the inner context restores the outer override
        assert run_handler(handler, {}, InvocationContext()) == "outer"

    # and leaving the outer context restores the original provider
    assert run_handler(handler, {}, InvocationContext()) == "original"


def test_replacement_subdeps_are_overridden_but_the_root_is_not() -> None:
    # given a replacement whose own dependency is another overridden provider
    def provide_leaf() -> str:
        return "leaf"

    def provide_leaf_replacement() -> str:
        return "leaf-replacement"

    def provide_middle(leaf: str = Depends(provide_leaf)) -> str:
        return f"middle:{leaf}"

    def provide_middle_replacement(leaf: str = Depends(provide_leaf)) -> str:
        return f"replacement:{leaf}"

    def provide_middle_final() -> str:
        return "final"

    @bind
    def handler(event, context, value: str = Depends(provide_middle)) -> str:
        return value

    # when both the middle and its leaf are overridden
    with override_dependencies(
        {
            provide_middle: provide_middle_replacement,
            provide_leaf: provide_leaf_replacement,
        }
    ):
        # then the replacement's own subdependency is substituted
        assert (
            run_handler(handler, {}, InvocationContext())
            == "replacement:leaf-replacement"
        )

    # when the replacement root is also a key
    with override_dependencies(
        {
            provide_middle: provide_middle_replacement,
            provide_middle_replacement: provide_middle_final,
        }
    ):
        # then the root is not replaced recursively
        assert (
            run_handler(handler, {}, InvocationContext())
            == "replacement:leaf"
        )

    # but a direct reference to the replacement is still an original reference
    @bind
    def direct(
        event,
        context,
        value: str = Depends(provide_middle_replacement),
    ):
        return value

    with override_dependencies(
        {provide_middle_replacement: provide_middle_final}
    ):
        assert run_handler(direct, {}, InvocationContext()) == "final"


def test_container_cache_keeps_distinct_original_identities_separate() -> None:
    # given two original providers and one shared replacement generator
    events: list[str] = []

    def provide_first() -> object:
        return object()

    def provide_second() -> object:
        return object()

    def provide_replacement():
        events.append("open")
        yield object()
        events.append("close")

    @bind
    def handler(
        event,
        context,
        first=Depends(provide_first, scope="container"),
        second=Depends(provide_second, scope="container"),
        first_again=Depends(provide_first, scope="container"),
    ):
        return first, second, first_again

    with override_dependencies(
        {
            provide_first: provide_replacement,
            provide_second: provide_replacement,
        }
    ):
        # when both original declarations resolve to the same replacement
        first, second, first_again = run_handler(
            handler, {}, InvocationContext()
        )

        # then each original declaration owns a separate container entry
        assert first is first_again
        assert first is not second
        assert events == ["open", "open"]

        # and a warm invocation reuses both entries without calling again
        again = run_handler(handler, {}, InvocationContext())
        assert again[0] is first
        assert again[1] is second
        assert events == ["open", "open"]

    # and exiting the context closes both owned resources exactly once
    assert events == ["open", "open", "close", "close"]


def test_different_originals_do_not_collide_in_the_invocation_cache() -> None:
    # given two originals mapped to the same invocation-scoped replacement
    def provide_first() -> object:
        return object()

    def provide_second() -> object:
        return object()

    def provide_replacement() -> object:
        return object()

    @bind
    def handler(
        event,
        context,
        first=Depends(provide_first),
        second=Depends(provide_second),
    ):
        return first is second

    with override_dependencies(
        {
            provide_first: provide_replacement,
            provide_second: provide_replacement,
        }
    ):
        # then the two declaration sites receive separate values
        assert run_handler(handler, {}, InvocationContext()) is False


def test_use_cache_false_is_inherited_by_the_replacement() -> None:
    # given an uncached original declaration
    calls: list[str] = []

    def provide_original() -> object:
        return object()

    def provide_replacement() -> object:
        calls.append("call")
        return object()

    @bind
    def handler(
        event,
        context,
        first=Depends(provide_original, use_cache=False),
        second=Depends(provide_original, use_cache=False),
    ):
        return first, second

    with override_dependencies({provide_original: provide_replacement}):
        first, second = run_handler(handler, {}, InvocationContext())

        # then the inherited uncached setting gives each site a fresh value
        assert first is not second
        assert calls == ["call", "call"]


def test_container_original_rejects_invocation_scoped_replacement() -> None:
    # given a replacement with an invocation-scoped dependency
    def provide_leaf() -> str:
        return "leaf"

    def provide_original() -> str:
        return "original"

    def provide_replacement(leaf: str = Depends(provide_leaf)) -> str:
        return leaf

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    with override_dependencies({provide_original: provide_replacement}):
        # when the inherited container scope is applied to the replacement
        # then the invalid scope edge is rejected
        with pytest.raises(RuntimeError, match="invocation-scoped"):
            run_handler(handler, {}, InvocationContext())


def test_replacement_cycle_is_rejected() -> None:
    # given a replacement that depends on the original it replaces
    def provide_original() -> str:
        return "original"

    def provide_replacement(value: str = Depends(provide_original)) -> str:
        return value

    @bind
    def handler(event, context, value: str = Depends(provide_original)) -> str:
        return value

    with override_dependencies({provide_original: provide_replacement}):
        # when the effective graph is compiled
        # then the substitution cycle is reported
        with pytest.raises(RuntimeError, match="cycle"):
            run_handler(handler, {}, InvocationContext())


def test_sync_context_closes_sync_container_generators_without_a_loop() -> None:
    # given a sync generator replacement used in container scope
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    def provide_replacement():
        events.append("open")
        yield "scoped"
        events.append("close")

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    with override_dependencies({provide_original: provide_replacement}):
        # when the handler runs and the sync context exits outside the loop
        assert run_handler(handler, {}, InvocationContext()) == "scoped"
        assert events == ["open"]

    # then the sync generator was closed without starting a new event loop
    assert events == ["open", "close"]


def test_sync_context_rejects_async_container_generators() -> None:
    # given an async generator replacement used in container scope
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    async def provide_replacement():
        events.append("open")
        yield "scoped"
        events.append("close")

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    with override_dependencies({provide_original: provide_replacement}):
        # when a synchronous context tries to acquire it
        with pytest.raises(RuntimeError, match="async"):
            run_handler(handler, {}, InvocationContext())

    # then the async generator never started
    assert events == []


def test_async_context_closes_sync_and_async_container_resources() -> None:
    # given one sync and one async container generator replacement
    events: list[str] = []

    def provide_sync_original() -> str:
        return "sync-original"

    def provide_async_original() -> str:
        return "async-original"

    def provide_sync_replacement():
        events.append("sync-open")
        yield "sync"
        events.append("sync-close")

    async def provide_async_replacement():
        events.append("async-open")
        yield "async"
        events.append("async-close")

    @bind
    async def handler(
        event,
        context,
        sync_value=Depends(provide_sync_original, scope="container"),
        async_value=Depends(provide_async_original, scope="container"),
    ):
        return sync_value, async_value

    async def scenario() -> None:
        async with override_dependencies(
            {
                provide_sync_original: provide_sync_replacement,
                provide_async_original: provide_async_replacement,
            }
        ):
            # when both resources are acquired in one async context
            assert await handler({}, InvocationContext()) == ("sync", "async")
            assert events == ["sync-open", "async-open"]

        # then both close in reverse creation order on async exit
        assert events == [
            "sync-open",
            "async-open",
            "async-close",
            "sync-close",
        ]

    asyncio.run(scenario())


def test_invocation_cleanup_runs_inside_a_sync_override_context() -> None:
    # given an async generator replacement at invocation scope
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    async def provide_replacement():
        events.append("open")
        yield "scoped"
        events.append("close")

    @bind
    async def handler(event, context, value=Depends(provide_original)) -> str:
        events.append("handler")
        return value

    with override_dependencies({provide_original: provide_replacement}):
        # when the bound async handler runs under a sync override context
        assert run_handler(handler, {}, InvocationContext()) == "scoped"

    # then invocation cleanup still ran inside the handler call
    assert events == ["open", "handler", "close"]


def test_override_container_is_isolated_from_the_global_container() -> None:
    # given a global container resource
    events: list[str] = []

    def provide_shared():
        events.append("global-open")
        yield "global"
        events.append("global-close")

    def provide_scoped():
        events.append("scoped-open")
        yield "scoped"
        events.append("scoped-close")

    @bind
    def handler(
        event, context, value: str = Depends(provide_shared, scope="container")
    ) -> str:
        return value

    # when the global container has already created its value
    assert run_handler(handler, {}, InvocationContext()) == "global"

    with override_dependencies({provide_shared: provide_scoped}):
        # then the context owns a separate container value
        assert run_handler(handler, {}, InvocationContext()) == "scoped"
        assert events == ["global-open", "scoped-open"]

    # and the context's resource closed without touching the global one
    assert events == ["global-open", "scoped-open", "scoped-close"]
    assert run_handler(handler, {}, InvocationContext()) == "global"
    assert events == ["global-open", "scoped-open", "scoped-close"]


def test_nested_contexts_own_separate_container_caches() -> None:
    # given outer and inner container generator replacements
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    def provide_outer():
        events.append("outer-open")
        yield "outer"
        events.append("outer-close")

    def provide_inner():
        events.append("inner-open")
        yield "inner"
        events.append("inner-close")

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    with override_dependencies({provide_original: provide_outer}):
        assert run_handler(handler, {}, InvocationContext()) == "outer"

        with override_dependencies({provide_original: provide_inner}):
            assert run_handler(handler, {}, InvocationContext()) == "inner"

        # then only the inner resource closed when its context exited
        assert events == ["outer-open", "inner-open", "inner-close"]

        # and the outer cache survived the inner context
        assert run_handler(handler, {}, InvocationContext()) == "outer"
        assert events == ["outer-open", "inner-open", "inner-close"]

    # and the outer resource closed when the outer context exited
    assert events == [
        "outer-open",
        "inner-open",
        "inner-close",
        "outer-close",
    ]


def test_concurrent_tasks_keep_independent_override_contexts() -> None:
    # given two replacements selected by two concurrent tasks
    def provide_original() -> str:
        return "original"

    def provide_first() -> str:
        return "first"

    def provide_second() -> str:
        return "second"

    @bind
    async def handler(event, context, value: str = Depends(provide_original)):
        return value

    async def scenario() -> list[str]:
        async def run_with(replacement) -> str:
            async with override_dependencies({provide_original: replacement}):
                await asyncio.sleep(0)
                return await handler({}, InvocationContext())

        return await asyncio.gather(
            run_with(provide_first),
            run_with(provide_second),
        )

    # then contextvars keep each task's override separate
    assert asyncio.run(scenario()) == ["first", "second"]


def test_child_task_created_in_a_context_fails_after_it_exits() -> None:
    # given a bound handler with an overridden provider
    def provide_original() -> str:
        return "original"

    def provide_replacement() -> str:
        return "replacement"

    @bind
    async def handler(event, context, value: str = Depends(provide_original)):
        return value

    async def scenario() -> None:
        context = override_dependencies(
            {provide_original: provide_replacement}
        )

        with context:
            # when a child task is created but has not started yet
            task = asyncio.ensure_future(handler({}, InvocationContext()))

        # then resolving after the owner's context exited is rejected
        with pytest.raises(RuntimeError, match="exited"):
            await task

    asyncio.run(scenario())


def test_container_resolution_started_before_exit_fails_and_closes() -> None:
    # given a container provider that is still running when the context exits
    events: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    def provide_original() -> str:
        return "original"

    async def provide_slow():
        events.append("open")
        started.set()
        await release.wait()
        yield "value"
        events.append("close")

    @bind
    async def handler(
        event, context, value=Depends(provide_original, scope="container")
    ):
        return value

    async def scenario() -> None:
        context = override_dependencies({provide_original: provide_slow})

        async with context:
            task = asyncio.ensure_future(handler({}, InvocationContext()))
            await started.wait()

        # when the provider completes after the owner context exited
        release.set()

        # then the resolution is rejected and the produced resource closed
        with pytest.raises(RuntimeError, match="exited"):
            await task

    asyncio.run(scenario())
    assert events == ["open", "close"]


def test_child_task_cannot_enter_a_new_context_after_parent_exit() -> None:
    # given a parent context whose child task starts after exit
    async def scenario() -> None:
        parent = override_dependencies({})

        with parent:
            task = asyncio.ensure_future(child())

        with pytest.raises(RuntimeError, match="exited"):
            await task

    async def child() -> None:
        # when the child tries to enter a fresh context
        with override_dependencies({}):
            pass

    asyncio.run(scenario())


def test_sync_cleanup_failure_still_restores_the_context() -> None:
    # given a sync generator replacement whose cleanup fails
    def provide_original() -> str:
        return "original"

    def provide_bad():
        yield "scoped"
        raise RuntimeError("cleanup-boom")

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    # when the context exits with a cleanup failure
    with pytest.raises(RuntimeError, match="cleanup-boom"):
        with override_dependencies({provide_original: provide_bad}):
            assert run_handler(handler, {}, InvocationContext()) == "scoped"

    # then the contextvar was restored and the original provider works again
    assert run_handler(handler, {}, InvocationContext()) == "original"


def test_cleanup_failure_does_not_replace_an_active_exception() -> None:
    # given a container replacement whose cleanup fails
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    def provide_bad():
        yield "scoped"
        events.append("closed")
        raise RuntimeError("cleanup-boom")

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    # when the body raises after acquiring the resource
    with pytest.raises(ValueError, match="body-boom"):
        with override_dependencies({provide_original: provide_bad}):
            assert run_handler(handler, {}, InvocationContext()) == "scoped"
            raise ValueError("body-boom")

    # then cleanup ran, the body error survived, and the token was restored
    assert events == ["closed"]
    assert run_handler(handler, {}, InvocationContext()) == "original"


def test_async_cleanup_failure_still_restores_the_context() -> None:
    # given an async generator replacement whose cleanup fails
    def provide_original() -> str:
        return "original"

    async def provide_bad():
        yield "scoped"
        raise RuntimeError("async-cleanup-boom")

    @bind
    async def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="async-cleanup-boom"):
            async with override_dependencies({provide_original: provide_bad}):
                assert await handler({}, InvocationContext()) == "scoped"

        # then a later context can use the original provider again
        async with override_dependencies({}):
            assert await handler({}, InvocationContext()) == "original"

    asyncio.run(scenario())


def test_mapping_is_validated_when_the_context_is_entered() -> None:
    # given mappings with non-callable keys or values
    def provide_original() -> str:
        return "original"

    bad_value = override_dependencies(
        {provide_original: 42}  # type: ignore[dict-item]
    )
    bad_key = override_dependencies(
        {42: provide_original}  # type: ignore[dict-item]
    )
    not_a_mapping = override_dependencies(42)  # type: ignore[arg-type]

    # then validation happens at context entry, not at construction
    with pytest.raises(TypeError, match="values"):
        with bad_value:
            pass

    with pytest.raises(TypeError, match="keys"):
        with bad_key:
            pass

    with pytest.raises(TypeError, match="mapping"):
        with not_a_mapping:
            pass


def test_override_matching_uses_identity_not_equality() -> None:
    # given callable providers that compare equal to everything
    class Provider:
        def __init__(self, value: str) -> None:
            self.value = value

        def __call__(self) -> str:
            return self.value

        def __eq__(self, other: object) -> bool:
            return True

        def __hash__(self) -> int:
            return 1

    original = Provider("original")
    distinct_but_equal = Provider("distinct")
    replacement = Provider("replacement")

    @bind
    def handler(event, context, value: str = Depends(original)) -> str:
        return value

    # when a different but equality-matching callable is the key
    with override_dependencies({distinct_but_equal: replacement}):
        assert run_handler(handler, {}, InvocationContext()) == "original"

    # then only the identical original object is replaced
    with override_dependencies({original: replacement}):
        assert run_handler(handler, {}, InvocationContext()) == "replacement"


def test_override_works_through_a_wraps_decorator() -> None:
    # given a functools.wraps decorator around the bound handler
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    def provide_replacement() -> str:
        return "replacement"

    def track(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            events.append("before")
            try:
                return await func(*args, **kwargs)
            finally:
                events.append("after")

        return wrapper

    @bind
    @track
    async def handler(event, context, value: str = Depends(provide_original)):
        """Handle a value."""
        events.append("handler")
        return value

    # when the decorated handler runs under an override
    with override_dependencies({provide_original: provide_replacement}):
        assert run_handler(handler, {}, InvocationContext()) == "replacement"

    # then the bound execution and decorator bookkeeping both ran
    assert events == ["before", "handler", "after"]

    # and the handler metadata and published signature are preserved
    assert handler.__name__ == "handler"
    assert handler.__doc__ == "Handle a value."
    assert list(inspect.signature(handler).parameters) == ["event", "context"]


def test_sync_context_rejects_indirectly_returned_async_generators() -> None:
    # given provider factories that build an async generator resource
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    async def build_resource():
        events.append("open")
        yield "scoped"
        events.append("close")

    def sync_factory() -> object:
        return build_resource()

    async def async_factory() -> object:
        return build_resource()

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    # when a sync context tries either factory shape
    for factory in (sync_factory, async_factory):
        with override_dependencies({provide_original: factory}):
            # then the indirectly returned async generator is rejected
            with pytest.raises(RuntimeError, match="async with"):
                run_handler(handler, {}, InvocationContext())

    # and no generator body ever started
    assert events == []


def test_async_context_starts_indirectly_returned_async_generators() -> None:
    # given provider factories that build an async generator resource
    events: list[str] = []

    def provide_original() -> str:
        return "original"

    async def build_resource():
        events.append("open")
        yield "scoped"
        events.append("close")

    def sync_factory() -> object:
        return build_resource()

    async def async_factory() -> object:
        return build_resource()

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    async def scenario() -> None:
        # when either factory shape runs under async with
        for factory in (sync_factory, async_factory):
            events.clear()
            async with override_dependencies({provide_original: factory}):
                # then the resource is acquired inside the context
                assert await handler({}, InvocationContext()) == "scoped"
                assert events == ["open"]
            # and it is closed when the async context exits
            assert events == ["open", "close"]

    asyncio.run(scenario())


def test_replacement_compile_failure_names_original_replacement_and_path() -> None:
    # given a broken replacement reached only through its parent override
    def bind_leaf() -> str:
        return "leaf"

    def glue_invalid(required) -> str:
        return required

    def wrap_top(leaf: str = Depends(bind_leaf)) -> str:
        return f"top:{leaf}"

    def wrap_top_fake(leaf: str = Depends(bind_leaf)) -> str:
        return f"fake:{leaf}"

    @bind
    def chain_handler(event, context, value: str = Depends(wrap_top)) -> str:
        return value

    with override_dependencies(
        {wrap_top: wrap_top_fake, bind_leaf: glue_invalid}
    ):
        # when the effective graph is compiled
        with pytest.raises(RuntimeError) as excinfo:
            run_handler(chain_handler, {}, InvocationContext())

    # then the diagnostic names the original, the replacement, and the path
    message = str(excinfo.value)
    assert "bind_leaf" in message
    assert "glue_invalid" in message
    assert "wrap_top" in message
    assert " -> " in message
    # and the chain runs from the handler's overridden edge down to the
    # edge whose replacement failed to compile
    assert message.index("wrap_top_fake") < message.index(" -> ")
    assert message.index(" -> ") < message.rindex("glue_invalid")


def test_container_cache_keys_factory_generated_providers_by_identity() -> None:
    # given two provider objects generated from one factory target
    calls: list[str] = []

    def factory_target() -> object:
        calls.append("call")
        return object()

    primary = functools.partial(factory_target)
    secondary = functools.partial(factory_target)

    @bind
    def handler(
        event,
        context,
        first=Depends(primary, scope="container"),
        second=Depends(secondary, scope="container"),
        first_again=Depends(primary, scope="container"),
    ):
        return first, second, first_again

    with override_dependencies({}):
        # when the generated providers resolve inside the context
        first, second, first_again = run_handler(
            handler, {}, InvocationContext()
        )

        # then identity keeps the two generated providers in separate
        # entries even though they wrap the same target
        assert first is first_again
        assert first is not second
        assert calls == ["call", "call"]

        # and a warm invocation reuses both cached entries
        again = run_handler(handler, {}, InvocationContext())
        assert again[0] is first
        assert again[1] is second
        assert calls == ["call", "call"]


def test_override_matches_a_cached_factory_result_by_identity() -> None:
    # given a memoized factory that hands out one provider per table name
    calls: list[str] = []

    @functools.cache
    def get_table(name: str):
        def provide_table() -> object:
            calls.append(name)
            return object()

        return provide_table

    replacement_value = object()

    def provide_fake_table() -> object:
        return replacement_value

    @bind
    def handler(
        event,
        context,
        table=Depends(get_table("users"), scope="container"),
    ) -> object:
        return table

    # and the factory result is stable enough to serve as an identity key
    assert get_table("users") is get_table("users")

    # then the factory-produced original answers outside the context
    assert run_handler(handler, {}, InvocationContext()) is not replacement_value
    assert calls == ["users"]

    # when the same cached factory result is used as the override key
    with override_dependencies({get_table("users"): provide_fake_table}):
        # then the identity-matched original is substituted by the replacement
        assert run_handler(handler, {}, InvocationContext()) is replacement_value
        # and the original factory-produced provider is never called
        assert calls == ["users"]


def test_transitive_override_bypasses_an_already_cached_container_dependency() -> None:
    # given a container graph whose leaf is later replaced transitively
    leaf_calls: list[str] = []
    middle_calls: list[str] = []

    def provide_leaf() -> str:
        leaf_calls.append("original")
        return "leaf"

    def provide_fake_leaf() -> str:
        leaf_calls.append("fake")
        return "fake-leaf"

    def provide_middle(leaf: str = Depends(provide_leaf, scope="container")) -> str:
        middle_calls.append("call")
        return f"middle:{leaf}"

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_middle, scope="container"),
    ) -> str:
        return value

    # when the normal container has already cached both providers
    assert run_handler(handler, {}, InvocationContext()) == "middle:leaf"
    assert leaf_calls == ["original"]
    assert middle_calls == ["call"]

    with override_dependencies({provide_leaf: provide_fake_leaf}):
        # then the transitive override resolves the fake leaf even though
        # the normal container already holds the original
        assert run_handler(handler, {}, InvocationContext()) == "middle:fake-leaf"
        assert leaf_calls == ["original", "fake"]
        # and the un-replaced middle was rebuilt in the context's own cache
        assert middle_calls == ["call", "call"]

    # and leaving the context returns to the untouched normal cache
    assert run_handler(handler, {}, InvocationContext()) == "middle:leaf"
    assert leaf_calls == ["original", "fake"]
    assert middle_calls == ["call", "call"]


def test_replaced_provider_subdependencies_are_never_resolved() -> None:
    # given an original provider with its own subdependency
    leaf_calls: list[str] = []

    def provide_leaf() -> str:
        leaf_calls.append("call")
        return "leaf"

    def provide_original(leaf: str = Depends(provide_leaf)) -> str:
        return f"original:{leaf}"

    def provide_replacement() -> str:
        return "replacement"

    @bind
    def handler(event, context, value: str = Depends(provide_original)) -> str:
        return value

    # when the original runs without an override it resolves its leaf
    assert run_handler(handler, {}, InvocationContext()) == "original:leaf"
    assert leaf_calls == ["call"]

    with override_dependencies({provide_original: provide_replacement}):
        # then the replacement answers without ever resolving the
        # original provider's own subdependency
        assert run_handler(handler, {}, InvocationContext()) == "replacement"
        assert leaf_calls == ["call"]


@pytest.mark.parametrize("value", [None, 0, "", False, [], {}])
def test_falsey_container_values_are_cached_per_context(value) -> None:
    # given a replacement whose result is falsey
    calls: list[str] = []

    def provide_original() -> str:
        return "original"

    def provide_falsey() -> object:
        calls.append("call")
        return value

    @bind
    def handler(
        event,
        context,
        cached: object = Depends(provide_original, scope="container"),
    ) -> object:
        return cached

    with override_dependencies({provide_original: provide_falsey}):
        # then the falsey value is cached, never read as a cache miss
        assert run_handler(handler, {}, InvocationContext()) is value
        assert run_handler(handler, {}, InvocationContext()) is value
        assert calls == ["call"]

    # and a fresh sibling context starts from an empty container cache
    with override_dependencies({provide_original: provide_falsey}):
        assert run_handler(handler, {}, InvocationContext()) is value
        assert calls == ["call", "call"]


def test_failed_container_replacement_is_not_cached_in_the_context() -> None:
    # given a replacement that fails on its first execution
    calls: list[str] = []

    def provide_original() -> str:
        return "original"

    def provide_flaky() -> str:
        calls.append("call")
        if len(calls) == 1:
            raise RuntimeError("replacement-boom")
        return "ready"

    @bind
    def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    with override_dependencies({provide_original: provide_flaky}):
        # then the failure surfaces with the provider's original error
        # and is not cached in the context container
        with pytest.raises(RuntimeError, match="replacement-boom"):
            run_handler(handler, {}, InvocationContext())

        # and a retry executes the replacement again instead of replaying
        # the failure, then serves the successful value from the cache
        assert run_handler(handler, {}, InvocationContext()) == "ready"
        assert run_handler(handler, {}, InvocationContext()) == "ready"
        assert calls == ["call", "call"]


def test_cancelled_container_replacement_is_not_cached_in_the_context() -> None:
    # given a replacement cancelled on its first execution
    calls: list[str] = []

    def provide_original() -> str:
        return "original"

    async def provide_flaky() -> str:
        calls.append("call")
        if len(calls) == 1:
            raise asyncio.CancelledError
        return "ready"

    @bind
    async def handler(
        event,
        context,
        value: str = Depends(provide_original, scope="container"),
    ) -> str:
        return value

    async def scenario() -> None:
        async with override_dependencies({provide_original: provide_flaky}):
            # then the cancellation propagates instead of becoming a result
            with pytest.raises(asyncio.CancelledError):
                await handler({}, InvocationContext())

            # and the next resolution runs the replacement again, after
            # which the value is cached for the rest of the context
            assert await handler({}, InvocationContext()) == "ready"
            assert await handler({}, InvocationContext()) == "ready"

    asyncio.run(scenario())
    assert calls == ["call", "call"]
