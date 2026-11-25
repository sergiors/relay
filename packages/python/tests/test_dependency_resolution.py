"""Resolution forms: default Depends, Annotated, nesting, event/context."""

import asyncio
from typing import Annotated

from relay_runtime import Depends, bind

from di_support import InvocationContext, run_handler


def test_public_api_exports_depends_and_bind() -> None:
    # given the installed relay_runtime package
    import relay_runtime

    # then both injection entry points are importable from the top level
    assert relay_runtime.Depends is Depends
    assert relay_runtime.bind is bind
    # and the pre-existing handler typing export is unchanged
    assert relay_runtime.HandlerContext is not None


def test_default_depends_form_injects_the_provider_value() -> None:
    # given a provider producing a value
    def provide_token() -> str:
        return "tok-1"

    # given a handler declaring the provider as a parameter default
    @bind
    def handler(event, context, token: str = Depends(provide_token)) -> str:
        return token

    # when the bound handler runs
    result = run_handler(handler, {"msg": "hi"}, InvocationContext())

    # then the provider's value reaches the handler
    assert result == "tok-1"


def test_annotated_depends_form_needs_no_parameter_default() -> None:
    # given a handler declaring the provider through Annotated only
    def provide_user() -> str:
        return "ada"

    @bind
    def handler(event, context, user: Annotated[str, Depends(provide_user)]) -> str:
        return user

    # when the bound handler runs
    result = run_handler(handler, {}, InvocationContext())

    # then the annotated dependency is injected despite the absent default
    assert result == "ada"


def test_nested_providers_resolve_their_own_dependencies() -> None:
    # given a provider that itself depends on another provider
    def provide_base() -> str:
        return "base"

    def provide_greeting(base: Annotated[str, Depends(provide_base)]) -> str:
        return f"hello {base}"

    # given a handler depending on the outer provider
    @bind
    def handler(event, context, greeting: str = Depends(provide_greeting)) -> str:
        return greeting

    # when the bound handler runs
    result = run_handler(handler, {}, InvocationContext())

    # then the whole chain resolves down to the leaf provider
    assert result == "hello base"


def test_providers_receive_the_event_and_context() -> None:
    # given providers reading the invocation's event and context
    def identify(context) -> str:
        return context.request_id

    def describe(event) -> str:
        return event["msg"]

    def provide_token(
        ident: str = Depends(identify),
        msg: str = Depends(describe),
    ) -> str:
        return f"{ident}:{msg}"

    @bind
    def handler(event, context, token: str = Depends(provide_token)) -> str:
        return token

    # when the bound handler runs with a distinctive event and context
    context = InvocationContext(request_id="req-42")
    result = run_handler(handler, {"msg": "hi"}, context)

    # then the nested provider saw the same invocation state
    assert result == "req-42:hi"


def test_handler_may_rely_on_plain_parameter_defaults() -> None:
    # given a handler whose extra parameter has a plain default
    @bind
    def handler(event, context, limit: int = 5) -> int:
        return limit

    # when the bound handler runs
    result = run_handler(handler, {}, InvocationContext())

    # then the default is passed through untouched
    assert result == 5


def test_async_provider_resolves_for_a_sync_handler() -> None:
    # given an async provider layered on a sync provider
    def provide_base() -> str:
        return "sync"

    async def provide_value(base: str = Depends(provide_base)) -> str:
        await asyncio.sleep(0)
        return base + "+async"

    @bind
    def handler(event, context, value: str = Depends(provide_value)) -> str:
        return value

    # when the bound handler is invoked and settled
    result = run_handler(handler, {}, InvocationContext())

    # then both provider forms resolve
    assert result == "sync+async"


def test_async_handler_receives_dependencies_and_returns_result() -> None:
    # given an async handler with an injected dependency
    def provide_value() -> str:
        return "dep"

    @bind
    async def handler(event, context, value: str = Depends(provide_value)) -> str:
        await asyncio.sleep(0)
        return f"{event['msg']}:{value}"

    # when the bound handler is invoked and awaited
    result = run_handler(handler, {"msg": "e"}, InvocationContext())

    # then the dependency is injected and the result survives the await
    assert result == "e:dep"


def test_legacy_event_context_handler_keeps_its_calling_convention() -> None:
    # given a pre-injection two-argument handler
    def handler(event, context):
        return {"echo": event, "id": context.request_id}

    # when it is bound (or left untouched)
    bound = bind(handler)

    # then both calling forms behave exactly as before
    context = InvocationContext(request_id="legacy")
    assert run_handler(bound, {"a": 1}, context) == {"echo": {"a": 1}, "id": "legacy"}
    assert handler({"a": 2}, context) == {"echo": {"a": 2}, "id": "legacy"}
