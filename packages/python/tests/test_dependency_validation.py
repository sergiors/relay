"""Eager validation of the dependency graph at declaration and bind time.

Validation failures surface as RuntimeError subclasses (the public API exports
no dedicated error type); a bad Depends() argument fails as TypeError. The
assertions pin the failure itself plus the identifiers the error must carry
(handler, provider, parameter, dependency path) - never its exact wording.
Scope-configuration declarations are wrapped together with bind() because the
declaration may fail fast either when Depends() is built or when the handler
is bound.
"""

from typing import Annotated

import pytest

from relay_runtime import Depends, bind

from di_support import InvocationContext, run_handler

BIND_FAILURE = RuntimeError


def test_required_parameter_without_provider_is_rejected() -> None:
    # given a handler with a required parameter nothing can satisfy
    def settle_invoice(event, context, token: str) -> str:
        return token

    # when the handler is bound
    # then binding fails fast, naming the handler and the parameter
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(settle_invoice)
    message = str(excinfo.value)
    assert "settle_invoice" in message
    assert "token" in message


def test_provider_with_unsatisfiable_parameter_is_rejected() -> None:
    # given a provider with a required parameter of its own
    def provide_token(secret: str) -> str:
        return secret

    def invoice_handler(event, context, token: str = Depends(provide_token)) -> str:
        return token

    # when the handler is bound
    # then binding fails fast, naming the provider and the parameter
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(invoice_handler)
    message = str(excinfo.value)
    assert "provide_token" in message
    assert "secret" in message


def test_non_callable_provider_is_rejected() -> None:
    # when a dependency marker wraps a non-callable and the handler is bound
    with pytest.raises((ValueError, TypeError)) as excinfo:

        def literal_handler(event, context, value=Depends(42)):
            return value

        bind(literal_handler)

    # then the invalid provider signature is rejected before any invocation
    assert "42" in str(excinfo.value)


def test_circular_dependency_is_rejected() -> None:
    # given two providers that depend on each other
    def provider_a(seed=None) -> str:
        return "a"

    def provider_b(a: str = Depends(provider_a)) -> str:
        return "b"

    # close the cycle on provider_a once both functions exist
    provider_a.__defaults__ = (Depends(provider_b),)

    def cycle_handler(event, context, value: str = Depends(provider_a)) -> str:
        return value

    # when the handler is bound
    # then the cycle is rejected, naming both providers and the path
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(cycle_handler)
    message = str(excinfo.value)
    assert "provider_a" in message
    assert "provider_b" in message
    assert " -> " in message


def test_conflicting_dependency_declarations_are_rejected() -> None:
    # given one parameter declaring two different providers
    def provide_one() -> str:
        return "1"

    def provide_two() -> str:
        return "2"

    def conflict_handler(
        event,
        context,
        token: Annotated[str, Depends(provide_one)] = Depends(provide_two),
    ) -> str:
        return token

    # when the handler is bound
    # then the conflict is rejected, naming the handler and the parameter
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(conflict_handler)
    message = str(excinfo.value)
    assert "conflict_handler" in message
    assert "token" in message


def test_unknown_scope_name_is_rejected() -> None:
    # when a dependency requests a scope that does not exist and the handler
    # is bound
    with pytest.raises(BIND_FAILURE) as excinfo:

        def provide_value() -> str:
            return "v"

        def wide_handler(
            event, context, value=Depends(provide_value, scope="app")
        ) -> str:
            return value

        bind(wide_handler)

    # then only the two supported scope names are accepted
    assert "'app'" in str(excinfo.value)


def test_container_scope_with_cache_disabled_is_rejected() -> None:
    # when a container-scoped dependency refuses caching and the handler is
    # bound
    with pytest.raises(BIND_FAILURE) as excinfo:

        def provide_value() -> str:
            return "v"

        def uncached_handler(
            event,
            context,
            value=Depends(provide_value, scope="container", use_cache=False),
        ) -> str:
            return value

        bind(uncached_handler)

    # then the impossible combination is rejected at declaration time
    message = str(excinfo.value)
    assert "container" in message
    assert "use_cache" in message


def test_container_scope_may_not_depend_on_invocation_scope_directly() -> None:
    # given a container-scoped provider with an invocation-scoped dependency
    def provide_request_local() -> str:
        return "local"

    def provide_shared(label: str = Depends(provide_request_local)) -> str:
        return label

    def container_handler(
        event, context, value=Depends(provide_shared, scope="container")
    ) -> str:
        return value

    # when the handler is bound
    # then the incompatible edge is rejected, naming both sides of the path
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(container_handler)
    message = str(excinfo.value)
    assert "provide_shared" in message
    assert "provide_request_local" in message


def test_container_scope_may_not_depend_on_invocation_scope_indirectly() -> None:
    # given a container -> container -> invocation dependency chain
    def provide_local() -> str:
        return "l"

    def provide_mid(local: str = Depends(provide_local)) -> str:
        return local

    def provide_top(mid: str = Depends(provide_mid, scope="container")) -> str:
        return mid

    def deep_handler(
        event, context, value=Depends(provide_top, scope="container")
    ) -> str:
        return value

    # when the handler is bound
    # then the second-hop violation is rejected with its provider path
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(deep_handler)
    message = str(excinfo.value)
    assert "provide_mid" in message
    assert "provide_local" in message


def test_container_scope_provider_may_not_read_the_invocation_context() -> None:
    # given a container-scoped provider taking the per-invocation context
    def provide_resource(context) -> str:
        return context.request_id

    def shared_resource_handler(
        event, context, value=Depends(provide_resource, scope="container")
    ) -> str:
        return value

    # when the handler is bound
    # then depending on invocation-specific state is rejected, naming the
    # provider and the reserved parameter
    with pytest.raises(BIND_FAILURE) as excinfo:
        bind(shared_resource_handler)
    message = str(excinfo.value)
    assert "provide_resource" in message
    assert "context" in message


def test_binding_never_executes_providers() -> None:
    # given a provider with an observable side effect
    calls: list[str] = []

    def provide_side_effect() -> str:
        calls.append("executed")
        return "v"

    def handler(event, context, value: str = Depends(provide_side_effect)) -> str:
        return value

    # when the dependency graph is validated and compiled
    bound = bind(handler)

    # then no provider ran during validation
    assert calls == []

    # and execution stays lazy: the provider runs at invocation time
    assert run_handler(bound, {}, InvocationContext()) == "v"
    assert calls == ["executed"]
