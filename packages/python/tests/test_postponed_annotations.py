"""Dependency declarations under postponed (PEP 563) string annotations.

The future import makes every annotation in this module a string, so the
dependency layer must resolve ``Annotated[..., Depends(...)]`` markers and
plain ``Depends(...)`` defaults through annotation evaluation - including
when an unrelated parameter's annotation cannot be resolved at all.
"""

from __future__ import annotations

from typing import Annotated

from relay_runtime import Depends, bind

from di_support import InvocationContext, run_handler


def provide_base() -> str:
    return "base"


def provide_greeting(base: Annotated[str, Depends(provide_base)]) -> str:
    return f"hello {base}"


def default_form_handler(
    event: object,
    context: object,
    greeting: str = Depends(provide_greeting),
) -> str:
    return greeting


def annotated_form_handler(
    event: object,
    context: object,
    greeting: Annotated[str, Depends(provide_greeting)],
) -> str:
    return greeting


def test_default_depends_resolves_under_postponed_annotations() -> None:
    # given a handler whose annotations are stored as strings
    bound = bind(default_form_handler)

    # when the bound handler runs
    result = run_handler(bound, {}, InvocationContext())

    # then the default Depends declaration resolves through the string layer
    # across the nested provider chain
    assert result == "hello base"


def test_annotated_depends_resolves_under_postponed_annotations() -> None:
    # given a handler declaring its dependency through a stringified Annotated
    bound = bind(annotated_form_handler)

    # when the bound handler runs
    result = run_handler(bound, {}, InvocationContext())

    # then the Annotated Depends marker is found after annotation evaluation
    assert result == "hello base"


def test_default_depends_resolves_when_unrelated_annotation_fails() -> None:
    # given a handler whose extra parameter has an unresolvable annotation
    def handler(
        event: object,
        context: object,
        greeting: str = Depends(provide_greeting),
        broken: UndefinedAnnotation = None,
    ) -> str:
        return greeting

    # when the handler is bound and invoked
    bound = bind(handler)

    # then the unrelated annotation failure neither fails validation nor
    # blocks the default Depends declaration next to it
    assert run_handler(bound, {}, InvocationContext()) == "hello base"


def test_annotated_depends_resolves_when_unrelated_annotation_fails() -> None:
    # given a handler whose Annotated dependency sits beside a parameter
    # whose annotation cannot be resolved
    def handler(
        event: object,
        context: object,
        greeting: Annotated[str, Depends(provide_greeting)],
        broken: UndefinedAnnotation = None,
    ) -> str:
        return greeting

    # when the handler is bound and invoked
    bound = bind(handler)

    # then the resolvable Annotated marker still injects its dependency
    assert run_handler(bound, {}, InvocationContext()) == "hello base"
