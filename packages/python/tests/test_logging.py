"""Contract tests for relay_runtime.logging's structured Logger.

The suite runs standalone: the test environment installs no OpenTelemetry
packages, so every test doubles as proof that the SDK works without them.
Records are observed only through the public contract — the default handler's
JSON on stderr, or handlers attached through the documented methods — never
through module internals.
"""

import ast
import contextvars
import inspect
import io
import json
import logging
import os
import queue
import re
import subprocess
import sys
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path

import pytest

from relay_runtime.logging import Logger

CORE_KEYS = ("timestamp", "level", "service", "message")
INVOCATION_KEYS = (
    "app",
    "handler",
    "request_id",
    "attempt",
    "max_attempts",
    "cold_start",
    "message_id",
    "trace_id",
    "span_id",
)


def _service(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _json_records(text: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("{"):
            records.append(json.loads(stripped))
    return records


def _records_with_message(text: str, message: str) -> list[dict[str, object]]:
    return [
        record for record in _json_records(text) if record.get("message") == message
    ]


def _one_record(text: str, message: str) -> dict[str, object]:
    matches = _records_with_message(text, message)
    assert len(matches) == 1, (
        f"records with message {message!r} = {len(matches)}, want exactly 1; stderr:\n{text}"
    )
    return matches[0]


def _require_core_keys(
    record: dict[str, object], *, service: str | None = None
) -> None:
    for key in CORE_KEYS:
        assert key in record, f"required key {key!r} missing from record: {record!r}"
    timestamp = record["timestamp"]
    assert isinstance(timestamp, str)
    datetime.fromisoformat(timestamp)
    resolved_service = record["service"]
    assert isinstance(resolved_service, str) and resolved_service, (
        f"service must be a non-empty string: {record!r}"
    )
    if service is not None:
        assert resolved_service == service


def _attributes(record: dict[str, object]) -> dict[str, object]:
    nested = record.get("attributes")
    assert isinstance(nested, dict), f"attributes must be a nested object: {record!r}"
    return nested


def _location_line(location: object) -> int:
    if isinstance(location, dict):
        for value in location.values():
            if isinstance(value, int) and not isinstance(value, bool):
                return value
        raise AssertionError(f"location object carries no line number: {location!r}")
    if isinstance(location, str):
        matches = re.findall(r":(\d+)", location)
        if matches:
            return int(matches[-1])
    raise AssertionError(f"unrecognized location: {location!r}")


def _emit_with_stacklevel(logger: Logger, message: str) -> int:
    frame = inspect.currentframe()
    assert frame is not None
    emit_line = frame.f_lineno + 1
    logger.info(message, stacklevel=2)
    return emit_line


class _RaisingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        raise RuntimeError("handler-emit-boom")


class _RaisingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        raise RuntimeError("formatter-format-boom")


def test_default_constructor_emits_json_to_stderr(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # given a Logger built with no arguments at all; a unique environment
    # identity keeps this case's registry entry — and the SDK-owned handler's
    # construction-time stream binding — independent of the other bare-Logger
    # cases, which must still resolve the documented fallback themselves
    monkeypatch.setenv("OTEL_SERVICE_NAME", "default-ctor-svc")
    log = Logger()

    # then the default level admits INFO records without any configuration
    assert log.isEnabledFor(logging.INFO)
    assert not log.isEnabledFor(logging.DEBUG)

    # when a record is emitted
    log.info("default-constructor-record", extra={"k": "v"})

    # then the default handler writes exactly one JSON record to stderr with
    # every required key and the structured attributes nested
    record = _one_record(capsys.readouterr().err, "default-constructor-record")
    _require_core_keys(record)
    assert record["level"] == "INFO"
    assert _attributes(record)["k"] == "v"


@pytest.mark.parametrize("style", ["positional", "keyword"])
def test_explicit_service_appears_in_records(
    capsys: pytest.CaptureFixture[str], style: str
) -> None:
    service = _service("service")
    log = (
        Logger(service, "INFO")
        if style == "positional"
        else Logger(service=service, level="INFO")
    )

    log.info("service-record")

    record = _one_record(capsys.readouterr().err, "service-record")
    _require_core_keys(record, service=service)
    assert record["level"] == "INFO"


def test_constructor_level_filters_records(capsys: pytest.CaptureFixture[str]) -> None:
    log = Logger(_service("ctor-level"), "ERROR")

    log.info("ctor-info-record")
    log.error("ctor-error-record")

    err = capsys.readouterr().err
    assert not _records_with_message(err, "ctor-info-record")
    assert _records_with_message(err, "ctor-error-record")


def test_positional_message_arguments_follow_stdlib_formatting(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("fmt"), "INFO")

    log.info("order %s accepted in %dms", "paid", 42, extra={"order": "o-1"})

    record = _one_record(capsys.readouterr().err, "order paid accepted in 42ms")
    assert _attributes(record)["order"] == "o-1"


@pytest.mark.parametrize(
    ("method", "expected_level"),
    [
        ("debug", "DEBUG"),
        ("info", "INFO"),
        ("warning", "WARNING"),
        ("error", "ERROR"),
        ("critical", "CRITICAL"),
    ],
)
def test_levelled_methods_emit_their_level(
    capsys: pytest.CaptureFixture[str], method: str, expected_level: str
) -> None:
    log = Logger(_service("level"), "DEBUG")
    message = f"level-{method}-record"

    getattr(log, method)(message)

    record = _one_record(capsys.readouterr().err, message)
    _require_core_keys(record)
    assert record["level"] == expected_level


def test_exception_method_logs_at_error_level(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("exc-method"), "DEBUG")

    log.exception("method-level-record")

    record = _one_record(capsys.readouterr().err, "method-level-record")
    assert record["level"] == "ERROR"


def test_log_method_uses_the_explicit_numeric_level(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("numeric"), "DEBUG")

    log.log(logging.WARNING, "numeric-warning-record")

    record = _one_record(capsys.readouterr().err, "numeric-warning-record")
    assert record["level"] == "WARNING"


def test_set_level_and_is_enabled_for_agree_with_emission(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("levels"))
    log.setLevel("WARNING")

    for level_name, level in (
        ("debug", logging.DEBUG),
        ("info", logging.INFO),
        ("warning", logging.WARNING),
    ):
        message = f"gate-{level_name}-record"
        enabled = log.isEnabledFor(level)
        log.log(level, message)
        emitted = bool(_records_with_message(capsys.readouterr().err, message))
        assert emitted == enabled, (
            f"emission of {message} = {emitted}, but isEnabledFor = {enabled}"
        )

    assert not log.isEnabledFor(logging.INFO)
    assert log.isEnabledFor(logging.ERROR)

    log.setLevel(logging.INFO)
    assert log.isEnabledFor(logging.INFO)
    log.info("after-setlevel-record")
    assert _records_with_message(capsys.readouterr().err, "after-setlevel-record")


def test_add_and_remove_handler_controls_delivery(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("handlers"), "INFO")
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(logging.Formatter("%(message)s"))

    log.addHandler(handler)
    log.info("delivered-record")
    assert buffer.getvalue().splitlines() == ["delivered-record"]

    log.removeHandler(handler)
    log.info("removed-record")
    assert "removed-record" not in buffer.getvalue()
    assert _records_with_message(capsys.readouterr().err, "removed-record")


def test_repeated_construction_replaces_the_default_handler(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("replace")
    first = io.StringIO()
    Logger(service, "INFO", logger_handler=logging.StreamHandler(first))

    second = io.StringIO()
    log = Logger(service, "INFO", logger_handler=logging.StreamHandler(second))
    log.info("replaced-record")

    assert "replaced-record" not in first.getvalue()
    assert second.getvalue().splitlines() == ["replaced-record"]
    assert not _records_with_message(capsys.readouterr().err, "replaced-record")


def test_custom_formatter_replaces_json_and_last_construction_wins(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("formatter")
    log = Logger(
        service, "INFO", logger_formatter=logging.Formatter("[old] %(message)s")
    )
    log.info("formatter-one-record")

    log = Logger(
        service, "INFO", logger_formatter=logging.Formatter("[new] %(message)s")
    )
    log.info("formatter-two-record")

    lines = [line for line in capsys.readouterr().err.splitlines() if line]
    assert lines == ["[old] formatter-one-record", "[new] formatter-two-record"]


def test_handler_formatter_is_preserved_without_an_explicit_formatter(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("preserved")
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(logging.Formatter("PRESERVED %(message)s"))

    log = Logger(service, "INFO", logger_handler=handler)
    log.info("preserved-record")
    again = Logger(service, "INFO")
    again.info("preserved-again-record")

    assert buffer.getvalue().splitlines() == [
        "PRESERVED preserved-record",
        "PRESERVED preserved-again-record",
    ]
    err = capsys.readouterr().err
    assert not _records_with_message(err, "preserved-record")
    assert not _records_with_message(err, "preserved-again-record")


def test_raising_formatter_does_not_break_the_record(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("badfmt"), "INFO", logger_formatter=_RaisingFormatter())
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)

    log.info("badformatter-record")

    assert buffer.getvalue().splitlines() == ["badformatter-record"]


def test_raising_handler_does_not_break_other_handlers_or_the_call(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("isolate"), "INFO")
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(_RaisingHandler())
    log.addHandler(handler)

    log.info("isolation-record")

    assert buffer.getvalue().splitlines() == ["isolation-record"]
    assert _records_with_message(capsys.readouterr().err, "isolation-record")


def test_repeat_construction_shares_state_and_emits_one_record(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("repeat")
    first = Logger(service)
    second = Logger(service)

    first.setLevel("INFO")
    assert second.isEnabledFor(logging.INFO)

    second.info("repeat-record")
    assert len(_records_with_message(capsys.readouterr().err, "repeat-record")) == 1


def test_root_logger_is_never_reconfigured() -> None:
    root = logging.getLogger()
    before = (
        root.level,
        tuple(root.handlers),
        root.propagate,
        root.disabled,
        tuple(root.filters),
    )

    log = Logger(_service("root"), "INFO")
    log.append_keys(layer="root-check")
    log.info("root-record", extra={"x": 1})
    log.setLevel("DEBUG")
    handler = logging.StreamHandler(io.StringIO())
    log.addHandler(handler)
    log.removeHandler(handler)
    log.setLevel("WARNING")
    log.warning("root-warning-record")

    after = (
        root.level,
        tuple(root.handlers),
        root.propagate,
        root.disabled,
        tuple(root.filters),
    )
    assert after == before, (
        "constructing and using a Logger reconfigured the root logger"
    )


def test_append_keys_persist_and_remove_keys_drops_them(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("appended"), "INFO")
    log.append_keys(component="billing", region="eu")

    log.info("appended-one-record")
    attributes = _attributes(
        _one_record(capsys.readouterr().err, "appended-one-record")
    )
    assert attributes["component"] == "billing"
    assert attributes["region"] == "eu"

    log.remove_keys(["region"])
    log.info("appended-two-record")
    attributes = _attributes(
        _one_record(capsys.readouterr().err, "appended-two-record")
    )
    assert attributes["component"] == "billing"
    assert "region" not in attributes


def test_appended_keys_are_isolated_between_different_services(
    capsys: pytest.CaptureFixture[str],
) -> None:
    first = Logger(_service("appended-a"), "INFO")
    second = Logger(_service("appended-b"), "INFO")

    first.append_keys(layer="only-first")
    second.append_keys(layer="only-second")

    first.info("appended-first-record")
    second.info("appended-second-record")

    err = capsys.readouterr().err
    attributes = _attributes(_one_record(err, "appended-first-record"))
    assert attributes["layer"] == "only-first"
    assert "only-second" not in attributes
    attributes = _attributes(_one_record(err, "appended-second-record"))
    assert attributes["layer"] == "only-second"
    assert "only-first" not in attributes


def test_same_service_instances_share_appended_and_context_keys(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("shared-identity")
    first = Logger(service, "INFO")
    second = Logger(service, "INFO")

    first.append_keys(component="shared-component")
    with first.append_context_keys(tenant="shared-tenant"):
        second.info("shared-identity-record")
    second.info("shared-after-record")

    err = capsys.readouterr().err
    attributes = _attributes(_one_record(err, "shared-identity-record"))
    assert attributes["component"] == "shared-component"
    assert attributes["tenant"] == "shared-tenant"
    after = _one_record(err, "shared-after-record")
    after_attributes = after.get("attributes") or {}
    assert isinstance(after_attributes, dict)
    assert after_attributes.get("component") == "shared-component"
    assert "tenant" not in after_attributes


def test_precedence_is_extra_over_temporary_over_appended(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("precedence"), "INFO")
    log.append_keys(layer="appended")

    with log.append_context_keys(layer="temporary"):
        log.info("precedence-temporary-record")
        err = capsys.readouterr().err
        attributes = _attributes(_one_record(err, "precedence-temporary-record"))
        assert attributes["layer"] == "temporary"

        log.info("precedence-extra-record", extra={"layer": "extra", "fresh": "value"})
        err = capsys.readouterr().err
        attributes = _attributes(_one_record(err, "precedence-extra-record"))
        assert attributes["layer"] == "extra"
        assert attributes["fresh"] == "value"

    log.info("precedence-restored-record")
    err = capsys.readouterr().err
    attributes = _attributes(_one_record(err, "precedence-restored-record"))
    assert attributes["layer"] == "appended"

    appended_only = Logger(_service("precedence-base"), "INFO")
    appended_only.append_keys(layer="appended")
    appended_only.info("precedence-appended-record")
    err = capsys.readouterr().err
    attributes = _attributes(_one_record(err, "precedence-appended-record"))
    assert attributes["layer"] == "appended"


def test_context_keys_do_not_leak_into_a_separate_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("cow"), "INFO")

    isolated = contextvars.copy_context()

    def enter_and_emit() -> None:
        with log.append_context_keys(tenant="inner"):
            log.info("inner-context-record")

    isolated.run(enter_and_emit)

    log.info("outer-context-record")
    err = capsys.readouterr().err
    attributes = _attributes(_one_record(err, "inner-context-record"))
    assert attributes["tenant"] == "inner"
    outer = _one_record(err, "outer-context-record")
    # A record with no fields for this logger may omit the attributes object
    # entirely; when it is present it must not carry the isolated keys.
    outer_attributes = outer.get("attributes") or {}
    assert isinstance(outer_attributes, dict)
    assert "tenant" not in outer_attributes


def test_context_keys_are_scoped_per_logger(
    capsys: pytest.CaptureFixture[str],
) -> None:
    first = Logger(_service("per-logger-a"), "INFO")
    second = Logger(_service("per-logger-b"), "INFO")

    with first.append_context_keys(layer="first-layer"):
        with second.append_context_keys(layer="second-layer"):
            first.info("per-logger-first-record")
            second.info("per-logger-second-record")
    first.info("per-logger-after-record")

    err = capsys.readouterr().err
    attributes = _attributes(_one_record(err, "per-logger-first-record"))
    assert attributes["layer"] == "first-layer"
    attributes = _attributes(_one_record(err, "per-logger-second-record"))
    assert attributes["layer"] == "second-layer"
    after = _one_record(err, "per-logger-after-record")
    after_attributes = after.get("attributes") or {}
    assert isinstance(after_attributes, dict)
    assert "layer" not in after_attributes


def test_context_key_scopes_nest_and_restore_after_exceptions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("scopes"), "INFO")

    with log.append_context_keys(base="base"):
        with log.append_context_keys(layer="outer"):
            with log.append_context_keys(layer="inner"):
                log.info("inner-record")
            log.info("mid-record")
            with pytest.raises(RuntimeError, match="leave-scope"):
                with log.append_context_keys(layer="broken"):
                    log.info("pre-crash-record")
                    raise RuntimeError("leave-scope")
            log.info("after-crash-record")
        log.info("final-record")
    log.info("post-base-record")

    err = capsys.readouterr().err
    inner = _attributes(_one_record(err, "inner-record"))
    assert inner["layer"] == "inner"
    assert inner["base"] == "base"
    mid = _attributes(_one_record(err, "mid-record"))
    assert mid["layer"] == "outer"
    assert mid["base"] == "base"
    pre_crash = _attributes(_one_record(err, "pre-crash-record"))
    assert pre_crash["layer"] == "broken"
    after_crash = _attributes(_one_record(err, "after-crash-record"))
    assert after_crash["layer"] == "outer"
    assert "broken" not in after_crash
    final = _attributes(_one_record(err, "final-record"))
    assert "layer" not in final
    assert final["base"] == "base"
    post_base = _one_record(err, "post-base-record")
    post_base_attributes = post_base.get("attributes") or {}
    assert isinstance(post_base_attributes, dict)
    assert "base" not in post_base_attributes
    assert "layer" not in post_base_attributes


def test_leaving_a_context_scope_keeps_mutations_made_inside_it(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("scope-mutations"), "INFO")
    log.append_keys(component="keep", obsolete="old", tier="bronze")

    # when appended and removed keys change INSIDE the temporary scope
    with log.append_context_keys(tenant="inside"):
        log.append_keys(region="eu", tier="gold")
        log.remove_keys(["obsolete"])

    # then leaving the block removes only its own layer: the mutations made
    # inside survive, and only the scope's temporary field expires
    log.info("scope-mutations-record")
    attributes = _attributes(
        _one_record(capsys.readouterr().err, "scope-mutations-record")
    )
    assert attributes["component"] == "keep"
    assert attributes["region"] == "eu"
    assert attributes["tier"] == "gold"
    assert "obsolete" not in attributes
    assert "tenant" not in attributes


def test_context_scopes_may_be_left_out_of_order(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("scope-order"), "INFO")
    outer = log.append_context_keys(outer_only="outer")
    inner = log.append_context_keys(inner_only="inner")
    outer.__enter__()
    inner.__enter__()
    log.info("order-both-record")

    # when the OUTER scope leaves before the inner one
    outer.__exit__(None, None, None)
    log.info("order-inner-record")
    inner.__exit__(None, None, None)
    log.info("order-none-record")

    # then each exit removed exactly its own layer, in any order
    err = capsys.readouterr().err
    both = _attributes(_one_record(err, "order-both-record"))
    assert both["outer_only"] == "outer"
    assert both["inner_only"] == "inner"
    after_outer = _attributes(_one_record(err, "order-inner-record"))
    assert "outer_only" not in after_outer
    assert after_outer["inner_only"] == "inner"
    after_inner = _one_record(err, "order-none-record")
    after_inner_attributes = after_inner.get("attributes") or {}
    assert isinstance(after_inner_attributes, dict)
    assert "outer_only" not in after_inner_attributes
    assert "inner_only" not in after_inner_attributes


def test_exiting_a_scope_bound_to_another_context_is_a_noop(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("stale-scope"), "INFO")
    kept: list = []

    def enter_and_keep() -> None:
        manager = log.append_context_keys(tenant="detached")
        manager.__enter__()
        kept.append(manager)

    # given a scope handle whose layer lives only in an isolated context
    contextvars.copy_context().run(enter_and_keep)

    # when that stale handle exits inside an active scope of the same logger
    with log.append_context_keys(tenant="active"):
        kept[0].__exit__(None, None, None)
        log.info("stale-exit-record")
    log.info("stale-after-record")

    # then the stale exit must not remove the current context's layer
    err = capsys.readouterr().err
    during = _attributes(_one_record(err, "stale-exit-record"))
    assert during["tenant"] == "active"
    after = _one_record(err, "stale-after-record")
    after_attributes = after.get("attributes") or {}
    assert isinstance(after_attributes, dict)
    assert "tenant" not in after_attributes


def test_extra_values_are_preserved_as_nested_attributes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("extra"), "INFO")

    log.info(
        "extra-record",
        extra={
            "count": 3,
            "ok": True,
            "nothing": None,
            "items": [1, 2],
            "nested": {"a": "b"},
        },
    )

    attributes = _attributes(_one_record(capsys.readouterr().err, "extra-record"))
    assert attributes["count"] == 3
    assert attributes["ok"] is True
    assert attributes["nothing"] is None
    assert attributes["items"] == [1, 2]
    assert attributes["nested"] == {"a": "b"}


def test_records_outside_an_invocation_carry_no_invocation_metadata(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("standalone-meta"), "INFO")

    log.info("no-invocation-record")

    record = _one_record(capsys.readouterr().err, "no-invocation-record")
    _require_core_keys(record)
    for key in INVOCATION_KEYS:
        assert key not in record, (
            f"{key!r} must be absent outside an invocation: {record!r}"
        )


def test_location_points_at_the_call_site_and_honors_stacklevel(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("location"), "INFO")

    frame = inspect.currentframe()
    assert frame is not None
    direct_line = frame.f_lineno + 1
    log.info("direct-record")

    frame = inspect.currentframe()
    assert frame is not None
    wrapped_caller_line = frame.f_lineno + 1
    _emit_with_stacklevel(log, "wrapped-record")

    err = capsys.readouterr().err
    direct = _one_record(err, "direct-record")
    assert "location" in direct, f"record carries no location: {direct!r}"
    assert "test_logging" in json.dumps(direct["location"])
    assert _location_line(direct["location"]) == direct_line

    wrapped = _one_record(err, "wrapped-record")
    assert _location_line(wrapped["location"]) == wrapped_caller_line


@pytest.mark.parametrize("mode", ["method", "flag", "instance", "tuple"])
def test_exc_info_renders_the_exception_object(
    capsys: pytest.CaptureFixture[str], mode: str
) -> None:
    log = Logger(_service("exc"), "DEBUG")
    message = f"exception-{mode}-record"

    if mode == "method":
        try:
            raise ValueError("boom-method")
        except ValueError:
            log.exception(message)
    elif mode == "flag":
        try:
            raise ValueError("boom-flag")
        except ValueError:
            log.info(message, exc_info=True)
    elif mode == "instance":
        log.error(message, exc_info=ValueError("boom-instance"))
    else:
        try:
            raise ValueError("boom-tuple")
        except ValueError:
            log.error(message, exc_info=sys.exc_info())

    record = _one_record(capsys.readouterr().err, message)
    expected_level = "INFO" if mode == "flag" else "ERROR"
    assert record["level"] == expected_level
    exception = record.get("exception")
    assert isinstance(exception, dict), f"exception object missing: {record!r}"
    assert exception["type"] == "ValueError"
    assert str(exception["message"]).startswith("boom-")
    # The stack text must be rendered somewhere in the exception object,
    # whatever key carries it; a traceback-less exc_info still renders the
    # "ValueError: message" line as contiguous text no plain field would form.
    serialized = json.dumps(exception)
    expected_stack_line = f"ValueError: {exception['message']}"
    assert expected_stack_line in serialized, (
        f"exception object renders no stack text: {serialized}"
    )


def test_stack_info_is_rendered(capsys: pytest.CaptureFixture[str]) -> None:
    log = Logger(_service("stack"), "INFO")

    log.info("stack-info-record", stack_info=True)

    record = _one_record(capsys.readouterr().err, "stack-info-record")
    assert "Stack (most recent call last)" in json.dumps(record)


def test_unserializable_values_get_markers_without_breaking_records(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("unsafe"), "INFO")
    circular: dict[str, object] = {}
    circular["self"] = circular

    log.info(
        "unsafe-record",
        extra={
            "circular": circular,
            "decimal": Decimal("1.5"),
            "nan_decimal": Decimal("NaN"),
            "infinite_float": float("inf"),
            "opaque": object(),
        },
    )

    record = _one_record(capsys.readouterr().err, "unsafe-record")
    _require_core_keys(record)
    attributes = _attributes(record)
    assert attributes["circular"] == {"self": {"$circular": True}}
    assert attributes["decimal"] == "1.5"
    assert attributes["nan_decimal"] == {"$unsupported": "Decimal"}
    assert attributes["infinite_float"] == {"$unsupported": "float"}
    assert attributes["opaque"] == {"$unsupported": "object"}


def test_item_bounds_truncate_oversized_collections(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("item-bounds"), "INFO")

    # when a sequence and a mapping sit exactly at the 10,000-item bound and
    # one item beyond it
    log.info(
        "item-bounds-record",
        extra={
            "list_at_bound": list(range(10_000)),
            "list_over_bound": list(range(10_001)),
            "map_at_bound": {f"k{i}": i for i in range(10_000)},
            "map_over_bound": {f"k{i}": i for i in range(10_001)},
        },
    )

    attributes = _attributes(
        _one_record(capsys.readouterr().err, "item-bounds-record")
    )
    at_bound = attributes["list_at_bound"]
    assert isinstance(at_bound, list)
    assert len(at_bound) == 10_000
    assert at_bound[0] == 0 and at_bound[-1] == 9_999

    over = attributes["list_over_bound"]
    assert isinstance(over, list)
    assert len(over) == 10_001
    assert over[-1] == {"$truncated": True}

    map_at_bound = attributes["map_at_bound"]
    assert isinstance(map_at_bound, dict)
    assert len(map_at_bound) == 10_000

    # an oversized mapping becomes one truncation marker, never a partial object
    assert attributes["map_over_bound"] == {"$truncated": True}


def test_a_line_over_one_mib_emits_the_safe_range_error_fallback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("line-bound")
    log = Logger(service, "INFO")

    # when the rendered line would exceed 1 MiB
    log.info("line-bound-record", extra={"blob": "x" * (1024 * 1024)})

    # then one bounded fallback line is emitted and the call continues
    lines = [
        line
        for line in capsys.readouterr().err.splitlines()
        if line.startswith("{")
    ]
    assert len(lines) == 1, f"stderr lines = {len(lines)}, want exactly 1"
    assert len(lines[0]) <= 1024 * 1024, "the fallback line must stay bounded"
    record = json.loads(lines[0])
    assert record["message"] == "log record could not be serialized"
    assert record["error"] == "RangeError"
    assert record["level"] == "INFO"
    assert record["service"] == service


def test_an_oversized_fallback_line_collapses_to_the_minimal_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # given application-controlled metadata (the service name) large enough
    # that even the fallback line would exceed 1 MiB
    log = Logger("s" * (1024 * 1024), "INFO")

    log.info("minimal-fallback-record")

    lines = [
        line
        for line in capsys.readouterr().err.splitlines()
        if line.startswith("{")
    ]
    assert len(lines) == 1, f"stderr lines = {len(lines)}, want exactly 1"
    record = json.loads(lines[0])
    assert record == {
        "level": "ERROR",
        "logger": "relay.logging",
        "message": "log record could not be serialized",
    }


def test_queue_listener_preserves_attributes_at_record_creation() -> None:
    log = Logger(_service("queue"), "INFO")
    log.append_keys(component="queued")

    buffer = io.StringIO()
    target = logging.StreamHandler(buffer)
    target.setFormatter(logging.Formatter("%(message)s|%(attributes)s"))
    records: queue.Queue[logging.LogRecord] = queue.Queue()
    listener = QueueListener(records, target)
    handler = QueueHandler(records)
    log.addHandler(handler)
    listener.start()
    try:
        with log.append_context_keys(tenant="queued-tenant"):
            log.info("queued-record", extra={"order": "q-1"})
    finally:
        listener.stop()
    log.removeHandler(handler)

    lines = buffer.getvalue().splitlines()
    assert len(lines) == 1, f"queued output = {lines!r}"
    message, attributes = lines[0].split("|", 1)
    assert message == "queued-record"
    assert ast.literal_eval(attributes) == {
        "component": "queued",
        "tenant": "queued-tenant",
        "order": "q-1",
    }


def test_logger_works_without_opentelemetry() -> None:
    src = Path(__file__).resolve().parents[1] / "src"
    script = """
import io
import json
import sys

sys.modules["opentelemetry"] = None

from relay_runtime.logging import Logger

captured = io.StringIO()
sys.stderr = captured
try:
    log = Logger("standalone", "INFO")
    log.info("standalone-record", extra={"k": 1})
finally:
    sys.stderr = sys.__stderr__

lines = [line for line in captured.getvalue().splitlines() if line.startswith("{")]
assert len(lines) == 1, lines
payload = json.loads(lines[0])
assert payload["message"] == "standalone-record", payload
assert payload["level"] == "INFO", payload
assert payload["service"] == "standalone", payload
assert payload["attributes"]["k"] == 1, payload
print("standalone-ok")
"""
    env = dict(os.environ, PYTHONPATH=str(src))
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"standalone run failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "standalone-ok" in result.stdout


def test_json_formatter_is_usable_as_a_public_formatter(
    capsys: pytest.CaptureFixture[str],
) -> None:
    try:
        from relay_runtime.logging import JsonFormatter
    except ImportError:
        pytest.skip("JsonFormatter is not part of the public surface")

    assert issubclass(JsonFormatter, logging.Formatter)
    log = Logger(_service("json-formatter"), "INFO", logger_formatter=JsonFormatter())
    log.info("json-formatter-record")
    record = _one_record(capsys.readouterr().err, "json-formatter-record")
    _require_core_keys(record)


def test_invalid_level_names_raise_the_documented_error() -> None:
    # given an unknown level name at construction and on setLevel
    with pytest.raises(ValueError, match="unknown log level"):
        Logger(_service("bad-level-ctor"), "BOGUS")

    log = Logger(_service("bad-level-setter"), "INFO")
    with pytest.raises(ValueError, match="unknown log level"):
        log.setLevel("BOGUS")

    # then the rejected setLevel leaves the previous level effective
    assert log.isEnabledFor(logging.INFO)


def test_disabled_debug_does_not_format_message_arguments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _Unstringable:
        def __init__(self) -> None:
            self.str_calls = 0

        def __str__(self) -> str:
            self.str_calls += 1
            raise AssertionError("a disabled record must not format its arguments")

    log = Logger(_service("disabled-debug"), "INFO")
    assert not log.isEnabledFor(logging.DEBUG)
    argument = _Unstringable()

    # when a disabled DEBUG record carries a format argument
    log.debug("never formatted %s", argument)

    # then the argument is never converted and nothing is emitted
    assert argument.str_calls == 0
    assert "never formatted" not in capsys.readouterr().err


def test_reserved_framework_field_in_extra_stays_nested_and_never_hijacks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _service("reserved-extra")
    log = Logger(service, "INFO")

    # when an application field reuses a Relay-owned top-level name
    log.info("reserved-extra-record", extra={"service": "hijack"})

    # then the record is emitted with that field nested under attributes while
    # the top-level metadata keeps the Logger's own value
    record = _one_record(capsys.readouterr().err, "reserved-extra-record")
    _require_core_keys(record, service=service)
    assert record["service"] == service
    assert _attributes(record)["service"] == "hijack"


def test_stdlib_reserved_record_collision_keeps_stdlib_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("stdlib-extra"), "INFO")

    # then a standard LogRecord attribute name keeps stdlib's own KeyError
    with pytest.raises(KeyError, match="Attempt to overwrite"):
        log.info("stdlib-extra-record", extra={"name": "collision"})

    assert capsys.readouterr().err == ""


def test_datetime_date_uuid_and_non_finite_float_serialization(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log = Logger(_service("temporal"), "INFO")
    moment = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    day = date(2024, 5, 6)
    identifier = uuid.uuid4()

    log.info(
        "temporal-record",
        extra={
            "moment": moment,
            "day": day,
            "identifier": identifier,
            "bad_float": float("nan"),
        },
    )

    attributes = _attributes(_one_record(capsys.readouterr().err, "temporal-record"))
    assert attributes["moment"] == moment.isoformat()
    assert attributes["day"] == day.isoformat()
    assert attributes["identifier"] == str(identifier)
    assert attributes["bad_float"] == {"$unsupported": "float"}


def test_handler_level_and_filter_are_preserved(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _OnlyMarked(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return getattr(record, "marker", None) == "keep"

    log = Logger(_service("handler-rules"), "INFO")
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.setLevel(logging.WARNING)
    handler.addFilter(_OnlyMarked())
    log.addHandler(handler)

    log.info("handler-info-record")  # below the application handler's level
    log.warning("handler-unmarked-record")  # rejected by its filter
    log.warning("handler-kept-record", extra={"marker": "keep"})  # passes both

    assert buffer.getvalue().splitlines() == ["handler-kept-record"]
    # the application handler's level did not reconfigure the logger itself
    assert _records_with_message(capsys.readouterr().err, "handler-info-record")


def test_explicit_service_overrides_the_environment_service(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # given an environment-provided service identity
    monkeypatch.setenv("OTEL_SERVICE_NAME", "env-service")

    # then an explicit service argument wins over it
    log = Logger("explicit-service", "INFO")
    log.info("explicit-service-record")

    record = _one_record(capsys.readouterr().err, "explicit-service-record")
    _require_core_keys(record, service="explicit-service")


def test_environment_service_is_used_without_an_explicit_service(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OTEL_SERVICE_NAME", "env-service")

    log = Logger()
    log.info("env-service-record")

    record = _one_record(capsys.readouterr().err, "env-service-record")
    _require_core_keys(record, service="env-service")


def test_standalone_is_the_service_fallback_outside_relay(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)

    log = Logger()
    log.info("standalone-fallback-record")

    record = _one_record(capsys.readouterr().err, "standalone-fallback-record")
    _require_core_keys(record, service="standalone")
