"""Structured application logging built on Python's standard logging API."""

from __future__ import annotations

import importlib
import logging
import os
import threading
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from types import MethodType, TracebackType
from typing import Any

from ._bridge import BRIDGE
from ._context import Invocation
from ._format import JsonFormatter

__all__ = ["Logger"]

_ExcInfo = (
    bool
    | BaseException
    | tuple[type[BaseException], BaseException, TracebackType]
    | None
)
_SDK_HANDLER_FLAG = "_relay_structured_logging_handler"
_CONFIGURED_HANDLER_ATTR = "_relay_configured_logging_handler"
_CONFIG_LOCK = threading.RLock()
_FRAMEWORK_FIELDS = frozenset(
    {
        "app",
        "attempt",
        "attributes",
        "cold_start",
        "exception",
        "handler",
        "level",
        "location",
        "logger",
        "message",
        "request_id",
        "service",
        "span_id",
        "stack_info",
        "timestamp",
        "trace_id",
        "exc_info",
        "extra",
        "stacklevel",
    }
)

_TRACE_API_UNSET = object()
_TRACE_API: Any = _TRACE_API_UNSET
_TRACE_API_LOCK = threading.Lock()


class Logger:
    """Structured logger for a service.

    The stdlib logger name is ``relay_runtime.<service>``. Service identity is
    resolved once at construction from ``service``, ``OTEL_SERVICE_NAME``, the
    active Relay app, or ``standalone`` in that order.
    """

    __slots__ = ("_logger", "_service")

    def __init__(
        self,
        service: str | None = None,
        level: str | int | None = None,
        *,
        logger_handler: logging.Handler | None = None,
        logger_formatter: logging.Formatter | None = None,
    ) -> None:
        resolved_service = _resolve_service(service)
        if logger_handler is not None and not isinstance(
            logger_handler, logging.Handler
        ):
            raise TypeError("logger_handler must be a logging.Handler")
        if logger_formatter is not None and not isinstance(
            logger_formatter, logging.Formatter
        ):
            raise TypeError("logger_formatter must be a logging.Formatter")

        logger_name = f"relay_runtime.{resolved_service}"
        stdlib_logger = logging.getLogger(logger_name)
        if level is not None:
            stdlib_logger.setLevel(_resolve_level(level))
        elif stdlib_logger.level == logging.NOTSET:
            stdlib_logger.setLevel(logging.INFO)
        stdlib_logger.propagate = False
        _install_safe_dispatch(stdlib_logger)
        _configure_handler(
            stdlib_logger, logger_handler, logger_formatter
        )

        self._logger = stdlib_logger
        self._service = resolved_service

    @property
    def name(self) -> str:
        """Return the deterministic stdlib logger name."""
        return self._logger.name

    @property
    def service(self) -> str:
        """Return the service identity emitted by this logger."""
        return self._service

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(
            logging.DEBUG, msg, args, exc_info, stack_info, stacklevel, extra
        )

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(logging.INFO, msg, args, exc_info, stack_info, stacklevel, extra)

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(
            logging.WARNING, msg, args, exc_info, stack_info, stacklevel, extra
        )

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(logging.ERROR, msg, args, exc_info, stack_info, stacklevel, extra)

    def exception(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = True,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(logging.ERROR, msg, args, exc_info, stack_info, stacklevel, extra)

    def critical(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(
            logging.CRITICAL, msg, args, exc_info, stack_info, stacklevel, extra
        )

    def log(
        self,
        level: int,
        msg: object,
        *args: object,
        exc_info: _ExcInfo = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        self._log(level, msg, args, exc_info, stack_info, stacklevel, extra)

    def setLevel(self, level: str | int) -> None:
        """Set this named logger's level using standard logging semantics."""
        self._logger.setLevel(_resolve_level(level))

    def isEnabledFor(self, level: int) -> bool:
        """Return whether the stdlib logger accepts ``level``."""
        return self._logger.isEnabledFor(level)

    def addHandler(self, handler: logging.Handler) -> None:
        """Attach a handler without taking ownership of its lifetime."""
        self._logger.addHandler(handler)

    def removeHandler(self, handler: logging.Handler) -> None:
        """Detach a handler without closing it."""
        self._logger.removeHandler(handler)

    def append_keys(self, **keys: object) -> None:
        """Append fields for this named logger using copy-on-write context state."""
        _validate_fields(keys)
        BRIDGE.append_keys(self.name, keys)

    def remove_keys(self, *keys: str | Iterable[str]) -> None:
        """Remove appended fields by name or by an iterable of names."""
        if not keys:
            raw_names: Iterable[object] = ()
        elif len(keys) == 1 and isinstance(keys[0], str):
            raw_names = keys
        elif len(keys) == 1:
            candidate = keys[0]
            try:
                raw_names = iter(candidate)
            except TypeError as exc:
                raise TypeError(
                    "keys must be strings or an iterable of strings"
                ) from exc
        else:
            raw_names = keys

        names: list[str] = []
        for key in raw_names:
            if not isinstance(key, str):
                raise TypeError("field names must be strings")
            names.append(key)
        BRIDGE.remove_keys(self.name, tuple(names))

    @contextmanager
    def append_context_keys(self, **keys: object) -> Iterator[None]:
        """Temporarily add context-local fields for this named logger."""
        _validate_fields(keys)
        token = BRIDGE.bind_fields(self.name, keys)
        try:
            yield
        finally:
            BRIDGE.reset_fields(token)

    def _log(
        self,
        level: int,
        msg: object,
        args: tuple[object, ...],
        exc_info: _ExcInfo,
        stack_info: bool,
        stacklevel: int,
        extra: Mapping[str, object] | None,
    ) -> None:
        if not self._logger.isEnabledFor(level):
            return
        if isinstance(stacklevel, bool) or not isinstance(stacklevel, int):
            raise TypeError("stacklevel must be an integer")
        if stacklevel < 1:
            raise ValueError("stacklevel must be at least 1")

        call_fields = _copy_fields(extra)
        _validate_fields(call_fields)
        invocation, appended, temporary, expired = BRIDGE.snapshot(self.name)
        fields: dict[str, object] = dict(appended)
        fields.update(temporary)
        fields.update(call_fields)
        _validate_fields(fields)

        record_extra = _record_extra(
            self._service,
            invocation,
            fields,
            include_active_span=not expired,
        )
        self._logger.log(
            level,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel + 2,
            extra=record_extra,
        )


def _copy_fields(extra: Mapping[str, object] | None) -> dict[str, object]:
    if extra is None:
        return {}
    if not isinstance(extra, Mapping):
        raise TypeError("extra must be a mapping")
    fields: dict[str, object] = {}
    for key, value in extra.items():
        if not isinstance(key, str):
            raise TypeError("extra mapping keys must be strings")
        fields[key] = value
    return fields


def _resolve_service(service: str | None) -> str:
    if service is not None:
        if not isinstance(service, str):
            raise TypeError("service must be a string or None")
        resolved = service.strip()
        if not resolved:
            raise ValueError("service must be a non-empty string")
        return resolved

    configured = os.environ.get("OTEL_SERVICE_NAME", "").strip()
    if configured:
        return configured

    invocation, _, _, _ = BRIDGE.snapshot()
    if invocation is not None and invocation.app.strip():
        return invocation.app.strip()
    return "standalone"


def _resolve_level(level: str | int) -> int:
    if isinstance(level, bool):
        raise TypeError("level must be an integer or a level name")
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        resolved = logging.getLevelNamesMapping().get(level.upper())
        if resolved is not None:
            return resolved
        raise ValueError(f"unknown log level: {level!r}")
    raise TypeError("level must be an integer or a level name")


def _configure_handler(
    logger: logging.Logger,
    logger_handler: logging.Handler | None,
    logger_formatter: logging.Formatter | None,
) -> None:
    with _CONFIG_LOCK:
        configured = getattr(logger, _CONFIGURED_HANDLER_ATTR, None)
        if not isinstance(configured, logging.Handler):
            configured = next(
                (
                    candidate
                    for candidate in logger.handlers
                    if getattr(candidate, _SDK_HANDLER_FLAG, False)
                ),
                None,
            )

        if logger_handler is not None:
            if configured is not None and configured is not logger_handler:
                logger.removeHandler(configured)
            elif configured is None:
                for candidate in tuple(logger.handlers):
                    if getattr(candidate, _SDK_HANDLER_FLAG, False):
                        logger.removeHandler(candidate)
            if logger_formatter is not None:
                logger_handler.setFormatter(logger_formatter)
            logger.addHandler(logger_handler)
            setattr(logger, _CONFIGURED_HANDLER_ATTR, logger_handler)
            return

        if configured is not None and not getattr(
            configured, _SDK_HANDLER_FLAG, False
        ):
            if logger_formatter is not None:
                configured.setFormatter(logger_formatter)
            logger.addHandler(configured)
            return

        handler = (
            configured
            if configured is not None
            else _ensure_sdk_handler(logger)
        )
        logger.addHandler(handler)
        setattr(logger, _CONFIGURED_HANDLER_ATTR, handler)
        if logger_formatter is not None:
            handler.setFormatter(logger_formatter)


def _ensure_sdk_handler(logger: logging.Logger) -> logging.Handler:
    handler = next(
        (
            candidate
            for candidate in logger.handlers
            if getattr(candidate, _SDK_HANDLER_FLAG, False)
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler()
        setattr(handler, _SDK_HANDLER_FLAG, True)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return handler


def _install_safe_dispatch(logger: logging.Logger) -> None:
    if getattr(logger, "_relay_safe_handler_dispatch", False):
        return
    setattr(logger, "callHandlers", MethodType(_safe_call_handlers, logger))
    setattr(logger, "_relay_safe_handler_dispatch", True)


def _safe_call_handlers(logger: logging.Logger, record: logging.LogRecord) -> None:
    current: logging.Logger | None = logger
    found = 0
    while current is not None:
        for handler in current.handlers:
            found += 1
            if record.levelno < handler.level:
                continue
            try:
                handler.handle(record)
            except Exception:
                continue
        if not current.propagate:
            current = None
        else:
            current = current.parent

    last_resort = logging.lastResort
    if found == 0 and last_resort is not None and record.levelno >= last_resort.level:
        try:
            last_resort.handle(record)
        except Exception:
            pass


def _record_extra(
    service: str,
    invocation: Invocation | None,
    fields: Mapping[str, object],
    *,
    include_active_span: bool,
) -> dict[str, object]:
    trace_id: str | None = None
    span_id: str | None = None
    active_trace, active_span = (
        _active_span_ids() if include_active_span else (None, None)
    )
    if active_trace is not None:
        trace_id = active_trace
        span_id = active_span
    elif invocation is not None:
        trace_id = invocation.trace_id

    extra: dict[str, object] = {
        "service": service,
        "attributes": dict(fields),
    }
    if invocation is not None:
        extra.update(
            {
                "app": invocation.app,
                "handler": invocation.handler,
                "request_id": invocation.request_id,
                "attempt": invocation.attempt,
                "cold_start": invocation.cold_start,
            }
        )
    if trace_id is not None:
        extra["trace_id"] = trace_id
    if span_id is not None:
        extra["span_id"] = span_id

    for name, value in fields.items():
        extra[name] = value
    return extra


def _active_span_ids() -> tuple[str | None, str | None]:
    global _TRACE_API
    if _TRACE_API is _TRACE_API_UNSET:
        with _TRACE_API_LOCK:
            if _TRACE_API is _TRACE_API_UNSET:
                _TRACE_API = _load_trace_api()
    if _TRACE_API is None:
        return None, None
    try:
        span = _TRACE_API.get_current_span()
        span_context = span.get_span_context()
        if not span_context.is_valid:
            return None, None
        return (
            format(span_context.trace_id, "032x"),
            format(span_context.span_id, "016x"),
        )
    except Exception:
        return None, None


def _load_trace_api() -> Any:
    try:
        api = importlib.import_module("opentelemetry.trace")
    except Exception:
        return None
    return api if hasattr(api, "get_current_span") else None


def _validate_fields(fields: Mapping[str, object]) -> None:
    reserved = sorted(set(fields).intersection(_FRAMEWORK_FIELDS))
    if reserved:
        raise ValueError("reserved Relay log field name(s): " + ", ".join(reserved))
