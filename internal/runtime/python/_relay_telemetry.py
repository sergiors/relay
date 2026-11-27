"""Managed OpenTelemetry tracer provider for one function process.

Embedded beside the bootstrap and written to ``/relay/_relay_telemetry.py`` in
the runtime image.

The process resolves its effective OTel configuration exactly once: after the
first invocation's application environment is applied, and before any handler
module is imported. Every later invocation reuses that provider, so an
import-time log record or span created by a handler module already exports
under Relay's invocation trace. A warm container therefore keeps ONE provider
for its whole life; a changed effective configuration requires a new process
(Relay replaces the container when its provider configuration changes) because
the OpenTelemetry API allows a global provider to be registered only once.

Ownership is deliberately narrow:

  - ``OTEL_SDK_DISABLED=true`` or no configured OTLP endpoint means Relay
    creates no provider and no exporter. There is no implicit localhost
    target: Relay never dials a collector the operator did not name.
  - If an application already registered a global provider before this module
    runs, Relay does not replace it and does not shut it down. The public API
    exposes the "nothing configured yet" sentinel as
    ``opentelemetry.trace.ProxyTracerProvider``; a different global provider is
    application-owned. This check avoids private SDK state: the only public
    fact needed is that ``set_tracer_provider`` is first-writer-wins.
  - ``RELAY_OTEL_PROVIDER`` is Relay's app-only provider-ownership setting. It
    accepts ``managed`` (the default when unset) or ``application``,
    case-insensitive. In ``application`` mode Relay neither creates nor replaces
    a provider, so the application can install and own its own. Any other
    non-empty value is rejected with a fixed diagnostic that never echoes the
    supplied value, and provider creation is skipped as well, so a typo cannot
    silently override the application's provider. This is not part of the
    public SDK surface and is never injected from the worker. Because the global
    API is first-writer-wins, an application that wants its own provider must set
    ``application`` (or register it before the first invocation); a later
    registration cannot replace Relay's.

Configuration is standard OTel environment configuration, not a Relay
dialect: the official SDK builds the resource (``OTEL_SERVICE_NAME`` /
``OTEL_RESOURCE_ATTRIBUTES``), selects the sampler, and reads the batch
processor variables; the official OTLP exporter reads its endpoint, headers,
TLS, compression, and timeout variables (including the trace-specific
precedence) itself. Relay only decides whether to construct them and which of
the two supported protocols to use. An effective endpoint that embeds userinfo,
a query string, or a fragment is rejected before construction with a fixed
diagnostic, because official exporter transport diagnostics may echo the
endpoint target (the HTTP exporter echoes query text). Authentication belongs
in ``OTEL_EXPORTER_OTLP_HEADERS`` (or the ``application`` ownership mode).

Telemetry is best-effort observability: setup, export, and shutdown failures
never fail an invocation. Relay's own diagnostics are fixed text plus, at most,
an exception class; they never include an endpoint, header, or other configured
value. Relay's logger is ``relay.telemetry``.

Relay does not install logging handlers, attach filters, or otherwise change the
application's logging configuration. The official OTLP exporters log their own
retry diagnostics through the ``opentelemetry`` logger hierarchy. Verified
against the installed exporter sources, their error and warning messages may
include the configured endpoint target in transport failures but never header
values; the SDK's batch export path suppresses instrumentation while exporting,
so those diagnostics cannot recurse into new spans. Relay cannot redact or
guarantee records produced by SDK loggers or by a handler the application
installs or replaces at any time; an application that treats its endpoint as
sensitive must configure handler-level filtering itself.

Shutdown semantics (verified against installed SDK sources, not assumed):

  - ``TracerProvider.shutdown()`` is synchronous and exposes no timeout.
  - The batch processor flushes its remaining queue and joins the export
    worker, but the join bound is SDK-version dependent: newer releases cap it
    at a fixed default, while older ones join without a timeout.
  - Relay therefore runs the SDK shutdown on a daemon thread and waits a fixed
    ``SHUTDOWN_TIMEOUT_SECONDS``. If the SDK is still exporting at that point,
    the process exits and the daemon thread/export is abandoned; if the daemon
    cannot be started, the flush is abandoned rather than run unbounded in
    place. This bounds the flush opportunity; it cannot guarantee delivery to an
    unreachable collector. The container's own lifecycle must still give the
    process a moment to run this before it is killed.
  - Application container-dependency cleanup runs first, while this provider is
    still active, and is itself bounded by
    ``DEPENDENCY_CLEANUP_TIMEOUT_SECONDS`` in ``_relay_server``. That budget plus
    this one stays below the worker's 10s shutdown drain grace, leaving room for
    interpreter exit. Neither bound can stop a synchronous or blocking
    application callback; such a callback can still delay exit until the worker
    kills the container.
"""

import logging
import os
import threading
from urllib.parse import urlsplit

_LOGGER = logging.getLogger("relay.telemetry")

ENV_SDK_DISABLED = "OTEL_SDK_DISABLED"
ENV_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
ENV_TRACES_ENDPOINT = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
ENV_PROTOCOL = "OTEL_EXPORTER_OTLP_PROTOCOL"
ENV_TRACES_PROTOCOL = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"

# Relay's app-only provider-ownership opt-out. Not public SDK API.
ENV_PROVIDER_OWNER = "RELAY_OTEL_PROVIDER"
PROVIDER_OWNER_MANAGED = "managed"
PROVIDER_OWNER_APPLICATION = "application"

PROTOCOL_GRPC = "grpc"
PROTOCOL_HTTP_PROTOBUF = "http/protobuf"
DEFAULT_PROTOCOL = PROTOCOL_HTTP_PROTOBUF

# The SDK's own shutdown join is not configurable through the public provider
# or batch-processor API, so Relay bounds the whole call on a daemon thread.
# The runtime's shutdown drain allows 10s for this process to exit after its
# channel closes (capped to half the remaining manager shutdown budget). The
# bootstrap spends at most _relay_server.DEPENDENCY_CLEANUP_TIMEOUT_SECONDS (1s)
# waiting for application container-dependency cleanup BEFORE this flush, so
# this bound plus that one stays below the drain grace with margin for
# interpreter exit. Both are named constants so the Go tests can pin the
# combined budget.
SHUTDOWN_TIMEOUT_SECONDS = 8.0

_started = False
_stopped = False
_provider = None


def ensure_started():
    """Initialize the process's managed provider once, if Relay owns it.

    Called after the invocation's application environment is applied and before
    the handler module is imported. Attempted exactly once per process: a warm
    container never reconfigures a provider that is already registered, and a
    failed setup is not retried on every invocation.
    """
    global _started, _provider
    if _started or _stopped:
        return
    _started = True
    try:
        _provider = _start()
    except Exception as exc:
        # Configuration errors are observability-only. Name the failure type,
        # never an endpoint or header value from the environment.
        _LOGGER.warning("telemetry: provider setup failed (%s)", type(exc).__name__)
        _provider = None


def shutdown():
    """Flush and shut down Relay's provider exactly once, if Relay owns one.

    Never raises. The SDK call is bounded: an exporter that is still retrying
    at the deadline is abandoned to process exit instead of extending the
    container's shutdown indefinitely. An application-owned provider (a
    pre-existing global provider, or ``RELAY_OTEL_PROVIDER=application``) is
    never touched here.
    """
    global _stopped, _provider
    if _stopped:
        return
    _stopped = True
    provider = _provider
    _provider = None
    if provider is not None:
        _bounded_shutdown(provider)


def _start():
    """Build and register the managed provider, or return None when Relay
    leaves tracing to the application (or there is nothing to export)."""
    owner = _provider_owner()
    if owner != PROVIDER_OWNER_MANAGED:
        # "application" leaves tracing to the app; None means the setting was
        # invalid and has already been reported. Both skip provider creation so
        # Relay never installs a provider it could not safely own.
        return None

    trace_api = _load_trace_api()
    if trace_api is None:
        return None
    if _sdk_disabled():
        return None
    endpoint_name, endpoint = _effective_endpoint()
    if endpoint is None:
        return None
    if _provider_registered(trace_api):
        return None
    if _endpoint_has_credentials(endpoint):
        _LOGGER.warning(
            "telemetry: %s includes userinfo, a query, or a fragment; use a "
            "credential-free endpoint and configure authentication with "
            "OTEL_EXPORTER_OTLP_HEADERS, or set RELAY_OTEL_PROVIDER=application "
            "to own the provider",
            endpoint_name,
        )
        return None

    protocol = _resolve_protocol()
    if protocol is None:
        return None
    sdk = _load_sdk()
    if sdk is None:
        return None
    Resource, TracerProvider, BatchSpanProcessor = sdk
    resource = Resource.create({})

    # Build the provider before the batch processor: the processor starts its
    # export worker immediately, and a provider configuration error must not
    # leave that worker running without an owner.
    exporter = _build_exporter(protocol)
    if exporter is None:
        return None
    try:
        provider = TracerProvider(resource=resource, shutdown_on_exit=False)
    except Exception:
        _discard_provider(exporter=exporter)
        raise

    try:
        processor = BatchSpanProcessor(exporter)
    except Exception:
        _discard_provider(exporter=exporter)
        raise

    processor_attached = False
    try:
        provider.add_span_processor(processor)
        processor_attached = True
        trace_api.set_tracer_provider(provider)
    except Exception as exc:
        # A registration failure after construction must not leak a started
        # export worker. Only the provider Relay just created is released; an
        # application-owned global provider is never touched.
        _LOGGER.warning(
            "telemetry: provider registration failed (%s)", type(exc).__name__
        )
        _discard_provider(
            provider,
            processor,
            processor_attached=processor_attached,
        )
        return None

    if not _registered_provider_is(trace_api, provider):
        # Another writer won the API's single global registration between the
        # check above and this call. Reload is impossible, so release only the
        # exporter/processor Relay just created and keep the other provider.
        _LOGGER.warning("telemetry: another tracer provider is already registered")
        _discard_provider(
            provider,
            processor,
            processor_attached=processor_attached,
        )
        return None
    _LOGGER.debug("telemetry: provider enabled")
    return provider


def _load_trace_api():
    try:
        from opentelemetry import trace as trace_api
    except ImportError:  # pragma: no cover - the runtime image ships the API
        return None
    return trace_api


def _load_sdk():
    try:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        # The API without the SDK keeps W3C propagation and no-op spans.
        return None
    return Resource, TracerProvider, BatchSpanProcessor


def _provider_owner():
    """Resolve Relay's app-only provider-ownership setting.

    Absent or ``managed`` means Relay owns the provider. ``application`` means
    the application owns it. Any other non-empty value is invalid: Relay emits
    a fixed diagnostic that never echoes the supplied value and also skips
    provider creation, so a typo cannot silently override a provider the
    application installs.
    """
    value = _env(ENV_PROVIDER_OWNER).lower()
    if not value or value == PROVIDER_OWNER_MANAGED:
        return PROVIDER_OWNER_MANAGED
    if value == PROVIDER_OWNER_APPLICATION:
        return PROVIDER_OWNER_APPLICATION
    _LOGGER.warning(
        "telemetry: unsupported value for %s; set it to %r or %r",
        ENV_PROVIDER_OWNER,
        PROVIDER_OWNER_MANAGED,
        PROVIDER_OWNER_APPLICATION,
    )
    return None


def _provider_registered(trace_api):
    """Report whether a global provider was already registered.

    ``get_tracer_provider()`` returns the public ``ProxyTracerProvider``
    sentinel until the first ``set_tracer_provider`` (or an
    ``OTEL_PYTHON_TRACER_PROVIDER`` entry point, which it loads and registers).
    Anything else is application-owned; Relay never overwrites or shuts it
    down. An unreadable state is treated as application-owned, because
    guessing would risk replacing a provider Relay does not own.
    """
    try:
        current = trace_api.get_tracer_provider()
    except Exception:
        return True
    proxy = getattr(trace_api, "ProxyTracerProvider", None)
    if proxy is None:
        return True
    return not isinstance(current, proxy)


def _registered_provider_is(trace_api, provider):
    try:
        return trace_api.get_tracer_provider() is provider
    except Exception:
        return False


def _env(name):
    return os.environ.get(name, "").strip()


def _sdk_disabled():
    return _env(ENV_SDK_DISABLED).lower() == "true"


def _effective_endpoint():
    """Return the (variable name, value) the trace exporter will use.

    The standard trace-specific endpoint wins over the generic one; an empty
    value is treated as unset. ``(None, None)`` means Relay has no target and
    must not construct an exporter.
    """
    for name in (ENV_TRACES_ENDPOINT, ENV_ENDPOINT):
        value = _env(name)
        if value:
            return name, value
    return None, None


def _endpoint_has_credentials(endpoint):
    """Report whether an endpoint embeds userinfo, a query, or a fragment.

    Official exporters echo the endpoint target in transport diagnostics, and
    the HTTP exporter's transport error text includes the query string, so a
    managed provider refuses such values before constructing anything. A
    scheme-ful URL is parsed for userinfo; a scheme-less gRPC authority is
    checked for the userinfo delimiter directly. Fragments and query strings
    are rejected because they are never part of a collector target and can
    carry tokens.
    """
    if "?" in endpoint or "#" in endpoint:
        return True
    if "://" not in endpoint:
        return "@" in endpoint
    try:
        parsed = urlsplit(endpoint)
    except ValueError:
        # An unparseable authority cannot be classified safely; keep the
        # userinfo delimiter check rather than exporting a possible credential.
        return "@" in endpoint
    return parsed.username is not None or parsed.password is not None


def _resolve_protocol():
    """Resolve the OTLP protocol with the standard precedence:
    trace-specific, then generic, then the OTel default. An unsupported
    non-empty value disables Relay's own export instead of silently picking a
    different transport.
    """
    for name in (ENV_TRACES_PROTOCOL, ENV_PROTOCOL):
        value = _env(name)
        if not value:
            continue
        if value in (PROTOCOL_GRPC, PROTOCOL_HTTP_PROTOBUF):
            return value
        _LOGGER.warning(
            "telemetry: unsupported value for %s; set it to %r or %r",
            name,
            PROTOCOL_GRPC,
            PROTOCOL_HTTP_PROTOBUF,
        )
        return None
    return DEFAULT_PROTOCOL


def _build_exporter(protocol):
    """Construct the official OTLP span exporter for the protocol.

    The exporter reads the standard endpoint, headers, TLS, compression, and
    timeout variables itself, including their trace-specific precedence, so
    Relay passes no options, credentials, or transport settings and never
    appends a path or opens a connection here.

    For gRPC, the official exporter derives insecure transport from an
    ``http://`` endpoint scheme and honors the standard
    ``OTEL_EXPORTER_OTLP_INSECURE`` / ``OTEL_EXPORTER_OTLP_TRACES_INSECURE``
    booleans when they are present. A scheme-less authority needs an inherited
    insecure boolean to reach a plaintext collector.
    """
    try:
        if protocol == PROTOCOL_GRPC:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )
        else:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )
    except ImportError:
        _LOGGER.warning("telemetry: OTLP exporter is unavailable")
        return None
    return OTLPSpanExporter()


def _discard_provider(
    provider=None,
    processor=None,
    *,
    processor_attached=False,
    exporter=None,
):
    """Bound cleanup to one Relay-owned component.

    Provider shutdown owns an attached processor; otherwise clean up the
    unattached processor or the exporter created before it.
    """
    if processor_attached and provider is not None:
        _bounded_shutdown(provider)
    elif processor is not None:
        _run_bounded_shutdown(processor.shutdown, "span processor shutdown")
    elif exporter is not None:
        _run_bounded_shutdown(exporter.shutdown, "exporter shutdown")


def _bounded_shutdown(provider):
    """Run the SDK's synchronous provider shutdown within a fixed window.

    ``TracerProvider.shutdown()`` has no timeout and joins the batch worker
    with the SDK's own fixed default, so it runs on a daemon thread. If it is
    still exporting at the deadline, process exit abandons the daemon. Never
    raises, including when the daemon thread cannot start.
    """
    _run_bounded_shutdown(provider.shutdown, "provider shutdown")


def _run_bounded_shutdown(close, label):
    """Run one synchronous SDK shutdown call boundedly and never raise.

    If the daemon thread cannot start, the call has no timeout and cannot be
    run safely in place, so it is abandoned: a failed flush must not block
    container exit.
    """
    finished = threading.Event()

    def run():
        try:
            close()
        except Exception as exc:
            _LOGGER.warning("telemetry: %s failed (%s)", label, type(exc).__name__)
        finally:
            finished.set()

    try:
        threading.Thread(target=run, name="relay-otel-shutdown", daemon=True).start()
    except Exception as exc:
        _LOGGER.warning(
            "telemetry: %s could not be started (%s); abandoning the flush",
            label,
            type(exc).__name__,
        )
        return

    if not finished.wait(SHUTDOWN_TIMEOUT_SECONDS):
        _LOGGER.warning(
            "telemetry: %s exceeded %gs; abandoning the flush",
            label,
            SHUTDOWN_TIMEOUT_SECONDS,
        )
