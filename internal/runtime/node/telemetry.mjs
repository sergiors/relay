import { createRequire } from "node:module";

const BAKED_ROOT = "/app";
const OTEL_ROOT = "/relay/otel";
// Leave shutdown headroom within Relay's 10-second execution drain grace.
const SHUTDOWN_TIMEOUT_MS = 7500;
const PROVIDER_OWNER = "RELAY_OTEL_PROVIDER";

const appRequire = createRequire(BAKED_ROOT + "/package.json");
const otelRequire = createRequire(OTEL_ROOT + "/package.json");
const ownedShutdowns = new WeakMap();

let started = false;
let stopped = false;
let provider = null;
let shutdownPromise = null;
const pendingProviderShutdowns = new Set();
let api = null;
let tracePropagator = null;
let baggagePropagator = null;
let propagatorsLoaded = false;
const extractionWarnings = new Set();
const contextWarnings = new Set();

function diagnostic(level, message) {
  try {
    process.stderr.write(`telemetry: ${level}: ${message}\n`);
  } catch {
    // Telemetry diagnostics must not affect invocation outcomes.
  }
}

function errorName(error) {
  try {
    return error && typeof error.name === "string" ? error.name : "Error";
  } catch {
    return "Error";
  }
}

function loadApi() {
  if (api !== null) return api;
  api = appRequire("@opentelemetry/api");
  return api;
}

function loadPropagators() {
  if (propagatorsLoaded) {
    return tracePropagator !== null && baggagePropagator !== null;
  }
  propagatorsLoaded = true;
  if (tracePropagator !== null && baggagePropagator !== null) return true;
  try {
    const { W3CBaggagePropagator, W3CTraceContextPropagator } = otelRequire(
      "@opentelemetry/core",
    );
    tracePropagator = new W3CTraceContextPropagator();
    baggagePropagator = new W3CBaggagePropagator();
    return true;
  } catch (error) {
    diagnostic("warning", `W3C propagators unavailable (${errorName(error)})`);
    return false;
  }
}

function env(name) {
  return typeof process.env[name] === "string" ? process.env[name].trim() : "";
}

function providerOwner() {
  const value = env(PROVIDER_OWNER).toLowerCase();
  if (!value || value === "managed") return "managed";
  if (value === "application") return "application";
  diagnostic(
    "warning",
    `unsupported ${PROVIDER_OWNER}; set it to "managed" or "application"`,
  );
  return null;
}

function effectiveEndpoint() {
  for (const name of [
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
  ]) {
    const value = env(name);
    if (value) return { name, value };
  }
  return null;
}

function endpointHasCredentials(endpoint) {
  if (endpoint.includes("?") || endpoint.includes("#")) return true;
  if (!endpoint.includes("://")) return endpoint.includes("@");
  try {
    const parsed = new URL(endpoint);
    return parsed.username !== "" || parsed.password !== "";
  } catch {
    return endpoint.includes("@");
  }
}

function installContextManager(otelApi) {
  const { AsyncLocalStorageContextManager } = otelRequire(
    "@opentelemetry/context-async-hooks",
  );
  const manager = new AsyncLocalStorageContextManager();
  manager.enable();
  if (!otelApi.context.setGlobalContextManager(manager)) {
    manager.disable();
  }
}

function createExporter(protocol) {
  const packageName =
    protocol === "grpc"
      ? "@opentelemetry/exporter-trace-otlp-grpc"
      : "@opentelemetry/exporter-trace-otlp-proto";
  const { OTLPTraceExporter } = otelRequire(packageName);
  return new OTLPTraceExporter();
}

function configuredSampler(core, sdkTrace) {
  const samplerName = core.getStringFromEnv("OTEL_TRACES_SAMPLER");
  if (samplerName === undefined) return undefined;

  switch (samplerName) {
    case "always_on":
      return new sdkTrace.AlwaysOnSampler();
    case "always_off":
      return new sdkTrace.AlwaysOffSampler();
    case "parentbased_always_on":
      return new sdkTrace.ParentBasedSampler({
        root: new sdkTrace.AlwaysOnSampler(),
      });
    case "parentbased_always_off":
      return new sdkTrace.ParentBasedSampler({
        root: new sdkTrace.AlwaysOffSampler(),
      });
    case "traceidratio":
      return new sdkTrace.TraceIdRatioBasedSampler(samplerRatio(core));
    case "parentbased_traceidratio":
      return new sdkTrace.ParentBasedSampler({
        root: new sdkTrace.TraceIdRatioBasedSampler(samplerRatio(core)),
      });
    default:
      diagnostic(
        "warning",
        "unsupported OTEL_TRACES_SAMPLER; using the default",
      );
      return undefined;
  }
}

function samplerRatio(core) {
  const value = core.getNumberFromEnv("OTEL_TRACES_SAMPLER_ARG");
  if (value === undefined) return 1;
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    diagnostic(
      "warning",
      "invalid OTEL_TRACES_SAMPLER_ARG; using the default ratio",
    );
    return 1;
  }
  return value;
}

function batchProcessorOptions(core) {
  function positiveValue(name) {
    const value = core.getNumberFromEnv(name);
    if (value !== undefined && value <= 0) {
      diagnostic("warning", `invalid ${name}; using the SDK default`);
      return undefined;
    }
    return value;
  }

  return {
    maxQueueSize: positiveValue("OTEL_BSP_MAX_QUEUE_SIZE"),
    scheduledDelayMillis: positiveValue("OTEL_BSP_SCHEDULE_DELAY"),
    exportTimeoutMillis: positiveValue("OTEL_BSP_EXPORT_TIMEOUT"),
    maxExportBatchSize: positiveValue("OTEL_BSP_MAX_EXPORT_BATCH_SIZE"),
  };
}

function configuredResource(resources) {
  const attributes =
    resources.resourceAttributesEnvDetector.detect().attributes;
  const serviceName = resources.serviceNameEnvDetector.detect().attributes;
  return resources
    .defaultResource()
    .merge(resources.resourceFromAttributes(attributes))
    .merge(resources.resourceFromAttributes(serviceName));
}

function createManagedProvider(endpoint) {
  if (endpointHasCredentials(endpoint.value)) {
    diagnostic(
      "warning",
      `${endpoint.name} must not contain userinfo, a query, or a fragment; configure authentication with OTEL_EXPORTER_OTLP_HEADERS`,
    );
    return null;
  }

  const protocol =
    env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL") ||
    env("OTEL_EXPORTER_OTLP_PROTOCOL") ||
    "http/protobuf";
  if (protocol !== "grpc" && protocol !== "http/protobuf") {
    diagnostic(
      "warning",
      'unsupported OTLP protocol; set the trace or generic protocol to "grpc" or "http/protobuf"',
    );
    return null;
  }

  const core = otelRequire("@opentelemetry/core");
  const resources = otelRequire("@opentelemetry/resources");
  const sdkTrace = otelRequire("@opentelemetry/sdk-trace");
  const resource = configuredResource(resources);
  const sampler = configuredSampler(core, sdkTrace);
  const batchOptions = batchProcessorOptions(core);
  const exporter = createExporter(protocol);
  let processor = null;
  try {
    processor = new sdkTrace.BatchSpanProcessor({
      exporter,
      ...batchOptions,
    });
    return new sdkTrace.TracerProvider({
      resource,
      sampler,
      spanProcessors: [processor],
    });
  } catch (error) {
    // The processor owns the exporter once constructed, so clean up only the
    // most-derived component and let its shutdown close the exporter.
    void shutdownOwnedComponent(
      processor ?? exporter,
      processor === null ? "exporter" : "span processor",
    );
    throw error;
  }
}

export function ensureStarted() {
  if (started || stopped) return;
  started = true;
  loadPropagators();

  let otelApi;
  try {
    otelApi = loadApi();
  } catch (error) {
    diagnostic(
      "warning",
      `OpenTelemetry API unavailable (${errorName(error)})`,
    );
    return;
  }

  try {
    installContextManager(otelApi);
  } catch (error) {
    diagnostic("warning", `context setup failed (${errorName(error)})`);
  }

  const owner = providerOwner();
  if (
    owner !== "managed" ||
    env("OTEL_SDK_DISABLED").toLowerCase() === "true"
  ) {
    return;
  }

  const endpoint = effectiveEndpoint();
  if (endpoint === null) return;

  try {
    const managedProvider = createManagedProvider(endpoint);
    if (managedProvider === null) return;
    let registered;
    try {
      registered = otelApi.trace.setGlobalTracerProvider(managedProvider);
    } catch (error) {
      void shutdownOwnedProvider(managedProvider);
      throw error;
    }
    if (!registered) {
      void shutdownOwnedProvider(managedProvider);
      diagnostic("warning", "managed tracer provider registration failed");
      return;
    }
    provider = managedProvider;
  } catch (error) {
    diagnostic("warning", `provider setup failed (${errorName(error)})`);
  }
}

export function extract(carrier) {
  const otelApi = loadApi();
  const root = otelApi.ROOT_CONTEXT;
  if (!loadPropagators()) {
    try {
      return otelApi.propagation.extract(root, carrier ?? {});
    } catch (error) {
      warnExtractionFailure("fallback", error);
      return root;
    }
  }

  let parent = root;
  const textMapGetter = otelApi.defaultTextMapGetter;
  const textMapCarrier = carrier ?? {};
  try {
    parent = tracePropagator.extract(root, textMapCarrier, textMapGetter);
  } catch (error) {
    warnExtractionFailure("trace", error);
  }
  try {
    parent = baggagePropagator.extract(parent, textMapCarrier, textMapGetter);
  } catch (error) {
    warnExtractionFailure("baggage", error);
  }
  return parent;
}

function warnExtractionFailure(kind, error) {
  const failure = `${kind}:${errorName(error)}`;
  if (extractionWarnings.has(failure)) return;
  extractionWarnings.add(failure);
  diagnostic(
    "warning",
    `${kind} context extraction failed (${errorName(error)})`,
  );
}

export function withParent(carrier, callback) {
  let otelApi;
  try {
    otelApi = loadApi();
  } catch (error) {
    warnContextUnavailable(error);
    return callback();
  }

  let contextApi;
  try {
    contextApi = otelApi.context;
    if (typeof contextApi?.with !== "function") {
      return callback();
    }
  } catch (error) {
    warnContextUnavailable(error);
    return callback();
  }

  let parent;
  try {
    parent = extract(carrier);
  } catch (error) {
    warnExtractionFailure("parent", error);
    return callback();
  }

  let callbackStarted = false;
  const invoke = () => {
    if (callbackStarted) return;
    callbackStarted = true;
    return callback();
  };
  try {
    return contextApi.with(parent, invoke);
  } catch (error) {
    if (callbackStarted) throw error;
    warnContextUnavailable(error);
    return callback();
  }
}

function warnContextUnavailable(error) {
  if (contextWarnings.has(errorName(error))) return;
  contextWarnings.add(errorName(error));
  diagnostic("warning", `context unavailable (${errorName(error)})`);
}

function shutdownOwnedProvider(managedProvider) {
  return shutdownOwnedComponent(managedProvider, "provider");
}

function shutdownOwnedComponent(component, kind) {
  let shutdown = ownedShutdowns.get(component);
  if (shutdown !== undefined) return shutdown;

  // The pending set stores this bounded promise so final shutdown joins the
  // existing deadline instead of starting another wait for the same component.
  let timer;
  const cleanup = Promise.resolve()
    .then(() => component.shutdown())
    .catch((error) => {
      diagnostic("warning", `${kind} shutdown failed (${errorName(error)})`);
    });
  const deadline = new Promise((resolve) => {
    timer = setTimeout(() => {
      diagnostic(
        "warning",
        `${kind} shutdown timed out; abandoning pending export`,
      );
      resolve();
    }, SHUTDOWN_TIMEOUT_MS);
  });
  shutdown = Promise.race([cleanup, deadline]).finally(() =>
    clearTimeout(timer),
  );
  ownedShutdowns.set(component, shutdown);
  pendingProviderShutdowns.add(shutdown);
  void shutdown.then(
    () => pendingProviderShutdowns.delete(shutdown),
    () => pendingProviderShutdowns.delete(shutdown),
  );
  return shutdown;
}

function boundedFinalShutdown(managedProvider) {
  const pending = new Set(pendingProviderShutdowns);
  if (managedProvider !== null) {
    pending.add(shutdownOwnedProvider(managedProvider));
  }

  return Promise.all(pending);
}

export function shutdown() {
  if (shutdownPromise !== null) return shutdownPromise;
  stopped = true;
  const managedProvider = provider;
  provider = null;
  shutdownPromise = boundedFinalShutdown(managedProvider);
  return shutdownPromise;
}
