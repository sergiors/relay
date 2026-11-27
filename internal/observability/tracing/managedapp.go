package tracing

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

// ManagedAppConfig is the resolved OpenTelemetry process configuration for a
// managed application process (the Python or Node invocation bootstrap).
//
// Env is the worker-inherited, non-secret subset of OTEL_* variables plus the
// app-name service-name default. Application-specific OTEL values (including
// OTEL_RESOURCE_ATTRIBUTES, endpoint overrides, exporter authentication, and
// the app-only ownership opt-out) deliberately stay on the per-invocation
// environment frame, exactly like every other application env value; the
// bootstrap applies that frame before initializing its provider. Env therefore
// contains only values Relay inherits or defaults, and is safe to place in
// Docker Config.Env.
//
// Fingerprint is a private in-memory identity of the effective configuration.
// It includes the application's template OTEL values so a change rotates warm
// containers when the process-lifetime provider's inputs change. It is never
// persisted, logged, or placed in Docker/container metadata.
type ManagedAppConfig struct {
	Env         []string
	Fingerprint string
	// InheritedTraceSpecific lists signal-specific names whose effective value
	// comes from the worker tier. The runtime uses this provenance to add an
	// explicit empty trace-specific frame override when the application's
	// generic OTEL value (from its template env or a secret) would otherwise
	// lose to the inherited worker trace-specific value. It contains names only.
	InheritedTraceSpecific []string
}

// Standard OTEL variables handled by the managed-app resolution. The exporter
// and SDK still parse and apply the values themselves; this package owns the
// allowlist, source-tier precedence, and the few value shapes that must not
// reach a managed process unchecked.
const (
	envResourceAttributes    = "OTEL_RESOURCE_ATTRIBUTES"
	envServiceName           = "OTEL_SERVICE_NAME"
	envTimeout               = "OTEL_EXPORTER_OTLP_TIMEOUT"
	envTracesTimeout         = "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"
	envCompression           = "OTEL_EXPORTER_OTLP_COMPRESSION"
	envTracesCompression     = "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"
	envInsecure              = "OTEL_EXPORTER_OTLP_INSECURE"
	envTracesInsecure        = "OTEL_EXPORTER_OTLP_TRACES_INSECURE"
	envTracesSampler         = "OTEL_TRACES_SAMPLER"
	envTracesSamplerArg      = "OTEL_TRACES_SAMPLER_ARG"
	envBSPScheduleDelay      = "OTEL_BSP_SCHEDULE_DELAY"
	envBSPExportTimeout      = "OTEL_BSP_EXPORT_TIMEOUT"
	envBSPMaxQueueSize       = "OTEL_BSP_MAX_QUEUE_SIZE"
	envBSPMaxExportBatchSize = "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
	managedAppConfigVersion  = "relay-managed-otel-v1"
)

// managedAppWorkerKeys is the allowlisted inheritance set from the worker
// process. OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES are deliberately
// absent: a managed app never inherits Relay's own identity or resource
// attributes. Exporter authentication (headers, certificates, client keys) and
// every unlisted OTEL_* variable are also absent.
//
// The insecure booleans ARE inherited: they are non-secret standard settings,
// and an authority-form endpoint such as "collector:4317" is only plaintext
// when OTEL_EXPORTER_OTLP_INSECURE=true is present. The endpoint scheme still
// provides the default (http:// is insecure, https:// is secure), so Compose's
// http:// endpoint needs no extra setting. Application values stay on the frame
// and their changes join the private provider fingerprint.
var managedAppWorkerKeys = map[string]bool{
	envSDKDisabled:           true,
	envEndpoint:              true,
	envTracesEndpoint:        true,
	envProtocol:              true,
	envTracesProtocol:        true,
	envTimeout:               true,
	envTracesTimeout:         true,
	envCompression:           true,
	envTracesCompression:     true,
	envInsecure:              true,
	envTracesInsecure:        true,
	envTracesSampler:         true,
	envTracesSamplerArg:      true,
	envBSPScheduleDelay:      true,
	envBSPExportTimeout:      true,
	envBSPMaxQueueSize:       true,
	envBSPMaxExportBatchSize: true,
}

// providerConfigEnv reports whether an application environment name can affect
// the managed process's provider construction when it is applied before the
// first invocation. It deliberately covers EVERY OTEL_* name (not only the
// injected allowlist): exporter headers, certificates, client keys, TLS and
// insecure flags, span limits, app-only auto-configuration such as
// OTEL_PYTHON_TRACER_PROVIDER and OTEL_CONFIG_FILE, and any future standard
// variable are hashed privately. Omitting one would let a changed value
// silently mutate a warm process whose provider is already fixed. The app-only
// provider-ownership opt-out is the one non-OTEL name that changes ownership.
// None of these values are injected by this decision; they remain on the
// application environment path. Relay cannot observe a provider registered
// directly in application code or an untracked configuration source; those are
// outside the environment identity and require a new process to take effect.
func providerConfigEnv(name string) bool {
	return name == "RELAY_OTEL_PROVIDER" || strings.HasPrefix(name, "OTEL_")
}

// signalPair is one generic/signal-specific OTel configuration pair. The
// standard SDK gives the trace-specific variable precedence over the generic
// one, so an app-tier generic override must explicitly clear an inherited
// trace-specific value to be effective.
type signalPair struct {
	generic       string
	traceSpecific string
}

var managedAppSignalPairs = []signalPair{
	{generic: envEndpoint, traceSpecific: envTracesEndpoint},
	{generic: envProtocol, traceSpecific: envTracesProtocol},
	{generic: envTimeout, traceSpecific: envTracesTimeout},
	{generic: envCompression, traceSpecific: envTracesCompression},
	{generic: envInsecure, traceSpecific: envTracesInsecure},
}

// managedValue is one overlay value with an explicit present bit, preserving the
// difference between "absent" and "explicitly empty" (empty means the standard
// SDK default, but still clears an inherited lower-tier value).
type managedValue struct {
	value   string
	present bool
}

// ResolveManagedAppConfig resolves the process-lifetime OpenTelemetry
// environment Relay INJECTS for a managed app, and the private identity of the
// effective configuration.
//
// Env contains only the worker's allowlisted inherited values plus the app-name
// service-name default. Application-specific OTEL values stay on the normal
// per-invocation environment frame (see ManagedAppInvocationEnvOverrides) and
// are applied before the bootstrap initializes its provider.
//
// The private fingerprint covers the injected values plus every app template
// OTEL_* value, so an application template change rotates warm processes even
// when the injected values are unchanged. An explicitly empty app value is the
// standard "unset" spelling and participates in that identity; the app-name
// default service identity is handled per invocation so an app empty
// OTEL_SERVICE_NAME or a service.name resource attribute can still take effect.
//
// The worker's OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES are never
// inherited. No endpoint is ever synthesized, so a process with no configured
// endpoint stays disabled rather than dialing localhost.
func ResolveManagedAppConfig(appName string, appEnv map[string]string) ManagedAppConfig {
	return resolveManagedAppConfig(appName, appEnv, os.Environ())
}

// resolveManagedAppConfig is ResolveManagedAppConfig with an injectable
// environment for deterministic tests.
func resolveManagedAppConfig(appName string, appEnv map[string]string, environ []string) ManagedAppConfig {
	worker := collectManagedValuesFromEnviron(environ, managedAppWorkerKeys)

	envValues := make(map[string]managedValue, len(worker)+1)
	for name, value := range worker {
		envValues[name] = value
	}
	if appName != "" {
		envValues[envServiceName] = managedValue{value: appName, present: true}
	}

	// Record every inherited trace-specific value that an app-tier generic
	// value could need to clear. The per-invocation resolver decides from the
	// actual frame whether the application supplies one.
	var inheritedTraceSpecific []string
	for _, pair := range managedAppSignalPairs {
		value, ok := worker[pair.traceSpecific]
		if ok && strings.TrimSpace(value.value) != "" {
			inheritedTraceSpecific = append(inheritedTraceSpecific, pair.traceSpecific)
		}
	}
	sort.Strings(inheritedTraceSpecific)

	return ManagedAppConfig{
		Env:                    canonicalManagedEnv(envValues),
		Fingerprint:            managedBaseFingerprint(envValues, appEnv),
		InheritedTraceSpecific: inheritedTraceSpecific,
	}
}

// ManagedAppInvocationEnvOverrides returns the per-invocation frame values Relay
// must add to honor standard OTel precedence. It is applied to the request
// frame's env map, never to Docker Config.Env.
//
//   - An app-tier generic endpoint/protocol/timeout/compression/insecure must beat an
//     inherited worker trace-specific value: the frame gets an explicit empty
//     trace-specific entry. If the application also supplies the trace-specific
//     value, that value wins and no empty entry is added.
//   - A non-empty app OTEL_SERVICE_NAME wins unchanged. A present-but-empty app
//     value is standard-unset: it clears the injected app-name default unless
//     the app's resource attributes name the service, in which case the
//     Resource attribute wins. With no service-name resource attribute, the
//     app-name default is restored so the provider does not fall back to an
//     unknown service. A non-empty service.name in the app's
//     OTEL_RESOURCE_ATTRIBUTES clears the injected default.
func ManagedAppInvocationEnvOverrides(appName string, inheritedTraceSpecific []string, extraEnv []string) map[string]string {
	values := make(map[string]managedValue, len(extraEnv))
	for _, kv := range extraEnv {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !providerConfigEnv(name) {
			continue
		}
		values[name] = managedValue{value: value, present: true}
	}

	overrides := make(map[string]string)
	for _, pair := range managedAppSignalPairs {
		if _, appSpecific := values[pair.traceSpecific]; appSpecific {
			continue
		}
		if _, appGeneric := values[pair.generic]; !appGeneric {
			continue
		}
		for _, name := range inheritedTraceSpecific {
			if name == pair.traceSpecific {
				overrides[pair.traceSpecific] = ""
				break
			}
		}
	}

	service, hasService := values[envServiceName]
	attrs, hasAttrs := values[envResourceAttributes]
	attrsNameService := hasAttrs && resourceAttributesHaveServiceName(attrs.value)
	switch {
	case hasService && strings.TrimSpace(service.value) != "":
		// Explicit non-empty app name wins; the frame already carries it.
	case hasService:
		// Explicitly empty is standard-unset. Keep it empty when the resource
		// attributes name the service; otherwise restore the app identity.
		if !attrsNameService && appName != "" {
			overrides[envServiceName] = appName
		}
	case attrsNameService:
		// Clear the injected app-name default so the Resource attribute wins.
		overrides[envServiceName] = ""
	}
	return overrides
}

// canonicalManagedEnv renders a managedValue map as sorted K=V entries. Presence
// is preserved, so an explicitly empty value is emitted as "=..." rather than
// omitted.
func canonicalManagedEnv(values map[string]managedValue) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	env := make([]string, 0, len(names))
	for _, name := range names {
		env = append(env, name+"="+values[name].value)
	}
	return env
}

// collectManagedValuesFromEnviron parses an environ slice, last value winning on
// duplicate names (matching os.Environ semantics).
func collectManagedValuesFromEnviron(environ []string, allowed map[string]bool) map[string]managedValue {
	out := make(map[string]managedValue, len(allowed))
	for _, kv := range environ {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !allowed[name] {
			continue
		}
		out[name] = managedValue{value: sanitizeManagedValue(name, value), present: true}
	}
	return out
}

// sanitizeManagedValue filters one inherited worker value to a shape the managed
// provider can safely consume. A credential-bearing endpoint is omitted rather
// than rewritten; unsupported compression becomes empty (the standard "none"
// default) because only the verified non-secret forms are propagated. Other
// standard values are passed through for the SDK to interpret, so Relay does
// not silently replace an operator's configuration.
func sanitizeManagedValue(name, value string) string {
	switch name {
	case envEndpoint, envTracesEndpoint:
		return sanitizeEndpoint(value)
	case envCompression, envTracesCompression:
		return sanitizeCompression(value)
	default:
		return value
	}
}

// sanitizeEndpoint refuses a credential-bearing endpoint by omitting it. A
// worker endpoint with userinfo, a query, or a fragment is NOT rewritten into an
// unauthenticated variant that silently changes the operator's target: the
// value is dropped (empty means the standard "unset"), and the app can set its
// own endpoint through its normal environment. A malformed URL is likewise
// dropped so it cannot smuggle components into Config.Env.
func sanitizeEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	// A gRPC endpoint is commonly a bare authority ("127.0.0.1:4317",
	// "[::1]:4317", "collector:4317"), which is not always a valid absolute URL.
	// Any credential marker in that opaque form is refused, not edited.
	if !strings.Contains(raw, "://") {
		if strings.ContainsAny(raw, "?#@") {
			return ""
		}
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return ""
	}
	return parsed.String()
}

// sanitizeCompression keeps only the non-secret compression values the managed
// exporters support. Anything else becomes empty (the standard "none" default)
// rather than reaching an exporter constructor that would abort provider setup.
func sanitizeCompression(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "gzip", "none":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return ""
	}
}

// resourceAttributesHaveServiceName reports whether a standard
// OTEL_RESOURCE_ATTRIBUTES value carries a NON-EMPTY service.name. An empty
// service.name is the standard's "unset" spelling, so the app-name default must
// still apply; a non-empty one preserves standard Resource precedence over the
// default. The value's exact form is the SDK's concern.
func resourceAttributesHaveServiceName(raw string) bool {
	for _, pair := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		if strings.TrimSpace(key) == envServiceNameKey && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

// envServiceNameKey is the standard resource attribute key whose presence an app
// may use instead of OTEL_SERVICE_NAME.
const envServiceNameKey = "service.name"

// ManagedAppInvocationFingerprint extends the prepared managed-config
// fingerprint with the per-invocation OTEL values carried by the application
// environment frame (including secret exporter authentication). A managed
// invocation provider reads the process environment after the first
// invocation's application environment is applied, so those values are part of
// its effective configuration even though they are never placed in Docker
// Config.Env.
//
// The result is a private in-memory identity: values are hashed, never stored,
// logged, or exposed. An extraEnv with no relevant OTEL name returns base
// unchanged, preserving the caller's existing generation identity.
func ManagedAppInvocationFingerprint(base string, extraEnv []string) string {
	overrides := make(map[string]managedValue)
	for _, kv := range extraEnv {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !providerConfigEnv(name) {
			continue
		}
		overrides[name] = managedValue{value: value, present: true}
	}
	if len(overrides) == 0 {
		return base
	}
	names := make([]string, 0, len(overrides))
	for name := range overrides {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	writeFingerprintPart(h, managedAppConfigVersion+":invocation")
	writeFingerprintPart(h, base)
	for _, name := range names {
		writeFingerprintPart(h, name)
		writeFingerprintPart(h, overrides[name].value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// managedBaseFingerprint derives the private identity of the injected worker
// configuration plus every application template value that can affect provider
// construction. App values are hashed raw because the provider reads the frame
// value, not a sanitized copy; they are never stored or exposed. Unambiguous
// length prefixes and environment/source markers prevent collisions through
// concatenation.
func managedBaseFingerprint(env map[string]managedValue, appEnv map[string]string) string {
	envNames := make([]string, 0, len(env))
	for name := range env {
		envNames = append(envNames, name)
	}
	sort.Strings(envNames)
	appNames := make([]string, 0, len(appEnv))
	for name := range appEnv {
		if providerConfigEnv(name) {
			appNames = append(appNames, name)
		}
	}
	sort.Strings(appNames)

	h := sha256.New()
	writeFingerprintPart(h, managedAppConfigVersion)
	for _, name := range envNames {
		writeFingerprintPart(h, "env")
		writeFingerprintPart(h, name)
		writeFingerprintPart(h, env[name].value)
	}
	for _, name := range appNames {
		writeFingerprintPart(h, "app")
		writeFingerprintPart(h, name)
		writeFingerprintPart(h, appEnv[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// writeFingerprintPart writes one length-prefixed field so a value containing a
// separator cannot shift the boundary into a colliding pair.
func writeFingerprintPart(h interface{ Write([]byte) (int, error) }, part string) {
	_, _ = h.Write([]byte(strconv.Itoa(len(part))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(part))
}
