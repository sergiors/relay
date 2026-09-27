package config

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// setRequiredEnv sets the three required REDIS_* variables to sentinel values.
// It is a helper so individual Load tests only override the variable they are
// exercising (and clear it where a missing-required-var case is under test).
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("REDIS_URI", "redis:6379")
	t.Setenv("REDIS_STREAM", "stream")
	t.Setenv("REDIS_GROUP", "group")
}

// testLogger returns a logger writing into an in-memory buffer so tests can
// assert on what Load/parseRetention logs without polluting stderr. The buffer
// is returned for assertions on the logged text.
func testLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// discardLogger returns a logger writing to io.Discard, for Load calls where
// the specific log output is irrelevant. It stays local (rather than using
// testutil.DiscardLogger) because testutil imports config, so a config test
// importing testutil would be an import cycle.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mustLoad loads configuration with a discard logger and fails the test if Load
// returns an error. Tests that assert on a returned configuration error call
// Load directly instead.
func mustLoad(t *testing.T) Config {
	t.Helper()
	cfg, err := Load(discardLogger())
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	return cfg
}

// TestParseNetworks pins the NETWORKS parser contract: unset/empty/whitespace
// yields nil, entries are trimmed, empty entries ignored, duplicates removed,
// and declaration order preserved.
func TestParseNetworks(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{"unset", "", nil},
		{"whitespace only", "   ", nil},
		{"single", "backend", []string{"backend"}},
		{"ordered, trimmed", " backend , frontend ", []string{"backend", "frontend"}},
		{"duplicates removed, order kept", "b,a,b,a", []string{"b", "a"}},
		{"empty entries ignored", "a,,b,", []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseNetworks(tt.value)
			if len(got) != len(tt.want) {
				t.Fatalf("ParseNetworks(%q) = %v, want %v", tt.value, got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("ParseNetworks(%q) = %v, want %v", tt.value, got, tt.want)
				}
			}
		})
	}
}

// TestLoadNetworks pins that NETWORKS resolves through Load exactly as
// ParseNetworks parses it, and stays nil when unset.
func TestLoadNetworks(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NETWORKS", "")
	if got := mustLoad(t).Networks; got != nil {
		t.Fatalf("Networks = %v, want nil when unset", got)
	}

	t.Setenv("NETWORKS", "backend, frontend ,backend")
	got := mustLoad(t).Networks
	if len(got) != 2 || got[0] != "backend" || got[1] != "frontend" {
		t.Fatalf("Networks = %v, want [backend frontend]", got)
	}
}

// TestLoadResolvesFields proves Load() resolves every Config field from the
// environment: the required Redis settings pass through, the consumer name is
// resolved to the hostname, the optional retention window parses, and the
// optional metrics address stays opt-in (empty when unset, passed through when
// set).
func TestLoadResolvesFields(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_STREAM_RETENTION", "6h")
	t.Setenv("METRICS_ADDR", ":9090")

	cfg := mustLoad(t)
	if cfg.RedisURI != "redis:6379" || cfg.RedisStream != "stream" || cfg.RedisGroup != "group" {
		t.Fatalf("redis fields = %+v, want address/stream/group sentinels", cfg)
	}
	if cfg.StreamRetention != 6*time.Hour {
		t.Fatalf("StreamRetention = %v, want 6h", cfg.StreamRetention)
	}
	if cfg.MetricsAddr != ":9090" {
		t.Fatalf("MetricsAddr = %q, want %q", cfg.MetricsAddr, ":9090")
	}
	host, hostErr := os.Hostname()
	if hostErr != nil || host == "" {
		t.Fatalf("os.Hostname unavailable on this host: %v", hostErr)
	}
	if cfg.ConsumerName != host {
		t.Fatalf("ConsumerName = %q, want hostname %q", cfg.ConsumerName, host)
	}
}

// TestLoadMetricsAddrOptIn pins that METRICS_ADDR stays empty when unset, so the
// metrics HTTP server remains opt-in (a non-empty guard in the worker decides).
func TestLoadMetricsAddrOptIn(t *testing.T) {
	setRequiredEnv(t)
	cfg := mustLoad(t)
	if cfg.MetricsAddr != "" {
		t.Fatalf("MetricsAddr = %q, want empty (opt-in)", cfg.MetricsAddr)
	}
}

// TestMetricsAddrPassthrough pins that METRICS_ADDR is passed through as-is when
// set, so a configured metrics address reaches the Config exactly.
func TestMetricsAddrPassthrough(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("METRICS_ADDR", ":9091")
	cfg := mustLoad(t)
	if cfg.MetricsAddr != ":9091" {
		t.Fatalf("MetricsAddr = %q, want %q", cfg.MetricsAddr, ":9091")
	}
}

// TestLoadGitWebhookAddrOptIn pins that GIT_WEBHOOK_ADDR stays empty when
// unset, so the GitHub webhook server remains opt-in (a non-empty guard in the
// worker decides whether to bind it).
func TestLoadGitWebhookAddrOptIn(t *testing.T) {
	setRequiredEnv(t)
	cfg := mustLoad(t)
	if cfg.GitWebhookAddr != "" {
		t.Fatalf("GitWebhookAddr = %q, want empty (opt-in)", cfg.GitWebhookAddr)
	}
}

// TestLoadGitWebhookAddrPassthrough pins that GIT_WEBHOOK_ADDR is passed
// through as-is when set, so a configured webhook address reaches the Config
// exactly.
func TestLoadGitWebhookAddrPassthrough(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GIT_WEBHOOK_ADDR", ":8080")
	cfg := mustLoad(t)
	if cfg.GitWebhookAddr != ":8080" {
		t.Fatalf("GitWebhookAddr = %q, want %q", cfg.GitWebhookAddr, ":8080")
	}
}

// TestStreamRetention covers the REDIS_STREAM_RETENTION parsing contract via the
// pure parseRetention helper: retention is log-and-disable: unset/empty disables
// retention (0, no log); a valid duration parses; an invalid duration and a
// zero/negative value log a line mentioning REDIS_STREAM_RETENTION and return 0
// (disabled). A bad value therefore never fails startup.
func TestStreamRetention(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		want       time.Duration
		wantLogged bool
	}{
		{"unset disables, no log", "", 0, false},
		{"valid 6h", "6h", 6 * time.Hour, false},
		{"valid 30m", "30m", 30 * time.Minute, false},
		{"invalid duration logs and disables", "bogus", 0, true},
		{"zero logs and disables", "0", 0, true},
		{"negative logs and disables", "-5m", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, buf := testLogger()
			got := parseRetention(logger, tt.value)
			if got != tt.want {
				t.Fatalf("parseRetention(%q) = %v, want %v", tt.value, got, tt.want)
			}
			gotLogged := strings.Contains(buf.String(), "REDIS_STREAM_RETENTION")
			if gotLogged != tt.wantLogged {
				t.Fatalf("parseRetention(%q) logged REDIS_STREAM_RETENTION = %v, want %v; log: %q",
					tt.value, gotLogged, tt.wantLogged, buf.String())
			}
		})
	}
}

// TestLoadRetentionErrorMentionsVariable pins that a malformed
// REDIS_STREAM_RETENTION is logged rather than fatal: Load returns nil error and
// a Config with retention disabled (0), and the captured log mentions the
// offending variable so an operator can find the misconfiguration. A bad
// retention must not fail startup.
func TestLoadRetentionErrorMentionsVariable(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_STREAM_RETENTION", "bogus")

	logger, buf := testLogger()
	cfg, err := Load(logger)
	if err != nil {
		t.Fatalf("Load returned error %v, want nil (retention is log-and-disable)", err)
	}
	if cfg.StreamRetention != 0 {
		t.Fatalf("StreamRetention = %v, want 0 (disabled) on malformed value", cfg.StreamRetention)
	}
	if !strings.Contains(buf.String(), "REDIS_STREAM_RETENTION") {
		t.Fatalf("Load log does not mention REDIS_STREAM_RETENTION: %q", buf.String())
	}
}

// TestLoadReturnsErrorOnMissingVariable pins the required-variable contract:
// Load RETURNS an error naming the missing REDIS_* variable (rather than exiting
// the process), the returned Config is zero, and nothing is logged — config
// failures are surfaced once through the returned error at the process boundary,
// not duplicated as a fatal slog line.
func TestLoadReturnsErrorOnMissingVariable(t *testing.T) {
	for _, missEnv := range []string{"REDIS_URI", "REDIS_STREAM", "REDIS_GROUP"} {
		t.Run(missEnv, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(missEnv, "")

			logger, buf := testLogger()
			cfg, err := Load(logger)
			if err == nil {
				t.Fatalf("Load returned nil error for missing %s; want an error", missEnv)
			}
			if !strings.Contains(err.Error(), missEnv) {
				t.Fatalf("error %q does not name %s", err, missEnv)
			}
			if !strings.Contains(err.Error(), "not set") {
				t.Fatalf("error %q missing 'not set'", err)
			}
			if cfg.RedisURI != "" || cfg.RedisStream != "" || cfg.RedisGroup != "" {
				t.Fatalf("Config = %+v, want zero value on error", cfg)
			}
			if buf.Len() != 0 {
				t.Fatalf("Load logged %q for a returned config error; want no fatal log", buf.String())
			}
		})
	}
}

// TestConsumerNameResolvesToHostname proves Load() resolves the consumer name to
// the hostname on a normal test host. We compare against os.Hostname()
// directly; the two may legitimately differ only in weird environments where
// the hostname is empty, in which case the test fails rather than skips (an
// empty hostname is a returned error in consumerNameFromHost, so a normal host
// must resolve it).
func TestConsumerNameResolvesToHostname(t *testing.T) {
	host, hostErr := os.Hostname()
	if hostErr != nil || host == "" {
		t.Fatalf("os.Hostname unavailable on this host: %v", hostErr)
	}
	setRequiredEnv(t)
	cfg := mustLoad(t)
	if cfg.ConsumerName != host {
		t.Fatalf("ConsumerName = %q, want hostname %q", cfg.ConsumerName, host)
	}
}

// NOTE on consumerNameFromHost failure paths: consumerNameFromHost calls
// os.Hostname internally and RETURNS an error on both a resolution failure and
// an empty hostname. Those paths are not injectable (os.Hostname cannot be
// stubbed), so they cannot be unit-tested in-process and are deliberately left
// untested. The returned error is what makes them non-fatal, so the caller owns
// the process exit.

// TestLoadDefaultLogLevel pins that an unset LOG_LEVEL resolves to Info.
func TestLoadDefaultLogLevel(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("LOG_LEVEL", "")
	logger, _ := testLogger()
	cfg, err := Load(logger)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("LogLevel = %v, want INFO default", cfg.LogLevel)
	}
}

// TestLoadLogLevelParses pins that each explicit (case-insensitive, whitespace-
// trimmed) LOG_LEVEL resolves to the correct slog level. An empty value is the
// default (INFO). "warning" is not an alias and is covered by the invalid-value
// test.
func TestLoadLogLevelParses(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  slog.Level
	}{
		{"", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{" INFO ", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
	} {
		t.Run(tc.value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("LOG_LEVEL", tc.value)
			cfg := mustLoad(t)
			if cfg.LogLevel != tc.want {
				t.Fatalf("LogLevel = %v, want %v", cfg.LogLevel, tc.want)
			}
		})
	}
}

// TestLoadInvalidLogLevelReturnsError pins the invalid-LOG_LEVEL contract: Load
// RETURNS an error naming the variable and the valid values (rather than exiting
// the process) and logs nothing.
func TestLoadInvalidLogLevelReturnsError(t *testing.T) {
	for _, value := range []string{"bogus", "verbose", "warning"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("LOG_LEVEL", value)

			logger, buf := testLogger()
			_, err := Load(logger)
			if err == nil {
				t.Fatalf("Load returned nil error for invalid LOG_LEVEL %q; want an error", value)
			}
			for _, want := range []string{"LOG_LEVEL", "DEBUG", "INFO", "WARN", "ERROR"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not mention %s", err, want)
				}
			}
			if buf.Len() != 0 {
				t.Fatalf("Load logged %q for a returned config error; want no fatal log", buf.String())
			}
		})
	}
}

// TestLoadConcurrencyDefaults pins that unset MAX_CONCURRENCY and
// MAX_BUFFERED_EVENTS resolve to their documented defaults (8 and 16). Load
// needs the required REDIS_* vars set (setRequiredEnv).
func TestLoadConcurrencyDefaults(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAX_CONCURRENCY", "")
	t.Setenv("MAX_BUFFERED_EVENTS", "")
	cfg := mustLoad(t)
	if cfg.MaxConcurrency != DefaultMaxConcurrency {
		t.Fatalf("MaxConcurrency = %d, want default %d", cfg.MaxConcurrency, DefaultMaxConcurrency)
	}
	if cfg.MaxBufferedEvents != DefaultMaxBufferedEvents {
		t.Fatalf("MaxBufferedEvents = %d, want default %d", cfg.MaxBufferedEvents, DefaultMaxBufferedEvents)
	}
}

// TestLoadConcurrencyExplicitValues pins that explicit positive values are
// honored.
func TestLoadConcurrencyExplicitValues(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MAX_CONCURRENCY", "4")
	t.Setenv("MAX_BUFFERED_EVENTS", "32")
	cfg := mustLoad(t)
	if cfg.MaxConcurrency != 4 {
		t.Fatalf("MaxConcurrency = %d, want 4", cfg.MaxConcurrency)
	}
	if cfg.MaxBufferedEvents != 32 {
		t.Fatalf("MaxBufferedEvents = %d, want 32", cfg.MaxBufferedEvents)
	}
}

// TestLoadTraefikOptionalValues pins the three optional Traefik routing
// values: all empty/nil when unset (labels omitted; no defaults forced), and
// passed through as-is when set (including TRAEFIK_PRIORITY to a pointer).
func TestLoadTraefikOptionalValues(t *testing.T) {
	setRequiredEnv(t)
	cfg := mustLoad(t)
	if cfg.TraefikEntryPoints != "" || cfg.TraefikCertResolver != "" || cfg.TraefikPriority != nil {
		t.Fatalf("Traefik optional fields = %+v, want zero/nil when unset", cfg)
	}

	t.Setenv("TRAEFIK_ENTRYPOINTS", "websecure")
	t.Setenv("TRAEFIK_CERTRESOLVER", "letsencrypt")
	t.Setenv("TRAEFIK_PRIORITY", "100")
	cfg = mustLoad(t)
	if cfg.TraefikEntryPoints != "websecure" {
		t.Fatalf("TraefikEntryPoints = %q, want websecure", cfg.TraefikEntryPoints)
	}
	if cfg.TraefikCertResolver != "letsencrypt" {
		t.Fatalf("TraefikCertResolver = %q, want letsencrypt", cfg.TraefikCertResolver)
	}
	if cfg.TraefikPriority == nil || *cfg.TraefikPriority != 100 {
		t.Fatalf("TraefikPriority = %v, want &100", cfg.TraefikPriority)
	}
}

// TestLoadTraefikHostOverride pins the optional TRAEFIK_HOST_OVERRIDE value:
// empty when unset (no override, prior behavior) and passed through verbatim
// when set. Validation is the routing layer's concern, so config does not
// reject a non-hostname here — it is hostname-dumb passthrough like the other
// TRAEFIK_* fields.
func TestLoadTraefikHostOverride(t *testing.T) {
	setRequiredEnv(t)
	cfg := mustLoad(t)
	if cfg.TraefikHostOverride != "" {
		t.Fatalf("TraefikHostOverride = %q, want empty when unset", cfg.TraefikHostOverride)
	}

	t.Setenv("TRAEFIK_HOST_OVERRIDE", "localhost")
	cfg = mustLoad(t)
	if cfg.TraefikHostOverride != "localhost" {
		t.Fatalf("TraefikHostOverride = %q, want localhost", cfg.TraefikHostOverride)
	}
}

// TestParseOptionalPositiveInt pins the ParseOptionalPositiveInt contract:
// unset/empty/whitespace → (nil, nil) — the "not configured" pointer-nil state;
// positive ints parse to a pointer; zero, negative, non-numeric, and float
// values error naming the variable. Load surfaces that error unchanged (no
// os.Exit); this exported helper is where the validation logic lives.
func TestParseOptionalPositiveInt(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantNil   bool
		want      int
		wantError bool
	}{
		{"unset → nil", "", true, 0, false},
		{"empty → nil", "  ", true, 0, false},
		{"whitespace → nil", "\t\n ", true, 0, false},
		{"positive parses to pointer", "100", false, 100, false},
		{"trimmed positive", " 7 ", false, 7, false},
		{"zero rejected", "0", true, 0, true},
		{"negative rejected", "-1", true, 0, true},
		{"non-numeric rejected", "abc", true, 0, true},
		{"float rejected", "1.5", true, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOptionalPositiveInt("TRAEFIK_PRIORITY", tt.value)
			if tt.wantError {
				if err == nil {
					t.Fatalf("ParseOptionalPositiveInt(%q) = %v, nil; want error", tt.value, got)
				}
				if !strings.Contains(err.Error(), "TRAEFIK_PRIORITY") {
					t.Fatalf("error should name the variable: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOptionalPositiveInt(%q) error: %v", tt.value, err)
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("ParseOptionalPositiveInt(%q) = %v, want nil", tt.value, *got)
				}
				return
			}
			if got == nil {
				t.Fatalf("ParseOptionalPositiveInt(%q) = nil, want non-nil pointer", tt.value)
			}
			if *got != tt.want {
				t.Fatalf("ParseOptionalPositiveInt(%q) = %d, want %d", tt.value, *got, tt.want)
			}
		})
	}
}

// TestParsePositiveInt exercises the shared positive-integer parser directly.
// Defaults are resolved at the getEnv call site (see Load), so an empty value is
// a parse error here; whitespace-trimmed positive ints parse; and empty, zero,
// negative, non-numeric, float, and overflow values error. Load surfaces that
// error unchanged (no os.Exit); ParsePositiveInt is where the validation logic
// lives.
func TestParsePositiveInt(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      int
		wantError bool
	}{
		{"empty rejected", "", 0, true},
		{"whitespace rejected", "  ", 0, true},
		{"positive", "8", 8, false},
		{"trimmed positive", " 8 ", 8, false},
		{"explicit buffers", "32", 32, false},
		{"zero rejected", "0", 0, true},
		{"negative rejected", "-1", 0, true},
		{"non-numeric rejected", "abc", 0, true},
		{"float rejected", "1.5", 0, true},
		{"overflow rejected", "999999999999999999999", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePositiveInt("MAX_CONCURRENCY", tt.value)
			if tt.wantError {
				if err == nil {
					t.Fatalf("ParsePositiveInt(%q) = %d, nil; want error", tt.value, got)
				}
				if !strings.Contains(err.Error(), "MAX_CONCURRENCY") {
					t.Fatalf("error should name the variable: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePositiveInt(%q) error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("ParsePositiveInt(%q) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

// TestLoadInvalidConcurrencyReturnsError pins the invalid tuning-knob contract:
// Load RETURNS an error naming the variable and its positive-integer
// requirement, and logs nothing.
func TestLoadInvalidConcurrencyReturnsError(t *testing.T) {
	for _, tt := range []struct {
		env, value, wantVar string
	}{
		{"MAX_CONCURRENCY", "0", "MAX_CONCURRENCY"},
		{"MAX_CONCURRENCY", "abc", "MAX_CONCURRENCY"},
		{"MAX_BUFFERED_EVENTS", "-1", "MAX_BUFFERED_EVENTS"},
	} {
		t.Run(tt.env+"="+tt.value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tt.env, tt.value)

			logger, buf := testLogger()
			_, err := Load(logger)
			if err == nil {
				t.Fatalf("Load returned nil error for invalid %s=%s; want an error", tt.env, tt.value)
			}
			for _, want := range []string{tt.wantVar, "positive integer"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not mention %q", err, want)
				}
			}
			if buf.Len() != 0 {
				t.Fatalf("Load logged %q for a returned config error; want no fatal log", buf.String())
			}
		})
	}
}

// TestParsePositiveDuration exercises the shared positive-duration parser.
// Defaults are resolved at the getEnv call site (see Load), so an empty value is
// a parse error here; valid Go durations parse (trimmed); and empty, zero,
// negative, and malformed values error naming the variable. Load surfaces that
// error unchanged (no os.Exit); ParsePositiveDuration is where the validation
// logic lives.
func TestParsePositiveDuration(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      time.Duration
		wantError bool
	}{
		{"empty rejected", "", 0, true},
		{"whitespace rejected", "  ", 0, true},
		{"whitespace only rejected", "\t\n ", 0, true},
		{"valid 5m", "5m", 5 * time.Minute, false},
		{"valid 90s", "90s", 90 * time.Second, false},
		{"valid compound", "1h30m", 90 * time.Minute, false},
		{"trimmed valid", " 30s ", 30 * time.Second, false},
		{"zero rejected", "0", 0, true},
		{"zero duration rejected", "0s", 0, true},
		{"negative rejected", "-5m", 0, true},
		{"malformed rejected", "bogus", 0, true},
		{"bare number rejected", "300", 0, true},
		{"fractional valid", "1.5s", 1500 * time.Millisecond, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePositiveDuration("WARM_CONTAINER_IDLE_TIMEOUT", tt.value)
			if tt.wantError {
				if err == nil {
					t.Fatalf("ParsePositiveDuration(%q) = %v, nil; want error", tt.value, got)
				}
				if !strings.Contains(err.Error(), "WARM_CONTAINER_IDLE_TIMEOUT") {
					t.Fatalf("error should name the variable: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePositiveDuration(%q) error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("ParsePositiveDuration(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// TestLoadWarmContainerIdleTimeoutDefault pins that an unset
// WARM_CONTAINER_IDLE_TIMEOUT resolves to the documented 5m default.
func TestLoadWarmContainerIdleTimeoutDefault(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WARM_CONTAINER_IDLE_TIMEOUT", "")
	cfg := mustLoad(t)
	if cfg.WarmContainerIdleTimeout != DefaultWarmContainerIdleTimeout {
		t.Fatalf("WarmContainerIdleTimeout = %v, want default %v",
			cfg.WarmContainerIdleTimeout, DefaultWarmContainerIdleTimeout)
	}
	if DefaultWarmContainerIdleTimeout != 5*time.Minute {
		t.Fatalf("DefaultWarmContainerIdleTimeout = %v, want 5m", DefaultWarmContainerIdleTimeout)
	}
}

// TestLoadWarmContainerIdleTimeoutExplicit pins that an explicit Go duration is
// honored exactly.
func TestLoadWarmContainerIdleTimeoutExplicit(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WARM_CONTAINER_IDLE_TIMEOUT", "90s")
	cfg := mustLoad(t)
	if cfg.WarmContainerIdleTimeout != 90*time.Second {
		t.Fatalf("WarmContainerIdleTimeout = %v, want 90s", cfg.WarmContainerIdleTimeout)
	}
}

// TestLoadInvalidWarmContainerIdleTimeoutReturnsError pins the invalid
// WARM_CONTAINER_IDLE_TIMEOUT contract: Load RETURNS an error naming the
// variable and its positive-duration requirement, and logs nothing. A malformed
// or non-positive duration is a configuration error that fails startup (unlike
// REDIS_STREAM_RETENTION's log-and-disable).
func TestLoadInvalidWarmContainerIdleTimeoutReturnsError(t *testing.T) {
	for _, value := range []string{"0", "-5m", "bogus", "300"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("WARM_CONTAINER_IDLE_TIMEOUT", value)

			logger, buf := testLogger()
			_, err := Load(logger)
			if err == nil {
				t.Fatalf("Load returned nil error for invalid WARM_CONTAINER_IDLE_TIMEOUT=%q; want an error", value)
			}
			for _, want := range []string{"WARM_CONTAINER_IDLE_TIMEOUT", "positive duration"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not mention %q", err, want)
				}
			}
			if buf.Len() != 0 {
				t.Fatalf("Load logged %q for a returned config error; want no fatal log", buf.String())
			}
		})
	}
}
