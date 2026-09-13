package config

import (
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// setRequiredEnv sets the three required REDIS_* variables to sentinel values.
// It is a helper so individual Load tests only override the variable they are
// exercising (and clear it where a missing-required-var case is under test).
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_STREAM", "stream")
	t.Setenv("REDIS_GROUP", "group")
}

// testLogger returns a logger writing into an in-memory buffer so tests can
// assert on what Load/parseRetention logs without polluting stderr. The buffer
// is returned for assertions on the logged text.
func testLogger() (*log.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return log.New(&buf, "", 0), &buf
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

	cfg := Load(log.New(io.Discard, "", 0))
	if cfg.RedisAddr != "redis:6379" || cfg.RedisStream != "stream" || cfg.RedisGroup != "group" {
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
	cfg := Load(log.New(io.Discard, "", 0))
	if cfg.MetricsAddr != "" {
		t.Fatalf("MetricsAddr = %q, want empty (opt-in)", cfg.MetricsAddr)
	}
}

// TestMetricsAddrPassthrough pins that METRICS_ADDR is passed through as-is when
// set, so a configured metrics address reaches the Config exactly.
func TestMetricsAddrPassthrough(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("METRICS_ADDR", ":9091")
	cfg := Load(log.New(io.Discard, "", 0))
	if cfg.MetricsAddr != ":9091" {
		t.Fatalf("MetricsAddr = %q, want %q", cfg.MetricsAddr, ":9091")
	}
}

// TestStreamRetention covers the REDIS_STREAM_RETENTION parsing contract via the
// pure parseRetention helper now that retention is log-and-disable: unset/empty
// disables retention (0, no log); a valid duration parses; an invalid duration
// and a zero/negative value log a line mentioning REDIS_STREAM_RETENTION and
// return 0 (disabled). A bad value therefore no longer fails startup.
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
				t.Fatalf("parseRetention(%q) logged REDIS_STREAM_RETENTION = %v, want %v; log: %q", tt.value, gotLogged, tt.wantLogged, buf.String())
			}
		})
	}
}

// TestLoadRetentionErrorMentionsVariable pins that a malformed REDIS_STREAM_RETENTION
// is logged rather than fatal: Load returns a Config with retention disabled (0)
// and the captured log mentions the offending variable so an operator can find
// the misconfiguration. A bad retention must not fail startup.
func TestLoadRetentionErrorMentionsVariable(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_STREAM_RETENTION", "bogus")

	logger, buf := testLogger()
	cfg := Load(logger)
	if cfg.StreamRetention != 0 {
		t.Fatalf("StreamRetention = %v, want 0 (disabled) on malformed value", cfg.StreamRetention)
	}
	if !strings.Contains(buf.String(), "REDIS_STREAM_RETENTION") {
		t.Fatalf("Load log does not mention REDIS_STREAM_RETENTION: %q", buf.String())
	}
}

// TestLoadFatalOnMissing is the subprocess-based test for the required-variable
// path. config.Load calls logger.Fatalf (os.Exit) when a required REDIS_*
// variable is missing, so it cannot be exercised in-process (t.Setenv cannot be
// combined with os.Exit — the test binary would die). Instead we re-exec the
// test binary in a subprocess (the standard Go pattern for os.Exit paths,
// https://go.dev/blog/os-exec) and assert the child exits non-zero with output
// naming the offending variable. Each test case sets a child-only marker env var
// that testLoadFatalOnMissingSubprocess reads on the other side.
func TestLoadFatalOnMissing(t *testing.T) {
	for _, tt := range []struct {
		missEnv string
		wantVar string
	}{
		{"REDIS_ADDR", "REDIS_ADDR"},
		{"REDIS_STREAM", "REDIS_STREAM"},
		{"REDIS_GROUP", "REDIS_GROUP"},
	} {
		t.Run(tt.missEnv, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestLoadFatalOnMissingSubprocess$")
			cmd.Env = append(os.Environ(), "RELAY_TEST_MISSING="+tt.missEnv)
			// The child writes its fatalf log (mentioning the var) to stderr via
			// log.New(os.Stderr, "", 0), so capture and assert both the exit
			// code and the message.
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("subprocess exited 0; want non-zero exit for missing %s", tt.missEnv)
			}
			if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() == 0 {
				t.Fatalf("subprocess error = %v, want non-zero exit", err)
			}
			if !strings.Contains(string(out), tt.wantVar) {
				t.Fatalf("subprocess output does not mention %s:\n%s", tt.wantVar, string(out))
			}
			if !strings.Contains(string(out), "not set") {
				t.Fatalf("subprocess output missing 'not set':\n%s", string(out))
			}
		})
	}
}

// TestLoadFatalOnMissingSubprocess is not a standalone test: it is the subprocess
// side of TestLoadFatalOnMissing (matched by -test.run=TestLoadFatalOnMissingSubprocess$).
// It reads RELAY_TEST_MISSING (set only in the child process), clears that required
// variable, and calls Load — which must call logger.Fatalf (process exit) naming
// the variable. The default os.Exit code from Fatalf is 1, so the parent asserts
// a non-zero exit. This function itself must never be run as a normal top-level
// test; the parent always runs it as a subprocess.
func TestLoadFatalOnMissingSubprocess(t *testing.T) {
	missVar := os.Getenv("RELAY_TEST_MISSING")
	// The parent never sets this in its own environment, so running this as a
	// plain test (not as the subprocess) has nothing to test.
	if missVar == "" {
		t.Skip("only meaningful as a Load subprocess (RELAY_TEST_MISSING unset)")
	}
	setRequiredEnv(t)
	// Clear the variable under test so requiredEnv sees it missing.
	t.Setenv(missVar, "")
	// Fatalf writes to stderr; log.New(os.Stderr, "", 0) routes the fatal line
	// where the parent's CombinedOutput captures it.
	Load(log.New(os.Stderr, "", 0))
	// Load should have exited via Fatalf; reaching here is a failure.
	t.Fatal("Load returned instead of calling logger.Fatalf on missing variable")
}

// TestConsumerNameResolvesToHostname proves Load() resolves the consumer name to
// the hostname on a normal test host. We compare against os.Hostname()
// directly; the two may legitimately differ only in weird environments where
// the hostname is empty, in which case we fail rather than skip (an empty
// hostname would be a fatal path in consumerNameFromHost, so a normal host must
// resolve it).
func TestConsumerNameResolvesToHostname(t *testing.T) {
	host, hostErr := os.Hostname()
	if hostErr != nil || host == "" {
		t.Fatalf("os.Hostname unavailable on this host: %v", hostErr)
	}
	setRequiredEnv(t)
	cfg := Load(log.New(io.Discard, "", 0))
	if cfg.ConsumerName != host {
		t.Fatalf("ConsumerName = %q, want hostname %q", cfg.ConsumerName, host)
	}
}

// NOTE on consumerNameFromHost failure paths: consumerNameFromHost now takes
// only a logger and calls os.Hostname internally, calling logger.Fatalf (process
// exit) on both a resolution error and an empty hostname. Those fatal paths are
// not injectable (os.Hostname cannot be stubbed), so they cannot be unit-tested
// in-process — unlike the old direct-args helper, there is no way to force the
// failure without exiting the test binary. Load's own missing-required-var fatal
// path is covered by the TestLoadFatalOnMissing subprocess test above; the
// hostname-fatal paths are deliberately left untested because no environment or
// argument can exercise them without stubbing os.Hostname.
