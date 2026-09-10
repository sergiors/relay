package cli

import (
	"errors"
	"strings"
	"testing"
)

// checkHealth reports the first failing check (redis first) and prints
// "healthy" only when both pass.
func TestCheckHealth(t *testing.T) {
	ok := func() error { return nil }
	fail := func() error { return errors.New("boom") }

	tests := []struct {
		name       string
		redis      func() error
		docker     func() error
		wantCode   int
		wantStderr string
		wantStdout string
	}{
		{"both pass", ok, ok, 0, "", "healthy\n"},
		{"redis fails", fail, ok, 1, "boom", ""},
		{"docker fails", ok, fail, 1, "boom", ""},
		{"both fail reports redis", fail, fail, 1, "boom", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			errOut := captureErr(t, func() {
				out := capture(t, func() {
					code = checkHealth(tt.redis, tt.docker)
				})
				if out != tt.wantStdout {
					t.Fatalf("stdout = %q, want %q", out, tt.wantStdout)
				}
			})
			if code != tt.wantCode {
				t.Fatalf("exit = %d, want %d", code, tt.wantCode)
			}
			if tt.wantStderr != "" && !strings.Contains(errOut, tt.wantStderr) {
				t.Fatalf("stderr missing %q: %q", tt.wantStderr, errOut)
			}
		})
	}
}

// TestHealthRedisConfigErrorRedactsCredentials pins that a malformed REDIS_ADDR
// DSN reported by `relay health` never leaks the password to stderr. The
// redis config error path (RedisOptions) is redacted, and this test exercises
// the full runHealthCommand path to guard the wiring end to end.
func TestHealthRedisConfigErrorRedactsCredentials(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis://default:s3cr3t-pw@:63799x")
	var code int
	errOut := captureErr(t, func() {
		code = runHealthCommand()
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if strings.Contains(errOut, "s3cr3t-pw") {
		t.Fatalf("stderr leaks password: %q", errOut)
	}
	if !strings.Contains(errOut, "redis config") {
		t.Fatalf("stderr missing redis config error: %q", errOut)
	}
}
