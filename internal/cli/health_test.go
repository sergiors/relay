package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
)

// checkHealth reports the first failing check (redis first) and writes
// "healthy" only when both pass.
func TestCheckHealth(t *testing.T) {
	ok := func() error { return nil }
	fail := func() error { return errors.New("boom") }

	tests := []struct {
		name    string
		redis   func() error
		docker  func() error
		wantErr bool
		wantOut string
	}{
		{"both pass", ok, ok, false, "healthy\n"},
		{"redis fails", fail, ok, true, ""},
		{"docker fails", ok, fail, true, ""},
		{"both fail reports redis", fail, fail, true, ""},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			var w bytes.Buffer
			err := checkHealth(&w, c.redis, c.docker)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, c.wantErr)
			}
			if w.String() != c.wantOut {
				t.Fatalf("stdout = %q, want %q", w.String(), c.wantOut)
			}
			if c.wantErr && err.Error() != "boom" {
				t.Fatalf("err = %q, want %q", err.Error(), "boom")
			}
		})
	}
}

// TestHealthRedisConfigErrorRedactsCredentials pins that a malformed REDIS_URI
// DSN reported by `relay health` never leaks the password. The redis config
// error path (RedisOptions) is redacted, and this test exercises the full
// runHealthCommand path to guard the wiring end to end.
func TestHealthRedisConfigErrorRedactsCredentials(t *testing.T) {
	// runHealthCommand loads full config via config.Load(logger), which exits
	// via logger.Fatalf when a required REDIS_* variable is missing, so set all
	// three required variables. REDIS_STREAM/REDIS_GROUP only need to be
	// non-empty; the malformed REDIS_URI DSN below is what drives the
	// RedisOptions failure after Load succeeds (the password-redaction
	// assertion works because RedisOptions(cfg.RedisURI) rejects the bad DSN).
	t.Setenv("REDIS_URI", "redis://default:s3cr3t-pw@:63799x")
	t.Setenv("REDIS_STREAM", "stream")
	t.Setenv("REDIS_GROUP", "group")
	logger := log.New(io.Discard, "", 0)

	var w bytes.Buffer
	err := runHealthCommand(context.Background(), &w, logger)
	if err == nil {
		t.Fatal("expected an error from a malformed REDIS_URI")
	}
	if strings.Contains(err.Error(), "s3cr3t-pw") {
		t.Fatalf("error leaks password: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "redis config") {
		t.Fatalf("error missing redis config message: %q", err.Error())
	}
}

// The health command exits 2 on extra args.
func TestHealthArgError(t *testing.T) {
	_, _, err := runCLI(t, "", "health", "extra")
	if err == nil || !strings.Contains(err.Error(), "health: too many arguments") {
		t.Fatalf("returned error missing usage error: %v", err)
	}
}
