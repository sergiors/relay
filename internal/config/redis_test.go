package config

import (
	"strings"
	"testing"
)

// TestRedisOptions covers the two accepted REDIS_ADDR forms: a plain
// host:port address (backward compatible) and a redis(s):// DSN. DSNs are
// parsed with redis.ParseURL, so the resulting options carry the parsed
// username, password, and (for rediss) TLS config.
func TestRedisOptions(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		wantAddr string
		wantUser string
		wantPass string
		wantTLS  bool
		wantErr  bool
	}{
		{"plain redis:6379", "redis:6379", "redis:6379", "", "", false, false},
		{"plain localhost:6379", "localhost:6379", "localhost:6379", "", "", false, false},
		{"redis dsn", "redis://default:password@host:6379", "host:6379", "default", "password", false, false},
		{"rediss dsn tls", "rediss://default:password@host:6379", "host:6379", "default", "password", true, false},
		{"invalid dsn", "redis://[::1", "", "", "", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("REDIS_ADDR", tt.addr)
			opts, err := RedisOptions()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("RedisOptions(%q) = nil error, want error", tt.addr)
				}
				return
			}
			if err != nil {
				t.Fatalf("RedisOptions(%q) error: %v", tt.addr, err)
			}
			if opts.Addr != tt.wantAddr {
				t.Errorf("Addr = %q, want %q", opts.Addr, tt.wantAddr)
			}
			if opts.Username != tt.wantUser {
				t.Errorf("Username = %q, want %q", opts.Username, tt.wantUser)
			}
			if opts.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", opts.Password, tt.wantPass)
			}
			if (opts.TLSConfig != nil) != tt.wantTLS {
				t.Errorf("TLSConfig non-nil = %v, want %v", opts.TLSConfig != nil, tt.wantTLS)
			}
		})
	}
}

// TestRedisOptionsErrorRedactsCredentials pins that a malformed DSN error never
// leaks the password. redis.ParseURL's own error text echoes the full URL
// (including userinfo), so RedisOptions must return a redacted message. The
// assertion documents the intent and guards against a regression that would
// print credentials to logs.
func TestRedisOptionsErrorRedactsCredentials(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis://default:s3cr3t-pw@:63799x")
	_, err := RedisOptions()
	if err == nil {
		t.Fatal("RedisOptions() = nil error, want error")
	}
	if strings.Contains(err.Error(), "s3cr3t-pw") {
		t.Fatalf("error leaks password: %q", err.Error())
	}
}

// TestRedisOptionsUnsetPanics pins the fail-fast behavior: an unset REDIS_ADDR
// panics via MustEnv with a clear message rather than returning a zero-value
// client.
func TestRedisOptionsUnsetPanics(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("RedisOptions() with empty REDIS_ADDR did not panic")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "REDIS_ADDR") {
			t.Fatalf("panic message = %v, want mention of REDIS_ADDR", r)
		}
	}()
	_, _ = RedisOptions()
}
