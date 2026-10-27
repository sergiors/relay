package testutil

import (
	"strings"
	"testing"
)

// TestDatabaseEndpointRedactsCredentials pins that a missing-database
// diagnostic can name the server without echoing the DSN. Keyword/value DSNs
// carry credentials inline, so they must fall back to a generic label.
func TestDatabaseEndpointRedactsCredentials(t *testing.T) {
	const fallback = "the configured PostgreSQL server"
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "URL with port",
			dsn:  "postgres://relay:s3cret@db.example.com:5433/relay",
			want: "db.example.com:5433",
		},
		{
			name: "URL with default port",
			dsn:  "postgresql://relay:s3cret@db.example.com/relay",
			want: "db.example.com:5432",
		},
		{
			name: "IPv6 URL",
			dsn:  "postgres://relay:s3cret@[2001:db8::1]/relay",
			want: "[2001:db8::1]:5432",
		},
		{
			name: "numeric port out of range",
			dsn:  "postgres://relay:s3cret@db.example.com:99999/relay",
			want: fallback,
		},
		{
			name: "URL without host",
			dsn:  "postgres://relay:s3cret@:5432/relay",
			want: fallback,
		},
		{
			name: "keyword/value DSN",
			dsn:  "host=db.example.com user=relay password=s3cret dbname=relay",
			want: fallback,
		},
		{
			name: "malformed URL",
			dsn:  "postgres://relay:s3cret@db.example.com:not-a-port/relay",
			want: fallback,
		},
		{
			name: "non-postgres scheme",
			dsn:  "mysql://relay:s3cret@db.example.com:3306/relay",
			want: fallback,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := databaseEndpoint(tc.dsn)
			if got != tc.want {
				t.Fatalf("databaseEndpoint = %q, want %q", got, tc.want)
			}
			for _, secret := range []string{"s3cret", "password=", "@"} {
				if strings.Contains(got, secret) {
					t.Fatalf("databaseEndpoint leaked %q in %q", secret, got)
				}
			}
		})
	}
}

// TestRedisEndpointRedactsCredentials pins the same contract for Redis
// diagnostics: a URL contributes its host, while a plain host:port passes
// through unchanged.
func TestRedisEndpointRedactsCredentials(t *testing.T) {
	const fallback = "the configured Redis server"
	cases := []struct {
		name string
		addr string
		want string
	}{
		{
			name: "plain host:port",
			addr: "localhost:6379",
			want: "localhost:6379",
		},
		{
			name: "redis URL",
			addr: "redis://default:s3cret@redis.example.com:6380/0",
			want: "redis.example.com:6380",
		},
		{
			name: "rediss IPv6 URL",
			addr: "rediss://:s3cret@[2001:db8::1]:6379",
			want: "[2001:db8::1]:6379",
		},
		{
			name: "malformed URL",
			addr: "redis://default:s3cret@[::1",
			want: fallback,
		},
		{
			name: "URL without host",
			addr: "redis://",
			want: fallback,
		},
		{
			name: "malformed plain address with userinfo",
			addr: "default:s3cret@redis.example.com:6379",
			want: fallback,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redisEndpoint(tc.addr)
			if got != tc.want {
				t.Fatalf("redisEndpoint = %q, want %q", got, tc.want)
			}
			for _, secret := range []string{"s3cret", "@"} {
				if strings.Contains(got, secret) {
					t.Fatalf("redisEndpoint leaked %q in %q", secret, got)
				}
			}
		})
	}
}

// TestRedactEndpointCredentials covers driver errors that quote the endpoint:
// Docker includes DOCKER_HOST verbatim, so a tcp:// DSN with userinfo would
// otherwise put a password in the failure message.
func TestRedactEndpointCredentials(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{
			name: "docker ping error",
			text: "Cannot connect to the Docker daemon at " +
				"tcp://user:s3cret@127.0.0.1:1. Is the docker daemon running?",
			want: "Cannot connect to the Docker daemon at " +
				"tcp://127.0.0.1:1. Is the docker daemon running?",
		},
		{
			name: "docker client parse error",
			text: "unable to parse docker host `tcp://user:s3cret@not a url`",
			want: "unable to parse docker host `tcp://not a url`",
		},
		{
			name: "malformed userinfo containing slash",
			text: "unable to parse docker host `tcp://user:pa/ss@not a url`",
			want: "unable to parse docker host `tcp://not a url`",
		},
		{
			name: "unix socket unchanged",
			text: "failed to connect at unix:///var/run/docker.sock",
			want: "failed to connect at unix:///var/run/docker.sock",
		},
		{
			name: "plain text unchanged",
			text: "connect: connection refused",
			want: "connect: connection refused",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactEndpointCredentials(tc.text)
			if got != tc.want {
				t.Fatalf("redactEndpointCredentials = %q, want %q", got, tc.want)
			}
			if strings.Contains(got, "s3cret") || strings.Contains(got, "user:") {
				t.Fatalf("redactEndpointCredentials leaked credentials in %q", got)
			}
		})
	}
}
