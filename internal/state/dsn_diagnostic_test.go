package state

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// databaseEndpointFallback is the non-secret description used when the DSN is
// not a well-formed postgres URL. Keyword/value DSNs carry credentials inline
// (password=...), so the raw input must never be echoed.
const databaseEndpointFallback = "the configured PostgreSQL server"

// safeDatabaseEndpoint renders the configured server for diagnostics without
// credentials. Only a valid postgres:// or postgresql:// URL contributes a
// host:port; keyword/value and malformed inputs get a generic label.
func safeDatabaseEndpoint(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return databaseEndpointFallback
	}
	switch strings.ToLower(u.Scheme) {
	case "postgres", "postgresql":
	default:
		return databaseEndpointFallback
	}
	host := u.Hostname()
	if host == "" {
		return databaseEndpointFallback
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return databaseEndpointFallback
	}
	return net.JoinHostPort(host, port)
}

// TestSafeDatabaseEndpoint pins that diagnostics identify a valid server
// without ever echoing credentials or the raw DSN, including for keyword/value
// DSNs that have no URL structure.
func TestSafeDatabaseEndpoint(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "postgres URL",
			dsn:  "postgres://relay:s3cret@db.example.com:5433/relay?sslmode=disable",
			want: "db.example.com:5433",
		},
		{
			name: "postgresql URL with default port",
			dsn:  "postgresql://relay:s3cret@[2001:db8::1]/relay",
			want: "[2001:db8::1]:5432",
		},
		{
			name: "keyword/value DSN",
			dsn:  "host=db.example.com port=5433 user=relay password=s3cret dbname=relay",
			want: databaseEndpointFallback,
		},
		{
			name: "malformed URL",
			dsn:  "postgres://relay:s3cret@db.example.com:not-a-port/relay",
			want: databaseEndpointFallback,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := safeDatabaseEndpoint(tc.dsn)
			if got != tc.want {
				t.Fatalf("safeDatabaseEndpoint = %q, want %q", got, tc.want)
			}
			if strings.Contains(got, tc.dsn) {
				t.Fatalf("safeDatabaseEndpoint echoed the raw DSN: %q", got)
			}
			for _, secret := range []string{"s3cret", "password=", "@"} {
				if strings.Contains(got, secret) {
					t.Fatalf("safeDatabaseEndpoint leaked %q in %q", secret, got)
				}
			}
		})
	}
}
