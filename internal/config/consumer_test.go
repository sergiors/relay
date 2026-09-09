package config

import (
	"errors"
	"os"
	"testing"
)

// TestConsumerNameIsHostname proves ConsumerName() returns the hostname on a
// normal test host. We compare against os.Hostname() directly; the two may
// legitimately differ only in weird environments where the hostname is empty,
// in which case we skip.
func TestConsumerNameIsHostname(t *testing.T) {
	host, hostErr := os.Hostname()
	if hostErr != nil || host == "" {
		t.Skipf("os.Hostname unavailable on this host: %v", hostErr)
	}
	got, err := ConsumerName()
	if err != nil {
		t.Fatalf("ConsumerName() error = %v, want nil", err)
	}
	if got != host {
		t.Fatalf("ConsumerName() = %q, want hostname %q", got, host)
	}
}

// TestConsumerNameRejectsEmptyHostname covers the failure paths of
// consumerNameFromHost directly: os.Hostname is not injectable, so the pure
// helper is unit-tested instead of the thin os.Hostname wrapper.
func TestConsumerNameRejectsEmptyHostname(t *testing.T) {
	// Empty hostname with no error is rejected.
	if _, err := consumerNameFromHost("", nil); err == nil {
		t.Fatal("consumerNameFromHost(\"\", nil) error = nil, want an error")
	}

	// A valid hostname passes through unchanged.
	got, err := consumerNameFromHost("web-1", nil)
	if err != nil {
		t.Fatalf("consumerNameFromHost(\"web-1\", nil) error = %v, want nil", err)
	}
	if got != "web-1" {
		t.Fatalf("consumerNameFromHost(\"web-1\", nil) = %q, want %q", got, "web-1")
	}

	// A hostname resolution error is wrapped.
	boom := errors.New("boom")
	if _, err := consumerNameFromHost("", boom); err == nil {
		t.Fatal("consumerNameFromHost(\"\", boom) error = nil, want a wrapped error")
	}
}

// TestConsumerNameStablePerProcess asserts ConsumerName() returns the same
// non-empty value on repeated calls within the process (stability for the
// process lifetime). Distinctness across replicas is NOT testable in-process:
// it comes from platform-unique hostnames (Docker/K8s give each container/pod
// a unique hostname), which os.Hostname() cannot vary within a single test
// process. What IS testable is that the identity is stable and non-empty.
func TestConsumerNameStablePerProcess(t *testing.T) {
	first, err := ConsumerName()
	if err != nil {
		t.Fatalf("ConsumerName() error = %v, want nil", err)
	}
	if first == "" {
		t.Fatal("ConsumerName() = \"\", want a non-empty hostname")
	}
	for i := 0; i < 5; i++ {
		got, err := ConsumerName()
		if err != nil {
			t.Fatalf("ConsumerName() call %d error = %v, want nil", i, err)
		}
		if got != first {
			t.Fatalf("ConsumerName() call %d = %q, want stable %q", i, got, first)
		}
	}
}
