package testutil

import (
	"strings"
	"testing"

	"relay/internal/function"
)

// TestUniqueRespectsFunctionNameCap pins Unique's output to function.ValidName
// for a very long raw test name, the exact shape that broke the integration
// suite when the old construction appended a stamp without truncating.
func TestUniqueRespectsFunctionNameCap(t *testing.T) {
	long := "TestIntegrationGracefulShutdownMidHandler/with-a-deeply/nested/subtest-name-that-is-far-too-long"
	got := Unique("shutdown", long)

	if len(got) > nameMaxLen {
		t.Fatalf("Unique() = %q (%d chars), want <= %d", got, len(got), nameMaxLen)
	}
	if err := function.ValidName(got); err != nil {
		t.Fatalf("Unique() = %q is not a valid function name: %v", got, err)
	}
	if !strings.HasPrefix(got, "shutdown-") {
		t.Fatalf("Unique() = %q, want prefix %q", got, "shutdown-")
	}
}

// TestUniqueIsSanitizedAndStamped verifies lowercase/[a-z0-9._-] sanitization
// and the trailing decimal nanosecond stamp.
func TestUniqueIsSanitizedAndStamped(t *testing.T) {
	got := Unique("recon", "Test Thing/With Spaces")
	if err := function.ValidName(got); err != nil {
		t.Fatalf("Unique() = %q is not a valid function name: %v", got, err)
	}
	if !strings.HasPrefix(got, "recon-test-thing-with-spaces-") {
		t.Fatalf("Unique() = %q, want sanitized name component in the result", got)
	}
	stamp := strings.TrimPrefix(got, "recon-test-thing-with-spaces-")
	if len(stamp) != 19 || strings.Trim(stamp, "0123456789") != "" {
		t.Fatalf("Unique() = %q, want a 19-digit nanosecond stamp suffix, got %q", got, stamp)
	}
}

// TestUniqueEmptySanitizedComponent covers a raw name that sanitizes to nothing
// usable (all separators trimmed), which must still yield a valid name.
func TestUniqueEmptySanitizedComponent(t *testing.T) {
	got := Unique("svc", "-")
	if err := function.ValidName(got); err != nil {
		t.Fatalf("Unique() = %q is not a valid function name: %v", got, err)
	}
}

// TestUniqueNameMatchesUnique keeps the *testing.T and pure variants in sync.
func TestUniqueNameMatchesUnique(t *testing.T) {
	got := UniqueName(t, "recon")
	if err := function.ValidName(got); err != nil {
		t.Fatalf("UniqueName() = %q is not a valid function name: %v", got, err)
	}
	if !strings.HasPrefix(got, "recon-"+sanitizeNameComponent(t.Name())+"-") {
		t.Fatalf("UniqueName() = %q, want the recon-<t.Name()>-<stamp> shape", got)
	}
}
