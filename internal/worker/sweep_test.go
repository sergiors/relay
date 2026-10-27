package worker

import (
	"testing"
)

// TestStartupImageKeepSet pins the startup sweep's keep-set policy without
// Docker: the exact image each app was prepared with this boot, every
// running service container's image, and every recorded last-active image are
// kept; blanks are ignored. It takes prepared refs verbatim, so no source tree
// is re-hashed to reconstruct an expected tag.
func TestStartupImageKeepSet(t *testing.T) {
	// An image ref that is deliberately NOT derivable by hashing a tree: the
	// keep-set must retain exactly what it is given rather than recomputing a
	// tag.
	const preparedImg = "relay-app-fn:deadbeefdeadbeef"
	keep := startupImageKeepSet(
		[]string{preparedImg, ""},
		[]string{"svc-img", ""},
		[]string{"recorded-img", ""},
	)

	for _, want := range []string{preparedImg, "svc-img", "recorded-img"} {
		if !keep[want] {
			t.Errorf("keep set missing %q: %v", want, keep)
		}
	}
	if keep[""] {
		t.Errorf("keep set must not contain a blank image: %v", keep)
	}
	if len(keep) != 3 {
		t.Errorf("keep set = %v, want exactly 3 entries", keep)
	}
}

// TestStartupImageKeepSetDoesNotRehashSource is the regression for the
// startup-sweep optimization: the keep-set takes the ACTUAL prepared image
// references and never touches the filesystem, so it stays correct even for a
// app whose directory has since vanished. An app that failed to build
// contributes no prepared image (its still-serving version is covered by the
// recorded state image instead).
func TestStartupImageKeepSetDoesNotRehashSource(t *testing.T) {
	keep := startupImageKeepSet(
		[]string{"relay-app-gone:0123456789abcdef"},
		nil,
		[]string{"relay-app-gone:previousserving"},
	)
	if !keep["relay-app-gone:0123456789abcdef"] {
		t.Fatalf("prepared image not kept: %v", keep)
	}
	if !keep["relay-app-gone:previousserving"] {
		t.Fatalf("recorded image not kept: %v", keep)
	}
	if len(keep) != 2 {
		t.Fatalf("keep set = %v, want exactly the two supplied refs", keep)
	}
}

// TestStartupImageKeepSetEmptyInputs verifies an unavailable/no-runtime boot
// with no services and no recorded state contributes no keep entry.
func TestStartupImageKeepSetEmptyInputs(t *testing.T) {
	keep := startupImageKeepSet(nil, nil, nil)
	if len(keep) != 0 {
		t.Fatalf("keep set = %v, want empty", keep)
	}
}
