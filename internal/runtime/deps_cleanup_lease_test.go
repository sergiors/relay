package runtime

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"relay/internal/testutil"
)

// depGCImageListJSON renders an image list with one unreferenced managed
// dependency image (relay.type=dependency, tagged relay-dep-x).
func depGCImageListJSON(dep string) string {
	return `[{"RepoTags":["` + dep + `"],"Id":"sha256:deadbeef","Labels":{"relay.type":"dependency","relay.runtime":"python3.14"}}]`
}

// TestDependencyGCSkipsWhileLeaseHeld pins the TOCTOU closure for dependency
// consumption: while an active Prepare holds a dependency lease (admitted before
// its FROM build), dependency GC must NOT remove the layer.
func TestDependencyGCSkipsWhileLeaseHeld(t *testing.T) {
	dels := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/json", body: depGCImageListJSON("relay-dep-aaaaaaaaaaaaaaaa")},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	// An active Prepare holds the dependency lease.
	lease, err := m.AcquireImageLease("relay-dep-aaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	removed, err := m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC with held lease must not surface a genuine error: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0 while a dependency lease is held", removed)
	}
	if dels != 0 {
		t.Fatalf("DELETE issued while a dependency lease was held: %d", dels)
	}
	// The skipped candidate cleared its gate: it must be reusable concurrently.
	again, err := m.AcquireImageLease("relay-dep-aaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("dependency must be reusable after a skipped GC, got %v", err)
	}
	again.Release()

	// The build consumes and releases its lease; GC now removes the layer.
	lease.Release()
	removed, err = m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC after lease release: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1 after the dependency lease released", removed)
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want 1", dels)
	}
}

// TestDependencyGCFailedRemoveRetryable pins that a failed dependency removal
// clears the gate so the layer stays usable and a later pass retries.
func TestDependencyGCFailedRemoveRetryable(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/json", body: depGCImageListJSON("relay-dep-bbbbbbbbbbbbbbbb")},
		dockerRoute{method: http.MethodDelete, path: "/images/", status: http.StatusInternalServerError, body: `{"message":"boom"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	if _, err := m.CleanupUnusedDependencies(context.Background()); err == nil {
		t.Fatal("expected the failed removal to surface")
	}
	if m.IsImageRetiring("relay-dep-bbbbbbbbbbbbbbbb") {
		t.Fatal("a failed dependency removal must clear the retirement gate")
	}
	if _, err := m.AcquireImageLease("relay-dep-bbbbbbbbbbbbbbbb"); err != nil {
		t.Fatalf("dependency must be reusable after a failed removal, got %v", err)
	}
}

// TestDependencyGCDuplicateRetireDefers pins that a concurrent retirement of the
// same dependency is reported retryable and never issues a second DELETE.
func TestDependencyGCDuplicateRetireDefers(t *testing.T) {
	cli := newScriptedDockerClient(t)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	// Commit the retirement by hand; GC's attempt must defer.
	m.leaseCoord().beginRetire("relay-dep-cccccccccccccccc")
	if err := m.removeUnreferencedDependencyImage(context.Background(), "relay-dep-cccccccccccccccc"); !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("duplicate dependency retire = %v, want ErrImageRetiring", err)
	}
}
