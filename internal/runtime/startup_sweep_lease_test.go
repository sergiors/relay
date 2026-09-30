package runtime

import (
	"context"
	"net/http"
	"testing"

	"relay/internal/testutil"
)

// TestRemoveImagesExceptSkipsLeasedImage pins the startup sweeps's admission
// respect: a stale Relay-owned image that an admitted lease still holds (a
// registry publication or an in-flight build) is NOT removed by the sweep — the
// non-blocking removal reports the retryable ErrImageRetiring, the sweep leaves
// it for the owning function's reconcile, and once the lease drains the same
// sweep removes it. This is the startup keep-set's runtime half: the keep-set
// names what must survive, and the lease gate is the second line of defense for
// anything the keep-set did not know about.
func TestRemoveImagesExceptSkipsLeasedImage(t *testing.T) {
	const held = "relay-fn-a:1111111111111111"
	deletes := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodGet, path: "/images/json", body: labeledImageListJSON(
			scriptedImage{tags: []string{held}, labels: functionLabels("a")},
		)},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { deletes++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	lease, err := m.AcquireImageLease(held)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	removed, err := m.RemoveImagesExcept(context.Background(), map[string]bool{})
	if err != nil {
		t.Fatalf("sweep with a leased image must not surface a genuine error: %v", err)
	}
	if removed != 0 || deletes != 0 {
		t.Fatalf("removed=%d deletes=%d, want 0/0 while the image is admitted", removed, deletes)
	}

	// The lease drains; the same sweep now removes the image exactly once.
	lease.Release()
	removed, err = m.RemoveImagesExcept(context.Background(), map[string]bool{})
	if err != nil {
		t.Fatalf("sweep after the lease drained: %v", err)
	}
	if removed != 1 || deletes != 1 {
		t.Fatalf("removed=%d deletes=%d, want 1/1 after the lease drained", removed, deletes)
	}
}
