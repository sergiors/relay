package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// newRetireManager builds a Manager whose Docker client is the scripted one and
// whose logger is discarded, so the authority-aware removal paths can be
// exercised against the real gate without a daemon.
func newRetireManager(t *testing.T, routes ...dockerRoute) *Manager {
	t.Helper()
	cli := newScriptedDockerClient(t, routes...)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), containers: newContainerCache()}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// retirementSignal is a deterministic synchronization point on the manager's
// image lease coordinator: it closes a channel the instant a manager-owned
// removal commits image's retirement (the moment the gate is established). It
// replaces polling/time.Sleep-based "wait until retiring" assertions.
type retirementSignal struct {
	once  sync.Once
	image string
	ch    chan struct{}
}

// signalOnRetirement installs the test-only coordinator hook and returns a
// signal that fires when image's retirement gate is first established. It must
// be called before the removal goroutine is started.
func signalOnRetirement(m *Manager, image string) *retirementSignal {
	s := &retirementSignal{image: image, ch: make(chan struct{})}
	m.leaseCoord().setTestHooks(&coordinatorHooks{
		retireEntered: func(got string) {
			if got == image {
				s.once.Do(func() { close(s.ch) })
			}
		},
	})
	return s
}

// wait blocks until the retirement gate has been established. Its timeout is a
// deadlock failure bound only: on success the signal is delivered by the
// coordinator at the exact commit, never by a delay.
func (s *retirementSignal) wait(t *testing.T) {
	t.Helper()
	select {
	case <-s.ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("image %s's retirement gate was never established", s.image)
	}
}

// TestRetireImageHeldByColdExecutionLease pins the central TOCTOU closure: an
// execution's own admitted lease blocks removal until it drains, and only then
// does RemoveImage issue the delete.
func TestRetireImageHeldByColdExecutionLease(t *testing.T) {
	dels := 0
	m := newRetireManager(t,
		// RemoveImage's container-reference guard lists containers (none).
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)

	// A direct execution acquires its own lease (as Manager.Execute does when no
	// lease is on ctx).
	lease, err := m.AcquireImageLease("relay-app-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	signal := signalOnRetirement(m, "relay-app-a:v1")
	done := make(chan error, 1)
	go func() { done <- m.RemoveImage(context.Background(), "relay-app-a:v1") }()

	// Wait deterministically until the removal owns the retirement gate. The
	// gate commits before the drain wait, so at this exact point the removal is
	// blocked on the held lease and has issued no DELETE. No delay-based proof
	// is needed: the held lease is what keeps the drain open.
	signal.wait(t)
	if !m.IsImageRetiring("relay-app-a:v1") {
		t.Fatal("the gate must be established once the removal owns it")
	}
	if dels != 0 {
		t.Fatalf("DELETE issued while lease held: %d", dels)
	}
	select {
	case err := <-done:
		t.Fatalf("removal completed while an execution lease was held: %v", err)
	default:
	}

	lease.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("remove after lease release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removal did not proceed after the execution lease released")
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want 1", dels)
	}
}

// TestRetireImageRemovalFailureRetryable pins that a failed ImageRemove clears
// the retirement gate, so the image is reusable and a later pass can retry.
func TestRetireImageRemovalFailureRetryable(t *testing.T) {
	attempts := 0
	m := newRetireManager(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", status: http.StatusInternalServerError,
			body: `{"message":"boom"}`, onMatch: func() { attempts++ }},
	)

	if err := m.RemoveImage(context.Background(), "relay-app-a:v1"); err == nil {
		t.Fatal("expected the failed removal to surface")
	}
	if attempts != 1 {
		t.Fatalf("DELETE attempts = %d, want 1", attempts)
	}
	// The failed removal cleared the gate: the image is retiring no more and can
	// be leased again.
	if m.IsImageRetiring("relay-app-a:v1") {
		t.Fatal("a failed removal must clear the retirement gate")
	}
	if _, err := m.AcquireImageLease("relay-app-a:v1"); err != nil {
		t.Fatalf("image must be reusable after a failed removal, got %v", err)
	}
}

// TestRetireImageDuplicateIsDeferred pins that a second, concurrent retirement
// of the same image reports the retryable ErrImageRetiring and never issues a
// second DELETE (the first owner performs the single removal).
func TestRetireImageDuplicateIsDeferred(t *testing.T) {
	dels := 0
	m := newRetireManager(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)

	// Hold a lease so the first retirement is still draining when the second
	// arrives.
	lease, err := m.AcquireImageLease("relay-app-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	signal := signalOnRetirement(m, "relay-app-a:v1")
	first := make(chan error, 1)
	go func() { first <- m.RemoveImage(context.Background(), "relay-app-a:v1") }()

	// Wait deterministically until the first retirement owns the gate. The held
	// lease then guarantees it stays blocked, so the duplicate below observes
	// the committed retirement rather than racing the first removal.
	signal.wait(t)

	// The duplicate is rejected immediately as retryable.
	err = m.RemoveImage(context.Background(), "relay-app-a:v1")
	if !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("duplicate removal = %v, want wrapped ErrImageRetiring", err)
	}

	lease.Release()
	if err := <-first; err != nil {
		t.Fatalf("first removal: %v", err)
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want exactly 1", dels)
	}
}

// TestRetireExternalImageUntouched pins the ownership guard: an external service
// image is never leased, retired, or deleted by Relay.
func TestRetireExternalImageUntouched(t *testing.T) {
	const ext = "ghcr.io/acme/api:1.2"
	m := newRetireManager(t)

	if IsRelayImage(ext) {
		t.Fatalf("IsRelayImage(%q) must be false", ext)
	}
	// Acquiring a lease for a non-Relay image returns nil (nothing to pin).
	lease, err := m.AcquireImageLease(ext)
	if err != nil || lease != nil {
		t.Fatalf("AcquireImageLease(external) = (%v, %v), want (nil, nil)", lease, err)
	}
}
