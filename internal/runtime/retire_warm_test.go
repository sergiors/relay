package runtime

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"relay/internal/testutil"
)

// TestWarmBusyContainerRetiredDuringImageRemoval pins the warm-container half of
// image retirement: while an execution holds the image's lease, removal blocks;
// InvalidateImage retires the busy warm container (idle discarded now, busy on
// release) without blocking; when the execution releases its image lease the
// busy container is discarded and the image removal proceeds. It uses the real
// container cache (with a fake container) and the scripted DELETE path.
func TestWarmBusyContainerRetiredDuringImageRemoval(t *testing.T) {
	dels := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), containers: newContainerCache()}
	t.Cleanup(func() { _ = m.Close() })

	// An execution holds the image lease (as Manager.Execute does) and is
	// running on a leased warm container.
	execLease, err := m.AcquireImageLease("relay-app-a:v1")
	if err != nil {
		t.Fatalf("acquire image lease: %v", err)
	}

	releaseContainer := make(chan struct{})
	entered := make(chan struct{})
	fake := &fakeContainer{entered: entered, release: releaseContainer}
	lease, err := m.containers.acquire(context.Background(), "a", "relay-app-a:v1", 1, func() (reusableContainer, error) {
		return fake, nil
	})
	if err != nil {
		t.Fatalf("acquire container lease: %v", err)
	}

	// Commit removal: it must block on the execution's image lease. Wait for the
	// gate deterministically, then prove it is blocked (the held execution lease
	// keeps the drain open) without a delay-based assertion.
	signal := signalOnRetirement(m, "relay-app-a:v1")
	done := make(chan error, 1)
	go func() { done <- m.RemoveImage(context.Background(), "relay-app-a:v1") }()
	signal.wait(t)
	select {
	case err := <-done:
		t.Fatalf("removal completed while an execution lease was held: %v", err)
	default:
	}

	// Retirement invalidates the image's warm containers without blocking; the
	// busy container is marked retired and discarded on release.
	m.InvalidateImage("relay-app-a:v1")

	// The execution ends: release the container lease (discards the retired
	// busy container) and the image lease (lets the removal drain).
	lease.release()
	if !fake.dead() {
		t.Fatal("a retired busy container must be discarded on release")
	}
	execLease.Release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("remove after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removal did not proceed after the execution released its lease")
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want 1", dels)
	}
}

// TestRemoveImageNowSkipsWhileLeaseHeld pins the non-blocking removal used by the
// startup sweep and service retirement: it never blocks on a held lease.
func TestRemoveImageNowSkipsWhileLeaseHeld(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}
	lease, err := m.AcquireImageLease("relay-app-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release()

	err = m.RemoveImageNow(context.Background(), "relay-app-a:v1")
	if !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("RemoveImageNow while leased = %v, want ErrImageRetiring", err)
	}
	if m.IsImageRetiring("relay-app-a:v1") {
		t.Fatal("a skipped non-blocking removal must clear the retirement gate")
	}
}
