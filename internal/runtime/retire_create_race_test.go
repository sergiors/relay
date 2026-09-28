package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestExecuteContainerCreateVsRetire is the deterministic proof of the
// "container creation vs retire" race: an invocation admitted BEFORE an image's
// retirement holds its admitted image lease across the cold-container
// /containers/create it issues, so removal commits the retirement gate but can
// never issue the ImageRemove while that create is in flight. Only once the
// create concludes (here with a scripted error, which makes Execute exit and
// release its lease) does the removal's drain close and the DELETE run.
//
// Every wait below is a channel signaled at the exact production transition (the
// coordinator's retire-entered hook, and the scripted daemon's request entry);
// the time.After cases are deadlock failure bounds only, never a delay used as
// synchronization. There are no sleeps.
func TestExecuteContainerCreateVsRetire(t *testing.T) {
	const image = "relay-fn-a:v1"

	// The /containers/create route signals that the cold start reached the
	// daemon, then blocks until the test releases it (or its request context is
	// cancelled, so a wedged test can never leak the goroutine past its bound).
	createEntered := make(chan struct{})
	createRelease := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	releaseCreate := func() { releaseOnce.Do(func() { close(createRelease) }) }
	// Ensure a mid-test Fatalf still unblocks the create goroutine before the
	// manager's cleanup closes the Docker client.
	defer releaseCreate()

	createErr := errors.New("scripted container create failure")
	dels := 0
	m := newRetireManager(t,
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			onRequest: func(req *http.Request) {
				enteredOnce.Do(func() { close(createEntered) })
				select {
				case <-createRelease:
				case <-req.Context().Done():
				}
			},
			fail: func(*http.Request) error { return createErr },
		},
		// RemoveImage's container-reference guard lists no containers, and the
		// pre-create identity resolution inspects the image (absent here, so it
		// degrades to the reference identity and the create proceeds).
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodGet, path: "/images/relay-fn-a:v1/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)

	prepared := &Prepared{Name: "a", Image: image}
	execDone := make(chan error, 1)
	go func() { execDone <- m.Execute(context.Background(), prepared, "h", nil, nil) }()

	// The invocation's cold container creation is now parked inside the daemon
	// call, holding its admitted image lease.
	select {
	case <-createEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("container create was never entered")
	}
	// Deterministic proof that the execution's admitted lease is held across the
	// blocked create: the reference count is exactly the one Execute acquired.
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("admitted image lease count during blocked create = %d, want 1 (Execute must pin it across the create)", got)
	}

	// Start removal and wait for the exact instant it owns the retirement gate.
	signal := signalOnRetirement(m, image)
	removeDone := make(chan error, 1)
	go func() { removeDone <- m.RemoveImage(context.Background(), image) }()
	signal.wait(t)

	if !m.IsImageRetiring(image) {
		t.Fatal("the removal must own the retirement gate once it is entered")
	}
	// The gate is open and the execution's lease is still the sole admitted
	// reference, so the drain cannot have closed and the DELETE path is
	// structurally unreachable until that lease releases.
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("admitted image lease count at the retirement gate = %d, want 1 (the removal must be blocked on the drain)", got)
	}
	if dels != 0 {
		t.Fatalf("ImageRemove issued while create was blocked: %d", dels)
	}
	// The create's admitted lease keeps the drain open, so removal is parked and
	// Execute is still parked in create.
	select {
	case err := <-removeDone:
		t.Fatalf("removal completed while create was blocked: %v", err)
	default:
	}
	select {
	case err := <-execDone:
		t.Fatalf("Execute returned while create was still blocked: %v", err)
	default:
	}

	// Release create with a scripted error: Execute exits and its deferred lease
	// release lets the removal's drain close.
	releaseCreate()
	select {
	case err := <-execDone:
		if err == nil {
			t.Fatal("Execute must fail when the scripted container create fails")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not return after the create was released")
	}

	// Removal can now proceed and issues exactly one DELETE.
	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatalf("remove after Execute released its lease: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removal did not proceed after Execute released its lease")
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want 1", dels)
	}
	if m.IsImageRetiring(image) {
		t.Fatal("a completed removal must clear the retirement gate")
	}
}
