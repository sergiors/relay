package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestExecuteCarriedLeaseFromAnotherManagerAcquiresOwn pins that a lease minted
// by a DIFFERENT manager's coordinator is not accepted as an entitlement: it does
// not protect this manager's image, whose removal this manager's gate governs, so
// Execute acquires its own reference from the executing manager's coordinator.
func TestExecuteCarriedLeaseFromAnotherManagerAcquiresOwn(t *testing.T) {
	const image = "relay-fn-a:v1"

	foreign := &Manager{}

	foreignLease, err := foreign.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire foreign lease: %v", err)
	}
	defer foreignLease.Release()

	var ownDuringCreate int
	m := newEntitlementManager(t, func(mgr *Manager, _ resolvedImage) (reusableContainer, error) {
		ownDuringCreate = mgr.LeaseCount(image)
		return &fakeContainer{}, nil
	})

	if err := m.Execute(
		WithImageLease(context.Background(), foreignLease),
		&Prepared{Name: "a", Image: image, Concurrency: 1},
		"h", nil, nil,
	); err != nil {
		t.Fatalf("Execute with a foreign-manager lease: %v", err)
	}
	if ownDuringCreate != 1 {
		t.Fatalf("lease count on the executing manager's image during create = %d, want 1 (own reference)", ownDuringCreate)
	}
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("executing manager lease count after Execute = %d, want 0", got)
	}
	if got := foreign.LeaseCount(image); got != 1 {
		t.Fatalf("foreign manager lease count = %d, want 1 (its own lease is untouched)", got)
	}
}

// TestImageCoordinatorRemovalWaitsForEveryIndependentReference pins the drain
// semantics against MULTIPLE independent references (not a share of one):
// beginRemoval commits the gate, rejects new independent acquires atomically,
// and its drain closes only after the LAST reference is released. It uses the
// coordinator's retireEntered hook as the exact commit signal rather than a
// delay-based assertion.
func TestImageCoordinatorRemovalWaitsForEveryIndependentReference(t *testing.T) {
	const image = "relay-fn-a:v1"
	c := newImageCoordinator()

	first, err := c.acquire(image)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	second, err := c.acquire(image)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	third, err := c.acquire(image)
	if err != nil {
		t.Fatalf("acquire third: %v", err)
	}
	if got := c.count(image); got != 3 {
		t.Fatalf("reference count = %d, want 3", got)
	}

	entered := make(chan struct{})
	var once sync.Once
	c.setTestHooks(&coordinatorHooks{retireEntered: func(got string) {
		if got == image {
			once.Do(func() { close(entered) })
		}
	}})

	type beginResult struct {
		drained <-chan struct{}
		owner   bool
		err     error
	}
	beginCh := make(chan beginResult, 1)
	go func() {
		drained, owner, err := c.beginRemoval(image)
		beginCh <- beginResult{drained: drained, owner: owner, err: err}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("beginRemoval never committed the retirement gate")
	}

	// The gate is committed: a new independent acquire is rejected atomically.
	if _, err := c.acquire(image); !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("new independent acquire after commit = %v, want ErrImageRetiring", err)
	}

	var begin beginResult
	select {
	case begin = <-beginCh:
	case <-time.After(2 * time.Second):
		t.Fatal("beginRemoval did not return")
	}
	if begin.err != nil || !begin.owner {
		t.Fatalf("beginRemoval = (owner=%v, err=%v), want owned", begin.owner, begin.err)
	}

	// Releasing references one by one: the drain stays open until the last.
	first.Release()
	select {
	case <-begin.drained:
		t.Fatal("drain closed while two references were still held")
	default:
	}
	second.Release()
	select {
	case <-begin.drained:
		t.Fatal("drain closed while one reference was still held")
	default:
	}
	third.Release()
	select {
	case <-begin.drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not close after the last reference released")
	}
}

// TestExecuteCarriedEntitlementReleasedOnFailedCreate pins that the execution's
// OWN share of a carried entitlement is released even when the cold container
// create fails, so a retirement waiting on the image is not stranded by a failed
// execution. The removal stays parked on the carried admission pin (which the
// caller owns) and proceeds only once that pin is released.
func TestExecuteCarriedEntitlementReleasedOnFailedCreate(t *testing.T) {
	const image = "relay-fn-a:v1"

	dels := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)

	createEntered := make(chan struct{})
	createRelease := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	releaseCreate := func() { releaseOnce.Do(func() { close(createRelease) }) }
	defer releaseCreate()

	createErr := errors.New("scripted create failure")
	var m *Manager
	m = newEntitlementManager(t, func(*Manager, resolvedImage) (reusableContainer, error) {
		enteredOnce.Do(func() { close(createEntered) })
		<-createRelease
		return nil, createErr
	})
	m.cli = cli

	carried, err := m.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}

	// Commit retirement and park the removal on the carried pin.
	signal := signalOnRetirement(m, image)
	removeDone := make(chan error, 1)
	go func() { removeDone <- m.RemoveImage(context.Background(), image) }()
	signal.wait(t)

	execDone := make(chan error, 1)
	go func() {
		execDone <- m.Execute(
			WithImageLease(context.Background(), carried),
			&Prepared{Name: "a", Image: image, Concurrency: 1},
			"h", nil, nil,
		)
	}()
	select {
	case <-createEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the execution never reached its container create")
	}
	if got := m.LeaseCount(image); got != 2 {
		t.Fatalf("lease count during the admitted create = %d, want 2 (carried pin + execution share)", got)
	}

	// The create fails: Execute returns its error and releases its OWN share,
	// leaving only the carried pin.
	releaseCreate()
	select {
	case err := <-execDone:
		if !errors.Is(err, createErr) {
			t.Fatalf("Execute error = %v, want the scripted create failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not return after the create failed")
	}
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("lease count after a failed create = %d, want 1 (the carried pin only)", got)
	}
	select {
	case err := <-removeDone:
		t.Fatalf("removal completed while the carried pin was held: %v", err)
	default:
	}

	carried.Release()
	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatalf("remove after the pin released: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removal did not proceed after the pin released")
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want 1", dels)
	}
}
