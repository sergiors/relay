package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/testutil"
)

// newEntitlementManager builds a Manager with the two identity/start seams stubbed
// so Execute's admission and container-start path run without a Docker daemon.
// start receives the manager (so a test can inspect the admitted leases held
// DURING the execution window) and the resolved image identity the execution
// creates from.
func newEntitlementManager(t *testing.T, start func(m *Manager, img resolvedImage) (reusableContainer, error)) *Manager {
	t.Helper()
	var m *Manager
	m = &Manager{log: testutil.DiscardLogger(), containers: newContainerCache()}
	m.resolveImageIdentityFn = func(_ context.Context, ref, fp string) (resolvedImage, error) {
		return resolvedImage{ref: ref, id: "sha256:" + ref, fingerprint: fp}, nil
	}
	m.startContainerFn = func(_ context.Context, _ string, img resolvedImage, _ []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		return start(m, img)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// TestExecuteCarriedEntitlementHoldsOwnReferenceAcrossRetirement is the central
// admission-entitlement proof. An execution ADMITTED before an image's
// retirement carries that admitted lease on ctx (the registry snapshot's shared
// publication lease in production). Retirement is then committed (the gate is
// set and removal is blocked on the drain) BEFORE the execution reaches its
// container create. Execute must:
//
//   - accept the carried entitlement (the execution is NOT rejected by the gate);
//   - hold its OWN share of it for the whole create window, so the reference
//     count is the carried pin PLUS the execution's own reference; and
//   - keep the removal parked until BOTH drain — the execution's own reference
//     is dropped when the invocation concludes, and the carried pin only when
//     its holder releases it.
//
// Every wait is a channel signalled at the exact production transition (the
// coordinator's retire-entered hook, the stubbed create boundary); the
// time.After cases are deadlock bounds only.
func TestExecuteCarriedEntitlementHoldsOwnReferenceAcrossRetirement(t *testing.T) {
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

	m := newEntitlementManager(t, func(*Manager, resolvedImage) (reusableContainer, error) {
		enteredOnce.Do(func() { close(createEntered) })
		<-createRelease
		return &fakeContainer{}, nil
	})
	m.cli = cli

	// The admitted authority an execution received before retirement.
	carried, err := m.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}

	// Commit retirement and wait for the exact instant the gate is established.
	signal := signalOnRetirement(m, image)
	removeDone := make(chan error, 1)
	go func() { removeDone <- m.RemoveImage(context.Background(), image) }()
	signal.wait(t)
	if !m.IsImageRetiring(image) {
		t.Fatal("the removal must own the retirement gate once it is entered")
	}
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("lease count at the gate = %d, want 1 (the carried pin)", got)
	}

	// The admitted execution now starts and parks inside its container create.
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

	// The execution did not fail against the committed gate, and it holds its
	// OWN reference on top of the carried pin.
	if got := m.LeaseCount(image); got != 2 {
		t.Fatalf("lease count during the admitted create = %d, want 2 (carried pin + execution's own reference)", got)
	}
	if dels != 0 {
		t.Fatalf("ImageRemove issued while an admitted execution held a reference: %d", dels)
	}

	// The create concludes; the execution's own reference is released, but the
	// carried pin still keeps the drain open.
	releaseCreate()
	if err := <-execDone; err != nil {
		t.Fatalf("admitted Execute failed: %v", err)
	}
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("lease count after the execution concluded = %d, want 1 (the carried pin)", got)
	}
	select {
	case err := <-removeDone:
		t.Fatalf("removal completed while the admitted pin was still held: %v", err)
	default:
	}

	// Releasing the admission pin drains the last reference: removal proceeds.
	carried.Release()
	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatalf("remove after the pin released: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removal did not proceed after the admitted pin released")
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want 1", dels)
	}
}

// TestExecuteCarriedLeaseForOtherImageAcquiresOwn pins the entitlement check: a
// carried lease that pins a DIFFERENT image does NOT entitle this execution, so
// Execute acquires its own independent reference for the image it will use
// rather than silently riding a token that protects another image. The carried
// lease is left untouched (its holder owns it).
func TestExecuteCarriedLeaseForOtherImageAcquiresOwn(t *testing.T) {
	const image = "relay-fn-a:v1"
	const other = "relay-fn-b:v1"

	var ownDuringCreate, carriedDuringCreate int
	m := newEntitlementManager(t, func(mgr *Manager, _ resolvedImage) (reusableContainer, error) {
		ownDuringCreate = mgr.LeaseCount(image)
		carriedDuringCreate = mgr.LeaseCount(other)
		return &fakeContainer{}, nil
	})

	carried, err := m.AcquireImageLease(other)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}
	defer carried.Release()

	if err := m.Execute(
		WithImageLease(context.Background(), carried),
		&Prepared{Name: "a", Image: image, Concurrency: 1},
		"h", nil, nil,
	); err != nil {
		t.Fatalf("Execute with a mismatched carried lease: %v", err)
	}
	if ownDuringCreate != 1 {
		t.Fatalf("lease count on the executed image during create = %d, want 1 (Execute must acquire its own)", ownDuringCreate)
	}
	if carriedDuringCreate != 1 {
		t.Fatalf("lease count on the other image during create = %d, want 1 (the carried lease is untouched)", carriedDuringCreate)
	}
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("lease count after Execute = %d, want 0 (the execution's own lease was released)", got)
	}
}

// TestExecuteReleasedCarriedLeaseAcquiresOwn pins that a lease the holder already
// released no longer entitles anything: Execute acquires its own independent
// reference instead of trusting a spent token.
func TestExecuteReleasedCarriedLeaseAcquiresOwn(t *testing.T) {
	const image = "relay-fn-a:v1"

	var ownDuringCreate int
	m := newEntitlementManager(t, func(mgr *Manager, _ resolvedImage) (reusableContainer, error) {
		ownDuringCreate = mgr.LeaseCount(image)
		return &fakeContainer{}, nil
	})

	carried, err := m.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}
	carried.Release()

	if err := m.Execute(
		WithImageLease(context.Background(), carried),
		&Prepared{Name: "a", Image: image, Concurrency: 1},
		"h", nil, nil,
	); err != nil {
		t.Fatalf("Execute with a released carried lease: %v", err)
	}
	if ownDuringCreate != 1 {
		t.Fatalf("lease count during create = %d, want 1 (Execute must acquire its own)", ownDuringCreate)
	}
}

// TestExecuteReleasedCarriedLeaseCannotBypassRetirement pins the security half of
// the entitlement check: a released carried lease offers no authority, so an
// execution against a RETIRING image is rejected with the retryable
// ErrImageRetiring — it may not slip past the gate on a spent token.
func TestExecuteReleasedCarriedLeaseCannotBypassRetirement(t *testing.T) {
	const image = "relay-fn-a:v1"

	created := 0
	m := newEntitlementManager(t, func(*Manager, resolvedImage) (reusableContainer, error) {
		created++
		return &fakeContainer{}, nil
	})

	carried, err := m.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}
	carried.Release()

	// Commit the retirement gate (no owner: the lower-level commit primitive).
	m.leaseCoord().beginRetire(image)

	err = m.Execute(
		WithImageLease(context.Background(), carried),
		&Prepared{Name: "a", Image: image, Concurrency: 1},
		"h", nil, nil,
	)
	if !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("Execute against a retiring image with a released carried lease = %v, want wrapped ErrImageRetiring", err)
	}
	if created != 0 {
		t.Fatalf("container creates = %d, want 0 (the execution must not start)", created)
	}
}

// TestExecuteCarriedLeaseReleasedMidExecutionKeepsOwnReference pins that the
// execution's own share is what protects it: a holder that releases the carried
// entitlement WHILE the invocation is running cannot pull the image out from
// under it (the execution still holds its own reference).
func TestExecuteCarriedLeaseReleasedMidExecutionKeepsOwnReference(t *testing.T) {
	const image = "relay-fn-a:v1"

	createEntered := make(chan struct{})
	createRelease := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	releaseCreate := func() { releaseOnce.Do(func() { close(createRelease) }) }
	defer releaseCreate()

	m := newEntitlementManager(t, func(*Manager, resolvedImage) (reusableContainer, error) {
		enteredOnce.Do(func() { close(createEntered) })
		<-createRelease
		return &fakeContainer{}, nil
	})

	carried, err := m.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}

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

	// The holder drops the carried entitlement mid-execution.
	carried.Release()
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("lease count after the holder released mid-execution = %d, want 1 (the execution's own reference)", got)
	}

	releaseCreate()
	if err := <-execDone; err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("lease count after Execute = %d, want 0", got)
	}
}

// TestStartServiceCarriedEntitlementSharesUnderRetirement pins the service half
// of the entitlement: a service pass converging to a Relay-owned image that was
// admitted before retirement (its lease rides on ctx) may still start the
// replacement under that admitted authority while the image is retiring, and the
// service takes its OWN child share for the whole create+start window.
func TestStartServiceCarriedEntitlementSharesUnderRetirement(t *testing.T) {
	const image = "relay-fn-a:v1"

	var heldDuringCreate int
	var m0 *Manager
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`,
			onRequest: func(*http.Request) { heldDuringCreate = m0.LeaseCount(image) }},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json", body: `{"Id":"cid-1","State":{"Running":true}}`},
	)
	m0 = &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	t.Cleanup(func() { _ = m0.Close() })

	carried, err := m0.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}
	defer carried.Release()

	// Commit the retirement gate with no owner: the image is retiring, but the
	// service's admitted lease may still be shared.
	m0.leaseCoord().beginRetire(image)

	ctx := WithImageLease(context.Background(), carried)
	if _, err := m0.StartService(ctx, ServiceSpec{
		Function: "fn", Name: "svc", SourceRef: "service.js", Port: 80, Image: image, Env: []string{"PORT=80"},
	}, 0); err != nil {
		t.Fatalf("StartService under retirement with an admitted lease failed: %v", err)
	}
	if heldDuringCreate != 2 {
		t.Fatalf("lease count during service create = %d, want 2 (carried pin + service child share)", heldDuringCreate)
	}
	// The service's child share is released once the start concludes.
	if got := m0.LeaseCount(image); got != 1 {
		t.Fatalf("lease count after StartService = %d, want 1 (the carried pin)", got)
	}
}

// TestStartServiceCarriedLeaseForOtherImageDoesNotBypassRetirement pins the
// service entitlement check: a carried lease for a DIFFERENT image must not be
// mistaken for authority over the service's retiring image. The old behavior
// shared the mismatched lease (which succeeds), skipped the independent acquire,
// and started a container on a retiring image; now it acquires a fresh lease,
// which the gate rejects.
func TestStartServiceCarriedLeaseForOtherImageDoesNotBypassRetirement(t *testing.T) {
	const image = "relay-fn-a:v1"
	const other = "relay-fn-b:v1"

	creates := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`,
			onMatch: func() { creates++ }},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json", body: `{"Id":"cid-1","State":{"Running":true}}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	t.Cleanup(func() { _ = m.Close() })

	carried, err := m.AcquireImageLease(other)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}
	defer carried.Release()

	// The service's own image is retiring; the carried lease is for another image.
	m.leaseCoord().beginRetire(image)

	_, err = m.StartService(WithImageLease(context.Background(), carried), ServiceSpec{
		Function: "fn", Name: "svc", SourceRef: "service.js", Port: 80, Image: image, Env: []string{"PORT=80"},
	}, 0)
	if !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("StartService with a mismatched carried lease on a retiring image = %v, want wrapped ErrImageRetiring", err)
	}
	if creates != 0 {
		t.Fatalf("container creates = %d, want 0 (must not create on a retiring image)", creates)
	}
}

// TestStartServiceCarriedLeaseForOtherImageAcquiresOwn is the non-retiring twin
// of the test above: with the service's image live, a mismatched carried lease is
// ignored and the service acquires its own reference for the image it starts.
func TestStartServiceCarriedLeaseForOtherImageAcquiresOwn(t *testing.T) {
	const image = "relay-fn-a:v1"
	const other = "relay-fn-b:v1"

	var heldDuringCreate, otherDuringCreate int
	var m0 *Manager
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`,
			onRequest: func(*http.Request) {
				heldDuringCreate = m0.LeaseCount(image)
				otherDuringCreate = m0.LeaseCount(other)
			}},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json", body: `{"Id":"cid-1","State":{"Running":true}}`},
	)
	m0 = &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	t.Cleanup(func() { _ = m0.Close() })

	carried, err := m0.AcquireImageLease(other)
	if err != nil {
		t.Fatalf("acquire carried lease: %v", err)
	}
	defer carried.Release()

	if _, err := m0.StartService(WithImageLease(context.Background(), carried), ServiceSpec{
		Function: "fn", Name: "svc", SourceRef: "service.js", Port: 80, Image: image, Env: []string{"PORT=80"},
	}, 0); err != nil {
		t.Fatalf("StartService with a mismatched carried lease: %v", err)
	}
	if heldDuringCreate != 1 {
		t.Fatalf("lease count on the service image during create = %d, want 1 (the service must acquire its own)", heldDuringCreate)
	}
	if otherDuringCreate != 1 {
		t.Fatalf("lease count on the carried image during create = %d, want 1 (untouched)", otherDuringCreate)
	}
}
