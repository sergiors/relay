//go:build integration

// Concurrency regression for the ownerless dependency-image coordination
// connection (IR-004 follow-up).
//
// An ownerless State (CLI/admin handle, or the two independent handles the
// dependency-lease tests open) pins its own coordination connection. Every
// dependency query must be serialized on that one *sql.Conn: database/sql
// serializes individual driver calls but not a whole query + row scan, so two
// overlapping calls make pgx fail with "conn busy". DependencyImageLease.Verify
// deliberately does not take the lease gate (it is a read-only liveness check),
// so without a dedicated connection mutex concurrent Verify calls from
// concurrent Managers sharing one ownerless State can overlap.
//
// The suite requires PostgreSQL at TEST_DATABASE_URL and fails rather than skips
// without one, matching the rest of the state integration suite.
package state_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"relay/internal/state"
	"relay/internal/testutil"
)

// TestIntegrationOwnerlessDependencyLeaseConcurrentVerify hammers one held
// ownerless lease with concurrent Verify calls while other goroutines acquire,
// verify, and release leases on the same handle. Every call must succeed: an
// overlap on the pinned connection would surface as a "conn busy" error.
func TestIntegrationOwnerlessDependencyLeaseConcurrentVerify(t *testing.T) {
	st := testutil.OpenState(t, testutil.NewStateDSN(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const ref = "relay-dep-ownerless-verify"
	held, err := st.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}

	const (
		workers    = 16
		iterations = 30
	)
	errs := make(chan error, workers*iterations)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if w%2 == 0 {
					if err := held.Verify(ctx); err != nil {
						errs <- fmt.Errorf("held Verify: %w", err)
						return
					}
					continue
				}
				// A distinct reference keeps this a pure "many queries on one
				// pinned connection" stress without GC contention.
				other := fmt.Sprintf("%s-%d", ref, w)
				lease, err := st.AcquireDependencyImageUse(ctx, other)
				if err != nil {
					errs <- fmt.Errorf("AcquireDependencyImageUse(%s): %w", other, err)
					return
				}
				if err := lease.Verify(ctx); err != nil {
					errs <- fmt.Errorf("Verify(%s): %w", other, err)
				}
				if err := lease.Release(); err != nil {
					errs <- fmt.Errorf("Release(%s): %w", other, err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("%v", err)
	}

	if err := held.Release(); err != nil {
		t.Fatalf("release held lease: %v", err)
	}
}

// TestIntegrationOwnerfulDependencyLeaseConcurrentVerify is the ownerful
// counterpart: a worker handle reuses its pinned ownership connection for
// dependency coordination, serialized by ownerMu. Concurrent Verify calls must
// not overlap pgx calls there either, and must not disturb the ownership lock
// verification path that shares that connection.
func TestIntegrationOwnerfulDependencyLeaseConcurrentVerify(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	// Advisory locks are database-scoped, so the worker identity must be unique
	// across concurrently running tests in this database.
	owner := testutil.UniqueName(t, "dep-lease-owner")
	st := testutil.OpenState(t, dsn, state.Options{Owner: owner, Instance: "inst"})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const ref = "relay-dep-ownerful-verify"
	held, err := st.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}

	const (
		workers    = 8
		iterations = 20
	)
	errs := make(chan error, workers*iterations)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if w%2 == 0 {
					if err := held.Verify(ctx); err != nil {
						errs <- fmt.Errorf("held Verify: %w", err)
						return
					}
					// The shared ownership-connection check must stay healthy while
					// dependency queries interleave with it.
					if err := st.VerifyOwnerLock(ctx); err != nil {
						errs <- fmt.Errorf("VerifyOwnerLock: %w", err)
						return
					}
					continue
				}
				other := fmt.Sprintf("%s-%d", ref, w)
				lease, err := st.AcquireDependencyImageUse(ctx, other)
				if err != nil {
					errs <- fmt.Errorf("AcquireDependencyImageUse(%s): %w", other, err)
					return
				}
				if err := lease.Release(); err != nil {
					errs <- fmt.Errorf("Release(%s): %w", other, err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("%v", err)
	}

	if err := held.Release(); err != nil {
		t.Fatalf("release held lease: %v", err)
	}
}

// TestIntegrationOwnerlessDependencyLeaseCloseDuringVerify closes an ownerless
// handle while Verify calls are in flight. Close must return promptly (it is not
// blocked behind a wedged connection), the verifying goroutine must terminate,
// and every later coordination call on the handle must fail closed.
func TestIntegrationOwnerlessDependencyLeaseCloseDuringVerify(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := state.Open(ctx, state.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}

	const ref = "relay-dep-ownerless-close"
	held, err := st.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		_ = st.Close()
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}

	verifyDone := make(chan struct{})
	go func() {
		defer close(verifyDone)
		for i := 0; i < 100; i++ {
			// While the handle is open these verify; during/after Close they must
			// fail closed rather than panic or hang. Only termination matters here.
			_ = held.Verify(ctx)
		}
	}()

	closeDone := make(chan error, 1)
	go func() { closeDone <- st.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close while verifying: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return while Verify calls were in flight")
	}
	select {
	case <-verifyDone:
	case <-time.After(15 * time.Second):
		t.Fatal("the verifying goroutine never terminated")
	}

	if err := held.Verify(ctx); err == nil {
		t.Fatal("Verify after Close must fail closed")
	}
	if _, err := st.AcquireDependencyImageUse(ctx, ref); err == nil {
		t.Fatal("AcquireDependencyImageUse after Close must fail closed")
	}
	if _, _, err := st.TryAcquireDependencyImageGC(ctx, ref); err == nil {
		t.Fatal("TryAcquireDependencyImageGC after Close must fail closed")
	}
}
