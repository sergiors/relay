package state

import (
	"context"
	"sync"
	"testing"
	"time"
)

// oneHandlerNodeTmpl is a config that differs from twoHandlerTmpl in runtime,
// handler count, and handler name, so a lost read-modify-write is visible on any
// of those fields.
const oneHandlerNodeTmpl = `runtime: node24
events:
  - handler: jobs.run.handler
    pattern:
      event_name: [INSERT]
`

// TestConcurrentAppSnapshotWritesPreserveFields is the regression test for the
// connection-pool read-modify-write race: without per-app serialization two
// pooled transactions can read the same snapshot and then each write a whole
// payload back, so the second silently clobbers the first's merged fields.
//
// Each round reseeds the full snapshot (config A + active generation), then
// releases a mix of status writers and desired-generation writers at once. A
// status write only changes status but writes the WHOLE snapshot it read, so
// without per-app serialization a status writer that commits after a desired
// writer (and read before it) reverts the config/desired fingerprint. Every round
// must therefore end with the desired generation's config and fingerprint AND the
// preserved active generation, regardless of the interleaving.
func TestConcurrentAppSnapshotWritesPreserveFields(t *testing.T) {
	c := openTestState(t)
	tmplA := mustTemplate(t, twoHandlerTmpl)
	tmplB := mustTemplate(t, oneHandlerNodeTmpl)
	const name = "race"

	// Precompute the app values up front: fnFor calls t.TempDir(), which is not
	// safe to call from goroutines.
	fnA := fnFor(t, name, tmplA)
	fnB := fnFor(t, name, tmplB)

	const (
		rounds  = 10
		writers = 8
	)
	for r := 0; r < rounds; r++ {
		// Seed a converged snapshot: config/runtime A, desired fp-A, and a usable
		// active generation.
		fencingTokenA := seedFencingTokenWithFingerprint(t, c, fnA, "fp-A")
		if _, err := c.RecordReconcileSuccess(context.Background(), name, "img-A", "fp-A", time.Now(), fnA, fencingTokenA); err != nil {
			t.Errorf("state write error: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				switch {
				case i < 2: // desired-generation writers change config + desired fingerprint
					if _, err := c.RecordPreparingWithFingerprint(context.Background(), name, fnB, "fp-B"); err != nil {
						t.Errorf("state write error: %v", err)
					}
				case i%2 == 0:
					if _, err := c.RecordReconcileBuilding(context.Background(), name, fencingTokenA); err != nil {
						t.Errorf("state write error: %v", err)
					}
				default:
					if _, err := c.RecordReconciling(context.Background(), name, fencingTokenA); err != nil {
						t.Errorf("state write error: %v", err)
					}
				}
			}(i)
		}
		close(start)
		wg.Wait()

		detail, ok := c.GetApp(name)
		if !ok {
			t.Fatalf("round %d: row missing", r)
		}
		// The active generation is never displaced by a desired-only or status
		// write: it survives every interleaving.
		if detail.Image != "img-A" || detail.Fingerprint != "fp-A" {
			t.Fatalf("round %d: active generation lost: image=%q fingerprint=%q", r, detail.Image, detail.Fingerprint)
		}
		// The desired generation and its config must be the latest desired write
		// (B) regardless of which writer committed last: a status writer reads the
		// current snapshot, so it can only preserve B, and a desired writer writes
		// B. Seeing A here means a stale snapshot was written back.
		if detail.DesiredFingerprint != "fp-B" {
			t.Fatalf("round %d: desired fingerprint = %q, want fp-B (stale snapshot clobbered it)", r, detail.DesiredFingerprint)
		}
		if detail.Runtime != "node24" {
			t.Fatalf("round %d: runtime = %q, want node24 (stale config clobbered it)", r, detail.Runtime)
		}
		if len(detail.Handlers) != 1 || detail.Handlers[0].Name != "jobs.run.handler" {
			t.Fatalf("round %d: handlers = %+v, want the desired config's single handler", r, detail.Handlers)
		}
	}
}

// TestAppWriteTakesPerAppAdvisoryLock deterministically proves the serialization
// seam: while an external transaction holds the app's advisory lock, a status
// write for the same app blocks (it cannot read-modify-write concurrently), and
// it completes once the lock is released.
func TestAppWriteTakesPerAppAdvisoryLock(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	const name = "locked"
	fencingToken := seedFencingTokenWithFingerprint(t, c, fnFor(t, name, tmpl), "fp")

	ctx := context.Background()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Hold the same per-app lock the State write paths take.
	if err := lockAppTx(ctx, tx, name); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock: %v", err)
	}

	done := make(chan struct{})
	go func() {
		if _, err := c.RecordReconcileBuilding(context.Background(), name, fencingToken); err != nil {
			t.Errorf("state write error: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		_ = tx.Rollback()
		t.Fatal("status write completed while the app lock was held: no per-app serialization")
	case <-time.After(300 * time.Millisecond):
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("status write did not complete after the app lock was released")
	}

	detail, ok := c.GetApp(name)
	if !ok {
		t.Fatal("row missing after the status write")
	}
	if detail.Status != StatusBuilding {
		t.Fatalf("status = %q, want building", detail.Status)
	}
}

// TestAppWriteSerializesAbsentRowCreation pins the app-row-ABSENT case: a row
// lock (SELECT ... FOR UPDATE) locks nothing for a row that does not exist, so
// per-app serialization must key on the NAME. While an external transaction holds
// the app lock for a name with no apps row, creating that row blocks, and it lands
// once the lock is released.
func TestAppWriteSerializesAbsentRowCreation(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	const name = "absent"
	fn := fnFor(t, name, tmpl)

	ctx := context.Background()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := lockAppTx(ctx, tx, name); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock: %v", err)
	}

	done := make(chan struct{})
	go func() {
		if _, err := c.RecordDiscoveredWithFingerprint(context.Background(), fn, "fp"); err != nil {
			t.Errorf("state write error: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		_ = tx.Rollback()
		t.Fatal("row creation completed while the app lock was held: absent-row writes are not serialized")
	case <-time.After(300 * time.Millisecond):
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("row creation did not complete after the app lock was released")
	}

	detail, ok := c.GetApp(name)
	if !ok {
		t.Fatal("row missing after creation")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing", detail.Status)
	}
}
