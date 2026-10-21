package reconciler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// crossWorkerServiceCompletion captures one service-convergence pass's
// callbacks so the test can decide exactly when the older worker's reconcile
// completes. Nothing sleeps: completion is driven by the captured closures.
type crossWorkerServiceCompletion struct {
	onReconcileStart func()
	onComplete       func(error)
}

// crossWorkerFixture stands in for two workers reconciling the same /apps
// entry against one PostgreSQL database. Each worker has its own Reconciler
// (and therefore its own local generation map), registry, and state handle,
// exactly as two concurrent processes sharing one state database would.
type crossWorkerFixture struct {
	fn         app.App
	older      *Reconciler
	newer      *Reconciler
	olderCalls []crossWorkerServiceCompletion
	newerCalls []crossWorkerServiceCompletion
	olderDB    *state.State
	newerDB    *state.State
}

func newCrossWorkerFixture(t *testing.T, name string) *crossWorkerFixture {
	t.Helper()

	root := t.TempDir()
	dir := writeServicesDir(t, root, name)
	tmpl := mustParse(servicesTemplate)
	fn := app.App{Name: name, Dir: dir, Template: tmpl}

	// Two handles onto ONE schema model two workers sharing PostgreSQL. The
	// schema is created by NewStateDSN and dropped on cleanup. Ownerless handles
	// are sufficient: app rows are shared by name and do not depend on the
	// per-worker stats ownership.
	dsn := testutil.NewStateDSN(t)
	olderDB := testutil.OpenState(t, dsn)
	newerDB := testutil.OpenState(t, dsn)
	if _, err := olderDB.RecordDiscovered(context.Background(), fn); err != nil {
		t.Errorf("state write error: %v", err)
	}

	f := &crossWorkerFixture{
		fn:      fn,
		olderDB: olderDB,
		newerDB: newerDB,
	}
	f.older = newCrossWorkerReconciler(
		t, root, fn, olderDB, "img-"+name+"-older", &f.olderCalls)
	f.newer = newCrossWorkerReconciler(
		t, root, fn, newerDB, "img-"+name+"-newer", &f.newerCalls)

	// Both workers start from the same seeded fingerprint. The older worker sees
	// a change and begins its local generation first; its service completion is
	// captured and deliberately left pending.
	writeServiceSource(t, dir, "older-generation")
	f.older.reconcileApp(name)

	// The newer worker then observes a still-newer desired generation and
	// prepares it independently. Its callback is also captured, so the test can
	// persist the newer outcome before the older one completes.
	writeServiceSource(t, dir, "newer-generation")
	f.newer.reconcileApp(name)

	// The older worker is deliberately not reconciled again, so its local
	// generation guard still considers its captured generation current.

	if len(f.olderCalls) != 1 {
		t.Fatalf("older worker captured %d service completions, want 1",
			len(f.olderCalls))
	}
	if len(f.newerCalls) != 1 {
		t.Fatalf("newer worker captured %d service completions, want 1",
			len(f.newerCalls))
	}
	return f
}

func newCrossWorkerReconciler(
	t *testing.T,
	root string,
	fn app.App,
	st *state.State,
	image string,
	completions *[]crossWorkerServiceCompletion,
) *Reconciler {
	t.Helper()

	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{
		runner.NewPrepared(
			fn,
			&runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name + "-initial"},
			&fakeBuilder{},
		),
	})
	r := New(Config{
		Root:     root,
		Debounce: 10 * time.Millisecond,
		Interval: time.Hour,
		State:    st,
		UpdateServicesWithStatus: func(
			_ string,
			_ *app.Template,
			_ string,
			onReconcileStart func(),
			onComplete func(error),
		) {
			*completions = append(*completions, crossWorkerServiceCompletion{
				onReconcileStart: onReconcileStart,
				onComplete:       onComplete,
			})
		},
	}, reg, &fixedImageBuilder{image: image}, testutil.DiscardLogger())
	seedCurrent(r, fn)
	return r
}

// writeServiceSource changes the app's source content so the next reconcile
// observes a new desired generation without touching the template.
func writeServiceSource(t *testing.T, dir, version string) {
	t.Helper()
	body := fmt.Sprintf("export function hi(e){ console.log(%q); }\n", version)
	err := os.WriteFile(filepath.Join(dir, "index.js"), []byte(body), 0o644)
	if err != nil {
		t.Fatalf("write source %s: %v", version, err)
	}
}

func persistedApp(t *testing.T, st *state.State, name string) state.Detail {
	t.Helper()
	detail, ok := st.GetApp(name)
	if !ok {
		t.Fatalf("expected a persisted state row for %q", name)
	}
	return detail
}

func assertAppStateUnchanged(
	t *testing.T,
	st *state.State,
	name string,
	want state.Detail,
) {
	t.Helper()
	got, ok := st.GetApp(name)
	if !ok {
		t.Fatalf("state row for %q disappeared; want it unchanged", name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stale completion overwrote newer state:\n"+
			" got: %+v\nwant: %+v", got, want)
	}
}

// TestCrossWorkerStaleFencingTokenCannotOverwriteNewerState pins the F-02
// regression: local callback counters cannot fence another Reconciler. An older
// worker's callbacks must not rewrite state after a newer worker persists it;
// the PostgreSQL fencing token rejects those stale writes.
func TestCrossWorkerStaleFencingTokenCannotOverwriteNewerState(t *testing.T) {
	t.Run("stale success cannot overwrite newer success", func(t *testing.T) {
		f := newCrossWorkerFixture(t, "shared")

		// The newer worker's current generation completes successfully and is
		// persisted first.
		f.newerCalls[0].onReconcileStart()
		f.newerCalls[0].onComplete(nil)
		newer := persistedApp(t, f.newerDB, f.fn.Name)
		if newer.Status != state.StatusReady ||
			newer.Image != "img-shared-newer" {
			t.Fatalf("precondition: newer outcome = %+v, "+
				"want ready on img-shared-newer", newer)
		}

		// The older worker's still-local-current generation finishes later with
		// its stale built image and fingerprint.
		f.olderCalls[0].onComplete(nil)

		assertAppStateUnchanged(t, f.olderDB, f.fn.Name, newer)
	})

	t.Run("stale failure cannot overwrite newer success", func(t *testing.T) {
		f := newCrossWorkerFixture(t, "shared")

		f.newerCalls[0].onReconcileStart()
		f.newerCalls[0].onComplete(nil)
		newer := persistedApp(t, f.newerDB, f.fn.Name)
		if newer.Status != state.StatusReady {
			t.Fatalf("precondition: newer status = %q, want ready", newer.Status)
		}

		// A service failure from the older generation must not degrade the app.
		f.olderCalls[0].onComplete(errors.New("stale service convergence failed"))

		assertAppStateUnchanged(t, f.olderDB, f.fn.Name, newer)
	})

	t.Run("stale reconcile-start cannot overwrite newer success",
		func(t *testing.T) {
			f := newCrossWorkerFixture(t, "shared")

			f.newerCalls[0].onReconcileStart()
			f.newerCalls[0].onComplete(nil)
			newer := persistedApp(t, f.newerDB, f.fn.Name)
			if newer.Status != state.StatusReady {
				t.Fatalf("precondition: newer status = %q, want ready", newer.Status)
			}

			// The older generation's corrective-work callback must not move the
			// newer, already-ready app back to reconciling.
			f.olderCalls[0].onReconcileStart()

			assertAppStateUnchanged(t, f.olderDB, f.fn.Name, newer)
		})

	t.Run("stale success cannot resurrect a removed app", func(t *testing.T) {
		f := newCrossWorkerFixture(t, "shared")

		// The newer worker observes the removal and deletes the persisted row.
		if err := os.RemoveAll(f.fn.Dir); err != nil {
			t.Fatalf("remove app dir: %v", err)
		}
		f.newer.reconcileApp(f.fn.Name)
		if _, ok := f.newerDB.GetApp(f.fn.Name); ok {
			t.Fatal("precondition: newer worker removal must delete the state row")
		}

		// The older generation's guarded success must not recreate it.
		f.olderCalls[0].onComplete(nil)

		if got, ok := f.olderDB.GetApp(f.fn.Name); ok {
			t.Fatalf("stale success resurrected a removed app row: %+v", got)
		}
	})
}

// TestCrossWorkerCurrentServiceCompletionStillPersists keeps the controls
// alongside the regression: the CURRENT generation's callbacks must still be
// able to persist success and failure. This prevents a future fix from simply
// disabling the state writes.
func TestCrossWorkerCurrentServiceCompletionStillPersists(t *testing.T) {
	t.Run("current success persists", func(t *testing.T) {
		f := newCrossWorkerFixture(t, "shared")

		f.newerCalls[0].onReconcileStart()
		f.newerCalls[0].onComplete(nil)

		got := persistedApp(t, f.newerDB, f.fn.Name)
		if got.Status != state.StatusReady {
			t.Fatalf("status = %q, want ready", got.Status)
		}
		if got.Image != "img-shared-newer" || got.Fingerprint == "" {
			t.Fatalf("image/fingerprint = %q/%q, "+
				"want the newer generation's active identity",
				got.Image, got.Fingerprint)
		}
		if got.LastReconcileStatus != state.ReconcileSuccess ||
			got.LastError != "" {
			t.Fatalf("last reconcile = %q/%q, want success with no error",
				got.LastReconcileStatus, got.LastError)
		}
	})

	t.Run("current failure persists", func(t *testing.T) {
		f := newCrossWorkerFixture(t, "shared")
		wantErr := errors.New("current service convergence failed")

		f.newerCalls[0].onReconcileStart()
		f.newerCalls[0].onComplete(wantErr)

		got := persistedApp(t, f.newerDB, f.fn.Name)
		if got.LastReconcileStatus != state.ReconcileFailed {
			t.Fatalf("last_reconcile_status = %q, want failed", got.LastReconcileStatus)
		}
		if got.LastError != wantErr.Error() {
			t.Fatalf("last_error = %q, want %q", got.LastError, wantErr.Error())
		}
	})
}
