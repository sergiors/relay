package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
)

// statusEvent is one observer notification.
type statusEvent struct {
	name   string
	status string
}

// recordingObserver returns an observer that appends every notification and a
// pointer to the collected events.
func recordingObserver() (func(name, status string), *[]statusEvent) {
	var events []statusEvent
	return func(name, status string) {
		events = append(events, statusEvent{name: name, status: status})
	}, &events
}

// TestStatusObserverTransitions covers the full lifecycle an operator observes:
// discovery/preparing, building, reconciling, ready, failures (degraded with a
// usable generation, unavailable without), removal (empty status), and prune.
func TestStatusObserverTransitions(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	observer, events := recordingObserver()
	st.SetStatusObserver(observer)

	tmpl := &app.Template{Runtime: "python3.14"}
	fn := app.App{Name: "demo", Dir: t.TempDir(), Template: tmpl}

	// Discovery -> preparing.
	st.RecordDiscoveredWithFingerprint(fn, "fp-1")
	// Building / reconciling transient stages.
	st.RecordReconcileBuilding("demo")
	st.RecordReconciling("demo")
	// Success -> ready.
	st.RecordReconcileSuccess("demo", "img-1", "fp-1", time.Now(), fn)
	// Failure with a usable generation -> degraded.
	st.RecordReconcileFailure("demo", errors.New("boom"))
	// Service failure with a usable generation -> degraded.
	st.RecordServiceFailure("demo", errors.New("svc boom"))
	// Removal -> empty status.
	st.RecordRemoved("demo")

	want := []statusEvent{
		{"demo", StatusPreparing},
		{"demo", StatusBuilding},
		{"demo", StatusReconciling},
		{"demo", StatusReady},
		{"demo", StatusDegraded},
		{"demo", StatusDegraded},
		{"demo", ""},
	}
	if len(*events) != len(want) {
		t.Fatalf("events = %+v, want %+v", *events, want)
	}
	for i := range want {
		if (*events)[i] != want[i] {
			t.Fatalf("event %d = %+v, want %+v", i, (*events)[i], want[i])
		}
	}
}

// TestStatusObserverFailureWithoutGenerationUnavailable pins the unavailable
// branch: a failure on an app that never had a usable generation notifies
// with unavailable.
func TestStatusObserverFailureWithoutGenerationUnavailable(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	observer, events := recordingObserver()
	st.SetStatusObserver(observer)

	tmpl := &app.Template{Runtime: "python3.14"}
	fn := app.App{Name: "fresh", Dir: t.TempDir(), Template: tmpl}
	st.RecordDiscoveredWithFingerprint(fn, "fp")
	st.RecordReconcileFailure("fresh", errors.New("first build failed"))

	last := (*events)[len(*events)-1]
	if last.status != StatusUnavailable {
		t.Fatalf("last event = %+v, want unavailable", last)
	}
	// No notification claims ready for an app that never converged.
	for _, e := range *events {
		if e.status == StatusReady {
			t.Fatalf("unexpected ready notification: %+v", *events)
		}
	}
}

// TestStatusObserverNotNotifiedForMissingRow pins that a transient status write
// against an absent row (building/reconciling on an app with no persisted
// row) does not notify the observer: the gauge must not claim a transition the
// database did not record.
func TestStatusObserverNotNotifiedForMissingRow(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	observer, events := recordingObserver()
	st.SetStatusObserver(observer)

	st.RecordReconcileBuilding("ghost")
	st.RecordReconciling("ghost")
	if len(*events) != 0 {
		t.Fatalf("missing-row status writes must not notify: %+v", *events)
	}
}

// TestStatusObserverNotNotifiedForRemovalOfMissingRow pins that removal of an
// absent app is idempotent and still notifies the empty-status signal (a
// delete of already-absent series is harmless), while a fresh discovery after
// removal emits preparing again.
func TestStatusObserverPruneNotifiesRemoval(t *testing.T) {
	root := t.TempDir()
	st, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	observer, events := recordingObserver()
	st.SetStatusObserver(observer)

	tmpl := &app.Template{Runtime: "python3.14"}
	fn := app.App{Name: "gone", Dir: filepath.Join(root, "gone"), Template: tmpl}
	if err := os.MkdirAll(fn.Dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	st.RecordDiscoveredWithFingerprint(fn, "fp")

	// Prune removes the row for an app whose directory no longer exists.
	if err := os.RemoveAll(fn.Dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	st.PruneRemoved(root)

	last := (*events)[len(*events)-1]
	if last.name != "gone" || last.status != "" {
		t.Fatalf("prune must notify an empty status, got %+v", last)
	}
}

// TestStatusObserverNilSafe pins that a state opened without an observer (the
// default) tolerates every status write, and that clearing the observer is safe.
func TestStatusObserverNilSafe(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	observer, events := recordingObserver()
	st.SetStatusObserver(observer)
	st.SetStatusObserver(nil)

	tmpl := &app.Template{Runtime: "python3.14"}
	fn := app.App{Name: "quiet", Dir: t.TempDir(), Template: tmpl}
	st.RecordDiscoveredWithFingerprint(fn, "fp")
	st.RecordReconcileSuccess("quiet", "img", "fp", time.Now(), fn)
	if len(*events) != 0 {
		t.Fatalf("cleared observer must not be notified: %+v", *events)
	}
	var nilSt *State
	nilSt.SetStatusObserver(observer)
	nilSt.notifyStatus("x", "ready")
}
