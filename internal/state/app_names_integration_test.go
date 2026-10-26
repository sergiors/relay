//go:build integration

package state

import (
	"context"
	"slices"
	"testing"
)

// seedAppRow persists an apps row for name using the suite's app builder.
func seedAppRow(t *testing.T, st *State, name string) {
	t.Helper()
	if _, err := st.RecordDiscovered(context.Background(), fnFor(t, name, mustTemplate(t, twoHandlerTmpl))); err != nil {
		t.Fatalf("seed app row %q: %v", name, err)
	}
}

// TestAppNamesCompleteRead pins the success contract: every app row is
// returned (nothing dropped), ordered by name, with ok=true.
func TestAppNamesCompleteRead(t *testing.T) {
	st := openTestState(t)
	seedAppRow(t, st, "beta")
	seedAppRow(t, st, "alpha")
	seedAppRow(t, st, "gamma")

	names, ok := st.AppNames(context.Background())
	if !ok {
		t.Fatal("AppNames reported ok=false on a healthy database")
	}
	// Two records are out of insertion order on purpose: the result must be
	// the complete set, name-ordered.
	if want := []string{"alpha", "beta", "gamma"}; !slices.Equal(names, want) {
		t.Fatalf("AppNames = %v, want %v (complete, name-ordered)", names, want)
	}
}

// TestAppNamesEmptyLiveSetSucceeds pins the distinction the sweep depends on:
// zero app rows is a KNOWN empty live set (ok=true), not a failed read. If the
// flag collapsed "empty" and "failed" together, the worker's sweep could never
// legitimately clear a drained live set without also skipping it on errors.
func TestAppNamesEmptyLiveSetSucceeds(t *testing.T) {
	st := openTestState(t)

	names, ok := st.AppNames(context.Background())
	if !ok {
		t.Fatal("an empty-but-healthy live set must report ok=true")
	}
	if len(names) != 0 {
		t.Fatalf("AppNames = %v, want no names", names)
	}
}

// TestAppNamesCancelledContextFails pins that a caller-cancelled context is a
// failed read: the worker passes its flush context, so a cancelled flush must
// produce ok=false and no names (sweep keeps its fail-open behavior). No
// blanket select is involved — the SQL driver surfaces the cancellation itself.
func TestAppNamesCancelledContextFails(t *testing.T) {
	st := openTestState(t)
	seedAppRow(t, st, "alpha")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the read begins

	names, ok := st.AppNames(ctx)
	if ok {
		t.Fatal("AppNames must report ok=false for a cancelled context")
	}
	if len(names) != 0 {
		t.Fatalf("AppNames on a cancelled context = %v, want no names", names)
	}
}
