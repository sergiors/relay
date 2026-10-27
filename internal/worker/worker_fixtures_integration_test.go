//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/state"
)

// seedFencingToken persists fn's desired generation and returns the exact
// fencing token the write stored. Guarded writes must carry it; zero is a
// failed write and never persists.
func seedFencingToken(t *testing.T, st *state.State, fn app.App) state.FencingToken {
	t.Helper()
	fencingToken, err := st.RecordDiscovered(context.Background(), fn)
	if err != nil {
		t.Fatalf("RecordDiscovered(%q): %v", fn.Name, err)
	}
	if fencingToken <= 0 {
		t.Fatalf("RecordDiscovered(%q) returned fencing token %d, want positive", fn.Name, fencingToken)
	}
	return fencingToken
}

// recordSuccess gives fn a usable persisted generation without reading the
// fencing token back from apps.fencing_token: the desired write returns the
// exact token and the success is conditioned on it.
func recordSuccess(
	t *testing.T,
	st *state.State,
	fn app.App,
	image, fingerprint string,
	preparedAt time.Time,
) state.FencingToken {
	t.Helper()
	fencingToken := seedFencingToken(t, st, fn)
	landed, err := st.RecordReconcileSuccess(context.Background(), fn.Name, image, fingerprint, preparedAt, fn, fencingToken)
	if err != nil {
		t.Fatalf("RecordReconcileSuccess(%q): %v", fn.Name, err)
	}
	if !landed {
		t.Fatalf("RecordReconcileSuccess(%q) did not persist with fencing token %d",
			fn.Name, fencingToken)
	}
	return fencingToken
}
