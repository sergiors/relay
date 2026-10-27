//go:build integration

package reconciler

import (
	"context"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// recordSuccess gives name a usable persisted generation without reading the
// fencing token back from the database: the desired write returns the exact
// token and the success is conditioned on it. A test that then drives the
// reconciler through a fencing-token-guarded callback seeds this token with
// Reconciler.SeedFencingToken so the local fence matches the persisted row.
func recordSuccess(
	t *testing.T,
	st *state.State,
	name, image, fingerprint string,
	preparedAt time.Time,
	fn app.App,
) state.FencingToken {
	t.Helper()
	fencingToken, err := st.RecordDiscoveredWithFingerprint(context.Background(), fn, fingerprint)
	if err != nil {
		t.Fatalf("RecordDiscoveredWithFingerprint(%q): %v", name, err)
	}
	if fencingToken <= 0 {
		t.Fatalf("RecordDiscoveredWithFingerprint(%q) returned fencing token %d, want positive",
			name, fencingToken)
	}
	landed, err := st.RecordReconcileSuccess(context.Background(), name, image, fingerprint, preparedAt, fn, fencingToken)
	if err != nil {
		t.Fatalf("RecordReconcileSuccess(%q): %v", name, err)
	}
	if !landed {
		t.Fatalf("RecordReconcileSuccess(%q) did not persist with fencing token %d",
			name, fencingToken)
	}
	return fencingToken
}

// newTestStateReconciler is newTestReconciler with a temp state DB wired, and
// returns the state handle so tests can assert persisted rows.
func newTestStateReconciler(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedApp,
	cfgHook func(*Config, *state.State),
) (*Reconciler, *runner.Registry, *state.State) {
	t.Helper()
	st := testutil.NewState(t)
	r, reg := newTestReconciler(t, root, builder, initial, func(cfg *Config) {
		cfg.State = st
		if cfgHook != nil {
			cfgHook(cfg, st)
		}
	})
	return r, reg, st
}
