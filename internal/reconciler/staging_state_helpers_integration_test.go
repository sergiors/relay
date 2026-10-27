//go:build integration

package reconciler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// newTestStateReconcilerLogging mirrors newTestStateReconciler but routes the
// reconciler's logger through the supplied buffer so a test can assert on the
// absence of staging warnings.
func newTestStateReconcilerLogging(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedApp,
	logs *testutil.SyncBuffer,
) (*Reconciler, *runner.Registry, *state.State) {
	t.Helper()
	testutil.RequireStateDatabase(t)
	st := testutil.NewState(t)
	reg := &runner.Registry{}
	reg.Set(initial)
	cfg := Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st}
	r := New(cfg, reg, builder, slog.New(slog.NewTextHandler(logs, nil)))
	for _, pf := range initial {
		seedCurrent(r, pf.App())
		fencingToken, err := st.RecordDiscovered(context.Background(), pf.App())
		if err != nil {
			t.Fatalf("record initial app %q: %v", pf.App().Name, err)
		}
		r.SeedFencingToken(pf.App().Name, fencingToken)
	}
	return r, reg, st
}
