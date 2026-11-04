package cli

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/state"
)

// This file pins the renderer write-failure contract: every data-only renderer
// returns its writer's failure instead of silently rendering a truncated table
// as an exit-0 success. The shared output wrapper already collects the first
// write error and the tabwriter's flush error; a writer that fails always
// exercises the whole path each renderer owns (buffered table writes and any
// direct lines) with no database or store needed.

// renderWriteErr is the failure the failing writer injects on every Write, so
// the assertions can require the renderer to report exactly that error — not
// merely some non-nil result.
var renderWriteErr = errors.New("cli test fixture: write failed")

// failingWriter fails every write with renderWriteErr. It is deliberately
// compact: the renderers under test produce data only, so one always-failing
// writer covers their whole write surface.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, renderWriteErr }

// requireWriteFailure renders and asserts the injected write failure is
// returned, so propagation is exact rather than just non-nil.
func requireWriteFailure(t *testing.T, renderer string, render func() error) {
	t.Helper()
	if err := render(); !errors.Is(err, renderWriteErr) {
		t.Fatalf("%s: err = %v, want the injected write failure", renderer, err)
	}
}

// TestRenderStatsPropagatesWriteFailure pins `relay stats`' renderer: a table
// that fails to write is a returned error, never a silent success.
func TestRenderStatsPropagatesWriteFailure(t *testing.T) {
	requireWriteFailure(t, "renderStats", func() error {
		return renderStats(failingWriter{}, state.Stats{EventsReceivedTotal: 153000})
	})
}

// TestRenderAppListPropagatesWriteFailure pins the `app ls` renderer: a
// failed write must not let the command present a partial listing as a
// success.
func TestRenderAppListPropagatesWriteFailure(t *testing.T) {
	rows := []state.Row{
		{Name: "alpha", Runtime: "python3.14", Status: state.StatusReady},
		{Name: "beta", Runtime: "node24", Status: state.StatusPreparing},
	}
	requireWriteFailure(t, "renderAppList", func() error {
		return renderAppList(failingWriter{}, rows)
	})
}

// TestRenderAppDetailPropagatesWriteFailure pins `app inspect`'s detail
// renderer with a detail exercising every optional section (handlers,
// schedules, services, env, secrets): whichever section's write fails, the
// error must reach the caller instead of stopping the render silently.
func TestRenderAppDetailPropagatesWriteFailure(t *testing.T) {
	detail := state.Detail{
		Row:      state.Row{Name: "alpha", Runtime: "python3.14", Status: state.StatusReady},
		Handlers: []state.Handler{{Name: "events.created.handler", Timeout: 6 * time.Second}},
		Schedules: []state.Schedule{{
			Name: "report", Handler: "report.handler", Cron: "0 8 * * 1-5",
			Timezone: "UTC", Timeout: 20 * time.Second, Retries: 3,
		}},
		Services: []state.Service{{Name: "svc", Entrypoint: "service.js", Port: 3000, Replicas: 2}},
		Env:      map[string]string{"API_URL": state.RedactedEnvValue},
		Secrets:  map[string]string{"DATABASE_URL": "database-url"},
	}
	requireWriteFailure(t, "renderAppDetail", func() error {
		return renderAppDetail(failingWriter{}, detail, state.AppStats{EventsMatchedTotal: 9})
	})
}

// TestRenderDLQListPropagatesWriteFailure pins the `dlq ls` renderer: a failed
// write of the dead-letter table is returned, never swallowed.
func TestRenderDLQListPropagatesWriteFailure(t *testing.T) {
	entries := []dlq.Entry{
		dlqTestEntry(dlqUnitID, "1699999999999-0", "alpha", "events.a.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
	}
	requireWriteFailure(t, "renderDLQList", func() error {
		return renderDLQList(failingWriter{}, entries)
	})
}

// TestRenderDLQEntryPropagatesWriteFailure pins the `dlq inspect` renderer,
// including its direct (non-table) Event section: the failure must propagate
// from whichever write hits the broken writer first.
func TestRenderDLQEntryPropagatesWriteFailure(t *testing.T) {
	entry := dlqTestEntry(dlqUnitID, "1699999999999-0", "alpha", "events.a.handler",
		`{"event_name":"INSERT","id":7}`, "2026-09-23T10:00:00Z")
	requireWriteFailure(t, "renderDLQEntry", func() error {
		return renderDLQEntry(failingWriter{}, entry)
	})
}

// TestRenderRuntimePoolPropagatesWriteFailure pins the Runtime pool section's
// renderer (both the unknown-gauges branch and the persisted counters): a
// failed write is returned.
func TestRenderRuntimePoolPropagatesWriteFailure(t *testing.T) {
	stats := state.AppStats{WarmAcquiresTotal: 7, ColdStartsTotal: 3, DiscardedTotal: 2}
	requireWriteFailure(t, "renderRuntimePool", func() error {
		return renderRuntimePool(failingWriter{}, stats, nil)
	})
}

// TestSecretListPropagatesWriteFailure pins `secret ls`'s renderer against a
// store that resolves one real secret (so the failure is the WRITER's, not a
// store error): the failed listing write is returned.
func TestSecretListPropagatesWriteFailure(t *testing.T) {
	deps, store, _ := newSecretDeps(t)
	if err := store.Set(context.Background(), "db", "s3cr3t"); err != nil {
		t.Fatalf("seed secret: %v", err)
	}

	requireWriteFailure(t, "secretList", func() error {
		return secretList(context.Background(), failingWriter{}, deps)
	})
}
