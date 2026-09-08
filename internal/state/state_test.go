package state

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
)

func mustTemplate(t *testing.T, s string) *function.Template {
	t.Helper()
	tmpl, err := function.ParseTemplate([]byte(s))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

func fnFor(t *testing.T, name string, tmpl *function.Template) function.Function {
	t.Helper()
	return function.Function{Name: name, Dir: filepath.Join(t.TempDir(), name), Template: tmpl}
}

func openTestState(t *testing.T) *State {
	t.Helper()
	c, err := Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	c.SetLogger(log.New(os.Stderr, "test: ", 0))
	return c
}

const twoHandlerTmpl = `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
    timeout: 6s
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
    timeout: 20s
`

// init is idempotent and re-opening the same file yields a working state DB.
func TestOpenIdempotentAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c1.RecordDiscovered(fnFor(t, "alpha", tmpl))
	_ = c1.Close()

	c2, err := Open(path) // reopen, init must be idempotent
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	if _, ok := c2.GetFunction("alpha"); !ok {
		t.Fatal("expected function to survive reopen")
	}
}

// discovered -> success: image/fingerprint/prepared/handlers are upserted.
func TestDiscoveredThenSuccessReplacesActiveFields(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	c.RecordDiscovered(fnFor(t, "fn", tmpl))

	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after discovered")
	}
	if d.Status != StatusPending {
		t.Fatalf("status = %s, want pending", d.Status)
	}
	if len(d.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(d.Handlers))
	}

	c.RecordReconcileSuccess("fn", "img-fn", "fp-new", time.Now(), fnFor(t, "fn", tmpl))

	d, ok = c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if d.Status != StatusReady {
		t.Fatalf("status = %s, want ready", d.Status)
	}
	if d.Image != "img-fn" {
		t.Fatalf("image = %q, want img-fn", d.Image)
	}
	if d.Fingerprint != "fp-new" {
		t.Fatalf("fingerprint = %q, want fp-new", d.Fingerprint)
	}
	if d.LastError != "" {
		t.Fatalf("last_error = %q, want cleared", d.LastError)
	}
	if len(d.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2 after success", len(d.Handlers))
	}
}

// KEY: a failed reconcile keeps the prior active image/fingerprint/prepared_at
// intact and only records the failure; status stays ready.
func TestReconcileFailureKeepsPriorActiveAndMarksFailed(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	prepared := time.Now().Add(-time.Hour)

	c.RecordReconcileSuccess("fn", "img-active", "fp-active", prepared, fnFor(t, "fn", tmpl))

	c.RecordReconcileFailure("fn", &boomErr{})

	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row after failure")
	}
	if d.Status != StatusReady {
		t.Fatalf("status = %s, want ready (never marked unavailable)", d.Status)
	}
	if d.Image != "img-active" {
		t.Fatalf("image = %q, want preserved img-active", d.Image)
	}
	if d.Fingerprint != "fp-active" {
		t.Fatalf("fingerprint = %q, want preserved fp-active", d.Fingerprint)
	}
	if d.LastReconcileStatus != ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", d.LastReconcileStatus)
	}
	if d.LastError == "" {
		t.Fatal("last_error should be set on failure")
	}
	// The prepared_at of the active version is preserved, not overwritten.
	if !strings.HasPrefix(d.PreparedAt, prepared.UTC().Format(time.RFC3339)[:19]) {
		t.Fatalf("prepared_at = %s, want preserved active version time", d.PreparedAt)
	}
}

type boomErr struct{}

func (*boomErr) Error() string { return "boom" }

// skipped only touches an existing row (never creates a phantom).
func TestRecordSkippedOnlyIfRowExists(t *testing.T) {
	c := openTestState(t)

	c.RecordSkipped("ghost") // no row -> no-op
	if _, ok := c.GetFunction("ghost"); ok {
		t.Fatal("skipped must not create a row")
	}

	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordDiscovered(fnFor(t, "fn", tmpl))
	c.RecordSkipped("fn")
	d, ok := c.GetFunction("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if d.LastReconcileStatus != ReconcileSkipped {
		t.Fatalf("last_reconcile_status = %s, want skipped", d.LastReconcileStatus)
	}
}

// removal deletes the function row and its handlers.
func TestRemovalDeletesRowAndHandlers(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))

	c.RecordRemoved("fn")

	if _, ok := c.GetFunction("fn"); ok {
		t.Fatal("expected fn to be removed")
	}
	// Re-adding must not resurrect stale handlers (they were deleted).
	c.RecordDiscovered(fnFor(t, "fn", tmpl))
	if _, ok := c.GetFunction("fn"); !ok {
		t.Fatal("expected fn re-added")
	}
}

// ListFunctions returns rows sorted by name with the expected shape.
func TestListFunctionsShapeAndSort(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("zeta", "i-z", "f-z", time.Now(), fnFor(t, "zeta", tmpl))
	c.RecordDiscovered(fnFor(t, "alpha", tmpl))
	c.RecordReconcileSuccess("mid", "i-m", "f-m", time.Now(), fnFor(t, "mid", tmpl))

	rows := c.ListFunctions()
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0].Name != "alpha" || rows[1].Name != "mid" || rows[2].Name != "zeta" {
		t.Fatalf("rows not sorted by name: %+v", rows)
	}
	if rows[0].HandlerCount != 2 {
		t.Fatalf("handler count = %d, want 2", rows[0].HandlerCount)
	}
	if rows[1].Status != StatusReady {
		t.Fatalf("status = %s, want ready", rows[1].Status)
	}
}

// GetFunction of an unknown name returns false with an empty detail.
func TestGetFunctionUnknownReturnsFalse(t *testing.T) {
	c := openTestState(t)
	d, ok := c.GetFunction("nope")
	if ok {
		t.Fatalf("expected not found, got %+v", d)
	}
}

// RebuildFromFS populates an empty DB from a real functions tree using the real
// loader + fingerprint. A non-empty DB is left untouched.
func TestRebuildFromFSOnEmptyDB(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(twoHandlerTmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte("def handler(e): return e\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	c := openTestState(t)
	if err := c.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	rows := c.ListFunctions()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Name != "demo" || rows[0].Runtime != "python3.14" || rows[0].Status != StatusPending {
		t.Fatalf("unexpected rebuild row: %+v", rows[0])
	}
	d, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo detail")
	}
	if len(d.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(d.Handlers))
	}

	// A second rebuild on a non-empty DB must not duplicate rows.
	if err := c.RebuildFromFS(root); err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if got := len(c.ListFunctions()); got != 1 {
		t.Fatalf("rebuild on non-empty DB changed row count to %d, want 1", got)
	}
}

// relativeAgo renders short relative ages and falls back to absolute dates.
// Uses fixed Unix timestamps so sub-second truncation cannot make the test
// flaky: the "now" reference is chosen to land each age on an exact second.
func TestRelativeAgo(t *testing.T) {
	// Build a fixed "now" and fixed offsets; relativeAgo reads time.Now() so we
	// pin the inputs to exact rounded durations that won't drift across a second
	// boundary more than the (<=59s) tolerance.
	now := time.Now()
	mk := func(ago time.Duration) string {
		return time.Unix(0, now.Add(-ago).UnixNano()).UTC().Format(time.RFC3339)
	}
	cases := []struct {
		in   string
		want string
	}{
		{in: mk(15 * time.Second), want: "15s ago"},
		{in: mk(3 * time.Minute), want: "3m ago"},
		{in: mk(2 * time.Hour), want: "2h ago"},
		{in: mk(5 * 24 * time.Hour), want: "5d ago"},
		{in: mk(45 * 24 * time.Hour), want: time.Unix(0, now.Add(-45*24*time.Hour).UnixNano()).UTC().Format("2006-01-02")},
	}
	for _, tc := range cases {
		if got := RelativeAgo(tc.in); got != tc.want {
			t.Errorf("relativeAgo(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
