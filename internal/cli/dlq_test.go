package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// fakeDLQStore is an in-memory DLQStore for CLI tests: it preserves insertion
// order (the Redis stream order the real store returns) and records deletes, so
// the command/presentation layer is exercised without Redis.
type fakeDLQStore struct {
	entries []stream.DLQEntry
	deleted []string
	// listErr/getErr/deleteErr inject failures.
	listErr   error
	getErr    error
	deleteErr error
	closed    bool
}

func (f *fakeDLQStore) List(context.Context) ([]stream.DLQEntry, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]stream.DLQEntry(nil), f.entries...), nil
}

func (f *fakeDLQStore) Get(_ context.Context, id string) (stream.DLQEntry, bool, error) {
	if f.getErr != nil {
		return stream.DLQEntry{}, false, f.getErr
	}
	for _, e := range f.entries {
		if e.ID == id {
			return e, true, nil
		}
	}
	return stream.DLQEntry{}, false, nil
}

func (f *fakeDLQStore) Delete(_ context.Context, id string) (bool, error) {
	if f.deleteErr != nil {
		return false, f.deleteErr
	}
	for i, e := range f.entries {
		if e.ID == id {
			f.entries = append(f.entries[:i], f.entries[i+1:]...)
			f.deleted = append(f.deleted, id)
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeDLQStore) Close() error { f.closed = true; return nil }

// withFakeDLQ returns dependencies whose DLQ opener yields store, so the
// command tree uses the fake instead of Redis. It captures whether the store
// was closed by the command.
func withFakeDLQ(t *testing.T, store *fakeDLQStore) Dependencies {
	t.Helper()
	deps := testDeps(t)
	deps.OpenDLQ = func(*slog.Logger) (DLQStore, func(), error) {
		return store, func() { _ = store.Close() }, nil
	}
	return deps
}

// dlqTestEntry builds a current-format entry with sensible defaults.
func dlqTestEntry(id, originalID, function, handler, event, timestamp string) stream.DLQEntry {
	return stream.DLQEntry{
		ID:              id,
		OriginalStream:  "events",
		OriginalID:      originalID,
		Group:           "relay",
		Consumer:        "worker-1",
		Event:           event,
		Reason:          fmt.Sprintf("invocation exhausted: function %q handler %q exhausted after 5 handler attempts", function, handler),
		Function:        function,
		Handler:         handler,
		Deliveries:      9,
		HandlerAttempts: 5,
		Timestamp:       timestamp,
	}
}

// TestDLQBareShowsHelp verifies a bare `relay dlq` shows the subcommand help
// listing every subcommand and exits 0 (the namespace contract).
func TestDLQBareShowsHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "dlq")
	if err != nil {
		t.Fatalf("dlq alone: err = %v, want nil", err)
	}
	for _, want := range []string{"ls", "inspect", "replay", "rm"} {
		if !strings.Contains(out, want) {
			t.Fatalf("dlq help missing %q:\n%s", want, out)
		}
	}
}

// TestDLQHelpFlagsAndUnknownCommand verifies `relay dlq --help` renders the
// subcommands and an unknown token is the shared friendly usage error naming the
// full path.
func TestDLQHelpFlagsAndUnknownCommand(t *testing.T) {
	out, _, err := runCLI(t, "", "dlq", "--help")
	if err != nil {
		t.Fatalf("dlq --help: err = %v, want nil", err)
	}
	for _, want := range []string{"ls", "inspect", "replay", "rm"} {
		if !strings.Contains(out, want) {
			t.Fatalf("dlq --help missing %q:\n%s", want, out)
		}
	}

	_, _, err = runCLI(t, "", "dlq", "bogus")
	if err == nil {
		t.Fatal("dlq bogus: err = nil, want usage error")
	}
	for _, want := range []string{
		"relay: unknown command: relay dlq bogus",
		"Run 'relay dlq --help' for more information",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("dlq bogus err missing %q: %v", want, err)
		}
	}
}

// TestDLQListColumnsAndOrder verifies ls renders one row per entry in stream
// order with ID, source message, function, handler, attempts, and age, and that
// two entries from the SAME source message ID are listed as distinct rows.
func TestDLQListColumnsAndOrder(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1700000000000-0", "1699999999999-0", "alpha", "events.a.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
		// Same original_id, different exhausted invocation: a multi-handler
		// message produces one entry each.
		dlqTestEntry("1700000000000-1", "1699999999999-0", "alpha", "events.b.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry("1700000000001-0", "1699999999998-0", "beta", "index.run", `{"b":2}`, "2026-09-23T09:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected header + 3 rows, got %d:\n%s", len(lines), out)
	}
	hdr := lines[0]
	for _, col := range []string{"ID", "ORIGINAL", "FUNCTION", "HANDLER", "ATTEMPTS", "AGE"} {
		if !strings.Contains(hdr, col) {
			t.Fatalf("header missing %q: %q", col, hdr)
		}
	}
	// Stream order is preserved (not sorted by anything else).
	if !strings.HasPrefix(lines[1], "1700000000000-0") {
		t.Fatalf("row 1 wrong: %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "1700000000000-1") {
		t.Fatalf("row 2 wrong: %q", lines[2])
	}
	if !strings.HasPrefix(lines[3], "1700000000001-0") {
		t.Fatalf("row 3 wrong: %q", lines[3])
	}
	// Both same-source rows render their exact function/handler and attempts.
	for _, row := range []string{lines[1], lines[2]} {
		if !strings.Contains(row, "events/1699999999999-0") ||
			!strings.Contains(row, "alpha") || !strings.Contains(row, "5") {
			t.Fatalf("same-source row missing metadata: %q", row)
		}
	}
	if !strings.Contains(lines[1], "events.a.handler") || !strings.Contains(lines[2], "events.b.handler") {
		t.Fatalf("rows must carry their distinct handlers:\n%s", out)
	}
}

// TestDLQListEmpty verifies an empty DLQ renders just the header (not an error).
func TestDLQListEmpty(t *testing.T) {
	deps := withFakeDLQ(t, &fakeDLQStore{})
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls empty: %v", err)
	}
	if got := normWS(out); got != "ID ORIGINAL FUNCTION HANDLER ATTEMPTS AGE" {
		t.Fatalf("empty ls = %q", got)
	}
}

// TestDLQListTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQListTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "ls", "extra")
	if err == nil || !strings.Contains(err.Error(), "dlq ls: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestDLQInspectFieldsAndPrettyEvent verifies inspect renders every current
// field and pretty-prints the original JSON event.
func TestDLQInspectFieldsAndPrettyEvent(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1700000000000-0", "1699999999999-0", "alpha", "events.a.handler", `{"event_name":"INSERT","id":7}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", "1700000000000-0")
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	flat := normWS(out)
	for _, want := range []string{
		"ID: 1700000000000-0",
		"Original stream: events",
		"Original ID: 1699999999999-0",
		"Group: relay",
		"Consumer: worker-1",
		"Function: alpha",
		"Handler: events.a.handler",
		"Handler attempts: 5",
		"Deliveries: 9",
		"Timestamp: 2026-09-23T10:00:00Z",
		"Reason: invocation exhausted",
		"Event:",
	} {
		if !strings.Contains(flat, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// The original JSON is pretty-printed (indented across lines).
	for _, want := range []string{`"event_name": "INSERT"`, `"id": 7`} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect event not pretty-printed; missing %q\n%s", want, out)
		}
	}
}

// TestDLQInspectNonJSONEventVerbatim verifies a non-JSON event (the "-"
// placeholder for a malformed message) is rendered verbatim rather than hidden.
func TestDLQInspectNonJSONEventVerbatim(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "-", "-", "-", "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", "1-0")
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	if !strings.Contains(out, "Event:\n-\n") {
		t.Fatalf("non-JSON event must render verbatim:\n%s", out)
	}
}

// TestDLQInspectUnknownID verifies an unknown ID is a clear error.
func TestDLQInspectUnknownID(t *testing.T) {
	deps := withFakeDLQ(t, &fakeDLQStore{})
	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", "nope")
	if err == nil || !strings.Contains(err.Error(), `unknown DLQ entry "nope"`) {
		t.Fatalf("err = %v, want unknown-entry error", err)
	}
}

// TestDLQInspectTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQInspectTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "inspect", "1-0", "extra")
	if err == nil || !strings.Contains(err.Error(), "dlq inspect: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestDLQRmRemovesExactlyOne verifies rm deletes exactly the named entry (XDEL
// semantics: one ID) and leaves the others.
func TestDLQRmRemovesExactlyOne(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "alpha", "h", `{}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry("2-0", "2-0", "beta", "h", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", "1-0")
	if err != nil {
		t.Fatalf("dlq rm: %v", err)
	}
	if strings.TrimSpace(out) != "DLQ entry removed" {
		t.Fatalf("rm output = %q", out)
	}
	if len(store.entries) != 1 || store.entries[0].ID != "2-0" {
		t.Fatalf("rm must remove exactly one entry: %+v", store.entries)
	}
}

// TestDLQRmUnknownID verifies rm of an unknown ID is a clear error and removes
// nothing.
func TestDLQRmUnknownID(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{dlqTestEntry("1-0", "1-0", "a", "h", `{}`, "2026-09-23T10:00:00Z")}}
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", "9-9")
	if err == nil || !strings.Contains(err.Error(), `unknown DLQ entry "9-9"`) {
		t.Fatalf("err = %v, want unknown-entry error", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("unknown rm must not delete anything: %+v", store.entries)
	}
}

// TestDLQRmTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQRmTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "rm", "1-0", "extra")
	if err == nil || !strings.Contains(err.Error(), "dlq rm: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestDLQCommandsCloseStore verifies the command plumbing always closes the
// store, including on the presentation path.
func TestDLQCommandsCloseStore(t *testing.T) {
	store := &fakeDLQStore{}
	deps := withFakeDLQ(t, store)
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls"); err != nil {
		t.Fatalf("dlq ls: %v", err)
	}
	if !store.closed {
		t.Fatal("the DLQ store must be closed after the command")
	}
}

// TestDLQReplayUnknownID verifies replay of an unknown ID fails clearly without
// dialing the worker.
func TestDLQReplayUnknownID(t *testing.T) {
	deps := withFakeDLQ(t, &fakeDLQStore{})
	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "nope")
	if err == nil || !strings.Contains(err.Error(), `unknown DLQ entry "nope"`) {
		t.Fatalf("err = %v, want unknown-entry error", err)
	}
}

// TestDLQReplayTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQReplayTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "replay", "1-0", "extra")
	if err == nil || !strings.Contains(err.Error(), "dlq replay: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestDLQNoOpenerConfigured verifies a nil opener (no Redis) fails clearly
// rather than panicking.
func TestDLQNoOpenerConfigured(t *testing.T) {
	deps := testDeps(t)
	deps.OpenDLQ = nil
	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err == nil || !strings.Contains(err.Error(), "redis is not configured") {
		t.Fatalf("err = %v, want a clear no-Redis error", err)
	}
}

// dlqReplayExecutor records the exact handlers it executed, so the full-stack
// replay test proves only the recorded handler ran.
type dlqReplayExecutor struct {
	mu       sync.Mutex
	handlers []string
	payloads [][]byte
	fail     bool
}

func (e *dlqReplayExecutor) Execute(_ context.Context, _ *runtime.Prepared, handler string, eventJSON []byte, _ []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
	e.payloads = append(e.payloads, append([]byte(nil), eventJSON...))
	if e.fail {
		return fmt.Errorf("handler boom")
	}
	return nil
}

func (e *dlqReplayExecutor) got() ([]string, [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.handlers...), append([][]byte(nil), e.payloads...)
}

// dlqReplayRunner wires the REAL runner (over a recording executor) through the
// real worker query socket and returns deps whose DLQ opener yields store. It is
// the full end-to-end replay path without Docker.
func dlqReplayRunner(t *testing.T, store *fakeDLQStore, exec *dlqReplayExecutor) Dependencies {
	t.Helper()
	deps := withFakeDLQ(t, store)

	// Two matching event rules: a replay must run only the recorded handler, not
	// every matching rule.
	tmpl, err := function.ParseTemplate([]byte(`runtime: node24
events:
  - handler: events.created.handler
    pattern: {}
  - handler: events.other.handler
    pattern: {}
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	prepared := runner.NewPrepared(
		function.Function{Name: "user-events", Template: tmpl},
		&runtime.Prepared{Name: "user-events", Image: "x"},
		exec,
	)
	run := runner.New([]*runner.PreparedFunction{prepared}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	startTestSocketWithReplayer(t, deps.SocketPath, run)
	return deps
}

// TestDLQReplayExecutesExactHandlerAndDeletes verifies the end-to-end replay: the
// exact stored function/handler/event reaches the live runner over the socket,
// only that handler executes, the success line is printed, and the entry is
// deleted.
func TestDLQReplayExecutesExactHandlerAndDeletes(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err != nil {
		t.Fatalf("dlq replay: %v", err)
	}
	if strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("stdout = %q", out)
	}
	handlers, payloads := exec.got()
	if len(handlers) != 1 || handlers[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v, want exactly [events.created.handler]", handlers)
	}
	if !strings.Contains(string(payloads[0]), "INSERT") {
		t.Fatalf("replayed payload = %q", payloads[0])
	}
	if len(store.entries) != 0 {
		t.Fatalf("successful replay must delete exactly the entry: %+v", store.entries)
	}
}

// TestDLQReplayRemovedHandlerKeepsEntry verifies a removed handler fails with a
// clear error, runs nothing, and keeps the entry.
func TestDLQReplayRemovedHandlerKeepsEntry(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.removed.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "replay 1-0") {
		t.Fatalf("err = %v, want a replay error", err)
	}
	if hs, _ := exec.got(); len(hs) != 0 {
		t.Fatalf("a removed handler must not execute: %v", hs)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a removed handler must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayFailedHandlerKeepsEntry verifies a handler that executes and
// fails keeps the entry and returns a nonzero error.
func TestDLQReplayFailedHandlerKeepsEntry(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{fail: true}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "handler boom") {
		t.Fatalf("err = %v, want the handler failure surfaced", err)
	}
	if hs, _ := exec.got(); len(hs) != 1 {
		t.Fatalf("the handler must have executed once: %v", hs)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a failed handler must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayUnknownFunctionKeepsEntry verifies a function no longer in the
// registry is surfaced and the entry kept.
func TestDLQReplayUnknownFunctionKeepsEntry(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "ghost", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("err = %v, want it to name the unknown function", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unknown function must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayMalformedPlaceholderNotReplayable verifies a malformed-message
// placeholder (no function/handler, non-JSON event) is rejected as
// non-replayable BEFORE dialing the worker: the error names why, it never
// reports a misleading socket-unavailability, no invalid JSON is encoded, and
// the entry is kept.
func TestDLQReplayMalformedPlaceholderNotReplayable(t *testing.T) {
	placeholder := dlqTestEntry("1-0", "1-0", "-", "-", "-", "2026-09-23T10:00:00Z")
	placeholder.HandlerAttempts = 0
	store := &fakeDLQStore{entries: []stream.DLQEntry{placeholder}}
	// No socket server is started: a dial attempt would surface as an
	// unavailable-worker error, which is exactly what this path must not do.
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil {
		t.Fatal("a malformed-message placeholder must not be replayable")
	}
	if !strings.Contains(err.Error(), `DLQ entry "1-0" is not replayable`) {
		t.Fatalf("err = %v, want a concise non-replayable error", err)
	}
	if strings.Contains(err.Error(), "invocation unavailable") ||
		strings.Contains(err.Error(), "unexpected end of JSON") {
		t.Fatalf("err = %v, must not report socket unavailability or a JSON encode failure", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a non-replayable entry must be kept: %+v", store.entries)
	}
}

// TestDLQReplayMalformedPlaceholderDoesNotExecute verifies the guard runs before
// the live worker seam: even with a wired replayer, a placeholder entry never
// reaches it and the entry stays.
func TestDLQReplayMalformedPlaceholderDoesNotExecute(t *testing.T) {
	placeholder := dlqTestEntry("1-0", "1-0", "-", "-", "-", "2026-09-23T10:00:00Z")
	placeholder.HandlerAttempts = 0
	store := &fakeDLQStore{entries: []stream.DLQEntry{placeholder}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "is not replayable") {
		t.Fatalf("err = %v, want a non-replayable error", err)
	}
	if hs, _ := exec.got(); len(hs) != 0 {
		t.Fatalf("a placeholder must not execute any handler: %v", hs)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a non-replayable entry must be kept: %+v", store.entries)
	}
}

// TestDLQReplayWorkerUnavailableKeepsEntry verifies an unavailable worker is a
// hard error (no offline fallback) and the entry is kept.
func TestDLQReplayWorkerUnavailableKeepsEntry(t *testing.T) {
	store := &fakeDLQStore{entries: []stream.DLQEntry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	// No socket server started at deps.SocketPath.
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "replay 1-0") {
		t.Fatalf("err = %v, want a replay error", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unavailable worker must keep the entry: %+v", store.entries)
	}
}
