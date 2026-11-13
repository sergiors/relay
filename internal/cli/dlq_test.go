package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/dlq"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/schedule"
)

// fakeDLQStore is an in-memory dlq.Store for CLI tests: it preserves insertion
// order (the Redis stream order the real store returns) and records deletes, so
// the command/presentation layer is exercised without Redis.
//
// The replay-admission seam is modeled as one claimant per entry with a
// monotonic fencing token: a second claim while one is held is refused
// (ErrReplayInProgress), only the CURRENT token may release or success-delete,
// and a stale token's release/delete are no-ops. The lease clock and expiry
// semantics themselves belong to the state package's real-PostgreSQL coverage;
// the fake holds a claim until it is released or superseded.
type fakeDLQStore struct {
	entries []dlq.Entry
	deleted []string
	// listErr/getErr/deleteErr inject failures.
	listErr   error
	getErr    error
	deleteErr error
	closed    bool

	// mu guards the replay-claim model: the concurrent-admission tests drive
	// two CLI commands against one store instance.
	mu        sync.Mutex
	claimGen  map[string]int64 // per-entry fencing generations ever allocated
	heldClaim map[string]int64 // per-entry currently held token (absent = unclaimed)
	releases  []replayRelease  // recorded release calls, in call order
}

// replayRelease records one ReleaseDLQReplay call (the entry ID and the token
// the caller presented), so tests can prove which outcomes release a claim and
// which token was presented.
type replayRelease struct {
	id    string
	token int64
}

func (f *fakeDLQStore) ListDLQEntries(context.Context) ([]dlq.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]dlq.Entry(nil), f.entries...), nil
}

func (f *fakeDLQStore) GetDLQEntry(_ context.Context, id string) (dlq.Entry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return dlq.Entry{}, false, f.getErr
	}
	for _, e := range f.entries {
		if e.ID == id {
			return e, true, nil
		}
	}
	return dlq.Entry{}, false, nil
}

// DeleteDLQEntry mirrors the production removal boundary: an UNSETTLED entry
// (its source delivery may still be recoverable) is refused with
// ErrEntryUnsettled, an entry with an active replay claim is refused with
// ErrReplayInProgress, and anything else is removed exactly by its full ID.
func (f *fakeDLQStore) DeleteDLQEntry(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return false, f.deleteErr
	}
	for i, e := range f.entries {
		if e.ID != id {
			continue
		}
		if e.SettledAt == "" {
			return false, fmt.Errorf("delete %s: %w", id, dlq.ErrEntryUnsettled)
		}
		if f.heldClaim[id] > 0 {
			return false, fmt.Errorf("delete %s: %w", id, dlq.ErrReplayInProgress)
		}
		f.entries = append(f.entries[:i], f.entries[i+1:]...)
		f.deleted = append(f.deleted, id)
		return true, nil
	}
	return false, nil
}

// ClaimDLQReplay models the production admission seam: a settled entry is
// claimable exactly once at a time, the fencing generation is monotonic, and a
// held claim refuses later claimants before any worker dial.
func (f *fakeDLQStore) ClaimDLQReplay(_ context.Context, id string, lease time.Duration) (dlq.ReplayClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return dlq.ReplayClaim{}, f.getErr
	}
	for _, e := range f.entries {
		if e.ID != id {
			continue
		}
		if e.SettledAt == "" {
			return dlq.ReplayClaim{}, fmt.Errorf("claim %s: %w", id, dlq.ErrEntryUnsettled)
		}
		if f.heldClaim[id] > 0 {
			return dlq.ReplayClaim{}, fmt.Errorf("claim %s: %w", id, dlq.ErrReplayInProgress)
		}
		if f.claimGen == nil {
			f.claimGen, f.heldClaim = map[string]int64{}, map[string]int64{}
		}
		f.claimGen[id]++
		f.heldClaim[id] = f.claimGen[id]
		return dlq.ReplayClaim{Entry: e, Token: f.claimGen[id], LeaseUntil: time.Now().Add(lease)}, nil
	}
	return dlq.ReplayClaim{}, fmt.Errorf("claim %s: %w", id, dlq.ErrEntryNotFound)
}

// ReleaseDLQReplay records the release and takes the claim down only when the
// presented token is still the current claimant's (a stale release is a no-op).
func (f *fakeDLQStore) ReleaseDLQReplay(_ context.Context, id string, token int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases = append(f.releases, replayRelease{id: id, token: token})
	if f.heldClaim[id] == token {
		delete(f.heldClaim, id)
	}
	return nil
}

// DeleteDLQReplay models the fenced success delete: only the entry currently
// held under the presented token is removed; any other token fails closed with
// ErrReplayClaimLost and the entry is retained.
func (f *fakeDLQStore) DeleteDLQReplay(_ context.Context, id string, token int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heldClaim[id] != token {
		return fmt.Errorf("delete %s: %w", id, dlq.ErrReplayClaimLost)
	}
	for i, e := range f.entries {
		if e.ID == id {
			f.entries = append(f.entries[:i], f.entries[i+1:]...)
			f.deleted = append(f.deleted, id)
			delete(f.heldClaim, id)
			return nil
		}
	}
	return fmt.Errorf("delete %s: %w", id, dlq.ErrReplayClaimLost)
}

// SaveDLQEntry appends the entry (the fixture seed seam).
func (f *fakeDLQStore) SaveDLQEntry(_ context.Context, entry dlq.Entry) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, entry)
	return true, nil
}

func (f *fakeDLQStore) MarkDLQDeliverySettled(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (f *fakeDLQStore) ListUnsettledDLQDeliveries(_ context.Context, _ string, _ int) ([]dlq.DeliveryRef, error) {
	return nil, nil
}

func (f *fakeDLQStore) Close() error { f.closed = true; return nil }

// releasedTokensFor returns the tokens presented to ReleaseDLQReplay for id, in
// call order.
func (f *fakeDLQStore) releasedTokensFor(id string) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var tokens []int64
	for _, r := range f.releases {
		if r.id == id {
			tokens = append(tokens, r.token)
		}
	}
	return tokens
}

// heldToken returns the claim token currently held for id (false when the
// entry is unclaimed).
func (f *fakeDLQStore) heldToken(id string) (int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	token, ok := f.heldClaim[id]
	return token, ok
}

// supersedeClaim models the held claim's lease expiring while its handler is
// still running and a newer claimant immediately re-claiming: the fencing
// generation is incremented and the new claimant holds the row.
func (f *fakeDLQStore) supersedeClaim(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimGen[id]++
	f.heldClaim[id] = f.claimGen[id]
}

// withFakeDLQStore returns dependencies whose DLQ opener yields the given
// store, so the plain in-memory fake can drive the command tree. It closes the store
// after the command.
func withFakeDLQStore(t *testing.T, store dlq.Store) Dependencies {
	t.Helper()
	deps := testDeps(t)
	deps.OpenDLQ = func(context.Context, *slog.Logger) (dlq.Store, func(), error) {
		// dlq.Store has no Close (the opener's cleanup owns closing); fakes
		// that model one (recording it for assertions) are closed when present.
		return store, func() {
			if closer, ok := store.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}, nil
	}
	return deps
}

// withFakeDLQ is withFakeDLQStore for the plain in-memory fake.
func withFakeDLQ(t *testing.T, store *fakeDLQStore) Dependencies {
	t.Helper()
	return withFakeDLQStore(t, store)
}

// dlqTestEntry builds a current-format entry with sensible defaults. A real
// invocation carries the entry's stored replay payload (the exact logical
// handler payload the stream persisted when dead-lettering); the "-" malformed
// placeholder carries none and is never replayable.
func dlqTestEntry(id, originalID, function, handler, event, timestamp string) dlq.Entry {
	entry := dlq.Entry{
		ID:              id,
		Source:          "events",
		SourceMessageID: originalID,
		SourceGroup:     "relay",
		Consumer:        "worker-1",
		Event:           event,
		Reason:          fmt.Sprintf("invocation exhausted: function %q handler %q exhausted after 5 handler attempts", function, handler),
		App:             function,
		Handler:         handler,
		Deliveries:      9,
		HandlerAttempts: 5,
		Timestamp:       timestamp,
		SettledAt:       timestamp,
	}
	if function != "-" && handler != "-" {
		entry.ReplayPayload = []byte(event)
	}
	return entry
}

// dlqHexID builds a deterministic full-length (64-character)
// lowercase hexadecimal entry ID out of lowercase-hex fragments: the fragments
// are joined and the remainder is zero-padded to 64. It gives tests control
// over displayed leading characters while every produced ID keeps the exact
// shape a real dlq.NewID value has. It never returns invalid hex or a non-64
// length.
func dlqHexID(parts ...string) string {
	id := strings.Join(parts, "")
	if !dlqHexOnly(id) || len(id) > 64 {
		return ""
	}
	return id + strings.Repeat("0", 64-len(id))
}

// dlqHexOnly reports whether s consists solely of lowercase hex characters.
func dlqHexOnly(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Deterministic full-length entry-ID fixtures for the CLI unit tests. Real IDs
// come from dlq.NewID (SHA-256 rendered lowercase hex); the fake stores used
// here constrain only the same shape, so tests build fixed IDs with controlled
// leading hexadecimal characters via dlqHexID.
var (
	// dlqUnitID is the full-length ID of the single entry most CLI unit
	// fixtures seed.
	dlqUnitID = dlqHexID("deadc0de01")

	// dlqUnitSecondID is a second entry ID with different leading digits.
	dlqUnitSecondID = dlqHexID("deadc0de02")

	// dlqMissingID is a well-formed full-length ID that no fixture stores, so
	// exact-ID lookups exercise the unknown-entry path.
	dlqMissingID = dlqHexID("ff00cc11ff")
)

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

// TestDLQListHelpClarifiesIDDisplay verifies the list help explains that
// abbreviated IDs are display-only and documents --no-trunc.
func TestDLQListHelpClarifiesIDDisplay(t *testing.T) {
	out, _, err := runCLI(t, "", "dlq", "ls", "--help")
	if err != nil {
		t.Fatalf("dlq ls --help: err = %v, want nil", err)
	}
	for _, want := range []string{"--no-trunc", "display-only", "full ID"} {
		if !strings.Contains(out, want) {
			t.Fatalf("dlq ls --help missing %q:\n%s", want, out)
		}
	}
}

// TestDLQListColumnsAndOrder verifies ls renders one row per entry in stream
// order with the display-only abbreviated ID, app, handler, attempts, and age —
// no ORIGINAL column — and that two entries from the SAME source message ID are
// listed as distinct rows.
func TestDLQListColumnsAndOrder(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, "1699999999999-0", "alpha", "events.a.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
		// Same original_id, different exhausted invocation: a multi-handler
		// message produces one entry each.
		dlqTestEntry(dlqUnitSecondID, "1699999999999-0", "alpha", "events.b.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqMissingID, "1699999999998-0", "beta", "index.run", `{"b":2}`, "2026-09-23T09:00:00Z"),
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
	if got := normWS(lines[0]); got != "ID APP HANDLER ATTEMPTS AGE" {
		t.Fatalf("header = %q, want exactly \"ID APP HANDLER ATTEMPTS AGE\"", got)
	}
	// Stream order is preserved (not sorted by anything else), and every row
	// shows the display-only abbreviated ID while omitting the original source
	// message.
	for i, id := range []string{dlqUnitID, dlqUnitSecondID, dlqMissingID} {
		fields := strings.Fields(lines[i+1])
		gotID := ""
		if len(fields) > 0 {
			gotID = fields[0]
		}
		if len(fields) < 5 || gotID != id[:12] {
			t.Fatalf("row %d ID = %q, want exactly the first 12 characters %q",
				i+1, gotID, id[:12])
		}
	}
	if strings.Contains(out, "1699999999999-0") {
		t.Fatalf("ls has no ORIGINAL column; the original source ID leaked:\n%s", out)
	}
	// Both same-source rows still render their exact app/handler and attempts.
	for _, row := range []string{lines[1], lines[2]} {
		if !strings.Contains(row, "alpha") || !strings.Contains(row, "5") {
			t.Fatalf("same-source row missing metadata: %q", row)
		}
	}
	if !strings.Contains(lines[1], "events.a.handler") || !strings.Contains(lines[2], "events.b.handler") {
		t.Fatalf("rows must carry their distinct handlers:\n%s", out)
	}
}

// TestDLQListNoTruncShowsCompleteIDs verifies `dlq ls --no-trunc` renders each
// entry's complete 64-character ID as its ID cell — the exact identity
// inspect, replay, and rm require — that the displayed value is usable
// verbatim as that identifier, and that rendering never rewrites the stored
// entries. The default abbreviation is pinned in TestDLQListColumnsAndOrder.
func TestDLQListNoTruncShowsCompleteIDs(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, "1699999999999-0", "alpha", "events.a.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqUnitSecondID, "1699999999998-0", "beta", "index.run", `{"b":2}`, "2026-09-23T09:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	// When: the listing is rendered with --no-trunc.
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls", "--no-trunc")
	if err != nil {
		t.Fatalf("dlq ls --no-trunc: %v", err)
	}
	// Then: one row per entry, each row's ID cell the complete ID.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected header + 2 rows, got %d:\n%s", len(lines), out)
	}
	if got := normWS(lines[0]); got != "ID APP HANDLER ATTEMPTS AGE" {
		t.Fatalf("header = %q, want exactly \"ID APP HANDLER ATTEMPTS AGE\"", got)
	}
	for i, id := range []string{dlqUnitID, dlqUnitSecondID} {
		fields := strings.Fields(lines[i+1])
		if len(fields) < 5 || fields[0] != id {
			t.Fatalf("row %d = %q, want its ID cell to be the complete ID %q",
				i+1, lines[i+1], id)
		}
	}
	// And: the copied value IS the exact identifier inspect accepts.
	shownID := strings.Fields(lines[1])[0]
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", shownID); err != nil {
		t.Fatalf("dlq inspect %s: %v", shownID, err)
	}
	// Display-only: the stored entries keep their complete IDs.
	if store.entries[0].ID != dlqUnitID || store.entries[1].ID != dlqUnitSecondID {
		t.Fatalf("ls --no-trunc must not modify stored entries: %+v", store.entries)
	}
}

// TestDLQListEmpty verifies an empty DLQ renders just the header (not an error).
func TestDLQListEmpty(t *testing.T) {
	deps := withFakeDLQ(t, &fakeDLQStore{})
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls empty: %v", err)
	}
	if got := normWS(out); got != "ID APP HANDLER ATTEMPTS AGE" {
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
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, "1699999999999-0", "alpha", "events.a.handler", `{"event_name":"INSERT","id":7}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	flat := normWS(out)
	for _, want := range []string{
		"ID: " + dlqUnitID,
		"Original stream: events",
		"Original ID: 1699999999999-0",
		"Group: relay",
		"Consumer: worker-1",
		"App: alpha",
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
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "-", "-", "-", "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	if !strings.Contains(out, "Event:\n-\n") {
		t.Fatalf("non-JSON event must render verbatim:\n%s", out)
	}
}

// TestDLQInspectTraceOptional verifies the optional trace lineage renders when
// present and the Trace line is omitted entirely for a pre-tracing entry.
func TestDLQInspectTraceOptional(t *testing.T) {
	const lineage = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	withTrace := dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "h", `{}`, "2026-09-23T10:00:00Z")
	withTrace.Trace = lineage
	deps := withFakeDLQ(t, &fakeDLQStore{entries: []dlq.Entry{withTrace}})
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	if !strings.Contains(normWS(out), "Trace: "+lineage) {
		t.Fatalf("inspect output missing the trace lineage:\n%s", out)
	}

	withoutTrace := dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "h", `{}`, "2026-09-23T10:00:00Z")
	deps = withFakeDLQ(t, &fakeDLQStore{entries: []dlq.Entry{withoutTrace}})
	out, _, err = runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	if strings.Contains(out, "Trace:") {
		t.Fatalf("a pre-tracing entry must not render a Trace line:\n%s", out)
	}
}

// TestDLQInspectUnknownID verifies an unknown ID is a clear error.
func TestDLQInspectUnknownID(t *testing.T) {
	deps := withFakeDLQ(t, &fakeDLQStore{})
	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqMissingID)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unknown DLQ entry %q", dlqMissingID)) {
		t.Fatalf("err = %v, want unknown-entry error", err)
	}
}

// TestDLQCommandsTreatArbitraryUnknownIDsAsUnknown verifies inspect, replay,
// and rm use the exact supplied ID. Short or malformed values are ordinary
// misses: they return the established unknown-entry error without execution or
// deletion.
func TestDLQCommandsTreatArbitraryUnknownIDsAsUnknown(t *testing.T) {
	store := &fakeDLQStore{}
	deps := withFakeDLQ(t, store)
	replayer := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, replayer)

	unknownIDs := []string{
		"deadbeef01", // abbreviated prefix
		"not-a-hex-id",
		"1-0",
	}
	for _, cmd := range []string{"inspect", "replay", "rm"} {
		for _, id := range unknownIDs {
			_, _, err := runCLIWithDeps(t, deps, "", "dlq", cmd, id)
			if want := fmt.Sprintf("unknown DLQ entry %q", id); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("dlq %s %q err = %v, want %q", cmd, id, err, want)
			}
		}
	}
	if calls, _, _, _ := replayer.got(); calls != 0 {
		t.Fatalf("unknown replay IDs reached the worker %d times", calls)
	}
	if len(store.deleted) != 0 || len(store.entries) != 0 {
		t.Fatalf("unknown IDs changed the store: deleted=%v entries=%+v", store.deleted, store.entries)
	}
	if len(store.heldClaim) != 0 {
		t.Fatalf("unknown replay IDs claimed entries: %v", store.heldClaim)
	}
}

// TestDLQInspectTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQInspectTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "inspect", dlqMissingID, "extra")
	if err == nil || !strings.Contains(err.Error(), "dlq inspect: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestDLQRmRemovesExactlyOne verifies rm deletes exactly the named entry (XDEL
// semantics: one ID) and leaves the others.
func TestDLQRmRemovesExactlyOne(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "h", `{}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqUnitSecondID, dlqUnitSecondID, "beta", "h", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq rm: %v", err)
	}
	if strings.TrimSpace(out) != "DLQ entry removed" {
		t.Fatalf("rm output = %q", out)
	}
	if len(store.entries) != 1 || store.entries[0].ID != dlqUnitSecondID {
		t.Fatalf("rm must remove exactly one entry: %+v", store.entries)
	}
}

// TestDLQRmUnknownID verifies rm of an unknown ID is a clear error and removes
// nothing.
func TestDLQRmUnknownID(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{dlqTestEntry(dlqUnitID, dlqUnitID, "a", "h", `{}`, "2026-09-23T10:00:00Z")}}
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", dlqMissingID)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unknown DLQ entry %q", dlqMissingID)) {
		t.Fatalf("err = %v, want unknown-entry error", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("unknown rm must not delete anything: %+v", store.entries)
	}
}

// TestDLQRmTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQRmTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "rm", dlqMissingID, "extra")
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
	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqMissingID)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unknown DLQ entry %q", dlqMissingID)) {
		t.Fatalf("err = %v, want unknown-entry error", err)
	}
}

// TestDLQReplayTooManyArgs verifies extra positional arguments are a usage error.
func TestDLQReplayTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "dlq", "replay", dlqMissingID, "extra")
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
	if err == nil || !strings.Contains(err.Error(), "PostgreSQL is not configured") {
		t.Fatalf("err = %v, want a clear unconfigured-store error", err)
	}
}

// recordingReplayer captures the DLQ-replay arguments the CLI forwards over the
// socket, so a test can prove the entry's optional trace lineage travels on the
// control path (separately from the event payload). onReplay, when non-nil,
// runs synchronously inside the replay exchange (mid-execution): it is the hook
// concurrent-admission tests use to interleave a competing store operation at
// exactly the moment the handler is running.
type recordingReplayer struct {
	mu              sync.Mutex
	calls           int
	trace           string
	sourceMessageID string
	event           []byte
	onReplay        func()
}

func (r *recordingReplayer) ReplayDLQ(_ context.Context, _, _ string, event []byte, trace, sourceMessageID string) error {
	r.mu.Lock()
	r.calls++
	r.event = append([]byte(nil), event...)
	r.trace = trace
	r.sourceMessageID = sourceMessageID
	if r.onReplay != nil {
		hook := r.onReplay
		r.mu.Unlock()
		hook()
		r.mu.Lock()
	}
	defer r.mu.Unlock()
	return nil
}

func (r *recordingReplayer) got() (int, []byte, string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, append([]byte(nil), r.event...), r.trace, r.sourceMessageID
}

// TestDLQReplayUnsettledEntryDoesNotExecute verifies the settlement preflight
// runs before the worker socket/replay: an unsettled source delivery may still
// be recoverable, so replay must not execute a handler whose entry cannot yet be
// deleted.
func TestDLQReplayUnsettledEntryDoesNotExecute(t *testing.T) {
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z")
	entry.SettledAt = ""
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil || !strings.Contains(err.Error(), "is not settled") {
		t.Fatalf("err = %v, want an explicit not-settled error", err)
	}
	handlers, _ := exec.got()
	if len(handlers) != 0 {
		t.Fatalf("an unsettled entry must not execute: handlers = %v", handlers)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unsettled entry must be kept: %+v", store.entries)
	}
}

// TestDLQReplayForwardsTraceControlPath proves the replay sends the entry's
// optional trace lineage to the worker as control-path metadata, separate from
// the event payload (the payload never contains the lineage).
func TestDLQReplayForwardsTraceControlPath(t *testing.T) {
	const lineage = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z")
	entry.Trace = lineage
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	deps := withFakeDLQ(t, store)

	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq replay: %v", err)
	}
	if strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("stdout = %q", out)
	}
	calls, event, trace, sourceMessageID := rep.got()
	if calls != 1 {
		t.Fatalf("replayer calls = %d, want 1", calls)
	}
	if trace != lineage {
		t.Fatalf("forwarded trace = %q, want %q", trace, lineage)
	}
	if sourceMessageID != entry.SourceMessageID {
		t.Fatalf("forwarded source message ID = %q, want %q", sourceMessageID, entry.SourceMessageID)
	}
	if strings.Contains(string(event), lineage) {
		t.Fatalf("lineage leaked into the event payload: %q", event)
	}
	if len(store.entries) != 0 {
		t.Fatalf("successful replay must delete the entry: %+v", store.entries)
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
	return dlqReplayRunnerWithExecutor(t, store, exec)
}

// dlqReplayRunnerWithExecutor is dlqReplayRunner with a caller-supplied
// runner.Executor, so scenarios can substitute behavior like a handler that
// blocks mid-execution while other operations race the held replay claim.
func dlqReplayRunnerWithExecutor(t *testing.T, store *fakeDLQStore, exec runner.Executor) Dependencies {
	t.Helper()
	return dlqReplayRunnerOn(t, withFakeDLQ(t, store), exec)
}

// dlqReplayRunnerOn wires the caller-built dependencies (deeper tests override
// deps.OpenDLQ afterwards) with the real runner over exec at deps.SocketPath.
// Two matching event rules: a replay must run only the recorded handler, not
// every matching rule.
func dlqReplayRunnerOn(t *testing.T, deps Dependencies, exec runner.Executor) Dependencies {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(`runtime: nodejs24.x
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
		app.App{Name: "user-events", Template: tmpl},
		&runtime.Prepared{Name: "user-events", Image: "x"},
		exec,
	)
	run := runner.New([]*runner.PreparedApp{prepared}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	startTestSocketWithReplayer(t, deps.SocketPath, run)
	return deps
}

// TestDLQReplayExecutesExactHandlerAndDeletes verifies the end-to-end replay: the
// exact stored app/handler/event reaches the live runner over the socket,
// only that handler executes, the success line is printed, and the entry is
// deleted.
func TestDLQReplayExecutesExactHandlerAndDeletes(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
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
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.removed.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil || !strings.Contains(err.Error(), "replay "+dlqUnitID) {
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
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{fail: true}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
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

// TestDLQReplayUnknownAppKeepsEntry verifies an app no longer in the
// registry is surfaced and the entry kept.
func TestDLQReplayUnknownAppKeepsEntry(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "ghost", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("err = %v, want it to name the unknown function", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unknown function must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayMalformedPlaceholderNotReplayable verifies a malformed-message
// placeholder (no app/handler, non-JSON event) is rejected as
// non-replayable BEFORE dialing the worker: the error names why, it never
// reports a misleading socket-unavailability, no invalid JSON is encoded, and
// the entry is kept.
func TestDLQReplayMalformedPlaceholderNotReplayable(t *testing.T) {
	placeholder := dlqTestEntry(dlqUnitID, dlqUnitID, "-", "-", "-", "2026-09-23T10:00:00Z")
	placeholder.HandlerAttempts = 0
	store := &fakeDLQStore{entries: []dlq.Entry{placeholder}}
	// No socket server is started: a dial attempt would surface as an
	// unavailable-worker error, which is exactly what this path must not do.
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil {
		t.Fatal("a malformed-message placeholder must not be replayable")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("DLQ entry %q is not replayable", dlqUnitID)) {
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
	placeholder := dlqTestEntry(dlqUnitID, dlqUnitID, "-", "-", "-", "2026-09-23T10:00:00Z")
	placeholder.HandlerAttempts = 0
	store := &fakeDLQStore{entries: []dlq.Entry{placeholder}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
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
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	// No socket server started at deps.SocketPath.
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil || !strings.Contains(err.Error(), "replay "+dlqUnitID) {
		t.Fatalf("err = %v, want a replay error", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unavailable worker must keep the entry: %+v", store.entries)
	}
}

// TestOpenPostgresDLQStoreRequiresDatabaseURL pins that the production
// `relay dlq` opener selects the PostgreSQL state store and reports a missing
// DATABASE_URL clearly, without needing Redis or a running worker.
func TestOpenPostgresDLQStoreRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, _, err := openPostgresDLQStore(context.Background(), logger); err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("openPostgresDLQStore without DATABASE_URL = %v, want an error naming DATABASE_URL", err)
	}
}

// scheduleReplayOccurrence is the exhausted schedule occurrence the replay tests
// work with: exactly what an exhausted schedule invocation dead-letters (the
// envelope's app/schedule/handler and the scheduled instant).
func scheduleReplayOccurrence() schedule.Occurrence {
	return schedule.Occurrence{
		App:         "user-events",
		Schedule:    "cleanup",
		Handler:     "events.created.handler",
		ScheduledAt: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC),
	}
}

// scheduleReplayEnvelope returns the exact stream body the DLQ stores for the
// exhausted schedule invocation: the occurrence's envelope, not its handler
// payload (see stream's schedule exhaustion path).
func scheduleReplayEnvelope(t *testing.T) string {
	t.Helper()
	envelope, err := scheduleReplayOccurrence().Envelope()
	if err != nil {
		t.Fatalf("schedule envelope: %v", err)
	}
	return string(envelope)
}

// TestDLQReplayScheduleEnvelopeInspectedAsStoredEnvelope verifies `dlq inspect`
// keeps showing the FULL stored schedule envelope — with the coordination fields
// (app, schedule, handler, occurrence_id) an operator needs to identify the
// dead-lettered occurrence — and leaves the entry untouched.
func TestDLQReplayScheduleEnvelopeInspectedAsStoredEnvelope(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", scheduleReplayEnvelope(t), "2026-10-01T08:05:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	for _, want := range []string{
		`"app": "user-events"`,
		`"handler": "events.created.handler"`,
		`"schedule": "cleanup"`,
		`"occurrence_id": "schedule:user-events:cleanup:2026-10-01T08:00:00Z"`,
		`"scheduled_at": "2026-10-01T08:00:00Z"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect must show the stored schedule envelope; missing %q\n%s", want, out)
		}
	}
	if len(store.entries) != 1 || store.entries[0].Event != scheduleReplayEnvelope(t) {
		t.Fatalf("inspect must keep the entry's stored event verbatim: %+v", store.entries)
	}
}

// TestDLQReplayScheduleStoredPayloadExecutesHandlerPayload verifies the
// full-stack replay of an exhausted schedule invocation: the DLQ row carries
// the coordination envelope in its diagnostic Event body (what the stream
// dead-lettered) and Occurrence.Payload() — the bytes the LIVE schedule path
// handed the runner — as its stored replay payload. The real runner, reached
// over the real worker socket, executes exactly the recorded handler exactly
// once with the stored payload, and the successful replay deletes the entry.
func TestDLQReplayScheduleStoredPayloadExecutesHandlerPayload(t *testing.T) {
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler",
		scheduleReplayEnvelope(t), "2026-10-01T08:05:00Z")
	entry.ReplayPayload = scheduleReplayOccurrence().Payload()
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
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
	if string(payloads[0]) != string(scheduleReplayOccurrence().Payload()) {
		t.Fatalf(
			"replayed payload = %q, want exactly Occurrence.Payload() %q",
			payloads[0], scheduleReplayOccurrence().Payload(),
		)
	}
	if strings.Contains(string(payloads[0]), "occurrence_id") {
		t.Fatalf("replay leaked the envelope's coordination fields into the handler payload: %q", payloads[0])
	}
	if len(store.entries) != 0 {
		t.Fatalf("a successful schedule replay must delete the entry: %+v", store.entries)
	}
}

// TestDLQReplayScheduleStoredPayloadForwardsTrace verifies the schedule
// replay still carries the entry's optional trace lineage to the worker as
// control-path metadata, separate from (and alongside) the stored handler
// payload.
func TestDLQReplayScheduleStoredPayloadForwardsTrace(t *testing.T) {
	const lineage = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler",
		scheduleReplayEnvelope(t), "2026-10-01T08:05:00Z")
	entry.ReplayPayload = scheduleReplayOccurrence().Payload()
	entry.Trace = lineage
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	deps := withFakeDLQ(t, store)
	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID); err != nil {
		t.Fatalf("dlq replay: %v", err)
	}
	calls, event, trace, _ := rep.got()
	if calls != 1 {
		t.Fatalf("replayer calls = %d, want 1", calls)
	}
	if trace != lineage {
		t.Fatalf("forwarded trace = %q, want %q", trace, lineage)
	}
	if string(event) != string(scheduleReplayOccurrence().Payload()) {
		t.Fatalf(
			"forwarded event = %q, want the handler payload %q",
			event, scheduleReplayOccurrence().Payload(),
		)
	}
}

// TestDLQReplayScheduleClaimWithoutPayloadKeepsEntryPreDial verifies a
// malformed schedule claim — the kind whose dead-lettering produced NO handler
// payload (the stream routes it non-retryably to the DLQ with a nil replay
// payload) — is refused with an explicit error BEFORE anything is dialed or
// executed, and the entry is kept for inspection.
func TestDLQReplayScheduleClaimWithoutPayloadKeepsEntryPreDial(t *testing.T) {
	var envelope map[string]any
	if err := json.Unmarshal([]byte(scheduleReplayEnvelope(t)), &envelope); err != nil {
		t.Fatalf("unmarshal schedule envelope: %v", err)
	}
	delete(envelope, "handler")
	malformed, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal malformed envelope: %v", err)
	}
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler",
		string(malformed), "2026-10-01T08:05:00Z")
	entry.ReplayPayload = nil // the malformed claim never produced a payload
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	// A wired replayer records ANY worker-side call, so it doubles as proof the
	// socket seam was never reached.
	deps := withFakeDLQ(t, store)
	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	_, _, err = runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil {
		t.Fatal("a schedule claim with no stored payload must not be replayed")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("DLQ entry %q is not replayable", dlqUnitID)) ||
		!strings.Contains(err.Error(), "no stored handler payload") {
		t.Fatalf("err = %v, want the no-stored-payload replay error naming the resolved entry", err)
	}
	if calls, _, _, _ := rep.got(); calls != 0 {
		t.Fatalf("a payload-less claim must fail before the replayer: calls = %d", calls)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a payload-less claim must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayNonReplayablePayloadlessEventKeepsEntryPreDial verifies an
// entry with a real app/handler but no stored replay payload (a malformed body
// that never reached a handler) is rejected before the worker (no misleading
// socket-unavailable error), executes nothing, and keeps the entry.
func TestDLQReplayNonReplayablePayloadlessEventKeepsEntryPreDial(t *testing.T) {
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `[1,2,3]`, "2026-10-01T08:05:00Z")
	entry.ReplayPayload = nil
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	deps := withFakeDLQ(t, store)
	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID)
	if err == nil {
		t.Fatal("an entry with no stored handler payload must not be replayed")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("DLQ entry %q is not replayable", dlqUnitID)) ||
		!strings.Contains(err.Error(), "no stored handler payload") {
		t.Fatalf("err = %v, want an explicit pre-dial replay error naming the resolved entry", err)
	}
	if strings.Contains(err.Error(), "invocation unavailable") {
		t.Fatalf("err = %v, must not report a misleading worker unavailability", err)
	}
	if calls, _, _, _ := rep.got(); calls != 0 {
		t.Fatalf("a payload-less event must fail before the replayer: calls = %d", calls)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a payload-less event must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayOrdinaryEventShipsStoredCanonicalPayload proves the ordinary
// replay path forwards the STORED replay payload — the canonical JSON the live
// runner handed the handler — and never the diagnostic Event body: key order,
// whitespace, and formatting of the dead-lettered body never reach the wire.
// (The socket passes the payload through as a raw message; the executor
// therefore receives exactly the stored bytes.)
func TestDLQReplayOrdinaryEventShipsStoredCanonicalPayload(t *testing.T) {
	// The Event body has whitespace and non-lexicographic key order — none of
	// that is the handler's logical payload. The stream stored the canonical
	// re-encoding of the decoded event (sorted keys, compact).
	const storedBody = "{\n  \"zzz\": 1,\n  \"entry_name\": \"INSERT\"\n}"
	const canonical = `{"entry_name":"INSERT","zzz":1}`
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", storedBody, "2026-10-01T08:05:00Z")
	entry.ReplayPayload = []byte(canonical)
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID); err != nil {
		t.Fatalf("dlq replay: %v", err)
	}
	_, payloads := exec.got()
	if len(payloads) != 1 || string(payloads[0]) != canonical {
		t.Fatalf(
			"replayed payload = %q, want the stored canonical payload %q",
			payloads, canonical,
		)
	}
	if strings.Contains(string(payloads[0]), "\n") {
		t.Fatalf("the diagnostic Event body leaked onto the wire: %q", payloads[0])
	}
	if len(store.entries) != 0 {
		t.Fatalf("a successful ordinary replay must delete the entry: %+v", store.entries)
	}
}
