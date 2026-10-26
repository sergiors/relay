//go:build integration

package state

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"relay/internal/app"
)

// invalidParseErr is a representative deterministic template diagnostic. The
// state layer treats the error opaquely; only the caller-supplied identity
// classifies the failure.
type invalidParseErr struct{ msg string }

func (e *invalidParseErr) Error() string { return e.msg }

// recordInvalidSource is the shared call shape for a deterministic parse failure
// of the exact bytes whose identity is supplied. A nil observation means "take a
// fresh observation now", which is the correct pre-read snapshot when the test
// does not model a concurrent update.
func recordInvalidSource(
	t *testing.T,
	st *State,
	name, identity string,
	err error,
	observed *AppObservation,
) InvalidDesiredResult {
	t.Helper()
	if observed == nil {
		o := observeApp(t, st, name)
		observed = &o
	}
	result, writeErr := st.RecordInvalidDesiredWithSource(
		context.Background(), name, identity, err, observed)
	if writeErr != nil {
		t.Fatalf("RecordInvalidDesiredWithSource(%q): %v", name, writeErr)
	}
	return result
}

func observeApp(t *testing.T, st *State, name string) AppObservation {
	t.Helper()
	observation, err := st.ObserveApp(context.Background(), name)
	if err != nil {
		t.Fatalf("ObserveApp(%q): %v", name, err)
	}
	return observation
}

// TestRecordInvalidDesiredWithSourceFreshRowRecordsIdentity pins the first
// observation of a fresh invalid template: the exact content identity and the
// deterministic error are persisted, the app is unavailable (nothing ever
// served), and the returned token owns the new row.
func TestRecordInvalidDesiredWithSourceFreshRowRecordsIdentity(t *testing.T) {
	c := openTestState(t)
	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	parseErr := &invalidParseErr{msg: `app "broken": unknown runtime "python9.9"`}

	result := recordInvalidSource(t, c, "broken", identity, parseErr, nil)
	if !result.Wrote {
		t.Fatal("first invalid observation must write")
	}
	fencingToken := result.FencingToken
	if fencingToken <= 0 {
		t.Fatalf("fencing token = %d, want a positive token", fencingToken)
	}

	detail, ok := c.GetApp("broken")
	if !ok {
		t.Fatal("a first invalid template must insert a visible row")
	}
	if detail.Status != StatusUnavailable {
		t.Fatalf("status = %q, want unavailable (no usable generation)", detail.Status)
	}
	if detail.InvalidSource != identity {
		t.Fatalf("invalid_source = %q, want %q", detail.InvalidSource, identity)
	}
	if detail.LastReconcileStatus != ReconcileFailed || detail.LastError != parseErr.Error() {
		t.Fatalf("outcome = %q/%q, want failed with the parse error", detail.LastReconcileStatus, detail.LastError)
	}
	if detail.Image != "" || detail.Fingerprint != "" || detail.PreparedAt != "" {
		t.Fatalf("fresh invalid row must have no active generation: %q/%q/%q",
			detail.Image, detail.Fingerprint, detail.PreparedAt)
	}
	if obs := observeApp(t, c, "broken"); !obs.Found || obs.FencingToken != fencingToken {
		t.Fatalf("observation = %+v, want found with token %d", obs, fencingToken)
	}
}

// TestRecordInvalidDesiredWithSourceRepeatedIdentityIsReadOnly pins the
// idempotency contract: the exact same invalid bytes are the persisted invalid
// desired state, so a repeat allocates no fencing token and changes no field,
// while the retained active generation keeps serving.
func TestRecordInvalidDesiredWithSourceRepeatedIdentityIsReadOnly(t *testing.T) {
	c := openTestState(t)
	fn := fnFor(t, "fn", mustTemplate(t, twoHandlerTmpl))
	prepared := time.Now().Add(-time.Hour)

	fencingToken := seedFencingToken(t, c, fn)
	if landed, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-active", "fp-active", prepared, fn, fencingToken); err != nil || !landed {
		t.Fatalf("seed success: landed=%v err=%v", landed, err)
	}

	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	parseErr := &invalidParseErr{msg: `app "fn": unknown runtime "python9.9"`}
	invalidResult := recordInvalidSource(t, c, "fn", identity, parseErr, nil)
	if !invalidResult.Wrote {
		t.Fatal("first invalid observation must write")
	}
	invalidToken := invalidResult.FencingToken
	first, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("invalid template must keep the row")
	}
	if first.Status != StatusDegraded {
		t.Fatalf("status = %q, want degraded (active generation retained)", first.Status)
	}
	if first.Image != "img-active" || first.Fingerprint != "fp-active" {
		t.Fatalf("active generation = %q/%q, want preserved", first.Image, first.Fingerprint)
	}

	// A second scan of the SAME bytes (even carrying a differently worded error)
	// is the same invalid desired definition: no write and no token allocation.
	repeatResult := recordInvalidSource(t, c, "fn", identity,
		&invalidParseErr{msg: "a different wording must not matter"}, nil)
	if repeatResult.Wrote || repeatResult.Stale {
		t.Fatalf("repeated identical invalid bytes = %+v, want the read-only fast path", repeatResult)
	}
	if repeatResult.FencingToken != invalidToken {
		t.Fatalf("repeat returned token %d, want the existing token %d", repeatResult.FencingToken, invalidToken)
	}

	second, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("repeated invalid observation must keep the row")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated invalid observation changed the row:\n before: %+v\n  after: %+v", first, second)
	}
	if obs := observeApp(t, c, "fn"); obs.FencingToken != invalidToken {
		t.Fatalf("persisted token = %d, want unchanged %d", obs.FencingToken, invalidToken)
	}
	if got := c.ActiveImages()["fn"]; got != "img-active" {
		t.Fatalf("active image = %q, want img-active retained", got)
	}
}

// TestRecordInvalidDesiredWithSourceChangedBytesAdvanceTransition pins that a
// different invalid byte sequence is a NEW transition: it writes, advances the
// fencing token, and replaces the recorded identity and diagnostic.
func TestRecordInvalidDesiredWithSourceChangedBytesAdvanceTransition(t *testing.T) {
	c := openTestState(t)
	fn := fnFor(t, "fn", mustTemplate(t, twoHandlerTmpl))
	fencingToken := seedFencingToken(t, c, fn)
	if landed, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-active", "fp-active", time.Now(), fn, fencingToken); err != nil || !landed {
		t.Fatalf("seed success: landed=%v err=%v", landed, err)
	}

	identityA := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	identityB := app.TemplateIdentity([]byte("runtime: python9.9\n# changed\n"))
	resultA := recordInvalidSource(t, c, "fn", identityA,
		&invalidParseErr{msg: "error A"}, nil)
	if !resultA.Wrote {
		t.Fatal("first invalid observation must write")
	}
	resultB := recordInvalidSource(t, c, "fn", identityB,
		&invalidParseErr{msg: "error B"}, nil)
	if !resultB.Wrote {
		t.Fatal("changed invalid bytes must write a new transition")
	}
	if resultB.FencingToken <= resultA.FencingToken {
		t.Fatalf("changed-bytes token = %d, want it to advance past %d",
			resultB.FencingToken, resultA.FencingToken)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.InvalidSource != identityB {
		t.Fatalf("invalid_source = %q, want the changed identity %q", detail.InvalidSource, identityB)
	}
	if detail.LastError != "error B" {
		t.Fatalf("last_error = %q, want error B", detail.LastError)
	}
	if detail.Image != "img-active" {
		t.Fatalf("image = %q, want the active generation retained", detail.Image)
	}
}

// TestRecordInvalidDesiredWithSourceOperationalFailureClearsAndRetries pins the
// failure-class boundary: an operational failure (no read identity) always
// writes and CLEARS the prior deterministic identity, so a transient read can
// never be mistaken for the earlier invalid input. Re-observing that earlier
// input afterwards is therefore a fresh transition, not a fast-path skip.
func TestRecordInvalidDesiredWithSourceOperationalFailureClearsAndRetries(t *testing.T) {
	c := openTestState(t)
	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))

	invalidResult := recordInvalidSource(t, c, "fn", identity,
		&invalidParseErr{msg: "parse failed"}, nil)
	if !invalidResult.Wrote {
		t.Fatal("first invalid observation must write")
	}

	opErr := errors.New("app \"fn\": read template: permission denied")
	operationalResult := recordInvalidSource(t, c, "fn", "", opErr, nil)
	if !operationalResult.Wrote {
		t.Fatal("an operational failure must stay retryable and write")
	}
	if operationalResult.FencingToken <= invalidResult.FencingToken {
		t.Fatalf("operational failure token = %d, want it to advance past %d",
			operationalResult.FencingToken, invalidResult.FencingToken)
	}
	detail, _ := c.GetApp("fn")
	if detail.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want cleared by an operational failure", detail.InvalidSource)
	}
	if detail.LastError != opErr.Error() {
		t.Fatalf("last_error = %q, want the operational error", detail.LastError)
	}

	// Seeing the formerly-persisted invalid bytes again is a new transition
	// because the operational failure cleared the identity.
	againResult := recordInvalidSource(t, c, "fn", identity,
		&invalidParseErr{msg: "parse failed again"}, nil)
	if !againResult.Wrote {
		t.Fatal("re-observing bytes after an operational failure must write")
	}
	if againResult.FencingToken <= operationalResult.FencingToken {
		t.Fatalf("re-observation token = %d, want it to advance past %d",
			againResult.FencingToken, operationalResult.FencingToken)
	}
	detail, _ = c.GetApp("fn")
	if detail.InvalidSource != identity {
		t.Fatalf("invalid_source = %q, want the re-observed identity %q", detail.InvalidSource, identity)
	}
}

// TestRecordInvalidDesiredWithSourceValidCorrectionClearsIdentityAndRecovers
// pins recovery: a valid desired write clears the invalid identity, and the
// subsequent successful reconcile replaces the retained active generation with a
// ready row.
func TestRecordInvalidDesiredWithSourceValidCorrectionClearsIdentityAndRecovers(t *testing.T) {
	c := openTestState(t)
	fn := fnFor(t, "fn", mustTemplate(t, twoHandlerTmpl))
	oldToken := seedFencingToken(t, c, fn)
	if landed, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-old", "fp-old", time.Now(), fn, oldToken); err != nil || !landed {
		t.Fatalf("seed success: landed=%v err=%v", landed, err)
	}

	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	if result := recordInvalidSource(t, c, "fn", identity,
		&invalidParseErr{msg: "parse failed"}, nil); !result.Wrote {
		t.Fatal("invalid observation must write")
	}
	invalid, _ := c.GetApp("fn")
	invalidObs := observeApp(t, c, "fn")
	if invalid.Status != StatusDegraded || invalid.InvalidSource != identity {
		t.Fatalf("invalid state = %+v, want degraded with identity", invalid)
	}

	// A valid correction is the normal desired-generation write. It starts from a
	// fresh snapshot, so the invalid-source identity is cleared.
	validToken, err := c.RecordPreparingWithFingerprint(context.Background(), "fn", fn, "fp-corrected")
	if err != nil {
		t.Fatalf("RecordPreparingWithFingerprint: %v", err)
	}
	if validToken <= 0 {
		t.Fatalf("valid correction token = %d, want positive", validToken)
	}
	if validToken == invalidObs.FencingToken {
		t.Fatal("a valid correction must advance the fencing token")
	}
	preparing, _ := c.GetApp("fn")
	if preparing.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want cleared by a valid desired write", preparing.InvalidSource)
	}
	if preparing.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing", preparing.Status)
	}
	if preparing.Image != "img-old" {
		t.Fatalf("image = %q, want the old generation retained until success", preparing.Image)
	}

	landed, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-new", "fp-corrected", time.Now(), fn, validToken)
	if err != nil || !landed {
		t.Fatalf("RecordReconcileSuccess: landed=%v err=%v", landed, err)
	}
	ready, _ := c.GetApp("fn")
	if ready.Status != StatusReady {
		t.Fatalf("status = %q, want ready after recovery", ready.Status)
	}
	if ready.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want empty on a ready row", ready.InvalidSource)
	}
	if ready.Image != "img-new" || ready.LastError != "" {
		t.Fatalf("recovered row = %q/%q, want img-new with no error", ready.Image, ready.LastError)
	}
}

// TestRecordInvalidDesiredWithSourceStaleObservationRejected pins the
// optimistic guard: an invalid observation that began before another worker's
// newer write is discarded. It neither overwrites that newer state nor claims the
// newer fencing token.
func TestRecordInvalidDesiredWithSourceStaleObservationRejected(t *testing.T) {
	dsn := newTestDSN(t)
	older := testOpenOrFail(t, dsn)
	newer := testOpenOrFail(t, dsn)

	fn := fnFor(t, "shared", mustTemplate(t, twoHandlerTmpl))
	token := seedFencingToken(t, older, fn)
	if landed, err := older.RecordReconcileSuccess(context.Background(), "shared", "img-valid", "fp-valid", time.Now(), fn, token); err != nil || !landed {
		t.Fatalf("seed success: landed=%v err=%v", landed, err)
	}

	// The older worker snapshots the current row BEFORE it reads the template.
	observed := observeApp(t, older, "shared")
	if !observed.Found {
		t.Fatal("expected the valid row to be observable")
	}

	// A newer worker persists a valid correction while the older observation is
	// still in flight.
	newToken, err := newer.RecordDiscoveredWithFingerprint(context.Background(), fn, "fp-newer")
	if err != nil {
		t.Fatalf("newer RecordDiscoveredWithFingerprint: %v", err)
	}
	if newToken <= observed.FencingToken {
		t.Fatalf("newer token = %d, want it to advance past %d", newToken, observed.FencingToken)
	}
	corrected, _ := newer.GetApp("shared")

	// The stale invalid observation must be rejected: no write, no token claim,
	// and (crucially) not an operational error.
	stale := recordInvalidSource(t, older, "shared",
		app.TemplateIdentity([]byte("runtime: python9.9\n")),
		&invalidParseErr{msg: "stale parse failure"}, &observed)
	if stale.Wrote {
		t.Fatal("a stale invalid observation must not write")
	}
	if !stale.Stale {
		t.Fatalf("stale observation result = %+v, want Stale=true", stale)
	}
	if stale.FencingToken != 0 {
		t.Fatalf("stale observation returned token %d, want zero (fail closed)", stale.FencingToken)
	}

	after, ok := older.GetApp("shared")
	if !ok {
		t.Fatal("stale observation must not remove the newer row")
	}
	if !reflect.DeepEqual(corrected, after) {
		t.Fatalf("stale invalid observation overwrote newer state:\n got: %+v\nwant: %+v", after, corrected)
	}
	if after.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want the newer valid row untouched", after.InvalidSource)
	}
}

// TestRecordInvalidDesiredWithSourceABAIdentityIsNotFence pins two things at
// once: a content identity is never a fencing token (A -> B -> A must allocate a
// fresh token for the second A), and a completion carrying the first A's token
// cannot land after the later transitions.
func TestRecordInvalidDesiredWithSourceABAIdentityIsNotFence(t *testing.T) {
	c := openTestState(t)
	fn := fnFor(t, "fn", mustTemplate(t, twoHandlerTmpl))
	identityA := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	identityB := app.TemplateIdentity([]byte("runtime: python9.9\n# b\n"))

	resultA1 := recordInvalidSource(t, c, "fn", identityA,
		&invalidParseErr{msg: "A1"}, nil)
	if !resultA1.Wrote {
		t.Fatal("first A must write")
	}
	resultB := recordInvalidSource(t, c, "fn", identityB,
		&invalidParseErr{msg: "B"}, nil)
	if !resultB.Wrote || resultB.FencingToken <= resultA1.FencingToken {
		t.Fatalf("B write = %+v, want a fresh token after %d", resultB, resultA1.FencingToken)
	}
	resultA2 := recordInvalidSource(t, c, "fn", identityA,
		&invalidParseErr{msg: "A2"}, nil)
	if !resultA2.Wrote {
		t.Fatal("A -> B -> A must treat the second A as a new transition")
	}
	if resultA2.FencingToken <= resultB.FencingToken {
		t.Fatalf("second A token = %d, want it to advance past B's %d (identity is not a fence)",
			resultA2.FencingToken, resultB.FencingToken)
	}
	if resultA2.FencingToken == resultA1.FencingToken {
		t.Fatal("the second A must not reuse the first A's fencing token")
	}

	// A completion captured for the first A is stale and must not land.
	landed, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-stale", "fp-stale", time.Now(), fn, resultA1.FencingToken)
	if err != nil {
		t.Fatalf("stale RecordReconcileSuccess: %v", err)
	}
	if landed {
		t.Fatal("a completion carrying the first A's token must be rejected")
	}
	detail, _ := c.GetApp("fn")
	if detail.InvalidSource != identityA || detail.Image != "" {
		t.Fatalf("row = %+v, want the final A invalid state untouched by the stale completion", detail)
	}
}

// TestRecordInvalidDesiredWithSourceCrossWorkerSameIdentityIsReadOnly pins
// cross-worker idempotency: a second worker observing the same persisted identity
// gets the existing token and performs no write.
// TestRecordInvalidDesiredWithSourceMissingObservationRejected pins the
// fail-closed guard: the guarded form requires a real observation. A missing
// (unreadable) state view is not evidence that the row is absent, so it must not
// authorize an unconditional write.
func TestRecordInvalidDesiredWithSourceMissingObservationRejected(t *testing.T) {
	c := openTestState(t)
	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))

	_, err := c.RecordInvalidDesiredWithSource(context.Background(), "broken", identity,
		&invalidParseErr{msg: "parse failed"}, nil)
	if !errors.Is(err, ErrMissingObservation) {
		t.Fatalf("error = %v, want ErrMissingObservation", err)
	}
	if _, ok := c.GetApp("broken"); ok {
		t.Fatal("a missing observation must not create a row")
	}
}

// TestRecordInvalidDesiredWithSourceWriteFailureIsOperationalError pins the
// other half of the distinction: a real PostgreSQL failure returns an error and
// is never reported as a benign stale rejection.
func TestRecordInvalidDesiredWithSourceWriteFailureIsOperationalError(t *testing.T) {
	c := openTestState(t)
	observed := observeApp(t, c, "broken")

	if _, err := c.db.ExecContext(context.Background(),
		`ALTER TABLE apps RENAME TO apps_unwritable`); err != nil {
		t.Fatalf("make apps unwritable: %v", err)
	}
	result, err := c.RecordInvalidDesiredWithSource(context.Background(), "broken",
		app.TemplateIdentity([]byte("runtime: python9.9\n")),
		&invalidParseErr{msg: "parse failed"}, &observed)
	if err == nil {
		t.Fatalf("write failure result = %+v, want an operational error", result)
	}
	if result.Stale || result.Wrote || result.FencingToken != 0 {
		t.Fatalf("write failure result = %+v, want no write and no stale flag", result)
	}
	if _, err := c.db.ExecContext(context.Background(),
		`ALTER TABLE apps_unwritable RENAME TO apps`); err != nil {
		t.Fatalf("restore apps table: %v", err)
	}
}

// TestObserveAppReadFailureIsNotAbsence pins the distinction a caller needs:
// a failed read returns an error, never Found=false. Treating it as absence
// would let a write bypass concurrency (or resurrect a removed app).
func TestObserveAppReadFailureIsNotAbsence(t *testing.T) {
	c := openTestState(t)
	rename := func(from, to string) {
		t.Helper()
		if _, err := c.db.ExecContext(context.Background(),
			`ALTER TABLE `+from+` RENAME TO `+to); err != nil {
			t.Fatalf("rename %s to %s: %v", from, to, err)
		}
	}

	rename("apps", "apps_unreadable")
	observation, err := c.ObserveApp(context.Background(), "missing")
	if err == nil {
		t.Fatalf("ObserveApp with an unreadable apps table = %+v, want an error", observation)
	}
	if observation.Found {
		t.Fatal("a failed read must not be reported as a found row")
	}
	if observation.Detail.Name != "" {
		t.Fatalf("a failed read must not synthesize a detail: %+v", observation.Detail)
	}

	rename("apps_unreadable", "apps")
	if _, ok := c.GetApp("missing"); ok {
		t.Fatal("a failed observation must not have created a row")
	}
}

// TestRecordInvalidDesiredWithSourceStaleObservationSameIdentityRejected pins
// that the observation check precedes the unchanged-identity fast path. A stale
// observer must not claim the token of a newer generation even when that newer
// row happens to carry the same invalid identity (A -> valid -> A).
func TestRecordInvalidDesiredWithSourceStaleObservationSameIdentityRejected(t *testing.T) {
	dsn := newTestDSN(t)
	older := testOpenOrFail(t, dsn)
	newer := testOpenOrFail(t, dsn)

	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	first := recordInvalidSource(t, older, "shared", identity,
		&invalidParseErr{msg: "first invalid"}, nil)
	if !first.Wrote {
		t.Fatal("first invalid observation must write")
	}
	observed := observeApp(t, older, "shared")

	// Another worker applies a valid correction (clears the identity) and then
	// the same invalid bytes reappear (a fresh transition with a newer token).
	fn := fnFor(t, "shared", mustTemplate(t, twoHandlerTmpl))
	if _, err := newer.RecordDiscoveredWithFingerprint(context.Background(), fn, "fp-valid"); err != nil {
		t.Fatalf("valid correction: %v", err)
	}
	second := recordInvalidSource(t, newer, "shared", identity,
		&invalidParseErr{msg: "second invalid"}, nil)
	if !second.Wrote || second.FencingToken <= first.FencingToken {
		t.Fatalf("second invalid = %+v, want a new token after %d", second, first.FencingToken)
	}

	stale := recordInvalidSource(t, older, "shared", identity,
		&invalidParseErr{msg: "stale observer"}, &observed)
	if stale.Wrote || !stale.Stale || stale.FencingToken != 0 {
		t.Fatalf("stale observation = %+v, want rejected with no token", stale)
	}
	after := observeApp(t, older, "shared")
	if after.FencingToken != second.FencingToken || after.Detail.InvalidSource != identity {
		t.Fatalf("stale observation changed the newer row: %+v", after)
	}
}

// TestRebuildFromAppsFailedReadLeavesObservationsConsistent pins that a rebuild
// failure is not "the database is empty": the transaction writes nothing, and a
// pre-read observation taken before the failure still reflects the real row
// (absence), so a subsequent guarded write applies correctly.
func TestRebuildFromAppsFailedReadLeavesObservationsConsistent(t *testing.T) {
	c := openTestState(t)
	observed := observeApp(t, c, "broken")
	if observed.Found {
		t.Fatal("precondition: broken must not exist")
	}

	rename := func(from, to string) {
		t.Helper()
		if _, err := c.db.ExecContext(context.Background(),
			`ALTER TABLE `+from+` RENAME TO `+to); err != nil {
			t.Fatalf("rename %s to %s: %v", from, to, err)
		}
	}
	rename("apps", "apps_unreadable")
	fn := fnFor(t, "seed", mustTemplate(t, twoHandlerTmpl))
	err := c.RebuildFromApps(context.Background(),
		[]DiscoveredApp{{App: fn, Fingerprint: "fp-seed"}})
	if err == nil {
		t.Fatal("RebuildFromApps with an unreadable apps table must fail, not assume empty")
	}
	rename("apps_unreadable", "apps")

	if _, ok := c.GetApp("seed"); ok {
		t.Fatal("a failed rebuild must not seed a partial row")
	}
	result := recordInvalidSource(t, c, "broken",
		app.TemplateIdentity([]byte("runtime: python9.9\n")),
		&invalidParseErr{msg: "parse failed"}, &observed)
	if !result.Wrote {
		t.Fatalf("guarded write after a failed rebuild = %+v, want it to write", result)
	}
}

func TestRecordInvalidDesiredWithSourceCrossWorkerSameIdentityIsReadOnly(t *testing.T) {
	dsn := newTestDSN(t)
	older := testOpenOrFail(t, dsn)
	newer := testOpenOrFail(t, dsn)

	identity := app.TemplateIdentity([]byte("runtime: python9.9\n"))
	first := recordInvalidSource(t, older, "shared", identity,
		&invalidParseErr{msg: "parse failed"}, nil)
	if !first.Wrote {
		t.Fatal("first invalid observation must write")
	}
	firstDetail, _ := older.GetApp("shared")

	observed := observeApp(t, newer, "shared")
	if !observed.Found || observed.Detail.InvalidSource != identity {
		t.Fatalf("newer observation = %+v, want the persisted identity", observed)
	}
	second := recordInvalidSource(t, newer, "shared", identity,
		&invalidParseErr{msg: "same bytes, same failure"}, &observed)
	if second.Wrote || second.Stale {
		t.Fatalf("second worker result = %+v, want the read-only fast path", second)
	}
	if second.FencingToken != first.FencingToken {
		t.Fatalf("second worker token = %d, want the existing %d", second.FencingToken, first.FencingToken)
	}
	secondDetail, _ := newer.GetApp("shared")
	if !reflect.DeepEqual(firstDetail, secondDetail) {
		t.Fatalf("cross-worker fast path changed the row:\n got: %+v\nwant: %+v", secondDetail, firstDetail)
	}
}
