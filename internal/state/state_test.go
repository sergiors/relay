package state

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
)

func mustTemplate(t *testing.T, s string) *app.Template {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(s))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

func fnFor(t *testing.T, name string, tmpl *app.Template) app.App {
	t.Helper()
	return app.App{Name: name, Dir: filepath.Join(t.TempDir(), name), Template: tmpl}
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
	path := newTestDSN(t)
	c1, err := testOpen(t, path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	tmpl := mustTemplate(t, twoHandlerTmpl)
	if _, err := c1.RecordDiscovered(context.Background(), fnFor(t, "alpha", tmpl)); err != nil {
		t.Errorf("state write error: %v", err)
	}
	_ = c1.Close()

	c2, err := testOpen(t, path) // reopen, init must be idempotent
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	if _, ok := c2.GetApp("alpha"); !ok {
		t.Fatal("expected function to survive reopen")
	}
}

// discovered -> success: image/fingerprint/prepared/handlers are upserted.
func TestDiscoveredThenSuccessReplacesActiveFields(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after discovered")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %s, want preparing", detail.Status)
	}
	if len(detail.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(detail.Handlers))
	}

	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-fn", "fp-new", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok = c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.Status != StatusReady {
		t.Fatalf("status = %s, want ready", detail.Status)
	}
	if detail.Image != "img-fn" {
		t.Fatalf("image = %q, want img-fn", detail.Image)
	}
	if detail.Fingerprint != "fp-new" {
		t.Fatalf("fingerprint = %q, want fp-new", detail.Fingerprint)
	}
	if detail.LastError != "" {
		t.Fatalf("last_error = %q, want cleared", detail.LastError)
	}
	if detail.DesiredFingerprint != "fp-new" {
		t.Fatalf("desired_fingerprint = %q, want fp-new (a success converges the desired generation)", detail.DesiredFingerprint)
	}
	if len(detail.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2 after success", len(detail.Handlers))
	}
}

// TestDiscoveryPreservesActiveGenerationAndOutcome pins startup rediscovery
// semantics: a re-discovery records the new desired fingerprint and resets the
// public status to preparing, but must NOT erase the last usable active
// generation (image/fingerprint/prepared_at) nor the previous last-reconcile
// outcome. This is what keeps the startup image sweep's keep reference and an
// operator-visible failure from vanishing on every restart.
func TestDiscoveryPreservesActiveGenerationAndOutcome(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	prepared := time.Now().Add(-time.Hour)

	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-active", "fp-active", prepared, fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, err := c.RecordReconcileFailure(context.Background(), "fn", &boomErr{}, token); err != nil {
		t.Errorf("state write error: %v", err)
	} // degraded, last reconciliation failed

	if _, err := c.RecordDiscoveredWithFingerprint(context.Background(), fn, "fp-desired-next"); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after rediscovery")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing after rediscovery", detail.Status)
	}
	if detail.Image != "img-active" || detail.Fingerprint != "fp-active" {
		t.Fatalf("active generation = %q/%q after rediscovery, want preserved img-active/fp-active",
			detail.Image, detail.Fingerprint)
	}
	if detail.PreparedAt != prepared.UTC().Format(time.RFC3339) {
		t.Fatalf("prepared_at = %q after rediscovery, want preserved %q", detail.PreparedAt, prepared.UTC().Format(time.RFC3339))
	}
	if detail.DesiredFingerprint != "fp-desired-next" {
		t.Fatalf("desired_fingerprint = %q after rediscovery, want fp-desired-next", detail.DesiredFingerprint)
	}
	if detail.LastReconcileStatus != ReconcileFailed || detail.LastReconcileAt == "" || detail.LastError != "boom" {
		t.Fatalf("reconcile outcome lost on rediscovery: status=%q at=%q error=%q",
			detail.LastReconcileStatus, detail.LastReconcileAt, detail.LastError)
	}
}

// TestDiscoveryFreshRowRecordsDesiredFingerprintOnly pins that the first write
// for an app (no prior row) records the desired fingerprint with an empty
// active generation and no fabricated reconcile outcome.
func TestDiscoveryFreshRowRecordsDesiredFingerprintOnly(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	if _, err := c.RecordDiscoveredWithFingerprint(context.Background(), fnFor(t, "fn", tmpl), "fp-first"); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after discovery")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing", detail.Status)
	}
	if detail.DesiredFingerprint != "fp-first" {
		t.Fatalf("desired_fingerprint = %q, want fp-first", detail.DesiredFingerprint)
	}
	if detail.Image != "" || detail.Fingerprint != "" || detail.PreparedAt != "" {
		t.Fatalf("fresh row must have no active generation: image=%q fingerprint=%q prepared_at=%q",
			detail.Image, detail.Fingerprint, detail.PreparedAt)
	}
	if detail.LastReconcileStatus != "" || detail.LastReconcileAt != "" || detail.LastError != "" {
		t.Fatalf("fresh row must have no reconcile outcome: status=%q at=%q error=%q",
			detail.LastReconcileStatus, detail.LastReconcileAt, detail.LastError)
	}
}

// TestSuccessReplacesActiveAndConvergesDesiredGeneration pins the success
// contract: a successful reconcile replaces the active image/fingerprint/
// prepared_at, clears the error, and sets DesiredFingerprint to the same final
// fingerprint (the successful generation IS the desired generation), so the next
// reconcile is a no-op skip rather than a spurious preparation.
func TestSuccessReplacesActiveAndConvergesDesiredGeneration(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)

	token := seedTokenWithFingerprint(t, c, fn, "fp-v1")
	if _, err := c.RecordReconcileFailure(context.Background(), "fn", &boomErr{}, token); err != nil {
		t.Errorf("state write error: %v", err)
	} // unavailable: no prior image

	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-v1", "fp-v1", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.Status != StatusReady {
		t.Fatalf("status = %q, want ready", detail.Status)
	}
	if detail.Image != "img-v1" || detail.Fingerprint != "fp-v1" || detail.DesiredFingerprint != "fp-v1" {
		t.Fatalf("active/desired = %q/%q/%q, want img-v1/fp-v1/fp-v1",
			detail.Image, detail.Fingerprint, detail.DesiredFingerprint)
	}
	if detail.LastError != "" || detail.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("outcome = error=%q status=%q, want cleared/success", detail.LastError, detail.LastReconcileStatus)
	}
}

// TestPreparingRecordsDesiredFingerprintAndRetainsActive pins the live
// desired-generation path: RecordPreparingWithFingerprint refreshes the desired
// fingerprint and status while preserving the active generation, and a following
// failure leaves both the active and desired generations intact but exposes the
// failure as degraded.
func TestPreparingRecordsDesiredFingerprintAndRetainsActive(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)

	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-v1", "fp-v1", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	next := requireDesired(t, "fn", func() (Token, error) {
		return c.RecordPreparingWithFingerprint(context.Background(), "fn", fn, "fp-v2")
	})
	requireToken(t, fn.Name, next)

	detail, _ := c.GetApp("fn")
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing", detail.Status)
	}
	if detail.DesiredFingerprint != "fp-v2" {
		t.Fatalf("desired_fingerprint = %q, want fp-v2", detail.DesiredFingerprint)
	}
	if detail.Image != "img-v1" || detail.Fingerprint != "fp-v1" {
		t.Fatalf("active generation = %q/%q, want preserved img-v1/fp-v1", detail.Image, detail.Fingerprint)
	}

	if _, err := c.RecordReconcileFailure(context.Background(), "fn", &boomErr{}, next); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, _ = c.GetApp("fn")
	if detail.Status != StatusDegraded {
		t.Fatalf("status = %q after failure, want degraded", detail.Status)
	}
	if detail.Image != "img-v1" || detail.Fingerprint != "fp-v1" || detail.DesiredFingerprint != "fp-v2" {
		t.Fatalf("generations after failure = active %q/%q desired %q, want img-v1/fp-v1/fp-v2",
			detail.Image, detail.Fingerprint, detail.DesiredFingerprint)
	}
}

func TestReconcileStatusTransitionsRetainHealthyGeneration(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-v1", "fp-v1", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	token = requireDesired(t, fn.Name, func() (Token, error) {
		return c.RecordPreparing(context.Background(), "fn", fn)
	})
	if got, _ := c.GetApp("fn"); got.Status != StatusPreparing || got.Image != "img-v1" {
		t.Fatalf("preparing status/image = %q/%q, want preparing/img-v1", got.Status, got.Image)
	}
	if _, err := c.RecordReconcileBuilding(context.Background(), "fn", token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if got, _ := c.GetApp("fn"); got.Status != StatusBuilding {
		t.Fatalf("building status = %q, want building", got.Status)
	}
	if _, err := c.RecordReconciling(context.Background(), "fn", token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if got, _ := c.GetApp("fn"); got.Status != StatusReconciling {
		t.Fatalf("reconciling status = %q, want reconciling", got.Status)
	}
	if _, err := c.RecordServiceFailure(context.Background(), "fn", &boomErr{}, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	got, _ := c.GetApp("fn")
	if got.Status != StatusDegraded || got.Image != "img-v1" {
		t.Fatalf("service failed status/image = %q/%q, want degraded/img-v1", got.Status, got.Image)
	}
}

func TestReconcileFailuresWithoutActiveImageAreUnavailable(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileFailure(context.Background(), "fn", &boomErr{}, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	got, _ := c.GetApp("fn")
	if got.Status != StatusUnavailable {
		t.Fatalf("image failure status = %q, want unavailable", got.Status)
	}
	token = requireDesired(t, fn.Name, func() (Token, error) {
		return c.RecordPreparing(context.Background(), "fn", fn)
	})
	if _, err := c.RecordServiceFailure(context.Background(), "fn", &boomErr{}, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	got, _ = c.GetApp("fn")
	if got.Status != StatusUnavailable {
		t.Fatalf("service failure status = %q, want unavailable", got.Status)
	}
}

// noRuntimeServiceTmpl is a no-runtime, external-image service-only template: it
// has no event/schedule handlers and its single service brings its own image, so
// a successful prepare produces NO app image (PreparedAt is the only usable
// generation marker).
const noRuntimeServiceTmpl = `services:
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 8080
`

// TestImageLessPreparedAppTransitionsToDegradedOnFailure pins the
// no-runtime external-image service-only case: a successful generation with an
// EMPTY app Image but a non-empty PreparedAt is still usable, so a later
// failed prepare or service reconcile must report degraded rather than
// unavailable. A never-successful row (discovered but never prepared) has
// neither marker and stays unavailable.
func TestImageLessPreparedAppTransitionsToDegradedOnFailure(t *testing.T) {
	t.Run("reconcile failure", func(t *testing.T) {
		c := openTestState(t)
		tmpl := mustTemplate(t, noRuntimeServiceTmpl)
		fn := fnFor(t, "svc", tmpl)

		// Successful no-runtime prepare: no app image, but prepared.
		token := seedToken(t, c, fn)
		if _, err := c.RecordReconcileSuccess(context.Background(), "svc", "", "fp-svc", time.Now(), fn, token); err != nil {
			t.Errorf("state write error: %v", err)
		}
		if got, _ := c.GetApp("svc"); got.Image != "" || got.PreparedAt == "" {
			t.Fatalf("precondition: image=%q prepared_at=%q, want empty image with prepared_at set",
				got.Image, got.PreparedAt)
		}

		if _, err := c.RecordReconcileFailure(context.Background(), "svc", &boomErr{}, token); err != nil {
			t.Errorf("state write error: %v", err)
		}
		got, _ := c.GetApp("svc")
		if got.Status != StatusDegraded {
			t.Fatalf("status = %q, want degraded (image-less but prepared generation is usable)", got.Status)
		}
		if got.PreparedAt == "" {
			t.Fatal("prepared_at must be preserved on failure")
		}
	})

	t.Run("service failure", func(t *testing.T) {
		c := openTestState(t)
		tmpl := mustTemplate(t, noRuntimeServiceTmpl)
		fn := fnFor(t, "svc", tmpl)

		token := seedToken(t, c, fn)
		if _, err := c.RecordReconcileSuccess(context.Background(), "svc", "", "fp-svc", time.Now(), fn, token); err != nil {
			t.Errorf("state write error: %v", err)
		}
		if _, err := c.RecordServiceFailure(context.Background(), "svc", &boomErr{}, token); err != nil {
			t.Errorf("state write error: %v", err)
		}
		got, _ := c.GetApp("svc")
		if got.Status != StatusDegraded {
			t.Fatalf("status = %q, want degraded (image-less but prepared generation is usable)", got.Status)
		}
	})

	t.Run("never successful stays unavailable", func(t *testing.T) {
		c := openTestState(t)
		tmpl := mustTemplate(t, noRuntimeServiceTmpl)
		fn := fnFor(t, "svc", tmpl)

		// Discovered but never prepared: no image and no prepared_at.
		token := seedToken(t, c, fn)
		if got, _ := c.GetApp("svc"); got.Image != "" || got.PreparedAt != "" {
			t.Fatalf("precondition: image=%q prepared_at=%q, want both empty", got.Image, got.PreparedAt)
		}

		if _, err := c.RecordReconcileFailure(context.Background(), "svc", &boomErr{}, token); err != nil {
			t.Errorf("state write error: %v", err)
		}
		got, _ := c.GetApp("svc")
		if got.Status != StatusUnavailable {
			t.Fatalf("status = %q, want unavailable (no successful generation ever)", got.Status)
		}

		token = requireDesired(t, fn.Name, func() (Token, error) {
			return c.RecordPreparing(context.Background(), "svc", fn)
		})
		if _, err := c.RecordServiceFailure(context.Background(), "svc", &boomErr{}, token); err != nil {
			t.Errorf("state write error: %v", err)
		}
		got, _ = c.GetApp("svc")
		if got.Status != StatusUnavailable {
			t.Fatalf("status = %q, want unavailable (no successful generation ever)", got.Status)
		}
	})
}

// KEY: a failed reconcile keeps the prior active image/fingerprint/prepared_at
// intact — the last good version still serves — but the failure is visible as
// degraded rather than hidden behind a ready status.
func TestReconcileFailureKeepsPriorActiveAndMarksFailed(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	prepared := time.Now().Add(-time.Hour)
	fn := fnFor(t, "fn", tmpl)

	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img-active", "fp-active", prepared, fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, err := c.RecordReconcileFailure(context.Background(), "fn", &boomErr{}, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after failure")
	}
	if detail.Status != StatusDegraded {
		t.Fatalf("status = %s, want degraded (prior usable image retained)", detail.Status)
	}
	if detail.Image != "img-active" {
		t.Fatalf("image = %q, want preserved img-active", detail.Image)
	}
	if detail.Fingerprint != "fp-active" {
		t.Fatalf("fingerprint = %q, want preserved fp-active", detail.Fingerprint)
	}
	if detail.LastReconcileStatus != ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", detail.LastReconcileStatus)
	}
	if detail.LastError == "" {
		t.Fatal("last_error should be set on failure")
	}
	// The prepared_at of the active version is preserved, not overwritten.
	if !strings.HasPrefix(detail.PreparedAt, prepared.UTC().Format(time.RFC3339)[:19]) {
		t.Fatalf("prepared_at = %s, want preserved active version time", detail.PreparedAt)
	}
}

type boomErr struct{}

func (*boomErr) Error() string { return "boom" }

// TestRecordReconcileSuccessAdvancesLastReconcileAt is the regression for the
// upsert that omitted last_reconcile_at: a success on an EXISTING row (one
// already seeded by RecordDiscovered) must persist its reconcile timestamp and
// status. A second, later success must advance the timestamp. Time is driven by
// the injected State clock, so no sleep is needed: the first write is stamped at
// t1 and the second at t1+2s (RFC3339 has 1s resolution).
func TestRecordReconcileSuccessAdvancesLastReconcileAt(t *testing.T) {
	c := openTestState(t)
	clock := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c.nowFn = func() time.Time { return clock }
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn) // existing row, empty reconcile fields

	img1, fp1 := "img-v1", "fp-v1"
	t1 := clock
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", img1, fp1, t1, fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	d1, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if d1.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", d1.LastReconcileStatus, ReconcileSuccess)
	}
	at1, err := time.Parse(time.RFC3339, d1.LastReconcileAt)
	if err != nil {
		t.Fatalf("first last_reconcile_at not a valid RFC3339 timestamp %q: %v", d1.LastReconcileAt, err)
	}

	// Advance the injected clock past the RFC3339 second resolution and record a
	// second success; the persisted timestamp must advance.
	clock = clock.Add(2 * time.Second)
	t2 := clock
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", img1, fp1, t2, fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	d2, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after second success")
	}
	if d2.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", d2.LastReconcileStatus, ReconcileSuccess)
	}
	at2, err := time.Parse(time.RFC3339, d2.LastReconcileAt)
	if err != nil {
		t.Fatalf("second last_reconcile_at not a valid RFC3339 timestamp %q: %v", d2.LastReconcileAt, err)
	}
	if !at2.After(at1) {
		t.Fatalf("last_reconcile_at did not advance: first=%s second=%s", at1, at2)
	}
}

// TestLastReconcileSurvivesDiscoveredUpsert pins the full upsert contract: (i) a
// success on an existing row persists its status and timestamp, and (ii) a
// subsequent re-discovery upsert PRESERVES that outcome view, because a discovery
// is not itself a meaningful reconcile but must not erase the last one. The
// public status is reset to preparing by the discovery (a new desired
// generation), while last_reconcile_status/last_reconcile_at and the active
// generation stay intact.
func TestLastReconcileSurvivesDiscoveredUpsert(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)

	// (i) success on the existing row persists status + timestamp.
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img", "fp", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}
	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want %s", detail.LastReconcileStatus, ReconcileSuccess)
	}
	if detail.LastReconcileAt == "" {
		t.Fatal("last_reconcile_at must be persisted by a success on an existing row")
	}

	// (ii) re-discovery preserves the outcome view and active generation, and
	// resets the public status to preparing.
	if _, err := c.RecordDiscovered(context.Background(), fnFor(t, "fn", tmpl)); err != nil {
		t.Errorf("state write error: %v", err)
	}
	detail, ok = c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after re-discovery")
	}
	if detail.LastReconcileStatus != ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s after re-discovery, want preserved %s", detail.LastReconcileStatus, ReconcileSuccess)
	}
	if detail.LastReconcileAt == "" {
		t.Fatal("last_reconcile_at must survive re-discovery")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %s after re-discovery, want preparing", detail.Status)
	}
	if detail.Image != "img" || detail.Fingerprint != "fp" {
		t.Fatalf("active generation = %q/%q after re-discovery, want preserved img/fp", detail.Image, detail.Fingerprint)
	}
}

// removal deletes the app row and its snapshot.
func TestRemovalDeletesRowAndHandlers(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img", "fp", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, err := c.RecordRemoved(context.Background(), "fn", token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, ok := c.GetApp("fn"); ok {
		t.Fatal("expected fn to be removed")
	}
	// Re-adding must not resurrect stale handlers (the old snapshot was deleted).
	if _, err := c.RecordDiscovered(context.Background(), fnFor(t, "fn", tmpl)); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, ok := c.GetApp("fn"); !ok {
		t.Fatal("expected fn re-added")
	}
}

// ListApps returns rows sorted by name with the expected shape.
func TestListAppsShapeAndSort(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	zeta := fnFor(t, "zeta", tmpl)
	zetaToken := seedToken(t, c, zeta)
	if _, err := c.RecordReconcileSuccess(context.Background(), "zeta", "i-z", "f-z", time.Now(), zeta, zetaToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, err := c.RecordDiscovered(context.Background(), fnFor(t, "alpha", tmpl)); err != nil {
		t.Errorf("state write error: %v", err)
	}
	mid := fnFor(t, "mid", tmpl)
	midToken := seedToken(t, c, mid)
	if _, err := c.RecordReconcileSuccess(context.Background(), "mid", "i-m", "f-m", time.Now(), mid, midToken); err != nil {
		t.Errorf("state write error: %v", err)
	}

	rows := c.ListApps()
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

// GetApp of an unknown name returns false with an empty detail.
func TestGetAppUnknownReturnsFalse(t *testing.T) {
	c := openTestState(t)
	detail, ok := c.GetApp("nope")
	if ok {
		t.Fatalf("expected not found, got %+v", detail)
	}
}

// RebuildFromFS populates an empty DB from a real apps tree using the real
// loader + fingerprint. A non-empty DB is left untouched.
func TestRebuildFromFSOnEmptyDB(t *testing.T) {
	root := t.TempDir()
	writeAppsDir(t, root)

	c := openTestState(t)
	if err := c.RebuildFromFS(context.Background(), root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	rows := c.ListApps()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Name != "demo" || rows[0].Runtime != "python3.14" || rows[0].Status != StatusPreparing {
		t.Fatalf("unexpected rebuild row: %+v", rows[0])
	}
	detail, ok := c.GetApp("demo")
	if !ok {
		t.Fatal("expected demo detail")
	}
	if len(detail.Handlers) != 2 {
		t.Fatalf("handler count = %d, want 2", len(detail.Handlers))
	}

	// A second rebuild on a non-empty DB must not duplicate rows.
	if err := c.RebuildFromFS(context.Background(), root); err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if got := len(c.ListApps()); got != 1 {
		t.Fatalf("rebuild on non-empty DB changed row count to %d, want 1", got)
	}
}

// TestRecordDiscoveredWithFingerprintPersistsCallerFingerprint proves the
// caller-supplied fingerprint is persisted verbatim as the DESIRED fingerprint
// even when the source changes AFTER the caller computed it. This is the worker's
// state-phase contract: the fingerprint is hashed once, reused for the rebuild
// and the discovery upsert, and a concurrent/next-moment source edit must not
// silently swap in a recomputed value. On a fresh row there is no usable active
// generation, so the active Fingerprint stays empty and only DesiredFingerprint is
// set. The old RecordDiscovered recomputes (so it would persist the changed
// value), which this test also pins as the standalone behavior.
func TestRecordDiscoveredWithFingerprintPersistsCallerFingerprint(t *testing.T) {
	root := t.TempDir()
	writeAppsDir(t, root)
	dir := filepath.Join(root, "demo")

	fn, err := app.LoadSingle(root, "demo")
	if err != nil {
		t.Fatalf("load single: %v", err)
	}
	computed, err := app.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	// Change the source after the fingerprint was computed. A recompute would
	// now yield a different value.
	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte("def handler(e): return 999\n"), 0o644); err != nil {
		t.Fatalf("rewrite source: %v", err)
	}
	changed, err := app.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint after change: %v", err)
	}
	if changed == computed {
		t.Fatal("source edit did not change the fingerprint; test setup is broken")
	}

	c := openTestState(t)
	if _, err := c.RecordDiscoveredWithFingerprint(context.Background(), fn, computed); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("demo")
	if !ok {
		t.Fatal("expected demo row")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing", detail.Status)
	}
	if detail.DesiredFingerprint != computed {
		t.Fatalf("desired_fingerprint = %q, want the caller-supplied %q (not a recompute %q)",
			detail.DesiredFingerprint, computed, changed)
	}
	if detail.Fingerprint != "" {
		t.Fatalf("active fingerprint = %q, want empty on a fresh row (no usable generation yet)", detail.Fingerprint)
	}

	// The legacy API keeps its recompute semantics for standalone callers: it
	// observes the changed source and persists the new digest as the desired one.
	if _, err := c.RecordDiscovered(context.Background(), fn); err != nil {
		t.Errorf("state write error: %v", err)
	}
	detail, _ = c.GetApp("demo")
	if detail.DesiredFingerprint != changed {
		t.Fatalf("RecordDiscovered desired_fingerprint = %q, want recomputed %q", detail.DesiredFingerprint, changed)
	}
}

// TestRebuildFromAppsUsesCallerFingerprintsAndLoadedSet proves the worker's
// rebuild seed consumes the already-loaded set and caller fingerprints without
// re-reading /apps: the app's Dir does not exist on disk, yet the row
// is written with the supplied fingerprint. A second call on a now-populated DB
// is a no-op.
func TestRebuildFromAppsUsesCallerFingerprintsAndLoadedSet(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	// A Dir that was never created: if the rebuild tried to load or fingerprint
	// from it, this test would observe an error or an empty fingerprint.
	fn := app.App{Name: "ghost", Dir: filepath.Join(t.TempDir(), "missing"), Template: tmpl}
	const callerFP = "caller-supplied-fingerprint"

	if err := c.RebuildFromApps(context.Background(), []DiscoveredApp{{App: fn, Fingerprint: callerFP}}); err != nil {
		t.Fatalf("rebuild from apps: %v", err)
	}

	detail, ok := c.GetApp("ghost")
	if !ok {
		t.Fatal("expected ghost row after rebuild")
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("status = %q, want preparing", detail.Status)
	}
	if detail.DesiredFingerprint != callerFP {
		t.Fatalf("desired_fingerprint = %q, want the caller-supplied %q", detail.DesiredFingerprint, callerFP)
	}

	// A populated DB is never overwritten by a second seed.
	fn2 := app.App{Name: "other", Dir: filepath.Join(t.TempDir(), "other"), Template: tmpl}
	if err := c.RebuildFromApps(context.Background(), []DiscoveredApp{{App: fn2, Fingerprint: "other-fp"}}); err != nil {
		t.Fatalf("second rebuild from apps: %v", err)
	}
	if _, ok := c.GetApp("other"); ok {
		t.Fatal("rebuild on a non-empty DB must be a no-op")
	}
}

// RecordReconcileSuccess round-trips a fingerprint-versioned image reference
// like "relay-app-user-events:3f8a2c1d..." exactly, since the state DB is the
// authoritative persisted holder of the full fingerprinted image.
func TestRecordReconcileSuccessRoundTripsFingerprintedImage(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	img := "relay-app-user-events:3f8a2c1d9b6e4a17"
	fp := "3f8a2c1d9b6e4a1700aa11bb22cc33dd44ee55ff66778899aabbccddeeff0011"
	fn := fnFor(t, "user-events", tmpl)
	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "user-events", img, fp, time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("user-events")
	if !ok {
		t.Fatal("expected row after success")
	}
	if detail.Image != img {
		t.Fatalf("image = %q, want %q", detail.Image, img)
	}
	if detail.Fingerprint != fp {
		t.Fatalf("fingerprint = %q, want %q", detail.Fingerprint, fp)
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

// TestPruneRemovedSweepsReservedStagingRow pins the buggy-discovery cleanup: a
// persisted row for a Relay-owned staging name (git's ".sync-*") is stale debris
// and is pruned even when a transient directory of that name exists on disk. A
// genuinely present normal app survives, and the reserved prune removes its
// app_stats too.
func TestPruneRemovedSweepsReservedStagingRow(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// A reserved name that (from a previous bad discovery) has a state row and
	// app_stats, plus a transient stage directory on disk.
	stage := fnFor(t, ".sync-old", tmpl)
	stageToken := seedToken(t, c, stage)
	if _, err := c.RecordReconcileSuccess(context.Background(), ".sync-old", "img-stage", "fp-stage", time.Now(), stage, stageToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	c.RecordAppStats(AppStats{App: ".sync-old", EventsMatchedTotal: 3})
	// A normal app that must survive.
	kept := fnFor(t, "kept", tmpl)
	keptToken := seedToken(t, c, kept)
	if _, err := c.RecordReconcileSuccess(context.Background(), "kept", "img-kept", "fp-kept", time.Now(), kept, keptToken); err != nil {
		t.Errorf("state write error: %v", err)
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".sync-old"), 0o755); err != nil {
		t.Fatalf("mkdir stage: %v", err)
	}
	keptDir := filepath.Join(root, "kept")
	if err := os.MkdirAll(keptDir, 0o755); err != nil {
		t.Fatalf("mkdir kept: %v", err)
	}
	if err := os.WriteFile(filepath.Join(keptDir, "template.yaml"), []byte(twoHandlerTmpl), 0o644); err != nil {
		t.Fatalf("write kept template: %v", err)
	}

	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, ok := c.GetApp(".sync-old"); ok {
		t.Fatal("a reserved staging row must be pruned (stale debris), even while the dir exists")
	}
	if _, ok := c.AppStats(".sync-old"); ok {
		t.Fatal("a reserved staging row's app_stats must be pruned too")
	}
	if _, ok := c.GetApp("kept"); !ok {
		t.Fatal("a normal function must survive the reserved prune")
	}
}

// TestPruneRemovedDoesNotDropRealHiddenInvalidDir pins that the reservation is
// narrow: a persisted row for a genuinely invalid user directory (a hidden name
// that merely looks similar) is NOT unconditionally pruned by the reserved
// branch. It is kept while template.yaml is PRESENT (an invalid desired
// definition still exists on disk) and pruned by the normal candidate rule once
// the template disappears.
func TestPruneRemovedDoesNotDropRealHiddenInvalidDir(t *testing.T) {
	c := openTestState(t)

	if _, err := c.RecordInvalidDesired(context.Background(), ".hidden", &boomErr{}); err != nil {
		t.Errorf("state write error: %v", err)
	}

	root := t.TempDir()
	hiddenDir := filepath.Join(root, ".hidden")
	if err := os.MkdirAll(hiddenDir, 0o755); err != nil {
		t.Fatalf("mkdir hidden: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hiddenDir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write hidden template: %v", err)
	}

	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, ok := c.GetApp(".hidden"); !ok {
		t.Fatal("a non-reserved hidden directory with a present template must not be pruned as reserved")
	}

	// The candidate contract decides removal: without template.yaml the row is
	// no longer backed by an app definition, even for an invalid name.
	if err := os.Remove(filepath.Join(hiddenDir, "template.yaml")); err != nil {
		t.Fatalf("remove hidden template: %v", err)
	}
	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, ok := c.GetApp(".hidden"); ok {
		t.Fatal("a template-less directory must be pruned by the normal candidate rule")
	}
}

// PruneRemoved removes state for apps missing from the authoritative dir,
// INCLUDING their snapshot and app_stats, while keeping apps that
// still exist on disk and leaving the global stats row untouched.
func TestPruneRemovedSweepsStaleApps(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// "gone" exists only in the DB; "kept" exists on disk too. Give both state
	// rows (snapshot + app_stats) so the sweep must clean them inclusively.
	gone := fnFor(t, "gone", tmpl)
	goneToken := seedToken(t, c, gone)
	if _, err := c.RecordReconcileSuccess(context.Background(), "gone", "img", "fp", time.Now(), gone, goneToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	c.RecordAppStats(AppStats{App: "gone", EventsMatchedTotal: 5})
	kept := fnFor(t, "kept", tmpl)
	keptToken := seedToken(t, c, kept)
	if _, err := c.RecordReconcileSuccess(context.Background(), "kept", "img", "fp", time.Now(), kept, keptToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	c.RecordAppStats(AppStats{App: "kept", EventsMatchedTotal: 9})
	// The global stats row must never be touched by pruning.
	want := Stats{EventsMatchedTotal: 55}
	c.RecordStats(want)

	// Real roots: create "kept" with a template, leave "gone" out, plus a stray
	// non-app file to confirm only candidates matter.
	root := t.TempDir()
	keptDir := filepath.Join(root, "kept")
	if err := os.MkdirAll(keptDir, 0o755); err != nil {
		t.Fatalf("mkdir kept: %v", err)
	}
	if err := os.WriteFile(filepath.Join(keptDir, "template.yaml"), []byte(twoHandlerTmpl), 0o644); err != nil {
		t.Fatalf("write kept template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write stray: %v", err)
	}

	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}

	// "gone" fully removed.
	if _, ok := c.GetApp("gone"); ok {
		t.Fatal("gone function row must be pruned")
	}
	if _, ok := c.AppStats("gone"); ok {
		t.Fatal("gone app_stats must be pruned")
	}

	// "kept" survives with its app_stats.
	if _, ok := c.GetApp("kept"); !ok {
		t.Fatal("kept function row must survive")
	}
	ks, ok := c.AppStats("kept")
	if !ok || ks.EventsMatchedTotal != 9 {
		t.Fatalf("kept app_stats = %+v, ok=%v; want events 9", ks, ok)
	}

	// Global stats untouched.
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("global stats row must survive pruning")
	}
	if gs.EventsMatchedTotal != 55 {
		t.Fatalf("global stats changed by prune: %+v", gs)
	}
}

// TestPruneRemovedSweepsTemplateLessDirectory pins the candidate-based startup
// prune: a recorded app whose directory still exists but has no template.yaml is
// no longer an app, so its apps row and app_stats are removed even though the
// path itself was not deleted.
func TestPruneRemovedSweepsTemplateLessDirectory(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	draft := fnFor(t, "draft", tmpl)
	draftToken := seedToken(t, c, draft)
	if _, err := c.RecordReconcileSuccess(context.Background(), "draft", "img-draft", "fp-draft", time.Now(), draft, draftToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	c.RecordAppStats(AppStats{App: "draft", EventsMatchedTotal: 4})

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "draft"), 0o755); err != nil {
		t.Fatalf("mkdir draft: %v", err)
	}

	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, ok := c.GetApp("draft"); ok {
		t.Fatal("an app directory without template.yaml must be pruned")
	}
	if _, ok := c.AppStats("draft"); ok {
		t.Fatal("a template-less app's app_stats must be pruned too")
	}
}

// TestPruneRemovedKeepsTemplatePresentInvalidRow pins that invalidity is not
// absence: a directory whose template.yaml is PRESENT but malformed remains a
// desired app candidate, so its persisted invalid view survives the startup
// prune.
func TestPruneRemovedKeepsTemplatePresentInvalidRow(t *testing.T) {
	c := openTestState(t)
	if _, err := c.RecordInvalidDesired(context.Background(), "broken", &boomErr{}); err != nil {
		t.Errorf("state write error: %v", err)
	}

	root := t.TempDir()
	brokenDir := filepath.Join(root, "broken")
	if err := os.MkdirAll(brokenDir, 0o755); err != nil {
		t.Fatalf("mkdir broken: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenDir, "template.yaml"), []byte("not: [valid: yaml"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}

	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}

	got, ok := c.GetApp("broken")
	if !ok {
		t.Fatal("a present malformed template must keep its invalid desired row")
	}
	if got.Status != StatusUnavailable || got.LastError == "" {
		t.Fatalf("broken = %+v, want unavailable with an error", got)
	}
}

// PruneRemoved is a no-op when the DB is empty and never drops an app that
// still exists on disk even if its state row predates the disk contents.
func TestPruneRemovedEmptyDBAndMissingDirName(t *testing.T) {
	c := openTestState(t)
	root := t.TempDir()
	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	} // empty DB: no-op, must not error or log fatally

	tmpl := mustTemplate(t, twoHandlerTmpl)
	if _, err := c.RecordDiscovered(context.Background(), fnFor(t, "demo", tmpl)); err != nil {
		t.Errorf("state write error: %v", err)
	}
	demoDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(demoDir, 0o755); err != nil {
		t.Fatalf("mkdir demo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(demoDir, "template.yaml"), []byte(twoHandlerTmpl), 0o644); err != nil {
		t.Fatalf("write demo template: %v", err)
	}
	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, ok := c.GetApp("demo"); !ok {
		t.Fatal("demo must survive pruning while present on disk")
	}
}

// TestPruneRemovedNotResurrectedByDiscovery pins the removal/discovery
// interaction at the worker startup boundary: the startup sweep prunes a
// removed app's row (including its active generation) and the subsequent
// discovery pass only upserts apps actually loaded from disk, so the pruned
// row is not resurrected by the discovery-preservation behavior.
func TestPruneRemovedNotResurrectedByDiscovery(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// Both have active state; only "kept" still exists on disk.
	gone := fnFor(t, "gone", tmpl)
	goneToken := seedToken(t, c, gone)
	if _, err := c.RecordReconcileSuccess(context.Background(), "gone", "img-gone", "fp-gone", time.Now(), gone, goneToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	kept := fnFor(t, "kept", tmpl)
	keptToken := seedToken(t, c, kept)
	if _, err := c.RecordReconcileSuccess(context.Background(), "kept", "img-kept", "fp-kept", time.Now(), kept, keptToken); err != nil {
		t.Errorf("state write error: %v", err)
	}

	root := t.TempDir()
	keptDir := filepath.Join(root, "kept")
	if err := os.MkdirAll(keptDir, 0o755); err != nil {
		t.Fatalf("mkdir kept: %v", err)
	}
	if err := os.WriteFile(filepath.Join(keptDir, "template.yaml"), []byte(twoHandlerTmpl), 0o644); err != nil {
		t.Fatalf("write kept template: %v", err)
	}

	// Worker startup order: prune, then discover the loaded (on-disk) set.
	if err := c.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, err := c.RecordDiscoveredWithFingerprint(context.Background(), fnFor(t, "kept", tmpl), "fp-kept-next"); err != nil {
		t.Errorf("state write error: %v", err)
	}

	if _, ok := c.GetApp("gone"); ok {
		t.Fatal("pruned function must not be resurrected by the discovery pass")
	}
	detail, ok := c.GetApp("kept")
	if !ok {
		t.Fatal("kept function must survive")
	}
	if detail.Image != "img-kept" {
		t.Fatalf("kept image = %q, want preserved img-kept", detail.Image)
	}
}

// TestEnvSecretsMappingsPersisted verifies the env names and secret REFERENCE
// names round-trip through the apps table, and that the env values are
// REDACTED: the literal template env values must never appear in the persisted
// snapshot (readable by any operator with database access). Env-var keys are
// preserved so inspect can render them; values are the fixed marker.
func TestEnvSecretsMappingsPersisted(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, `runtime: python3.14
env:
  API_URL: https://api.example.com
  CONN: postgres://user:pass@host/db
secrets:
  DATABASE_URL: database-url
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img", "fp", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if _, ok := detail.Env["API_URL"]; !ok {
		t.Errorf("env API_URL key missing, got %v", detail.Env)
	}
	if _, ok := detail.Env["CONN"]; !ok {
		t.Errorf("env CONN key missing, got %v", detail.Env)
	}
	for name, val := range detail.Env {
		if val != RedactedEnvValue {
			t.Errorf("env %s = %q, want the redaction marker %q", name, val, RedactedEnvValue)
		}
	}
	if detail.Secrets["DATABASE_URL"] != "database-url" {
		t.Errorf("secrets DATABASE_URL = %q, want database-url (the reference, never a value)", detail.Secrets["DATABASE_URL"])
	}

	// The raw stored snapshot must not contain any literal env value.
	data, _, _ := rawAppData(t, c, "fn")
	for _, leaked := range []string{"https://api.example.com", "postgres://user:pass@host/db"} {
		if strings.Contains(data, leaked) {
			t.Errorf("raw snapshot leaked literal env value %q:\n%s", leaked, data)
		}
	}
	if !strings.Contains(data, RedactedEnvValue) {
		t.Errorf("raw snapshot must carry the redaction marker:\n%s", data)
	}
}

// TestEnvSecretsMappingsNilWhenAbsent verifies a template with no env/secrets
// yields nil maps (not empty non-nil maps) — including a hand-built Template
// whose Env/Secrets maps are nil (never parsed), which must not panic.
func TestEnvSecretsMappingsNilWhenAbsent(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	token := seedToken(t, c, fn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "fn", "img", "fp", time.Now(), fn, token); err != nil {
		t.Errorf("state write error: %v", err)
	}

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.Env != nil {
		t.Errorf("env = %v, want nil when absent", detail.Env)
	}
	if detail.Secrets != nil {
		t.Errorf("secrets = %v, want nil when absent", detail.Secrets)
	}

	// A directly-constructed Template (nil Env/Secrets maps) must not panic and
	// must also persist nil maps.
	direct := &app.Template{Runtime: "node24"}
	directFn := fnFor(t, "direct", direct)
	directToken := seedToken(t, c, directFn)
	if _, err := c.RecordReconcileSuccess(context.Background(), "direct", "img", "fp", time.Now(), directFn, directToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	d, ok := c.GetApp("direct")
	if !ok {
		t.Fatal("expected direct row")
	}
	if d.Env != nil || d.Secrets != nil {
		t.Errorf("direct-built template env/secrets = %v/%v, want nil", d.Env, d.Secrets)
	}
}

// TestStatsRelationalMetadataColumns pins the schema shape: only the stable
// metadata columns exist on stats/app_stats (plus data), so no counter is
// duplicated as a column. The stats table additionally carries the ownership
// instance that fences stale process incarnations.
func TestStatsRelationalMetadataColumns(t *testing.T) {
	c := openTestState(t)

	statsCols := tableColumnSet(t, c, "stats")
	for _, want := range []string{"worker", "instance", "data", "updated_at"} {
		if !statsCols[want] {
			t.Errorf("stats missing metadata column %q: %v", want, statsCols)
		}
	}
	if len(statsCols) != 4 {
		t.Errorf("stats must have exactly worker/instance/data/updated_at, got %v", statsCols)
	}

	fnCols := tableColumnSet(t, c, "app_stats")
	for _, want := range []string{"app_name", "worker", "data", "updated_at"} {
		if !fnCols[want] {
			t.Errorf("app_stats missing metadata column %q: %v", want, fnCols)
		}
	}
	if len(fnCols) != 4 {
		t.Errorf("app_stats must have exactly app_name/worker/data/updated_at, got %v", fnCols)
	}
}

// tableColumnSet returns the set of column names of table using a zero-row
// SELECT, so tests can pin the current schema shape without a production
// schema-introspection helper.
func tableColumnSet(t *testing.T, c *State, table string) map[string]bool {
	t.Helper()
	rows, err := c.db.QueryContext(context.Background(), "SELECT * FROM "+table+" LIMIT 0")
	if err != nil {
		t.Fatalf("read %s columns: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns of %s: %v", table, err)
	}
	set := make(map[string]bool, len(cols))
	for _, col := range cols {
		set[col] = true
	}
	return set
}

// TestFreshSchemaHasCurrentColumns checks the current schema shape directly:
// initSchema must create the apps table with exactly name/data/updated_at plus
// the internal generation token, and must NOT create any of the removed child
// tables. The whole nested app configuration (handlers, schedules, services,
// env/secrets) lives inside apps.data, so there are no
// per-handler/schedule/service tables. The global generation sequence backs the
// token.
func TestFreshSchemaHasCurrentColumns(t *testing.T) {
	c := openTestState(t)

	fnCols := tableColumnSet(t, c, "apps")
	for _, want := range []string{"name", "data", "updated_at", "generation"} {
		if !fnCols[want] {
			t.Errorf("apps missing current column %q: %v", want, fnCols)
		}
	}
	if len(fnCols) != 4 {
		t.Errorf("apps must have exactly name/data/updated_at/generation, got %v", fnCols)
	}

	var hasSequence bool
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.sequences
		                WHERE sequence_schema = current_schema()
		                  AND sequence_name = 'apps_generation_seq')`,
	).Scan(&hasSequence); err != nil {
		t.Fatalf("check apps_generation_seq: %v", err)
	}
	if !hasSequence {
		t.Error("apps_generation_seq sequence must exist")
	}

	for _, table := range []string{"handlers", "schedules", "services"} {
		if tableExists(t, c, table) {
			t.Errorf("obsolete child table %q must not exist in the current schema", table)
		}
	}
}

// tableExists reports whether table is present in the current schema.
func tableExists(t *testing.T, c *State, table string) bool {
	t.Helper()
	var n int
	err := c.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`, table).Scan(&n)
	if err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	return n > 0
}
