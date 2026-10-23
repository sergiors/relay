package reconciler

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/source"
	"relay/internal/state"
	"relay/internal/testutil"
)

const (
	invalidTemplateA = "runtime: python9.9\n"
	invalidTemplateB = "events: [unclosed\n"
)

// invalidIdentity is the deterministic content identity the reconciler must
// persist for these exact bytes.
func invalidIdentity(data string) string {
	return app.TemplateIdentity([]byte(data))
}

func persistedObservation(t *testing.T, st *state.State, name string) state.AppObservation {
	t.Helper()
	observation, err := st.ObserveApp(context.Background(), name)
	if err != nil {
		t.Fatalf("ObserveApp(%q): %v", name, err)
	}
	return observation
}

func localFencingToken(t *testing.T, r *Reconciler, name string) state.FencingToken {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fencingTokens[name]
}

func writeInvalidTemplate(t *testing.T, dir, data string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(dir, "template.yaml")); err != nil {
		t.Fatalf("remove template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(data), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
}

// TestReconcileInvalidSourceFirstObservationPersistsIdentity pins the live first
// observation: the deterministic invalid bytes persist their exact identity and
// diagnostic, a fresh app is unavailable (nothing ever served), the registry
// stays empty, and no build is attempted.
func TestReconcileInvalidSourceFirstObservationPersistsIdentity(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "broken")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir broken: %v", err)
	}
	writeInvalidTemplate(t, dir, invalidTemplateA)

	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, nil)

	r.reconcileApp("broken")

	detail, ok := st.GetApp("broken")
	if !ok {
		t.Fatal("a first invalid template must persist a visible row")
	}
	if want := invalidIdentity(invalidTemplateA); detail.InvalidSource != want {
		t.Fatalf("invalid_source = %q, want %q", detail.InvalidSource, want)
	}
	if detail.Status != state.StatusUnavailable {
		t.Fatalf("status = %q, want unavailable (no usable generation)", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileFailed || detail.LastError == "" {
		t.Fatalf("outcome = %q/%q, want failed with a diagnostic", detail.LastReconcileStatus, detail.LastError)
	}
	if detail.Image != "" || detail.Fingerprint != "" {
		t.Fatalf("fresh invalid app must have no active generation: %q/%q", detail.Image, detail.Fingerprint)
	}
	if pf := reg.GetByName("broken"); pf != nil {
		t.Fatal("an invalid template must never enter the live registry")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0", b.prepares())
	}

	observation := persistedObservation(t, st, "broken")
	if observation.FencingToken <= 0 {
		t.Fatalf("persisted token = %d, want positive", observation.FencingToken)
	}
	if got := localFencingToken(t, r, "broken"); got != observation.FencingToken {
		t.Fatalf("local token = %d, want the persisted %d", got, observation.FencingToken)
	}
}

// TestReconcileInvalidSourceUnchangedBytesSkipsWriteAndKeepsActive pins the
// idempotent live path: rescanning the exact same invalid bytes allocates no new
// state token, changes no field, performs no build, and keeps the previously
// active runtime in the registry.
func TestReconcileInvalidSourceUnchangedBytesSkipsWriteAndKeepsActive(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	fencingToken := recordSuccess(t, st, "guarded", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("guarded", fencingToken)

	writeInvalidTemplate(t, dir, invalidTemplateA)
	r.reconcileApp("guarded")

	first, ok := st.GetApp("guarded")
	if !ok {
		t.Fatal("invalid template must keep the row")
	}
	if first.Status != state.StatusDegraded || first.InvalidSource != invalidIdentity(invalidTemplateA) {
		t.Fatalf("state = %+v, want degraded with the invalid identity", first)
	}
	if first.Image != "img-old" || first.Fingerprint != "fp-old" {
		t.Fatalf("active generation = %q/%q, want preserved", first.Image, first.Fingerprint)
	}
	tokenAfterFirst := persistedObservation(t, st, "guarded").FencingToken
	if tokenAfterFirst <= fencingToken {
		t.Fatalf("invalid transition token = %d, want it to advance past %d", tokenAfterFirst, fencingToken)
	}

	r.reconcileApp("guarded") // exact same bytes

	second, ok := st.GetApp("guarded")
	if !ok {
		t.Fatal("repeat must keep the row")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeat changed the row:\n before: %+v\n  after: %+v", first, second)
	}
	tokenAfterSecond := persistedObservation(t, st, "guarded").FencingToken
	if tokenAfterSecond != tokenAfterFirst {
		t.Fatalf("repeat token = %d, want unchanged %d", tokenAfterSecond, tokenAfterFirst)
	}
	if got := localFencingToken(t, r, "guarded"); got != tokenAfterFirst {
		t.Fatalf("local token = %d, want the unchanged %d", got, tokenAfterFirst)
	}
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the previously active runtime must keep serving after an unchanged invalid scan")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0", b.prepares())
	}
}

// TestReconcileInvalidSourceChangedBytesAdvanceTransition pins that different
// invalid bytes are a new transition: the state token advances and the recorded
// identity/diagnostic are replaced while the active generation still serves.
func TestReconcileInvalidSourceChangedBytesAdvanceTransition(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "changing-bad")

	fn := initialFn("changing-bad", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)
	fencingToken := recordSuccess(t, st, "changing-bad", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("changing-bad", fencingToken)

	writeInvalidTemplate(t, dir, invalidTemplateA)
	r.reconcileApp("changing-bad")
	firstToken := persistedObservation(t, st, "changing-bad").FencingToken
	first, _ := st.GetApp("changing-bad")

	writeInvalidTemplate(t, dir, invalidTemplateB)
	r.reconcileApp("changing-bad")

	secondToken := persistedObservation(t, st, "changing-bad").FencingToken
	if secondToken <= firstToken {
		t.Fatalf("changed-bytes token = %d, want it to advance past %d", secondToken, firstToken)
	}
	second, ok := st.GetApp("changing-bad")
	if !ok {
		t.Fatal("expected row")
	}
	if want := invalidIdentity(invalidTemplateB); second.InvalidSource != want {
		t.Fatalf("invalid_source = %q, want changed identity %q", second.InvalidSource, want)
	}
	if second.LastError == first.LastError {
		t.Fatal("changed invalid bytes must record their new diagnostic")
	}
	if second.Image != "img-old" || second.Fingerprint != "fp-old" {
		t.Fatalf("active generation = %q/%q, want preserved", second.Image, second.Fingerprint)
	}
}

// TestReconcileInvalidSourceValidCorrectionClearsIdentity pins recovery through
// the live rebuild path: a valid correction clears the invalid identity and a
// successful build replaces the degraded view with ready.
func TestReconcileInvalidSourceValidCorrectionClearsIdentity(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "recovers")

	fn := initialFn("recovers", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)
	fencingToken := recordSuccess(t, st, "recovers", "img-v1", "fp-v1", time.Now(), fn.App())
	r.SeedFencingToken("recovers", fencingToken)

	writeInvalidTemplate(t, dir, invalidTemplateA)
	r.reconcileApp("recovers")
	invalidToken := persistedObservation(t, st, "recovers").FencingToken
	if got, _ := st.GetApp("recovers"); got.InvalidSource == "" {
		t.Fatal("precondition: invalid identity must be recorded")
	}

	writeFnDir(t, root, "recovers") // restore a valid template and source
	r.reconcileApp("recovers")

	recovered, ok := st.GetApp("recovers")
	if !ok {
		t.Fatal("expected row after recovery")
	}
	if recovered.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready after a valid successful correction", recovered.Status)
	}
	if recovered.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want cleared by a valid correction", recovered.InvalidSource)
	}
	if recovered.LastError != "" || recovered.LastReconcileStatus != state.ReconcileSuccess {
		t.Fatalf("outcome = %q/%q, want success with no error", recovered.LastReconcileStatus, recovered.LastError)
	}
	if recovered.Image != "img-recovers" {
		t.Fatalf("image = %q, want the newly prepared img-recovers", recovered.Image)
	}
	if recoveredToken := persistedObservation(t, st, "recovers").FencingToken; recoveredToken <= invalidToken {
		t.Fatalf("recovery token = %d, want it to advance past the invalid %d", recoveredToken, invalidToken)
	}
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want exactly the recovery build", b.prepares())
	}
}

// TestReconcileInvalidSourceFingerprintFailureClearsIdentityAndRetries pins the
// operational-failure boundary on the live path: a fingerprint failure carries no
// invalid identity, clears the prior one so the app stays retryable, and a later
// successful fingerprint recovers without any edit to the template.
func TestReconcileInvalidSourceFingerprintFailureClearsIdentityAndRetries(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "retryable")

	fn := initialFn("retryable", dir)
	b := &fakeBuilder{}
	failFingerprint := false
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
		cfg.Fingerprint = func(dir string, tmpl *app.Template) (*source.Selection, string, error) {
			if failFingerprint {
				return nil, "", errBoom
			}
			return app.SelectAndFingerprintApp(dir, tmpl)
		}
	})
	fencingToken := recordSuccess(t, st, "retryable", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("retryable", fencingToken)

	writeInvalidTemplate(t, dir, invalidTemplateA)
	r.reconcileApp("retryable")
	invalidToken := persistedObservation(t, st, "retryable").FencingToken

	// Valid bytes now, but the identity resolver fails operationally.
	writeFnDir(t, root, "retryable")
	failFingerprint = true
	r.reconcileApp("retryable")

	failed, ok := st.GetApp("retryable")
	if !ok {
		t.Fatal("a fingerprint failure must not remove the app")
	}
	if failed.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want cleared by an operational failure", failed.InvalidSource)
	}
	if failed.Status != state.StatusDegraded || failed.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("state = %q/%q, want degraded/failed", failed.Status, failed.LastReconcileStatus)
	}
	if failed.Image != "img-old" {
		t.Fatalf("image = %q, want the active generation retained", failed.Image)
	}
	failedToken := persistedObservation(t, st, "retryable").FencingToken
	if failedToken <= invalidToken {
		t.Fatalf("fingerprint failure token = %d, want it to advance past %d", failedToken, invalidToken)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 on a fingerprint failure", b.prepares())
	}

	// The transient failure clears: the next pass retries and recovers with no
	// template edit.
	failFingerprint = false
	r.reconcileApp("retryable")

	recovered, ok := st.GetApp("retryable")
	if !ok {
		t.Fatal("expected row after retry")
	}
	if recovered.Status != state.StatusReady || recovered.InvalidSource != "" || recovered.LastError != "" {
		t.Fatalf("recovered state = %+v, want ready with no invalid identity/error", recovered)
	}
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want the retry build", b.prepares())
	}
}

// TestReconcileInvalidSourceReadFailureClearsIdentityAndRetries pins the
// transient-read branch: an unreadable template carries no identity, clears the
// previously persisted identity, keeps the active runtime, and retries once the
// file becomes readable again.
func TestReconcileInvalidSourceReadFailureClearsIdentityAndRetries(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)
	fencingToken := recordSuccess(t, st, "flaky", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("flaky", fencingToken)

	writeInvalidTemplate(t, dir, invalidTemplateA)
	r.reconcileApp("flaky")
	invalidToken := persistedObservation(t, st, "flaky").FencingToken

	// The template path is still present (so this is not a removal) but cannot be
	// read: replace the file with a directory of the same name.
	templatePath := filepath.Join(dir, "template.yaml")
	if err := os.Remove(templatePath); err != nil {
		t.Fatalf("remove template: %v", err)
	}
	if err := os.Mkdir(templatePath, 0o755); err != nil {
		t.Fatalf("mkdir template path: %v", err)
	}
	r.reconcileApp("flaky")

	failed, ok := st.GetApp("flaky")
	if !ok {
		t.Fatal("a read failure must not remove the app")
	}
	if failed.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want cleared by an operational read failure", failed.InvalidSource)
	}
	if failed.Status != state.StatusDegraded || failed.Image != "img-old" {
		t.Fatalf("state = %q/%q, want degraded with the active generation retained", failed.Status, failed.Image)
	}
	if failed.LastReconcileStatus != state.ReconcileFailed || failed.LastError == "" {
		t.Fatalf("outcome = %q/%q, want failed with the read diagnostic", failed.LastReconcileStatus, failed.LastError)
	}
	failedToken := persistedObservation(t, st, "flaky").FencingToken
	if failedToken <= invalidToken {
		t.Fatalf("read failure token = %d, want it to advance past %d", failedToken, invalidToken)
	}
	if pf := reg.GetByName("flaky"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a read failure must leave the active runtime serving")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 on a read failure", b.prepares())
	}

	if err := os.Remove(templatePath); err != nil {
		t.Fatalf("remove template dir: %v", err)
	}
	writeFnDir(t, root, "flaky")
	r.reconcileApp("flaky")

	recovered, ok := st.GetApp("flaky")
	if !ok {
		t.Fatal("expected row after retry")
	}
	if recovered.Status != state.StatusReady || recovered.InvalidSource != "" || recovered.LastError != "" {
		t.Fatalf("recovered state = %+v, want ready with no invalid identity/error", recovered)
	}
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want the retry build", b.prepares())
	}
}

// TestReconcileUnreadableAppDirRetainsActiveAndRetries pins the operational
// failure boundary of the live read with REAL permission bits (chmod, the
// portable way to induce a genuine filesystem error): a present app directory
// the process cannot search is an ambiguous observation — the active generation
// is retained, no deterministic identity is claimed, no build runs, and the
// read is retryable (a later readable pass rebuilds the same bytes and replaces
// the failure view without any edit).
func TestReconcileUnreadableAppDirRetainsActiveAndRetries(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	dir := writeFnDir(t, root, "locked")

	fn := initialFn("locked", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)
	fencingToken := recordSuccess(t, st, "locked", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("locked", fencingToken)

	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod app dir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	r.reconcileApp("locked")

	detail, ok := st.GetApp("locked")
	if !ok {
		t.Fatal("a flaky read must not remove the app")
	}
	if detail.Status != state.StatusDegraded || detail.InvalidSource != "" {
		t.Fatalf("state = %+v, want degraded with NO invalid identity (operational)", detail)
	}
	if detail.Image != "img-old" || detail.Fingerprint != "fp-old" {
		t.Fatalf("active generation = %q/%q, want preserved", detail.Image, detail.Fingerprint)
	}
	if pf := reg.GetByName("locked"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a flaky read must leave the active runtime serving")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 on an unreadable app dir", b.prepares())
	}

	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("restore app dir: %v", err)
	}
	r.reconcileApp("locked")

	recovered, ok := st.GetApp("locked")
	if !ok {
		t.Fatal("expected row after recovery")
	}
	if recovered.Status != state.StatusReady || recovered.InvalidSource != "" || recovered.LastError != "" {
		t.Fatalf("recovered state = %+v, want ready with no error", recovered)
	}
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want the recovery build", b.prepares())
	}
}

// TestReconcileInvalidSourceObservationReadFailureFailsClosed pins that a
// failed pre-read observation is not an absent row: the reconciler still keeps
// the active runtime serving but does not write an invalid state view without
// concurrency authority.
func TestReconcileInvalidSourceObservationReadFailureFailsClosed(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")
	fn := initialFn("guarded", dir)

	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.State = st
	})
	fencingToken := recordSuccess(t, st, "guarded", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("guarded", fencingToken)
	writeInvalidTemplate(t, dir, invalidTemplateA)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `ALTER TABLE apps RENAME TO apps_unreadable`); err != nil {
		t.Fatalf("make apps unreadable: %v", err)
	}
	r.reconcileApp("guarded")
	if _, err := db.ExecContext(ctx, `ALTER TABLE apps_unreadable RENAME TO apps`); err != nil {
		t.Fatalf("restore apps table: %v", err)
	}

	if got := localFencingToken(t, r, "guarded"); got != 0 {
		t.Fatalf("local token = %d, want cleared when the observation is unavailable", got)
	}
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a failed observation must leave the active runtime serving")
	}
	detail, ok := st.GetApp("guarded")
	if !ok {
		t.Fatal("expected the previously seeded row to survive")
	}
	if detail.Status != state.StatusReady || detail.InvalidSource != "" ||
		detail.Image != "img-old" {
		t.Fatalf("row = %+v, want the prior ready generation untouched", detail)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0", b.prepares())
	}
}

// TestReconcileInvalidSourceTemplateDeletionRemovesRow pins that persisted
// invalid identity never outlives its definition: once template.yaml is deleted,
// the app is a removal (not an invalid desired state) and its row and local
// transition token disappear.
func TestReconcileInvalidSourceTemplateDeletionRemovesRow(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "removed-after-invalid")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeInvalidTemplate(t, dir, invalidTemplateA)

	var removed []string
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, func(cfg *Config, _ *state.State) {
		cfg.RemoveApp = func(name string) {
			removed = append(removed, name)
		}
	})

	r.reconcileApp("removed-after-invalid")
	if _, ok := st.GetApp("removed-after-invalid"); !ok {
		t.Fatal("precondition: invalid row must exist")
	}
	if localFencingToken(t, r, "removed-after-invalid") <= 0 {
		t.Fatal("precondition: invalid transition must hold a local token")
	}

	if err := os.Remove(filepath.Join(dir, "template.yaml")); err != nil {
		t.Fatalf("remove template: %v", err)
	}
	r.reconcileApp("removed-after-invalid")

	if _, ok := st.GetApp("removed-after-invalid"); ok {
		t.Fatal("deleting template.yaml must remove the invalid row, not retain it")
	}
	if reg.GetByName("removed-after-invalid") != nil {
		t.Fatal("deleted template must not leave a registry entry")
	}
	if got := localFencingToken(t, r, "removed-after-invalid"); got != 0 {
		t.Fatalf("local token = %d, want cleared on removal", got)
	}
	if len(removed) != 1 || removed[0] != "removed-after-invalid" {
		t.Fatalf("RemoveApp calls = %v, want one removal", removed)
	}
}
