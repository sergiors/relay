package reconciler

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
)

// reverifyFixture drives the two re-verification paths side by side: "gated"
// is a desired-invalid app whose local bytes parse and fingerprint fine (the
// materialization-fingerprint-error class), and "zzz-drifted" is an authorized
// app with locally edited content (ordinary drift). Both requests arrive
// through the same DeploymentDrift callback, so counting events per name
// separates the throttled invalid path from the immediate drift path.
type reverifyFixture struct {
	root       string
	b          *fakeBuilder
	progress   *[]deploymentEvent
	drifts     *[]deploymentEvent
	r          *Reconciler
	reg        *runner.Registry
	gatedDir   string
	tid        string
	driftedFP  string
	driftedTID string
}

func newReverifyFixture(t *testing.T) *reverifyFixture {
	t.Helper()
	root := t.TempDir()
	gatedDir := writeFnDir(t, root, "gated")
	unavailable := runner.NewUnavailable(app.App{
		Name: "gated", Dir: gatedDir, Template: mustParse(template),
	})
	driftedDir := writeFnDir(t, root, "zzz-drifted")
	drifted := initialFn("zzz-drifted", driftedDir)

	f := &reverifyFixture{root: root, gatedDir: gatedDir}
	var progress, drifts *[]deploymentEvent
	f.b = &fakeBuilder{}
	f.r, f.reg = newTestReconciler(t, root, f.b,
		[]*runner.PreparedApp{unavailable, drifted}, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
		})
	f.progress, f.drifts = progress, drifts
	_, f.tid = authorizationFor(t, root, "gated")
	f.driftedFP, f.driftedTID = authorizationFor(t, root, "zzz-drifted")

	if err := os.WriteFile(filepath.Join(driftedDir, "index.js"),
		[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit drifted app: %v", err)
	}
	return f
}

// install reinstalls one effective authorization. Calling it again with the
// same arguments models a successful same-generation materialization
// reinstalling the authorization (which clears the per-generation suppression).
func (f *reverifyFixture) install(generation int64) {
	f.r.SetDeploymentAuthorizationWithInvalid(generation,
		map[string]string{"zzz-drifted": f.driftedFP},
		map[string]string{"zzz-drifted": f.driftedTID},
		map[string]string{"gated": "fingerprint failed at materialization"},
		map[string]string{"gated": f.tid}, nil)
}

// countFor returns how often name requested re-verification/drift.
func countFor(events []deploymentEvent, name string) int {
	n := 0
	for _, e := range events {
		if e.name == name {
			n++
		}
	}
	return n
}

// TestInvalidReverificationThrottledWhileOrdinaryDriftStaysImmediate pins the
// split under repeated same-generation materializations: reinstalling the
// identical authorization and re-reconciling the still-invalid app must NOT
// re-request re-verification at reconcile frequency (one request per effective
// authorization, bounded by the reconciler interval), while the ordinary
// local-drift path — same cycles, same callback — stays immediate every time.
func TestInvalidReverificationThrottledWhileOrdinaryDriftStaysImmediate(t *testing.T) {
	f := newReverifyFixture(t)

	f.install(1)
	f.r.reconcileApp("gated")
	f.r.reconcileApp("zzz-drifted")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("initial invalid re-verification requests = %d, want 1", got)
	}
	if got := countFor(*f.drifts, "zzz-drifted"); got != 1 {
		t.Fatalf("initial drift reports = %d, want 1", got)
	}

	// Three same-generation materialization cycles: the reinstall clears the
	// per-generation suppression each time, so only the cooldown can keep the
	// invalid app from forcing a Git sync on every reconcile.
	const cycles = 3
	for i := 0; i < cycles; i++ {
		f.install(1)
		f.r.reconcileApp("gated")
		f.r.reconcileApp("zzz-drifted")
	}
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("invalid re-verification requests after %d same-generation "+
			"reinstalls = %d, want 1 (a rapid Kick loop)", cycles, got)
	}
	if got := countFor(*f.drifts, "zzz-drifted"); got != 1+cycles {
		t.Fatalf("drift reports after %d reinstalls = %d, want %d (ordinary drift "+
			"never consults the cooldown)", cycles, got, 1+cycles)
	}

	// The failed diagnostic stays unconditional on every pass.
	if last, ok := lastProgressFor(*f.progress, "gated"); !ok ||
		last.phase != "failed" || last.detail == "" {
		t.Fatalf("last gated progress = (%+v, %v), want the failed diagnostic on "+
			"every pass", last, ok)
	}
	if f.b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", f.b.prepares())
	}
}

// TestInvalidReverificationCooldownExpiresWithReconcilerInterval pins the
// rate-limit boundary deterministically: while the cooldown window stands, a
// reinstalled identical authorization stays silent; once the window has
// elapsed (the test moves the existing reconciler interval to zero with no
// pump running), the still-invalid app requests re-verification again so a
// persistent failure is retried rather than silenced forever.
func TestInvalidReverificationCooldownExpiresWithReconcilerInterval(t *testing.T) {
	f := newReverifyFixture(t)

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("initial invalid re-verification requests = %d, want 1", got)
	}

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("requests inside the cooldown window = %d, want 1", got)
	}

	// Same-package lever 1: backdate the recorded cooldown timestamp beyond the
	// reconciler interval — time has passed for this app, deterministically and
	// without any sleep.
	f.r.mu.Lock()
	entry := f.r.reverifyCooldown["gated"]
	entry.at = entry.at.Add(-2 * f.r.interval)
	f.r.reverifyCooldown["gated"] = entry
	f.r.mu.Unlock()

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("requests after the cooldown timestamp aged out = %d, want 2 "+
			"(a persistent failure must be retried, not silenced)", got)
	}

	// The successful request re-arms the window: an identical reinstall inside
	// it is silent again.
	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("requests after re-arming inside the window = %d, want 2", got)
	}

	// Same-package lever 2: the cooldown is bounded by the reconciler's own
	// interval; with no pump or ticker running, collapsing it to zero is the
	// deterministic passage of time.
	f.r.mu.Lock()
	f.r.interval = 0
	f.r.mu.Unlock()

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 3 {
		t.Fatalf("requests after the cooldown window elapsed = %d, want 3 "+
			"(a persistent failure must be retried, not silenced)", got)
	}
}

// TestLocalDriftReportBypassesActiveInvalidReverificationCooldown pins the map
// separation: an admitted desired-invalid re-verification arms the per-name
// cooldown WITHOUT touching the local-drift admission, so a genuine local
// drift observation for the same app under the same authorization reports and
// requests a Kick immediately — while the invalid path stays cooled down, a
// changed effective authorization reopens it, and neither path leaks into the
// other.
func TestLocalDriftReportBypassesActiveInvalidReverificationCooldown(t *testing.T) {
	f := newReverifyFixture(t)

	// Admit one desired-invalid re-verification: its cooldown is now active for
	// "gated" under this authorization.
	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("initial invalid re-verification requests = %d, want 1", got)
	}

	// A genuine local drift observation for the same app must report and Kick
	// immediately, despite the active invalid cooldown for that name.
	f.r.reportDeploymentDrift("gated", "materialized template changed locally")
	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("local drift reports with an active invalid cooldown = %d, want 2 "+
			"(an invalid re-verification must never suppress genuine drift)", got)
	}
	if last := (*f.drifts)[len(*f.drifts)-1]; last.detail != "materialized template changed locally" {
		t.Fatalf("last request = %+v, want the genuine local-drift reason", last)
	}

	// The invalid path stays cooled down: an identical reinstall inside the
	// window must not re-request...
	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("invalid re-verification after an identical reinstall = %d, want 2 "+
			"(still cooled down)", got)
	}

	// ...while a changed effective authorization reopens it immediately.
	f.install(2)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 3 {
		t.Fatalf("invalid re-verification under a new generation = %d, want 3", got)
	}
	if f.b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", f.b.prepares())
	}
}

// TestRemovalClearsLocalDriftReportAdmission pins the removal half of the
// separation cleanup: a genuine drift report recorded for a name must be
// forgotten when the app is removed, so the same drifted state observed again
// under the unchanged standing authorization reports afresh instead of being
// silenced by an admission that no longer describes a live app.
func TestRemovalClearsLocalDriftReportAdmission(t *testing.T) {
	f := newReverifyFixture(t)

	f.install(1)
	f.r.reportDeploymentDrift("gated", "first local drift")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("initial drift reports = %d, want 1", got)
	}
	// Repeated observation under the same authorization is admitted once.
	f.r.reportDeploymentDrift("gated", "repeat observation")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("drift reports after a repeat = %d, want the per-generation "+
			"suppression to hold", got)
	}

	// A later generation drops the app...
	f.r.SetDeploymentAuthorization(3,
		map[string]string{"zzz-drifted": f.driftedFP},
		map[string]string{"zzz-drifted": f.driftedTID}, []string{"gated"})
	// ...and a local recreation under that removal authorization is genuine
	// drift, which re-arms the admission...
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("recreation drift under the removal set = %d, want 2", got)
	}

	// ...then the sweep removes the app: removal must forget the admission.
	if err := os.RemoveAll(f.gatedDir); err != nil {
		t.Fatalf("remove gated: %v", err)
	}
	f.r.reconcileApp("gated")
	if f.reg.GetByName("gated") != nil {
		t.Fatal("the dropped app must take the removal path")
	}

	// The same drifted state reappears under the UNCHANGED standing
	// authorization (no reinstall in between): only removal's clearance can
	// admit a fresh report.
	if err := os.MkdirAll(f.gatedDir, 0o755); err != nil {
		t.Fatalf("recreate gated dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.gatedDir, "template.yaml"),
		[]byte(template), 0o644); err != nil {
		t.Fatalf("recreate gated template: %v", err)
	}
	f.r.reconcileApp("gated")

	if got := countFor(*f.drifts, "gated"); got != 3 {
		t.Fatalf("drift reports after removal and recreation = %d, want 3 "+
			"(removal must clear the local-drift admission)", got)
	}
	if last := (*f.drifts)[len(*f.drifts)-1]; last.detail != "not part of the active deployment" {
		t.Fatalf("last request = %+v, want the unauthorized-recreation reason", last)
	}
}

// TestInvalidReverificationCooldownKeyedByEffectiveAuthorization pins the
// signature key of the cooldown: an entry recorded under a DIFFERENT effective
// authorization (a renewed generation, invalid-set membership, fingerprint, or
// materialized template identity) never suppresses a request for the current
// one — only an entry with the same signature inside the interval does.
func TestInvalidReverificationCooldownKeyedByEffectiveAuthorization(t *testing.T) {
	f := newReverifyFixture(t)

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("initial invalid re-verification requests = %d, want 1", got)
	}

	// An identical reinstall inside the window is held to the cooldown.
	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("requests after an identical reinstall = %d, want 1", got)
	}

	// The recorded entry belongs to a different effective authorization
	// (modeling a signature change such as a new generation or a renewed
	// materialized identity): the request for the CURRENT authorization is
	// admitted immediately, without waiting for the interval.
	f.r.mu.Lock()
	stale := f.r.reverifyCooldown["gated"]
	stale.authorization = "a-different-effective-authorization"
	f.r.reverifyCooldown["gated"] = stale
	f.r.mu.Unlock()

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("requests under a changed effective authorization = %d, want 2 "+
			"(the cooldown is keyed by the signature, not the app name alone)", got)
	}
}

// TestRemovalClearsInvalidReverificationThrottle pins the throttle's lifetime:
// removing the app clears its per-app cooldown entry, so when the same
// desired-invalid state reappears under the exact effective authorization that
// was originally requested, the re-verification is immediate instead of being
// silenced by an entry that no longer describes a live app.
func TestRemovalClearsInvalidReverificationThrottle(t *testing.T) {
	f := newReverifyFixture(t)

	f.install(1)
	f.r.reconcileApp("gated")
	if got := countFor(*f.drifts, "gated"); got != 1 {
		t.Fatalf("initial invalid re-verification requests = %d, want 1", got)
	}

	// A later generation drops the app and its directory disappears: the
	// removal path runs and must clear the throttle with it.
	f.r.SetDeploymentAuthorization(3,
		map[string]string{"zzz-drifted": f.driftedFP},
		map[string]string{"zzz-drifted": f.driftedTID}, []string{"gated"})
	if err := os.RemoveAll(f.gatedDir); err != nil {
		t.Fatalf("remove gated: %v", err)
	}
	f.r.reconcileApp("gated")
	if f.reg.GetByName("gated") != nil {
		t.Fatal("the dropped app must take the removal path")
	}

	// The same desired-invalid state reappears under the SAME effective
	// authorization as the original request: only the cleared throttle can let
	// it through immediately.
	if err := os.MkdirAll(f.gatedDir, 0o755); err != nil {
		t.Fatalf("recreate gated dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.gatedDir, "template.yaml"),
		[]byte(template), 0o644); err != nil {
		t.Fatalf("recreate gated template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.gatedDir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("recreate gated index: %v", err)
	}
	f.install(1)
	f.r.reconcileApp("gated")

	if got := countFor(*f.drifts, "gated"); got != 2 {
		t.Fatalf("invalid re-verification requests after removal and recreation "+
			"= %d, want 2 (removal must clear the per-app throttle)", got)
	}
}
