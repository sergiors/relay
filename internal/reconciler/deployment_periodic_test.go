package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
)

// drainDispatched returns the names sitting in the reconcile queue. A test that
// never launches the pump therefore observes reconcileAll's exact dispatch set
// (and order) with no consumer racing it.
func drainDispatched(r *Reconciler) []string {
	var out []string
	for {
		select {
		case name := <-r.incoming:
			out = append(out, name)
		default:
			return out
		}
	}
}

// channelDeploymentRecorder wires the cluster callbacks to buffered channels so
// a pump-driven test can wait for observations without racing plain slices.
func channelDeploymentRecorder(cfg *Config) (progress, drifts chan deploymentEvent) {
	progress = make(chan deploymentEvent, 16)
	drifts = make(chan deploymentEvent, 8)
	cfg.DeploymentProgress = func(name, phase, errMessage string) {
		progress <- deploymentEvent{name: name, phase: phase, detail: errMessage}
	}
	cfg.DeploymentDrift = func(name, reason string) {
		drifts <- deploymentEvent{name: name, detail: reason}
	}
	return progress, drifts
}

// TestPeriodicPassDetectsMissedFsnotifyEditAsDrift pins the periodic pass as
// the drift verifier: an authorized app whose content is edited with NO
// fsnotify event delivered is reported as drift through the real single pump,
// the active generation keeps serving without a build, the observation is
// recorded in the verification ledger (so the coordinator's independent
// backstop does not re-hash the same bytes), and a repeated observation under
// the same authorization is suppressed.
func TestPeriodicPassDetectsMissedFsnotifyEditAsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "authorized")

	fn := initialFn("authorized", dir)
	b := &fakeBuilder{}
	var progress chan deploymentEvent
	var drifts chan deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = channelDeploymentRecorder(cfg)
	})
	fp, tid := authorizationFor(t, root, "authorized")
	r.SetDeploymentAuthorization(1,
		map[string]string{"authorized": fp},
		map[string]string{"authorized": tid}, nil)

	// The edit lands while no watch event reaches the reconciler: only the
	// periodic pass can observe it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){ /* missed edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit app: %v", err)
	}

	passStart := time.Now()
	launchSkeletonPump(t, r)
	r.reconcileAll()

	select {
	case got := <-drifts:
		if got.name != "authorized" || got.detail != "materialized content changed locally" {
			t.Fatalf("drift report = %+v, want the content-edit reason for authorized", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the periodic pass never reported the missed edit as drift")
	}
	if _, ok := r.VerifiedSince(passStart)["authorized"]; !ok {
		t.Fatal("the pass that read the drifted content must record its verification " +
			"in the ledger (recorded even when the comparison fails)")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (local drift never builds)", b.prepares())
	}
	if got := reg.GetByName("authorized"); got == nil || got.Prepared() == nil ||
		got.Prepared().Image != "img-authorized" {
		t.Fatalf("registry = %+v, want the active img-authorized generation untouched", got)
	}
	if reg.HasPending("authorized") {
		t.Fatal("local drift must not leave a pending placeholder behind")
	}
	select {
	case got := <-progress:
		if got.phase != "drifted" {
			t.Fatalf("progress event = %+v, want the drifted phase", got)
		}
	default:
		t.Fatal("the drift observation must record a drifted progress phase")
	}

	// A repeated observation under the same authorization is suppressed: one
	// re-materialization request per name per generation.
	r.reconcileApp("authorized")
	select {
	case extra := <-drifts:
		t.Fatalf("duplicate drift report = %+v, want suppression for this generation", extra)
	default:
	}
}

// TestChangedAuthorizationResetsReverificationSuppression pins the suppression
// lifetime: local drift requests re-verification once per authorized
// generation, repeats are silent, and installing a new authorization
// generation resets the suppression so the next observation requests again.
func TestChangedAuthorizationResetsReverificationSuppression(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "authorized")

	fn := initialFn("authorized", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})
	fp, tid := authorizationFor(t, root, "authorized")
	fingerprints := map[string]string{"authorized": fp}
	identities := map[string]string{"authorized": tid}
	r.SetDeploymentAuthorization(1, fingerprints, identities, nil)

	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit app: %v", err)
	}

	r.reconcileApp("authorized")
	if len(*drifts) != 1 {
		t.Fatalf("drift reports = %+v, want exactly one", *drifts)
	}
	r.reconcileApp("authorized")
	if len(*drifts) != 1 {
		t.Fatalf("drift reports after a repeat pass = %+v, want suppression", *drifts)
	}

	r.SetDeploymentAuthorization(2, fingerprints, identities, nil)

	r.reconcileApp("authorized")
	if len(*drifts) != 2 {
		t.Fatalf("drift reports after the authorization advanced = %+v, want the "+
			"suppression reset by the new generation", *drifts)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (drift never builds)", b.prepares())
	}
	if got := reg.GetByName("authorized"); got == nil || got.Prepared() == nil ||
		got.Prepared().Image != "img-authorized" {
		t.Fatalf("registry = %+v, want the active generation untouched", got)
	}
	if !containsEvent(*progress, deploymentEvent{name: "authorized", phase: "drifted"}) {
		t.Fatalf("progress = %+v, want a drifted phase", *progress)
	}
}

// TestPeriodicPassSweepsAuthorizedNamesMissingFromDisk pins the missing-name
// sweep's authorization half: a deployment-authorized app can be absent from
// the registry, pending set, and fencing tokens at once (startup filtered it
// out), and the periodic pass must still dispatch it and classify its absence
// as drift — never as a removal — for both valid and desired-invalid names.
func TestPeriodicPassSweepsAuthorizedNamesMissingFromDisk(t *testing.T) {
	assertMissingReported := func(
		t *testing.T,
		r *Reconciler,
		reg *runner.Registry,
		b *fakeBuilder,
		progress, drifts *[]deploymentEvent,
		removals *[]string,
		dispatched []string,
		name string,
		wantDriftedPhase bool,
	) {
		t.Helper()
		if !slices.Contains(dispatched, name) {
			t.Fatalf("dispatched = %v, want the authorized name %q swept despite no "+
				"registry/pending/fencing entry", dispatched, name)
		}
		r.reconcileApp(name)
		if !containsEvent(*drifts, deploymentEvent{
			name: name, detail: "materialized app is missing locally",
		}) {
			t.Fatalf("drift reports = %+v, want the missing-directory reason for %s",
				*drifts, name)
		}
		if wantDriftedPhase {
			if !containsEvent(*progress, deploymentEvent{name: name, phase: "drifted"}) {
				t.Fatalf("progress = %+v, want a drifted phase for %s", *progress, name)
			}
		} else if containsEvent(*progress, deploymentEvent{name: name, phase: "drifted"}) {
			t.Fatalf("progress = %+v, want no Drifted overwrite for desired-invalid %s "+
				"(its persisted Failed authorization must survive a restart)",
				*progress, name)
		}
		if len(*removals) != 0 {
			t.Fatalf("removal hooks = %v, want none (an authorized absence is drift, "+
				"never a removal)", *removals)
		}
		if reg.GetByName(name) != nil {
			t.Fatalf("registry = %+v, want no entry synthesized for the missing app", name)
		}
		if b.prepares() != 0 {
			t.Fatalf("prepare calls = %d, want 0", b.prepares())
		}
	}

	t.Run("valid authorized app", func(t *testing.T) {
		root := t.TempDir()
		b := &fakeBuilder{}
		var progress, drifts *[]deploymentEvent
		removals := []string{}
		r, reg := newTestReconciler(t, root, b, nil, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
			cfg.RemoveServices = func(name string, _ func()) { removals = append(removals, name) }
		})
		// Startup filtered this locally drifted authorized app out, so it never
		// entered any local index; only the authorization can name it. Its
		// directory is absent, so the fingerprint value is never compared.
		r.SetDeploymentAuthorization(1,
			map[string]string{"filtered": "fp-filtered"},
			map[string]string{"filtered": "tid-filtered"}, nil)

		r.reconcileAll()
		assertMissingReported(t, r, reg, b, progress, drifts, &removals,
			drainDispatched(r), "filtered", true)
	})

	t.Run("desired-invalid app", func(t *testing.T) {
		root := t.TempDir()
		b := &fakeBuilder{}
		var progress, drifts *[]deploymentEvent
		removals := []string{}
		r, reg := newTestReconciler(t, root, b, nil, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
			cfg.RemoveServices = func(name string, _ func()) { removals = append(removals, name) }
		})
		authorizeDesiredInvalid(r, "invalid-missing", "tid-invalid-missing")

		r.reconcileAll()
		assertMissingReported(t, r, reg, b, progress, drifts, &removals,
			drainDispatched(r), "invalid-missing", false)
	})
}

// TestPeriodicPassHandlesAuthorizedRemovalSet pins both sides of the removal
// set in the periodic sweep: a removed-set name that was locally recreated on
// disk is dispatched as a candidate and classified as drift, while a removed-set
// name that is absent — the state the generation authorizes — is never
// dispatched and reconciling it directly stays silent.
func TestPeriodicPassHandlesAuthorizedRemovalSet(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "recreated")

	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	removals := []string{}
	r, reg := newTestReconciler(t, root, b, nil, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.RemoveServices = func(name string, _ func()) { removals = append(removals, name) }
	})
	r.SetDeploymentAuthorization(2,
		map[string]string{}, map[string]string{},
		[]string{"recreated", "vanished"})

	r.reconcileAll()

	dispatched := drainDispatched(r)
	if !slices.Contains(dispatched, "recreated") {
		t.Fatalf("dispatched = %v, want the present removed-set name as a candidate",
			dispatched)
	}
	if slices.Contains(dispatched, "vanished") {
		t.Fatalf("dispatched = %v, an absent authorized removal is the expected "+
			"state and must not be reconciled", dispatched)
	}

	r.reconcileApp("recreated")
	if !containsEvent(*drifts, deploymentEvent{
		name: "recreated", detail: "app is not part of the active deployment",
	}) {
		t.Fatalf("drift reports = %+v, want the local-recreation reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{name: "recreated", phase: "drifted"}) {
		t.Fatalf("progress = %+v, want a drifted phase for recreated", *progress)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (a recreation never builds)", b.prepares())
	}
	if reg.GetByName("recreated") != nil {
		t.Fatal("a local recreation must not synthesize a registry entry")
	}

	// A stale/direct caller naming the absent removal must stay silent: the
	// directory is gone exactly as authorized.
	r.reconcileApp("vanished")
	if len(*drifts) != 1 {
		t.Fatalf("drift reports = %+v, want no observation for an absent authorized "+
			"removal", *drifts)
	}
	if len(removals) != 0 {
		t.Fatalf("removal hooks = %v, want none", removals)
	}
}

// TestPeriodicPassIgnoresStandaloneUnknownNonCandidates pins the standalone
// (no coordinated authorization) sweep: root entries that are not app
// candidates are never dispatched and never enter the registry even when a
// stale caller reconciles them directly, while a previously known app missing
// from disk still rides the missing-name sweep — proving the pass ran past the
// unknown entries.
func TestPeriodicPassIgnoresStandaloneUnknownNonCandidates(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "stranger"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write unknown file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "no-template"), 0o755); err != nil {
		t.Fatalf("mkdir template-less dir: %v", err)
	}
	goneDir := writeFnDir(t, root, "gone")
	gone := initialFn("gone", goneDir)
	if err := os.RemoveAll(goneDir); err != nil {
		t.Fatalf("remove gone: %v", err)
	}

	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	removals := []string{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{gone}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.RemoveServices = func(name string, _ func()) { removals = append(removals, name) }
	})

	r.reconcileAll()

	dispatched := drainDispatched(r)
	for _, name := range []string{"stranger", "no-template"} {
		if slices.Contains(dispatched, name) {
			t.Fatalf("dispatched = %v, a standalone non-candidate %q must never be "+
				"dispatched", dispatched, name)
		}
	}
	if !slices.Contains(dispatched, "gone") {
		t.Fatalf("dispatched = %v, want the known missing app to prove the sweep ran",
			dispatched)
	}

	// Even a stale direct dispatch must not admit the unknown entries.
	for _, name := range []string{"stranger", "no-template"} {
		r.reconcileApp(name)
		if reg.GetByName(name) != nil {
			t.Fatalf("registry = %+v, a non-candidate %q must never enter the registry",
				name, name)
		}
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
	if len(removals) != 0 {
		t.Fatalf("removal hooks = %v, want none (only known apps are removed)", removals)
	}
	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none outside a coordinated deployment",
			*drifts)
	}
	if reg.GetByName("gone") == nil {
		t.Fatal("the known missing app must still be registered before its reconcile")
	}
	r.reconcileApp("gone")
	if reg.GetByName("gone") != nil {
		t.Fatal("the known missing app must take the normal removal path")
	}
	if !containsEvent(*progress, deploymentEvent{name: "gone", phase: "removed"}) {
		t.Fatalf("progress = %+v, want the removed phase for gone", *progress)
	}
	for _, e := range *progress {
		if e.name == "stranger" || e.name == "no-template" {
			t.Fatalf("progress = %+v, a standalone non-candidate must never report "+
				"deployment progress", e)
		}
	}
}

// TestPeriodicPassTransientReadErrorReportsDriftOnceThenRecovers pins the
// operational-failure contract for an authorized app: a transient unreadable
// template is reported as drift exactly once per generation (the name audit
// keeps the app out of the verification ledger while unreadable), the active
// generation keeps serving with no build, and once the read succeeds again the
// same pass converges back to active without requesting re-verification again.
func TestPeriodicPassTransientReadErrorReportsDriftOnceThenRecovers(t *testing.T) {
	skipWithoutPosixPermissions(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "authorized")

	fn := initialFn("authorized", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})
	fp, tid := authorizationFor(t, root, "authorized")
	r.SetDeploymentAuthorization(1,
		map[string]string{"authorized": fp},
		map[string]string{"authorized": tid}, nil)

	templatePath := filepath.Join(dir, "template.yaml")
	if err := os.Chmod(templatePath, 0o000); err != nil {
		t.Fatalf("chmod template: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(templatePath, 0o644) })

	windowStart := time.Now()
	r.reconcileAll()
	dispatched := drainDispatched(r)
	if !slices.Contains(dispatched, "authorized") {
		t.Fatalf("dispatched = %v, want the authorized app dispatched despite the "+
			"unreadable template", dispatched)
	}
	r.reconcileApp("authorized")

	if !containsEvent(*drifts, deploymentEvent{
		name: "authorized", detail: "materialized app can no longer be loaded locally",
	}) {
		t.Fatalf("drift reports = %+v, want the unreadable-template reason", *drifts)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (a read failure never builds)", b.prepares())
	}
	if got := reg.GetByName("authorized"); got == nil || got.Prepared() == nil ||
		got.Prepared().Image != "img-authorized" {
		t.Fatalf("registry = %+v, want the active generation retained through the error", got)
	}
	if _, ok := r.VerifiedSince(windowStart)["authorized"]; ok {
		t.Fatal("an unreadable app must stay out of the verification ledger so the " +
			"coordinator's backstop keeps auditing it")
	}

	// The transient failure clears: the same pass verifies the content again,
	// converges to active, and must not request re-verification a second time.
	if err := os.Chmod(templatePath, 0o644); err != nil {
		t.Fatalf("restore template permissions: %v", err)
	}
	recoverStart := time.Now()
	r.reconcileApp("authorized")

	if len(*drifts) != 1 {
		t.Fatalf("drift reports after recovery = %+v, want the original single request",
			*drifts)
	}
	if !containsEvent(*progress, deploymentEvent{name: "authorized", phase: "active"}) {
		t.Fatalf("progress = %+v, want an active phase after the read recovered", *progress)
	}
	if _, ok := r.VerifiedSince(recoverStart)["authorized"]; !ok {
		t.Fatal("the recovered pass must record its verification in the ledger")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (unchanged verified content is not rebuilt)",
			b.prepares())
	}
}

// TestPeriodicPassDesiredInvalidTemplateClassification pins the two
// desired-invalid classifications through the periodic dispatch path: bytes
// that still match the materialized invalid identity are reported failed (the
// diagnostic stays the phase, never drift, never a build), while a locally
// changed identity — valid or invalid — is drift that leaves the previous
// active generation serving.
func TestPeriodicPassDesiredInvalidTemplateClassification(t *testing.T) {
	setup := func(t *testing.T) (
		root string, b *fakeBuilder, progress, drifts *[]deploymentEvent,
		r *Reconciler, reg *runner.Registry, fn *runner.PreparedApp,
	) {
		t.Helper()
		root = t.TempDir()
		dir := writeFnDir(t, root, "gated")
		fn = initialFn("gated", dir)
		b = &fakeBuilder{}
		r, reg = newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
		})
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
			[]byte("runtime: python9.9\n"), 0o644); err != nil {
			t.Fatalf("write invalid template: %v", err)
		}
		src, err := app.ReadTemplateSource(root, "gated")
		if err != nil {
			t.Fatalf("template source: %v", err)
		}
		authorizeDesiredInvalid(r, "gated", src.Identity)
		return root, b, progress, drifts, r, reg, fn
	}

	t.Run("unchanged invalid identity is failed, not drift", func(t *testing.T) {
		_, b, progress, drifts, r, reg, fn := setup(t)

		r.reconcileAll()
		if dispatched := drainDispatched(r); !slices.Contains(dispatched, "gated") {
			t.Fatalf("dispatched = %v, want the desired-invalid candidate dispatched",
				dispatched)
		}
		r.reconcileApp("gated")

		if len(*drifts) != 0 {
			t.Fatalf("drift reports = %+v, want none (unchanged materialized invalid "+
				"bytes are a retryable failure, not drift)", *drifts)
		}
		if !containsEvent(*progress, deploymentEvent{name: "gated", phase: "failed"}) {
			t.Fatalf("progress = %+v, want a failed phase", *progress)
		}
		if b.prepares() != 0 {
			t.Fatalf("prepare calls = %d, want 0 (invalid desired bytes never build)",
				b.prepares())
		}
		if got := reg.GetByName("gated"); got != fn {
			t.Fatalf("registry = %+v, want the previous active entry untouched", got)
		}
	})

	t.Run("changed invalid identity is local drift", func(t *testing.T) {
		root, b, progress, drifts, r, reg, fn := setup(t)

		// The unchanged bytes first record the Failed authorization for this
		// desired-invalid app.
		r.reconcileApp("gated")
		if !containsEvent(*progress, deploymentEvent{name: "gated", phase: "failed"}) {
			t.Fatalf("progress = %+v, want the Failed diagnostic recorded first",
				*progress)
		}

		if err := os.WriteFile(filepath.Join(root, "gated", "template.yaml"),
			[]byte("runtime: python9.1\n"), 0o644); err != nil {
			t.Fatalf("edit invalid template: %v", err)
		}
		r.reconcileAll()
		if dispatched := drainDispatched(r); !slices.Contains(dispatched, "gated") {
			t.Fatalf("dispatched = %v, want the desired-invalid candidate dispatched",
				dispatched)
		}
		r.reconcileApp("gated")

		if !containsEvent(*drifts, deploymentEvent{
			name: "gated", detail: "materialized template changed locally",
		}) {
			t.Fatalf("drift reports = %+v, want the template-edit reason", *drifts)
		}
		// The drift callback still fires (the coordinator Kicks), but the
		// persisted Failed authorization must not be overwritten with an
		// empty-fingerprint Drifted row: loadApplied drops that row on restart
		// and the authorized name would be lost while its directory is absent.
		if containsEvent(*progress, deploymentEvent{name: "gated", phase: "drifted"}) {
			t.Fatalf("progress = %+v, want no Drifted overwrite for a desired-invalid app",
				*progress)
		}
		if last, ok := lastProgressFor(*progress, "gated"); !ok ||
			last.phase != "failed" {
			t.Fatalf("last gated progress = (%+v, %v), want the Failed diagnostic to "+
				"remain the reported state", last, ok)
		}
		if b.prepares() != 0 {
			t.Fatalf("prepare calls = %d, want 0 (a local identity change never builds)",
				b.prepares())
		}
		if got := reg.GetByName("gated"); got != fn {
			t.Fatalf("registry = %+v, want the previous active entry serving", got)
		}
	})
}

// TestDesiredInvalidParseErrorWithoutRecordedIdentityReportsFailedOnly pins
// the reconciler's side of the unverifiable-invalid contract: when the
// materialized revision recorded NO template identity for a desired-invalid
// app, a parse error on the local bytes cannot distinguish a locally changed
// invalid template from the materialized one, so this path must NOT claim
// drift or request restoration from unverifiable content. It reports failed
// with the parse diagnostic on every pass, retains the previous active entry,
// clears stale pending rules, and never builds — requesting re-authorization
// of the exact shared SHA is the coordinator's independent backstop (see the
// drift-check issue matrix in the deployment package).
func TestDesiredInvalidParseErrorWithoutRecordedIdentityReportsFailedOnly(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	// Desired-invalid authorization with an EMPTY materialized identity: the
	// materialization could not read/hash the template, so nothing verifies
	// the local bytes.
	r.SetDeploymentAuthorizationWithInvalid(1,
		map[string]string{}, map[string]string{},
		map[string]string{"gated": "template invalid"},
		map[string]string{}, nil)

	r.reconcileApp("gated")

	if len(*drifts) != 0 {
		t.Fatalf("re-verification requests = %+v, want none: without a recorded "+
			"identity the reconciler cannot verify local bytes, so it must not "+
			"request restoration of unverifiable content", *drifts)
	}
	last, ok := lastProgressFor(*progress, "gated")
	if !ok || last.phase != "failed" || last.detail == "" {
		t.Fatalf("last progress = (%+v, %v), want failed with the parse diagnostic",
			last, ok)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (invalid desired bytes never build)",
			b.prepares())
	}
	if got := reg.GetByName("gated"); got != fn {
		t.Fatalf("registry = %+v, want the previous active entry retained", got)
	}
	if reg.HasPending("gated") {
		t.Fatal("the parse-error path must clear stale pending rules")
	}
}

// gateBuilder blocks the first preparation of gateName until release is
// closed, so a test can hold the serial pump inside one build.
type gateBuilder struct {
	mu       sync.Mutex
	prepCnt  int
	gateName string
	started  chan string
	release  chan struct{}
}

func (g *gateBuilder) Prepare(_ context.Context, fn app.App) (*runtime.Prepared, error) {
	g.mu.Lock()
	g.prepCnt++
	gated := fn.Name == g.gateName
	g.mu.Unlock()
	if gated {
		g.started <- fn.Name
		<-g.release
	}
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (g *gateBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

func (g *gateBuilder) prepares() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.prepCnt
}

// TestSerialPumpPreservesBacklogBehindBlockedBuild pins the pump's ordering
// and backlog guarantees with channels and barriers instead of sleeps:
// while one authorized app's build holds the pump, the drifted app queued
// behind it is NOT reconciled concurrently (serialized, not lost), a second
// periodic pass during the block re-enqueues the backlog instead of dropping
// it, and after the build is released the pump drains everything in FIFO
// order — the drifted app's report is never starved, the blocked app's
// duplicate queued pass still runs (observable through the fingerprint seam)
// and skips without a second build, and no duplicate drift request appears.
func TestSerialPumpPreservesBacklogBehindBlockedBuild(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "01-blocked")
	driftedDir := writeFnDir(t, root, "zzz-drifted")
	drifted := initialFn("zzz-drifted", driftedDir)

	blockedFP, blockedTID := authorizationFor(t, root, "01-blocked")
	driftedFP, driftedTID := authorizationFor(t, root, "zzz-drifted")

	gate := &gateBuilder{
		gateName: "01-blocked",
		started:  make(chan string, 8),
		release:  make(chan struct{}),
	}
	fpCalls := make(chan string, 16)
	var drifts chan deploymentEvent
	r, reg := newTestReconciler(t, root, gate,
		[]*runner.PreparedApp{drifted}, func(cfg *Config) {
			_, drifts = channelDeploymentRecorder(cfg)
			cfg.Fingerprint = func(
				dir string, tmpl *app.Template,
			) (*source.Selection, string, error) {
				selection, fp, err := app.SelectAndFingerprintApp(dir, tmpl)
				if err == nil {
					fpCalls <- filepath.Base(dir)
				}
				return selection, fp, err
			}
		})
	r.SetDeploymentAuthorization(1,
		map[string]string{"01-blocked": blockedFP, "zzz-drifted": driftedFP},
		map[string]string{"01-blocked": blockedTID, "zzz-drifted": driftedTID}, nil)
	if err := os.WriteFile(filepath.Join(driftedDir, "index.js"),
		[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit drifted app: %v", err)
	}

	launchSkeletonPump(t, r)
	// os.ReadDir sorts, so the queue starts with the app whose build blocks.
	r.reconcileAll()

	select {
	case name := <-gate.started:
		if name != "01-blocked" {
			t.Fatalf("first build = %q, want the leading 01-blocked", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the leading build never entered the pump")
	}
	select {
	case name := <-fpCalls:
		if name != "01-blocked" {
			t.Fatalf("first fingerprint = %q, want 01-blocked", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the blocked app never resolved its fingerprint")
	}
	// Serialized, not concurrent: the drifted app queued behind the blocked
	// build must not be reconciled while the pump is inside that build.
	select {
	case got := <-drifts:
		t.Fatalf("drift reported while the pump was still inside the blocked build: "+
			"%+v", got)
	default:
	}
	// A periodic pass while the build is blocked re-enqueues the backlog
	// (01-blocked then zzz-drifted) instead of dropping anything.
	r.reconcileAll()

	close(gate.release)

	// FIFO drain: zzz-drifted was queued first, so it is reconciled and
	// reported — never starved behind the re-enqueued build.
	select {
	case name := <-fpCalls:
		if name != "zzz-drifted" {
			t.Fatalf("next fingerprint = %q, want zzz-drifted draining first", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the drifted app queued behind the blocked build was never reconciled")
	}
	select {
	case got := <-drifts:
		if got.name != "zzz-drifted" ||
			got.detail != "materialized content changed locally" {
			t.Fatalf("drift report = %+v, want the zzz-drifted content-edit reason", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the drifted app queued behind the blocked build was starved")
	}
	// The duplicate queued pass for the blocked app runs next and skips.
	select {
	case name := <-fpCalls:
		if name != "01-blocked" {
			t.Fatalf("next fingerprint = %q, want the duplicate 01-blocked pass", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("duplicate queued work for the blocked app never ran")
	}
	// Then the duplicate pass of the drifted app completes the drain.
	select {
	case name := <-fpCalls:
		if name != "zzz-drifted" {
			t.Fatalf("next fingerprint = %q, want the duplicate zzz-drifted pass", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("duplicate queued work for the drifted app never ran")
	}

	if gate.prepares() != 1 {
		t.Fatalf("prepare calls = %d, want 1 (the duplicate pass skips unchanged, "+
			"unauthorized-to-build content)", gate.prepares())
	}
	select {
	case got := <-drifts:
		t.Fatalf("duplicate drift report = %+v, want one request per generation", got)
	default:
	}
	select {
	case name := <-fpCalls:
		t.Fatalf("unexpected extra fingerprint resolution for %q after the drain", name)
	default:
	}
	if got := reg.GetByName("01-blocked"); got == nil || got.Prepared() == nil ||
		got.Prepared().Image != "img-01-blocked" {
		t.Fatalf("registry[01-blocked] = %+v, want the built generation published", got)
	}
	if got := reg.GetByName("zzz-drifted"); got == nil || got.Prepared() == nil ||
		got.Prepared().Image != "img-zzz-drifted" {
		t.Fatalf("registry[zzz-drifted] = %+v, want the active generation retained", got)
	}
	if reg.HasPending("zzz-drifted") {
		t.Fatal("the drift observation must not leave a pending placeholder")
	}
}
