package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/deployment"
	"relay/internal/observability/metrics"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
)

// insertDeliveryEvent is the fixture delivery every worker template's single
// rule matches (event_name: INSERT).
func insertDeliveryEvent() map[string]any {
	return map[string]any{"event_name": "INSERT"}
}

// requireUnavailableRegistryCandidate asserts name is registered as an
// unavailable candidate: present for matching, with no runnable generation.
func requireUnavailableRegistryCandidate(
	t *testing.T,
	registry *runner.Registry,
	name string,
) *runner.PreparedApp {
	t.Helper()
	pf := registry.GetByName(name)
	if pf == nil {
		t.Fatalf("%s missing from the registry; its deliveries would match "+
			"nothing", name)
	}
	if built := pf.Prepared(); built != nil {
		t.Fatalf("%s registry entry carries image %q, want an unavailable "+
			"candidate", name, built.Image)
	}
	return pf
}

// requireMatchedNotUnmatched proves safety at the actual startup
// ready-to-consume boundary: it builds a runner over the exact unavailable
// candidate the registry holds and delivers the fixture event through Handle,
// so the runner's own candidate index and classification — not a test-side
// matcher — decide the outcome. The delivery must classify
// matched-but-unavailable: a retryable runner.ErrAppUnavailable that keeps the
// message pending, never the unmatched branch (which returns nil and lets the
// stream layer ACK the delivery away). The counters pin both sides of the
// partition. A full worker Run/Redis ACK is impractical here (the production
// app root is fixed), so this Handle boundary is what decides the ACK.
func requireMatchedNotUnmatched(t *testing.T, pf *runner.PreparedApp) {
	t.Helper()
	if pf == nil {
		t.Fatal("no registry candidate to match against")
	}
	if built := pf.Prepared(); built != nil {
		t.Fatalf("%s carries image %q, want the unavailable candidate",
			pf.Name(), built.Image)
	}
	template := pf.App().Template
	if template == nil {
		t.Fatalf("%s's registry entry lost its template; matching would drop it",
			pf.Name())
	}

	m := metrics.New()
	r := runner.NewWithMetrics([]*runner.PreparedApp{pf}, discardLogger(), m)
	err := r.Handle(context.Background(), "f01-unavailable", insertDeliveryEvent())
	if !errors.Is(err, runner.ErrAppUnavailable) {
		t.Fatalf("Handle = %v, want runner.ErrAppUnavailable (matched-but-"+
			"unavailable stays pending; nil would ACK %s's delivery as unmatched)",
			err, pf.Name())
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Fatalf("events_matched_total = %d, want 1 (the delivery matched)",
			got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Fatalf("events_unmatched_total = %d, want 0 (never unmatched)", got)
	}
}

// coordinatedStartupSet runs the production coordinated startup load over root
// and fails the test unless the real deploymentStartupValidation check accepts
// the compatible authorization.
func coordinatedStartupSet(
	t *testing.T,
	root string,
	snapshot deployment.Snapshot,
) startupAppSet {
	t.Helper()
	set, err := checkedStartupLoad(t, root, snapshot, true, &fakeStartupState{})
	if err != nil {
		t.Fatalf("startup load with a compatible authorization: %v", err)
	}
	return set
}

// mutatingPreparer deterministically reproduces the load-then-edit TOCTOU
// window behind the appPreparer seam: Prepare runs only AFTER the real
// loadStartupAppsChecked validation has completed, so it first applies an
// actual source edit to the filesystem and then returns the identity computed
// from the resulting tree via app.SelectAndFingerprintApp — the same
// source-snapshot read the real Prepare performs. The mutation and the
// recomputation are synchronous inside the single Prepare call: no timers, no
// sleeps, no probabilistic racing. It records the validated scan fingerprint it
// was handed so the test can prove the edit happened strictly after
// validation. It must be safe for concurrent use like the real Manager.
type mutatingPreparer struct {
	mu sync.Mutex
	// supplied records, per app, the validated scan fingerprint Prepare was
	// handed by prepareApps.
	supplied map[string]string
}

func newMutatingPreparer() *mutatingPreparer {
	return &mutatingPreparer{supplied: map[string]string{}}
}

func (p *mutatingPreparer) PrepareWithFingerprintAndSelection(
	_ context.Context,
	fn app.App,
	fingerprint string,
	_ *source.Selection,
) (*runtime.Prepared, error) {
	sourcePath := filepath.Join(fn.Dir, "main.py")
	current, err := os.ReadFile(sourcePath)
	if err != nil {
		return nil, err
	}
	mutated := append(current,
		[]byte("\ndef post_check_edit():\n    return 1\n")...)
	if err := os.WriteFile(sourcePath, mutated, 0o644); err != nil {
		return nil, err
	}
	_, changed, err := app.SelectAndFingerprintApp(fn.Dir, fn.Template)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.supplied[fn.Name] = fingerprint
	p.mu.Unlock()
	return &runtime.Prepared{
		Name:        fn.Name,
		Image:       "img-" + fn.Name,
		Fingerprint: changed,
	}, nil
}

func (p *mutatingPreparer) MaxConcurrentBuilds() int { return 1 }

func (p *mutatingPreparer) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

// suppliedFingerprint returns the validated scan fingerprint Prepare was
// handed for name.
func (p *mutatingPreparer) suppliedFingerprint(name string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.supplied[name]
}

// TestStartupPublicationRejectsPreparedIdentityChangedAfterCheck pins the
// post-check/preparation TOCTOU gap against a REAL filesystem edit: the
// authorization check validates the load's own scan, then Prepare — running
// strictly after that validation — edits an actual source file and returns
// the identity computed from the resulting tree. That built identity no
// longer matches the coordinated generation and must NOT be published. The
// registry keeps an unavailable candidate carrying the desired rules (so a
// matching delivery stays matched-but-unavailable instead of being ACKed
// unmatched), and the name is queued for repreparation with a failure record.
func TestStartupPublicationRejectsPreparedIdentityChangedAfterCheck(t *testing.T) {
	// given a coordinated startup load whose authorization passed the real
	// deployment check over this exact scan
	root := t.TempDir()
	writeWorkerApp(t, root, "drift", "def handler(e, context): return 1\n")
	fingerprints, identities := computedAuthorization(t, root)
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["drift"] = fingerprints["drift"]
	snapshot.TemplateIdentities["drift"] = identities["drift"]
	set := coordinatedStartupSet(t, root, snapshot)
	if set.authorized == nil ||
		set.authorized["drift"] != fingerprints["drift"] {
		t.Fatalf("authorized set = %v, want the checked fingerprint map",
			set.authorized)
	}

	// when Prepare runs after the validated load and REALLY edits a source
	// file before snapshotting the resulting tree
	preparer := newMutatingPreparer()
	candidates := prepareApps(context.Background(), preparer, set.records, nil,
		nil, discardLogger())
	if len(candidates) != 1 || candidates[0].Prepared() == nil {
		t.Fatalf("candidates = %+v, want one prepared generation", candidates)
	}
	built := candidates[0].Prepared()

	// then Prepare was handed the validated scan identity — the edit is
	// strictly post-check — and its returned identity is the actual post-edit
	// filesystem identity, not a label and not the authorized one
	if got := preparer.suppliedFingerprint("drift"); got != fingerprints["drift"] {
		t.Fatalf("Prepare fingerprint = %q, want the validated scan value %q",
			got, fingerprints["drift"])
	}
	_, fromDisk, err := app.SelectAndFingerprintApp(
		candidates[0].App().Dir, candidates[0].App().Template)
	if err != nil {
		t.Fatalf("fingerprint the mutated tree: %v", err)
	}
	if built.Fingerprint != fromDisk {
		t.Fatalf("built fingerprint = %q, want the actual post-edit "+
			"filesystem identity %q", built.Fingerprint, fromDisk)
	}
	if built.Fingerprint == fingerprints["drift"] {
		t.Fatalf("precondition: the mutated tree must fingerprint differently "+
			"from the authorized value %q (both %q)", fingerprints["drift"],
			built.Fingerprint)
	}

	registry := &runner.Registry{}
	publisher := &startupPublicationFake{}
	stateSink := &startupPublicationStateFake{}
	published, retry := publishStartupApps(context.Background(), candidates,
		registry, publisher, stateSink, set, discardLogger())

	// then the changed generation is never published: the selection fence is
	// never even entered for it
	if len(publisher.published) != 0 || publisher.callbackCalls != 0 {
		t.Fatalf("publication callbacks = %v/%d, want none for a changed "+
			"prepared identity", publisher.published, publisher.callbackCalls)
	}
	if len(retry) != 1 || retry[0] != "drift" {
		t.Fatalf("retry names = %v, want [drift] for repreparation", retry)
	}
	if len(stateSink.failures) != 1 || stateSink.failures[0] != "drift" {
		t.Fatalf("failure writes = %v, want the rejected drift app",
			stateSink.failures)
	}

	// and the registry still holds an unavailable candidate whose desired rules
	// keep the delivery matched — never ACKed unmatched
	if len(published) != 1 || published[0].Prepared() != nil {
		t.Fatalf("published view = %d entries, want one unavailable placeholder",
			len(published))
	}
	placeholder := requireUnavailableRegistryCandidate(t, registry, "drift")
	requireMatchedNotUnmatched(t, placeholder)
}

// TestStartupPublicationAcceptsCompatiblePreparedIdentity is the positive
// control for the post-check mutation gate: when Prepare returns the SAME
// fingerprint the deployment authorized, coordinated startup publishes the
// generation into the registry with no retry or failure record.
func TestStartupPublicationAcceptsCompatiblePreparedIdentity(t *testing.T) {
	// given the same coordinated startup shape
	root := t.TempDir()
	writeWorkerApp(t, root, "stable", "def handler(e, context): return 1\n")
	fingerprints, identities := computedAuthorization(t, root)
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["stable"] = fingerprints["stable"]
	snapshot.TemplateIdentities["stable"] = identities["stable"]
	set := coordinatedStartupSet(t, root, snapshot)

	// when Prepare's snapshot still equals the authorized fingerprint
	spy := &startupSpyPreparer{}
	candidates := prepareApps(context.Background(), spy, set.records, nil, nil,
		discardLogger())
	if len(candidates) != 1 || candidates[0].Prepared() == nil {
		t.Fatalf("candidates = %+v, want one prepared generation", candidates)
	}

	registry := &runner.Registry{}
	publisher := &startupPublicationFake{}
	stateSink := &startupPublicationStateFake{}
	published, retry := publishStartupApps(context.Background(), candidates,
		registry, publisher, stateSink, set, discardLogger())

	// then the compatible generation publishes normally
	if len(publisher.published) != 1 || publisher.published[0] != "stable" {
		t.Fatalf("publication callbacks = %v, want [stable]", publisher.published)
	}
	if len(published) != 1 || published[0] != candidates[0] ||
		registry.GetByName("stable") != candidates[0] {
		t.Fatalf("published view/registry = %d/%v, want the accepted handle",
			len(published), registry.GetByName("stable"))
	}
	if len(retry) != 0 || len(stateSink.failures) != 0 {
		t.Fatalf("retry/failure writes = %v/%v, want none for a compatible "+
			"generation", retry, stateSink.failures)
	}
}

// TestStartupPublicationKeepsBuiltIdentityInStandaloneMode pins the standalone
// half of the mutation gate: a standalone load returns a NIL authorized set,
// and publication keeps whatever identity Prepare captured — the reconciler
// owns any later drift.
func TestStartupPublicationKeepsBuiltIdentityInStandaloneMode(t *testing.T) {
	// given a standalone startup load (no coordinated deployment)
	root := t.TempDir()
	writeWorkerApp(t, root, "solo", "def handler(e, context): return 1\n")
	store := &fakeStartupState{}
	set, err := loadStartupAppsChecked(context.Background(), discardLogger(),
		store, root, metrics.New(), 0, nil, nil, false, nil)
	if err != nil {
		t.Fatalf("standalone load: %v", err)
	}
	if set.authorized != nil {
		t.Fatalf("authorized set = %v, want nil in standalone mode",
			set.authorized)
	}

	// when the prepared identity differs from the startup scan (standalone
	// startup never gates preparation against a shared generation)
	spy := &startupSpyPreparer{overrideFingerprint: "locally-changed-identity"}
	candidates := prepareApps(context.Background(), spy, set.records, nil, nil,
		discardLogger())
	if len(candidates) != 1 || candidates[0].Prepared() == nil {
		t.Fatalf("candidates = %+v, want one prepared generation", candidates)
	}

	registry := &runner.Registry{}
	publisher := &startupPublicationFake{}
	stateSink := &startupPublicationStateFake{}
	published, retry := publishStartupApps(context.Background(), candidates,
		registry, publisher, stateSink, set, discardLogger())

	// then the built identity wins: the generation publishes unchanged
	if len(publisher.published) != 1 || publisher.published[0] != "solo" {
		t.Fatalf("publication callbacks = %v, want [solo]", publisher.published)
	}
	if len(published) != 1 || published[0] != candidates[0] ||
		registry.GetByName("solo") != candidates[0] {
		t.Fatalf("published view/registry = %d/%v, want the accepted handle",
			len(published), registry.GetByName("solo"))
	}
	if got := published[0].Prepared().Fingerprint; got != "locally-changed-identity" {
		t.Fatalf("published fingerprint = %q, want the built identity", got)
	}
	if len(retry) != 0 || len(stateSink.failures) != 0 {
		t.Fatalf("retry/failure writes = %v/%v, want none in standalone mode",
			retry, stateSink.failures)
	}
}

// TestStartupBuildFailureStaysMatchedButUnavailable pins the build-failure
// case at the registry boundary: a coordinated candidate whose image could not
// be built is still registered as an unavailable candidate with its desired
// rules, so a matching delivery classifies matched-but-unavailable — never
// unmatched/ACKed — while no publication callback or failure record is
// fabricated at the publication step (the preparation already owns that
// outcome).
func TestStartupBuildFailureStaysMatchedButUnavailable(t *testing.T) {
	// given a coordinated startup load whose authorized app fails to build
	root := t.TempDir()
	writeWorkerApp(t, root, "unbuilt", "def handler(e, context): return 1\n")
	fingerprints, identities := computedAuthorization(t, root)
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["unbuilt"] = fingerprints["unbuilt"]
	snapshot.TemplateIdentities["unbuilt"] = identities["unbuilt"]
	set := coordinatedStartupSet(t, root, snapshot)

	spy := newBlockingPreparer(1, nil)
	spy.setFail("unbuilt", errors.New("injected build failure"))
	candidates := prepareApps(context.Background(), spy, set.records, nil, nil,
		discardLogger())
	if len(candidates) != 1 || candidates[0].Prepared() != nil {
		t.Fatalf("candidates = %+v, want one unavailable candidate",
			candidates)
	}

	registry := &runner.Registry{}
	publisher := &startupPublicationFake{}
	stateSink := &startupPublicationStateFake{}
	published, retry := publishStartupApps(context.Background(), candidates,
		registry, publisher, stateSink, set, discardLogger())

	// then the failed build is registered unavailable with no publication
	// callback, no failure rewrite, and no publication retry
	if len(publisher.published) != 0 || publisher.callbackCalls != 0 {
		t.Fatalf("publication callbacks = %v/%d, want none for a failed build",
			publisher.published, publisher.callbackCalls)
	}
	if len(stateSink.failures) != 0 {
		t.Fatalf("failure writes = %v, want none (the preparation owns that "+
			"outcome)", stateSink.failures)
	}
	if len(retry) != 0 {
		t.Fatalf("retry names = %v, want none at the publication step", retry)
	}
	if len(published) != 1 || published[0].Prepared() != nil {
		t.Fatalf("published view = %d entries, want one unavailable placeholder",
			len(published))
	}

	// and the registry keeps the app matched: a delivery for its rules
	// classifies matched-but-unavailable instead of ACKed unmatched
	placeholder := requireUnavailableRegistryCandidate(t, registry, "unbuilt")
	requireMatchedNotUnmatched(t, placeholder)
}
