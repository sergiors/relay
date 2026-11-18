//go:build integration

package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/state"
)

const (
	deploymentCommitA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	deploymentCommitB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// publishDesiredForTest commits one operator revision as the shared desired
// generation.
func publishDesiredForTest(t *testing.T, st *state.State, commit string) state.DeploymentPublishResult {
	t.Helper()
	result, err := st.PublishDeploymentDesired(context.Background(),
		state.DeploymentPublishRequest{
			Desired: state.DeploymentDesired{
				Commit:     commit,
				Repository: "git@github.com:acme/r.git",
				Ref:        "main",
				Publisher:  "test",
			},
			ExpectedGeneration: -1,
			AllowPinned:        true,
		})
	if err != nil {
		t.Fatalf("publish desired %s: %v", commit, err)
	}
	return result
}

// advancingBuilder prepares the app AND makes a newer shared revision durable
// mid-preparation: every Prepare call publishes the next deployment generation
// before returning, exactly modeling a webhook landing while the build runs.
type advancingBuilder struct {
	st        *state.State
	image     string
	publishes int
}

func (b *advancingBuilder) Prepare(
	ctx context.Context, fn app.App,
) (*runtime.Prepared, error) {
	return b.PrepareWithFingerprintAndSelection(ctx, fn, "", nil)
}

// PrepareWithFingerprintAndSelection makes advancingBuilder a full
// selection-preparing builder, so the reconcile reaches the production
// publication seam.
func (b *advancingBuilder) PrepareWithFingerprintAndSelection(
	ctx context.Context, fn app.App, _ string, _ *source.Selection,
) (*runtime.Prepared, error) {
	b.publishes++
	if _, err := b.st.PublishDeploymentDesired(ctx, state.DeploymentPublishRequest{
		Desired: state.DeploymentDesired{
			Commit:     deploymentCommitB,
			Repository: "git@github.com:acme/r.git",
			Ref:        "main",
			Publisher:  "test-advance",
		},
		ExpectedGeneration: -1,
		AllowPinned:        true,
	}); err != nil {
		return nil, err
	}
	return &runtime.Prepared{Name: fn.Name, Image: b.image}, nil
}

func (b *advancingBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// RefreshRuntimeImageSelections and PublishPrepared complete the
// fencedSelectionBuilder pair: the publication runs through the same seam the
// runtime Manager uses, so the stale-generation branch under test is the
// production branch.
func (b *advancingBuilder) RefreshRuntimeImageSelections(context.Context) ([]string, error) {
	return nil, nil
}

func (b *advancingBuilder) PublishPrepared(
	_ context.Context, _ *runtime.Prepared, publish func() error,
) error {
	return publish()
}

// seedAuthorizedTree edits the app to NEW materialized content and installs
// the matching cluster authorization for generation, returning the new
// fingerprint. The reconciler's skip key still holds the ORIGINAL startup
// fingerprint, so the next pass sees a genuinely new authorized generation.
func seedAuthorizedTree(
	t *testing.T, r *Reconciler, root, name string, generation int64,
) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){ console.log('materialized'); }\n"), 0o644); err != nil {
		t.Fatalf("write materialized content: %v", err)
	}
	fp, err := app.FingerprintApp(dir, mustParse(template))
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	src, err := app.ReadTemplateSource(root, name)
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	r.SetDeploymentAuthorization(generation,
		map[string]string{name: fp},
		map[string]string{name: src.Identity}, nil)
	return fp
}

// TestStalePreparationCannotPublishAfterNewGeneration pins the core cluster
// invariant: a preparation started for deployment generation N must not become
// the active app after generation N+1 is durable in PostgreSQL — even though
// its content fingerprint matches the authorization. The registry keeps
// serving the previous generation and the stale outcome is reported.
func TestStalePreparationCannotPublishAfterNewGeneration(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &advancingBuilder{st: nil, image: "img-gated-g2"}
	var progress *[]deploymentEvent
	r, reg, st := newTestStateReconciler(t, root, b,
		[]*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
			progress, _ = deploymentRecorder(cfg)
		})
	b.st = st
	shutdownSkeletonReconciler(t, r)

	// Generation 1 is the shared desired revision and the authorization this
	// pass reconciles under.
	publishDesiredForTest(t, st, deploymentCommitA)
	authorizedFP := seedAuthorizedTree(t, r, root, "gated", 1)

	r.reconcileApp("gated")

	if b.publishes != 1 {
		t.Fatalf("prepare calls = %d, want 1 (the stale attempt still builds)", b.publishes)
	}
	got := reg.GetByName("gated")
	if got == nil || got.Prepared() == nil || got.Prepared().Image != "img-gated" {
		t.Fatalf("registry = %+v, want the previous img-gated generation still serving", got)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "gated", phase: state.DeploymentAppActivating,
		detail: "deployment generation changed during preparation",
	}) {
		t.Fatalf("progress = %+v, want the stale-generation report", *progress)
	}
	// The stale pass must not have published an active generation: the row
	// shows the desired fingerprint it prepared but no completed success.
	detail, ok := st.GetApp("gated")
	if !ok {
		t.Fatal("app row disappeared")
	}
	if detail.Fingerprint != "" || detail.Status == state.StatusReady {
		t.Fatalf("app row = %+v, want no active publication from the stale pass", detail)
	}
	if detail.DesiredFingerprint != authorizedFP {
		t.Fatalf("desired fingerprint = %q, want the prepared %q",
			detail.DesiredFingerprint, authorizedFP)
	}
}

// TestDesiredInvalidTemplateIsFailedNotDrift pins the live-reconciler side of
// a desired revision whose template is invalid: with the app authorized
// through the invalid set (and its exact materialized identity), a reconcile
// classifies it as a FAILED desired app — not local drift — records the
// invalid diagnostic against the state row, reports the failed activation
// phase, and leaves the previously active registry entry serving untouched:
// invalid template bytes never activate (no preparation runs), while the
// recorded identity still gates locally edited bytes as drift.
func TestDesiredInvalidTemplateIsFailedNotDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg, st := newTestStateReconciler(t, root, b,
		[]*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
			progress, drifts = deploymentRecorder(cfg)
		})

	// A usable active generation serves before the invalid materialization
	// arrives, so "retains the active generation" is observable.
	fencingToken := recordSuccess(t, st, "gated", "img-gated", "fp-active",
		time.Now(), fn.App())
	r.SeedFencingToken("gated", fencingToken)

	// The materialized revision carries an invalid template for "gated"; its
	// exact bytes are the identity the deployment recorded at materialization.
	invalidPath := filepath.Join(dir, "template.yaml")
	if err := os.WriteFile(invalidPath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	src, err := app.ReadTemplateSource(root, "gated")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	r.SetDeploymentAuthorizationWithInvalid(1,
		map[string]string{}, map[string]string{"gated": src.Identity},
		map[string]string{"gated": "runtime python9.9 is unknown"},
		map[string]string{"gated": src.Identity},
		nil)

	r.reconcileApp("gated")

	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none (a desired invalid app is not drift)",
			*drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "gated", phase: state.DeploymentAppFailed,
	}) {
		t.Fatalf("progress = %+v, want a failed phase for the desired invalid app",
			*progress)
	}
	if got := reg.GetByName("gated"); got != fn || got.Prepared() == nil ||
		got.Prepared().Image != "img-gated" {
		t.Fatalf("registry = %+v, want the original active generation still serving", got)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (an invalid desired app never builds)",
			b.prepares())
	}
	detail, ok := st.GetApp("gated")
	if !ok {
		t.Fatal("app row disappeared")
	}
	if detail.Status != state.StatusDegraded ||
		detail.LastReconcileStatus != state.ReconcileFailed || detail.LastError == "" {
		t.Fatalf("app row = status %q outcome %q error %q, "+
			"want a recorded invalid failure with the active generation retained",
			detail.Status, detail.LastReconcileStatus, detail.LastError)
	}
	if detail.Image != "img-gated" {
		t.Fatalf("active image = %q, want the previous img-gated retained", detail.Image)
	}

	// A repeat pass recognizes the persisted invalid identity: still no drift,
	// still no build.
	r.reconcileApp("gated")
	if len(*drifts) != 0 {
		t.Fatalf("drift reports after a repeat pass = %+v, want none", *drifts)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls after a repeat pass = %d, want 0", b.prepares())
	}
}

// TestAuthorizedGenerationActivates pins the positive path: when the shared
// desired generation matches the authorization, a reconcile of the
// materialized tree prepares, publishes inside the deployment publication
// boundary, records success stamped with the deployment generation, and reports
// the app active. The drift gate must not block a genuine deployment.
func TestAuthorizedGenerationActivates(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &fixedImageBuilder{image: "img-gated-g2"}
	var progress *[]deploymentEvent
	r, reg, st := newTestStateReconciler(t, root, b,
		[]*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
			progress, _ = deploymentRecorder(cfg)
		})

	// Generation 2 is durable AND installed as the authorization; the skip key
	// still holds the ORIGINAL startup fingerprint, so this is a new generation.
	publishDesiredForTest(t, st, deploymentCommitA)
	publishDesiredForTest(t, st, deploymentCommitB)
	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found || desired.Generation != 2 {
		t.Fatalf("precondition: desired = (%+v, %v, %v), want generation 2", desired, found, err)
	}
	authorizedFP := seedAuthorizedTree(t, r, root, "gated", 2)

	r.reconcileApp("gated")

	got := reg.GetByName("gated")
	if got == nil || got.Prepared() == nil || got.Prepared().Image != "img-gated-g2" {
		t.Fatalf("registry = %+v, want the new img-gated-g2 generation published", got)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "gated", phase: state.DeploymentAppActivating,
	}) {
		t.Fatalf("progress = %+v, want an activating phase", *progress)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "gated", phase: state.DeploymentAppActive,
	}) {
		t.Fatalf("progress = %+v, want an active phase after success", *progress)
	}
	detail, ok := st.GetApp("gated")
	if !ok {
		t.Fatal("app row disappeared")
	}
	if detail.Status != state.StatusReady || detail.Fingerprint != authorizedFP {
		t.Fatalf("app row = status %q fingerprint %q, want ready/%s",
			detail.Status, detail.Fingerprint, authorizedFP)
	}
	if detail.SourceGeneration != 2 {
		t.Fatalf("SourceGeneration = %d, want the authorized generation 2",
			detail.SourceGeneration)
	}
}
