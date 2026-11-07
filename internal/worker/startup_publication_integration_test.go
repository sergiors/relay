//go:build integration

package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// This critical-path integration test uses the real Manager lease coordinator
// and PostgreSQL selection lock. The source builds under the unpinned snapshot;
// a first pin is then committed before startup publication. The stale build
// must become an unavailable placeholder, enter the retry set, and relinquish
// its image lease rather than keeping an image the worker does not serve.
func TestIntegrationStartupStalePublicationReleasesImageLease(t *testing.T) {
	_ = testutil.RequireDocker(t)
	st := testutil.NewState(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	manager, err := runtime.NewManager(
		discardLogger(), metrics.New(), "startup-stale-test",
		runtime.WithLifecycleContext(ctx),
		runtime.WithDeferredMaintenance(),
		runtime.WithRuntimeImageSelectionStore(newStateRuntimeImageSelectionStore(st)),
	)
	if err != nil {
		t.Fatalf("open runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	fn := startupTestApp(t, "startup-stale", false)
	templateText := "runtime: nodejs24.x\nevents:\n  - handler: index.handler\n    pattern:\n      event_name: [INSERT]\n"
	if err := os.WriteFile(filepath.Join(fn.Dir, "template.yaml"), []byte(templateText), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fn.Dir, "index.js"), []byte("export function handler() { return 1; }\n"), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	selection, fingerprint, err := app.SelectAndFingerprintApp(fn.Dir, fn.Template)
	if err != nil {
		t.Fatalf("select and fingerprint: %v", err)
	}
	built, err := manager.PrepareWithFingerprintAndSelection(ctx, fn, fingerprint, selection)
	if err != nil {
		t.Fatalf("prepare unpinned startup image: %v", err)
	}
	image := built.Image
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = manager.RetireImageLease(cleanupCtx, image)
	})
	if built.ImageSelection != "" {
		t.Fatalf("prepared selection = %q, want unpinned identity", built.ImageSelection)
	}
	if manager.LeaseCount(built.Image) != 1 {
		t.Fatalf("prepared image lease count = %d, want 1 before publication", manager.LeaseCount(built.Image))
	}

	// Include a successful no-runtime app as a control: it exercises the accepted
	// publication/activation branch without needing another Docker build.
	serviceTemplate := "services:\n  - name: external\n    image: nginx:alpine\n    port: 80\n"
	serviceTmpl, err := app.ParseTemplate([]byte(serviceTemplate))
	if err != nil {
		t.Fatalf("parse external-service template: %v", err)
	}
	acceptedFn := app.App{Name: "startup-accepted", Dir: t.TempDir(), Template: serviceTmpl}
	if err := os.WriteFile(filepath.Join(acceptedFn.Dir, "template.yaml"), []byte(serviceTemplate), 0o644); err != nil {
		t.Fatalf("write external-service template: %v", err)
	}
	acceptedFingerprint, err := app.FingerprintApp(acceptedFn.Dir, acceptedFn.Template)
	if err != nil {
		t.Fatalf("fingerprint external-service app: %v", err)
	}
	acceptedBuilt, err := manager.PrepareWithFingerprintAndSelection(ctx, acceptedFn, acceptedFingerprint, nil)
	if err != nil {
		t.Fatalf("prepare external-service app: %v", err)
	}

	_, err = st.CompareAndSwapRuntimeImageSelection(ctx, state.RuntimeImageSelection{
		RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine",
		Digest: "sha256:" + strings.Repeat("a", 64),
	}, 0)
	if err != nil {
		t.Fatalf("pin runtime before publication: %v", err)
	}

	candidate := runner.NewPrepared(fn, built, manager)
	acceptedCandidate := runner.NewPrepared(acceptedFn, acceptedBuilt, manager)
	registry := &runner.Registry{}
	published, retry := publishStartupApps(
		ctx, []*runner.PreparedApp{acceptedCandidate, candidate}, registry, manager, nil,
		startupAppSet{fencingTokens: map[string]state.FencingToken{fn.Name: 1, acceptedFn.Name: 2}}, discardLogger(),
	)
	if len(retry) != 1 || retry[0] != fn.Name {
		t.Fatalf("retry names = %v, want [%s]", retry, fn.Name)
	}
	if len(published) != 2 || published[0] != acceptedCandidate || published[1].Prepared() != nil {
		t.Fatalf("published result = %+v, want accepted app and stale unavailable placeholder", published)
	}
	if registry.GetByName(acceptedFn.Name) != acceptedCandidate || acceptedCandidate.Prepared() == nil {
		t.Fatal("selection-current app was not accepted into the registry")
	}
	if registry.GetByName(fn.Name) == nil || registry.GetByName(fn.Name).Prepared() != nil {
		t.Fatal("stale image became available in the registry instead of preserving an unavailable match")
	}
	if _, ok := manager.PoolSnapshot(fn.Name); ok {
		t.Fatal("stale generation activated runtime state")
	}
	if manager.LeaseCount(built.Image) != 0 {
		t.Fatalf("stale image lease count = %d, want 0 after rejection", manager.LeaseCount(built.Image))
	}
	if images := startupPreparedImages(published); len(images) != 0 {
		t.Fatalf("stale image entered startup keep-set inputs: %v", images)
	}
}
