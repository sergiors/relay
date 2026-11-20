package reconciler

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/source"
)

// deploymentEvent is one captured DeploymentProgress or DeploymentDrift
// callback invocation.
type deploymentEvent struct {
	name   string
	phase  string
	detail string
}

// deploymentRecorder installs the two cluster callbacks on a Config and
// returns the slices they append to. reconcileApp runs synchronously in these
// tests, so plain slices need no locking.
func deploymentRecorder(cfg *Config) (progress, drifts *[]deploymentEvent) {
	prog := []deploymentEvent{}
	drift := []deploymentEvent{}
	cfg.DeploymentProgress = func(name, phase, errMessage string) {
		prog = append(prog, deploymentEvent{name: name, phase: phase, detail: errMessage})
	}
	cfg.DeploymentDrift = func(name, reason string) {
		drift = append(drift, deploymentEvent{name: name, detail: reason})
	}
	return &prog, &drift
}

// containsEvent matches every non-empty field of want against the recorded
// events: drift records assert name+detail, progress records assert
// name+phase, so each slice's meaningful fields are pinned without requiring
// the other slice's payload.
func containsEvent(events []deploymentEvent, want deploymentEvent) bool {
	for _, e := range events {
		if e.name != want.name || e.phase != want.phase {
			continue
		}
		if want.detail != "" && e.detail != want.detail {
			continue
		}
		return true
	}
	return false
}

// authorizationFor computes the cluster authorization triple (fingerprint,
// template identity) for an app as it currently exists on disk — the exact
// values the deployment coordinator records at materialization time.
func authorizationFor(t *testing.T, root, name string) (fingerprint, templateID string) {
	t.Helper()
	dir := filepath.Join(root, name)
	fp, err := app.FingerprintApp(dir, mustParse(template))
	if err != nil {
		t.Fatalf("fingerprint %s: %v", name, err)
	}
	src, err := app.ReadTemplateSource(root, name)
	if err != nil {
		t.Fatalf("template source %s: %v", name, err)
	}
	return fp, src.Identity
}

// TestClusterModeIgnoresLocalAppAddition pins that in cluster mode an app
// directory that is not part of the authorized generation is drift: it never
// enters the registry, never triggers a build, and its drift report is
// deduplicated per generation.
func TestClusterModeIgnoresLocalAppAddition(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "authorized")

	fn := initialFn("authorized", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})
	fp, tid := authorizationFor(t, root, "authorized")
	r.SetDeploymentAuthorization(3,
		map[string]string{"authorized": fp},
		map[string]string{"authorized": tid}, nil)

	writeFnDir(t, root, "local-add")
	r.reconcileApp("local-add")

	if !containsEvent(*drifts, deploymentEvent{
		name: "local-add", detail: "not part of the active deployment",
	}) {
		t.Fatalf("drift reports = %+v, want the local-addition reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "local-add", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want a drifted phase for local-add", *progress)
	}
	if pf := reg.GetByName("local-add"); pf != nil {
		t.Fatalf("local addition entered the registry: %+v", pf)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (a local addition never builds)", b.prepares())
	}

	// The drift report is deduplicated per authorized generation.
	r.reconcileApp("local-add")
	if len(*drifts) != 1 {
		t.Fatalf("drift reports after a repeat pass = %+v, want exactly one", *drifts)
	}
}

// TestClusterModeIgnoresLocalContentEdit pins that an authorized app whose
// live bytes no longer match the materialized fingerprint is drift: the
// registry keeps serving the active generation, no build runs, and no pending
// placeholder is left behind.
func TestClusterModeIgnoresLocalContentEdit(t *testing.T) {
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

	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("authorized")

	if !containsEvent(*drifts, deploymentEvent{
		name: "authorized", detail: "materialized content changed locally",
	}) {
		t.Fatalf("drift reports = %+v, want the content-edit reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "authorized", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want a drifted phase", *progress)
	}
	if pf := reg.GetByName("authorized"); pf == nil || pf.Prepared() == nil ||
		pf.Prepared().Image != "img-authorized" {
		t.Fatalf("registry = %+v, want the active img-authorized generation untouched", pf)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (local drift never builds)", b.prepares())
	}
	if reg.HasPending("authorized") {
		t.Fatal("local drift must not leave a pending placeholder in the registry")
	}

	r.reconcileApp("authorized")
	if len(*drifts) != 1 {
		t.Fatalf("drift reports after a repeat pass = %+v, want exactly one", *drifts)
	}
}

// TestClusterModeIgnoresLocalTemplateEdit pins that editing template.yaml in
// place — whether the edit stays valid or breaks the template — is drift, not
// a new desired generation: the identity gate fires before parsing, so neither
// variant prepares, replaces, or invalidates anything.
func TestClusterModeIgnoresLocalTemplateEdit(t *testing.T) {
	tests := []struct {
		name  string
		write func(t *testing.T, templatePath string)
	}{
		{
			name: "valid template edit",
			write: func(t *testing.T, templatePath string) {
				if err := os.WriteFile(templatePath, []byte(
					"# local edit\n"+template), 0o644); err != nil {
					t.Fatalf("write template: %v", err)
				}
			},
		},
		{
			name: "invalid template edit",
			write: func(t *testing.T, templatePath string) {
				if err := os.WriteFile(templatePath,
					[]byte("runtime: python9.9\n"), 0o644); err != nil {
					t.Fatalf("write template: %v", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := writeFnDir(t, root, "authorized")

			fn := initialFn("authorized", dir)
			b := &fakeBuilder{}
			var progress, drifts *[]deploymentEvent
			r, reg := newTestReconciler(t, root, b,
				[]*runner.PreparedApp{fn}, func(cfg *Config) {
					progress, drifts = deploymentRecorder(cfg)
				})
			fp, tid := authorizationFor(t, root, "authorized")
			r.SetDeploymentAuthorization(1,
				map[string]string{"authorized": fp},
				map[string]string{"authorized": tid}, nil)

			tc.write(t, filepath.Join(dir, "template.yaml"))
			r.reconcileApp("authorized")

			if !containsEvent(*drifts, deploymentEvent{
				name: "authorized", detail: "materialized template changed locally",
			}) {
				t.Fatalf("drift reports = %+v, want the template-edit reason", *drifts)
			}
			if !containsEvent(*progress, deploymentEvent{
				name: "authorized", phase: "drifted",
			}) {
				t.Fatalf("progress = %+v, want a drifted phase", *progress)
			}
			if pf := reg.GetByName("authorized"); pf == nil || pf.Prepared() == nil ||
				pf.Prepared().Image != "img-authorized" {
				t.Fatalf("registry = %+v, want the active generation untouched", pf)
			}
			if b.prepares() != 0 {
				t.Fatalf("prepare calls = %d, want 0 (a local template edit never builds)",
					b.prepares())
			}
		})
	}
}

// TestClusterModeAuthorizedDeletionIsDrift pins that an authorized app whose
// directory disappears locally is restored, never removed: the registry keeps
// serving it, the removal hooks stay quiet, and the coordinator is asked to
// restore the materialized tree.
func TestClusterModeAuthorizedDeletionIsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "authorized")

	fn := initialFn("authorized", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	removals := 0
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.RemoveApp = func(string) { removals++ }
		cfg.RemoveServices = func(string) { removals++ }
	})
	fp, tid := authorizationFor(t, root, "authorized")
	r.SetDeploymentAuthorization(1,
		map[string]string{"authorized": fp},
		map[string]string{"authorized": tid}, nil)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove app dir: %v", err)
	}
	r.reconcileApp("authorized")

	if !containsEvent(*drifts, deploymentEvent{
		name: "authorized", detail: "materialized app is missing locally",
	}) {
		t.Fatalf("drift reports = %+v, want the missing-directory reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "authorized", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want a drifted phase", *progress)
	}
	if pf := reg.GetByName("authorized"); pf == nil {
		t.Fatal("an authorized app deleted locally must keep serving from the registry")
	}
	if removals != 0 {
		t.Fatalf("removal hook calls = %d, want 0 (only authorized removals remove)", removals)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
}

// TestClusterModeAuthorizedRemovalApplied pins the ONLY removal cluster mode
// authorizes: a name listed in the generation's removed set whose directory is
// gone follows the normal removal lifecycle.
func TestClusterModeAuthorizedRemovalApplied(t *testing.T) {
	root := t.TempDir()
	goneDir := writeFnDir(t, root, "gone")

	fn := initialFn("gone", goneDir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	removals := []string{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.RemoveApp = func(name string) { removals = append(removals, name) }
	})
	r.SetDeploymentAuthorization(2,
		map[string]string{}, map[string]string{}, []string{"gone"})

	if err := os.RemoveAll(goneDir); err != nil {
		t.Fatalf("remove app dir: %v", err)
	}
	r.reconcileApp("gone")

	if reg.GetByName("gone") != nil {
		t.Fatal("an authorized removal must drop the registry entry")
	}
	if len(removals) != 1 || removals[0] != "gone" {
		t.Fatalf("removal hooks = %v, want [gone]", removals)
	}
	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none (this removal is authorized)", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{name: "gone", phase: "removed"}) {
		t.Fatalf("progress = %+v, want a removed phase for gone", *progress)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (removal never builds)", b.prepares())
	}
}

// TestClusterModeRecreationOfRemovedAppIsDrift pins the other side of the
// removal contract: a name in the removed set that reappears on disk is a
// local recreation, not an authorized resurrection — it is reported as drift
// and never prepared or swapped in.
func TestClusterModeRecreationOfRemovedAppIsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "recreated")

	fn := initialFn("recreated", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	removals := 0
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.RemoveApp = func(string) { removals++ }
	})
	r.SetDeploymentAuthorization(2,
		map[string]string{}, map[string]string{}, []string{"recreated"})

	r.reconcileApp("recreated")

	if !containsEvent(*drifts, deploymentEvent{
		name: "recreated", detail: "app is not part of the active deployment",
	}) {
		t.Fatalf("drift reports = %+v, want the recreation reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "recreated", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want a drifted phase", *progress)
	}
	if pf := reg.GetByName("recreated"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a local recreation must not change the registry in place")
	}
	if removals != 0 {
		t.Fatalf("removal hook calls = %d, want 0 (the directory exists)", removals)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (a recreation never builds)", b.prepares())
	}
}

// TestClusterModeFingerprintFailureIsDrift pins that a fingerprint error on an
// authorized app is treated as local drift — the active generation keeps
// serving and no build runs — instead of the standalone failure-recording path.
func TestClusterModeFingerprintFailureIsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "authorized")

	fn := initialFn("authorized", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.Fingerprint = func(string, *app.Template) (*source.Selection, string, error) {
			return nil, "", errBoom
		}
	})
	fp, tid := authorizationFor(t, root, "authorized")
	r.SetDeploymentAuthorization(1,
		map[string]string{"authorized": fp},
		map[string]string{"authorized": tid}, nil)

	r.reconcileApp("authorized")

	if !containsEvent(*drifts, deploymentEvent{
		name: "authorized", detail: "materialized source can no longer be fingerprinted",
	}) {
		t.Fatalf("drift reports = %+v, want the fingerprint-failure reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "authorized", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want a drifted phase", *progress)
	}
	if pf := reg.GetByName("authorized"); pf == nil || pf.Prepared() == nil ||
		pf.Prepared().Image != "img-authorized" {
		t.Fatalf("registry = %+v, want the active generation untouched", pf)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
}

// seedDesiredInvalid writes an INVALID template for name under root and
// returns the exact materialized identity the deployment recorded.
func seedDesiredInvalid(t *testing.T, root, name string) string {
	t.Helper()
	dir := writeFnDir(t, root, name)
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	src, err := app.ReadTemplateSource(root, name)
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	return src.Identity
}

// authorizeDesiredInvalid installs generation-1 authorization for one
// desired-but-unloadable app, mirroring the worker's
// SetDeploymentAuthorizationWithInvalid wiring (no content fingerprint; the
// invalid set carries the diagnostic and the recorded template identity).
func authorizeDesiredInvalid(r *Reconciler, name, templateID string) {
	r.SetDeploymentAuthorizationWithInvalid(1,
		map[string]string{}, map[string]string{},
		map[string]string{name: "template invalid"},
		map[string]string{name: templateID}, nil)
}

// TestClusterModeLocalEditOfDesiredInvalidAppIsDrift pins that a LOCAL edit to
// a desired-invalid app's bytes is drift: the recorded invalid identity no
// longer matches, so the app is never reclassified as a fresh desired state —
// the registry keeps the previous active entry and nothing builds.
func TestClusterModeLocalEditOfDesiredInvalidAppIsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})

	invalidPath := filepath.Join(dir, "template.yaml")
	if err := os.WriteFile(invalidPath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	src, err := app.ReadTemplateSource(root, "gated")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	authorizeDesiredInvalid(r, "gated", src.Identity)

	// A different local edit to the invalid bytes breaks the recorded identity.
	if err := os.WriteFile(invalidPath, []byte("runtime: python9.1\n"), 0o644); err != nil {
		t.Fatalf("edit invalid template: %v", err)
	}
	r.reconcileApp("gated")

	if !containsEvent(*drifts, deploymentEvent{
		name: "gated", detail: "materialized template changed locally",
	}) {
		t.Fatalf("drift reports = %+v, want the template-edit reason", *drifts)
	}
	// The drift request still reaches the coordinator (Kick), but a
	// desired-invalid app must never be overwritten with an empty-fingerprint
	// Drifted row: loadApplied drops that row on restart and the authorized
	// name would be lost while its directory is absent.
	if containsEvent(*progress, deploymentEvent{
		name: "gated", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want no Drifted overwrite for a desired-invalid app",
			*progress)
	}
	if got := reg.GetByName("gated"); got != fn {
		t.Fatalf("registry = %+v, want the previous active entry untouched", got)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
}

// TestClusterModeDeletionOfDesiredInvalidAppIsDrift pins that deleting a
// desired-invalid app locally is drift, never the removal path: the previous
// active entry keeps serving, the removal hooks stay quiet, and the coordinator
// is asked to restore the tree.
func TestClusterModeDeletionOfDesiredInvalidAppIsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	removals := 0
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.RemoveApp = func(string) { removals++ }
	})
	invalidPath := filepath.Join(dir, "template.yaml")
	if err := os.WriteFile(invalidPath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	src, err := app.ReadTemplateSource(root, "gated")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	authorizeDesiredInvalid(r, "gated", src.Identity)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove app dir: %v", err)
	}
	r.reconcileApp("gated")

	if !containsEvent(*drifts, deploymentEvent{
		name: "gated", detail: "materialized app is missing locally",
	}) {
		t.Fatalf("drift reports = %+v, want the missing-directory reason", *drifts)
	}
	// Missing-directory drift for a desired-invalid app also requests the
	// restore (above) but must not overwrite the persisted Failed
	// authorization with an empty-fingerprint Drifted row.
	if containsEvent(*progress, deploymentEvent{
		name: "gated", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want no Drifted overwrite for a desired-invalid app",
			*progress)
	}
	if got := reg.GetByName("gated"); got != fn {
		t.Fatalf("registry = %+v, want the previous active entry still serving", got)
	}
	if removals != 0 {
		t.Fatalf("removal hook calls = %d, want 0 (only authorized removals remove)",
			removals)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
}

// TestClusterModeRecreationOfDesiredInvalidAppIsDrift pins that recreating a
// desired-invalid app locally with DIFFERENT bytes is drift: the recorded
// invalid identity no longer matches, so locally introduced content is never
// adopted as the desired invalid state.
func TestClusterModeRecreationOfDesiredInvalidAppIsDrift(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	fn := initialFn("gated", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})

	invalidPath := filepath.Join(dir, "template.yaml")
	if err := os.WriteFile(invalidPath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	src, err := app.ReadTemplateSource(root, "gated")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	authorizeDesiredInvalid(r, "gated", src.Identity)

	// The app disappears and is locally recreated with different invalid bytes.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove app dir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("recreate dir: %v", err)
	}
	if err := os.WriteFile(invalidPath, []byte("runtime: no-such-runtime\n"), 0o644); err != nil {
		t.Fatalf("recreate invalid template: %v", err)
	}
	r.reconcileApp("gated")

	if !containsEvent(*drifts, deploymentEvent{
		name: "gated", detail: "materialized template changed locally",
	}) {
		t.Fatalf("drift reports = %+v, want the template-edit reason", *drifts)
	}
	// The drift request still reaches the coordinator (Kick), but a
	// desired-invalid app must never be overwritten with an empty-fingerprint
	// Drifted row: loadApplied drops that row on restart and the authorized
	// name would be lost while its directory is absent.
	if containsEvent(*progress, deploymentEvent{
		name: "gated", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want no Drifted overwrite for a desired-invalid app",
			*progress)
	}
	if got := reg.GetByName("gated"); got != fn {
		t.Fatalf("registry = %+v, want the previous active entry untouched", got)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
}

// TestClusterModeGenerationZeroAuthorizesNothing pins the configured-Git-node
// with no published revision (generation 0): the gate is active but authorizes
// no local app, so every local tree is inert drift — never prepared, never
// swapped — until a revision is materialized.
func TestClusterModeGenerationZeroAuthorizesNothing(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "local-app")

	fn := initialFn("local-app", dir)
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})
	r.SetDeploymentAuthorization(0, nil, nil, nil)

	r.reconcileApp("local-app")

	if !containsEvent(*drifts, deploymentEvent{
		name: "local-app", detail: "not part of the active deployment",
	}) {
		t.Fatalf("drift reports = %+v, want the unauthorized reason", *drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "local-app", phase: "drifted",
	}) {
		t.Fatalf("progress = %+v, want a drifted phase", *progress)
	}
	if got := reg.GetByName("local-app"); got != fn {
		t.Fatalf("registry = %+v, want the previous entry untouched", got)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (generation 0 authorizes nothing)",
			b.prepares())
	}
}

// TestClusterModeReprepareFailureKeepsUnavailablePlaceholder pins the
// startup-shaped reprepare of an authorized app whose build fails: the desired
// rules stay published as the matched-but-unavailable pending placeholder (so
// deliveries remain matched and pending, never ACKed unmatched), the failure is
// reported as deployment progress, and no drift is claimed.
func TestClusterModeReprepareFailureKeepsUnavailablePlaceholder(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "late")

	b := &fakeBuilder{}
	b.fail = true
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b, nil, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
	})
	fp, tid := authorizationFor(t, root, "late")
	r.SetDeploymentAuthorization(1,
		map[string]string{"late": fp},
		map[string]string{"late": tid}, nil)

	r.reconcileApp("late")

	if b.prepares() != 1 {
		t.Fatalf("prepare calls = %d, want 1 (the authorized reprepare runs and fails)",
			b.prepares())
	}
	if !reg.HasPending("late") {
		t.Fatal("a failed authorized reprepare must keep the matched-but-unavailable " +
			"pending placeholder in the registry")
	}
	if got := reg.GetByName("late"); got != nil {
		t.Fatalf("registry active entry = %+v, want none (the build failed)", got)
	}
	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none (a build failure is not drift)",
			*drifts)
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "late", phase: "failed",
	}) {
		t.Fatalf("progress = %+v, want a failed phase", *progress)
	}
}

// TestDesiredInvalidFingerprintErrorAppWaitsForRenewedAuthorization pins the
// materialization-fingerprint-error safety contract: a parseable desired-invalid
// app (its materialization-time fingerprint walk failed while the recorded
// template identity matches) must NOT be prepared from potentially locally
// edited /apps bytes — there is no authorized expected fingerprint to compare.
// Each pass requests exactly ONE forced re-materialization per authorized
// generation (suppressed on repeats, reset when the authorization generation
// advances), reports failed with the materialization diagnostic, and retains
// the matched-but-unavailable placeholder so matching deliveries stay pending;
// only after coordinator re-materialization renews the authorization
// (fingerprint installed, invalid set cleared) does the normal retry build.
func TestDesiredInvalidFingerprintErrorAppWaitsForRenewedAuthorization(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gated")

	// Startup installed the matched-but-unavailable placeholder for the
	// materialized-but-not-fingerprintable app.
	unavailable := runner.NewUnavailable(app.App{
		Name: "gated", Dir: dir, Template: mustParse(template),
	})
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent
	r, reg := newTestReconciler(t, root, b,
		[]*runner.PreparedApp{unavailable}, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
		})
	_, tidNow := authorizationFor(t, root, "gated")
	invalidSet := map[string]string{"gated": "fingerprint failed at materialization"}
	r.SetDeploymentAuthorizationWithInvalid(1,
		map[string]string{}, map[string]string{},
		invalidSet, map[string]string{"gated": tidNow}, nil)

	const reverifyReason = "materialized revision could not authorize this app; " +
		"re-verifying the materialized tree: fingerprint failed at materialization"

	// assertFailedNotActivated pins the per-pass invariants: no build, no
	// local activation, the failed diagnostic is the last reported phase, and
	// the unavailable placeholder still serves matching deliveries.
	assertFailedNotActivated := func(pass string) {
		t.Helper()
		if b.prepares() != 0 {
			t.Fatalf("%s: prepare calls = %d, want 0 (an app without an authorized "+
				"expected fingerprint must not build from local bytes)", pass, b.prepares())
		}
		for _, phase := range []string{"active", "activating", "drifted"} {
			if containsEvent(*progress, deploymentEvent{name: "gated", phase: phase}) {
				t.Fatalf("%s: progress = %+v, want no %q phase (the app must never "+
					"activate locally while unauthorized)", pass, *progress, phase)
			}
		}
		last, ok := lastProgressFor(*progress, "gated")
		if !ok || last.phase != "failed" ||
			last.detail != "fingerprint failed at materialization" {
			t.Fatalf("%s: last progress = (%+v, %v), want failed with the "+
				"materialization diagnostic", pass, last, ok)
		}
		if got := reg.GetByName("gated"); got == nil || got.Prepared() != nil {
			t.Fatalf("%s: registry = %+v, want the matched-but-unavailable "+
				"placeholder retained so matching deliveries stay pending", pass, got)
		}
	}

	// Phase 1: the local bytes fingerprint FINE (the danger case), parse, and
	// match the recorded template identity — yet no build may run. The pass
	// asks the coordinator for exactly one re-verification of the materialized
	// tree; it is deliberately not a claim about local bytes.
	r.reconcileApp("gated")

	if len(*drifts) != 1 || (*drifts)[0].name != "gated" ||
		(*drifts)[0].detail != reverifyReason {
		t.Fatalf("re-verification requests = %+v, want exactly one gated request "+
			"with reason %q", *drifts, reverifyReason)
	}
	assertFailedNotActivated("first pass")

	// A repeated pass under the SAME authorization suppresses the request, but
	// the failed diagnostic is unconditional and stays the reported phase.
	r.reconcileApp("gated")

	if len(*drifts) != 1 {
		t.Fatalf("re-verification requests after a repeat pass = %+v, want the "+
			"request suppressed for the same generation", *drifts)
	}
	assertFailedNotActivated("repeat pass")

	// A renewed AUTHORIZATION GENERATION resets the suppression: the same
	// materialization gap now asks for re-verification again, still without
	// activating anything locally.
	r.SetDeploymentAuthorizationWithInvalid(2,
		map[string]string{}, map[string]string{},
		invalidSet, map[string]string{"gated": tidNow}, nil)

	r.reconcileApp("gated")

	if len(*drifts) != 2 || (*drifts)[1].name != "gated" ||
		(*drifts)[1].detail != reverifyReason {
		t.Fatalf("re-verification requests after the generation advanced = %+v, "+
			"want one more gated request with reason %q", *drifts, reverifyReason)
	}
	assertFailedNotActivated("advanced-generation pass")

	// Phase 2: coordinator re-materialization renewed the authorization — the
	// expected fingerprint is installed and the invalid set is cleared. The
	// normal retry is now safe and publishes the build.
	fpNow, tidNow := authorizationFor(t, root, "gated")
	r.SetDeploymentAuthorizationWithInvalid(2,
		map[string]string{"gated": fpNow},
		map[string]string{"gated": tidNow},
		nil, nil, nil)

	r.reconcileApp("gated")

	if b.prepares() != 1 {
		t.Fatalf("prepare calls = %d, want 1 (renewed authorization allows the "+
			"retry)", b.prepares())
	}
	got := reg.GetByName("gated")
	if got == nil || got.Prepared() == nil || got.Prepared().Image != "img-gated" {
		t.Fatalf("registry = %+v, want the retried generation published", got)
	}
}

// TestClusterModeUnchangedAuthorizedAppReportsActiveWithoutDrift pins the
// other side of the identity/fingerprint reconstruction chain (R3): an
// authorized app whose local template and content still match the recovered
// authorization — the state a restart or failed fetch reconstructs — skips
// cleanly: no drift report, no rebuild, and the active phase reported for the
// generation. False drift after restart/fetch failure would surface here.
func TestClusterModeUnchangedAuthorizedAppReportsActiveWithoutDrift(t *testing.T) {
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

	r.reconcileApp("authorized")

	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none for an unchanged authorized app",
			*drifts)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (unchanged content is not rebuilt)",
			b.prepares())
	}
	if !containsEvent(*progress, deploymentEvent{
		name: "authorized", phase: "active",
	}) {
		t.Fatalf("progress = %+v, want the active report on the unchanged skip path",
			*progress)
	}
	if got := reg.GetByName("authorized"); got != fn {
		t.Fatalf("registry = %+v, want the serving entry untouched", got)
	}
}
