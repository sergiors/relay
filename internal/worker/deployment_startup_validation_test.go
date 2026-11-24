package worker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/deployment"
)

const validationTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n" +
	"    pattern:\n      event_name: [INSERT]\n"

// writeValidationApp materializes one valid app under root and returns the
// fingerprint plus raw template identity the deployment would have recorded for
// it.
func writeValidationApp(t *testing.T, root, name, template string) (string, string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte(template), 0o644); err != nil {
		t.Fatalf("write %s template: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write %s handler: %v", name, err)
	}
	tmpl, err := app.ParseTemplate([]byte(template))
	if err != nil {
		t.Fatalf("parse %s template: %v", name, err)
	}
	fingerprint, err := app.FingerprintApp(dir, tmpl)
	if err != nil {
		t.Fatalf("fingerprint %s: %v", name, err)
	}
	source, err := app.ReadTemplateSource(root, name)
	if err != nil {
		t.Fatalf("template source %s: %v", name, err)
	}
	return fingerprint, source.Identity
}

// emptyAuthorization builds an authoritative generation snapshot with no valid
// apps, mirroring a deployment whose only app is materialized-but-invalid.
func emptyAuthorization() deployment.Snapshot {
	return deployment.Snapshot{
		Generation:         1,
		Commit:             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Fingerprints:       map[string]string{},
		TemplateIdentities: map[string]string{},
		Invalid:            map[string]string{},
		InvalidTemplates:   map[string]string{},
		Authoritative:      true,
	}
}

// validateTreeAuthorization is the test-local replacement for the removed
// production recovery wrapper: it loads candidates and issues ONCE from the
// tree and runs the pure startup-authorization proof over that single scan.
// Production validates the consumed scan through deploymentStartupValidation
// inside loadStartupAppsChecked (pinned separately in
// deployment_startup_checked_test.go); no production re-scan helper exists.
func validateTreeAuthorization(
	t *testing.T,
	root string,
	snapshot deployment.Snapshot,
) error {
	t.Helper()
	apps, issues, err := app.NewLoader(root, discardLogger()).LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load %s: %v", root, err)
	}
	return validateStartupAuthorization(
		snapshot, selectAndFingerprintApps(apps, discardLogger()), issues)
}

// TestValidateRecoveredDeploymentAuthorizationRejectsValidInvalidStandIn pins
// the invalid-to-valid half of the startup guard: when a published revision could
// not be materialized, a locally VALID candidate for a name the previous
// generation materialized as invalid cannot prove that generation, even when its
// current template identity happens to equal the recorded one (a
// fingerprint-walk failure records a valid template identity). It fails closed
// instead of consuming possibly corrected target content. Without a failed
// materialization the ordinary startup filtering is preserved and the candidate
// is merely not authorized to activate.
func TestValidateRecoveredDeploymentAuthorizationRejectsValidInvalidStandIn(t *testing.T) {
	root := t.TempDir()
	_, identity := writeValidationApp(t, root, "gated", validationTemplate)

	snapshot := emptyAuthorization()
	snapshot.Invalid["gated"] = "fingerprint failed at materialization"
	snapshot.InvalidTemplates["gated"] = identity

	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("a valid candidate standing in for a materialized-invalid app = nil, " +
			"want a fail-closed error after a failed materialization")
	}

	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("non-partial startup = %v, want ordinary invalid drift filtering "+
			"preserved", err)
	}
}

// TestValidateRecoveredDeploymentAuthorizationRequiresIdentityOnRecovery pins
// the identity half of the authorization proof: an authorized app whose
// recorded template identity is empty cannot be proven from its content
// fingerprint alone (runtime-only template fields are excluded from it), so
// startup fails closed — at ordinary startup AND while recovering a failed
// materialization — and a matching recorded identity satisfies the proof in
// both modes.
func TestValidateRecoveredDeploymentAuthorizationRequiresIdentityOnRecovery(t *testing.T) {
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "authorized", validationTemplate)

	snapshot := emptyAuthorization()
	snapshot.Fingerprints["authorized"] = fingerprint

	// Ordinary startup with an unrecorded identity now fails closed too: the
	// exact carried identity is required for every authorized valid candidate.
	err := validateTreeAuthorization(t, root, snapshot)
	if err == nil {
		t.Fatal("ordinary startup with no recorded template identity = nil, " +
			"want a fail-closed error")
	}
	if want := "has no recorded template identity"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}

	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("failed materialization with no recorded template identity = nil, " +
			"want a fail-closed error")
	}

	// A matching recorded identity satisfies the proof in both modes.
	snapshot.TemplateIdentities = map[string]string{"authorized": identity}
	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("ordinary startup with a matching identity = %v, want nil", err)
	}
	partial = snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err != nil {
		t.Fatalf("failed materialization with a matching identity = %v, want nil", err)
	}
}

// TestValidateRecoveredDeploymentAuthorizationRequiresInvalidIdentityOnRecovery
// pins the desired-invalid half of the partial-target proof: when the previous
// generation recorded a deterministic identity for an app it materialized as
// invalid, a local invalid candidate whose current identity cannot be computed
// (here a symlinked template) is NOT proof that the bytes still match. Recovery
// from a failed materialization fails closed; ordinary startup keeps its existing
// filtering, and a matching deterministic identity satisfies the proof.
func TestValidateRecoveredDeploymentAuthorizationRequiresInvalidIdentityOnRecovery(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "gated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir gated: %v", err)
	}
	target := filepath.Join(t.TempDir(), "template.yaml")
	if err := os.WriteFile(target, []byte(validationTemplate), 0o644); err != nil {
		t.Fatalf("write template target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "template.yaml")); err != nil {
		t.Fatalf("symlink template: %v", err)
	}

	snapshot := emptyAuthorization()
	snapshot.Invalid["gated"] = "template invalid"
	snapshot.InvalidTemplates["gated"] = "recorded-invalid-identity"

	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("ordinary startup with an unhashable desired-invalid app = %v, want "+
			"the existing filtering behavior", err)
	}

	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("failed materialization with an unprovable desired-invalid identity " +
			"= nil, want a fail-closed error")
	}

	// Control: a deterministic identity that still matches the local invalid bytes
	// satisfies the recovery proof for the same app.
	matching := t.TempDir()
	const invalidTemplate = "runtime: python9.9\n"
	writeBrokenWorkerApp(t, matching, "gated", invalidTemplate)
	source, err := app.ReadTemplateSource(matching, "gated")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	proven := emptyAuthorization()
	proven.Invalid["gated"] = "template invalid"
	proven.InvalidTemplates["gated"] = source.Identity
	proven.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, matching, proven); err != nil {
		t.Fatalf("failed materialization with a matching invalid identity = %v, want nil", err)
	}
}

// TestValidateRecoveredDeploymentAuthorizationAuthorizedContent pins the base
// startup check in both modes: every persisted valid fingerprint must have a
// matching local candidate CARRIED WITH its recorded template identity — the
// identity is required in every case, not only while recovering a failed
// materialization — while extra local candidates only fail when a failed
// materialization may have left a partially materialized target.
func TestValidateRecoveredDeploymentAuthorizationAuthorizedContent(t *testing.T) {
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "authorized", validationTemplate)

	snapshot := emptyAuthorization()
	snapshot.Fingerprints["authorized"] = fingerprint
	snapshot.TemplateIdentities["authorized"] = identity

	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("matching authorized app = %v, want nil", err)
	}

	// A locally edited authorized app can no longer be served from the local
	// tree. The comment flows into the stripped-template digest byte-for-byte
	// (the runtime-only strip preserves comments), so this edit CHANGES THE
	// CONTENT FINGERPRINT and fails on the fingerprint gate — it is deliberately
	// not the identity-only drift covered by
	// TestValidateRecoveredDeploymentAuthorizationRuntimeOnlyIdentityDrift.
	editedFingerprint, _ := writeValidationApp(t, root, "authorized",
		validationTemplate+"# local edit\n")
	if editedFingerprint == fingerprint {
		t.Fatal("precondition: the template comment edit must change the content " +
			"fingerprint — otherwise this case would pin identity-only drift " +
			"instead of a fingerprint change")
	}
	if err := validateTreeAuthorization(t, root, snapshot); err == nil {
		t.Fatal("fingerprint-changed authorized app = nil, want a fail-closed error")
	}

	// Restore the authorized content and add a local-only app: ordinary startup
	// filters the extra app without failing, while a failed materialization treats
	// the extra candidate as evidence of a partially materialized target.
	writeValidationApp(t, root, "authorized", validationTemplate)
	writeValidationApp(t, root, "local-only", validationTemplate)
	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("local-only app at ordinary startup = %v, want it filtered without "+
			"failing startup", err)
	}

	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("local-only app after a failed materialization = nil, want a " +
			"fail-closed error")
	}

	// A missing authorized app fails closed even without a failed materialization.
	restored := t.TempDir()
	missing := emptyAuthorization()
	missing.Fingerprints["authorized"] = fingerprint
	missing.TemplateIdentities["authorized"] = identity
	if err := validateTreeAuthorization(t, restored, missing); err == nil {
		t.Fatal("missing authorized app = nil, want a fail-closed error")
	}
}

// TestValidateRecoveredDeploymentAuthorizationRuntimeOnlyIdentityDrift pins
// the always-on raw-template-identity boundary: a runtime-only template edit
// (here top-level resources) changes the RAW template identity but not the
// content fingerprint, because runtime-only fields are stripped from the
// digest. Startup now FAILS CLOSED for such an authorized app in every mode —
// ordinary startup included, because the edit can arrive after successful
// materialization but before this scan, and activating it would serve
// unauthorized config before the asynchronous reconciler catches the drift —
// while the same drift was previously left to the reconciler's restore. An
// unedited tree still validates in both modes, so the rejection is
// attributable to the carried identity alone. Missing or fingerprint-changed
// authorized apps also fail ordinary startup (pinned by
// TestValidateRecoveredDeploymentAuthorizationAuthorizedContent and
// TestValidateRecoveredDeploymentAuthorizationRequiresIdentityOnRecovery).
func TestValidateRecoveredDeploymentAuthorizationRuntimeOnlyIdentityDrift(t *testing.T) {
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "authorized",
		validationTemplate)

	snapshot := emptyAuthorization()
	snapshot.Fingerprints["authorized"] = fingerprint
	snapshot.TemplateIdentities["authorized"] = identity
	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")

	// Positive control: the unedited tree validates in both modes.
	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("unedited tree at ordinary startup = %v, want nil", err)
	}
	if err := validateTreeAuthorization(t, root, partial); err != nil {
		t.Fatalf("unedited tree during recovery = %v, want nil", err)
	}

	// A runtime-only template edit changes the raw identity but not the
	// content fingerprint (runtime-only fields are stripped from the digest).
	editedFingerprint, editedIdentity := writeValidationApp(t, root, "authorized",
		validationTemplate+"resources:\n  memory: 1GiB\n")
	if editedFingerprint != fingerprint {
		t.Fatalf("precondition: runtime-only template edit changed the content "+
			"fingerprint %s -> %s", fingerprint, editedFingerprint)
	}
	if editedIdentity == identity {
		t.Fatalf("precondition: runtime-only template edit must change the raw "+
			"template identity (both %q)", identity)
	}

	// Ordinary startup with identity-only drift now fails closed...
	err := validateTreeAuthorization(t, root, snapshot)
	if err == nil {
		t.Fatal("ordinary startup with runtime-only template identity drift = nil, " +
			"want a fail-closed error (the edit can land after materialization)")
	}
	if want := "template bytes no longer match the materialized revision"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}

	// ...and so does the same drift during a failed-materialization recovery.
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("runtime-only template identity drift after a failed " +
			"materialization = nil, want a fail-closed error")
	}
}

// TestValidateRecoveredDeploymentAuthorizationRejectsChangedDesiredInvalidIdentity
// pins the desired-invalid half the unprovable-identity case cannot reach: a
// materialized-invalid app whose local template bytes CHANGED after the failed
// materialization carries a different deterministic identity, so the tree can
// no longer be proven to be the materialized revision and startup fails closed
// — while ordinary startup keeps filtering local invalid drift without failing.
// The matching-identity assertion is the positive control: the same tree and
// snapshot validate until the bytes change.
func TestValidateRecoveredDeploymentAuthorizationRejectsChangedDesiredInvalidIdentity(
	t *testing.T,
) {
	root := t.TempDir()
	const broken = "runtime: python9.9\n"
	writeBrokenWorkerApp(t, root, "broken", broken)
	source, err := app.ReadTemplateSource(root, "broken")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}

	snapshot := emptyAuthorization()
	snapshot.Invalid["broken"] = "template invalid"
	snapshot.InvalidTemplates["broken"] = source.Identity

	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err != nil {
		t.Fatalf("desired-invalid app with matching identity after a failed "+
			"materialization = %v, want startup to proceed", err)
	}

	writeBrokenWorkerApp(t, root, "broken", broken+"# edited invalid\n")
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("changed desired-invalid template bytes after a failed " +
			"materialization = nil, want a fail-closed error")
	}
	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("ordinary startup with locally changed invalid bytes = %v, "+
			"want the invalid-drift filtering preserved", err)
	}
}

// TestValidateRecoveredDeploymentAuthorizationRequiresPriorAuthority pins the
// no-prior-authority split of the startup gate: with NEITHER an authoritative
// generation NOR a failed materialization (nothing was ever published — the
// normal standalone or no-desired-revision start) there is nothing to verify
// and startup proceeds, while a published revision that could not be
// materialized WITHOUT any previous authorized generation fails closed — there
// is no authorization the local tree could prove, so consuming could only ACK
// deliveries unmatched.
func TestValidateRecoveredDeploymentAuthorizationRequiresPriorAuthority(t *testing.T) {
	root := t.TempDir()

	idle := deployment.Snapshot{Generation: 0}
	if err := validateTreeAuthorization(t, root, idle); err != nil {
		t.Fatalf("nothing ever published = %v, want startup to proceed", err)
	}

	orphan := idle
	orphan.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, orphan); err == nil {
		t.Fatal("failed materialization without a prior authoritative " +
			"generation = nil, want a fail-closed error")
	}
}

// TestValidateRecoveredDeploymentAuthorizationLegitimatelyEmptyTree pins the
// legitimately empty deployment at the gate: an authoritative generation with
// no apps over an empty local tree proves itself (even while recovering a
// failed materialization — no authorized content exists that the tree could
// fail to serve), while a candidate the failed materialization may have left
// behind fails closed against that same empty authorization.
func TestValidateRecoveredDeploymentAuthorizationLegitimatelyEmptyTree(t *testing.T) {
	root := t.TempDir()
	empty := emptyAuthorization()

	partial := empty
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err != nil {
		t.Fatalf("authoritatively empty tree = %v, want startup to proceed", err)
	}

	writeValidationApp(t, root, "half-synced", validationTemplate)
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("extra candidate against an empty authorization after a failed " +
			"materialization = nil, want a fail-closed error")
	}
}

// TestValidateRecoveredDeploymentAuthorizationRemovedNameReappearing pins the
// removed-name case: a name the authorized generation REMOVED that exists
// locally again is not part of that generation — ordinary startup filters it
// without failing, while after a failed materialization (which may have
// restored the name from the target revision) it fails closed.
func TestValidateRecoveredDeploymentAuthorizationRemovedNameReappearing(t *testing.T) {
	root := t.TempDir()
	writeValidationApp(t, root, "removed-app", validationTemplate)

	snapshot := emptyAuthorization()
	snapshot.Removed = []string{"removed-app"}

	if err := validateTreeAuthorization(t, root, snapshot); err != nil {
		t.Fatalf("reappeared removed app at ordinary startup = %v, want it "+
			"filtered without failing startup", err)
	}

	partial := snapshot
	partial.Error = errors.New("deployment: materialize commit: fetch failed")
	if err := validateTreeAuthorization(t, root, partial); err == nil {
		t.Fatal("reappeared removed app after a failed materialization = nil, " +
			"want a fail-closed error")
	}
}
