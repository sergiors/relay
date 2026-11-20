package reconciler

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/state"
)

// reconcileAll discovers current app candidates and reconciles each, so periodic
// checks also handle removal (a name present in the registry/state tokens but no
// longer an app candidate on disk) and retry previously failed builds. A direct
// child directory is a candidate only when template.yaml is present; a
// template-less directory is not an app and is handled by the missing-name sweep
// when it was previously known. Each candidate is dispatched through the same
// single pump so reconciles stay serialized per app.
//
// In a coordinated deployment this pass also verifies each app's template
// identity and content fingerprint against the materialized authorization in
// reconcileApp, and it sweeps every deployment-authorized name that is not a
// candidate on disk (an app that was filtered out at startup, deleted, or
// replaced by an unsupported path), which no registry/pending/fencing-token
// sweep would find.
func (r *Reconciler) reconcileAll() {
	// Refresh the complete runtime base-image selection snapshot ONCE per pass,
	// before dispatching any app. Each reconcile then compares its app's recorded
	// built identity with the refreshed snapshot, so only runtime-backed apps whose
	// selection actually changed reprepare while every other app keeps its
	// unchanged skip path. A read/validation failure leaves the Manager's
	// last-known-good snapshot in place and is retried on the next pass.
	r.refreshRuntimeSelections()
	seen := map[string]bool{}
	entries, err := os.ReadDir(r.root)
	if err != nil {
		r.log.Warn(
			"Reconciler: read root",
			"root", r.root,
			"error", err,
		)
		return
	}
	for _, e := range entries {
		name := e.Name()
		if app.IsReservedDir(name) {
			// A Relay-owned transient staging directory (git's ".sync-*") is
			// never an app and never a desired definition: it is neither
			// dispatched for reconcile nor recorded as seen, so it cannot
			// produce a warning or an invalid state row while it briefly exists.
			continue
		}
		path := filepath.Join(r.root, name)
		realDir, dirErr := app.IsRealAppDir(path)
		if dirErr != nil {
			// The entry was enumerated, but its current type could not be
			// determined. Keep it out of the missing-name sweep and let the
			// single-app path record the operational failure without removing an
			// active generation.
			seen[name] = true
			r.dispatch(name)
			continue
		}
		if !realDir {
			// Files, symlinked directories, and broken links are unsupported, not
			// app candidates. Known names are reconciled through normal removal;
			// unknown root entries do not create app state.
			if r.knownLocally(name) {
				seen[name] = true
				r.dispatch(name)
			}
			continue
		}
		present, statErr := app.HasTemplate(path)
		if statErr != nil {
			// An ambiguous template stat is not proof of removal. Keep the name
			// seen and let the normal load classify it (a flaky read is recorded
			// as an invalid desired state and retains the active generation)
			// instead of dispatching it through the removal sweep.
			seen[name] = true
			r.dispatch(name)
			continue
		}
		if !present {
			// No template.yaml: not an app candidate. A previously-known name is
			// dispatched by missingNames as a removal.
			continue
		}
		seen[name] = true
		r.dispatch(name)
	}
	// Apps still registered, pending, carrying a fencing token, or authorized by
	// the materialized deployment generation but no longer candidates on disk must
	// still be dispatched. A brand-new app whose first preparation has not yet
	// completed exists ONLY as a pending desired entry (nothing is published as
	// active until the build succeeds), so PendingNames must be swept too:
	// otherwise a directory that vanishes during preparation would leave a stale
	// pending entry gating events forever. Fencing tokens cover a name that only
	// ever had a startup invalid-desired row and therefore never entered the
	// registry, and deployment authorization covers an authorized app that startup
	// filtered out because its local bytes already drifted; both are invisible to
	// the local indexes, so without the sweep their local deletion or unsupported
	// replacement would never be observed (and never restored).
	for _, name := range r.missingNames(seen) {
		r.dispatch(name)
	}
}

// refreshRuntimeSelections refreshes the runtime Manager's complete validated
// selection snapshot. An error is logged and leaves the last-known-good snapshot
// untouched; the next periodic pass retries. It reports whether the refresh
// itself succeeded (even when no runtime changed), so a stale-publication retry
// can distinguish a converged snapshot from a database/validation failure it
// must leave to the periodic backstop.
//
// It intentionally does NOT invalidate recorded build identities here: every
// reconcile compares each app's published selection identity against the current
// snapshot (see currentImageSelection), so only apps whose runtime actually
// changed reprepare, and a change that was already consumed by a snapshot refresh
// (notably during startup publication) is still detected.
func (r *Reconciler) refreshRuntimeSelections() bool {
	publisher, ok := r.builder.(selectionPublisher)
	if !ok {
		return false
	}
	changed, err := publisher.RefreshRuntimeImageSelections(r.rctx())
	if err != nil {
		r.log.Warn(
			"Runtime: image selection refresh failed; retaining last known good",
			"error", err,
		)
		return false
	}
	if len(changed) > 0 {
		r.log.Info(
			"Runtime: image selection changed; affected apps will reprepare",
			"runtimes", changed,
		)
	}
	return true
}

// missingNames returns the names that are NOT app candidates on disk but still
// need a reconcile pass, so reconcileAll dispatches them. It covers the
// locally-known names (active, pending-only, or carrying a startup fencing
// token) and, in a coordinated deployment, every name the materialized
// generation authorizes (valid or desired-invalid). Fencing tokens are included
// so a name that only ever had a startup invalid-desired row — and therefore
// never entered the registry — is still removed once its definition disappears.
// Deployment authorization is included because a coordinated app can be absent
// from every local index: startup filters out a locally drifted authorized app,
// so it is never registered and never receives a fencing token, yet the
// periodic pass must still observe that it is missing (or became an unsupported
// path) and report drift. An app that is BOTH active and pending (an available
// generation being rebuilt) is returned once.
func (r *Reconciler) missingNames(seen map[string]bool) []string {
	var out []string
	added := map[string]bool{}
	add := func(name string) {
		if seen[name] || added[name] {
			return
		}
		added[name] = true
		out = append(out, name)
	}
	for _, name := range r.reg.Names() {
		add(name)
	}
	for _, name := range r.reg.PendingNames() {
		add(name)
	}
	r.mu.Lock()
	fencingTokenNames := make([]string, 0, len(r.fencingTokens))
	for name := range r.fencingTokens {
		fencingTokenNames = append(fencingTokenNames, name)
	}
	// deploymentRemoved is deliberately excluded: those apps are absent from the
	// authorized generation, so absence is the expected state and a present
	// directory is already dispatched as a normal candidate.
	deploymentNames := make([]string, 0, len(r.deploymentFingerprints)+len(r.deploymentInvalid))
	if r.deploymentEnabled {
		for name := range r.deploymentFingerprints {
			deploymentNames = append(deploymentNames, name)
		}
		for name := range r.deploymentInvalid {
			deploymentNames = append(deploymentNames, name)
		}
	}
	r.mu.Unlock()
	for _, name := range fencingTokenNames {
		add(name)
	}
	for _, name := range deploymentNames {
		add(name)
	}
	return out
}

// reconcileApp is the core decision point for one app. It is serialized
// per name by the pump; when called directly (Reconcile/reconcileAll) callers
// coordinate it.
func (r *Reconciler) reconcileApp(name string) {
	// A Relay-owned transient staging directory (git's ".sync-*") is never a
	// app and never a desired definition. It is filtered before dispatch
	// (appForPath, reconcileAll), but guard here too so a direct/stale
	// caller can never turn a stage directory into a warning, an invalid state
	// row, or a removal of a healthy app. It is deliberately not treated as
	// ErrInvalidPath (which would record an invalid desired definition).
	if app.IsReservedDir(name) {
		return
	}
	// Read the whole authorization and its epoch under one lock. The epoch stamped
	// on this pass's content verification must be the authorization the pass
	// actually compared against, so a concurrent authorization update cannot
	// relabel content verified against the previous one as current.
	authorization := r.deploymentAuthorization(name)
	expectedFingerprint := authorization.fingerprint
	deploymentAuthorized := authorization.authorized
	deploymentRemovalOnly := authorization.removalOnly
	deploymentInvalidTemplate := authorization.invalidTemplate
	deploymentInvalidMessage := authorization.invalidMessage
	deploymentInvalid := authorization.invalid
	deploymentCluster := authorization.cluster
	if deploymentCluster && !deploymentAuthorized && !deploymentRemovalOnly && !deploymentInvalid {
		// A local-only app is not part of the materialized revision. Its bytes
		// must never become a desired generation; the deployment coordinator
		// removes it when it restores the authorized tree.
		r.reportDeploymentDrift(name, "not part of the active deployment")
		return
	}
	// Observe the persisted row BEFORE reading template.yaml. This snapshot is
	// the optimistic guard an invalid observation carries: if another worker
	// advances the app (for example with a valid correction) while this pass is
	// still running, the conditional invalid write is rejected instead of
	// overwriting the newer state. The fast path itself re-reads persisted state
	// before skipping, so it never trusts this snapshot alone.
	observed := r.observeApp(name)
	// Only inspect names that lexically identify one direct child. An unsafe
	// name must never let a removal check touch a sibling or ancestor; the loader
	// below will classify it as ErrInvalidPath instead.
	checkDir := isSinglePathElement(name)
	var templateSource app.TemplateSource
	var err error
	if checkDir {
		realDir, inspectErr := app.IsRealAppDir(filepath.Join(r.root, name))
		if inspectErr != nil {
			err = fmt.Errorf("app %q: inspect directory: %w", name, inspectErr)
		} else if !realDir {
			// A missing path, regular file, or direct-child symlink is not an
			// app. These are normal removals for known names, unlike an invalid
			// template inside a real app directory, which preserves the active
			// generation. In coordinated deployments only a name the deployment explicitly
			// removed may be removed; an authorized app disappearing locally is
			// drift and is restored, never treated as a desired removal.
			if deploymentCluster && !deploymentRemovalOnly {
				r.reportDeploymentDrift(name, "materialized app is missing locally")
				return
			}
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
	}
	if deploymentRemovalOnly {
		// The app is absent from the authorized generation but is present on
		// disk: this is a local recreation, not an authorized removal.
		r.reportDeploymentDrift(name, "app is not part of the active deployment")
		return
	}
	// ReadTemplateSource applies the same no-follow policy as startup discovery
	// and keeps parsing separate so an unchanged deterministic invalid input can
	// avoid a repeat parse. For a real app directory, a missing template is a
	// normal removal; a symlinked/non-regular template is invalid desired input.
	if err == nil {
		templateSource, err = app.ReadTemplateSource(r.root, name)
	}
	if err != nil {
		// Directory paths can change type after the initial no-follow check.
		// Reclassify invalid-path failures so missing/unsupported directories are
		// removed, while invalid templates in real directories are retained.
		if checkDir && errors.Is(err, app.ErrInvalidPath) {
			realDir, inspectErr := app.IsRealAppDir(filepath.Join(r.root, name))
			if inspectErr != nil {
				err = fmt.Errorf("app %q: inspect directory: %w", name, inspectErr)
			} else if !realDir {
				if deploymentCluster && !deploymentRemovalOnly {
					r.reportDeploymentDrift(name, "materialized app changed to an unsupported path")
					return
				}
				if r.knownLocally(name) {
					r.remove(name)
				}
				return
			}
		}
		if deploymentCluster && deploymentAuthorized {
			// An authorized app that disappeared, lost its template, or became
			// unreadable locally is drift, not a desired removal or invalid
			// definition. Never let a local deletion or edit change the active
			// generation.
			r.reportDeploymentDrift(name, "materialized app can no longer be loaded locally")
			return
		}
		if errors.Is(err, fs.ErrNotExist) {
			// Directory gone -> removal only when the deployment authorized it.
			// Otherwise this is local drift and the coordinator restores the tree.
			if deploymentCluster && !deploymentRemovalOnly {
				r.reportDeploymentDrift(name, "materialized app is missing locally")
				return
			}
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
		if errors.Is(err, app.ErrNotReady) {
			// The directory has no template.yaml, so it is not an app candidate.
			// On a coordinator-active worker only an authorized removal may
			// disappear; a desired invalid/active app losing its template locally is
			// drift, never a removal.
			if deploymentCluster && !deploymentRemovalOnly {
				r.reportDeploymentDrift(name, "materialized app lost its template locally")
				return
			}
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
		// Every other load failure (invalid template path, flaky read, unreadable
		// template) means the desired definition is not a valid generation to
		// prepare, so no desired pending rules are manufactured. Any stale
		// pending entry from a previous, now-invalid desired state is cleared so
		// removed rules cannot keep gating events; the previously-active
		// generation (if any) is retained exactly as today.
		r.reg.ClearPending(name)
		if errors.Is(err, app.ErrInvalidPath) {
			// An invalid name or template path inside a real app directory is
			// invalid desired input, not removal. Retain any previously-loaded
			// version and record the invalid view so its stale ready state is not
			// left behind.
			r.log.Warn(
				"App: invalid path; retaining previous version",
				"app", name,
				"error", err,
			)
			// No bytes were read, so there is no deterministic invalid identity:
			// this failure stays retryable and clears any earlier identity.
			if deploymentCluster && deploymentInvalid {
				r.reportDeployment(name, state.DeploymentAppFailed, err.Error())
			}
			r.recordInvalidDesired(name, "", err, observed)
			return
		}
		// A flaky read/stat (permissions, I/O) is not a removal: don't drop the
		// app, but surface it so staleness isn't silently ignored. The entry
		// is present, so record the failed desired view (the active generation is
		// preserved and the registry untouched). It must not claim an invalid
		// identity from an earlier, different read.
		r.log.Warn(
			"App: load error; retaining previous version",
			"app", name,
			"error", err,
		)
		if deploymentCluster && deploymentInvalid {
			r.reportDeployment(name, state.DeploymentAppFailed, err.Error())
		}
		r.recordInvalidDesired(name, "", err, observed)
		return
	}
	templateSrc := templateSource

	// A template edit is drift, not a new desired generation. Checking the exact
	// template identity here — before pending rules are published — preserves the
	// historical pending-before-fingerprint event window for genuine deployments
	// while refusing locally introduced template content. A DESIRED invalid app is
	// not drift when its bytes still match the materialized invalid identity; the
	// normal invalid-desired path below retains the diagnostic and previous active
	// generation. Without an invalid identity, a now-successful parse cannot be
	// distinguished from a local correction and is treated as drift.
	expectedTemplate := ""
	templateKnown := false
	if deploymentCluster {
		if deploymentAuthorized {
			expectedTemplate = authorization.templateIdentity
			templateKnown = authorization.templateIdentityKnown
		}
		if deploymentInvalid {
			expectedTemplate = deploymentInvalidTemplate
			templateKnown = expectedTemplate != ""
		}
	}
	if deploymentCluster && templateKnown && templateSrc.Identity != expectedTemplate {
		r.reportDeploymentDrift(name, "materialized template changed locally")
		return
	}

	// The bytes were read and hashed. If their exact deterministic-invalid
	// identity is CURRENTLY persisted, skip the parse entirely. This is a fresh
	// persisted-state read (not the pre-read snapshot and never a process-local
	// negative cache): the row is the authority, so an unchanged invalid input is
	// recognized across cycles and restarts, while another worker's valid
	// correction is never overridden by a stale local view.
	if templateSrc.Identity != "" && observed != nil && observed.Found &&
		observed.Detail.InvalidSource == templateSrc.Identity {
		if current := r.observeApp(name); current != nil &&
			current.Found && current.Detail.InvalidSource == templateSrc.Identity {
			r.reg.ClearPending(name)
			r.skipUnchangedInvalidDesired(name, current)
			return
		}
	}

	fn, err := templateSrc.Parse()
	if err != nil {
		if deploymentCluster && deploymentAuthorized {
			// A local template edit made the materialized app unparseable. It is
			// drift: keep serving the authorized active generation and restore the
			// tree instead of recording a locally introduced invalid desired state.
			r.reportDeploymentDrift(name, "materialized template changed locally")
			return
		}
		// Every other load failure (invalid path, flaky read, unreadable/invalid
		// template) means the desired definition is not a valid generation to
		// prepare, so no desired pending rules are manufactured. Any stale
		// pending entry from a previous, now-invalid desired state is cleared so
		// removed rules cannot keep gating events; the previously-active
		// generation (if any) is retained exactly as today.
		r.reg.ClearPending(name)
		// A flaky read/stat (permissions, I/O) is not a removal: don't drop the
		// app, but surface it so staleness isn't silently ignored. The entry
		// is present, so record the failed desired view (the active generation is
		// preserved and the registry untouched).
		r.log.Warn(
			"App: load error; retaining previous version",
			"app", name,
			"error", err,
		)
		identity, _ := app.InvalidTemplateIdentity(err)
		if deploymentCluster && deploymentInvalid {
			r.reportDeployment(name, state.DeploymentAppFailed, err.Error())
		}
		r.recordInvalidDesired(name, identity, err, observed)
		return
	}
	if deploymentCluster && deploymentInvalid && !templateKnown {
		// The materialized invalid app could not be verified byte-for-byte, and it
		// now parses successfully: treat that as local drift rather than activating
		// locally introduced content.
		r.reportDeploymentDrift(name, "desired invalid app changed locally")
		return
	}
	dir := fn.Dir

	// Publish the desired generation's event rules into the registry as PENDING
	// at the earliest safe point after the template parsed, BEFORE the
	// (potentially long) source fingerprint walk and image build. Matching is
	// registry-only, so without this a delivery arriving during first
	// preparation of a brand-new app (or during the rebuild of an existing one)
	// would match nothing and be ACKed as unmatched — the event-loss window this
	// closes. The pending representation is non-runnable (NewUnavailable): it
	// contributes matched-but-unavailable invocations so the message stays
	// pending, never executes, claims a TryStart, exhausts, ACKs, or DLQs.
	//
	// The representation depends on the CURRENT active entry:
	//   - available active generation (v1 being rebuilt to v2): keep v1 active
	//     and executable, and add v2's rules as pending, so v1-only rules run and
	//     v2-only rules hold.
	//   - unavailable active placeholder (a startup/previous build failure, no
	//     image): replace it in place with the current desired template so its
	//     unavailable rules are coherent (a removed old rule stops gating) and
	//     clear any pending entry. There is no executable generation to preserve.
	//   - absent (brand-new app): add the desired rules as pending only; nothing
	//     is published as active until preparation succeeds.
	//
	// A template with no event rules (a service-only/no-runtime app) has nothing
	// to hold, so any stale pending entry is cleared instead: service behavior is
	// never disturbed and no empty pending entry lingers.
	cur := r.reg.GetByName(name)
	switch {
	case cur != nil && !isAvailable(cur):
		r.reg.Replace(name, runner.NewUnavailable(fn))
	case len(fn.Template.Events) == 0:
		r.reg.ClearPending(name)
	default:
		r.reg.SetPending(name, fn)
	}

	// Compute the fingerprint AND (for a runtime-backed app) the resolved
	// source selection in ONE traversal, then carry BOTH into the rebuild below.
	// The selection is handed to the Manager's selection-aware Prepare so the
	// build stages exactly the source this hash was derived from, with no second
	// policy read that a concurrent .gitignore edit could diverge. For a
	// no-runtime template the selection is nil (no image is built) and the
	// template-only fingerprint is passed through untouched. The resolver is the
	// injected seam (production: app.SelectAndFingerprintApp), invoked
	// exactly once per reconcile.
	selection, fp, err := r.fingerprint(dir, fn.Template)
	if err != nil {
		if deploymentCluster && deploymentAuthorized {
			r.revertDeploymentPending(name, cur)
			r.reportDeploymentDrift(name, "materialized source can no longer be fingerprinted")
			return
		}
		r.log.Warn(
			"App: fingerprint error; retaining previous version",
			"app", name,
			"error", err,
		)
		// The desired definition is present and parseable but its source could
		// not be fingerprinted, so the desired generation cannot be trusted.
		// Record the failed view while retaining the active generation and
		// leaving the runtime registry untouched. The pending desired rules
		// published above are deliberately RETAINED: the configuration is valid
		// and preparation may succeed on a later pass, so holding matching
		// events pending (rather than ACKing them as unmatched) is the
		// at-least-once direction. No invalid source identity is claimed: a
		// fingerprint/source/dependency failure is operational and must stay
		// retryable (and clear any earlier deterministic invalid identity).
		r.recordInvalidDesired(name, "", err, observed)
		return
	}
	// Record the successful content verification for the deployment coordinator's
	// independent drift backstop, tagged with the authorization epoch this pass
	// actually compared against (captured atomically with the authorization). A
	// concurrent authorization update makes the mark conservative: either it is
	// filtered out by VerifiedSince (stale epoch), or the next pass records under
	// the new epoch. This happens before the authorization comparison: a mismatch
	// reports drift now, and the coordinator must not re-hash the same bytes to
	// reach the same conclusion.
	r.mu.Lock()
	if r.deploymentEpoch == authorization.epoch {
		r.verified[name] = verifiedContent{at: time.Now(), epoch: authorization.epoch}
	}
	r.mu.Unlock()

	if deploymentCluster && deploymentAuthorized && expectedFingerprint != "" &&
		fp != expectedFingerprint {
		r.revertDeploymentPending(name, cur)
		r.reportDeploymentDrift(name, "materialized content changed locally")
		return
	}

	if deploymentCluster && deploymentInvalid {
		// The materialized revision could not fingerprint this app. A later live
		// fingerprint success must NOT authorize a build from the current /apps
		// bytes: a local edit made during the failure could otherwise become active.
		// Leave the desired rules pending/unavailable and keep any previous active
		// generation serving. Request a forced re-materialization so authorize
		// re-runs against the exact shared SHA with this app's expected
		// fingerprint; a transient materialization failure (for example the attempt
		// context expiring after this app was skipped) then recovers instead of
		// staying invalid until a newer generation is published. The reason is
		// deliberately not a claim about local bytes.
		reason := "materialized revision could not authorize this app; re-verifying the materialized tree"
		if deploymentInvalidMessage != "" {
			reason += ": " + deploymentInvalidMessage
		}
		// Request first: the coordinator's request records a transient drifted row,
		// and the Failed diagnostic below must remain the row a restart
		// reconstructs for this desired-invalid app. Unlike the request, that
		// diagnostic is written on every pass, exactly as before.
		r.requestDeploymentReverification(name, reason)
		message := deploymentInvalidMessage
		if message == "" {
			message = "desired app is invalid in the materialized revision"
		}
		r.reportDeployment(name, state.DeploymentAppFailed, message)
		return
	}

	currentSelection := r.currentImageSelection(fn.Template)
	r.mu.Lock()
	known, hasFingerprint := r.fingerprints[name]
	knownSelection, hasSelection := r.imageSelections[name]
	r.mu.Unlock()
	// A tracked built identity must equal the Manager's current validated snapshot.
	// An untracked entry preserves the legacy source-fingerprint-only behavior for
	// fake/non-runtime-backed builders that never seed or publish a selection.
	selectionUnchanged := !hasSelection || knownSelection == currentSelection

	// Skip a rebuild only when the current build is healthy, content is unchanged,
	// AND the runtime base-image selection it was built from is still current. A
	// previously-failed build (unavailable) is retried even if the fingerprint is
	// stable, so a broken app recovers without edits. A changed selection always
	// reprepares, even when the source fingerprint is identical.
	if cur != nil && isAvailable(cur) && hasFingerprint && known == fp && selectionUnchanged {
		// No image transition is needed, so the pending desired rules published
		// above (which, for a byte-identical template, are the same rules as the
		// active generation) are cleared: no stale pending state may linger.
		r.reg.ClearPending(name)
		// Skipped checks are deliberately NOT persisted as the last reconcile:
		// RecordReconcileSuccess/Failure record the last MEANINGFUL operation and
		// its timestamp; a periodic no-op must not hide a recent success or
		// failure. It is surfaced in debug logging only.
		r.log.Debug(
			"App: unchanged; reconcile skipped",
			"app", name,
		)
		// An authorized unchanged app is active for this deployment generation.
		r.reportDeployment(name, state.DeploymentAppActive, "")
		// Resource limits do not participate in the image fingerprint, so a
		// resource-only template change lands on this skip path. Publish the
		// current effective limits to the live warm pool so containers rotate to
		// them (idle discarded, busy drained) without a rebuild. The runtime
		// no-ops an unchanged value, so a truly-unchanged periodic tick does not
		// churn. Non-Manager builders ignore it.
		if rs, ok := r.builder.(resourceSetter); ok {
			rs.SetAppResources(name, fn.Template.ResourceLimits())
		}
		// A skip path is NOT a full no-op when the app declares services:
		// without converging here, a crashed service replica would only be
		// repaired on the next content change. Reconcile is idempotent — when
		// the desired set is already running it lists containers once and
		// touches nothing — so calling updateServices on every periodic tick is
		// cheap and gives crash replacement within the default cadence without
		// a separate services-only loop. updateServices is only reachable when
		// the current build is available (this skip branch already guarantees
		// cur.Prepared() != nil via isAvailable). When that converge pass is a
		// no-op (nothing to stop or start), the caller logs it at Debug rather
		// than Info — the summary line only surfaces real state changes.
		if (r.updateServices != nil || r.updateServicesWithStatus != nil || r.updateServicesObservationWithStatus != nil) && len(fn.Template.Services) > 0 {
			if r.updateServicesWithStatus == nil && r.updateServicesObservationWithStatus == nil {
				r.updateServices(name, fn.Template, cur.Prepared().Image)
			} else {
				// This is an observation of the current source generation, not a
				// new desired generation. Its status callbacks must not invalidate
				// meaningful source work that is still completing. They reuse the
				// fencing token from that generation's desired-state write: no new
				// token is allocated, and a newer worker's token makes them stale.
				generation, fencingToken := r.generationAndFencingToken(name)
				// This is a periodic VERIFICATION of an unchanged, already-
				// available app: it must not claim that a new generation is
				// being prepared. No RecordPreparing here — the public status is
				// only moved to preparing when an actual desired-generation change
				// is detected (the rebuild path below).
				//
				// worked tracks whether the service pass actually did anything:
				// onReconcileStart fires only when real corrective container work
				// begins (see services.go). A no-op verification leaves it false,
				// so the completion below writes NOTHING and ready,
				// last_reconcile_status, and updated_at stay untouched. A pass that
				// did real corrective convergence ends ready. The callback is
				// per-generation and generation-guarded.
				worked := false
				updateStatus := r.updateServicesObservationWithStatus
				if updateStatus == nil {
					updateStatus = r.updateServicesWithStatus
				}
				updateStatus(name, fn.Template, cur.Prepared().Image,
					func() {
						worked = true
						if r.st != nil && r.currentGeneration(name, generation) {
							r.st.RecordReconciling(r.rctx(), name, fencingToken)
						}
					}, func(err error) {
						if r.st == nil || !r.currentGeneration(name, generation) {
							return
						}
						if err != nil {
							r.st.RecordServiceFailure(r.rctx(), name, err, fencingToken)
							return
						}
						if !worked {
							// Nothing was converged: a no-op verification must not
							// rewrite last_reconcile_status/last_reconcile_at or
							// touch updated_at.
							return
						}
						r.st.RecordReconcileSuccess(r.rctx(), name, cur.Prepared().Image, known, time.Now(), fn, fencingToken)
					})
			}
		}
		return
	}

	// A real preparation is starting: this is the application lifecycle's start
	// transition, distinct from the periodic no-op skip above. The classification
	// uses only state this pass already holds (the registry entry, the tracked
	// identity, and the pre-read observation), so it adds no database read, no
	// second fingerprint scan, and no registry lookup.
	prepOutcome := preparationOutcome(cur, observed, known, hasFingerprint, fp)
	prepAttrs := []any{
		"app", name,
		"fingerprint", fp,
		"outcome", prepOutcome,
	}
	if hasFingerprint {
		prepAttrs = append(prepAttrs, "active_fingerprint", known)
	}
	if cur != nil && isAvailable(cur) {
		// A prepared no-runtime app serves no app image; its services bring
		// their own, so an empty active_image is omitted rather than emitted.
		if image := cur.Prepared().Image; image != "" {
			prepAttrs = append(prepAttrs, "active_image", image)
		}
	}
	r.reportDeployment(name, state.DeploymentAppActivating, "")
	r.log.Info("App: preparing", prepAttrs...)

	r.mu.Lock()
	r.generations[name]++
	generation := r.generations[name]
	r.mu.Unlock()
	prepSourceGeneration := r.deploymentSourceGeneration()
	var fencingToken state.FencingToken
	if r.st != nil {
		// Reuse the fingerprint computed above for this desired generation: it is
		// recorded as DesiredFingerprint without a second filesystem scan, so the
		// state write transaction holds no external I/O. The returned fencing
		// token is the PostgreSQL-authoritative identity of this generation; every
		// callback below carries it so another worker's newer generation wins.
		fencingToken, _ = r.st.RecordPreparingWithSourceGeneration(
			r.rctx(), name, fn, fp, r.deploymentSourceGeneration())
		r.setFencingToken(name, fencingToken)
	}

	start := time.Now()
	built, err := r.prepareImage(r.prepareContext(name, generation, fencingToken), fn, fp, selection)
	if err != nil {
		// The runtime build boundary already reports the build itself at Warn
		// (failed/timed out) or Debug (cancelled), so this is the ONE terminal
		// deployment summary and it never restates the build error as a second
		// failure line.
		switch {
		case errors.Is(err, context.Canceled):
			// Relay shutdown cancels the preparation. That is an expected
			// transition, not a deployment failure, so it does not raise an
			// Error the operator would have to triage.
			r.log.Debug(
				"App: preparation cancelled",
				"app", name,
				"fingerprint", fp,
				"duration", time.Since(start),
				"outcome", "cancelled",
			)
		case cur != nil && isAvailable(cur):
			// A usable generation still serves, so the deployment is degraded
			// rather than lost; name it so the retained version is identifiable.
			attrs := []any{
				"app", name,
				"fingerprint", fp,
			}
			// A retained no-runtime generation has no app image, so active_image
			// is omitted rather than logged empty.
			if image := cur.Prepared().Image; image != "" {
				attrs = append(attrs, "active_image", image)
			}
			attrs = append(attrs,
				"duration", time.Since(start),
				"outcome", "degraded",
				"error", err,
			)
			if hasFingerprint {
				attrs = append(attrs, "active_fingerprint", known)
			}
			r.log.Error("App: deployment degraded; retaining active generation", attrs...)
		default:
			// Nothing usable serves: a first deployment failed, or the only
			// known generation was already unavailable. There is no active
			// image or fingerprint to correlate, because none is serving.
			r.log.Error(
				"App: deployment failed",
				"app", name,
				"fingerprint", fp,
				"duration", time.Since(start),
				"outcome", "failed",
				"error", err,
			)
		}
		// Keep the old active version AND the old fingerprint so a later change
		// (which alters the fingerprint) triggers a fresh attempt.
		if r.st != nil {
			// The prior active version is retained in the state database; only
			// the failure outcome is recorded.
			r.st.RecordReconcileFailure(r.rctx(), name, err, fencingToken)
		}
		phase := state.DeploymentAppFailed
		if cur != nil && isAvailable(cur) {
			phase = state.DeploymentAppDegraded
		}
		r.reportDeployment(name, phase, err.Error())
		return
	}
	pf := runner.NewPrepared(fn, built, r.builder)

	// The skip key and the persisted active fingerprint are the identity the
	// image was ACTUALLY built from (built.Fingerprint, derived inside Prepare
	// from the one immutable source snapshot the build staged), not the pre-build
	// scan `fp`. They can differ when the source mutated between the reconciler's
	// scan and the build's capture; recording the built value means the next
	// reconcile/periodic audit compares against what is really baked and rebuilds
	// the drift, instead of silently treating the mutated tree as current.
	builtFP := built.Fingerprint
	if builtFP == "" {
		// Defensive: a Builder that does not return an identity (hand-built
		// fakes) keeps the scanned value.
		builtFP = fp
	}

	// Determine the superseded image before swapping so we can retire it after
	// the registry points at the new version. In-flight Handles keep the old
	// snapshot's image; the runner's refcount guards removal until idle.
	var oldImage string
	if cur != nil && cur.Prepared() != nil {
		oldImage = cur.Prepared().Image
	}

	// withPrevious adds the generation this pass supersedes to a lifecycle line
	// when one was serving, so a replacement can be followed from the old
	// identity to the new one. It never includes a fencing token.
	withPrevious := func(attrs []any) []any {
		if hasFingerprint {
			attrs = append(attrs, "previous_fingerprint", known)
		}
		if oldImage != "" {
			attrs = append(attrs, "previous_image", oldImage)
		}
		return attrs
	}

	// Publish only if the selection this generation was built from is still the
	// authoritative one. The Manager takes the same per-runtime advisory lock a
	// selection CAS write takes for the whole check-plus-swap, so a concurrent
	// write cannot slip between them. A rejected generation releases its
	// preparation/publication lease and leaves the old active app, its services,
	// its schedules, and its warm pool untouched.
	publish := func() error {
		// Independent publication axes: the runtime image selection CAS below
		// fences the base-image pin, while this check-and-swap boundary fences the
		// Git deployment generation. The registry swap runs inside the deployment
		// publication lock, so a publisher cannot commit a newer generation
		// between the row check and activation. A preparation started for an older
		// desired generation therefore never becomes the active app.
		return r.publishDeploymentCurrent(r.rctx(), prepSourceGeneration, func() {
			r.reg.Replace(name, pf)
		})
	}
	if publisher, ok := r.builder.(fencedSelectionBuilder); ok {
		if pubErr := publisher.PublishPrepared(r.rctx(), built, publish); pubErr != nil {
			// The publication handle owns the transferred image lease; release
			// it (and the preparation handle's own reference defensively) so a
			// rejected generation strands no admitted image.
			pf.ReleasePublication()
			built.ReleaseLease()

			if errors.Is(pubErr, errDeploymentSourceStale) {
				r.reportDeployment(
					name, state.DeploymentAppActivating,
					"deployment generation changed during preparation",
				)
				r.log.Info(
					"App: deployment generation changed during preparation; retaining previous version",
					"app", name,
					"fingerprint", builtFP,
					"outcome", "stale",
				)
				return
			}

			if errors.Is(pubErr, runtime.ErrRuntimeImageSelectionStale) {
				attrs := []any{"app", name}
				attrs = append(attrs, appImageAttr(built.Image)...)
				attrs = append(attrs,
					"fingerprint", builtFP,
					"outcome", "stale",
					"error", pubErr,
				)
				r.log.Info(
					"App: runtime image selection changed during preparation; retaining previous version",
					attrs...,
				)
				// Refresh the complete snapshot before retrying so the next
				// attempt builds from the authoritative pin, then coalesce a
				// prompt retry. The old generation remains active throughout.
				// When the refresh itself fails, leave the retry to the periodic
				// backstop instead of spinning against an unavailable database.
				if r.refreshRuntimeSelections() {
					r.Enqueue(name)
				}
				return
			}

			attrs := []any{"app", name}
			attrs = append(attrs, appImageAttr(built.Image)...)
			attrs = append(attrs,
				"fingerprint", builtFP,
				"outcome", "failed",
				"error", pubErr,
			)
			r.log.Error(
				"App: publication failed; retaining previous version",
				attrs...,
			)
			if r.st != nil {
				r.st.RecordReconcileFailure(r.rctx(), name, pubErr, fencingToken)
			}
			return
		}
	} else if pubErr := publish(); pubErr != nil {
		pf.ReleasePublication()
		built.ReleaseLease()
		r.log.Info(
			"App: deployment generation changed during preparation; retaining previous version",
			"app", name,
			"fingerprint", builtFP,
			"outcome", "stale",
		)
		return
	}
	// Record the built identity as this Reconciler's skip key and the source
	// selection the published image was built from, not a later read of the
	// Manager snapshot: a pin change after publication must compare unequal on
	// the next pass and reprepare.
	r.mu.Lock()
	r.fingerprints[name] = builtFP
	r.imageSelections[name] = built.ImageSelection
	r.mu.Unlock()

	// The registry now serves this generation. This is the image-PUBLICATION
	// transition, not deployment readiness: schedules and service containers
	// have not converged yet and state still records the preparation as
	// building. Readiness is logged only after convergence completes.
	publicationOutcome := "updated"
	if cur == nil {
		publicationOutcome = "discovered"
	}
	publicationAttrs := withPrevious(deploymentAttrs(name, built, builtFP))
	publicationAttrs = append(publicationAttrs,
		"duration", time.Since(start),
		"outcome", publicationOutcome,
	)
	r.log.Info("App: image published", publicationAttrs...)

	ready := func() bool {
		if !r.currentGeneration(name, generation) {
			return false
		}
		if r.st == nil {
			return true
		}
		ctx := r.rctx()
		landed, err := r.st.RecordReconcileSuccess(
			ctx, name, built.Image, builtFP, time.Now(), fn, fencingToken,
		)
		// State is a non-authoritative view: a write error must not hide a
		// locally completed deployment. A confirmed no-row result, however,
		// means another generation (or removal) owns the persisted lifecycle.
		// Lifecycle cancellation is the exception: shutdown must not report a
		// late readiness transition after the reconciler has stopped.
		if err != nil {
			return !(ctx.Err() != nil && errors.Is(err, ctx.Err()))
		}
		if landed {
			r.reportDeployment(name, state.DeploymentAppActive, "")
		}
		return landed
	}
	failServices := func(serviceErr error) {
		if serviceErr != nil && r.st != nil && r.currentGeneration(name, generation) {
			r.st.RecordServiceFailure(r.rctx(), name, serviceErr, fencingToken)
		}
	}
	hasServices := len(fn.Template.Services) > 0
	// hasUsablePrior reports whether a generation that can still serve remains
	// from before this pass. A service failure then means the deployment is
	// degraded (the previous version still serves); without one — a first
	// deployment, or one whose prior generation already failed — the deployment
	// failed.
	hasUsablePrior := cur != nil && isAvailable(cur)
	readyToLog := false
	// logReady reports the DEPLOYMENT readiness transition, which is distinct
	// from image publication above. It is emitted once the app's desired work
	// has completed for this generation; a service failure never logs ready.
	logReady := func() {
		if !r.currentGeneration(name, generation) {
			return
		}
		attrs := withPrevious(deploymentAttrs(name, built, builtFP))
		attrs = append(attrs,
			"services", len(fn.Template.Services),
			"duration", time.Since(start),
			"outcome", "ready",
		)
		r.log.Info("App: ready", attrs...)
	}
	// Readiness is deferred whenever a status-capable service hook is wired: it
	// reports a completion for EVERY desired service set, including an empty
	// one. A template that removes its last service still enqueues cleanup of
	// the containers it used to run, and that cleanup can fail, so the
	// deployment must not be marked ready (or logged as ready) before it
	// completes. The legacy status-less hook has no completion signal at all, so
	// its immediately-available generation stays ready as before.
	if r.updateServicesWithStatus == nil {
		readyToLog = ready()
	}

	// After a successful swap, converge the scheduler's cron jobs to this
	// template's schedules. This runs on both discovery and update, never on
	// the skip path above nor on a build failure (the previous version — and
	// its schedules — are retained).
	if r.updateSchedules != nil {
		r.updateSchedules(name, fn.Template)
	}

	// After the scheduler converges, converge the app's persistent service
	// containers to the freshly prepared template and image. This runs on both
	// discovery and update paths (the registry now serves the new version), and
	// not on the skip path above nor on a build failure (where the previous
	// version — and its service containers — are retained).
	if r.updateServicesWithStatus != nil {
		r.updateServicesWithStatus(name, fn.Template, built.Image,
			func() {
				if r.st != nil && r.currentGeneration(name, generation) {
					r.st.RecordReconciling(r.rctx(), name, fencingToken)
				}
			}, func(err error) {
				if err != nil {
					failServices(err)
					if !r.currentGeneration(name, generation) {
						// A newer generation superseded this pass: its failure
						// is not a deployment-level outcome for this one.
						return
					}
					if errors.Is(err, context.Canceled) &&
						errors.Is(r.rctx().Err(), context.Canceled) {
						deployAttrs := withPrevious(deploymentAttrs(name, built, builtFP))
						deployAttrs = append(deployAttrs,
							"services", len(fn.Template.Services),
							"outcome", "cancelled",
							"error", err,
						)
						r.log.Debug("App: deployment cancelled", deployAttrs...)
						return
					}
					// A current-generation, non-cancellation service convergence
					// failure is terminal for this deployment progress. Report it as
					// degraded when a usable prior generation remains, otherwise
					// failed; never leave the cluster status stuck at activating and
					// never mark ready.
					if hasUsablePrior {
						r.reportDeployment(name, state.DeploymentAppDegraded, err.Error())
					} else {
						r.reportDeployment(name, state.DeploymentAppFailed, err.Error())
					}
					// The service reconciler already reported the failure
					// detail. This line records what the failure means for the
					// deployment, which depends on whether a usable generation
					// remains: a retained previous generation is degraded, and a
					// deployment with nothing usable left has failed. This holds
					// for an empty desired service set too, where the failure is
					// cleanup of the containers the app used to run.
					deployAttrs := withPrevious(deploymentAttrs(name, built, builtFP))
					deployAttrs = append(deployAttrs,
						"services", len(fn.Template.Services),
					)
					if hasUsablePrior {
						deployAttrs = append(deployAttrs, "outcome", "degraded")
						r.log.Warn("App: deployment degraded", deployAttrs...)
						return
					}
					deployAttrs = append(deployAttrs, "outcome", "failed")
					r.log.Error("App: deployment failed", deployAttrs...)
					return
				}
				// Completion — not publication — is readiness: every desired
				// service set converged, including removal of the containers a
				// zero-service template no longer wants.
				if ready() {
					logReady()
				}
			})
	} else if r.updateServices != nil {
		r.updateServices(name, fn.Template, built.Image)
	}

	// Only the legacy no-status path reaches here with nothing left to wait for:
	// schedules have converged above and no service container is desired, so
	// this generation is ready now. With the status-capable hook, readiness is
	// reported by the completion callback above for every desired service set.
	if r.updateServicesWithStatus == nil && !hasServices && readyToLog {
		logReady()
	}

	// Retire the superseded version now that the registry serves the new one,
	// persistent services have been converged to it, and — crucially — the
	// service containers that still ran on the old image have been replaced. The
	// hook (when wired) defers removal until the old image is no longer in use;
	// the runner-side reference guard (in-flight refcount plus Relay-owned
	// container references) is the second line of defense for partial service
	// reconcile failures, so a service container left on the old image keeps that
	// image from being removed. Skip a nil hook and an equal image (an
	// unavailable->unavailable retry, or a same-image re-prepare).
	if r.retire != nil && oldImage != "" && oldImage != built.Image {
		r.retire(name, oldImage)
	}
}

// deploymentAttrs returns the shared correlation attributes for an app's
// deployment lifecycle lines: the app, the image the registry serves (when the
// generation has one), the source identity that image was built from, and (when
// present) the shared dependency layer it consumes. It deliberately carries no
// fencing token or other internal generation authority.
func deploymentAttrs(name string, built *runtime.Prepared, fingerprint string) []any {
	attrs := []any{"app", name}
	attrs = append(attrs, appImageAttr(built.Image)...)
	attrs = append(attrs, "fingerprint", fingerprint)
	if built.Dependency != "" {
		attrs = append(attrs, "dependency", built.Dependency)
	}
	return attrs
}

// appImageAttr returns the app-level image attribute for a generation that has
// one. A no-runtime app builds no app image (its services bring their own), so
// omitting the attribute avoids claiming an image identity that does not exist.
func appImageAttr(image string) []any {
	if image == "" {
		return nil
	}
	return []any{"image", image}
}

// preparationOutcome classifies why this pass is preparing a generation, for
// logging only. It reads nothing: every input is state the pass already holds,
// and the result never influences behavior, admission, or state writes.
//
// A generation can fail while an older one keeps serving, and a first build can
// fail with no registry entry at all. In both cases the persisted desired
// fingerprint and failed status record the attempt, so the comparison runs
// BEFORE the registry-entry fallbacks: it distinguishes a RETRY of that same
// failed desired identity from a NEW GENERATION replacing it. Without the
// observation both look like a first discovery or a content change.
func preparationOutcome(
	cur *runner.PreparedApp,
	observed *state.AppObservation,
	trackedFingerprint string,
	hasTrackedFingerprint bool,
	desiredFingerprint string,
) string {
	failedDesiredFingerprint := ""
	if observed != nil && observed.Found &&
		observed.Detail.LastReconcileStatus == state.ReconcileFailed {
		failedDesiredFingerprint = observed.Detail.DesiredFingerprint
	}
	switch {
	case failedDesiredFingerprint != "" &&
		failedDesiredFingerprint == desiredFingerprint:
		// This exact desired generation failed before; the pass retries it
		// instead of applying a change.
		return "retry"
	case failedDesiredFingerprint != "":
		// A newer desired generation replaced one that failed, so the recorded
		// failure no longer describes the desired content.
		return "new_generation"
	case cur == nil:
		// No registry entry and no recorded failure: a first discovery.
		return "discovered"
	case !isAvailable(cur):
		// Registered with no usable image: the previous attempt left nothing
		// serving, so this pass retries it.
		return "retry"
	case hasTrackedFingerprint && trackedFingerprint == desiredFingerprint:
		// Content is unchanged; the runtime base-image selection moved.
		return "selection"
	case hasTrackedFingerprint:
		return "updated"
	default:
		// No tracked identity to compare against: report a generic change.
		return "changed"
	}
}

// errDeploymentSourceStale reports that the coordinated deployment generation
// changed while an app was being prepared. The prepared image is not activated.
var errDeploymentSourceStale = errors.New("coordinated deployment generation changed during preparation")

// deploymentSourceGeneration returns the coordinated generation stamped on a
// desired-state write, or 0 when this process is not participating. A
// coordinator-active worker with generation 0 still has the gate enabled; it just
// has no authorized revision yet.
func (r *Reconciler) deploymentSourceGeneration() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.deploymentEnabled {
		return 0
	}
	return r.deploymentGeneration
}

// deploymentAuthorization is one app's coordinated authorization, read atomically
// with the authorization epoch. A pass must compare content against one
// authorization and stamp its verification with that same epoch: a concurrent
// SetDeploymentAuthorizationWithInvalid cannot then relabel content verified
// against the previous authorization as current.
type deploymentAuthorization struct {
	cluster               bool
	epoch                 uint64
	fingerprint           string
	authorized            bool
	removalOnly           bool
	templateIdentity      string
	templateIdentityKnown bool
	invalidTemplate       string
	invalidMessage        string
	invalid               bool
}

// deploymentAuthorization returns the authorization decision, template identity,
// desired-invalid diagnostic, and epoch for name from one consistent read. In
// standalone mode every app is authorized and no deployment state applies.
func (r *Reconciler) deploymentAuthorization(name string) deploymentAuthorization {
	r.mu.Lock()
	defer r.mu.Unlock()
	authorization := deploymentAuthorization{
		cluster: r.deploymentEnabled,
		epoch:   r.deploymentEpoch,
	}
	if !r.deploymentEnabled {
		authorization.authorized = true
		return authorization
	}
	authorization.fingerprint, authorization.authorized = r.deploymentFingerprints[name]
	authorization.removalOnly = r.deploymentRemoved[name]
	authorization.templateIdentity, authorization.templateIdentityKnown =
		r.deploymentTemplates[name]
	message, invalid := r.deploymentInvalid[name]
	if invalid {
		authorization.invalid = true
		authorization.invalidMessage = message
		authorization.invalidTemplate = r.deploymentInvalidTemplates[name]
	}
	return authorization
}

// publishDeploymentCurrent runs publish inside a short check-and-swap boundary
// serialized with PublishDeploymentDesired's publication advisory lock. The
// shared desired row is re-read while that lock is held, so a publisher cannot
// commit a newer generation between the check and the registry swap/add. The
// callback must be tiny (an in-process registry operation): no build, fetch,
// checkout, or materialization may run inside it. Without coordinated participation, publish
// runs directly. A mismatched/absent generation, a read failure, or a lost lock is
// reported as errDeploymentSourceStale and the previous active generation keeps
// serving.
//
// Lock ordering: the runtime selection publication lock may already be held by
// the caller (PublishPrepared); this method then takes the deployment publication
// lock. The deployment publisher takes only the deployment lock and never a
// runtime selection lock.
func (r *Reconciler) publishDeploymentCurrent(
	ctx context.Context,
	generation int64,
	publish func(),
) error {
	r.mu.Lock()
	enabled := r.deploymentEnabled
	localGeneration := r.deploymentGeneration
	r.mu.Unlock()
	if !enabled {
		publish()
		return nil
	}
	if generation <= 0 || localGeneration != generation {
		return errDeploymentSourceStale
	}
	if r.st == nil {
		publish()
		return nil
	}
	err := r.st.WithDeploymentDesiredLock(ctx, func(desired state.DeploymentDesired, found bool) error {
		if !found || desired.Generation != generation {
			return errDeploymentSourceStale
		}
		publish()
		return nil
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, errDeploymentSourceStale) {
		return err
	}
	r.log.Warn("Deployment: verify desired generation before publication failed; retaining previous version",
		"error", err)
	return fmt.Errorf("%w: %v", errDeploymentSourceStale, err)
}

func (r *Reconciler) reportDeployment(name, phase, errMessage string) {
	if r.deploymentProgress != nil {
		r.deploymentProgress(name, phase, errMessage)
	}
}

// reportDeploymentDrift reports local drift once per name per authorized
// generation and asks the deployment coordinator to restore the materialized
// revision. It never mutates the registry.
//
// A name the authorization classifies as desired-invalid keeps its Failed
// progress row: it has no authorized fingerprint, so a Drifted row would carry an
// empty fingerprint and loadApplied would discard it on restart, losing the app's
// desired-invalid authorization (including when its directory later disappears).
// The mismatch is still logged and still requests restoration; the previous
// Failed row remains the restart authority. Valid and local-only drift keeps
// recording Drifted progress.
func (r *Reconciler) reportDeploymentDrift(name, reason string) {
	if !r.admitDeploymentReverification(name) {
		return
	}
	if !r.deploymentNameInvalid(name) {
		r.reportDeployment(name, state.DeploymentAppDrifted, reason)
	}
	r.announceDeploymentReverification(name, reason)
}

// deploymentNameInvalid reports whether the installed authorization classifies
// name as desired-invalid (not local drift).
func (r *Reconciler) deploymentNameInvalid(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, invalid := r.deploymentInvalid[name]
	return invalid
}

// requestDeploymentReverification asks the deployment coordinator for a forced
// re-materialization of the applied generation, without changing the reported
// phase. The desired-invalid recovery path uses it before writing its own
// unconditional Failed diagnostic: a materialization that could not authorize the
// app may have failed transiently, and a later successful live fingerprint must
// re-authorize the exact shared revision rather than trust local bytes.
//
// It is rate-limited to one request per reconciler interval for the same
// effective authorization, so a successful same-generation materialization that
// reinstalls the authorization and re-enqueues the still-invalid app cannot
// force another Git sync on every pass. A new generation or a changed effective
// authorization is admitted immediately. This state is entirely separate from
// ordinary local drift: a re-verification request here never delays a later
// genuine local drift report for the same name.
func (r *Reconciler) requestDeploymentReverification(name, reason string) {
	if r.admitInvalidReverification(name) {
		r.announceDeploymentReverification(name, reason)
	}
}

// admitInvalidReverification applies the desired-invalid retry cooldown. It
// never reads or writes deploymentDriftReported, which belongs to local drift
// once-per-generation suppression.
func (r *Reconciler) admitInvalidReverification(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	authorization := r.invalidReverificationSignature(name)
	now := time.Now()
	if entry, ok := r.reverifyCooldown[name]; ok &&
		entry.authorization == authorization && now.Sub(entry.at) < r.interval {
		return false
	}
	r.reverifyCooldown[name] = reverifyCooldownEntry{authorization: authorization, at: now}
	return true
}

// invalidReverificationSignature identifies a desired-invalid app's effective
// authorization. A new generation, the app entering or leaving the invalid set,
// or a different materialized template identity admits an immediate retry; an
// identical reinstall does not. Callers must hold r.mu.
func (r *Reconciler) invalidReverificationSignature(name string) string {
	_, invalid := r.deploymentInvalid[name]
	return fmt.Sprintf(
		"%d/%t/%s/%s",
		r.deploymentGeneration,
		invalid,
		r.deploymentFingerprints[name],
		r.deploymentInvalidTemplates[name],
	)
}

// admitDeploymentReverification records a re-materialization request for name in
// the current authorized generation and reports whether this is the first. It
// writes no progress and logs nothing, so a retried drift observation performs
// no duplicate progress write.
func (r *Reconciler) admitDeploymentReverification(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deploymentDriftReported[name] {
		return false
	}
	r.deploymentDriftReported[name] = true
	return true
}

func (r *Reconciler) announceDeploymentReverification(name, reason string) {
	r.log.Warn(
		"App: local content is not authorized by the materialized deployment; ignoring and restoring",
		"app", name,
		"reason", reason,
	)
	if r.deploymentDrift != nil {
		r.deploymentDrift(name, reason)
	}
}

func (r *Reconciler) clearDeploymentRemoved(name string) {
	r.mu.Lock()
	delete(r.deploymentRemoved, name)
	r.mu.Unlock()
}

// currentImageSelection returns the runtime Manager's current validated selection
// identity for the template's runtime, or "" for a no-runtime app or a builder
// that does not expose a live selection snapshot. It is read on every app pass so
// a pin change is detected by identity comparison, not only by the refresh
// change notification.
func (r *Reconciler) currentImageSelection(tmpl *app.Template) string {
	if tmpl == nil || !tmpl.NeedsRuntime() {
		return ""
	}
	if source, ok := r.builder.(selectionIdentitySource); ok {
		return source.RuntimeImageSelectionIdentity(tmpl.Runtime)
	}
	return ""
}

func (r *Reconciler) currentGeneration(name string, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generations[name] == generation
}

// generationAndFencingToken returns the current local generation plus the
// PostgreSQL fencing token for name under one lock, so a periodic observation
// cannot pair one generation's local counter with another's token.
func (r *Reconciler) generationAndFencingToken(name string) (uint64, state.FencingToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generations[name], r.fencingTokens[name]
}

// setFencingToken records the fencing token returned by the desired-state write
// that started the current generation. A failed write (zero) clears it so the
// generation's callbacks fail closed rather than reusing an older fence.
func (r *Reconciler) setFencingToken(name string, fencingToken state.FencingToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fencingToken > 0 {
		r.fencingTokens[name] = fencingToken
		return
	}
	delete(r.fencingTokens, name)
}

// revertDeploymentPending undoes the pending/placeholder mutation made from a
// locally drifted tree, restoring the previously active matching state before
// the deployment coordinator restores the authorized tree.
func (r *Reconciler) revertDeploymentPending(name string, previous *runner.PreparedApp) {
	if previous == nil || isAvailable(previous) {
		r.reg.ClearPending(name)
		return
	}
	r.reg.Replace(name, previous)
}

// prepareImage builds the app's image, passing the fingerprint the
// reconciler already computed (and, for a runtime-backed app, the source
// selection it was computed from) only when the builder implements BOTH the
// selection-aware preparation and its matching fenced publication. The runtime
// Manager implements both, so a live rebuild hands it BOTH the tag identity it
// compared and the exact policy to stage: the selection is never re-derived and
// the fingerprint is never re-hashed by the reconciler. The Manager still
// captures the selection once and derives the built identity from that capture;
// the caller records the RETURNED fingerprint.
//
// A Builder that implements only selectionPreparer is treated as an incomplete
// optional implementation and falls back to plain Prepare: the deferred
// preparation contract is only safe when the matching PublishPrepared will run
// and activate the accepted generation. A test or non-Manager Builder that
// implements neither seam also uses plain Prepare. fp is "" only for an app the
// reconciler could not hash (which then never reaches here: a fingerprint error
// retains the previous version), so the value supplied is always the one just
// compared.
func (r *Reconciler) prepareImage(
	ctx context.Context,
	fn app.App,
	fp string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	if fb, ok := r.builder.(fencedSelectionBuilder); ok {
		return fb.PrepareWithFingerprintAndSelection(ctx, fn, fp, selection)
	}
	return r.builder.Prepare(ctx, fn)
}

// prepareContext roots a Prepare call in the reconciler context and installs the
// app-image build observer that publishes status=building at the ACTUAL
// managed-runtime image build boundary (runtime.WithAppBuildObserver). The
// callback is local-generation-guarded, and the state write itself validates the
// PostgreSQL fencing token, so a stale in-flight build cannot overwrite a newer
// generation's status. When no state sink is wired, the plain reconciler context
// is returned.
func (r *Reconciler) prepareContext(
	name string,
	generation uint64,
	fencingToken state.FencingToken,
) context.Context {
	ctx := r.rctx()
	if r.st == nil {
		return ctx
	}
	return runtime.WithAppBuildObserver(ctx, func() {
		if r.currentGeneration(name, generation) {
			r.st.RecordReconcileBuilding(ctx, name, fencingToken)
		}
	})
}

// observeApp snapshots the persisted row/token for name BEFORE this pass reads
// template.yaml. It is the optimistic guard for an invalid observation: the
// state write is applied only while the row still matches. A read failure is
// logged and returns nil, which callers must treat as "no authority", never as
// "the row is absent": recordInvalidDesired then skips the write instead of
// bypassing the concurrency guard. The fast path re-reads state separately and
// never skips on an unreadable observation.
func (r *Reconciler) observeApp(name string) *state.AppObservation {
	if r.st == nil {
		return nil
	}
	observation, err := r.st.ObserveApp(r.rctx(), name)
	if err != nil {
		r.log.Warn("App: observe state failed", "app", name, "error", err)
		return nil
	}
	return &observation
}

// skipUnchangedInvalidDesired is the LIVE fast path for an invalid template whose
// exact content identity is (still) the persisted invalid desired state. The
// parse, warning, state write, and fencing-token allocation are all skipped; only
// process-local reconciliation bookkeeping is refreshed so a previously captured
// callback cannot keep serving the invalid generation. The persisted row, not
// this local map, remains the authority.
func (r *Reconciler) skipUnchangedInvalidDesired(name string, observation *state.AppObservation) {
	r.mu.Lock()
	delete(r.fingerprints, name)
	delete(r.imageSelections, name)
	r.generations[name]++
	if observation != nil && observation.Found && observation.FencingToken > 0 {
		r.fencingTokens[name] = observation.FencingToken
	} else {
		delete(r.fencingTokens, name)
	}
	r.mu.Unlock()
	r.log.Debug(
		"App: invalid template unchanged; reconcile skipped",
		"app", name,
	)
}

// recordInvalidDesired records a present-but-unloadable desired definition into
// the state view (degraded with a retained active generation, or unavailable
// without one) while leaving the runtime registry untouched. It is the shared
// seam for every reconcileApp failure that is NOT a removal and NOT a valid
// desired generation that failed to build: ErrInvalidPath, a flaky read/stat, an
// invalid template, and a fingerprint error. A directory without template.yaml
// is not an app candidate and follows the removal/no-op path instead. State is a
// view only — the previously-loaded version stays served — so an operator sees
// the failed desired state instead of a stale ready row. When no state sink is
// wired it is a no-op.
//
// identity is non-empty ONLY for a deterministic parse/validation failure of the
// template bytes that were actually read. The state write then takes its
// read-only fast path when that identity is already persisted (no write, no
// token allocation). Operational failures pass an empty identity and clear any
// earlier identity, so a transient read can never reuse it. observed is the
// pre-read state snapshot: a stale invalid observation (begun before another
// worker's valid change) is rejected by the conditional write. A nil observed
// means the snapshot could not be read; the write is skipped (fail closed)
// instead of bypassing the guard with an unconditional write.
//
// It also FORGETS the stored fingerprint for name and invalidates this
// Reconciler's local generation plus the persisted fencing token. That stored
// fingerprint is the reconciler's skip key: if it were left in place, a
// definition later restored byte-identically would compare equal and take the
// unchanged skip path, leaving the recorded failure visible forever (the skip
// path deliberately writes nothing). Forgetting it forces the next reconcile to
// re-resolve the identity, so a restored definition is re-verified and a real
// success replaces the failure — the same "retry even when the fingerprint is
// stable" behavior an unavailable app already has. Advancing the generation
// prevents a callback captured for the previous desired state from writing after
// the invalid state is recorded. It never writes a success itself.
func (r *Reconciler) recordInvalidDesired(
	name,
	identity string,
	err error,
	observed *state.AppObservation,
) {
	r.mu.Lock()
	delete(r.fingerprints, name)
	delete(r.imageSelections, name)
	r.generations[name]++
	delete(r.fencingTokens, name)
	r.mu.Unlock()
	if r.st == nil {
		return
	}
	if observed == nil {
		// A failed state read is not an absent row. Without the pre-read
		// snapshot the conditional write has no concurrency authority, so keep
		// this generation's token cleared (callbacks fail closed) and let a later
		// pass retry.
		r.log.Warn(
			"App: invalid desired state not recorded; state observation unavailable",
			"app", name,
			"error", err,
		)
		return
	}
	// The invalid desired write advances the persisted fencing token (except on
	// the unchanged-identity fast path, where no generation advances), invalidating
	// older completions from any worker. Keep the returned token as this
	// Reconciler's current state identity; zero (write failure or a rejected stale
	// observation) leaves it cleared, so later callbacks fail closed.
	result, writeErr := r.st.RecordInvalidDesiredWithSource(
		r.rctx(), name, identity, err, observed)
	if writeErr != nil {
		// The state layer already logged the operational failure. The token
		// stays cleared (fail closed); state is a view, not this pass's authority.
		return
	}
	if result.Stale {
		// Another worker advanced the app after this pass read the template.
		// Discarding the observation is a normal recovery, not a write failure;
		// keep the local token cleared so no captured callback can write.
		r.log.Debug("App: stale invalid desired observation rejected", "app", name)
		return
	}
	r.setFencingToken(name, result.FencingToken)
}

// remove drops an app from the registry and forgets its fingerprint. It
// invalidates the app's local generation and persisted fencing token before
// deleting, so a captured service completion from the removed generation can
// neither write a status nor recreate the row. Local services and images are
// still retired even when the state delete is suppressed by a newer generation:
// the directory is gone from THIS worker's point of view.
func (r *Reconciler) remove(name string) {
	r.reg.Replace(name, nil)
	r.mu.Lock()
	delete(r.fingerprints, name)
	delete(r.imageSelections, name)
	delete(r.verified, name)
	delete(r.reverifyCooldown, name)
	delete(r.deploymentDriftReported, name)
	r.generations[name]++
	fencingToken := r.fencingTokens[name]
	delete(r.fencingTokens, name)
	r.mu.Unlock()
	r.clearDeploymentRemoved(name)
	r.reportDeployment(name, state.DeploymentAppRemoved, "")
	if r.st != nil {
		r.st.RecordRemoved(r.rctx(), name, fencingToken)
	}
	// The directory is gone, so every version of this app's images is now
	// garbage. Let the runner retire all of them (once idle) when wired. The
	// wired RemoveApp hook also deletes the app's Prometheus series at
	// this same retirement point (the reconciler itself stays metrics-free; the
	// worker wires the metrics deletion by wrapping the hook).
	//
	// RemoveServices runs FIRST, before RemoveApp retires the images: a
	// running service container still references the app's images, so those
	// containers must be stopped and removed before the images become
	// retire-eligible. Nil-safe.
	if r.removeServices != nil {
		r.removeServices(name)
	}
	if r.removeApp != nil {
		r.removeApp(name)
	}
	r.log.Info(
		"App: removed",
		"app", name,
	)
}

// isAvailable reports whether a prepared app has a usable image.
func isAvailable(pf *runner.PreparedApp) bool {
	return pf.Prepared() != nil
}

// knownLocally reports whether this Reconciler already holds state for name: an
// active or pending registry entry, or a fencing token from startup
// discovery/invalid persistence. It deliberately consults no database:
// PostgreSQL is a view and must never decide whether an app is removed.
func (r *Reconciler) knownLocally(name string) bool {
	if r.reg != nil && (r.reg.GetByName(name) != nil || r.reg.HasPending(name)) {
		return true
	}
	r.mu.Lock()
	_, ok := r.fencingTokens[name]
	r.mu.Unlock()
	return ok
}

// isSinglePathElement reports whether name can safely be joined to the root to
// inspect exactly one child. App-name validation is intentionally separate:
// invalid but single-element names may still identify a previously-known app.
func isSinglePathElement(name string) bool {
	return name != "" && name != "." && name != ".." && !filepath.IsAbs(name) &&
		!strings.ContainsAny(name, "/\\") && name == filepath.Base(name)
}
