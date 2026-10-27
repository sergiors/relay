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
func (r *Reconciler) reconcileAll() {
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
	// Apps still registered, pending, or carrying a fencing token but no
	// longer candidates on disk were removed. A brand-new app whose first
	// preparation has not yet completed exists ONLY as a pending desired entry
	// (nothing is published as active until the build succeeds), so PendingNames
	// must be swept too: otherwise a directory that vanishes during preparation
	// would leave a stale pending entry gating events forever. Fencing tokens
	// cover a name that only ever had a startup invalid-desired row and therefore
	// never entered the registry.
	for _, name := range r.missingNames(seen) {
		r.dispatch(name)
	}
}

// missingNames returns the locally-known names (active, pending-only, or
// carrying a startup fencing token) that are NOT app candidates on disk, so
// reconcileAll dispatches them for removal. Fencing tokens are included so a
// name that only ever had a startup invalid-desired row — and therefore never
// entered the registry — is still removed once its definition disappears. An app
// that is BOTH active and pending (an available generation being rebuilt) is
// returned once.
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
	r.mu.Unlock()
	for _, name := range fencingTokenNames {
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
			// generation.
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
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
				if r.knownLocally(name) {
					r.remove(name)
				}
				return
			}
		}
		if errors.Is(err, fs.ErrNotExist) {
			// Directory gone -> remove from registry. In-flight invocations keep
			// the old snapshot; they are not killed.
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
		if errors.Is(err, app.ErrNotReady) {
			// The directory has no template.yaml, so it is not an app candidate.
			// If this Reconciler already knows the name (an active/pending
			// registry entry, or a startup fencing token for an invalid row),
			// losing its template is a normal removal. A never-app directory
			// holding only unrelated files is a no-op: no warning, no invalid
			// state write, no generation churn.
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
		r.recordInvalidDesired(name, "", err, observed)
		return
	}
	source := templateSource

	// The bytes were read and hashed. If their exact deterministic-invalid
	// identity is CURRENTLY persisted, skip the parse entirely. This is a fresh
	// persisted-state read (not the pre-read snapshot and never a process-local
	// negative cache): the row is the authority, so an unchanged invalid input is
	// recognized across cycles and restarts, while another worker's valid
	// correction is never overridden by a stale local view.
	if source.Identity != "" && observed != nil && observed.Found &&
		observed.Detail.InvalidSource == source.Identity {
		if current := r.observeApp(name); current != nil &&
			current.Found && current.Detail.InvalidSource == source.Identity {
			r.reg.ClearPending(name)
			r.skipUnchangedInvalidDesired(name, current)
			return
		}
	}

	fn, err := source.Parse()
	if err != nil {
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
		r.recordInvalidDesired(name, identity, err, observed)
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

	r.mu.Lock()
	known, hasFingerprint := r.fingerprints[name]
	r.mu.Unlock()

	// Skip a rebuild only when the current build is healthy AND content is
	// unchanged. A previously-failed build (unavailable) is retried even if the
	// fingerprint is stable, so a broken app recovers without edits.
	if cur != nil && isAvailable(cur) && hasFingerprint && known == fp {
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

	r.log.Debug(
		"App: changed; rebuilding",
		"app", name,
	)
	r.mu.Lock()
	r.generations[name]++
	generation := r.generations[name]
	r.mu.Unlock()
	var fencingToken state.FencingToken
	if r.st != nil {
		// Reuse the fingerprint computed above for this desired generation: it is
		// recorded as DesiredFingerprint without a second filesystem scan, so the
		// state write transaction holds no external I/O. The returned fencing
		// token is the PostgreSQL-authoritative identity of this generation; every
		// callback below carries it so another worker's newer generation wins.
		fencingToken, _ = r.st.RecordPreparingWithFingerprint(r.rctx(), name, fn, fp)
		r.setFencingToken(name, fencingToken)
	}

	start := time.Now()
	built, err := r.prepareImage(r.prepareContext(name, generation, fencingToken), fn, fp, selection)
	if err != nil {
		r.log.Error(
			"App: reload failed; retaining previous version",
			"app", name,
			"error", err,
			"duration", time.Since(start),
			"outcome", "failed",
		)
		// Keep the old active version AND the old fingerprint so a later change
		// (which alters the fingerprint) triggers a fresh attempt.
		if r.st != nil {
			// The prior active version is retained in the state database; only
			// the failure outcome is recorded.
			r.st.RecordReconcileFailure(r.rctx(), name, err, fencingToken)
		}
		return
	}
	pf := runner.NewPrepared(fn, built, r.builder)

	// Determine the superseded image before swapping so we can retire it after
	// the registry points at the new version. In-flight Handles keep the old
	// snapshot's image; the runner's refcount guards removal until idle.
	var oldImage string
	if cur != nil && cur.Prepared() != nil {
		oldImage = cur.Prepared().Image
	}

	r.reg.Replace(name, pf)
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
	r.mu.Lock()
	r.fingerprints[name] = builtFP
	r.mu.Unlock()

	ready := func() {
		if r.st == nil || !r.currentGeneration(name, generation) {
			return
		}
		r.st.RecordReconcileSuccess(r.rctx(), name, built.Image, builtFP, time.Now(), fn, fencingToken)
	}
	failServices := func(serviceErr error) {
		if serviceErr != nil && r.st != nil && r.currentGeneration(name, generation) {
			r.st.RecordServiceFailure(r.rctx(), name, serviceErr, fencingToken)
		}
	}
	if len(fn.Template.Services) == 0 || r.updateServicesWithStatus == nil {
		if r.st != nil {
			ready()
		}
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
					return
				}
				ready()
			})
	} else if r.updateServices != nil {
		r.updateServices(name, fn.Template, built.Image)
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

	if cur == nil {
		r.log.Info("App: discovered",
			"app", name,
			"duration", time.Since(start),
			"outcome", "discovered",
		)
	} else {
		r.log.Info("App: updated",
			"app", name,
			"duration", time.Since(start),
			"outcome", "updated",
		)
	}
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

// prepareImage builds the app's image, passing the fingerprint the
// reconciler already computed (and, for a runtime-backed app, the source
// selection it was computed from) when the builder supports it. The runtime
// Manager does, so a live rebuild hands it BOTH the tag identity it compared and
// the exact policy to stage: the selection is never re-derived and the
// fingerprint is never re-hashed by the reconciler. The Manager still captures
// the selection once and derives the built identity from that capture; the
// caller records the RETURNED fingerprint. A test or non-Manager Builder that
// only implements Builder falls back to Prepare, which computes them itself;
// those fakes do not care about the identity anyway. fp is "" only for an app
// the reconciler could not hash (which then never reaches here: a fingerprint
// error retains the previous version), so the value supplied is always the one
// just compared.
func (r *Reconciler) prepareImage(
	ctx context.Context,
	fn app.App,
	fp string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	if sp, ok := r.builder.(selectionPreparer); ok {
		return sp.PrepareWithFingerprintAndSelection(ctx, fn, fp, selection)
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
	r.generations[name]++
	fencingToken := r.fencingTokens[name]
	delete(r.fencingTokens, name)
	r.mu.Unlock()
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
