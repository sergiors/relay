package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/state"
)

// startupAppSet is the immutable result of the startup app phase. It carries
// only what later startup stages consume: the valid loaded apps, the loader's
// diagnostics for present-but-invalid directories, the one-pass
// fingerprint/source-selection records, and the fingerprint map used to seed
// the reconciler.
type startupAppSet struct {
	loaded       []app.App
	issues       []app.LoadIssue
	records      []startupApp
	fingerprints map[string]string
	// fencingTokens carries the PostgreSQL fencing token persisted for each app's
	// startup desired generation, keyed by app name. Startup preparation, its
	// build observer, the startup service callbacks, and the reconciler seed all
	// reuse these exact values instead of reading apps.fencing_token back.
	fencingTokens map[string]state.FencingToken
}

// startupParseGate makes the startup loader consult persisted state around each
// template read. Observe snapshots the row BEFORE the read so a later invalid
// write can be rejected if another worker advances the app; SkipParse confirms
// (with a FRESH persisted read) that the exact same deterministic invalid
// identity is still current before skipping the parse. An unreadable state view
// never authorizes a skip (the loader parses normally) and never leaves a
// fabricated observation: a missing snapshot makes the state phase skip that
// issue rather than write without concurrency authority.
type startupParseGate struct {
	ctx      context.Context
	st       *state.State
	logger   *slog.Logger
	observed map[string]state.AppObservation
}

func (g *startupParseGate) Observe(name string) {
	observation, err := g.st.ObserveApp(g.ctx, name)
	if err != nil {
		g.logger.Warn("Startup: observe app state failed", "app", name, "error", err)
		return
	}
	g.observed[name] = observation
}

func (g *startupParseGate) SkipParse(name, identity string) (bool, error) {
	observed, ok := g.observed[name]
	if !ok || !observed.Found || observed.Detail.InvalidSource == "" ||
		observed.Detail.InvalidSource != identity {
		return false, nil
	}
	// The pre-read snapshot only nominates a skip. Confirm against the current
	// persisted row so a stale observation cannot skip parsing after another
	// worker's valid correction. If the row moved, parse and record normally
	// with the pre-read observation (which the state write then rejects if it is
	// stale).
	current, err := g.st.ObserveApp(g.ctx, name)
	if err != nil {
		g.logger.Warn("Startup: re-observe invalid app failed", "app", name, "error", err)
		return false, nil
	}
	if !current.Found || current.Detail.InvalidSource != identity {
		return false, nil
	}
	// The confirmed fresh row is now the observation the state phase may use.
	// The pre-read snapshot may have been advanced by another worker that
	// persisted the same identity; the fresh token is the current authority for
	// the read-only fast path. No write has happened, so this is a pure
	// observation refresh.
	g.observed[name] = current
	if current.Detail.LastError != "" {
		return true, errors.New(current.Detail.LastError)
	}
	return true, fmt.Errorf("app %q: invalid template", name)
}

// loadStartupApps loads /apps, resolves each app's fingerprint and source
// selection exactly once, persists the startup discovery view, and restores
// this worker's persisted stats into the fresh registry.
//
// The startup records carry both the fingerprint and the source selection the
// fingerprint was computed from, so no later stage re-reads /apps or recomputes
// a fingerprint. The state DB receives only the narrow (app, fingerprint)
// discovery pairs; the selection stays worker-local because it is a filesystem
// concern the state package must not own. Invalid-app persistence is per-app and
// recoverable, but a valid app's discovery write is CRITICAL: its fencing token
// is the only authority that lets startup preparation, service publication, and
// the reconciler seed write state safely. Such a failure is therefore returned
// and startup aborts instead of preparing an app with zero/missing authority.
// state.Open already ran before this phase and is fatal on failure, so no
// fingerprint or discovery work happens against a broken state database; the
// state DB is a persisted local view, never the source of truth for matching,
// builds, or reconciliation.
func loadStartupApps(
	startupCtx context.Context,
	logger *slog.Logger,
	st *state.State,
	metricsInstance *metrics.Registry,
) (startupAppSet, error) {
	_, loaderSpan := tracing.Start(startupCtx, "apps.load")
	loader := app.NewLoader(app.Dir, logger)
	observed := make(map[string]state.AppObservation)
	var gate app.TemplateParseGate
	if st != nil {
		gate = &startupParseGate{
			ctx:      startupCtx,
			st:       st,
			logger:   logger,
			observed: observed,
		}
	}
	apps, loadIssues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		loaderSpan.RecordError(err)
		loaderSpan.SetStatus(codes.Error, err.Error())
		loaderSpan.End()
		// The worker cannot run without its app set. The deferred cleanup
		// releases the Redis client and stops the servers; the error is returned
		// for the CLI boundary to print and exit on.
		return startupAppSet{}, fmt.Errorf("load apps failed: %w", err)
	}
	loaderSpan.End()
	logger.Info("Loaded apps",
		"count", len(apps),
		"invalid", len(loadIssues),
		"root", app.Dir,
	)

	// Compute every loaded app's content fingerprint — and, for a
	// runtime-backed app, the source selection it was computed from — ONCE
	// for the whole startup. The fingerprint feeds the state phase (rebuild +
	// discovery upserts); the selection feeds Manager.Prepare, which captures it
	// into one immutable snapshot and derives the built image's tag, staged
	// context, and returned identity from that single read. The identity
	// actually built is what the state phase records and the reconciler is
	// seeded with, not this pre-prepare scan.
	_, fingerprintSpan := tracing.Start(startupCtx, "apps.fingerprint")
	fingerprintStart := time.Now()
	startup := selectAndFingerprintApps(apps, logger)
	fingerprints := make(map[string]string, len(startup))
	for _, s := range startup {
		fingerprints[s.App.Name] = s.Fingerprint
	}
	discovered := discoveredFromStartup(startup)
	fingerprintSpan.End()
	logger.Debug("Startup: fingerprints computed",
		"count", len(startup),
		"duration", time.Since(fingerprintStart),
	)

	// Present-but-invalid desired definitions are persisted alongside the valid
	// discovery; they stay out of the loaded set, so they never reach the
	// runtime registry, matching, the scheduler, or preparation. Invalid-issue
	// persistence failures are isolated per app because those apps are not
	// prepared. A valid app's discovery failure is critical: startup cannot
	// prepare it without its exact fencing token. Cancellation stops startup
	// promptly and follows the normal teardown path.
	_, discoverySpan := tracing.Start(startupCtx, "state.discovery")
	fencingTokens, rerr := persistStartupDiscovery(
		startupCtx, st, app.Dir, discovered, loadIssues, observed, logger,
	)
	if rerr != nil {
		discoverySpan.RecordError(rerr)
		discoverySpan.SetStatus(codes.Error, rerr.Error())
		discoverySpan.End()
		if startupInterrupted(startupCtx, rerr) {
			logger.Debug("Startup: state discovery interrupted by shutdown", "error", rerr)
			return startupAppSet{}, errStartupInterrupted
		}
		// Only critical discovery failures are returned; proceeding would either
		// prepare an app without fencing authority or hide an inconsistent state
		// view. Startup fails closed.
		return startupAppSet{}, fmt.Errorf("persist discovery: %w", rerr)
	}
	discoverySpan.End()

	// Seed the fresh registry with THIS worker's persisted cumulative totals so
	// the first snapshot writes them back instead of zeroing PostgreSQL. Each
	// worker restores only its own rows (stats are keyed by worker identity), so
	// two workers sharing the database never double-count. Gauges are NOT
	// restored (they are point-in-time snapshots refreshed each interval).
	_, statsRestoreSpan := tracing.Start(startupCtx, "state.restore_stats")
	restorePersistedStats(metricsInstance, st)
	statsRestoreSpan.End()

	return startupAppSet{
		loaded:        apps,
		issues:        loadIssues,
		records:       startup,
		fingerprints:  fingerprints,
		fencingTokens: fencingTokens,
	}, nil
}

// managedRuntimeBuildContext installs the app-image build observer that
// publishes the building status at the ACTUAL managed runtime image-build
// boundary. It is passed to Manager.Prepare, whose runtime fires the observer
// only when a build is really issued — after the reuse probes — so a reused
// image never flashes building, and a template that needs no runtime (Prepare is
// then a fast no-op) never fires it at all. It is nil-state-safe. Installed
// unconditionally: for a no-runtime template the observer simply never runs.
// fencingToken is the startup generation's PostgreSQL fencing token, so a build
// observed after another worker advanced the app cannot overwrite its status.
func managedRuntimeBuildContext(
	ctx context.Context,
	st *state.State,
	fn app.App,
	fencingToken state.FencingToken,
) context.Context {
	if st == nil {
		return ctx
	}
	return runtime.WithAppBuildObserver(ctx, func() {
		st.RecordReconcileBuilding(ctx, fn.Name, fencingToken)
	})
}

// startupApp is one loaded app's immutable startup identity: the
// app, the content fingerprint computed for it exactly once, and — for a
// runtime-backed app — the source selection that fingerprint was computed
// from. Carrying the selection alongside the digest lets every later startup
// stage (state writes, Manager.Prepare, the reconciler seed, and the image
// keep-set) reuse one traversal instead of re-reading /apps. It is
// worker-local on purpose: the selection is a filesystem concern the state
// package must not own, so state only ever sees the narrow
// (app, fingerprint) conversion below.
//
// Fingerprint here is the PRE-PREPARE scan: it seeds the desired state. The
// identity an image is ACTUALLY built from comes back as
// runtime.Prepared.Fingerprint from the one immutable snapshot Prepare captures,
// and that returned value is what the state phase records and the reconciler is
// seeded with.
//
// Selection is nil for a no-runtime (external-image-only) app, which builds
// no app image; its Fingerprint is then template-only by construction.
type startupApp struct {
	App         app.App
	Fingerprint string
	Selection   *source.Selection
}

// selectAndFingerprintApps computes each loaded app's fingerprint —
// and, for a runtime-backed app, resolves its source selection in the SAME
// traversal — exactly once for the whole startup. The returned records are the
// immutable startup identity reused by the state phase, Manager.Prepare, the
// reconciler seed, and the startup image keep-set. A fingerprint error is logged
// and yielded as "" with a nil selection — the same fallback the state package
// uses — so a transient read failure never blocks discovery (the empty seed then
// forces a reconcile rebuild).
func selectAndFingerprintApps(apps []app.App, logger *slog.Logger) []startupApp {
	return selectAndFingerprintAppsWith(apps, logger, app.SelectAndFingerprintApp)
}

// selectAndFingerprintAppsWith is selectAndFingerprintApps with the
// identity resolver injected. Production passes
// app.SelectAndFingerprintApp (the wrapper above); a test passes a
// counting/spying resolver so it can prove the helper resolves each loaded
// app's selection+fingerprint EXACTLY once and carries the SAME values into
// the returned records (which feed the state phase, Prepare, and the reconciler
// seed) without rehashing.
func selectAndFingerprintAppsWith(
	apps []app.App,
	logger *slog.Logger,
	resolve func(dir string, tmpl *app.Template) (*source.Selection, string, error),
) []startupApp {
	startup := make([]startupApp, 0, len(apps))
	for _, fn := range apps {
		selection, fp, err := resolve(fn.Dir, fn.Template)
		if err != nil {
			logger.Warn("App: fingerprint failed", "app", fn.Name, "error", err)
			selection, fp = nil, ""
		}
		startup = append(startup, startupApp{App: fn, Fingerprint: fp, Selection: selection})
	}
	return startup
}

// discoveredFromStartup converts the worker-local startup records to the narrow
// (app, fingerprint) pairs the state package consumes, so state never sees
// — or owns — the filesystem selection.
func discoveredFromStartup(startup []startupApp) []state.DiscoveredApp {
	discovered := make([]state.DiscoveredApp, 0, len(startup))
	for _, s := range startup {
		discovered = append(discovered, state.DiscoveredApp{App: s.App, Fingerprint: s.Fingerprint})
	}
	return discovered
}

// startupSeedFingerprints returns the seed value for each loaded app, keyed
// by name. The preferred seed is the identity the app's image was ACTUALLY
// built from (Prepared.Fingerprint); the pre-prepare scan (fingerprints) is only
// the fallback for an app that produced no built identity (an unavailable
// build, a no-runtime app that builds no image, or a hand-built test value).
//
// Seeding the BUILT identity is what makes the first reconcile compare the live
// tree against what is really baked: a source edit between the pre-prepare scan
// and the build's snapshot changes the built value, so the reconcile observes a
// rebuild instead of treating the mutated tree as already prepared. The rule is a
// pure helper so it is pinned by a deterministic unit test rather than inferred
// from Run's inline wiring.
func startupSeedFingerprints(
	apps []app.App,
	fingerprints map[string]string,
	prepared []*runner.PreparedApp,
) map[string]string {
	builtByFn := make(map[string]string, len(prepared))
	for _, pf := range prepared {
		if p := pf.Prepared(); p != nil && p.Fingerprint != "" {
			builtByFn[pf.Name()] = p.Fingerprint
		}
	}
	seeds := make(map[string]string, len(apps))
	for _, fn := range apps {
		seed := fingerprints[fn.Name]
		if built := builtByFn[fn.Name]; built != "" {
			seed = built
		}
		seeds[fn.Name] = seed
	}
	return seeds
}

// persistStartupDiscovery writes the startup state phase: it seeds a fresh
// database from the already-loaded (app, fingerprint) pairs, records each
// PRESENT-but-invalid app candidate from the loader diagnostics as invalid
// (preserving any active generation while clearing the untrustworthy desired
// snapshot), prunes rows for names that are no longer app candidates in dir
// (directory absent OR template.yaml absent), and records each valid app as
// discovered (preserving its active generation). A directory without
// template.yaml is not an app candidate and is never an issue, so a fresh
// template-less directory leaves no row, warning, or fencing token behind.
//
// Ordering matters. The invalid writes happen BEFORE the prune so a directory
// the loader observed as present but which vanished before the sweep is still
// removed by the filesystem-authoritative prune rather than resurrected as an
// invalid row — a disappearance is a removal, never an invalid desired state.
// The prune still runs BEFORE the valid discovery so an app removed while
// down is never resurrected, and the whole phase runs before
// restorePersistedStats (the caller) so a pruned app's stats row is gone
// before the fresh registry is seeded from it. Invalid apps stay out of the
// loaded set and are never passed to discovery, matching, the scheduler, or
// startup preparation — only their state view is updated. Relay-owned transient
// staging directories (app.IsReservedDir) are ignored entirely: a reserved
// issue is never recorded as an invalid desired definition, and PruneRemoved
// removes any stale reserved row a buggy prior discovery may have left, so a
// live sync's ".sync-*" directory can never create, update, or retain state.
//
// It is split out of Run as a pure seam over the state handle so the
// discovery/invalid persistence is unit-testable without Docker or Redis.
//
// Error semantics are deliberate. A VALID app's discovery write is the source
// of its fencing authority: if it fails, this call returns an error and startup
// must not prepare or publish that app, because no exact token exists for its
// guarded build/service callbacks. The same applies if a discovery write ever
// returns a non-positive token (an invariant violation). An invalid app's write
// is NOT required for startup (invalid apps are never prepared): a stale
// observation or an operational write failure for one issue is logged and
// isolated, and the remaining apps are still recorded. RebuildFromApps only
// seeds an empty database and is followed by the per-app discovery upserts, so
// its failure is logged but not fatal; a failed empty-check is never treated as
// "the database is empty". A lifecycle cancellation stops the phase promptly and
// is returned wrapped so the caller can classify it as a graceful startup
// interruption instead of a state failure.
//
// The returned map carries the EXACT fencing token each write persisted, so
// startup preparation, its build observer, the startup service callbacks, and
// the reconciler seed all use the same PostgreSQL fence instead of reading it
// back (which could claim another worker's newer generation). A zero/absent
// token means that app has no authority and must not be prepared. observations
// carries the pre-read state snapshots captured by the startup parse gate; they
// are REQUIRED for an invalid write. A missing snapshot (a failed state read) is
// never interpreted as "the row is absent": the issue write is skipped, so a
// failed read cannot bypass the optimistic concurrency guard.
func persistStartupDiscovery(
	ctx context.Context,
	st *state.State,
	dir string,
	discovered []state.DiscoveredApp,
	issues []app.LoadIssue,
	observed map[string]state.AppObservation,
	logger *slog.Logger,
) (map[string]state.FencingToken, error) {
	checkCtx := func() error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("seed state: %w", err)
		}
		return nil
	}
	if err := checkCtx(); err != nil {
		return nil, err
	}
	fencingTokens := make(map[string]state.FencingToken, len(discovered)+len(issues))
	if err := st.RebuildFromApps(ctx, discovered); err != nil {
		if startupInterrupted(ctx, err) {
			return fencingTokens, fmt.Errorf("seed state: %w", err)
		}
		// The seed only populates an empty database and every valid app is
		// upserted again below. A failed empty check is not "empty", and the
		// per-app writes still decide whether this worker has fencing authority.
		logger.Warn("State: rebuild from apps failed; continuing", "error", err)
	}
	for _, issue := range issues {
		if err := checkCtx(); err != nil {
			return fencingTokens, err
		}
		if app.IsReservedDir(issue.Name) {
			// Defense in depth: the loader already filters Relay-owned staging
			// directories, so a reserved issue should never reach here. If one
			// does (a future caller), it must not create or update state for a
			// directory that is not an app.
			continue
		}
		result, observedOK, err := recordStartupIssue(ctx, st, issue, observed)
		if !observedOK {
			// A failed observation read is not evidence of absence. Skip this
			// issue rather than writing unconditionally (which could overwrite
			// another worker's newer state). Invalid apps are never prepared, so
			// this is recoverable per-app and the next scan retries.
			logger.Warn("State: invalid desired observation unavailable; not recording",
				"app", issue.Name)
			continue
		}
		if err != nil {
			if startupInterrupted(ctx, err) {
				return fencingTokens, fmt.Errorf("record invalid desired: %w", err)
			}
			// Operational per-app failure: an invalid app is not prepared, so the
			// rest of startup is unaffected.
			logger.Warn("State: record invalid desired failed; continuing",
				"app", issue.Name, "error", err)
			continue
		}
		if result.Stale {
			// Another worker advanced this app after our pre-read snapshot. This
			// is a normal, recoverable rejection, not a PostgreSQL failure; keep
			// this app out of the token map (fail closed) and continue.
			logger.Debug("State: stale invalid desired observation rejected",
				"app", issue.Name)
			continue
		}
		if result.FencingToken <= 0 {
			logger.Warn("State: invalid desired write returned no fencing token",
				"app", issue.Name)
			continue
		}
		fencingTokens[issue.Name] = result.FencingToken
	}
	if err := checkCtx(); err != nil {
		return fencingTokens, err
	}
	if err := st.PruneRemoved(ctx, dir); err != nil {
		if startupInterrupted(ctx, err) {
			return fencingTokens, fmt.Errorf("prune removed: %w", err)
		}
		// Pruning stale rows does not grant or remove fencing authority for a
		// valid app, so it remains non-fatal at startup.
		logger.Warn("State: prune removed failed; continuing", "error", err)
	}
	for _, d := range discovered {
		if err := checkCtx(); err != nil {
			return fencingTokens, err
		}
		fencingToken, err := st.RecordDiscoveredWithFingerprint(ctx, d.App, d.Fingerprint)
		if err != nil {
			if startupInterrupted(ctx, err) {
				return fencingTokens, fmt.Errorf("record discovered: %w", err)
			}
			// Critical: a valid app without the exact token persisted by its own
			// discovery write has no safe fencing authority, so startup must not
			// proceed to prepare or publish it.
			return fencingTokens, fmt.Errorf(
				"record discovered %q: %w", d.App.Name, err)
		}
		if fencingToken <= 0 {
			return fencingTokens, fmt.Errorf(
				"record discovered %q: invalid fencing token %d", d.App.Name, fencingToken)
		}
		fencingTokens[d.App.Name] = fencingToken
	}
	if err := checkCtx(); err != nil {
		return fencingTokens, err
	}
	return fencingTokens, nil
}

// recordStartupIssue persists one loader diagnostic with the pre-read state
// observation captured by startupParseGate. observedOK is false when no
// observation exists (the state read failed): the caller must then skip the
// write instead of treating a failed read as an absent row. An issue whose
// deterministic invalid identity is already persisted takes the state layer's
// read-only fast path (no write, no token allocation); a stale observation is
// rejected rather than overwriting another worker's newer state.
func recordStartupIssue(
	ctx context.Context,
	st *state.State,
	issue app.LoadIssue,
	observed map[string]state.AppObservation,
) (state.InvalidDesiredResult, bool, error) {
	if observed == nil {
		return state.InvalidDesiredResult{}, false, nil
	}
	observation, ok := observed[issue.Name]
	if !ok {
		return state.InvalidDesiredResult{}, false, nil
	}
	result, err := st.RecordInvalidDesiredWithSource(
		ctx, issue.Name, issue.Identity, issue.Err, &observation)
	return result, true, err
}

// appPreparer is the narrow view of the runtime Manager that startup
// preparation needs: the selection-aware Prepare, the normalized build cap that
// sizes the bounded startup worker pool, plus (via runner.Executor) the Execute
// used to pair each returned handle with its executor for the runner.
// *runtime.Manager satisfies it; a test spy can implement it, so the startup
// handoff (the resolved fingerprint+selection must reach Prepare, not be
// recomputed) is unit-testable without Docker. prepareApps calls Prepare
// concurrently for distinct apps, so implementations must be safe for
// concurrent use (the Manager already is).
type appPreparer interface {
	PrepareWithFingerprintAndSelection(
		ctx context.Context,
		fn app.App,
		fingerprint string,
		selection *source.Selection,
	) (*runtime.Prepared, error)
	// MaxConcurrentBuilds is the effective MAX_CONCURRENT_BUILDS cap, including
	// its default normalization for a directly constructed Manager.
	MaxConcurrentBuilds() int
	runner.Executor
}

// prepareApps builds each app's image and returns the prepared set. An
// app whose image cannot be built is marked unavailable (the runner skips
// it) rather than failing startup; the rest carry their fresh image.
// Preparations run in a bounded worker pool sized by the runtime Manager's
// effective MAX_CONCURRENT_BUILDS, so startup never launches one goroutine per
// app. The Manager's single shared limiter still bounds actual image builds
// (non-build snapshot/probe work takes no slot), and the returned set remains in
// startup order. Jobs are assigned incrementally: each worker takes the next app
// as soon as it finishes the previous one, with no batch barrier. A cancelled
// lifecycle stops assigning new work and joins the active workers before
// returning, as startup preparation must complete before service enqueue and
// runner construction. It receives
// the startup records — each app with the fingerprint resolved exactly once
// and (for a runtime-backed app) the source selection that fingerprint was
// derived from — and supplies BOTH to the selection-aware Prepare. Prepare
// itself captures one immutable snapshot of the selected source and derives the
// build tag, the staged context, and the returned identity from it, so the built
// generation is coherent even if the tree mutates during the build; the startup
// scan is the input that says "a rebuild may be needed", never the label.
//
// The persisted record uses the RETURNED identity (prep.Fingerprint), so the
// state DB always describes the content the image actually serves. A source edit
// during the build is not in the image and is detected by the reconciler's
// watcher/periodic audit on the next pass, exactly as in the live reconcile path.
//
// The building status is published at the ACTUAL managed runtime image-build
// boundary via the observer installed by managedRuntimeBuildContext: a reused
// image and a no-runtime template never flash building, while a real build does.
// A successful no-service preparation reaches ready below; an app whose
// template declares services records its terminal outcome after service
// convergence (services have no separate image build, so building is
// published only for the managed-runtime image build itself), and a preparation
// failure uses the existing failure semantics (unavailable without an active
// image, degraded when a previous image is retained).
//
// fencingTokens carries the exact fencing tokens the startup discovery writes
// returned, keyed by app name; the build observer, failure, and success writes
// are all conditioned on them so a preparation superseded by another worker
// cannot overwrite the newer desired state. When a state sink is configured
// (st != nil), an app without a positive token is NOT prepared: startup fails
// closed rather than building/publishing an app whose guarded state writes have
// no authority. The discovery phase already treats such a write as fatal; this
// is the defense-in-depth check at the handoff boundary.
//
// ctx is the worker lifecycle context. It is passed to Prepare so a build (and
// the fast reuse probes) is cancelled on shutdown; Prepare itself roots the
// Dockerfile build in the manager lifecycle with its own 10m buildTimeout, so
// this context's lack of a short deadline is intentional and the build is never
// bounded by the 30s reconcileTimeout.
func prepareApps(
	ctx context.Context,
	manager appPreparer,
	startup []startupApp,
	st *state.State,
	fencingTokens map[string]state.FencingToken,
	logger *slog.Logger,
) []*runner.PreparedApp {
	// A small fixed worker pool, sized by the Manager's effective
	// MAX_CONCURRENT_BUILDS, takes jobs incrementally from an unbuffered channel:
	// when any worker finishes, it receives the next app. This keeps startup
	// concurrency bounded without a second semaphore, while the Manager's shared
	// limiter still bounds the actual builds. Slots are indexed by startup order
	// so the returned registry preserves that order regardless of completion
	// order. Cancellation is honored while waiting to hand out a job: the
	// scheduler then stops assigning and the workers finish only what was
	// already admitted.
	slots := make([]*runner.PreparedApp, len(startup))
	workers := manager.MaxConcurrentBuilds()
	if workers < 1 {
		workers = 1
	}
	if workers > len(startup) {
		workers = len(startup)
	}
	type prepareJob struct {
		index        int
		app          startupApp
		fencingToken state.FencingToken
	}
	jobs := make(chan prepareJob)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				slots[job.index] = prepareStartupApp(
					ctx, manager, job.app, st, job.fencingToken, logger,
				)
			}
		}()
	}

	stoppedAt := len(startup)
schedule:
	for i, s := range startup {
		if err := ctx.Err(); err != nil {
			// Stop ASSIGNING new preparations once the lifecycle is cancelled;
			// already-assigned ones are joined below. The remaining apps are
			// marked unavailable (the same terminal state a cancelled prepare
			// produces) without being built or written to state.
			stoppedAt = i
			logger.Debug("Startup: app preparation stopped by shutdown",
				"app", s.App.Name, "error", err)
			break
		}
		fn := s.App
		fencingToken := fencingTokens[fn.Name]
		if st != nil && fencingToken <= 0 {
			// Fail closed: without the exact token from this app's discovery
			// write there is no fencing authority for its build/service
			// callbacks. Do not build, publish, or admit it to the registry. A
			// later reconcile retries it with a freshly persisted generation.
			logger.Error(
				"App: startup preparation skipped without fencing token",
				"app", fn.Name,
			)
			continue
		}
		select {
		case jobs <- prepareJob{index: i, app: s, fencingToken: fencingToken}:
		case <-ctx.Done():
			stoppedAt = i
			logger.Debug("Startup: app preparation stopped by shutdown",
				"app", fn.Name, "error", ctx.Err())
			break schedule
		}
	}
	close(jobs)
	wg.Wait()
	// Every worker has joined. Apps that were never assigned because of the
	// cancellation are reported unavailable, in startup order, but are not
	// prepared or persisted.
	for i := stoppedAt; i < len(slots); i++ {
		slots[i] = runner.NewUnavailable(startup[i].App)
	}

	prepared := make([]*runner.PreparedApp, 0, len(slots))
	preparedCount := 0
	for _, pf := range slots {
		if pf == nil {
			continue
		}
		prepared = append(prepared, pf)
		if pf.Prepared() != nil {
			preparedCount++
		}
	}
	logger.Info("Prepared apps", "count", preparedCount)
	return prepared
}

// prepareStartupApp prepares ONE startup app and records its terminal state.
// It is the per-app unit run concurrently by prepareApps: all per-app failure
// semantics (unavailable without an active image, the interruption
// classification that suppresses a spurious state failure, and the
// building/ready observations) are unchanged. The exact startup fencing token
// for this app is used for every guarded write.
func prepareStartupApp(
	ctx context.Context,
	manager appPreparer,
	s startupApp,
	st *state.State,
	fencingToken state.FencingToken,
	logger *slog.Logger,
) *runner.PreparedApp {
	fn := s.App
	prep, err := manager.PrepareWithFingerprintAndSelection(
		managedRuntimeBuildContext(ctx, st, fn, fencingToken), fn, s.Fingerprint, s.Selection,
	)
	if err != nil {
		// A build cancelled by the lifecycle is a shutdown, not a build
		// failure: it must not record a spurious reconcile failure in the
		// state DB. The app is still marked unavailable, but Run is
		// already converging on shutdown.
		if startupInterrupted(ctx, err) {
			logger.Debug("App: prepare interrupted by shutdown", "app", fn.Name, "error", err)
		} else {
			logger.Warn("App: prepare failed", "app", fn.Name, "error", err)
			if st != nil {
				st.RecordReconcileFailure(ctx, fn.Name, err, fencingToken)
			}
		}
		return runner.NewUnavailable(fn)
	}
	if st != nil && len(fn.Template.Services) == 0 {
		// Persist the fingerprint the image was ACTUALLY built from
		// (prep.Fingerprint, derived from the one immutable source snapshot the
		// build staged), never a later live rescan. Recording a post-build scan
		// would claim the image serves content it does not: a source edit during
		// a long build is not in the image, and the reconciler's watcher/periodic
		// audit detects it by comparing the on-disk digest against this recorded
		// built identity on the next pass.
		st.RecordReconcileSuccess(ctx, fn.Name, prep.Image, prep.Fingerprint, time.Now(), fn, fencingToken)
	}
	return runner.NewPrepared(fn, prep, manager)
}
