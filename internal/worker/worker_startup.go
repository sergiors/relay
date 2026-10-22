package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/codes"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
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
