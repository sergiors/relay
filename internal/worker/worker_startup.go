package worker

import (
	"context"
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
	// tokens carries the PostgreSQL transition token persisted for each app's
	// startup desired generation, keyed by app name. Startup preparation, its
	// build observer, the startup service callbacks, and the reconciler seed all
	// reuse these exact values instead of reading apps.generation back.
	tokens map[string]state.Token
}

// loadStartupApps loads /apps, resolves each app's fingerprint and source
// selection exactly once, persists the startup discovery view, and restores
// this worker's persisted stats into the fresh registry.
//
// The startup records carry both the fingerprint and the source selection the
// fingerprint was computed from, so no later stage re-reads /apps or recomputes
// a fingerprint. The state DB receives only the narrow (app, fingerprint)
// discovery pairs; the selection stays worker-local because it is a filesystem
// concern the state package must not own. Discovery persistence is non-fatal:
// the app set is already loaded and serving, so a write failure is recorded on
// its span and startup continues. state.Open already ran before this phase and
// is fatal on failure, so no fingerprint or discovery work happens against a
// broken state database; the state DB is a persisted local view, never the
// source of truth for matching, builds, or reconciliation.
func loadStartupApps(
	startupCtx context.Context,
	logger *slog.Logger,
	st *state.State,
	metricsInstance *metrics.Registry,
) (startupAppSet, error) {
	_, loaderSpan := tracing.Start(startupCtx, "apps.load")
	loader := app.NewLoader(app.Dir, logger)
	apps, loadIssues, err := loader.LoadWithDiagnostics()
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
	// runtime registry, matching, the scheduler, or preparation. A write failure
	// is logged (non-fatal): the app set is already loaded and serving.
	_, discoverySpan := tracing.Start(startupCtx, "state.discovery")
	tokens, rerr := persistStartupDiscovery(
		st, app.Dir, discovered, loadIssues, logger,
	)
	if rerr != nil {
		discoverySpan.RecordError(rerr)
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
		loaded:       apps,
		issues:       loadIssues,
		records:      startup,
		fingerprints: fingerprints,
		tokens:       tokens,
	}, nil
}
