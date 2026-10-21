// Package reconciler watches the apps root and swaps app images live.
//
// This package drives live reload while a Builder (satisfied by the runtime
// Manager) does the image build:
//   - Watch: recursive fsnotify on the apps root; an atomically replaced app
//     directory is re-registered on its current tree (old detached-inode watches
//     are released first), and symlinks are not followed
//   - Debounce: per-app timers coalesce edit storms into one reconcile
//   - Rebuild: a changed fingerprint gates a per-app rebuild, committed to
//     the runner's registry as an atomic swap
//   - Fallback: a failed build or invalid template retains the previous version;
//     a periodic pass retries and catches events the watcher missed
//
// Key Guarantees:
//   - A healthy app is never replaced until its replacement is ready
//   - A previously-failed (unavailable) build is retried when its fingerprint
//     is stable, so a broken app recovers without further edits
//   - Event-loss safety during preparation: at the earliest safe point after a
//     template parses (before the source fingerprint walk and image build) the
//     desired generation's event rules are published into the registry as a
//     PENDING, non-runnable entry. Matching is registry-only, so without this a
//     delivery arriving during a brand-new app's first build (or an existing
//     app's rebuild to new rules) would match nothing and be ACKed as unmatched.
//     A pending match is matched-but-unavailable: it holds the message pending
//     and never executes, claims a TryStart, exhausts, ACKs, or DLQs. The active
//     generation is retained throughout, so v1 rules stay executable while v2 is
//     prepared; a build failure retains both v1 and the v2 pending rules; success
//     installs the new active generation and clears the pending entry atomically
//     (Registry.Replace). An invalid or removed desired definition, an unchanged
//     no-build path, and an unavailable-placeholder reconcile all clear pending
//     so no removed rule keeps gating events.
//   - Live/periodic reload applies the SAME discovery path policy as startup
//     (app.LoadSingle): a legal single-element name, a real direct-child
//     directory of the root, never a symlink, and a template.yaml present. An
//     invalid path (bad name, symlink — even to an in-root or outside target,
//     non-directory) RETAINS the previously-loaded app rather than removing or
//     replacing it. A truly missing directory is a removal, and so is a known
//     app whose directory remains but no longer contains template.yaml: a
//     template-less directory is not an app candidate, so it follows the single
//     remove lifecycle. An unknown template-less directory (one that was never
//     an app) is ignored with no warning and no state write.
//   - Relay-owned transient staging directories (app.IsReservedDir, e.g.
//     git's ".sync-*" created directly under the apps root during
//     materialization) are never apps: their events and periodic entries
//     are filtered before a debounce/reconcile is armed, so a live sync cannot
//     surface a warning, a state write, or a degraded/unavailable transition for
//     a directory that is merely mid-copy.
//   - The watcher arms a per-app debounce only for a directory that currently
//     has a template.yaml or for a name this worker already knows (registry,
//     pending rules, or a startup fencing token). Unrelated writes in a
//     never-app directory therefore cause no reconcile churn, while template
//     creation, deletion, and rename stay responsive.
//
// Usage: New, then PrepareWatch (establish change detection synchronously),
// then Seed each startup app with its already-computed fingerprint, then
// Start (which reuses the watcher). Establishing the watch BEFORE seeding is
// what makes a supplied seed safe: a change after the watch is observed as an
// event, and a change before it is caught by the first reconcile's rescan
// because the seeded value is older.
//
// When Config.State is set, reconcile outcomes are recorded into the local
// state database (a persisted, read-mostly state view) — success, failure
// (retaining the prior active version), unchanged-skip, removal, and invalid
// desired definitions. A present-but-invalid desired definition (an invalid
// path or an invalid/unreadable template) records a failed view via
// State.RecordInvalidDesired while preserving the previous active generation and
// leaving the runtime registry untouched; it is never treated as a removal. A
// genuinely missing directory, or a known app whose template.yaml disappeared,
// instead removes the entry through the same lifecycle. A directory that never
// had a template is not an app and is ignored entirely. The database is never
// the source of truth: the worker also writes it (startup discovery, the
// periodic stats flush), and state writes never drive reconcile decisions and
// never fail the reconcile loop; errors are only logged.
//
// When Config.UpdateSchedules is set, it is called after an app's new
// version is swapped into the registry (discovery and update paths only, never
// the skip path or a failed build) so the scheduler can converge its cron jobs
// to the template's schedules. Removal converges via RemoveApp instead.
//
// When Config.Audit is set, it is called once per periodic audit pass before
// that pass reconciles the apps. It is the re-drive point for DEFERRED image
// cleanup: an image whose bounded removal retries were exhausted (a persistent
// service container still referenced it) or whose removal failed is owned by the
// runner as a retained retirement, and the periodic pass retries it once the
// reference state may have changed. It is deliberately attached to the existing
// audit cycle — not a separate scheduler, and never a timer or goroutine per
// image.
//
// The service reconciler (services.go) is part of this package. Service
// convergence is driven by the Config hooks UpdateServices/RemoveServices (both
// nil-safe), which the worker wires to a *ServiceReconciler constructed from the
// runtime Manager's Docker seam. The worker also wires its stable WORKER_ID
// (WithWorkerID), so that reconciler only converges, adopts, removes, and sweeps
// containers stamped with its own relay.hostname: a restart of the SAME identity
// still adopts its running services, while another worker's containers on a
// shared daemon are never touched. A service's source (entrypoint or image) is
// resolved to a runnable image BEFORE any container action; a source that cannot
// be resolved leaves the service's existing healthy containers untouched. Under
// HOT_RELOAD a source-only change to a source-mounted entrypoint service is
// applied by an in-place restart (the same Docker container, same image, same
// bind) rather than a replace; the applied source fingerprint is tracked in
// memory because the container's relay.source label is immutable. A dependency
// or configuration change still replaces the container through the normal
// start-before-stop path.
//
// Reconcile owns its timeouts: it receives the LIFECYCLE context (the worker
// passes its signal context, never a pass-wide short deadline) and a reconcile
// timeout injected into the *ServiceReconciler, and derives a FRESH bound for
// each normal Docker operation itself. Lifecycle cancellation still cancels
// normal operations promptly.
//
// Ordering on a rebuild: the new version is prepared and swapped in, schedules
// converge, persistent services converge to the new image, and only THEN is the
// superseded image retired (via the Config.Retire hook). Retiring after service
// converge ensures a service container still running on the old image is
// replaced first, so the old image becomes removable; the runner's reference
// guard is a second line of defense for partial failures.
//
// The package does NOT build images; it delegates image preparation and
// invocation to a Builder.
package reconciler
