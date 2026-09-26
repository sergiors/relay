// Package reconciler watches the functions root and swaps function images live.
//
// This package drives live reload while a Builder (satisfied by the runtime
// Manager) does the image build:
//   - Watch: recursive fsnotify on the functions root; symlinks are not followed
//   - Debounce: per-function timers coalesce edit storms into one reconcile
//   - Rebuild: a changed fingerprint gates a per-function rebuild, committed to
//     the runner's registry as an atomic swap
//   - Fallback: a failed build or invalid template retains the previous version;
//     a periodic pass retries and catches events the watcher missed
//
// Key Guarantees:
//   - A healthy function is never replaced until its replacement is ready
//   - A previously-failed (unavailable) build is retried when its fingerprint
//     is stable, so a broken function recovers without further edits
//
// Usage: New, then PrepareWatch (establish change detection synchronously),
// then Seed each startup function with its already-computed fingerprint, then
// Start (which reuses the watcher). Establishing the watch BEFORE seeding is
// what makes a supplied seed safe: a change after the watch is observed as an
// event, and a change before it is caught by the first reconcile's rescan
// because the seeded value is older.
//
// When Config.State is set, reconcile outcomes are recorded into the local
// state database (a read-only state view) — success, failure (retaining the
// prior active version), unchanged-skip, and removal. State writes never drive
// decisions and never fail the reconcile loop; errors are only logged.
//
// When Config.UpdateSchedules is set, it is called after a function's new
// version is swapped into the registry (discovery and update paths only, never
// the skip path or a failed build) so the scheduler can converge its cron jobs
// to the template's schedules. Removal converges via RemoveFunction instead.
//
// The service reconciler (services.go) is part of this package. Service
// convergence is driven by the Config hooks UpdateServices/RemoveServices (both
// nil-safe), which the worker wires to a *ServiceReconciler constructed from the
// runtime Manager's Docker seam. A service's source (entrypoint or image) is
// resolved to a runnable image BEFORE any container action; a source that cannot
// be resolved leaves the service's existing healthy containers untouched.
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
