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
// Usage: New, then Seed the startup functions, then Start.
//
// When Config.State is set, reconcile outcomes are recorded into the local
// state database (a read-only state view) — success, failure (retaining the
// prior active version), unchanged-skip, and removal. State writes never drive
// decisions and never fail the reconcile loop; errors are only logged.
//
// The package does NOT build images; it delegates image preparation and
// invocation to a Builder.
package reconciler
