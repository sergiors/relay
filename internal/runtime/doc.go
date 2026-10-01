// Package runtime executes Relay apps in containers.
//
// This package owns the Docker (moby) client lifecycle and the per-invocation
// container lifecycle, turning app specs into runnable images:
//   - Dispatch: a runtime spec maps to an engine, which produces a build plan
//     (never a Dockerfile)
//   - Build: one image per app via a single generic Dockerfile renderer;
//     a declared dependency layer (plan.Deps) is built once as a content-
//     addressed, shared `relay-dep-*` base image that the app image builds
//     FROM, so unchanged dependency manifests are reused across source changes
//   - Build bounds: a Dockerfile build (app or dependency image) runs on
//     an independent buildTimeout (10m) rooted in the manager
//     lifecycle, NOT in the caller's context. In production the reconciler and
//     startup preparation pass the worker lifecycle context to Prepare (a seam
//     embedders and tests may call with any context), and a caller's deadline
//     must never cut off a legitimate image build; the lifecycle root keeps
//     builds cancellable on Relay shutdown.
//   - Execute: a leased container from the app's warm pool (bounded by the
//     app's resolved concurrency), event JSON on stdin, stdout/stderr
//     forwarded verbatim to the process output sink as a raw transport (not
//     slog); healthy idle containers are evicted by a single maintenance loop
//     once idle longer than the configured timeout, and a removed app's
//     warm state is discarded (see container_cache.go). NewManager starts that
//     loop eagerly; WithDeferredMaintenance suppresses it so a caller that must
//     verify its own prerequisites first (the worker's NETWORKS preflight)
//     starts it explicitly with StartMaintenance, and Close is safe either way.
//
// Env and secrets injection: an execution container's Docker Config.Env is only
// the app's plan env (runtime needs, e.g. PYTHONDONTWRITEBYTECODE) — never
// the template's literal env values and never a resolved secret. Per-invocation
// values (template env + resolved secrets) are carried in the request frame's
// "env" object and applied by the reused bootstrap process (python/ and node/),
// resolved by the runner immediately before each execution. They are never in
// Prepared, never in the fingerprint, never in the image, never in a Docker
// label, metric, log, or trace, and never in the state database. template.yaml
// is excluded from the build context so env values and secret references are
// never baked into image layers.
//
// Persistent service containers are the deliberate exception: a long-lived
// service process needs its environment at process START, so StartService
// writes the effective env (plan env + template env + resolved secrets + PORT)
// into the service container's Docker Config.Env. Those values are therefore
// readable through the Docker daemon/API (docker inspect, the Docker socket) by
// anyone already trusted with daemon access, which is the same trust boundary as
// the host itself. Relay never writes any value into a Docker label, log line,
// metric, or span; only the one-way relay.env_hash digest is labeled.
//
// The build context stages exactly the app's selected source via the shared
// internal/source policy (the app's .gitignore rules), the same selection
// the fingerprint uses: an ignored file is neither hashed nor baked into a layer.
//
// Key Features:
//   - A single reused Docker Engine client for every build and invocation
//   - Engines (python, node) answer "what does this runtime need?" as plan data;
//     this package answers "how do I build it?" and knows nothing about Python
//     imports or Node module resolution. Engines declare an app's reusable
//     dependency layer (manifest files + install command) via plan.Deps and any
//     external tool they need via plan.Spec.ToolCopies (the pinned uv binary for
//     Python); the renderer emits the COPY --from and the dependency image
//     inherits the tool. The dependency images and their content-addressed
//     `relay-dep-*` references live entirely here. The dependency fingerprint
//     keys on the base image TAG, not its digest (a documented limitation).
//   - The package knows nothing about matching or Redis
//
// Security baseline: every execution container is hardened. It runs as a
// non-root user created at build time (the engines set the plan's User/UserSetup),
// drops all Linux capabilities, is memory/CPU/pids-limited, has a read-only
// rootfs, and gets a bounded /tmp tmpfs as its only writable path. These are
// internal defaults, not configuration. Networking stays enabled (outbound
// access is a legitimate app need; network policy is a documented residual
// limitation). The app code is unaware of all of this: the hardening is
// applied entirely by the engines and the container runner.
//
// Usage: NewManager, then Prepare per app, then Execute per invocation.
package runtime
