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
//   - Source mount (SOURCE_MOUNT): when enabled, a runtime whose dependency
//     layout permits it (Python and Node; see plan.Spec.MountableSource) builds
//     the app image WITHOUT the source and bind-mounts the app's live directory
//     read-only at the runtime's source root. Python mounts at the workdir
//     (/app), where its system site-packages live outside the mount; Node mounts
//     at a distinct subdirectory (/app/src) so /app/node_modules and its
//     persisted pinned esbuild stay visible, and its bootstrap bundles any
//     TypeScript handler at container startup with that same esbuild. A
//     source-mounted Node image also carries a shared resolve hook
//     (/relay/resolve-hook.mjs) that the invocation bootstrap imports and a
//     mounted Node entrypoint service preloads with `node --import`, so a host
//     node_modules cannot shadow the dependency image (for an ESM import or a
//     CommonJS require) even when it appears after
//     preparation (and therefore carries no mask). A
//     source-only edit then reuses the same image while the advanced source
//     fingerprint recycles the app's warm containers and replaces its entrypoint
//     service replicas. SOURCE_MOUNT=false is the historical baked-source
//     behavior. The bind source is normally the app dir (fn.Dir), which the
//     Docker daemon resolves on its own host filesystem; when Relay itself runs
//     in a container on that daemon (the bundled Compose layout) and the app dir
//     lies under one of Relay's own bind mounts, Relay inspects its own container
//     and passes that mount's host Source plus the relative path instead, so the
//     daemon binds the same live tree read-only. See source_mount.go.
//   - Execute: a leased container from the app's warm pool (bounded by the
//     app's resolved concurrency, and by the worker-global MAX_WARM_CONTAINERS
//     bound across all apps), event JSON on stdin, stdout/stderr
//     forwarded verbatim to the process output sink as a raw transport (not
//     slog); healthy idle containers are evicted by a single maintenance loop
//     once idle longer than the configured timeout, and a removed app's
//     warm state is discarded (see container_cache.go and warm_budget.go).
//     NewManager starts that
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
// Build resource use does not scale with the source tree's total size, but it is
// not a host-wide memory bound. The selected source is captured to a private
// on-disk snapshot (which IS the build context) and fingerprinted in one pass;
// ordinary source bodies and dependency manifests are staged and hashed with
// bounded buffers and file metadata rather than held in memory, and the context
// tar is streamed to the daemon through a pipe, so the whole tar is never
// buffered. The one exception is the app's top-level template.yaml, which is read
// into memory because stripping its resources section for the fingerprint
// requires the whole YAML document; one template document may use memory
// proportional to that document. There is deliberately NO MAX_BUILD_CONTEXT_BYTES:
// the worker's heap stays bounded independently of the total context size (disk
// still scales with the context), but that is not a host-wide limit. A failed
// build's output diagnostic is retained up to 1 MiB (the remainder is drained and
// discarded, and marked [build output truncated]); while reading the daemon's
// JSON message stream, one decoded JSON message value (a stream or an error
// message string) is transiently materialized before the cap is applied, so peak
// memory for that message tracks the message size, while the retained diagnostic
// stays at 1 MiB plus the fixed marker.
//
// Key Features:
//   - A single reused Docker Engine client for every build and invocation
//   - Engines (python, node) answer "what does this runtime need?" as plan data;
//     this package answers "how do I build it?" and knows nothing about Python
//     imports or Node module resolution. Engines declare an app's reusable
//     dependency layer (manifest files + install command) via plan.Deps and any
//     external tools the runtime requires via plan.Spec.RuntimeTools (the
//     pinned uv binary for Python and the pinned pnpm standalone binary for
//     Node, the latter acquired as a checksum-pinned per-architecture remote
//     archive); the
//     renderer materializes each tool (a COPY --from for an image copy, or a
//     download/verify/extract RUN for a remote archive) and the dependency
//     image inherits the tool. The dependency images
//     and their content-addressed
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
