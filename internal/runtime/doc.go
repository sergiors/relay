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
//   - Dependency-image coordination: a dependency layer is shared daemon-wide
//     by every app version with the same manifest, so a Manager needs a
//     cross-process authority (WithDependencyImageCoordinator, wired by the
//     worker from the shared PostgreSQL state database). A Prepare that must
//     build from a dependency holds a SHARED use lease before the dependency
//     probe through the app-image build that consumes it via FROM, revalidating
//     before and after. Dependency GC takes a non-blocking EXCLUSIVE lease per
//     candidate and re-reads the daemon's label-derived referenced set before
//     removing. The daemon is not a parent-reference guard: force-free removal
//     can untag a layer a completed child image still references. Coordination,
//     not a Docker refusal, is authoritative. A Manager with no coordinator
//     fails closed for dependency-image builds and GC; there is no manager-local
//     fallback because a process-local lock cannot protect a shared Docker image.
//   - Runtime image pins (live periodic refresh): WithRuntimeImageSelections
//     supplies the startup snapshot read from the shared state database, and
//     WithRuntimeImageSelectionStore supplies the authoritative source the
//     reconciler refreshes once per periodic pass. A pinned runtime must name a
//     known runtime, its exact configured Spec.BaseImage, and a
//     "sha256:<64 lowercase hex>" multi-platform index digest; the Manager
//     rejects a malformed/mismatched entry for a known runtime, keeps its
//     last-known-good snapshot, and ignores unknown runtime IDs. Prepare captures
//     one selection per app (Prepared.ImageSelection, distinct from the
//     source-only Prepared.Fingerprint) and builds FROM
//     configured-ref@digest (no registry/child-digest lookup), folding the
//     immutable identity into dependency fingerprints, app image identities
//     (baked with/without deps, HOT_RELOAD, and zuvloop modes), and provenance
//     labels. Publication — including the initial startup registry population —
//     revalidates that captured identity against the authoritative row under the
//     same per-runtime advisory lock every selection CAS write takes, and only an
//     accepted generation activates warm state. A stale generation is discarded
//     with its leases released; the old active version stays serving (or the app
//     gets an unavailable placeholder that still matches events until a refreshed
//     reconcile retries). A missing row is UNPINNED and preserves the historical
//     byte-for-byte behavior. See state and docs/operations.md.
//   - Source mount (HOT_RELOAD): when enabled, a runtime whose dependency
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
//     fingerprint recycles the app's warm execution containers and restarts its
//     entrypoint service containers in place (same container ID, same image,
//     same bind; see internal/reconciler). HOT_RELOAD=false is the historical
//     baked-source behavior. The bind source is normally the app dir (fn.Dir), which the
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
// the host itself. The relay.environment label stores an environment fingerprint
// for reconciliation; it is not a confidentiality boundary.
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
//   - One Manager-owned Docker operation lifecycle (operations.go) admits,
//     tracks, cancels, and drains every Manager-owned Docker call — preparation
//     (reuse probes, self-mount inspect, build), service start/stop/list, orphan
//     sweeps, image inspect/pull/listing/removal, readiness Ping,
//     execution-container create, and every execution-container teardown and
//     exit-wait watcher. Manager shutdown closes normal admission atomically,
//     cancels every in-flight operation's context, runs teardown as a tracked
//     shutdown-owned phase, and drains all of them before the Docker client
//     closes, so the client is never closed beneath a live call.
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
//     `relay-dep-*` references live entirely here. An UNPINNED dependency
//     fingerprint keys on the base image TAG, not its digest (a documented
//     limitation); a pinned runtime hashes the selected ref@digest identity and
//     is not exposed to that tag-movement limitation.
//   - The package knows nothing about matching or Redis
//
// Security baseline: every execution and service container is hardened. It runs
// as a non-root user created at build time (the engines set the plan's
// User/UserSetup; external images keep their own USER), drops all Linux
// capabilities, enables no-new-privileges while preserving Docker's default
// seccomp profile, uses private PID/IPC/network namespaces, is
// memory/CPU/pids-limited, has a read-only rootfs, and gets a bounded /tmp tmpfs
// as its baseline writable path. A service declaring a port below 1024 adds only
// NET_BIND_SERVICE. A persistent service may declare additional bounded,
// nosuid,nodev,noexec tmpfs destinations (validated against the same narrow path
// policy as template parsing); each target and existing parent component is
// proven from the immutable image layers to be a non-symlink directory before
// create, because Docker follows rootfs symlinks when applying HostConfig.Tmpfs.
// Execution containers never add tmpfs destinations. A network labelled
// relay.control, and the reserved host /
// container:<id> pseudo-networks, are refused before create. Mounts are limited
// to the read-only HOT_RELOAD source bind and its read-only tmpfs masks; a
// container-runtime socket mount is rejected. An image whose Config declares
// Docker VOLUME destinations is refused before create, because the daemon would
// materialize them as implicit anonymous writable mounts outside the bounded
// /tmp contract; a service volume is allowed only when an explicitly configured
// tmpfs covers that exact destination. These are internal defaults, not
// configuration. Outbound
// networking stays enabled (a legitimate app need;
// network policy beyond control-plane isolation is a documented residual
// limitation). The app code is unaware of all of this: the hardening is applied
// entirely by the engines and the container runner.
//
// Usage: NewManager, then Prepare per app, then Execute per invocation.
package runtime
