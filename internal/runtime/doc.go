// Package runtime executes Relay functions in containers.
//
// This package owns the Docker (moby) client lifecycle and the per-invocation
// container lifecycle, turning function specs into runnable images:
//   - Dispatch: a runtime spec maps to an engine, which produces a build plan
//     (never a Dockerfile)
//   - Build: one image per function via a single generic Dockerfile renderer;
//     a declared dependency layer (plan.Deps) is built once as a content-
//     addressed, shared `relay-dep-*` base image that the function image builds
//     FROM, so unchanged dependency manifests are reused across source changes
//   - Execute: a leased container from the function's warm pool (bounded by the
//     function's resolved concurrency), event JSON on stdin, stdout/stderr
//     forwarded verbatim to the process output sink as a raw transport (not
//     slog); healthy idle containers are evicted by a single maintenance loop
//     once idle longer than the configured timeout, and a removed function's
//     warm state is discarded (see container_cache.go)
//
// Env and secrets injection: each execution container's environment is the base
// RELAY_HANDLER var, then the function's plan env (runtime needs), then the
// per-invocation extra env (the template's literal env values and resolved
// secret values, passed to Execute). Secret values are resolved by the runner
// immediately before each execution and live only in the container's Config.Env
// — never in Prepared, never in the fingerprint, never in the image, and never
// in the state database. template.yaml is excluded from the build context so env
// values and secret references are never baked into image layers.
//
// Key Features:
//   - A single reused Docker Engine client for every build and invocation
//   - Engines (python, node) answer "what does this runtime need?" as plan data;
//     this package answers "how do I build it?" and knows nothing about Python
//     imports or Node module resolution. Engines declare a function's reusable
//     dependency layer (manifest files + install command) via plan.Deps; the
//     dependency images and their content-addressed `relay-dep-*` references
//     live entirely here. The dependency fingerprint keys on the base image
//     TAG, not its digest (a documented limitation).
//   - The package knows nothing about matching or Redis
//
// Security baseline: every execution container is hardened. It runs as a
// non-root user created at build time (the engines set the plan's User/UserSetup),
// drops all Linux capabilities, is memory/CPU/pids-limited, has a read-only
// rootfs, and gets a bounded /tmp tmpfs as its only writable path. These are
// internal defaults, not configuration. Networking stays enabled (outbound
// access is a legitimate function need; network policy is a documented residual
// limitation). The function code is unaware of all of this: the hardening is
// applied entirely by the engines and the container runner.
//
// Usage: NewManager, then Prepare per function, then Execute per invocation.
package runtime
