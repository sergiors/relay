// Package runtime executes Relay functions in containers.
//
// This package owns the Docker (moby) client lifecycle and the per-invocation
// container lifecycle, turning function specs into runnable images:
//   - Dispatch: a runtime spec maps to an engine, which produces a build plan
//     (never a Dockerfile)
//   - Build: one image per function via a single generic Dockerfile renderer
//   - Execute: one disposable container per handler invocation, event JSON on
//     stdin
//
// Key Features:
//   - A single reused Docker Engine client for every build and invocation
//   - Engines (python, node) answer "what does this runtime need?" as plan data;
//     this package answers "how do I build it?" and knows nothing about Python
//     imports or Node module resolution
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
