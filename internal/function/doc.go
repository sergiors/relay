// Package function is the pure decision layer for Relay functions.
//
// This package reads the /functions root and models what the runtime needs:
//   - Discovery: each direct subdirectory is a function with a template.yaml
//   - Validation: templates are parsed and validated, including per-rule
//     timeout resolution and handler form
//   - Matching: rules pair handlers with patterns evaluated against events
//   - Fingerprinting: a deterministic hash of a function's contents gates
//     reconciler rebuilds
//
// Key Features:
//   - A rule that omits a timeout resolves to DefaultTimeout; zero, negative,
//     unparseable, or over-MaxTimeout values are rejected. MaxTimeout caps every
//     rule's handler timeout; it bounds the running deadline an invocation may
//     persist (stream layer) and the message-reclaim backstop derived from it.
//   - A rule that omits `retries` resolves to DefaultRetries (4): the number of
//     additional executions attempted after the initial one, so a failing
//     invocation is attempted 1 + Retries times in total. `retries` must be a
//     non-negative integer; a negative or non-integer value (e.g. "abc", "1.5")
//     fails template validation. `retries: 0` is valid (only the initial
//     attempt). The per-invocation attempt count and retry backoff are owned by
//     the stream/runner layers, not here.
//
// Env and secrets:
//   - `env` maps an env-var name to a literal string value, injected into every
//     execution container at runtime. Values are literal — never masked, never
//     treated as secret-looking. Empty values are allowed (flag-like variables).
//   - `secrets` maps an env-var name to a secret reference name (a SecretRef).
//     The reference is resolved to a value immediately before each execution by
//     the runner; the value never lives in this package, in the image, or in the
//     fingerprint. A variable may not be defined in both `env` and `secrets`.
//   - Env-var names must match [A-Za-z_][A-Za-z0-9_]*; secret references must
//     be valid secret names (see ValidSecretName). All validation errors are
//     value-free.
//
// The package has no side effects beyond reading the filesystem; building,
// execution, and Redis are owned elsewhere.
package function
