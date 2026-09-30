// Package function is the pure decision layer for Relay functions.
//
// This package reads the /functions root and models what the runtime needs:
//   - Discovery: each direct subdirectory is a function with a template.yaml.
//     Relay-owned transient staging directories (IsReservedDir, git's ".sync-"
//     created directly under the root during materialization) are ignored
//     entirely: never a function, never an invalid desired definition, never
//     logged — so a live sync cannot surface a spurious invalid/unavailable
//     state.
//   - Validation: templates are parsed and validated, including per-rule
//     timeout resolution and handler form. Loader.LoadWithDiagnostics surfaces
//     each PRESENT entry that cannot be loaded (invalid path, missing/invalid
//     template) so a caller can record the invalid desired state instead of
//     silently leaving a stale view; Load keeps its historical skip-and-log
//     behavior. A directory that vanished is a removal, never an invalid
//     definition.
//   - Matching: rules pair handlers with patterns evaluated against events. A
//     prepared template may also carry an immutable candidate index
//     (NewRuleIndex) that pre-filters rules by a conservative necessary
//     ("anchor") condition; the index is false-positive-only and always defers
//     the final decision to the exact matcher, so it never changes which rules
//     match.
//   - Fingerprinting: a deterministic hash of a function's SELECTED source
//     (internal/source: files included after applying the function's .gitignore
//     rules) gates reconciler rebuilds; applicable ignore files are hashed too,
//     so a rule edit is a source change. A template that needs no runtime (its
//     only services use the external `image` source) builds no image from
//     source, so FingerprintFunction narrows its fingerprint to template.yaml
//     ALONE — an irrelevant source edit can never look like a desired-state
//     change, and the tree is not scanned. The top-level `resources` mapping is
//     excluded from BOTH fingerprint paths (stripTemplateResources), so a
//     resource-only edit never changes the digest and never triggers a rebuild.
//
// Resource limits:
//   - `resources` (optional) declares per-container memory/CPU/PID limits for
//     every container the function runs (event/schedule/manual invocations and
//     both service source kinds). Memory uses binary suffixes (KiB/MiB/GiB)
//     only; cpus is a finite number > 0 (fractional allowed); pids is a positive
//     integer. Each field is optional and resolved independently, defaulting to
//     128MiB / 1 CPU / 128 PIDs; malformed, zero, or negative values are
//     rejected. Limits are per container, so a function's aggregate ceiling is
//     the configured limit times its concurrently running containers. The
//     effective limits are exposed via Template.ResourceLimits /
//     ResourceLimits.Fingerprint; the runtime maps them onto Docker's HostConfig
//     and keys its warm-container generation on the fingerprint, so a
//     resource-only change rotates containers without a rebuild.
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
// Schedules:
//   - `schedules` (optional) is a list of cron-triggered handlers. Each entry
//     requires `handler` (module.function) and `cron`; `timezone` is optional
//     and defaults to UTC (resolved with time.LoadLocation), and `timeout` is
//     identical to event rules. Cron validation is delegated to gocron/v2 (no
//     Relay cron regexes); an invalid cron expression, timezone, or timeout
//     fails template validation. An embedded TZ=/CRON_TZ= prefix is rejected.
//   - Only minute-granularity schedules are accepted: the standard 5-field form
//     and the calendar descriptors (@hourly, @daily/@midnight, @weekly,
//     @monthly, @yearly/@annually).
//     The 6-field (seconds) form is rejected because gocron's callback exposes
//     no scheduled-due instant, so a per-second occurrence could not be
//     identified deterministically across workers (breaking cluster-wide dedup);
//     the `@every <duration>` descriptor is rejected because it is anchored to
//     each worker's own job start rather than a shared calendar instant.
//
// The package has no side effects beyond reading the filesystem; building,
// execution, and Redis are owned elsewhere.
package function
