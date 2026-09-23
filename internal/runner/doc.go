// Package runner is the orchestration contract between the stream layer and
// the runtime.
//
// This package evaluates events against a snapshot-consistent function registry:
//   - Match: each event is evaluated against every loaded function's rules
//   - Execute: matching handlers run sequentially, bounded by their rule's timeout
//   - Skip: when the stream layer injects invocation state into the context,
//     a matching handler whose "<function>/<rule-handler>" invocation already
//     succeeded on a previous delivery, is protected by an active attempt
//     deadline or a retry backoff, or is exhausted is skipped (not executed, not
//     counted)
//   - Retry: a failing invocation records a per-invocation retry backoff
//     (1m/2m/5m/10m, capped at 10m) and counts function_retries_total; once its
//     attempts (1 + rule.Retries) are exhausted it is marked terminal. Outcomes
//     are per-invocation and aggregated after the full rule loop (the loop is
//     sequential, never parallel)
//   - Outcome: each matching invocation gets its own independent attempt on
//     every delivery — a failure in one handler never prevents the others from
//     running. Handle then aggregates the per-invocation outcomes into a single
//     message-level error: a retryable failure keeps the message pending; when
//     every matched invocation is terminal (complete or exhausted) and at least
//     one exhausted, the message is terminal and routed to the DLQ — the
//     returned *stream.HandlerExhaustedError carries EVERY exhausted invocation
//     (exact function/handler and handler attempt), so the stream writes one
//     correctly-attributed DLQ entry per invocation; a protected or slot-timeout
//     skip returns stream.ErrInvocationNotEligible so the message stays pending
//     (never acked while another replica may still be processing it, even if
//     other invocations succeeded this delivery)
//   - Panic boundary: each invocation's execution runs inside runInvocation,
//     which recovers an executor/runtime panic and converts it into a normal
//     failed attempt (metrics + recordFailure), so a panicking execution is
//     isolated and retried via the same retry/exhaustion machinery as any other
//     failure. The image refcount is still released and the invocation context
//     cancel is deferred, so no reference or timer leaks. Panics elsewhere
//     (startup, reconciler, Redis client) are not recovered and stay fatal
//   - Secrets: a template's secret references are resolved to values immediately
//     before each execution (via the provider set with SetSecretProvider) and
//     passed to the executor as extra env. Resolved values are never cached on
//     Prepared, never baked into images, and never logged; a resolution failure
//     is a failed attempt that flows through the normal retry/exhaustion
//     machinery. A template that references a secret with no provider configured
//     fails the invocation with a clear error naming the reference.
//
// Key Guarantees:
//   - Handle holds one registry snapshot for the whole call, so in-flight
//     executions never observe a half-replaced set during a live swap
//   - Invocation identity is "function/rule-handler", stable across restarts and
//     config reloads as long as the rule still exists; renaming a function or
//     handler invalidates old invocation state (old entries simply never match)
//
// The package owns no Redis, Docker, or matching internals; execution is
// delegated to a runtime executor.
//
// Schedules: cron-triggered handlers reach the runner through InvokeHandler
// (see internal/schedule for coordination and internal/cron for timing). Every worker evaluates a cron
// schedule locally but publishes one stream entry per occurrence cluster-wide
// (atomic publish-if-new), and the consumer routes that single schedule
// message directly to InvokeHandler, bypassing event matching. Schedules ride
// the normal stream machinery: retry, backoff, exhaustion, DLQ, and
// invocation-state semantics apply exactly like any other stream message. The
// handler timeout is resolved from the function's CURRENT template (single
// source of truth), so a hot-swapped template's new timeout applies to future
// occurrences; the scheduled container is attributable via the relay.type=
// schedule label (message id stamped on relay.message_id). Handler execution
// remains at-least-once.
package runner
