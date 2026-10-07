// Package runner is the orchestration contract between the stream layer and
// the runtime.
//
// This package evaluates events against a snapshot-consistent app registry:
//   - Match: each event is evaluated against every loaded app's rules,
//     INCLUDING apps that are configured but currently unavailable (their
//     image could not be built). An event matching only an unavailable app
//     is still MATCHED and engages that app; it is never misclassified as
//     unmatched
//   - Execute: matching handlers run sequentially, bounded by their rule's timeout
//   - Unavailable: a matched invocation of an unavailable app cannot run
//     this delivery. It claims no handler attempt and touches no handler counter
//     (no handler ran); Handle reports the retryable runner.ErrAppUnavailable
//     so the stream leaves the message pending — never ACKed, never DLQ'd solely
//     for unavailability. A mixed message runs its available invocations to
//     completion and holds pending only for the unavailable one; once the
//     app is rebuilt, a redelivery finishes the outstanding work
//   - Pending desired generations: beside the active set the registry can hold
//     a PENDING desired template for a valid generation being prepared but not
//     yet runnable (a brand-new app's first build, or an existing app's rebuild
//     to new rules). A pending rule matches and engages its app but is treated
//     exactly like an unavailable match: it never executes and does not claim
//     TryStart or advance its attempt count, never exhausts, ACKs, or DLQs;
//     it only holds the event pending, so an event arriving during preparation
//     is never ACKed away as unmatched. A pending
//     rule whose invocation is also matched by the active generation is deduped
//     by "<app>/<handler>", so the active generation executes it and it is not
//     separately held. Installing the new generation (or removing/invalidating
//     the app) clears the pending entry atomically with the registry mutation.
//   - Skip: when the stream layer injects invocation state into the context,
//     a matching handler whose "<app>/<rule-handler>" invocation already
//     succeeded on a previous delivery, is protected by an active attempt
//     deadline or a retry backoff, or is exhausted is skipped (not executed, not
//     counted)
//   - Retry: a failing invocation records a per-invocation retry backoff
//     (1m/2m/5m/10m, capped at 10m) and counts function_retries_total; once its
//     attempts (1 + rule.Retries) are exhausted it is marked terminal. Outcomes
//     are per-invocation and aggregated after the full rule loop (the loop is
//     sequential, never parallel)
//   - Attempt boundary: the concurrency slots and the warm-container budget are
//     admitted BEFORE the per-invocation attempt is claimed, so a capacity
//     rejection (a slot timeout or a saturated warm budget) records no attempt
//     and charges no retry or DLQ. The claim itself (stream.InvocationState.TryStart)
//     is the persisted attempt boundary: a crash after a confirmed claim but
//     before the handler starts spends that attempt when its running deadline
//     later elapses. A worker-shutdown cancellation changes no persisted
//     retry/exhaustion state: if it lands before the claim no attempt is claimed
//     at all, and if it lands after a confirmed claim that attempt is spent but
//     no retry backoff or exhausted/DLQ marker is written (recordFailure refuses
//     to issue a canceled delivery's transition), so shutdown cannot consume the
//     retry budget or dead-letter work. This is deliberately narrower than
//     "cancellation charges nothing": the handler failure telemetry
//     (handler_failure_total etc.) is still incremented for a post-claim
//     execution error before recordFailure's guard suppresses the persisted
//     transition. The configured retry
//     budget bounds normal failing executions but does NOT cap admitted claims
//     across repeated crashes, so the persisted count can exceed 1+retries before
//     any real failure — the accepted at-least-once window (see
//     internal/stream/doc.go). TryStart claims without a retry budget and never
//     writes a terminal marker, so a crash alone cannot DLQ: exhaustion is
//     recorded here only after a later real execution fails with the persisted
//     attempt already at or above 1+retries
//   - Outcome: each matching invocation gets its own independent attempt on
//     every delivery — a failure in one handler never prevents the others from
//     running. Handle then aggregates the per-invocation outcomes into a single
//     message-level error: a retryable failure keeps the message pending; when
//     every matched invocation is terminal (complete or exhausted) and at least
//     one exhausted, the message is terminal and routed to the DLQ — the
//     returned *stream.HandlerExhaustedError carries EVERY exhausted invocation
//     (exact app/handler and handler attempt), so the stream writes one
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
//     passed to the executor as extra env. The executor carries those values (and
//     the template's literal env values) in the per-invocation request frame to
//     the reused bootstrap process; they are never written into an execution
//     container's Docker Config.Env, never cached on Prepared, never baked into
//     images, never logged, and never persisted. A resolution failure is a failed
//     attempt that flows through the normal retry/exhaustion machinery. A
//     template that references a secret with no provider configured fails the
//     invocation with a clear error naming the reference.
//
// Key Guarantees:
//   - Handle holds one registry snapshot for the whole call, so in-flight
//     executions never observe a half-replaced set during a live swap
//   - Invocation identity is "app/rule-handler", stable across restarts and
//     config reloads as long as the rule still exists; renaming an app or
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
// invocation-state semantics apply exactly like any other stream message.
//
// Schedule admission boundary: BEFORE ADMISSION an occurrence resolves the
// app's CURRENT template by its stable schedule NAME on every delivery, so
// a handler/timeout/retries change under the same name applies and the name's
// current handler runs; a removed NAME with nothing admitted yet is obsolete.
// Because an occurrence may block for a long time waiting for a concurrency
// slot, that config snapshot is refreshed from the registry AFTER the slot is
// admitted and immediately before the atomic first claim, so a reload completed
// during the wait is observed. AT ADMISSION the first successful claim atomically
// pins an immutable ScheduleDescriptor (schedule name, handler, CAPPED timeout,
// retry budget) in the message's invocation-state hash, so concurrent replicas —
// or a reload racing the post-slot refresh — cannot diverge: the Redis
// first-writer wins and every other delivery adopts the pinned descriptor.
// AFTER ADMISSION the pinned descriptor is the single source of truth even if the
// schedule was renamed, retimed, or removed, so an admitted invocation completes
// its retry/DLQ lifecycle instead of being cancelled by a config change. The
// schedule name remains the occurrence/dedup identity; invocation/DLQ
// attribution stays handler-based. Handler execution remains at-least-once.
package runner
