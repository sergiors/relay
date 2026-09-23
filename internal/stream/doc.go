// Package stream owns Redis Stream consumption and recovery.
//
// This package manages the consumer-group lifecycle and message delivery:
//   - Group creation: XGROUP CREATE with MKSTREAM, tolerating BUSYGROUP
//   - Consumption: an XREADGROUP loop that hands each decoded event to a Handler
//   - Schedule occurrences: messages whose decode is recognized as a schedule
//     envelope (see internal/schedule) ride this same consumer-group / PEL /
//     XAUTOCLAIM / retry / DLQ machinery but bypass event matching: when a
//     ScheduleRunner is wired they invoke the named function/handler directly.
//   - Recovery: XAUTOCLAIM reclaims idle pending messages, with retry counts
//     sourced from XPENDING so delivery counts survive restarts. Reclaim is
//     message-ownership recovery only; whether a reclaimed message's invocation
//     is actually executed is decided at run time from per-invocation state
//     (complete / running-until-deadline / next-attempt-until-deadline /
//     exhausted / eligible). MinPendingIdle defaults to DefaultReclaimInterval
//     (1m) and is a message-level recovery-pacing backstop; it never defines
//     retry timing.
//   - Dead-lettering: exhausted or malformed messages are XADD'd to the DLQ
//     before the original is acknowledged. Exhaustion is per-invocation: when
//     every non-complete matched invocation is exhausted, the whole message is
//     routed to the DLQ and one entry is written PER exhausted invocation (see
//     ErrInvocationExhausted and HandlerExhaustedError), each carrying its exact
//     function/handler and handler attempt count. A malformed message that never
//     reached a handler produces a single entry with the "-" placeholder and an
//     explicit handler_attempts of 0.
//   - DLQ idempotency without scanning the DLQ: once an invocation's entry is
//     successfully XADD'd its marker becomes "exhausted:<attempts>:dlq"; a
//     redelivery (after an XACK failure, a crash, or a partially-written
//     multi-entry DLQ) skips the already-persisted entries and writes only the
//     missing ones. The original is ACKed only after all required entries are
//     persisted, so a write failure leaves the message pending.
//   - Invocation state: per-handler lifecycle is recorded in a Redis hash
//     (relay:invocation:{stream}:{group}:{msgID}, field "<function>/<handler>" →
//     "ok" when complete, "running:<deadline>#<attempts>" while an attempt is
//     protected, "next_attempt_at:<deadline>#<attempts>" while a failed attempt
//     waits out its retry backoff, "exhausted:<attempts>" when terminal, or
//     "exhausted:<attempts>:dlq" when terminal AND its DLQ entry is persisted;
//     TTL'd; stream/group names are percent-encoded in the key) so a
//     redelivered message skips handlers that already completed, are still
//     within an active attempt deadline or retry backoff, or are exhausted; the
//     message is acknowledged when all matching invocations are complete, and
//     the invocation state key is eagerly cleared on completion or DLQ
//
// Key Guarantees:
//   - A message is acknowledged only after the handler succeeds or the DLQ
//     write succeeds (at-least-once, never exactly-once)
//   - A message whose invocation is protected (running on another replica or
//     waiting out a retry backoff) is left pending, never acknowledged — this
//     is what prevents the cross-replica ACK hazard where a reclaiming replica
//     could ack a message another replica is still processing
//   - Invocation state is at-least-once, not exactly-once: a crash between a
//     handler's side effect and its MarkComplete re-runs the handler, so handlers
//     must remain idempotent. State read/mark/clear failures are logged and
//     fail open (re-run) rather than becoming a new failure source.
//   - Transient Redis failures are logged and retried, never fatal
//   - Redis outages are survived: the consume loop backs off with bounded,
//     jittered exponential backoff (1s..30s cap) and the consumer exposes a
//     health state (Healthy) fed by real operations, so an embedder or
//     orchestrator can observe readiness without a separate PING
//   - Shutdown cancellation leaves messages pending, not counted as attempts
//   - Panic boundary: processMessage registers a recover so a panic in the
//     handler handoff (including runner code outside its per-invocation
//     recover) is converted into the standard failure path — the message is
//     left pending (no ACK) and a later reclaim retries it (at-least-once).
//     This is defense in depth behind the runner's per-invocation boundary,
//     which already converts executor panics into normal failed attempts so
//     retry/exhaustion state machinery runs. Panics in Consume/reclaimLoop
//     are outside message processing and are deliberately NOT
//     recovered: they remain fatal
//
// Usage: NewConsumer, then EnsureGroup, then Consume with a Handler.
//
// The package knows nothing about matching or execution; it delegates each
// decoded event to the caller's Handler. Schedule-occurrence messages are a
// notable exception: they are routed to the ScheduleRunner seam (when wired),
// which is the runner's InvokeHandler executing the named function/handler
// directly without event matching. A ScheduleRunner that reports
// ErrInvocationObsolete (the function or schedule handler was removed while the
// occurrence was pending) causes the message to be acknowledged — an obsolete
// occurrence is terminal and is never retried or dead-lettered.
package stream
