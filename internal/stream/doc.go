// Package stream owns Redis Stream consumption and recovery.
//
// This package manages the consumer-group lifecycle and message delivery:
//   - Group creation: XGROUP CREATE with MKSTREAM, tolerating BUSYGROUP, via the
//     package-level EnsureGroup (Consumer.EnsureGroup delegates to it). The
//     worker runs it as the FIRST step of its external-dependency preflight,
//     before any app is loaded or fingerprinted and before the state DB and
//     runtime workload initialization (the Redis client and tracing already
//     exist; the Docker manager is opened afterwards, see internal/worker), so a
//     Redis stream/group problem short-circuits startup; the group is not
//     created lazily right before Consume. An embedder uses it before Consume.
//   - Consumption: an XREADGROUP loop that hands each decoded event to a Handler
//   - Schedule occurrences: messages whose decode is recognized as a schedule
//     envelope (see internal/schedule) ride this same consumer-group / PEL /
//     XAUTOCLAIM / retry / DLQ machinery but bypass event matching: when a
//     ScheduleRunner is wired they validate the occurrence against the current
//     template and invoke the named app/schedule directly. A message that claims
//     schedule identity (source=="relay.schedule") but is structurally
//     incomplete, malformed, or carries a mismatched occurrence_id is NEVER
//     treated as an ordinary event: it is dead-lettered as a non-retryable
//     failure, so a malformed claim can neither fall through to pattern matching
//     nor be ACKed as unmatched.
//   - Recovery: XAUTOCLAIM reclaims idle pending messages, with retry counts
//     sourced from XPENDING so delivery counts survive restarts. Reclaim is
//     message-ownership recovery only; whether a reclaimed message's invocation
//     is actually executed is decided at run time from per-invocation state
//     (complete / running-until-deadline / next-attempt-until-deadline /
//     exhausted / eligible). MinPendingIdle defaults to DefaultReclaimInterval
//     (1m) and is a message-level recovery-pacing backstop; it never defines
//     retry timing. A reclaimed entry whose stream body no longer exists
//     (trimmed or XDEL'd before acknowledgement) is never processed as success:
//     it is surfaced via a WARN log and the missing_payload_total counter and
//     its dangling PEL reference is cleared; no handler attempt, retry, or DLQ
//     entry is fabricated (Redis 7+ reports such entries in XAUTOCLAIM's purged
//     deleted-id array; older servers return them with nil values).
//   - Dead-lettering: exhausted or malformed messages are XADD'd to the DLQ
//     before the original is acknowledged. Exhaustion is per-invocation: when
//     every non-complete matched invocation is exhausted, the whole message is
//     routed to the DLQ and one entry is written PER exhausted invocation (see
//     ErrInvocationExhausted and HandlerExhaustedError), each carrying its exact
//     app/handler and handler attempt count. A malformed message that never
//     reached a handler produces a single entry with the "-" placeholder and an
//     explicit handler_attempts of 0. A message whose raw `event` value exceeds
//     MAX_EVENT_BYTES is checked at extraction time (before decode, schedule
//     classification, matching, and invocation-state migration) and routed the
//     same non-retryable way with the same "-" placeholder, but its DLQ `event`
//     field is a small diagnostic summary (EventOversizedError reports the
//     measured bytes and configured max) rather than the oversized payload; such
//     an entry is intentionally non-replayable.
//   - DLQ idempotency without scanning the DLQ: once an invocation's entry is
//     successfully XADD'd its marker becomes "exhausted:<attempt>:<token>:dlq"; a
//     redelivery (after an XACK failure, a crash, or a partially-written
//     multi-entry DLQ) skips the already-persisted entries and writes only the
//     missing ones. The original is ACKed only after all required entries are
//     persisted, so a write failure leaves the message pending.
//   - Invocation state: per-handler lifecycle is recorded in a Redis hash
//     (relay:invocation:{stream}:{group}:{msgID}, field "<app>/<handler>" →
//     "ok" when complete, "running:<deadline_ms>:<attempt>:<token>" while an
//     attempt is protected, "next_attempt_at:<deadline_ms>:<attempt>:<token>"
//     while a failed attempt waits out its retry backoff, "exhausted:<attempt>:<token>"
//     when terminal, or "exhausted:<attempt>:<token>:dlq" when terminal AND its
//     DLQ entry is persisted; stream/group names are percent-encoded in the key)
//     so a redelivered message skips handlers that already completed, are still
//     within an active attempt deadline or retry backoff, or are exhausted; the
//     message is acknowledged when all matching invocations are complete.
//     While the message is recoverable the key is PERSISTENT (no TTL); only
//     after a successful ACK (success, obsolete schedule, or DLQ) does the state
//     switch to terminal retention (a reserved marker plus the configured
//     invocation retention, REDIS_INVOCATION_RETENTION, default 48h) so a stale
//     in-memory delivery can neither mutate
//     nor resurrect it. A schedule occurrence additionally pins an immutable
//     admission descriptor under the reserved "__schedule" field, atomically
//     with its first successful claim (see ScheduleDescriptor and
//     TryStartScheduled), so the handler/timeout/retries it was admitted under
//     are stable through retry/reclaim/DLQ even if the schedule is later changed
//     or removed; the descriptor is a sibling field, never an invocation ID, and
//     it is never a DLQ/attribution key.
//
// Deadlines are integer Unix MILLISECONDS end to end. An active
// running/next_attempt_at marker is protected iff now_ms < deadline_ms and
// eligible iff now_ms >= deadline_ms: the comparison is exact (a
// length-then-lexicographic compare of decimal strings inside the Lua script),
// with no clock-skew fudge and no Lua floating point.
//
// Each successful claim generates a crypto-random, opaque token BEFORE the EVAL
// and persists it only on a win, so "<attempt>:<token>" is the claim identity.
// Every transition that originates from an active claim (fail/retry, complete,
// exhaust, and the DLQ marker upgrade) CASes BOTH attempt and token against the
// current active marker; a mismatch is an explicit stale/refused result (false),
// never a Redis error. Terminal exhausted markers retain their claim identity
// ("exhausted:<attempt>:<token>") so the DLQ-persistence upgrade can CAS the
// same exhausted claim and can never downgrade a newer exhausted marker or a
// success. Tokens are never logged, used as metric labels, or written to the DLQ.
//
// Invocation-state transitions are atomic: every lifecycle write (claim,
// failure, completion, exhaustion, classification, trace) is a single Lua
// script, so concurrent replicas cannot both start the same invocation and a
// stale claim can never overwrite a newer claim's or a terminal marker.
//
// The per-invocation state machine is:
//
//	unseen (absent) --TryStart--> running:<deadline_ms>:<n>:<token>
//	running --RecordFailure--> next_attempt_at:<deadline_ms>:<n>:<token>   (same claim)
//	running/next_attempt_at --(now_ms >= deadline_ms)--> eligible --> running:<...#<n+1>:<new token>>
//	running/next_attempt_at --MarkComplete--> ok                (terminal success, CASed on the claim)
//	running/next_attempt_at --MarkExhausted--> exhausted:<n>:<token>    (terminal, CASed on the claim)
//	exhausted:<n>:<token> --MarkExhaustedDLQ--> exhausted:<n>:<token>:dlq (terminal, entry persisted, CASed on the exhausted claim)
//
// "ok" and the exhausted forms are terminal: TryStart never re-opens them, and
// MarkComplete never downgrades an exhausted marker (success must not resurrect
// a message already routed to the DLQ). `handler_attempts` is the runner's
// persisted attempt count, distinct from the Redis stream delivery count.
//
// TryStart is the persisted attempt boundary: it is the only place handler_attempts
// advances, and every active-claim transition (failure/retry, completion,
// exhaustion) is CASed on the attempt+token it returns. The runner calls it only
// after the concurrency slots and the warm-budget admission have been granted, so
// a PRE-claim capacity rejection (a slot timeout or a saturated warm budget)
// records no attempt and charges no retry or DLQ. There is a deliberate
// claim-before-execute window: a crash (or a create-time warm-budget race) AFTER a
// confirmed claim but BEFORE the handler starts leaves the running marker
// persisted, so its deadline simply elapses and the next delivery claims the NEXT
// attempt. The unexecuted attempt is therefore spent — the configured retry budget
// bounds normal failing executions but does NOT cap admitted claims across
// repeated crashes, so the persisted count can exceed 1+retries before any real
// failure. Crucially, TryStart CLAIMS but does NOT itself EXHAUST: it carries no
// retry budget and never writes an exhausted marker, so a crash ALONE can never
// dead-letter a message — it only raises the persisted count. Exhaustion, and
// therefore the DLQ, still requires a later delivery whose handler actually RUNS
// and FAILS after the persisted count has reached 1+retries (the first such real
// failure exhausts); repeated lost claims can make that eventual real failure
// exhaust with a larger persisted attempt count. This is at-most-one-attempt of
// exposure per crash and is accepted: it is bounded, the message is never lost,
// the handler never runs in the window, and closing it would require a two-phase
// claim/execute transaction that at-least-once delivery does not provide. A
// Redis/transport error on TryStart is a genuinely unknown outcome and is handled
// separately (see below): nothing is claimed and the handler must not run.
//
// The trace lineage is a sibling hash field (never part of the lifecycle value).
// While the message is
// recoverable (still in the PEL) every mutation keeps the hash PERSISTENT — no
// TTL — so a reclaim always observes the marker and carries its attempt forward
// instead of resetting the attempt count, no matter how long the message sat
// pending. Only after a successful ACK does the state switch to terminal
// retention (a reserved marker plus the configured invocation retention,
// REDIS_INVOCATION_RETENTION, default 48h; empty/0/negative disables it so the
// marker is written but the hash stays persistent). A running marker that is
// not renewed (a crash) does not expire out of Redis —
// the persistent hash keeps it — but its deadline simply elapses, after which
// TryStart treats the marker as eligible: reclaim then starts attempt n+1 with a
// fresh token, so there is no permanent lock. Race ordering is defined by Redis's
// single-threaded script execution: among simultaneous claims exactly one sees
// the absent marker or a deadline that has elapsed and starts, and every later
// claim observes the new marker.
//
// A Redis/transport error on TryStart is NOT failed open: the claim outcome is
// unknown, so the runner leaves the message pending and does not execute the
// handler on that delivery (an ambiguous claim could race a replica that won).
//
// A refused (stale/terminal) transition result is likewise not an ACK: the
// runner treats it as unresolved and leaves the message pending, so a superseded
// claim's outcome can never acknowledge or dead-letter a newer claim's message.
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
//     must remain idempotent. State read/mark/retain failures are logged and
//     fail open (re-run) rather than becoming a new failure source; a retention
//     failure leaves the hash persistent (a leak) rather than losing it. Two
//     exceptions fail closed and leave the message pending (no handler run, no
//     ACK): an ambiguous TryStart claim (Redis/transport error), because running
//     a duplicate could race a replica that won the same claim, and a
//     makeRecoverable failure, because a legacy-TTL hash that could not be made
//     persistent may expire mid-delivery, so the state cannot be relied on.
//   - A pending entry whose stream body no longer exists is data loss, not a
//     success: it is counted as missing_payload_total, never run through a
//     handler, and never turned into a fabricated payload or DLQ entry.
//   - Transient Redis failures are logged and retried, never fatal
//   - Redis outages are survived: the consume loop backs off with bounded,
//     jittered exponential backoff (1s..30s cap) and the consumer exposes a
//     health state (Healthy) fed by real operations, so an embedder or
//     orchestrator can observe readiness without a separate PING
//   - Shutdown cancellation leaves messages pending, not counted as attempts
//   - The persisted handler attempt advances ONLY on a confirmed TryStart claim,
//     which the runner issues after the concurrency and warm-budget capacity gates;
//     a pre-claim capacity rejection charges no retry or DLQ. A crash (or a
//     create-time warm-budget race) between a confirmed claim and the handler start
//     consumes the claimed attempt, because the claim — not the executor dispatch —
//     advances the persisted attempt count. That claim alone never exhausts (a
//     crash alone cannot DLQ): the terminal exhausted marker is written only by a
//     later real failed attempt via MarkExhausted. The configured retry budget
//     bounds normal failing executions but does NOT cap admitted claims across
//     repeated crashes. The bounded exposure is documented above
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
// Usage: NewConsumer, then EnsureGroup, then Consume with a Handler. The
// group bootstrap may be done either through Consumer.EnsureGroup or the
// package-level EnsureGroup(ctx, client, stream, group) (the single
// implementation Consumer.EnsureGroup delegates to); the worker runs the
// package-level form as the first external-dependency preflight step before
// Consume.
//
// The package knows nothing about matching or execution; it delegates each
// decoded event to the caller's Handler. Schedule-occurrence messages are a
// notable exception: they are routed to the ScheduleRunner seam (when wired),
// which validates the occurrence against the current template and executes the
// named app's schedule by its stable schedule name directly without event
// matching. A ScheduleRunner that reports ErrInvocationObsolete (the app or
// schedule NAME was removed while the occurrence was pending) causes the message
// to be acknowledged — an obsolete occurrence is terminal and is never retried
// or dead-lettered. A ScheduleRunner that reports ErrScheduleInvalid (the
// occurrence is well-formed but is not a real firing of the current schedule
// definition) causes the message to be dead-lettered — an invalid claim is
// terminal and is never retried or ACKed as success.
package stream
