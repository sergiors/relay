// Package stream owns Redis Stream consumption and recovery.
//
// This package manages the consumer-group lifecycle and message delivery:
//   - Group creation: XGROUP CREATE with MKSTREAM, tolerating BUSYGROUP
//   - Consumption: an XREADGROUP loop that hands each decoded event to a Handler
//   - Recovery: XAUTOCLAIM reclaims idle pending messages, with retry counts
//     sourced from XPENDING so delivery counts survive restarts
//   - Dead-lettering: exhausted or malformed messages are XADD'd to the DLQ
//     before the original is acknowledged
//   - Invocation progress: per-handler success is recorded in a Redis hash
//     (relay:progress:{stream}:{group}:{msgID}, field "<function>/<handler>" →
//     "ok", TTL'd; stream/group names are percent-encoded in the key) so a
//     redelivered message skips handlers that already succeeded; the message is
//     acknowledged when all matching invocations are complete, and the progress
//     key is eagerly cleared on completion or DLQ
//
// Key Guarantees:
//   - A message is acknowledged only after the handler succeeds or the DLQ
//     write succeeds (at-least-once, never exactly-once)
//   - Invocation progress is at-least-once, not exactly-once: a crash between a
//     handler's side effect and its markSuccess re-runs the handler, so handlers
//     must remain idempotent. Progress read/mark/clear failures are logged and
//     fail open (re-run) rather than becoming a new failure source.
//   - Transient Redis failures are logged and retried, never fatal
//   - Redis outages are survived: the consume loop backs off with bounded,
//     jittered exponential backoff (1s..30s cap) and the consumer exposes a
//     health state (Healthy) fed by real operations, so the `relay health`
//     command and orchestrators can observe readiness without a separate PING
//   - Shutdown cancellation leaves messages pending, not counted as attempts
//
// Usage: NewConsumer, then EnsureGroup, then Consume with a Handler.
//
// The package knows nothing about matching or execution; it delegates each
// decoded event to the caller's Handler.
package stream
