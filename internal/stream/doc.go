// Package stream owns Redis Stream consumption and recovery.
//
// This package manages the consumer-group lifecycle and message delivery:
//   - Group creation: XGROUP CREATE with MKSTREAM, tolerating BUSYGROUP
//   - Consumption: an XREADGROUP loop that hands each decoded event to a Handler
//   - Recovery: XAUTOCLAIM reclaims idle pending messages, with retry counts
//     sourced from XPENDING so delivery counts survive restarts
//   - Dead-lettering: exhausted or malformed messages are XADD'd to the DLQ
//     before the original is acknowledged
//
// Key Guarantees:
//   - A message is acknowledged only after the handler succeeds or the DLQ
//     write succeeds (at-least-once, never exactly-once)
//   - Transient Redis failures are logged and retried, never fatal
//   - Shutdown cancellation leaves messages pending, not counted as attempts
//
// Usage: NewConsumer, then EnsureGroup, then Consume with a Handler.
//
// The package knows nothing about matching or execution; it delegates each
// decoded event to the caller's Handler.
package stream
