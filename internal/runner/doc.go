// Package runner is the orchestration contract between the stream layer and
// the runtime.
//
// This package evaluates events against a snapshot-consistent function registry:
//   - Match: each event is evaluated against every loaded function's rules
//   - Execute: matching handlers run sequentially, bounded by their rule's timeout
//   - Outcome: an error is returned when any invocation fails, so the stream
//     layer does not acknowledge the message
//
// Key Guarantees:
//   - Handle holds one registry snapshot for the whole call, so in-flight
//     executions never observe a half-replaced set during a live swap
//
// The package owns no Redis, Docker, or matching internals; execution is
// delegated to a runtime executor.
package runner
