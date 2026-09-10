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
//     attempts (1 + rule.Retries) are exhausted it is marked terminal and, when
//     every matched invocation is terminal, the message is routed to the DLQ
//   - Outcome: an error is returned when any invocation fails, so the stream
//     layer does not acknowledge the message. A protected-only skip returns
//     stream.ErrInvocationNotEligible so the message stays pending (never acked
//     while another replica may still be processing it); a fully-exhausted
//     message returns stream.ErrInvocationExhausted so it is routed to the DLQ
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
package runner
