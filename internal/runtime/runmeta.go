package runtime

import "context"

// RunMeta carries the diagnostic metadata attached as Docker labels to every
// function execution container. It is diagnostic-only: Relay's correctness
// never depends on these labels, and the sweep's ownership predicate relies only
// on the relay.function and relay.hostname labels.
//
// The values are all low-cardinality or bounded identifiers that Relay already
// uses as log fields or identities elsewhere — never raw payload content:
//   - Function/Handler are configuration (function name, rule handler).
//   - MessageID is the stream message ID (bounded by upstream event data).
//   - EventID/EventName are low-cardinality event fields already used as log
//     fields by the runner.
//   - Hostname is the worker identity (the same value as the Redis consumer
//     identity).
//   - Image is the exact pinned build reference the container runs.
//
// Empty values are allowed and are deliberately materialized as empty labels so
// the label set is total and greppable across every container Relay owns; no
// key is ever omitted.
type RunMeta struct {
	Function  string
	Handler   string
	MessageID string
	EventID   string
	EventName string
	Hostname  string
	Image     string
}

// Label keys Relay stamps on every execution container.
const (
	labelFunction  = "relay.function"
	labelHandler   = "relay.handler"
	labelMessageID = "relay.message_id"
	labelEventID   = "relay.event_id"
	labelEventName = "relay.event_name"
	labelHostname  = "relay.hostname"
	labelImage     = "relay.image"
)

// runLabels maps a RunMeta to the Docker label set for an execution container.
// Every key is always present (empty values become empty labels) so the set is
// total and stable. Nothing depends on the labels for correctness.
func runLabels(meta RunMeta) map[string]string {
	return map[string]string{
		labelFunction:  meta.Function,
		labelHandler:   meta.Handler,
		labelMessageID: meta.MessageID,
		labelEventID:   meta.EventID,
		labelEventName: meta.EventName,
		labelHostname:  meta.Hostname,
		labelImage:     meta.Image,
	}
}

// runMetaKey is the unexported context key carrying the per-invocation RunMeta
// into the executor, so the runtime package owns the contract.
type runMetaKey struct{}

// WithRunMeta returns a child of ctx carrying the invocation's diagnostic
// metadata. The runner injects this before calling the executor so the labels
// land on the execution container without widening the Executor interface (test
// fakes are unaffected).
func WithRunMeta(ctx context.Context, meta RunMeta) context.Context {
	return context.WithValue(ctx, runMetaKey{}, meta)
}

// RunMetaFrom returns the RunMeta carried in ctx, or the zero value when absent.
// It is best-effort context: an absent meta simply yields empty labels, never a
// panic.
func RunMetaFrom(ctx context.Context) RunMeta {
	meta, _ := ctx.Value(runMetaKey{}).(RunMeta)
	return meta
}
