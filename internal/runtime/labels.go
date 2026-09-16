package runtime

// The Relay Docker label model. A single relay.type label classifies every
// Relay-owned container population into exactly one of three disjoint types:
//
//	event    — a one-shot invocation container for a matching event rule
//	          (runner.Handle). relay.handler is the rule handler.
//	schedule — a one-shot invocation container for a schedule occurrence
//	          (runner.InvokeHandler). relay.handler is the schedule handler.
//	service  — a persistent, long-lived service container (Manager.StartService).
//	          relay.entrypoint is the application entrypoint file — the service
//	          identity. Service containers carry relay.entrypoint and NO
//	          relay.handler and NO relay.service label: the entrypoint IS the
//	          service.
//
// relay.handler identifies the handler for event/schedule containers only
// (the one-shot invocation form); service containers instead carry
// relay.entrypoint (the long-lived application entrypoint file). There is no
// relay.service label. The sweep and reconciliation predicates rely on
// relay.type being strict: an unknown or missing type is treated as NOT
// Relay-owned, so the strict label set is the single mechanism that keeps
// event/schedule/service populations distinguishable and non-Relay containers
// untouchable. (Backward compatibility with labels from Relay processes older
// than this model is deliberately NOT required: a restart simply treats such
// containers as non-Relay and sweeps/reconciles only typed ones.)

const (
	labelType      = "relay.type"
	labelFunction  = "relay.function"
	labelHandler   = "relay.handler"
	labelMessageID = "relay.message_id"
	labelEventID   = "relay.event_id"
	labelEventName = "relay.event_name"
	labelHostname  = "relay.hostname"
	labelImage     = "relay.image"

	labelEntrypoint = "relay.entrypoint"
	labelPort       = "relay.port"
	labelReplica    = "relay.replica"
)

// The relay.type values. Every Relay-owned container carries exactly one; the
// absence of a type (or the presence of an unknown type) means a container is
// NOT Relay-owned. These are exported so the runner and service reconciler can
// stamp them onto RunMeta / ServiceSpec without a stringly-typed duplicate.
const (
	ContainerTypeEvent    = "event"
	ContainerTypeSchedule = "schedule"
	ContainerTypeService  = "service"
)

// isServiceContainer reports whether labels classify a container as a
// persistent Relay service container. It is a strict equality on relay.type
// ONLY — it does NOT require relay.function. Services list all service
// containers regardless of function for reconciliation discovery: the stale
// set (a function removed while Relay was down) is identified after discovery
// from each container's Function, so requiring relay.function here would
// wrongly exclude a service container whose owning function was deleted.
func isServiceContainer(labels map[string]string) bool {
	return labels[labelType] == ContainerTypeService
}

// isInvocationContainer reports whether labels classify a container as a
// one-shot Relay invocation container (event or schedule). It is strict: an
// unknown or missing relay.type is NOT an invocation container, so the sweep can
// only ever remove containers that carry an explicit, known type.
func isInvocationContainer(labels map[string]string) bool {
	t := labels[labelType]
	return t == ContainerTypeEvent || t == ContainerTypeSchedule
}

// isRelayContainer reports whether labels classify a container as Relay-owned:
// its relay.type is one of the three known values. It is the "touch only
// Relay-owned" guard. Note isRelayContainer does not by itself make a container
// sweep-eligible — services are Relay-owned but reconciler-managed, so the
// sweep excludes them via isServiceContainer.
func isRelayContainer(labels map[string]string) bool {
	t := labels[labelType]
	return t == ContainerTypeEvent || t == ContainerTypeSchedule || t == ContainerTypeService
}
