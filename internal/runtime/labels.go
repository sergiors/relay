package runtime

// The Relay Docker label model. A single relay.type label classifies every
// Relay-owned container population into exactly one of three disjoint types:
//
//	event    — a one-shot invocation container for a matching event rule
//	          (runner.Handle). relay.handler is the rule handler.
//	schedule — a one-shot invocation container for a schedule occurrence
//	          (runner.InvokeHandler). relay.handler is the schedule handler.
//	service  — a persistent, long-lived service container (Manager.StartService).
//	          relay.identity is the service identity: the configured source
//	          descriptor (entrypoint file or external image
//	          reference). Service containers carry relay.identity and NO
//	          relay.handler and NO relay.service label: the source IS the
//	          service.
//
// relay.handler identifies the handler for event/schedule containers only
// (the one-shot invocation form); service containers instead carry
// relay.identity (the configured source descriptor). There is no
// relay.service label. The sweep and reconciliation predicates rely on
// relay.type being strict: an unknown or missing type is treated as NOT
// Relay-owned, so the strict label set is the single mechanism that keeps
// event/schedule/service populations distinguishable and non-Relay containers
// untouchable. (Backward compatibility with labels from Relay processes older
// than this model is deliberately NOT required: a restart simply treats such
// containers as non-Relay and sweeps/reconciles only typed ones.)
//
// The same strict relay.type model classifies Relay's IMAGES. A single
// relay.type label names either a function image or a dependency image; the
// label set is the source of truth for which images Relay owns and how they
// are wired. relay.fingerprint pins the content version the image was built
// from, relay.function names the owning function, relay.dependency names the
// exact dependency image this function image was built FROM (a full
// "relay-dep-*" repo tag), and relay.runtime names the runtime a dependency
// image was built for. The dependency GC reads exactly these labels and never
// infers ownership from repository names alone.

const (
	labelType      = "relay.type"
	labelFunction  = "relay.function"
	labelHandler   = "relay.handler"
	labelMessageID = "relay.message_id"
	labelEventID   = "relay.event_id"
	labelEventName = "relay.event_name"
	labelHostname  = "relay.hostname"
	labelImage     = "relay.image"

	// labelIdentity is the persistent service's identity: the configured source
	// descriptor (entrypoint file or external image reference). It replaces the
	// former relay.entrypoint label so every source kind has an honest identity.
	// labelImageID records the local content ID a
	// service container was started from (empty for content-addressed Relay
	// tags), so a moved external tag is detected.
	labelIdentity = "relay.identity"
	labelImageID  = "relay.image_id"
	labelPort     = "relay.port"
	labelReplica  = "relay.replica"

	// labelNetworks records the canonical, sorted, comma-separated set of
	// Docker networks a service container was created attached to (the routing
	// network). It exists
	// so the service reconciler can detect a routing-network change and replace
	// the stale container. It is omitted for a container with no extra networks,
	// so a service that declares no host is byte-for-byte the pre-networks
	// behavior. Docker network names never contain commas, so the comma-joined
	// form is unambiguous. It covers persistent service containers only:
	// execution containers join the worker-global NETWORKS set, which is startup
	// configuration and not tracked per container.
	labelNetworks = "relay.networks"

	// labelEnvHash pins the exact effective environment a service replica was
	// created with: a short content hash over the Config.Env slice (runtime plan
	// env + template env + resolved secrets + PORT), never any value itself.
	//
	// It exists because the environment is the one piece of a service's
	// configuration the image reference cannot carry: an external `image`
	// source keeps its reference when the template's env changes, and a rotated
	// secret value never changes the fingerprint at all. Without this label the
	// service reconciler would keep such a container and it would serve its old
	// environment forever. The hash is a one-way digest, so no secret value is
	// exposed; a container built before the label carries none and never
	// matches the desired hash, so it is replaced once.
	labelEnvHash = "relay.env_hash"

	// Managed-image labels. These pin the identity and wiring of a managed
	// image (function or dependency) so the dependency GC can classify images
	// and resolve function→dependency ownership without inferring anything
	// from repository names. relay.fingerprint holds the content fingerprint
	// prefix the image was built from; relay.dependency is the full
	// "relay-dep-*" reference the function image was built FROM (present on
	// function images only, and only when the function declares deps);
	// relay.runtime names the runtime spec.Name a dependency image was built
	// for (dependency images only).
	labelRuntime     = "relay.runtime"
	labelFingerprint = "relay.fingerprint"
	labelDependency  = "relay.dependency"
	// labelBootstrap pins the content hash of the runtime-injected bootstrap
	// (and the entrypoint) a function image was built with. Tags are
	// fingerprinted over the FUNCTION DIR only, so an image built by a previous
	// Relay version with an older one-shot bootstrap carries the same tag as a
	// new build would. The label lets Prepare detect stale-bootstrap images
	// holding a current tag and rebuild them (upgrade safety for the
	// persistent invocation protocol).
	labelBootstrap = "relay.bootstrap"
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

// The relay.type values for managed IMAGES. Every managed image carries exactly
// one; the absence of a type (or an unknown type) means an image is NOT managed
// — an unlabeled "relay-fn-*" or "relay-dep-*" image from a pre-labels build is
// unmanaged for classification purposes, and the dependency GC deliberately
// never touches it (function-image cleanup handles legacy function images by
// name; an inert, unlabeled legacy dependency image is left alone and documented
// as out of scope for GC). These are exported so the builder stamps them without
// a stringly-typed duplicate.
const (
	ImageTypeFunction   = "function"
	ImageTypeDependency = "dependency"
)

// imageLabels builds the managed-image label set for a single image. imageType
// is one of ImageTypeFunction / ImageTypeDependency; the remaining fields
// narrow that identity (see labelRuntime / labelFingerprint / labelDependency).
// Empty fields are simply omitted, so the caller can build a function label set
// (type+function+fingerprint+optional dependency) or a dependency label set
// (type+runtime+fingerprint) with one helper. These labels are the strict
// classification the dependency GC reads: an image is a managed function image
// iff its relay.type == ImageTypeFunction and a managed dependency image iff its
// relay.type == ImageTypeDependency.
func imageLabels(imageType string, fnName, runtimeName, fingerprint, dependency string) map[string]string {
	l := map[string]string{labelType: imageType}
	if fnName != "" {
		l[labelFunction] = fnName
	}
	if runtimeName != "" {
		l[labelRuntime] = runtimeName
	}
	if fingerprint != "" {
		l[labelFingerprint] = fingerprint
	}
	if dependency != "" {
		l[labelDependency] = dependency
	}
	return l
}

// functionImageLabels returns the managed function-image label set for a
// function version built from the given dependency reference ("" when the
// function declares no deps) and the bootstrap content hash the image was
// built with.
func functionImageLabels(fnName, fingerprint, dependency, bootstrap string) map[string]string {
	l := imageLabels(ImageTypeFunction, fnName, "", fingerprint, dependency)
	if bootstrap != "" {
		l[labelBootstrap] = bootstrap
	}
	return l
}

// dependencyImageLabels returns the managed dependency-image label set for a
// dependency layer built for the named runtime from the given dependency
// fingerprint.
func dependencyImageLabels(runtimeName, fingerprint string) map[string]string {
	return imageLabels(ImageTypeDependency, "", runtimeName, fingerprint, "")
}

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

// isManagedContainer reports whether labels classify a container as Relay-owned:
// its relay.type is one of the three known values. It is the "touch only
// Relay-owned" guard. Note isManagedContainer does not by itself make a container
// sweep-eligible — services are Relay-owned but reconciler-managed, so the
// sweep excludes them via isServiceContainer.
func isManagedContainer(labels map[string]string) bool {
	t := labels[labelType]
	return t == ContainerTypeEvent || t == ContainerTypeSchedule || t == ContainerTypeService
}
