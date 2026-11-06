package runtime

import "relay/internal/runtime/plan"

// The Relay Docker label model. A single relay.type label classifies every
// Relay-owned container population into exactly one of three disjoint types:
//
//	event    — a one-shot invocation container for a matching event rule
//	          (runner.Handle). relay.handler is the rule handler.
//	schedule — a one-shot invocation container for a schedule occurrence
//	          (runner.InvokeOccurrence). relay.handler is the schedule handler.
//	service  — a persistent, long-lived service container (Manager.StartService).
//	          relay.service is the service identity: the template's stable
//	          service name. relay.identity additionally records the configured
//	          source descriptor (entrypoint file or external image reference),
//	          used only for source resolution and implementation comparison.
//	          Service containers carry relay.service and relay.identity and NO
//	          relay.handler.
//
// relay.handler identifies the handler for event/schedule containers only
// (the one-shot invocation form); service containers instead carry
// relay.service (their stable name) and relay.identity (their source
// descriptor). The sweep and reconciliation predicates rely on relay.type
// being strict: an unknown or missing type is treated as NOT Relay-owned, so
// the strict label set is the single mechanism that keeps
// event/schedule/service populations distinguishable and non-Relay containers
// untouchable. (Backward compatibility with labels from Relay processes older
// than this model is deliberately NOT required: a restart simply treats such
// containers as non-Relay and sweeps/reconciles only typed ones.)
//
// The same strict relay.type model classifies Relay's IMAGES. A single
// relay.type label names either an app image or a dependency image; the
// label set is the source of truth for which images Relay owns and how they
// are wired. relay.fingerprint pins the content version the image was built
// from, relay.app names the owning app, relay.dependency names the
// exact dependency image this app image was built FROM (a full
// "relay-dep-*" repo tag), and relay.runtime names the runtime a dependency
// image was built for. The dependency GC reads exactly these labels and never
// infers ownership from repository names alone.

const (
	labelType      = "relay.type"
	labelApp       = "relay.app"
	labelHandler   = "relay.handler"
	labelMessageID = "relay.message_id"
	labelEventID   = "relay.event_id"
	labelEventName = "relay.event_name"
	labelNodeID    = "relay.node_id"
	labelImage     = "relay.image"

	// labelService is the persistent service's stable identity: the template's
	// service name. It is the grouping key for containers and the logical
	// replica identity; a source change under the same name replaces the same
	// replicas.
	labelService = "relay.service"

	// labelIdentity records the persistent service's configured source
	// descriptor (entrypoint file or external image reference). It is used only
	// for image/entry resolution and desired-implementation comparison, never to
	// identify a service (that is relay.service).
	labelIdentity = "relay.identity"
	labelImageID  = "relay.image_id"
	labelPort     = "relay.port"
	labelReplica  = "relay.replica"

	// labelNetworks records the canonical, sorted, comma-separated set of
	// Docker networks a service container was created attached to: the
	// worker-global NETWORKS set plus the routing network for a routed service.
	// It exists so the service reconciler can detect a network change and
	// replace the stale container. It is omitted for a container with no extra
	// networks, so a service with neither global nor routing networks is
	// byte-for-byte the pre-networks behavior. Docker network names never
	// contain commas, so the comma-joined form is unambiguous. It covers
	// persistent service containers only: execution containers join the
	// worker-global NETWORKS set, which is startup configuration and not tracked
	// per container.
	labelNetworks = "relay.networks"

	// labelSource pins the source fingerprint a HOT_RELOAD service container's
	// live read-only bind mount corresponds to. It is stamped only for a managed
	// entrypoint service whose runtime permits source mounting; the reconciler
	// compares it against the freshly resolved source fingerprint so a source-only
	// edit — which never changes the (source-independent) image reference —
	// restarts the running container in place (or replaces it where in-place
	// restart is unavailable) instead of leaving stale code. It is omitted
	// for every other container, so a container carries it only when it actually
	// has a source mount.
	labelSource = "relay.source"

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

	// labelResources pins the effective per-container resource configuration a
	// service replica was created with: a short digest over the resolved
	// memory/CPU/pids limits (app.ResourceLimits.Fingerprint). It is how a
	// resource-only template change replaces a running service even though the
	// image reference (and image fingerprint) are unchanged, and it holds no raw
	// values. A container created before the label carries none, so it is
	// replaced once.
	labelResources = "relay.resources"

	// Managed-image labels. These pin the identity and wiring of a managed
	// image (app or dependency) so the dependency GC can classify images
	// and resolve app→dependency ownership without inferring anything
	// from repository names. relay.fingerprint holds the content fingerprint
	// prefix the image was built from; relay.dependency is the full
	// "relay-dep-*" reference the app image was built FROM (present on
	// app images only, and only when the app declares deps);
	// relay.runtime names the runtime spec.Name a dependency image was built
	// for (dependency images only).
	labelRuntime     = "relay.runtime"
	labelFingerprint = "relay.fingerprint"
	labelDependency  = "relay.dependency"
	// labelBootstrap pins the content hash of the runtime-injected bootstrap
	// (and the entrypoint) an app image was built with. Tags are
	// fingerprinted over the APP DIR only, so an image built by a previous
	// Relay version with an older one-shot bootstrap carries the same tag as a
	// new build would. The label lets Prepare detect stale-bootstrap images
	// holding a current tag and rebuild them (upgrade safety for the
	// persistent invocation protocol).
	labelBootstrap = "relay.bootstrap"

	// Image provenance labels. These are diagnostic only and never participate
	// in ownership/GC classification (which stays strictly relay.type +
	// relay.app) or in reuse decisions (which stay keyed on the tag and
	// relay.bootstrap). They record the runtime a managed image was built for,
	// the configured source base reference, the selected multi-platform index
	// digest when the runtime is pinned (omitted otherwise), and the explicit
	// target platform. They are stamped on BOTH app and dependency images.
	//
	// They are deliberately separate from labelRuntime: relay.runtime keeps its
	// existing dependency-image-only meaning, and adding these labels must not
	// make an unpinned image look different to the bootstrap reuse probe (it
	// does not: bootstrapHash never includes labels).
	labelProvenanceRuntime     = "relay.provenance.runtime"
	labelProvenanceSourceImage = "relay.provenance.source_image"
	labelProvenanceDigest      = "relay.provenance.digest"
	labelProvenancePlatform    = "relay.provenance.platform"
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
// — an unlabeled "relay-app-*" or "relay-dep-*" image from a pre-labels build is
// unmanaged for classification purposes. Both the dependency GC and the
// app-image cleanup paths (relayTags, AppImageTags, RemoveImagesExcept)
// are strict and label-derived: an unlabeled image is never removed or
// considered a Relay version. These are exported so the builder stamps them
// without a stringly-typed duplicate.
const (
	ImageTypeApp        = "app"
	ImageTypeDependency = "dependency"
)

// imageLabels builds the managed-image label set for a single image. imageType
// is one of ImageTypeApp / ImageTypeDependency; the remaining fields
// narrow that identity (see labelRuntime / labelFingerprint / labelDependency).
// Empty fields are simply omitted, so the caller can build an app label set
// (type+app+fingerprint+optional dependency) or a dependency label set
// (type+runtime+fingerprint) with one helper. These labels are the strict
// classification the dependency GC reads: an image is a managed app image
// iff its relay.type == ImageTypeApp and a managed dependency image iff its
// relay.type == ImageTypeDependency.
func imageLabels(imageType string, fnName, runtimeName, fingerprint, dependency string) map[string]string {
	l := map[string]string{labelType: imageType}
	if fnName != "" {
		l[labelApp] = fnName
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

// appImageLabels returns the managed app-image label set for a
// app version built from the given dependency reference ("" when the
// app declares no deps) and the bootstrap content hash the image was
// built with.
func appImageLabels(fnName, fingerprint, dependency, bootstrap string) map[string]string {
	l := imageLabels(ImageTypeApp, fnName, "", fingerprint, dependency)
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

// imageProvenance is the diagnostic build provenance stamped on a managed app or
// dependency image. Empty fields are omitted. It never affects ownership/GC
// classification or the bootstrap reuse probe.
type imageProvenance struct {
	runtimeName string
	sourceImage string
	digest      string
	platform    string
}

// apply stamps the non-empty provenance fields onto a managed-image label set.
func (p imageProvenance) apply(labels map[string]string) {
	if labels == nil {
		return
	}
	if p.runtimeName != "" {
		labels[labelProvenanceRuntime] = p.runtimeName
	}
	if p.sourceImage != "" {
		labels[labelProvenanceSourceImage] = p.sourceImage
	}
	if p.digest != "" {
		labels[labelProvenanceDigest] = p.digest
	}
	if p.platform != "" {
		labels[labelProvenancePlatform] = p.platform
	}
}

// provenanceFor builds the provenance labels for a runtime image. sourceImage is
// the CONFIGURED base reference (never the pinned ref@digest form), and digest is
// set only when the runtime is pinned. platform is the explicit build target
// (e.g. "linux/amd64").
func provenanceFor(spec plan.Spec, selection imageSelection) imageProvenance {
	p := imageProvenance{
		runtimeName: spec.Name,
		sourceImage: spec.BaseImage,
		platform:    targetPlatformLabel(),
	}
	if selection.pinned() {
		p.sourceImage = selection.sourceImage
		p.digest = selection.digest
	}
	return p
}

// targetPlatformLabel renders the explicit build target platform for provenance
// labels. Relay always builds Linux images, varying only by architecture.
func targetPlatformLabel() string {
	target := buildTargetPlatform()
	return target.OS + "/" + target.Architecture
}

// isServiceContainer reports whether labels classify a container as a
// persistent Relay service container. It is a strict equality on relay.type
// ONLY — it does NOT require relay.app. Services list all service
// containers regardless of app for reconciliation discovery: the stale
// set (an app removed while Relay was down) is identified after discovery
// from each container's App, so requiring relay.app here would
// wrongly exclude a service container whose owning app was deleted.
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
