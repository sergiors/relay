package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	cerrdefs "github.com/containerd/errdefs"

	"relay/internal/app"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
	"relay/internal/runtime/python"
)

// Service container labels beyond the shared registry in labels.go. relay.type
// distinguishes persistent service containers (ContainerTypeService) from
// one-shot invocation containers (event/schedule), so sweeps/reconcilers can
// never confuse the two populations.

// ErrServicePending reports that a service operation was accepted but is not
// yet terminally converged. StartService implementations return it (possibly
// wrapped) when a replacement is running but a backend-specific readiness or
// observation step has not concluded (for example, a Docker healthcheck is still
// starting); the service reconciler treats that as a pending pass rather than a
// failure. The reconciler re-exports this sentinel.
var ErrServicePending = errors.New("service convergence pending")

// ServiceSpec describes one desired service replica's container. The service
// IDENTITY is its stable template name (app.Service.Name): relay.service is
// stamped with it and it is what Reconcile uses to group an app's containers
// by service, so a source change under the same name replaces the same replicas.
// relay.identity additionally records the configured source descriptor
// (app.Service.SourceRef) — the entrypoint file or image reference — which
// is used only to resolve the container image/entry and to compare the desired
// IMPLEMENTATION; it never identifies a service.
type ServiceSpec struct {
	App string
	// Name is the service's stable identity (relay.service): the template's
	// service name. It is the container grouping key and the logical replica
	// identity.
	Name string
	// SourceRef is the configured source descriptor (relay.identity): the
	// entrypoint file or image reference. It is honest for every source kind and
	// is used only for source resolution and implementation comparison.
	SourceRef string
	Port      int
	// BindAddr is the static host IP the container port is published on, or ""
	// for no host publication. The reconciler sets it only for a routed service
	// (one declaring a host) when SERVICE_BIND_ADDR is configured; an unrouted
	// service stays internal-only. Docker allocates the host port and Relay
	// stamps BindAddr as relay.bind_addr so a bind-address change replaces the
	// container. It is never the advertise address, which is presentation
	// metadata only.
	BindAddr string
	Image    string
	// ImageID is the local content ID of Image (relay.image_id), or "" when the
	// reference itself encodes the content (a Relay-built image tag). It lets
	// Reconcile detect a moved external tag pointing at different bytes and
	// replace the container.
	ImageID string
	// Entry is the per-container entrypoint override. Empty means preserve the
	// image's own ENTRYPOINT/CMD (an image source); non-empty is the
	// runtime-resolved command for an entrypoint source.
	Entry []string
	Env   []string // runtime env (plan env), no RELAY_HANDLER
	// Resources are the app's EFFECTIVE per-container resource limits,
	// applied to this service container exactly as to an invocation container.
	// A zero value is normalized to the package defaults, so a hand-built spec
	// never creates a container with a zero (unlimited) limit. The reconciler
	// stamps their fingerprint as relay.resources so a changed spec is detected
	// and the container replaced.
	Resources app.ResourceLimits
	// Tmpfs is the service's configured additional tmpfs destinations (the
	// template service's `tmpfs` list), beyond Relay's always-present /tmp. Each
	// entry is an absolute container path; StartService validates the same
	// policy as template parsing (see app.ValidateServiceTmpfs) and builds the
	// bounded, safe mount options itself, so a direct caller cannot inject mount
	// options. An empty list preserves the historical /tmp-only HostConfig.
	Tmpfs []string
	// Labels are EXTRA labels the caller wants on the container (routing
	// labels supplied by the service reconciler). They are merged onto the
	// relay ownership set, with Relay ownership keys always winning: a caller
	// can never spoof or clobber relay.* labels, which define ownership and
	// discovery.
	Labels map[string]string
	// Networks is the set of Docker networks the container must join at create
	// time: the worker-global NETWORKS set every service container joins, plus
	// the routing network (TRAEFIK_NETWORK) for a routed service. Empty means no
	// extra network (default bridge/network mode only). Each name is applied
	// exactly once (see NetworkSet); order is not significant — Docker's
	// EndpointsConfig is a map and the relay.networks label is canonicalized.
	//
	// The networks themselves are infrastructure owned OUTSIDE Relay — they are
	// never created here — and the caller (the service reconciler) validates the
	// routing network exists before starting routed containers while the worker
	// verifies the global set at startup. A network that disappears after
	// verification surfaces as a create error rather than a chaos fix-up.
	Networks []string
	// SourceMount, when non-nil, is the app's live read-only source mount
	// (HOT_RELOAD). It is set only for an entrypoint service whose runtime
	// permits it; an external `image` service never receives one. Its Identity is
	// stamped as relay.source so the reconciler can detect a source-only change
	// and restart the container in place (or, where in-place restart is
	// unavailable, replace it).
	SourceMount *SourceMount
	// Runtime is the configured template runtime used to derive the healthcheck
	// probe command for an `entrypoint` source. It is empty for a direct
	// ServiceSpec caller with no healthcheck and for an external `image` source,
	// which can never carry a Relay healthcheck.
	Runtime string
	// Healthcheck, when non-nil, is the service's Docker-native HTTP health probe.
	// StartService builds the exec-form test from Runtime and Port; the template
	// parser rejects a healthcheck on an external image source. A HOT_RELOAD
	// source mount bypasses Relay healthcheck behavior entirely.
	Healthcheck *app.ServiceHealthcheck
	// HealthcheckAttempt is the one-based retry generation stamped as
	// relay.healthcheck_attempt when a healthcheck is configured. It is Docker
	// metadata used to reconstruct a bounded retry backoff after a worker restart;
	// 0 (or any value below 1) is normalized to the first attempt.
	HealthcheckAttempt int
}

// ServiceContainer is one discovered service container, as stamped on its
// labels. NodeID is the node identity that created it (relay.node_id).
// Discovery is deliberately unscoped, but a worker-wired ServiceReconciler
// compares it to its stable NODE_ID so it only converges, adopts,
// and removes its OWN containers: a restart of the same NODE_ID still
// reconciles them, while another worker's service containers on a shared daemon
// are left untouched.
type ServiceContainer struct {
	ID  string
	App string
	// Name is the service's stable identity, parsed from its relay.service
	// label: the template's service name. It is the grouping key Reconcile uses
	// and the logical replica identity.
	Name string
	// SourceRef is the container's configured source descriptor, parsed from its
	// relay.identity label (the entrypoint file or image reference). It is used
	// only to compare the desired implementation; it never identifies a service.
	SourceRef string
	Image     string
	// ImageID is the local content ID of Image, parsed from relay.image_id ("").
	ImageID string
	NodeID  string
	State   container.ContainerState
	Replica int
	// Port is the container's configured internal port, parsed from its
	// relay.port label. It defaults to 0 when the label is missing/invalid.
	// The service reconciler uses it to detect a port change (a stale-config
	// container whose labelPort != the template's desired port is replaced).
	Port int
	// BindAddr is the static host IP the container published its port on,
	// parsed from relay.bind_addr. It is "" for a container with no host
	// publication (every unrouted service, and every routed service while
	// SERVICE_BIND_ADDR is unset). The service reconciler compares it to the
	// desired bind address and replaces the container when it differs; the
	// advertise address is never part of this identity.
	BindAddr string
	// EnvironmentFingerprint is the identity of the effective environment the
	// container was created with, parsed from relay.environment. It is "" for a
	// container created before the label existed (or with a missing/invalid value),
	// which never equals the desired fingerprint, so such a container is replaced
	// once — exactly like a missing relay.image_id or relay.port.
	EnvironmentFingerprint string
	// Resources is the effective resource configuration's fingerprint the
	// container was created with, parsed from relay.resources. It is "" for a
	// container created before the label existed, which never equals a desired
	// fingerprint, so such a container is replaced once.
	Resources string
	// Tmpfs is the canonical, sorted, comma-separated list of additional tmpfs
	// destinations the container was created with, parsed from relay.tmpfs. It
	// is "" for a service with no additional tmpfs (the historical /tmp-only
	// case). The service reconciler compares it to the desired configured list
	// and replaces the container when it differs; /tmp is always present and is
	// deliberately not part of this value.
	//
	// The label is the only configuration identity discovery can compare here:
	// a ContainerList summary exposes only HostConfig.NetworkMode/Annotations —
	// not HostConfig.Tmpfs — and ContainerInspect.Mounts does not report tmpfs
	// mounts on every Engine, so observing the live mount table would require an
	// extra inspect per container on every reconcile. The immutable label is
	// stamped from the exact destinations StartService applied.
	Tmpfs string
	// Networks is the canonical, sorted, comma-separated set of Docker
	// networks the container was created attached to (the worker-global
	// NETWORKS set plus the routing network for a routed service), parsed from
	// relay.networks. It is "" for a container with no networks (or one created
	// before the label existed). The service reconciler compares it to the
	// desired set to detect a network change.
	Networks string
	// Healthcheck is the canonical identity of the healthcheck configuration the
	// container was created with, parsed from relay.healthcheck. It is "" for a
	// service with no healthcheck (and for a container created before the label
	// existed), including every HOT_RELOAD container.
	Healthcheck string
	// HealthcheckAttempt is the one-based retry generation parsed from
	// relay.healthcheck_attempt. It is Docker-owned retry metadata used to
	// reconstruct bounded backoff after a restart; 0 means unlabeled.
	HealthcheckAttempt int
	// CreatedAt is the container's Docker create time (the daemon's durable
	// metadata). It is create metadata only: a container can be created well
	// before it is started, so readiness deadlines are derived from StartedAt
	// instead.
	CreatedAt time.Time
	// StartedAt is the instant Docker actually started the container. It is the
	// durable reference for a healthcheck candidate's startup grace: create time
	// cannot represent a delayed create→start, and Docker reports the start
	// instant with nanosecond precision. It is zero when the daemon reports no
	// usable start time (never started, or the inspect needed to read it failed);
	// callers must then fall back conservatively instead of substituting
	// CreatedAt.
	StartedAt time.Time
	// Health is Docker's current health status for a container that carries
	// Relay's own healthcheck label: "starting", "healthy", or "unhealthy". It
	// is "" when the service has no Relay healthcheck, including an external
	// image's own image-declared HEALTHCHECK, in which case the historical
	// running-is-converged behavior applies. Ready is derived from it.
	Health container.HealthStatus
	// SourceID is the source fingerprint a HOT_RELOAD container's live
	// read-only bind mount corresponds to, parsed from relay.source. It is "" for
	// a container with no source mount (including every container created before
	// HOT_RELOAD, and every external-image service). The service reconciler
	// compares it to the desired fingerprint to detect a container whose mounted
	// source has changed under an unchanged image reference and restart it in
	// place (or replace it where in-place restart is unavailable).
	SourceID string
	// Labels is the container's FULL label set (nil-safe; nil when the
	// container has none). The service reconciler compares routing metadata
	// through it without Relay parsing or interpreting foreign label names.
	Labels map[string]string
	// Ready is the backend's CURRENT readiness observation for a running
	// service container. nil means the backend reports no readiness signal,
	// preserving Docker's historical behavior where a running container is
	// converged. A non-nil false reports that the backend accepted the
	// replacement but has not terminally converged it yet; the service
	// reconciler then keeps the pass pending instead of starting a duplicate
	// replacement or stopping the previous generation. An explicitly unready
	// container is also never an in-place restart target, never counts as an
	// actually-ready survivor for the stopping safety net, and is not treated
	// as converged after a restart. For a service with a healthcheck it is
	// derived from Docker's health summary (healthy => true, starting/unhealthy
	// => false); for a service without one it remains nil.
	Ready *bool
}

// ServiceEndpoint is one service replica as observed on this worker's Docker
// daemon, plus the host endpoint Docker actually assigned for its container
// port. It is a live read-only view for `relay app inspect`; nothing here is
// persisted.
//
// HostIP and HostPort are filled ONLY when inspection proves a complete,
// expected binding: the container exposes the service's internal TCP port, has
// exactly one host binding for it, and that binding targets the configured bind
// address. Any absent, malformed, or unexpected binding leaves HostPort 0 and
// HostIP empty so callers render "not published" instead of guessing. State and
// Health remain Docker's actual observation even then (and for a stopped
// container), so callers can distinguish an inactive container from a missing
// or unpublished endpoint.
type ServiceEndpoint struct {
	Service     string
	Replica     int
	ContainerID string
	State       container.ContainerState
	// Missing reports that the container disappeared between discovery and
	// inspection. It carries no State; callers render it as not found rather
	// than substituting a stale label-derived state.
	Missing  bool
	Health   container.HealthStatus
	Port     int
	HostIP   string
	HostPort int
}

// serviceContainerNameLenCap caps the deterministic (logical) container name of
// one service replica. Ownership is always derived from labels, never from a
// name (names are not guaranteed unique/stable across restarts), so the name is
// purely for human greppability. The service identity part is sanitized to
// [A-Za-z0-9_.-] and the total is kept well under Docker's name length limit.
const serviceContainerNameLenCap = 100

// serviceContainerNameTokenCap bounds the per-start uniqueness token appended
// to the PHYSICAL container name (see serviceContainerNameForStart). It keeps
// the physical name (serviceContainerNameLenCap + 1 + token cap) far below
// Docker's 255-byte name limit. The actual token is newRequestID's
// fixed-length random hex string (12 chars), comfortably inside this cap.
const serviceContainerNameTokenCap = 16

// serviceIdentityHashLen is the number of hex characters in the
// collision-resistant suffix appended to a generated service container name. 16
// hex chars = 64 bits, matching the short-handle convention used for Relay's
// content-addressed image tags (see tagPrefixLen) and the routing layer's
// provider-id suffix. The routing and runtime layers derive their suffixes
// independently (routing is a leaf package that runtime does not import); both
// use the same full-input SHA-256/NUL-separated construction so the two stay
// consistent. A birthday collision at Relay's scale is astronomically unlikely.
const serviceIdentityHashLen = 16

// sanitizeContainerNamePart replaces any character outside [A-Za-z0-9_.-] with
// '-'. App names are already validated to a legal docker repo charset, but
// the service identity is an arbitrary descriptor (a file path or image
// reference), so it is sanitized defensively.
func sanitizeContainerNamePart(s string) string {
	var b strings.Builder
	for _, r := range s {
		ok := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-'
		if ok {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return b.String()
}

// serviceContainerName derives the deterministic LOGICAL container name for a
// replica: relay-svc-<app>-<name>-<hash>-<replica>. App and service
// names are both validated to a legal docker repo charset, so the name part is
// sanitized only defensively. The name ends with a collision-resistant hash
// suffix derived from the FULL app name and service name, so distinct names
// that sanitize to the same readable base still get distinct names. The readable
// base is trimmed to make room for the suffix, so the suffix and the replica
// index are never lost to the cap.
//
// This is the stable, human-greppable logical name of a replica slot. It is NOT
// the physical name a container is created with: a replacement generation must
// be able to coexist with the generation it replaces (create/start/inspect the
// new container before the old one is stopped), and Docker rejects duplicate
// container names. StartService therefore creates the container under
// serviceContainerNameForStart, which appends a per-start uniqueness token.
func serviceContainerName(appName, serviceName string, replica int) string {
	suffix := "-" + serviceNameHash(appName, serviceName) + "-" + strconv.Itoa(replica)
	base := "relay-svc-" + appName + "-" + sanitizeContainerNamePart(serviceName)
	maxBase := max(serviceContainerNameLenCap-len(suffix), 0)
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + suffix
}

// serviceContainerNameForStart returns the PHYSICAL Docker name for one
// StartService create: the deterministic logical name (serviceContainerName)
// plus a per-start uniqueness token. The token lets a replacement container
// coexist with the container it replaces during a zero-downtime swap (Docker
// refuses duplicate names otherwise) and is discarded with the container.
//
// The token is random (newRequestID) — never derived from configuration — so
// the resulting name is deliberately NOT stable across starts. That is safe
// because Relay never derives ownership, grouping, or a replica slot from a
// container name: every predicate is label-derived (relay.type, relay.app,
// relay.service, relay.replica). The physical name remains greppable (it keeps
// the logical prefix and replica suffix) and is bounded Docker-safe: the
// logical name is capped at serviceContainerNameLenCap and the token at
// serviceContainerNameTokenCap, so the physical name is always shorter than
// Docker's name limit.
func serviceContainerNameForStart(appName, serviceName string, replica int) string {
	return serviceContainerName(appName, serviceName, replica) + "-" + newRequestID()
}

// serviceContainerNamePhysicalLenCap is the upper bound on a physical service
// container name: the logical cap plus the separator and the token cap. It is
// the value the uniqueness test asserts against, and it stays far below
// Docker's 255-byte name limit.
const serviceContainerNamePhysicalLenCap = serviceContainerNameLenCap + 1 + serviceContainerNameTokenCap

// serviceNameHash returns the fixed-length hex collision-resistant suffix for a
// service name. It hashes the FULL app name and the FULL service name
// (never the sanitized or truncated base) with a NUL separator, so the two
// inputs cannot run together across the join and distinct names hash apart even
// when their sanitized bases coincide.
func serviceNameHash(appName, serviceName string) string {
	sum := sha256.Sum256([]byte(appName + "\x00" + serviceName))
	return hex.EncodeToString(sum[:])[:serviceIdentityHashLen]
}

// NetworkSet normalizes a network list into the de-duplicated, SORTED slice of
// non-empty names Relay treats as a set. It is the single normalization point
// behind both the Docker NetworkingConfig and the canonical relay.networks
// label, so a caller cannot introduce an order-dependent or repeated network.
// An empty/all-empty input yields nil.
func NetworkSet(networks []string) []string {
	seen := make(map[string]bool, len(networks))
	out := make([]string, 0, len(networks))
	for _, n := range networks {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

// serviceEndpoints builds the Docker NetworkingConfig EndpointsConfig for a
// service container from its normalized network set (see NetworkSet): every
// named network exactly once. The result is keyed by network name — Docker's
// EndpointsConfig is itself a map, so there is no meaningful iteration order and
// none is relied on. An empty result means no NetworkingConfig is sent (default
// bridge/network mode only).
func serviceEndpoints(networks []string) map[string]*network.EndpointSettings {
	set := NetworkSet(networks)
	if len(set) == 0 {
		return nil
	}
	endpoints := make(map[string]*network.EndpointSettings, len(set))
	for _, n := range set {
		endpoints[n] = &network.EndpointSettings{}
	}
	return endpoints
}

// NetworksLabel returns the canonical value of the relay.networks label for a
// container joining the given networks: the SORTED, de-duplicated,
// comma-separated set. An empty result means the container joins no extra
// network and the label is omitted. Sorting makes the value independent of
// input field order, so a reconciler comparing a desired value to a discovered
// label sees equality whenever the SET of networks matches.
//
// It is used for persistent service containers, whose networks are the
// worker-global set (NETWORKS) plus the routing layer's (TRAEFIK_NETWORK) for a
// routed service; the canonical form makes the comparison order- and
// duplicate-invariant.
func NetworksLabel(networks ...string) string {
	set := NetworkSet(networks)
	if len(set) == 0 {
		return ""
	}
	return strings.Join(set, ",")
}

// EnvironmentFingerprint returns the environment identity used by service
// reconciliation to determine whether a service container's effective environment
// has changed.
func EnvironmentFingerprint(env []string) string {
	h := sha256.New()
	for _, kv := range env {
		_, _ = h.Write([]byte(kv))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:serviceIdentityHashLen]
}

// ServiceTmpfsLabel returns the canonical value of a service's relay.tmpfs
// label: the sorted, de-duplicated, comma-separated list of additional tmpfs
// destinations. An empty result means the service declares no additional tmpfs
// and the label is omitted, so an unaffected service keeps its historical label
// set. Container paths cannot contain a comma under the template policy (only
// /run and /var/cache subtrees are accepted), so the joined form is
// unambiguous. The value is the configuration itself — not security state and
// not a version marker — so the reconciler can compare it directly.
func ServiceTmpfsLabel(destinations []string) string {
	if len(destinations) == 0 {
		return ""
	}
	seen := make(map[string]bool, len(destinations))
	set := make([]string, 0, len(destinations))
	for _, destination := range destinations {
		if destination == "" || seen[destination] {
			continue
		}
		seen[destination] = true
		set = append(set, destination)
	}
	if len(set) == 0 {
		return ""
	}
	sort.Strings(set)
	return strings.Join(set, ",")
}

// serviceLabels is the total, greppable label set stamped on every service
// container: relay.type=service plus relay.app + relay.node_id make a
// service container recognizable to Relay while the strict relay.type guard
// lets sweeps/reconcilers distinguish the service population from one-shot
// invocation containers. relay.service is the service IDENTITY (the template's
// stable service name) and is the grouping key Reconcile uses; relay.identity
// additionally records the configured source descriptor (entrypoint file or
// image reference) for implementation comparison and image resolution. Service
// containers carry no relay.handler. relay.image_id records the local content ID
// the container was started from (empty when the reference itself is
// content-addressed), so a moved external tag is detected. relay.environment pins
// the effective environment fingerprint so an env/secret change replaces a
// container whose image reference is unchanged (see labelEnvironment).
// relay.resources and relay.tmpfs pin the effective per-container resources and
// the configured additional tmpfs destinations, so a change to either replaces
// a running service. relay.healthcheck pins the configured healthcheck identity
// (path and timings) so a probe-only change replaces the running container
// without a rebuild; it is omitted for a service with no healthcheck.
//
// Extra caller-supplied labels (spec.Labels, e.g. routing labels) are merged
// on top, then the relay ownership keys are RE-applied last so Relay's
// ownership labels are always authoritative: a caller can add labels but never
// clobber or spoof a relay.* key.
func serviceLabels(spec ServiceSpec, nodeID string, replica int) map[string]string {
	environmentFingerprint := EnvironmentFingerprint(spec.Env)
	resourceHash := spec.Resources.OrDefault().Fingerprint()
	tmpfsLabel := ServiceTmpfsLabel(spec.Tmpfs)
	// A HOT_RELOAD source mount bypasses Relay healthcheck behavior entirely:
	// no Docker healthcheck configuration and no healthcheck metadata are
	// stamped, so a healthcheck-only template edit cannot make the container
	// stale or replace it under HOT_RELOAD.
	effectiveHealthcheck := spec.Healthcheck
	if spec.SourceMount != nil {
		effectiveHealthcheck = nil
	}
	healthcheckLabel := ServiceHealthcheckLabel(effectiveHealthcheck)
	healthcheckAttempt := 0
	if effectiveHealthcheck != nil {
		healthcheckAttempt = spec.HealthcheckAttempt
		if healthcheckAttempt < 1 {
			healthcheckAttempt = 1
		}
	}
	sourceID := ""
	if spec.SourceMount != nil {
		sourceID = spec.SourceMount.Identity
	}
	labels := map[string]string{
		labelType:        ContainerTypeService,
		labelApp:         spec.App,
		labelService:     spec.Name,
		labelIdentity:    spec.SourceRef,
		labelImage:       spec.Image,
		labelNodeID:      nodeID,
		labelPort:        strconv.Itoa(spec.Port),
		labelReplica:     strconv.Itoa(replica),
		labelEnvironment: environmentFingerprint,
		labelResources:   resourceHash,
	}
	if healthcheckLabel != "" {
		labels[labelHealthcheck] = healthcheckLabel
	}
	if healthcheckAttempt > 0 {
		labels[labelHealthcheckAttempt] = strconv.Itoa(healthcheckAttempt)
	}
	if spec.ImageID != "" {
		labels[labelImageID] = spec.ImageID
	}
	if networks := NetworksLabel(spec.Networks...); networks != "" {
		labels[labelNetworks] = networks
	}
	if sourceID != "" {
		labels[labelSource] = sourceID
	}
	maps.Copy(labels, spec.Labels)
	labels[labelType] = ContainerTypeService
	labels[labelApp] = spec.App
	labels[labelService] = spec.Name
	labels[labelIdentity] = spec.SourceRef
	labels[labelImage] = spec.Image
	labels[labelNodeID] = nodeID
	labels[labelPort] = strconv.Itoa(spec.Port)
	labels[labelReplica] = strconv.Itoa(replica)
	labels[labelEnvironment] = environmentFingerprint
	labels[labelResources] = resourceHash
	// The tmpfs configuration is identity metadata too; re-apply it last and
	// clear a spoofed value when the service declares no additional tmpfs.
	if tmpfsLabel != "" {
		labels[labelTmpfs] = tmpfsLabel
	} else {
		delete(labels, labelTmpfs)
	}
	// The healthcheck configuration is identity metadata too; re-apply it last
	// and clear a spoofed value when the service declares no healthcheck. The
	// attempt is retry metadata, never desired configuration: it is re-applied
	// for integrity but does not participate in the reconciler's config match.
	if healthcheckLabel != "" {
		labels[labelHealthcheck] = healthcheckLabel
		if healthcheckAttempt > 0 {
			labels[labelHealthcheckAttempt] = strconv.Itoa(healthcheckAttempt)
		} else {
			delete(labels, labelHealthcheckAttempt)
		}
	} else {
		delete(labels, labelHealthcheck)
		delete(labels, labelHealthcheckAttempt)
	}
	// The image content ID is an ownership key too; a caller can never spoof it,
	// and an empty ID (a content-addressed Relay tag) clears any spoofed value.
	if spec.ImageID != "" {
		labels[labelImageID] = spec.ImageID
	} else {
		delete(labels, labelImageID)
	}
	// The networks label is ownership metadata too: re-apply it last so a
	// caller-supplied label can never spoof the container's actual networks,
	// and clear a spoofed value when the spec declares none.
	if networks := NetworksLabel(spec.Networks...); networks != "" {
		labels[labelNetworks] = networks
	} else {
		delete(labels, labelNetworks)
	}
	// The bind address is configuration identity too; re-apply it last and
	// clear a spoofed value when the service publishes no host port. The
	// advertise address is deliberately never stamped.
	if spec.BindAddr != "" {
		labels[labelBindAddr] = spec.BindAddr
	} else {
		delete(labels, labelBindAddr)
	}
	// The source fingerprint is ownership metadata too; re-apply it last and
	// clear a spoofed value when the service has no source mount.
	if sourceID != "" {
		labels[labelSource] = sourceID
	} else {
		delete(labels, labelSource)
	}
	return labels
}

// serviceReplicaAttrs returns the correlation attributes shared by the
// per-container service lines. Identity a container does not carry — an old
// container with no relay.replica/relay.image label, or a direct caller's empty
// spec image — is omitted rather than logged as an empty or sentinel value.
func serviceReplicaAttrs(appName, service, image string, replica int) []any {
	attrs := make([]any, 0, 8)
	if appName != "" {
		attrs = append(attrs, "app", appName)
	}
	if service != "" {
		attrs = append(attrs, "service", service)
	}
	if replica >= 0 {
		attrs = append(attrs, "replica", replica)
	}
	if image != "" {
		attrs = append(attrs, "image", image)
	}
	return attrs
}

// parseServiceBindAddr validates the optional static host IP a service port is
// published on. Empty means no publication and returns the zero Addr. The
// policy mirrors config.ParseServiceAddress: only a usable unicast IPv4 or IPv6
// literal is accepted, never a hostname, wildcard, multicast, broadcast,
// IPv4-mapped, or zone-scoped address. It is repeated here as the last line of
// defense for direct StartService callers; configuration is the user-facing
// validation point.
func parseServiceBindAddr(value string) (netip.Addr, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return netip.Addr{}, nil
	}
	addr, err := netip.ParseAddr(v)
	if err != nil ||
		addr.Is4In6() ||
		addr.Zone() != "" ||
		addr.IsUnspecified() ||
		addr.IsMulticast() ||
		(!addr.IsGlobalUnicast() && !addr.IsLoopback() && !addr.IsLinkLocalUnicast()) {
		return netip.Addr{}, fmt.Errorf("invalid bind address %q", value)
	}
	return addr, nil
}

// StartService creates and starts ONE persistent service container. spec.Env is
// applied verbatim (container env semantics — the caller must assemble it,
// including PORT=<spec.Port> plus the template env values and resolved secrets;
// nothing here resolves secrets). The hardened HostConfig mirrors runContainer
// EXCEPT AutoRemove is deliberately FALSE: a service container is persistent and
// long-lived, so the reconciler owns its removal, never the daemon. The internal
// TCP port is always exposed as container metadata (ExposedPorts). When
// spec.BindAddr is non-empty the port is additionally published on that exact
// host IP with a Docker-allocated host port; the caller (the service reconciler)
// supplies a bind address only for a routed service while SERVICE_BIND_ADDR is
// configured, so an unrouted service remains internal-only.
//
// After ContainerStart, StartService performs a synchronous, deterministic Docker
// INSPECT and requires the container to report State.Running. Return only after
// that confirmation: the service reconciler uses a successful StartService as
// the gate for stopping the generation it replaces, so a container that created
// and exited immediately (a crash-on-boot, a bad command) must NEVER be treated
// as a viable replacement. No sleep or polling is involved — a single inspect
// is the daemon's authoritative answer at that instant. When the spec carries a
// healthcheck, a running container that Docker does not yet report as healthy is
// returned with ErrServicePending (the candidate is retained, not discarded), so
// the reconciler keeps the old generation serving until a later observation.
//
// On any error after create (create→start failure, or a start that reports
// success but inspects as non-running/absent), the created container is removed
// best-effort: a removal failure is logged but never replaces the more
// important create/start/inspect error, so the caller keeps the old generation.
// Removal uses removeContainerContext, which honors the caller's
// cancellation/deadline and treats not-found/conflict as benign. The physical
// name is unique per start (serviceContainerNameForStart) so a replacement can
// coexist with the container it replaces during a zero-downtime swap.
//
// Service containers deliberately do NOT go through the per-app execution
// containerCache: they are not leased, pooled, warmed, or generation-drained.
// The cache exists for one-shot invocation containers (Execute), each leased for
// one event and reused across invocations; a persistent service container is
// owned and reconciled by the service reconciler (see internal/reconciler), which
// starts one container per replica and replaces it when its labels (image
// content, env, resources, networks) go stale. A resource-only service change
// therefore replaces the container at the reconciler's label-comparison layer,
// not via a cache generation. This applies equally to external-image and
// entrypoint sources.
//
// One image serves both invocations and services: Manager.Prepare builds the
// app image (its ENTRYPOINT is the invocation bootstrap), and the service
// entrypoint is overridden per-container via spec.Entry. This keeps image
// retirement grouped under AppImageTags with no separate service images.
// An `image` source instead runs its external reference with Entry empty.
func (m *Manager) StartService(ctx context.Context, spec ServiceSpec, replica int) (string, error) {
	// Admit the whole create+start+inspect window as ONE Manager-owned Docker
	// operation: manager shutdown refuses a new start and cancels an in-flight
	// one, and CloseContext drains it before the Docker client closes.
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return "", fmt.Errorf("service: %w", err)
	}
	defer release()
	callerCtx := ctx
	ctx = opCtx

	// Refuse a selected control network before any image or container work.
	// This is the enforcement point shared by every persistent-service create;
	// the equivalent check for execution containers runs in Manager.startContainer.
	if err := m.validateWorkloadNetworks(ctx, spec.Networks); err != nil {
		return "", fmt.Errorf("service: %w", err)
	}

	// Validate and canonicalize the optional host bind address BEFORE any
	// container config is built. The reconciler supplies a startup-validated
	// address, but this is also the enforcement point for a direct caller: a
	// wildcard, multicast, broadcast, mapped, or zone-scoped value is refused
	// rather than silently widening publication. Canonicalizing here keeps
	// relay.bind_addr spelling-invariant with configuration.
	bindAddr, err := parseServiceBindAddr(spec.BindAddr)
	if err != nil {
		return "", fmt.Errorf("service: %w", err)
	}
	if bindAddr.IsValid() {
		spec.BindAddr = bindAddr.String()
	} else {
		spec.BindAddr = ""
	}

	// Pin the service image for the whole create+start window. For a Relay-owned
	// image (an entrypoint-source service runs the app image) this prevents
	// a concurrent retirement from committing removal between the caller's
	// reference check and the container create. admitLease uses the request's
	// admitted lease carried on ctx (a service pass that enqueued with a lease)
	// only when it actually ENTITLES spec.Image, and the service takes its OWN
	// child SHARE of it: the image stays admitted even while retirement drains,
	// and even if the caller releases concurrently. A lease for a different
	// image, or a released one, does not entitle this service, so an independent
	// lease is acquired instead; a retirement in progress then rejects it with a
	// retryable ErrImageRetiring. External images (never Relay-owned) are not
	// leased at all — Relay must never GC them.
	if IsRelayImage(spec.Image) {
		lease, err := m.admitLease(ctx, spec.Image)
		if err != nil {
			return "", fmt.Errorf("service: %w", err)
		}
		if lease != nil {
			defer lease.Release()
		}
	}

	cfg := &container.Config{
		Image:  spec.Image,
		Env:    spec.Env,
		Labels: serviceLabels(spec, m.nodeID, replica),
		// ExposedPorts describes the container's TCP port. Host publication is
		// added below only when the caller supplied a bind address; an unrouted
		// service remains internal-only.
		ExposedPorts: network.PortSet{network.MustParsePort(fmt.Sprintf("%d/tcp", spec.Port)): {}},
	}
	if spec.Healthcheck != nil && spec.SourceMount == nil {
		// Docker's native HEALTHCHECK, implemented with the runtime's own
		// node/python executable. It runs inside the hardened container with no
		// shell involved. A healthcheck is valid only for a runtime-managed
		// entrypoint source: an external image has no Entry override and cannot be
		// assumed to provide probe tooling, so a direct ServiceSpec caller fails
		// closed just like the template parser. HOT_RELOAD bypasses Relay
		// healthchecks entirely (no Docker probe and no readiness gate).
		if len(spec.Entry) == 0 {
			return "", fmt.Errorf("service: healthcheck requires an entrypoint source")
		}
		healthConfig, err := serviceHealthConfig(spec.Runtime, spec.Port, spec.Healthcheck)
		if err != nil {
			return "", fmt.Errorf("service: %w", err)
		}
		cfg.Healthcheck = healthConfig
	}
	if len(spec.Entry) > 0 {
		// An entrypoint-source service overrides the app image's
		// invocation-bootstrap entrypoint with the long-lived service command
		// (e.g. ["node", "/app/service.js"]). An image service leaves
		// Entry empty so the container preserves the image's own
		// ENTRYPOINT/CMD.
		cfg.Entrypoint = spec.Entry
	}
	// A HOT_RELOAD entrypoint service runs with the app's source root as its
	// working directory (Node's /app/src), matching the pooled runtime so
	// process.cwd() and relative paths see the app root. Empty preserves the
	// image WORKDIR (/app) exactly.
	if spec.SourceMount != nil {
		cfg.WorkingDir = spec.SourceMount.WorkDir
	}

	// The resolved resource limits apply exactly as they do to an invocation
	// container. A declared privileged port adds only NET_BIND_SERVICE on top
	// of the shared CapDrop ALL baseline. Additional service tmpfs mounts are
	// added here, before create: StartService validates the same destination
	// policy as template parsing and builds the bounded option strings itself.
	hostConfig := hardenedServiceHostConfig(spec.Resources, spec.Port)
	if bindAddr.IsValid() {
		// Publish the service's TCP port on the configured static host IP.
		// The host port is deliberately blank so Docker allocates a free one;
		// Relay reads the actual assignment from Docker for inspection.
		hostConfig.PortBindings = network.PortMap{
			network.MustParsePort(fmt.Sprintf("%d/tcp", spec.Port)): {
				{HostIP: bindAddr},
			},
		}
	}
	if err := applyServiceTmpfs(
		hostConfig, spec.Tmpfs, spec.Resources,
	); err != nil {
		return "", fmt.Errorf("service: %w", err)
	}

	createOps := client.ContainerCreateOptions{
		Config: cfg,
		// No AutoRemove: persistent, reconciler-owned (see doc comment).
		HostConfig: hostConfig,
		// A unique physical name per start, so the replacement generation can
		// be created while the generation it replaces is still running.
		Name: serviceContainerNameForStart(spec.App, spec.Name, replica),
	}
	sourceMounts := bindMounts(spec.SourceMount)
	sourceRoot := ""
	if spec.SourceMount != nil {
		sourceRoot = spec.SourceMount.HostPath
	}
	// A HOT_RELOAD entrypoint service mounts its live source read-only; an
	// external `image` service never does, so its HostConfig is byte-for-byte the
	// pre-HOT_RELOAD shape.
	if len(sourceMounts) > 0 {
		createOps.HostConfig.Mounts = sourceMounts
	}
	if err := validateContainerHostConfig(
		createOps.HostConfig, sourceMounts, spec.Port, sourceRoot,
	); err != nil {
		return "", fmt.Errorf("service: unsafe container configuration: %w", err)
	}
	// Docker materializes an image-declared VOLUME as an anonymous writable
	// mount even under ReadonlyRootfs, so refuse it before create just like an
	// unsafe HostConfig. This is the enforcement point for external `image`
	// services as well as Relay-built service images.
	inspectImage := spec.Image
	if spec.ImageID != "" {
		// Mutable external references are resolved by the reconciler before the
		// start pass. Inspect and create from that same content ID, never from a
		// tag that could move between validation and ContainerCreate.
		inspectImage = spec.ImageID
	}
	createImage, err := validateImageDeclaredVolumes(
		ctx, m.cli, inspectImage, spec.Tmpfs...)
	if err != nil {
		return "", fmt.Errorf("service: %w", err)
	}
	// Docker follows container-rootfs symlinks when applying HostConfig.Tmpfs,
	// so a path allowlist alone is not sufficient. Prove against the exact
	// immutable image content that no configured tmpfs target (or parent) is a
	// symlink or other non-directory redirection before create. This runs after
	// the image identity is pinned and before ContainerCreate; no image code is
	// executed, and an unreadable/undecodable archive fails closed. Successful
	// results are reused per immutable image ID and exact destination set, and
	// concurrent same-pair requests coalesce, so replicas do not each stream the
	// image archive. Failures are never reused.
	if err := m.validateImageTmpfsTargetsCached(
		ctx, createImage, spec.Tmpfs,
	); err != nil {
		return "", fmt.Errorf("service: %w", err)
	}
	if err := validateRecursiveReadOnlyMountSupport(m.cli, sourceMounts); err != nil {
		return "", fmt.Errorf("service: unsafe container configuration: %w", err)
	}
	// Keep relay.image and relay.identity tied to the configured source reference;
	// Config.Image is the immutable ID inspected above.
	cfg.Image = createImage
	if endpoints := serviceEndpoints(spec.Networks); len(endpoints) > 0 {
		// Join every configured network at create time (containers must belong to
		// a network from creation to be on it at start). The worker-global
		// NETWORKS set applies to service containers exactly as it does to
		// execution containers; a routed service additionally joins the routing
		// network. All are infrastructure owned OUTSIDE Relay — they are never
		// created here — and the caller (the service reconciler for the routing
		// network, the worker at startup for the global set) validates them
		// before starting containers; a network that disappears after
		// verification surfaces as a create error below rather than a chaos
		// fix-up.
		createOps.NetworkingConfig = &network.NetworkingConfig{EndpointsConfig: endpoints}
	}
	createResp, err := m.cli.ContainerCreate(ctx, createOps)
	if err != nil {
		return "", fmt.Errorf("service: create container: %w", m.classifyDockerErr(callerCtx, err))
	}
	id := createResp.ID

	// Best-effort cleanup of a created-but-not-confirmed container. It never
	// masks the primary error: a removal failure is logged so a partial
	// replacement is observable, and the next reconcile can clean it up (it is
	// a non-running relay.identity/relay.replica container, so it classifies as
	// stale).
	discard := func(reason string, cause error) error {
		if rmErr := removeContainerContext(ctx, m.cli, id); rmErr != nil {
			attrs := serviceReplicaAttrs(spec.App, spec.Name, spec.Image, replica)
			attrs = append(attrs,
				"container", id,
				"reason", reason,
				"error", rmErr,
			)
			if serviceOperationCancelled(ctx, rmErr) {
				attrs = append(attrs, "outcome", "cancelled")
				m.log.Debug("Service: discard failed container", attrs...)
			} else {
				m.log.Warn("Service: discard failed container", attrs...)
			}
		}
		return cause
	}

	if _, err := m.cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		// Created but never started; never leaves a live service behind.
		return "", discard("start_error", fmt.Errorf("service: start container: %w", m.classifyDockerErr(callerCtx, err)))
	}

	// The running gate: confirm the started container is actually running before
	// reporting success. A container that created and started but immediately
	// exited (crash-on-boot) must not be handed back as a replacement, or the
	// reconciler would stop the old generation and take the service down.
	insp, err := m.cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return "", discard("inspect_error", fmt.Errorf("service: inspect started container: %w", m.classifyDockerErr(callerCtx, err)))
	}
	if insp.Container.State == nil || !insp.Container.State.Running {
		status := ""
		if insp.Container.State != nil {
			status = string(insp.Container.State.Status)
		}
		if cfg.Healthcheck != nil {
			// The candidate exists and carries its durable attempt/identity labels. Do
			// NOT discard it: the reconciler must observe the exited candidate, persist
			// its failure instant, and enter bounded retry while preserving the old
			// replica. No-healthcheck starts keep the historical discard-and-fail path.
			m.log.Info("Service: started; exited before readiness",
				"app", spec.App,
				"service", spec.Name,
				"replica", replica,
				"image", spec.Image,
				"container", id,
				"state", status,
				"outcome", "pending",
			)
			return id, fmt.Errorf(
				"%w: container %s exited before readiness", ErrServicePending, id)
		}
		return "", discard("not_running", fmt.Errorf("service: started container is not running"))
	}

	// A published service is only a viable replacement once Docker reports the
	// expected host mapping. A container can be running while the daemon has not
	// materialized (or has mangled) the requested binding; handing it back as
	// success would let the reconciler stop the old, still-serving generation
	// despite having no verified published endpoint. The immediate post-start
	// inspect is authoritative: no polling and no ephemeral-port assumption.
	// Unpublished services keep the historical behavior below.
	if bindAddr.IsValid() {
		if _, _, ok := servicePublishedBinding(
			insp.Container, spec.Port, bindAddr,
		); !ok {
			return "", discard("publish_unverified", fmt.Errorf(
				"service: container %s did not report exactly one %d/tcp host binding on %s after start",
				id, spec.Port, bindAddr))
		}
	}

	// A healthcheck-configured replacement is not a viable swap target until
	// Docker reports it healthy. Reporting pending keeps the old generation
	// running and lets the coordinator observe the candidate again; the candidate
	// itself is never stopped or duplicated by the reconciler while it is starting.
	if cfg.Healthcheck != nil {
		status := container.Starting
		if insp.Container.State.Health != nil {
			status = insp.Container.State.Health.Status
		}
		if status != container.Healthy {
			attrs := []any{
				"app", spec.App,
				"service", spec.Name,
				"replica", replica,
				"image", spec.Image,
				"container", id,
				"health", string(status),
				"outcome", "pending",
			}
			m.log.Info("Service: started; awaiting health", attrs...)
			return id, fmt.Errorf("%w: container %s health status %s", ErrServicePending, id, status)
		}
	}

	m.log.Info("Service: started",
		"app", spec.App,
		"service", spec.Name,
		"replica", replica,
		"image", spec.Image,
		"container", id,
		"outcome", "started",
	)
	return id, nil
}

// RestartServiceContainer restarts ONE persistent service container IN PLACE:
// Docker stops and starts the SAME container (same ID, same Config, same bind
// mounts) rather than creating a replacement. It is how a source-only change is
// applied under HOT_RELOAD: the live read-only source bind is re-read by the
// fresh process, so the managed service runs the edited code while retaining its
// container identity.
//
// It returns only after Docker confirms the restarted container is RUNNING,
// mirroring StartService's running gate: a container that stops but fails to come
// back (e.g. the edited source crashes on boot) is reported as an error rather
// than treated as reloaded, so the reconciler can fall back to its normal crash
// replacement instead of recording a phantom reload. A restart is not free of a
// brief stop/start window; callers use it only for a source-only change, where a
// replacement is unnecessary because the image and configuration are unchanged.
func (m *Manager) RestartServiceContainer(ctx context.Context, containerID string) error {
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return fmt.Errorf("service: %w", err)
	}
	defer release()
	callerCtx := ctx
	ctx = opCtx
	if _, err := m.cli.ContainerRestart(ctx, containerID, client.ContainerRestartOptions{}); err != nil {
		return fmt.Errorf("service: restart container: %w", m.classifyDockerErr(callerCtx, err))
	}
	insp, err := m.cli.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil {
		return fmt.Errorf("service: inspect restarted container: %w", m.classifyDockerErr(callerCtx, err))
	}
	if insp.Container.State == nil || !insp.Container.State.Running {
		return fmt.Errorf("service: restarted container is not running")
	}
	// The inspect that confirms the restart also carries the ownership labels
	// Relay stamped at create time, so the line identifies the exact service
	// replica and image without an extra lookup. A container created before a
	// label existed simply omits that attribute.
	var labels map[string]string
	if insp.Container.Config != nil {
		labels = insp.Container.Config.Labels
	}
	replica, err := strconv.Atoi(labels[labelReplica])
	if err != nil {
		replica = -1
	}
	attrs := serviceReplicaAttrs(
		labels[labelApp], labels[labelService], labels[labelImage], replica,
	)
	attrs = append(attrs,
		"container", containerID,
		"outcome", "restarted",
	)
	m.log.Info("Service: restarted", attrs...)
	return nil
}

// dockerOpAdmitter admits one Manager-owned Docker operation and returns the
// operation context plus the release the caller must invoke exactly once, or
// the terminal admission error. Manager methods that exist in both an ordinary
// and a shutdown-only form share one body and select the gate through it, so
// the two forms cannot drift apart.
type dockerOpAdmitter func(ctx context.Context) (context.Context, func(), error)

// ServiceContainerList discovers every service container on the daemon. The
// filter is a strict relay.type=service match (isServiceContainer) — deliberately
// NOT combined with a non-empty relay.app check: stale containers (a
// app removed while Relay was down) must still be discovered and returned so
// the reconciler's cross-app orphan sweep can identify and remove them.
// NodeID is deliberately NOT part of discovery (services stay reconcilable
// across worker restarts; the worker-wired reconciler applies its own
// stable-NODE_ID scope, see ServiceContainer), but it IS included in the
// result. Replica is parsed from labelReplica, defaulting to -1 when
// missing/invalid. For a container carrying Relay's own relay.healthcheck label,
// the Docker health summary is converted into Ready and Health so the reconciler
// can observe a pending candidate without an extra inspect; otherwise Ready stays
// nil and an image's own healthcheck does not affect convergence. CreatedAt is
// parsed from Docker's durable create time for the healthcheck startup deadline.
func (m *Manager) ServiceContainerList(ctx context.Context) ([]ServiceContainer, error) {
	return m.serviceContainerList(ctx, m.beginDockerOp)
}

// ShutdownServiceContainerList is the shutdown-only form of
// ServiceContainerList used by the worker's graceful-shutdown service cleanup.
// It is identical except for its admission: the lookup is admitted through the
// narrow beginShutdownCleanupOp gate rather than the ordinary one, so it can
// run after the worker lifecycle has been cancelled (which is what refuses
// ordinary work) while remaining tracked and joined by Manager.CloseContext.
// It is refused before CloseContext begins and after it, and is the ONLY
// discovery the shutdown cleanup may use.
func (m *Manager) ShutdownServiceContainerList(ctx context.Context) ([]ServiceContainer, error) {
	return m.serviceContainerList(ctx, m.beginShutdownCleanupOp)
}

func (m *Manager) serviceContainerList(ctx context.Context, admit dockerOpAdmitter) ([]ServiceContainer, error) {
	callerCtx := ctx
	opCtx, release, err := admit(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = opCtx
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("service: list containers: %w", m.classifyDockerErr(callerCtx, err))
	}
	var out []ServiceContainer
	for _, c := range list.Items {
		if !isServiceContainer(c.Labels) {
			continue
		}
		// Replica is parsed from labelReplica: a missing OR non-numeric label both
		// default to -1, which Reconcile treats as always-stale (replaced).
		replica := -1
		if r, err := strconv.Atoi(c.Labels[labelReplica]); err == nil {
			replica = r
		}
		port := 0
		if p, err := strconv.Atoi(c.Labels[labelPort]); err == nil {
			port = p
		}
		healthcheckAttempt := 0
		if a, err := strconv.Atoi(c.Labels[labelHealthcheckAttempt]); err == nil {
			healthcheckAttempt = a
		}
		// The full label set is copied as-is (nil when the container has none)
		// so the reconciler can compare routing metadata without Relay owning
		// or interpreting foreign label names.
		var labelsCopy map[string]string
		if len(c.Labels) > 0 {
			labelsCopy = make(map[string]string, len(c.Labels))
			maps.Copy(labelsCopy, c.Labels)
		}
		// Docker's health summary is authoritative and cheap here: it is part of
		// the list response. It is used ONLY when the container carries Relay's
		// relay.healthcheck label; an external image's own image-declared
		// HEALTHCHECK is not Relay's contract and must not change convergence for a
		// service that declares no healthcheck. In that case Ready stays nil and
		// the historical running-is-converged behavior is unchanged.
		var health container.HealthStatus
		var ready *bool
		if c.Labels[labelHealthcheck] != "" &&
			c.Health != nil &&
			c.Health.Status != "" &&
			c.Health.Status != container.NoHealthcheck {
			health = c.Health.Status
			isReady := health == container.Healthy
			ready = &isReady
		}
		var createdAt time.Time
		if c.Created > 0 {
			createdAt = time.Unix(c.Created, 0)
		}
		out = append(out, ServiceContainer{
			ID:                     c.ID,
			App:                    c.Labels[labelApp],
			Name:                   c.Labels[labelService],
			SourceRef:              c.Labels[labelIdentity],
			Image:                  c.Labels[labelImage],
			ImageID:                c.Labels[labelImageID],
			NodeID:                 c.Labels[labelNodeID],
			State:                  c.State,
			Replica:                replica,
			Port:                   port,
			BindAddr:               c.Labels[labelBindAddr],
			EnvironmentFingerprint: c.Labels[labelEnvironment],
			Resources:              c.Labels[labelResources],
			Tmpfs:                  c.Labels[labelTmpfs],
			Networks:               c.Labels[labelNetworks],
			Healthcheck:            c.Labels[labelHealthcheck],
			HealthcheckAttempt:     healthcheckAttempt,
			CreatedAt:              createdAt,
			Health:                 health,
			SourceID:               c.Labels[labelSource],
			Ready:                  ready,
			Labels:                 labelsCopy,
		})
	}
	fillServiceStartedAt(ctx, m.cli, out)
	return out, nil
}

// fillServiceStartedAt reads Docker's actual start instant for the containers
// whose startup grace a caller can still be measuring: Relay healthcheck
// candidates that are not healthy yet. The list summary does not expose
// StartedAt, so those specific containers need one inspect; converged healthy
// containers, failed unhealthy ones (already terminal), and every service
// without Relay's probe add no Docker call.
//
// A failed inspect leaves StartedAt zero rather than failing discovery: the
// list is authoritative for convergence, and a missing start instant is handled
// conservatively by the caller (the startup window is not treated as elapsed).
func fillServiceStartedAt(
	ctx context.Context, cli client.APIClient, containers []ServiceContainer,
) {
	for i := range containers {
		ctr := &containers[i]
		if ctr.Healthcheck == "" ||
			ctr.State != container.StateRunning ||
			ctr.Health == container.Healthy ||
			ctr.Health == container.Unhealthy {
			continue
		}
		insp, err := cli.ContainerInspect(ctx, ctr.ID, client.ContainerInspectOptions{})
		if err != nil || insp.Container.State == nil {
			continue
		}
		ctr.StartedAt = parseDockerStartInstant(insp.Container.State.StartedAt)
	}
}

// parseDockerStartInstant parses Docker's State.StartedAt into the durable start
// instant. An empty, unparseable, or zero (year 1) value is not a usable start
// instant and yields the zero time, which callers treat conservatively rather
// than substituting create time.
func parseDockerStartInstant(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	startedAt, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || startedAt.Year() <= 1 {
		return time.Time{}
	}
	return startedAt
}

// ServiceEndpoints returns the service replicas of one app owned by THIS node
// plus the host endpoint Docker actually assigned for each. It is a read-only
// inspection view for `relay app inspect`; nothing is persisted and no
// container is mutated.
//
// Discovery is a container list, but every endpoint fact comes from a
// ContainerInspect: Relay never derives a host port from a label. A container
// is reported as published ONLY when inspection proves the expected internal
// TCP port is exposed, Docker reports exactly one host binding for it, and that
// binding targets expectedBindAddr. Absent, malformed, unexpected, or
// non-matching bindings leave the endpoint unpublished; the replica's actual
// Docker state and health are still reported so a stopped or starting container
// remains visible. Containers with no relay.bind_addr label (unrouted services,
// or every service while publication is disabled) are omitted: they have no
// local endpoint to report.
//
// A container that disappears between the list and its inspect is reported as
// Missing rather than failing the whole query. Any other inspect failure is
// returned as an error, so the caller renders the live view unavailable
// instead of substituting stale or guessed values. Results are sorted by
// service, replica, and container ID for deterministic output.
func (m *Manager) ServiceEndpoints(
	ctx context.Context, appName, expectedBindAddr string,
) ([]ServiceEndpoint, error) {
	expectedBind, err := parseServiceBindAddr(expectedBindAddr)
	if err != nil {
		return nil, fmt.Errorf("service: %w", err)
	}
	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = opCtx
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("service: list containers: %w", m.classifyDockerErr(callerCtx, err))
	}
	var out []ServiceEndpoint
	for _, c := range list.Items {
		if !isServiceContainer(c.Labels) || c.Labels[labelApp] != appName {
			continue
		}
		if m.nodeID != "" && c.Labels[labelNodeID] != m.nodeID {
			continue
		}
		if c.Labels[labelBindAddr] == "" {
			continue
		}
		replica := -1
		if r, parseErr := strconv.Atoi(c.Labels[labelReplica]); parseErr == nil {
			replica = r
		}
		port := 0
		if p, parseErr := strconv.Atoi(c.Labels[labelPort]); parseErr == nil {
			port = p
		}
		endpoint := ServiceEndpoint{
			Service:     c.Labels[labelService],
			Replica:     replica,
			ContainerID: c.ID,
			Port:        port,
		}
		insp, inspectErr := m.cli.ContainerInspect(
			ctx, c.ID, client.ContainerInspectOptions{})
		if inspectErr != nil {
			if cerrdefs.IsNotFound(inspectErr) {
				endpoint.Missing = true
				out = append(out, endpoint)
				continue
			}
			return nil, fmt.Errorf(
				"service: inspect container %s: %w",
				c.ID, m.classifyDockerErr(callerCtx, inspectErr))
		}
		if state := insp.Container.State; state != nil {
			endpoint.State = state.Status
			if state.Health != nil {
				endpoint.Health = state.Health.Status
			}
		}
		if hostIP, hostPort, ok := servicePublishedBinding(
			insp.Container, port, expectedBind,
		); ok {
			endpoint.HostIP = hostIP
			endpoint.HostPort = hostPort
		}
		out = append(out, endpoint)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		if out[i].Replica != out[j].Replica {
			return out[i].Replica < out[j].Replica
		}
		return out[i].ContainerID < out[j].ContainerID
	})
	return out, nil
}

// servicePublishedBinding extracts the one host binding Docker actually
// assigned for a service's internal TCP port. It returns ok=false for every
// incomplete or unexpected shape: an invalid port, missing network settings or
// exposed-port metadata, an absent or multiple binding, any other
// published port, a malformed host port, or a host IP that differs from the
// configured bind address. The returned address is Docker's actual binding
// (unmapped to a plain IPv4/IPv6 form), never a label or configuration value.
func servicePublishedBinding(
	insp container.InspectResponse, port int, expected netip.Addr,
) (string, int, bool) {
	if !expected.IsValid() || port < 1 || port > 65535 {
		return "", 0, false
	}
	portKey, ok := network.PortFrom(uint16(port), network.TCP)
	if !ok {
		return "", 0, false
	}
	if insp.Config == nil || len(insp.Config.ExposedPorts) == 0 {
		return "", 0, false
	}
	if _, exposed := insp.Config.ExposedPorts[portKey]; !exposed {
		return "", 0, false
	}
	if insp.NetworkSettings == nil {
		return "", 0, false
	}
	published := insp.NetworkSettings.Ports
	bindings, ok := published[portKey]
	if !ok || len(bindings) != 1 {
		return "", 0, false
	}
	// Relay publishes exactly one port. Any other live host binding is an
	// unexpected shape, so this container is not reported as a valid endpoint.
	for other, extra := range published {
		if other == portKey {
			continue
		}
		if len(extra) > 0 {
			return "", 0, false
		}
	}
	binding := bindings[0]
	if !binding.HostIP.IsValid() || binding.HostIP.IsUnspecified() ||
		binding.HostIP.IsMulticast() {
		return "", 0, false
	}
	hostIP := binding.HostIP.Unmap()
	if hostIP != expected.Unmap() {
		return "", 0, false
	}
	hostPort, err := strconv.Atoi(binding.HostPort)
	if err != nil || hostPort < 1 || hostPort > 65535 {
		return "", 0, false
	}
	return hostIP.String(), hostPort, true
}

// NetworkExists reports whether a Docker network exists on the daemon. It is
// verification only — Relay NEVER creates networking infrastructure — used by
// the service reconciler to refuse starting routed containers whose routing
// network does not exist. Others' networks are inspected but never modified.
//
// Not-found matching uses cerrdefs.IsNotFound rather than errors.Is against the
// sentinel: the containerd helper recognizes BOTH the errdefs.ErrNotFound
// sentinel (what the Docker client derives from an HTTP 404) AND any error type
// implementing the NotFound() interface (e.g. Moby's internal
// objectNotFoundError, returned for an empty object id). A non-not-found
// failure (a broken daemon, a permission error) is surfaced as a genuine error
// so it is never mistaken for a missing network.
func (m *Manager) NetworkExists(ctx context.Context, network string) (bool, error) {
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	callerCtx := ctx
	ctx = opCtx
	if _, err := m.cli.NetworkInspect(ctx, network, client.NetworkInspectOptions{}); err != nil {
		if cerrdefs.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("network inspect %q: %w", network, m.classifyDockerErr(callerCtx, err))
	}
	return true, nil
}

// networkRoleLabel names the role a Docker network plays in Relay's
// infrastructure. Role is separate from ownership: a role is enough to deny a
// network to workloads, while Relay reuses only a network whose role AND
// ownership labels match. Relay's own per-node control network carries
// executionControlNetworkRole together with its managed marker and
// relay.node_id; an operator-managed network may carry the role alone and is
// still refused to workloads.
const networkRoleLabel = "relay.network.role"

// executionControlNetworkRole marks a Docker network as Relay's execution
// control plane. A workload container must never join it: that network is for
// Relay itself (and operator control-plane services), so an app that names it
// in NETWORKS, TRAEFIK_NETWORK, or a persistent service's Networks would
// otherwise gain a path into Relay's private management surface.
const executionControlNetworkRole = "execution-control"

// validateWorkloadNetworks refuses selected networks that would break the
// container security boundary, BEFORE Docker ContainerCreate. Every workload
// create path calls it with the exact network set it is about to attach:
//
//   - a Docker network whose relay.network.role is execution-control is Relay's
//     execution control plane and is never available to workloads;
//   - the reserved host/container namespace pseudo-networks are refused
//     explicitly instead of being left for the daemon to interpret.
//
// The check is by role label, not by a configured name, so a renamed control
// network stays protected. A missing network is surfaced here as an inspect
// error; that is the same failure a create would produce, but it happens
// before create. Manager.VerifyNetworks applies the same reserved-name and
// role-label policy to the optional NETWORKS set at startup/readiness, before
// any create runs.
func (m *Manager) validateWorkloadNetworks(ctx context.Context, networks []string) error {
	for _, name := range NetworkSet(networks) {
		if err := validateWorkloadNetworkName(name); err != nil {
			return err
		}
		insp, err := m.cli.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
		if err != nil {
			return fmt.Errorf("inspect workload network %q: %w", name, err)
		}
		if err := validateWorkloadNetworkPolicy(name, insp.Network); err != nil {
			return err
		}
	}
	return nil
}

// validateWorkloadNetworkPolicy enforces the label-based part of the workload
// network contract on an already-inspected network. It is shared by the create
// path and the startup/readiness NETWORKS preflight so both reject the same
// networks.
func validateWorkloadNetworkPolicy(name string, nw network.Inspect) error {
	if nw.Labels[networkRoleLabel] == executionControlNetworkRole {
		return fmt.Errorf(
			"network %q is Relay's control network and cannot be joined by workload containers",
			name,
		)
	}
	return nil
}

// validateWorkloadNetworkName rejects Docker's namespace-sharing pseudo-network
// names. They are not ordinary networks and would move a workload onto the host
// or another container's namespace.
func validateWorkloadNetworkName(name string) error {
	switch {
	case name == "host":
		return fmt.Errorf("network %q is not allowed (host networking)", name)
	case strings.HasPrefix(name, "container:"):
		return fmt.Errorf("network %q is not allowed (container namespace sharing)", name)
	default:
		return nil
	}
}

// VerifyNetworks checks the optional NETWORKS set before any app is prepared or
// any container is created, and again from the readiness probe. It returns the
// first missing network name (and false) when one does not exist, preserving the
// operator-facing missing-network semantics, and otherwise validates each
// network with the SAME workload-safe policy every create applies: reserved
// host/container pseudo-networks and networks carrying the execution-control
// role are rejected here rather than surfacing only when the first event is
// consumed.
//
// The internal control network is managed separately and is NEVER passed to
// this validator. A user who names it in NETWORKS is therefore rejected by the
// same label policy instead of being treated as a workload network.
//
// A non-not-found inspect error is returned as a genuine error so a broken
// daemon never looks like a missing network, and a policy violation is returned
// as an error. The list is normalized defensively through NetworkSet first, so
// duplicates or empty names inspect each network at most once. One Manager
// operation covers the whole check, so readiness/shutdown cancellation and
// tracking are preserved.
func (m *Manager) VerifyNetworks(ctx context.Context, networks []string) (string, bool, error) {
	set := NetworkSet(networks)
	if len(set) == 0 {
		return "", true, nil
	}
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return "", false, err
	}
	defer release()
	for _, network := range set {
		if err := validateWorkloadNetworkName(network); err != nil {
			return network, false, err
		}
		insp, err := m.cli.NetworkInspect(opCtx, network, client.NetworkInspectOptions{})
		if err != nil {
			if cerrdefs.IsNotFound(err) {
				return network, false, nil
			}
			return network, false, fmt.Errorf(
				"inspect workload network %q: %w",
				network, m.classifyDockerErr(ctx, err),
			)
		}
		if err := validateWorkloadNetworkPolicy(network, insp.Network); err != nil {
			return network, false, err
		}
	}
	return "", true, nil
}

// StopServiceContainers stops and removes the given service containers,
// best-effort: per-container failures are logged and do not abort, and the first
// error is returned at the end (matching SweepOrphanContainers' style). A
// non-running container is removed without a stop call (stopping an exited
// container errors on docker). Removal uses removeContainerContext, which
// respects the caller's cancellation/deadline (bounded by its own
// containerOpTimeout cap) and treats not-found/conflict as benign, so a caller
// that passes a short-lived reconcile bound never issues an unbounded removal.
//
// The loop stops promptly once ctx is done: ContainerStop and
// removeContainerContext both observe ctx, and the loop checks it before each
// container so a cancelled/deadline-expired caller is not charged another
// round trip. The first context error is returned (via the removal/stop error)
// like any other failure.
func (m *Manager) StopServiceContainers(ctx context.Context, containers []ServiceContainer) error {
	_, err := m.StopServiceContainersWithResult(ctx, containers)
	return err
}

// StopServiceContainersWithResult is the result-bearing form used by
// reconciliation summaries. completed counts containers whose stop/removal
// finished successfully; per-container failures still do not abort later work.
func (m *Manager) StopServiceContainersWithResult(
	ctx context.Context,
	containers []ServiceContainer,
) (completed int, err error) {
	return m.stopServiceContainersWithResult(ctx, containers, m.beginDockerOp)
}

// ShutdownStopServiceContainers is the shutdown-only form of
// StopServiceContainers used by the worker's graceful-shutdown service cleanup
// to stop/remove exactly the containers captured by
// ShutdownServiceContainerList. It is identical except for its admission: the
// removal is admitted through the narrow beginShutdownCleanupOp gate rather
// than the ordinary one, so it can run after the worker lifecycle has been
// cancelled while remaining tracked and joined by Manager.CloseContext. It
// preserves the ordinary per-container best-effort semantics and the caller's
// cancellation checks, so a shutdown step that exceeded its bound still unwinds
// promptly.
func (m *Manager) ShutdownStopServiceContainers(ctx context.Context, containers []ServiceContainer) error {
	_, err := m.stopServiceContainersWithResult(ctx, containers, m.beginShutdownCleanupOp)
	return err
}

func (m *Manager) stopServiceContainersWithResult(
	ctx context.Context,
	containers []ServiceContainer,
	admit dockerOpAdmitter,
) (int, error) {
	callerCtx := ctx
	opCtx, release, err := admit(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	ctx = opCtx
	var firstErr error
	completed := 0
	for _, c := range containers {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if c.State == container.StateRunning {
			timeout := 10
			if _, err := m.cli.ContainerStop(ctx, c.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if !serviceOperationCancelled(ctx, err) {
					attrs := serviceReplicaAttrs(c.App, c.Name, c.Image, c.Replica)
					attrs = append(attrs, "container", c.ID, "error", err)
					m.log.Warn("Service: stop container failed", attrs...)
				}
				continue
			}
		}
		if err := removeContainerContext(ctx, m.cli, c.ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if !serviceOperationCancelled(ctx, err) {
				attrs := serviceReplicaAttrs(c.App, c.Name, c.Image, c.Replica)
				attrs = append(attrs, "container", c.ID, "error", err)
				m.log.Warn("Service: remove container failed", attrs...)
			}
		} else {
			completed++
		}
	}
	// A failure that concludes after Manager shutdown began is classified as the
	// terminal ErrManagerShuttingDown deferral rather than a spurious cleanup
	// failure; a genuine caller-owned cancellation is returned unchanged.
	return completed, m.classifyDockerErr(callerCtx, firstErr)
}

func serviceOperationCancelled(ctx context.Context, err error) bool {
	return ctx != nil &&
		errors.Is(ctx.Err(), context.Canceled) &&
		errors.Is(err, context.Canceled)
}

// RemoveAppServiceContainers stops and removes every service container
// belonging to the named app (relay.type=service AND relay.app=fnName),
// returning how many were removed.
func (m *Manager) RemoveAppServiceContainers(ctx context.Context, fnName string) (int, error) {
	containers, err := m.ServiceContainerList(ctx)
	if err != nil {
		return 0, err
	}
	var fnContainers []ServiceContainer
	for _, c := range containers {
		if c.App == fnName {
			fnContainers = append(fnContainers, c)
		}
	}
	if err := m.StopServiceContainers(ctx, fnContainers); err != nil {
		return 0, err
	}
	return len(fnContainers), nil
}

// validateServiceEntrypoint rejects a service entrypoint that cannot be launched
// safely inside the container. It must be a RELATIVE path inside the application
// directory (e.g. "service.js" or "app/service.js"): non-empty, no whitespace,
// no backslash, not absolute (no leading '/'), and every '/' -separated element
// must be non-empty and not "."-leading and not "."-leading-equal-to-"..". This
// allows nested files (app/main.py) while still refusing traversals that could
// escape the copied application directory.
func validateServiceEntrypoint(entrypoint string) error {
	if entrypoint == "" {
		return fmt.Errorf("service entrypoint: empty entrypoint")
	}
	if strings.ContainsAny(entrypoint, " \t\r\n") {
		return fmt.Errorf("service entrypoint %q: must not contain whitespace", entrypoint)
	}
	if strings.ContainsAny(entrypoint, "\\") {
		return fmt.Errorf("service entrypoint %q: must not contain backslashes", entrypoint)
	}
	if strings.HasPrefix(entrypoint, "/") {
		return fmt.Errorf("service entrypoint %q: must be a relative path inside the application directory", entrypoint)
	}
	for el := range strings.SplitSeq(entrypoint, "/") {
		if el == "" {
			return fmt.Errorf("service entrypoint %q: must not contain empty path elements", entrypoint)
		}
		if el == ".." {
			return fmt.Errorf("service entrypoint %q: path must not contain \"..\"", entrypoint)
		}
		if strings.HasPrefix(el, ".") {
			return fmt.Errorf("service entrypoint %q: invalid path element %q (path elements must not start with \".\")", entrypoint, el)
		}
	}
	return nil
}

// serviceEntryForSourceMount rewrites a service entry override so it addresses
// the app's live source at the HOT_RELOAD target instead of the baked image
// workdir, and preloads the runtime's shared resolve hook where one exists.
// A node entry is `node /app/<entrypoint>`; when the mount places the source
// elsewhere (Node mounts at /app/src so node_modules stays visible) the path
// token is re-rooted at the target AND the shared resolve hook is preloaded
// (`node --import /relay/resolve-hook.mjs /app/src/<entrypoint>`), because a
// mounted Node entrypoint service never runs the invocation bootstrap and would
// otherwise let a host node_modules that appears after preparation shadow the
// dependency image. A Python entry (`python -m app.main`) carries no
// workdir-prefixed token and is returned unchanged, and a target equal to the
// baked workdir makes both the rewrite and the preload a no-op, so an unmounted
// or Python service keeps its historical command byte-for-byte.
func serviceEntryForSourceMount(entry []string, target, runtimeName string) []string {
	if len(entry) == 0 || target == "" {
		return entry
	}
	var out []string
	for i, arg := range entry {
		if !strings.HasPrefix(arg, serviceBakedWorkDir+"/") {
			continue
		}
		if out == nil {
			out = append([]string(nil), entry...)
		}
		out[i] = target + strings.TrimPrefix(arg, serviceBakedWorkDir)
	}
	if out == nil {
		return entry
	}
	// Preload the shared hook only when the source is mounted at a DISTINCT root:
	// the hook re-anchors bare specifiers from MOUNT_ROOT to the baked workdir, so
	// a workdir-equal target (Python, and the Node no-op case) must stay
	// byte-for-byte. The hook file exists only in a source-mounted Node image, so
	// this is emitted only for a Node runtime under a distinct mount target.
	if target != serviceBakedWorkDir && isNodeRuntime(runtimeName) {
		out = append([]string{out[0], "--import", node.ResolveHookPath}, out[1:]...)
	}
	return out
}

// isNodeRuntime reports whether name resolves to a Node-engine runtime in the
// registry. It dispatches by engine so every Node version identifier
// (nodejs24.x, nodejs26.x) follows the same path instead of matching one name.
func isNodeRuntime(name string) bool {
	spec, err := lookup(name)
	return err == nil && spec.Engine == plan.EngineNode
}

// ServiceEntry returns the container entrypoint override for a service
// entrypoint file on the given runtime, or an error for an unsupported runtime
// or an invalid entrypoint. One image serves both invocations and services
// (the app image's bootstrap entrypoint is overridden per-container), so
// this is the only service-specific knowledge the generic layers need — no
// separate plan or image per service — and the engine dispatch here is the
// sanctioned runtime-specific point.
//
// entrypoint is a RELATIVE source FILE path (e.g. "app/main.py"); each engine
// decides how that file is executed:
//
//   - Node runs the file directly: `node /app/<entrypoint>`. The `/app/`
//     prefix is the image WORKDIR where the app directory is COPYied, so a
//     nested entrypoint like "app/service.js" resolves to /app/app/service.js.
//   - Python executes the file as a MODULE under the app directory
//     (`python -m <module>`, e.g. "app/main.py" → `python -m app.main`), so
//     package-relative imports (`from .deps import ...`) work. The conversion
//     and its Python-specific validation live in python.ServiceCommand.
func ServiceEntry(runtimeName, entrypoint string) ([]string, error) {
	if err := validateServiceEntrypoint(entrypoint); err != nil {
		return nil, err
	}
	spec, err := lookup(runtimeName)
	if err != nil {
		return nil, fmt.Errorf("unsupported runtime %q for services", runtimeName)
	}
	switch spec.Engine {
	case plan.EngineNode:
		return []string{"node", serviceBakedWorkDir + "/" + entrypoint}, nil
	case plan.EnginePython:
		return python.ServiceCommand(entrypoint)
	default:
		return nil, fmt.Errorf("unsupported runtime %q for services", runtimeName)
	}
}

// serviceBakedWorkDir is the image workdir where a baked app's source is
// COPYied, and the prefix a node service entry uses to address it.
const serviceBakedWorkDir = "/app"
