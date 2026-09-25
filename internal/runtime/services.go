package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	cerrdefs "github.com/containerd/errdefs"

	"relay/internal/runtime/python"
)

// Service container labels beyond the shared registry in labels.go. relay.type
// distinguishes persistent service containers (ContainerTypeService) from
// one-shot invocation containers (event/schedule), so sweeps/reconcilers can
// never confuse the two populations.

// ServiceSpec describes one desired service replica's container. The service
// identity is its configured source descriptor (function.Service.SourceRef):
// relay.identity is stamped with it (there is no separate relay.service label,
// and services carry no relay.handler), and it is what Reconcile uses to group a
// function's containers by service. The identity is honest for every source —
// an entrypoint file, a Dockerfile path, or an external image reference — never
// a synthetic entrypoint.
type ServiceSpec struct {
	Function string
	// Identity is the service's stable identity: the configured source
	// descriptor (entrypoint file, Dockerfile path, or image reference).
	Identity string
	Port     int
	Image    string
	// ImageID is the local content ID of Image (relay.image_id), or "" when the
	// reference itself encodes the content (a Relay-built image tag). It lets
	// Reconcile detect a moved external tag pointing at different bytes and
	// replace the container.
	ImageID string
	// Entry is the per-container entrypoint override. Empty means preserve the
	// image's own ENTRYPOINT/CMD (build/image sources); non-empty is the
	// runtime-resolved command for an entrypoint source.
	Entry []string
	Env   []string // runtime env (plan env), no RELAY_HANDLER
	// Labels are EXTRA labels the caller wants on the container (routing
	// labels supplied by the service reconciler). They are merged onto the
	// relay ownership set, with Relay ownership keys always winning: a caller
	// can never spoof or clobber relay.* labels, which define ownership and
	// discovery.
	Labels map[string]string
	// Network is the Docker network the container must join at create time
	// (e.g. the routing layer's network). Empty means no extra network (default
	// bridge/network mode only). The network itself is infrastructure owned
	// OUTSIDE Relay — it is never created here — and the caller (the service
	// reconciler) validates it exists before starting routed containers.
	//
	// Service networking is deliberately independent of the worker-global
	// NETWORKS set applied to execution containers: a persistent service is
	// reached through its routing layer (Traefik), which owns the network it
	// joins. A service that declares no host joins no network here.
	Network string
}

// ServiceContainer is one discovered service container, as stamped on its
// labels. Hostname is included for logging only — it is NOT part of the
// ownership predicate, because services must be reconcilable across worker
// restarts on the same host.
type ServiceContainer struct {
	ID       string
	Function string
	// Identity is the service's identity, parsed from its relay.identity label:
	// the configured source descriptor. It is the grouping key Reconcile uses.
	Identity string
	Image    string
	// ImageID is the local content ID of Image, parsed from relay.image_id ("").
	ImageID  string
	Hostname string
	State    container.ContainerState
	Replica  int
	// Port is the container's configured internal port, parsed from its
	// relay.port label. It defaults to 0 when the label is missing/invalid.
	// The service reconciler uses it to detect a port change (a stale-config
	// container whose labelPort != the template's desired port is replaced).
	Port int
	// EnvHash is the content hash of the effective environment the container
	// was created with, parsed from relay.env_hash. It is "" for a container
	// created before the label existed (or with a missing/invalid value), which
	// never equals a desired hash, so such a container is replaced once —
	// exactly like a missing relay.image_id or relay.port.
	EnvHash string
	// Networks is the canonical, sorted, comma-separated set of extra Docker
	// networks the container was created attached to, parsed from
	// relay.networks. It is "" for a container with no extra networks (or one
	// created before the label existed). The service reconciler compares it to
	// the desired set to detect a network change.
	Networks string
	// Labels is the container's FULL label set (nil-safe; nil when the
	// container has none). The service reconciler compares routing metadata
	// through it without Relay parsing or interpreting foreign label names.
	Labels map[string]string
}

// serviceContainerLabelName is the deterministic, docker-safe container name for
// one service replica. Ownership is always derived from labels, never from this
// name (names are not guaranteed unique/stable across restarts), so this is
// purely for human greppability. The service identity part is sanitized to
// [A-Za-z0-9_.-] and the total is capped well under docker's name length limit.
const serviceContainerNameLenCap = 100

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
// '-'. Function names are already validated to a legal docker repo charset, but
// the service identity is an arbitrary descriptor (a file path, Dockerfile
// path, or image reference), so it is sanitized defensively.
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

// serviceContainerName derives the deterministic container name for a replica:
// relay-svc-<function>-<identity>-<hash>-<replica>. Function names are already
// validated; the identity part (an entrypoint file, Dockerfile path, or image
// reference) is sanitized. The name ends with a collision-resistant hash suffix
// derived from the FULL function name and identity, so distinct identities that
// sanitize to the same readable base (e.g. "ghcr.io/acme/a/b:1" and
// "ghcr.io/acme/a-b:1") or that would truncate to the same prefix still get
// distinct names. The readable base is trimmed to make room for the suffix, so
// the suffix and the replica index are never lost to the cap.
func serviceContainerName(functionName, identity string, replica int) string {
	suffix := "-" + serviceIdentityHash(functionName, identity) + "-" + strconv.Itoa(replica)
	base := "relay-svc-" + functionName + "-" + sanitizeContainerNamePart(identity)
	maxBase := serviceContainerNameLenCap - len(suffix)
	if maxBase < 0 {
		maxBase = 0
	}
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + suffix
}

// serviceIdentityHash returns the fixed-length hex collision-resistant suffix
// for a service identity. It hashes the FULL function name and the FULL identity
// (never the sanitized or truncated base) with a NUL separator, so the two
// inputs cannot run together across the join and distinct identities hash apart
// even when their sanitized bases coincide.
func serviceIdentityHash(functionName, identity string) string {
	sum := sha256.Sum256([]byte(functionName + "\x00" + identity))
	return hex.EncodeToString(sum[:])[:serviceIdentityHashLen]
}

// serviceEndpoints builds the Docker NetworkingConfig EndpointsConfig for a
// service container: the routing network (primary, may be empty). The result is
// a set keyed by network name — Docker's EndpointsConfig is itself a map, so
// there is no meaningful iteration order and none is relied on. An empty result
// means no NetworkingConfig is sent (default bridge/network mode only).
func serviceEndpoints(primary string) map[string]*network.EndpointSettings {
	if primary == "" {
		return nil
	}
	return map[string]*network.EndpointSettings{primary: {}}
}

// NetworksLabel returns the canonical value of the relay.networks label for a
// container joining the given networks: the SORTED, de-duplicated,
// comma-separated set. An empty result means the container joins no extra
// network and the label is omitted. Sorting makes the value independent of
// input field order, so a reconciler comparing a desired value to a discovered
// label sees equality whenever the SET of networks matches.
//
// It is used for persistent service containers, whose only non-default network
// is the routing layer's (TRAEFIK_NETWORK); it is independent of the
// worker-global NETWORKS set, which applies to execution containers only.
func NetworksLabel(networks ...string) string {
	set := make(map[string]bool, len(networks))
	for _, n := range networks {
		if n != "" {
			set[n] = true
		}
	}
	if len(set) == 0 {
		return ""
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// EnvHash returns the short content hash of a service replica's effective
// environment (relay.env_hash): a deterministic, order-sensitive digest over the
// exact Config.Env slice StartService applies. It is a one-way hash, so no
// secret value is ever exposed in a label, log, metric, or inspect output — the
// environment itself is the only place values live.
//
// The reconciler uses it to detect that a container's environment no longer
// matches the template: a changed template env on an `image` source (whose image
// reference is unchanged), or a rotated secret value (which never changes the
// source fingerprint), must replace the stale container. The NUL separator is
// not valid inside an env entry, so the entries cannot run together across the
// join. The hash length matches serviceIdentityHashLen so the two label values
// share one convention.
func EnvHash(env []string) string {
	h := sha256.New()
	for _, kv := range env {
		_, _ = h.Write([]byte(kv))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:serviceIdentityHashLen]
}

// serviceLabels is the total, greppable label set stamped on every service
// container: relay.type=service plus relay.function + relay.hostname make a
// service container recognizable to Relay while the strict relay.type guard
// lets sweeps/reconcilers distinguish the service population from one-shot
// invocation containers. relay.identity is the service identity (the configured
// source descriptor) — there is no relay.service label there, and service
// containers carry no relay.handler. relay.image_id records the local content ID
// the container was started from (empty when the reference itself is
// content-addressed), so a moved external tag is detected. relay.env_hash pins
// the effective environment's content hash so an env/secret change replaces a
// container whose image reference is unchanged (see labelEnvHash).
//
// Extra caller-supplied labels (spec.Labels, e.g. routing labels) are merged
// on top, then the relay ownership keys are RE-applied last so Relay's
// ownership labels are always authoritative: a caller can add labels but never
// clobber or spoof a relay.* key.
func serviceLabels(spec ServiceSpec, hostname string, replica int) map[string]string {
	envHash := EnvHash(spec.Env)
	labels := map[string]string{
		labelType:     ContainerTypeService,
		labelFunction: spec.Function,
		labelIdentity: spec.Identity,
		labelImage:    spec.Image,
		labelHostname: hostname,
		labelPort:     strconv.Itoa(spec.Port),
		labelReplica:  strconv.Itoa(replica),
		labelEnvHash:  envHash,
	}
	if spec.ImageID != "" {
		labels[labelImageID] = spec.ImageID
	}
	if networks := NetworksLabel(spec.Network); networks != "" {
		labels[labelNetworks] = networks
	}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	labels[labelType] = ContainerTypeService
	labels[labelFunction] = spec.Function
	labels[labelIdentity] = spec.Identity
	labels[labelImage] = spec.Image
	labels[labelHostname] = hostname
	labels[labelPort] = strconv.Itoa(spec.Port)
	labels[labelReplica] = strconv.Itoa(replica)
	labels[labelEnvHash] = envHash
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
	if networks := NetworksLabel(spec.Network); networks != "" {
		labels[labelNetworks] = networks
	} else {
		delete(labels, labelNetworks)
	}
	return labels
}

// StartService creates and starts ONE persistent service container. spec.Env is
// applied verbatim (container env semantics — the caller must assemble it,
// including PORT=<spec.Port> plus the template env values and resolved secrets;
// nothing here resolves secrets). The hardened HostConfig mirrors runContainer
// EXCEPT AutoRemove is deliberately FALSE: a service container is persistent and
// long-lived, so the reconciler owns its removal, never the daemon. The internal
// port is exposed as metadata only (ExposedPorts) with NO HostConfig.PortBindings
// — no host port is published this iteration. On any error after create but
// before a successful start, the container is removed via removeContainer.
//
// One image serves both invocations and services: PrepareService builds the
// function image (its ENTRYPOINT is the invocation bootstrap), and the service
// entrypoint is overridden per-container via spec.Entry. This keeps image
// retirement grouped under FunctionImageTags with no separate service images.
func (m *Manager) StartService(ctx context.Context, spec ServiceSpec, replica int) (string, error) {
	cfg := &container.Config{
		Image:  spec.Image,
		Env:    spec.Env,
		Labels: serviceLabels(spec, m.hostname, replica),
		// ExposedPorts is metadata only (informational). Relay does NOT publish a
		// host port this iteration: no HostConfig.PortBindings.
		ExposedPorts: network.PortSet{network.MustParsePort(fmt.Sprintf("%d/tcp", spec.Port)): {}},
	}
	if len(spec.Entry) > 0 {
		// An entrypoint-source service overrides the function image's
		// invocation-bootstrap entrypoint with the long-lived service command
		// (e.g. ["node", "/app/service.js"]). A build/image service leaves
		// Entry empty so the container preserves the image's own
		// ENTRYPOINT/CMD.
		cfg.Entrypoint = spec.Entry
	}

	createOps := client.ContainerCreateOptions{
		Config: cfg,
		// No AutoRemove: persistent, reconciler-owned (see doc comment).
		HostConfig: hardenedHostConfig(false),
		Name:       serviceContainerName(spec.Function, spec.Identity, replica),
	}
	if endpoints := serviceEndpoints(spec.Network); len(endpoints) > 0 {
		// Join the routing network at create time (containers must belong to a
		// network from creation to be on it at start). The network is
		// infrastructure owned OUTSIDE Relay — it is never created here — and
		// the caller (the service reconciler) validates it exists before
		// starting containers; a missing network surfaces as a create error
		// below rather than a chaos fix-up. Service networking is independent of
		// the worker-global NETWORKS set applied to execution containers.
		createOps.NetworkingConfig = &network.NetworkingConfig{EndpointsConfig: endpoints}
	}
	createResp, err := m.cli.ContainerCreate(ctx, createOps)
	if err != nil {
		return "", fmt.Errorf("service: create container: %w", err)
	}
	id := createResp.ID

	if _, err := m.cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		// Created but never started; never leaves a live service behind, so
		// remove the created container before returning. removeContainer treats
		// not-found/conflict as benign.
		_ = removeContainer(m.cli, id)
		return "", fmt.Errorf("service: start container: %w", err)
	}

	m.log.Info("Service: started",
		"function", spec.Function,
		"service", spec.Identity,
		"replica", replica,
		"container", id,
	)
	return id, nil
}

// ServiceContainerList discovers every service container on the daemon. The
// filter is a strict relay.type=service match (isServiceContainer) — deliberately
// NOT combined with a non-empty relay.function check: stale containers (a
// function removed while Relay was down) must still be discovered and returned so
// the reconciler's cross-function orphan sweep can identify and remove them.
// Hostname is deliberately NOT part of discovery — services must be reconcilable
// across worker restarts on the same host — but it IS included in the result for
// logging. Replica is parsed from labelReplica, defaulting to -1 when
// missing/invalid.
func (m *Manager) ServiceContainerList(ctx context.Context) ([]ServiceContainer, error) {
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("service: list containers: %w", err)
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
		// The full label set is copied as-is (nil when the container has none)
		// so the reconciler can compare routing metadata without Relay owning
		// or interpreting foreign label names.
		var labelsCopy map[string]string
		if len(c.Labels) > 0 {
			labelsCopy = make(map[string]string, len(c.Labels))
			for k, v := range c.Labels {
				labelsCopy[k] = v
			}
		}
		out = append(out, ServiceContainer{
			ID:       c.ID,
			Function: c.Labels[labelFunction],
			Identity: c.Labels[labelIdentity],
			Image:    c.Labels[labelImage],
			ImageID:  c.Labels[labelImageID],
			Hostname: c.Labels[labelHostname],
			State:    c.State,
			Replica:  replica,
			Port:     port,
			EnvHash:  c.Labels[labelEnvHash],
			Networks: c.Labels[labelNetworks],
			Labels:   labelsCopy,
		})
	}
	return out, nil
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
	if _, err := m.cli.NetworkInspect(ctx, network, client.NetworkInspectOptions{}); err != nil {
		if cerrdefs.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("network inspect %q: %w", network, err)
	}
	return true, nil
}

// VerifyNetworks checks that every named Docker network exists, returning the
// first missing network name (and false) when one does not. It is the startup
// pre-flight the worker performs before any function is prepared or any
// container created, for the worker-global NETWORKS set: the networks are
// infrastructure owned OUTSIDE Relay, so a missing one is an operator condition
// Relay reports rather than fixes — it NEVER creates a network. A non-not-found
// inspect error is returned as a genuine error so a broken daemon never looks
// like a missing network. The worker treats either failure as fatal startup.
func (m *Manager) VerifyNetworks(ctx context.Context, networks []string) (string, bool, error) {
	for _, network := range networks {
		if network == "" {
			continue
		}
		ok, err := m.NetworkExists(ctx, network)
		if err != nil {
			return network, false, err
		}
		if !ok {
			return network, false, nil
		}
	}
	return "", true, nil
}

// StopServiceContainers stops and removes the given service containers,
// best-effort: per-container failures are logged and do not abort, and the first
// error is returned at the end (matching SweepOrphanContainers' style). A
// non-running container is removed without a stop call (stopping an exited
// container errors on docker). Removal uses removeContainer, which treats
// not-found/conflict as benign.
func (m *Manager) StopServiceContainers(ctx context.Context, containers []ServiceContainer) error {
	var firstErr error
	for _, c := range containers {
		if c.State == container.StateRunning {
			timeout := 10
			if _, err := m.cli.ContainerStop(ctx, c.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				m.log.Warn("Service: stop container failed",
					"container", c.ID,
					"function", c.Function,
					"service", c.Identity,
					"error", err,
				)
				continue
			}
		}
		if err := removeContainer(m.cli, c.ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			m.log.Warn("Service: remove container failed",
				"container", c.ID,
				"function", c.Function,
				"service", c.Identity,
				"error", err,
			)
		}
	}
	return firstErr
}

// RemoveFunctionServiceContainers stops and removes every service container
// belonging to the named function (relay.type=service AND relay.function=fnName),
// returning how many were removed.
func (m *Manager) RemoveFunctionServiceContainers(ctx context.Context, fnName string) (int, error) {
	containers, err := m.ServiceContainerList(ctx)
	if err != nil {
		return 0, err
	}
	var fnContainers []ServiceContainer
	for _, c := range containers {
		if c.Function == fnName {
			fnContainers = append(fnContainers, c)
		}
	}
	if err := m.StopServiceContainers(ctx, fnContainers); err != nil {
		return 0, err
	}
	return len(fnContainers), nil
}

// RetireServiceImages removes every local image in the named function's repo
// (via FunctionImageTags) that is NOT still referenced by an existing service
// container's labelImage. Building the keep-set across ALL remaining service
// containers (any function) guarantees an image is never removed while an old
// service container still depends on it, and only relay-fn-<name> tags are ever
// touched (never non-Relay images, nor other functions' repos). The keep-set is
// further backed by RemoveImage's container-reference guard, so an image that
// slips past the keep-set (e.g. a container whose label this boot has not yet
// observed) is still skipped rather than removed while referenced. Returns the
// number of images removed.
func (m *Manager) RetireServiceImages(ctx context.Context, fnName string) (int, error) {
	containers, err := m.ServiceContainerList(ctx)
	if err != nil {
		return 0, err
	}
	keep := make(map[string]bool)
	for _, c := range containers {
		if c.Image != "" {
			keep[c.Image] = true
		}
	}

	tags, err := m.FunctionImageTags(ctx, fnName)
	if err != nil {
		return 0, err
	}
	removed := 0
	var firstErr error
	for _, tag := range tags {
		if keep[tag] {
			continue
		}
		if err := m.RemoveImage(ctx, tag); err != nil {
			if errors.Is(err, ErrImageInUse) {
				// A container still references this image; the keep-set built from
				// ServiceContainerList above and the container-reference guard both
				// recognize this as the normal transitional state. Defer to a later
				// pass: log at debug, not counted, not surfaced as a failure.
				m.log.Debug("Image cleanup: image still in use; skipping", "image", tag)
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			m.log.Warn("Service: retire image failed", "image", tag, "error", err)
			continue
		}
		removed++
	}
	if firstErr != nil {
		return removed, firstErr
	}
	return removed, nil
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
	for _, el := range strings.Split(entrypoint, "/") {
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

// ServiceEntry returns the container entrypoint override for a service
// entrypoint file on the given runtime, or an error for an unsupported runtime
// or an invalid entrypoint. One image serves both invocations and services
// (the function image's bootstrap entrypoint is overridden per-container), so
// this is the only service-specific knowledge the generic layers need — no
// separate plan or image per service — and the runtime switch here is the
// sanctioned dispatch point.
//
// entrypoint is a RELATIVE source FILE path (e.g. "app/main.py"); each runtime
// decides how that file is executed:
//
//   - node24 runs the file directly: `node /app/<entrypoint>`. The `/app/`
//     prefix is the image WORKDIR where the function directory is COPYied, so a
//     nested entrypoint like "app/service.js" resolves to /app/app/service.js.
//   - python3.14 executes the file as a MODULE under the function directory
//     (`python -m <module>`, e.g. "app/main.py" → `python -m app.main`), so
//     package-relative imports (`from .deps import ...`) work. The conversion
//     and its Python-specific validation live in python.ServiceCommand.
func ServiceEntry(runtimeName, entrypoint string) ([]string, error) {
	if err := validateServiceEntrypoint(entrypoint); err != nil {
		return nil, err
	}
	switch runtimeName {
	case "node24":
		return []string{"node", "/app/" + entrypoint}, nil
	case "python3.14":
		return python.ServiceCommand(entrypoint)
	default:
		return nil, fmt.Errorf("unsupported runtime %q for services", runtimeName)
	}
}
