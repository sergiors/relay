package runtime

import (
	"context"
	"errors"
	"fmt"
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

// ServiceSpec describes one desired service replica's container. The entrypoint
// is the service's identity: relay.entrypoint is stamped with it (there is no
// separate relay.service label, and services carry no relay.handler), and it is
// what Reconcile uses to group a function's containers by service.
type ServiceSpec struct {
	Function   string
	Entrypoint string // application entrypoint file, e.g. "app/service.js"
	Port       int
	Image      string
	Entry      []string // the long-lived process command; empty = image entrypoint
	Env        []string // runtime env (plan env), no RELAY_HANDLER
	// Labels are EXTRA labels the caller wants on the container (routing
	// labels supplied by the service reconciler). They are merged onto the
	// relay ownership set, with Relay ownership keys always winning: a caller
	// can never spoof or clobber relay.* labels, which define ownership and
	// discovery.
	Labels map[string]string
	// Network is an additional Docker network the container must join at
	// create time (e.g. the routing layer's network). Empty means no extra
	// network (default bridge/network mode only). The network itself is
	// infrastructure owned OUTSIDE Relay — it is never created here — and the
	// caller (the service reconciler) validates it exists before starting
	// routed containers.
	Network string
}

// ServiceContainer is one discovered service container, as stamped on its
// labels. Hostname is included for logging only — it is NOT part of the
// ownership predicate, because services must be reconcilable across worker
// restarts on the same host.
type ServiceContainer struct {
	ID         string
	Function   string
	Entrypoint string
	Image      string
	Hostname   string
	State      container.ContainerState
	Replica    int
	// Port is the container's configured internal port, parsed from its
	// relay.port label. It defaults to 0 when the label is missing/invalid.
	// The service reconciler uses it to detect a port change (a stale-config
	// container whose labelPort != the template's desired port is replaced).
	Port int
	// Labels is the container's FULL label set (nil-safe; nil when the
	// container has none). The service reconciler compares routing metadata
	// through it without Relay parsing or interpreting foreign label names.
	Labels map[string]string
}

// serviceContainerLabelName is the deterministic, docker-safe container name for
// one service replica. Ownership is always derived from labels, never from this
// name (names are not guaranteed unique/stable across restarts), so this is
// purely for human greppability. The service entrypoint part is sanitized to
// [A-Za-z0-9_.-] and the total is capped well under docker's name length limit.
const serviceContainerNameLenCap = 100

// sanitizeContainerNamePart replaces any character outside [A-Za-z0-9_.-] with
// '-'. Function names are already validated to a legal docker repo charset, but
// the service entrypoint is an arbitrary filename the operator chose, so it is
// sanitized defensively.
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
// relay-svc-<function>-<entrypoint>-<replica>. Function names are already
// validated; the entrypoint part is sanitized and the whole name capped.
func serviceContainerName(functionName, entrypoint string, replica int) string {
	name := "relay-svc-" + functionName + "-" + sanitizeContainerNamePart(entrypoint) + "-" + strconv.Itoa(replica)
	if len(name) > serviceContainerNameLenCap {
		name = name[:serviceContainerNameLenCap]
	}
	return name
}

// serviceLabels is the total, greppable label set stamped on every service
// container: relay.type=service plus relay.function + relay.hostname make a
// service container recognizable to Relay while the strict relay.type guard
// lets sweeps/reconcilers distinguish the service population from one-shot
// invocation containers. relay.entrypoint is the service identity (the
// entrypoint string) — there is no relay.service label there, and service
// containers carry no relay.handler.
//
// Extra caller-supplied labels (spec.Labels, e.g. routing labels) are merged
// on top, then the relay ownership keys are RE-applied last so Relay's
// ownership labels are always authoritative: a caller can add labels but never
// clobber or spoof a relay.* key.
func serviceLabels(spec ServiceSpec, hostname string, replica int) map[string]string {
	labels := map[string]string{
		labelType:       ContainerTypeService,
		labelFunction:   spec.Function,
		labelEntrypoint: spec.Entrypoint,
		labelImage:      spec.Image,
		labelHostname:   hostname,
		labelPort:       strconv.Itoa(spec.Port),
		labelReplica:    strconv.Itoa(replica),
	}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	labels[labelType] = ContainerTypeService
	labels[labelFunction] = spec.Function
	labels[labelEntrypoint] = spec.Entrypoint
	labels[labelImage] = spec.Image
	labels[labelHostname] = hostname
	labels[labelPort] = strconv.Itoa(spec.Port)
	labels[labelReplica] = strconv.Itoa(replica)
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
		// Override the image's invocation-bootstrap entrypoint with the
		// long-lived service entrypoint (e.g. ["node", "/app/service.js"]).
		cfg.Entrypoint = spec.Entry
	}

	createOps := client.ContainerCreateOptions{
		Config: cfg,
		// No AutoRemove: persistent, reconciler-owned (see doc comment).
		HostConfig: hardenedHostConfig(false),
		Name:       serviceContainerName(spec.Function, spec.Entrypoint, replica),
	}
	if spec.Network != "" {
		// Join an additional Docker network at create time (containers must
		// belong to a network from creation to be on it at start). The network
		// is infrastructure owned OUTSIDE Relay — it is never created here —
		// and the caller (the service reconciler) validates it exists before
		// starting routed containers; a missing network surfaces as a create
		// error below rather than a chaos fix-up.
		createOps.NetworkingConfig = &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{spec.Network: {}},
		}
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
		"entrypoint", spec.Entrypoint,
		"replica", replica,
		"container", id,
	)
	return id, nil
}

// ServiceContainerList discovers every service container on the daemon. The
// filter is isServiceContainer — a strict relay.type=service match via
// isServiceContainer — deliberately NOT combined with a non-empty
// relay.function check: stale containers (a function removed while Relay was
// down) must still be discovered and returned so the reconciler's cross-function
// orphan sweep can identify and remove them. Hostname is deliberately NOT part
// of discovery — services must be reconcilable across worker restarts on the
// same host — but it IS included in the result for logging. Replica is parsed
// from labelReplica, defaulting to -1 when missing/invalid.
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
			ID:         c.ID,
			Function:   c.Labels[labelFunction],
			Entrypoint: c.Labels[labelEntrypoint],
			Image:      c.Labels[labelImage],
			Hostname:   c.Labels[labelHostname],
			State:      c.State,
			Replica:    replica,
			Port:       port,
			Labels:     labelsCopy,
		})
	}
	return out, nil
}

// NetworkExists reports whether a Docker network exists on the daemon. It is
// verification only — Relay NEVER creates networking infrastructure — used by
// the service reconciler to refuse starting routed containers whose routing
// network does not exist. Others' networks are inspected but never modified.
func (m *Manager) NetworkExists(ctx context.Context, network string) (bool, error) {
	if _, err := m.cli.NetworkInspect(ctx, network, client.NetworkInspectOptions{}); err != nil {
		if errors.Is(err, cerrdefs.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("network inspect %q: %w", network, err)
	}
	return true, nil
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
					"entrypoint", c.Entrypoint,
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
				"entrypoint", c.Entrypoint,
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
