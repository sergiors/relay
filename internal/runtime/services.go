package runtime

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// Service container labels beyond the shared registry in labels.go. relay.type
// distinguishes persistent service containers (ContainerTypeService) from
// one-shot invocation containers (event/schedule), so sweeps/reconcilers can
// never confuse the two populations.

// ServiceSpec describes one desired service replica's container. The handler is
// the service's identity: relay.handler is stamped with it (there is no separate
// relay.service label), and it is what Reconcile uses to group a function's
// containers by service.
type ServiceSpec struct {
	Function string
	Handler  string // handler/entrypoint identity, e.g. "service.js"
	Port     int
	Image    string
	Entry    []string // the long-lived process command; empty = image entrypoint
	Env      []string // runtime env (plan env), no RELAY_HANDLER
}

// ServiceContainer is one discovered service container, as stamped on its
// labels. Hostname is included for logging only — it is NOT part of the
// ownership predicate, because services must be reconcilable across worker
// restarts on the same host.
type ServiceContainer struct {
	ID       string
	Function string
	Handler  string
	Image    string
	Hostname string
	State    container.ContainerState
	Replica  int
	// Port is the container's configured internal port, parsed from its
	// relay.port label. It defaults to 0 when the label is missing/invalid.
	// The service reconciler uses it to detect a port change (a stale-config
	// container whose labelPort != the template's desired port is replaced).
	Port int
}

// serviceContainerLabelName is the deterministic, docker-safe container name for
// one service replica. Ownership is always derived from labels, never from this
// name (names are not guaranteed unique/stable across restarts), so this is
// purely for human greppability. The service handler part is sanitized to
// [A-Za-z0-9_.-] and the total is capped well under docker's name length limit.
const serviceContainerNameLenCap = 100

// sanitizeContainerNamePart replaces any character outside [A-Za-z0-9_.-] with
// '-'. Function names are already validated to a legal docker repo charset, but
// the service handler is an arbitrary filename the operator chose, so it is
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
// relay-svc-<function>-<handler>-<replica>. Function names are already
// validated; the handler part is sanitized and the whole name capped.
func serviceContainerName(functionName, handler string, replica int) string {
	name := "relay-svc-" + functionName + "-" + sanitizeContainerNamePart(handler) + "-" + strconv.Itoa(replica)
	if len(name) > serviceContainerNameLenCap {
		name = name[:serviceContainerNameLenCap]
	}
	return name
}

// serviceLabels is the total, greppable label set stamped on every service
// container: relay.type=service plus relay.function + relay.hostname make a
// service container recognizable to Relay while the strict relay.type guard
// lets sweeps/reconcilers distinguish the service population from one-shot
// invocation containers. relay.handler is the service identity (the handler
// string) — there is no relay.service label.
func serviceLabels(spec ServiceSpec, hostname string, replica int) map[string]string {
	return map[string]string{
		labelType:     ContainerTypeService,
		labelFunction: spec.Function,
		labelHandler:  spec.Handler,
		labelImage:    spec.Image,
		labelHostname: hostname,
		labelPort:     strconv.Itoa(spec.Port),
		labelReplica:  strconv.Itoa(replica),
	}
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

	createResp, err := m.cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: cfg,
		// No AutoRemove: persistent, reconciler-owned (see doc comment).
		HostConfig: hardenedHostConfig(false),
		Name:       serviceContainerName(spec.Function, spec.Handler, replica),
	})
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
		"handler", spec.Handler,
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
		out = append(out, ServiceContainer{
			ID:       c.ID,
			Function: c.Labels[labelFunction],
			Handler:  c.Labels[labelHandler],
			Image:    c.Labels[labelImage],
			Hostname: c.Labels[labelHostname],
			State:    c.State,
			Replica:  replica,
			Port:     port,
		})
	}
	return out, nil
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
				m.log.Warn(fmt.Sprintf("Service: stop container %s (%s %s): %v",
					c.ID, c.Function, c.Handler, err))
				continue
			}
		}
		if err := removeContainer(m.cli, c.ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			m.log.Warn(fmt.Sprintf("Service: remove container %s (%s %s): %v",
				c.ID, c.Function, c.Handler, err))
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
// touched (never non-Relay images, nor other functions' repos). Returns the
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
			if firstErr == nil {
				firstErr = err
			}
			m.log.Warn(fmt.Sprintf("Service: retire image %s: %v", tag, err))
			continue
		}
		removed++
	}
	if firstErr != nil {
		return removed, firstErr
	}
	return removed, nil
}

// validateServiceHandler rejects a service entrypoint that cannot be launched
// safely inside the container. v1 keeps it deliberately strict: a plain filename
// with no path separators — no absolute paths, no "..", no subpaths — so the
// launch path is always a directly-owned file in the image's WORKDIR.
func validateServiceHandler(handler string) error {
	if handler == "" {
		return fmt.Errorf("service entrypoint: empty handler")
	}
	if strings.ContainsAny(handler, " \t/") {
		return fmt.Errorf("service entrypoint %q: must be a plain filename (no spaces or path separators)", handler)
	}
	if strings.HasPrefix(handler, "..") || strings.HasPrefix(handler, ".") {
		return fmt.Errorf("service entrypoint %q: must be a plain filename, not a path", handler)
	}
	return nil
}

// ServiceEntry returns the container entrypoint override for a service
// entrypoint file on the given runtime, or an error for an unsupported runtime
// or an invalid handler filename. One image serves both invocations and services
// (the function image's bootstrap entrypoint is overridden per-container), so
// this is the only service-specific knowledge the runtime needs — no separate
// plan or image per service. node24 and python3.14 are the runtimes Relay
// supports as of this iteration.
func ServiceEntry(runtimeName, handler string) ([]string, error) {
	if err := validateServiceHandler(handler); err != nil {
		return nil, err
	}
	switch runtimeName {
	case "node24":
		return []string{"node", "/app/" + handler}, nil
	case "python3.14":
		return []string{"python", "/app/" + handler}, nil
	default:
		return nil, fmt.Errorf("unsupported runtime %q for services", runtimeName)
	}
}
