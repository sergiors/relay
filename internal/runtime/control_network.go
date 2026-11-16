package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const (
	// controlNetworkNamePrefix makes the per-node control network name
	// deterministic from NODE_ID, so a restarted Relay reuses exactly the
	// network its previous incarnation created.
	controlNetworkNamePrefix = "relay-control-"

	// controlNetworkManagedLabel is the Relay-managed marker. It is set only on
	// networks Relay created for its own control plane, so a same-name network
	// from another tool is never adopted merely because it carries the
	// execution-control role.
	controlNetworkManagedLabel = "relay.managed"

	// controlNetworkBridgeDriver is the only driver Relay accepts for its
	// control network: a normal user-defined bridge preserves outbound egress
	// and inter-container connectivity without host-published ports.
	controlNetworkBridgeDriver = "bridge"

	// Docker bridge driver options that would break Relay's execution channel
	// if an operator disabled them on a reused network.
	controlNetworkICCOption        = "com.docker.network.bridge.enable_icc"
	controlNetworkMasqueradeOption = "com.docker.network.bridge.enable_ip_masquerade"

	// maxControlNodeIDLen mirrors config's NODE_ID bound. The runtime package
	// deliberately does not import config, so the defensive bound lives here.
	maxControlNodeIDLen = 128
)

// controlNetworkName derives this node's persistent control-network name from
// its stable NODE_ID. NODE_ID is an ownership identity, never a container ID:
// the deterministic name is only the lookup key, and every reuse is validated
// against the ownership labels below.
func controlNetworkName(nodeID string) string {
	return controlNetworkNamePrefix + nodeID
}

// controlNetworkLabels are the strict ownership labels of a Relay control
// network: the execution-control role plus the managed and node-ID markers.
// All three must match before Relay reuses a network.
func controlNetworkLabels(nodeID string) map[string]string {
	return map[string]string{
		networkRoleLabel:           executionControlNetworkRole,
		labelNodeID:                nodeID,
		controlNetworkManagedLabel: "true",
	}
}

// validateControlNodeID rejects a NODE_ID that could not form a valid,
// unambiguous control-network name. config already validates NODE_ID; this is
// the manager's defense for direct construction and a clear startup error.
func validateControlNodeID(nodeID string) error {
	if nodeID == "" {
		return errors.New("control network: relay node identity is empty")
	}
	if len(nodeID) > maxControlNodeIDLen {
		return fmt.Errorf(
			"control network: relay node identity exceeds %d characters",
			maxControlNodeIDLen,
		)
	}
	for _, r := range nodeID {
		if (r < '0' || r > '9') &&
			(r < 'a' || r > 'z') &&
			(r < 'A' || r > 'Z') &&
			r != '.' && r != '_' && r != '-' {
			return fmt.Errorf(
				"control network: relay node identity %q contains an unsupported character",
				nodeID,
			)
		}
	}
	return nil
}

// EnsureControlNetwork provisions, discovers, validates, and (for a
// containerized Relay) attaches this node's persistent control network. The
// worker calls it once during startup preflight, after the NODE_ID ownership
// lock is held and before manager maintenance, app preparation, readiness, or
// event consumption.
//
// The network is reused across restarts but never adopted on name alone: a
// same-name network must carry Relay's managed/node/control labels and a
// compatible bridge configuration, or startup fails closed. Relay deliberately
// never removes the network on shutdown; permanent node retirement requires an
// explicit operator cleanup.
func (m *Manager) EnsureControlNetwork(ctx context.Context) error {
	if m == nil || m.cli == nil {
		return errors.New("control network: docker client unavailable")
	}
	if err := validateControlNodeID(m.nodeID); err != nil {
		return err
	}
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return err
	}
	defer release()

	name := controlNetworkName(m.nodeID)
	insp, found, err := m.inspectControlNetwork(opCtx, name)
	if err != nil {
		return err
	}
	if !found {
		insp, err = m.createControlNetwork(opCtx, name)
		if err != nil {
			return err
		}
	}
	containerized, selfID, err := m.connectControlNetworkAccess(opCtx, insp)
	if err != nil {
		return err
	}
	// Set only after every check passed. It is startup configuration: written
	// once before the worker becomes ready, read-only afterwards.
	m.controlNetwork = name
	m.controlSelfID = selfID
	m.Logger().Info(
		"Runtime: persistent control network ready",
		"containerized", containerized,
	)
	return nil
}

// VerifyControlNetwork checks that the provisioned control network still
// exists, is still Relay-owned with a compatible configuration, AND that this
// Relay process still has a working path to it: a containerized Relay must still
// have a usable endpoint on the validated network ID, while direct-route mode
// must still prove a local interface in the network's subnet. It is read-only:
// readiness never re-attaches or creates anything. A detached container or a
// lost host route therefore stops readiness and lets execution creates fail
// closed; recovery is an explicit Relay restart.
func (m *Manager) VerifyControlNetwork(ctx context.Context) error {
	if m == nil || m.cli == nil {
		return errors.New("control network: docker client unavailable")
	}
	if m.controlNetwork == "" {
		return errors.New("control network is not provisioned")
	}
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	nw, found, err := m.inspectControlNetwork(opCtx, m.controlNetwork)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("control network %q no longer exists", m.controlNetwork)
	}
	return m.verifyControlNetworkAccess(opCtx, nw)
}

// verifyControlNetworkAccess re-verifies this process's actual path to the
// control network without mutating it. Containerized mode inspects the
// previously verified full self ID (never NODE_ID) and requires an endpoint on
// the validated network ID with a usable address. Direct-route mode re-runs the
// exact local bridge/endpoint proof.
func (m *Manager) verifyControlNetworkAccess(
	ctx context.Context,
	nw network.Inspect,
) error {
	if m.controlSelfID != "" {
		return m.verifySelfControlEndpoint(ctx, nw)
	}
	return verifyLocalControlNetworkRoute(nw)
}

// executionNetworks returns the exact network set a new execution container
// joins: the internal control network plus the optional operator NETWORKS set.
// An empty control identity is an error: NETWORKS is application connectivity
// only and is never an execution-IPC fallback. NetworkSet canonicalizes the
// result for Docker's map-shaped EndpointsConfig.
func (m *Manager) executionNetworks() ([]string, error) {
	if m.controlNetwork == "" {
		return nil, errors.New("internal control network is not provisioned")
	}
	return NetworkSet(append([]string{m.controlNetwork}, m.networks...)), nil
}

// validateControlForExecution revalidates the control network immediately
// before an execution-container create, so a network deleted or altered while
// Relay is running fails the create instead of producing an unreachable
// container. An unprovisioned control network also fails here; the caller
// already holds a Manager operation.
func (m *Manager) validateControlForExecution(ctx context.Context) error {
	if m.controlNetwork == "" {
		return errors.New("internal control network is not provisioned")
	}
	_, found, err := m.inspectControlNetwork(ctx, m.controlNetwork)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf(
			"internal control network %q no longer exists; refusing to create an execution container",
			m.controlNetwork,
		)
	}
	return nil
}

// inspectControlNetwork inspects and validates a control network by name.
// found is false only for a genuine not-found; every other failure (including
// an ownership/configuration conflict) is returned as an error so a broken
// daemon or a conflicting network is never treated as absent.
func (m *Manager) inspectControlNetwork(
	ctx context.Context,
	name string,
) (network.Inspect, bool, error) {
	insp, err := m.cli.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return network.Inspect{}, false, nil
		}
		return network.Inspect{}, false, fmt.Errorf(
			"control network %q: inspect: %w",
			name, m.classifyDockerErr(ctx, err),
		)
	}
	if err := validateControlNetwork(insp.Network, name, m.nodeID); err != nil {
		return network.Inspect{}, true, err
	}
	return insp.Network, true, nil
}

// createControlNetwork creates the bridge network and then validates exactly
// what the daemon produced. The installed Moby client (v0.6.0, API v1.56.0)
// does not expose the deprecated CheckDuplicate create option; daemons that
// support API >=1.44 reject duplicate names systematically, so a concurrent
// create surfaces as a conflict and the winner is inspected and validated. To
// remain race-safe on older daemons too, the created network is validated by
// its returned ID and the name lookup must resolve back to that same ID.
// Relay never adopts a conflicting or ambiguous network silently.
func (m *Manager) createControlNetwork(
	ctx context.Context,
	name string,
) (network.Inspect, error) {
	created, err := m.cli.NetworkCreate(ctx, name, client.NetworkCreateOptions{
		Driver: controlNetworkBridgeDriver,
		Labels: controlNetworkLabels(m.nodeID),
	})
	if err != nil && !cerrdefs.IsConflict(err) {
		return network.Inspect{}, fmt.Errorf(
			"control network %q: create: %w",
			name, m.classifyDockerErr(ctx, err),
		)
	}
	if err == nil {
		if created.ID == "" {
			return network.Inspect{}, fmt.Errorf(
				"control network %q: create returned no id; refusing to reuse it by name",
				name,
			)
		}
		createdInsp, inspErr := m.cli.NetworkInspect(
			ctx, created.ID, client.NetworkInspectOptions{},
		)
		if inspErr != nil {
			return network.Inspect{}, fmt.Errorf(
				"control network %q: inspect created network: %w",
				name, m.classifyDockerErr(ctx, inspErr),
			)
		}
		if vErr := validateControlNetwork(
			createdInsp.Network, name, m.nodeID,
		); vErr != nil {
			return network.Inspect{}, vErr
		}
	}
	insp, found, err := m.inspectControlNetwork(ctx, name)
	if err != nil {
		return network.Inspect{}, err
	}
	if !found {
		return network.Inspect{}, fmt.Errorf(
			"control network %q: created network is not inspectable", name,
		)
	}
	if created.ID != "" && insp.ID != created.ID {
		// The name resolves to a different network than the one Relay just
		// created (possible on daemons that still allow duplicate names).
		// Refuse rather than let a later by-name lookup select an arbitrary
		// network.
		return network.Inspect{}, fmt.Errorf(
			"control network %q: name resolves to a different network than the one just created; refusing ambiguous reuse",
			name,
		)
	}
	return insp, nil
}

// validateControlNetwork enforces Relay's strict ownership and configuration
// requirements before a network is reused. The check is by label and driver,
// not by name alone.
func validateControlNetwork(
	nw network.Inspect,
	name string,
	nodeID string,
) error {
	if nw.Name != name {
		return fmt.Errorf(
			"control network %q: daemon returned network %q; refusing to reuse it",
			name, nw.Name,
		)
	}
	if nw.Labels[networkRoleLabel] != executionControlNetworkRole {
		return fmt.Errorf(
			"control network %q already exists without %s=%s; refusing to adopt or modify it",
			name, networkRoleLabel, executionControlNetworkRole,
		)
	}
	if nw.Labels[labelNodeID] != nodeID {
		return fmt.Errorf(
			"control network %q is owned by a different relay node; refusing to reuse it",
			name,
		)
	}
	if !strings.EqualFold(nw.Labels[controlNetworkManagedLabel], "true") {
		return fmt.Errorf(
			"control network %q is missing the Relay-managed marker %s=true; refusing to reuse it",
			name, controlNetworkManagedLabel,
		)
	}
	if nw.Driver != controlNetworkBridgeDriver {
		return fmt.Errorf(
			"control network %q uses driver %q; Relay requires %q",
			name, nw.Driver, controlNetworkBridgeDriver,
		)
	}
	if nw.Scope != "" && nw.Scope != "local" {
		return fmt.Errorf(
			"control network %q has scope %q; Relay requires a local network",
			name, nw.Scope,
		)
	}
	if nw.Internal {
		// An internal bridge has no outbound access, which would silently
		// remove the app egress the normal managed bridge provides.
		return fmt.Errorf(
			"control network %q is internal; Relay's control network must preserve outbound egress",
			name,
		)
	}
	if nw.ConfigOnly || nw.Ingress {
		return fmt.Errorf(
			"control network %q is a config-only or ingress network; Relay cannot use it",
			name,
		)
	}
	if strings.EqualFold(nw.Options[controlNetworkICCOption], "false") {
		return fmt.Errorf(
			"control network %q disables inter-container communication; Relay cannot use it",
			name,
		)
	}
	if strings.EqualFold(nw.Options[controlNetworkMasqueradeOption], "false") {
		return fmt.Errorf(
			"control network %q disables IP masquerade; Relay's workloads would lose outbound access",
			name,
		)
	}
	return nil
}

// connectControlNetworkAccess proves how this Relay process reaches the
// control network, or fails closed when it cannot.
//
// A container-ID-like hostname is the only safe self-container candidate: Relay
// inspects that container, verifies its full ID prefix and Config.Hostname,
// attaches it idempotently when needed, and verifies the endpoint address. Any
// failure there is an actionable startup error; Relay never falls back to
// attaching a different container.
//
// A non-ID hostname does NOT prove Relay is a host process: it may be a
// container with a custom hostname. For that case Relay requires an exact local
// route proof (see verifyLocalControlNetworkRoute): the local Docker bridge for
// this exact network ID carrying its IPAM gateway (native Linux host or
// host-network container), or a local interface exactly matching an endpoint
// Docker reports on this network (an operator-pre-attached custom-hostname
// container). A merely overlapping subnet is not accepted, so a remote Engine
// or Docker Desktop host process fails closed with guidance instead of being
// claimed as supported.
func (m *Manager) connectControlNetworkAccess(
	ctx context.Context,
	nw network.Inspect,
) (bool, string, error) {
	self, ok := m.selfContainerName()
	if !ok {
		if err := verifyLocalControlNetworkRoute(nw); err != nil {
			return false, "", err
		}
		return false, "", nil
	}
	id, err := m.attachSelfToControlNetwork(ctx, nw, self)
	if err != nil {
		return false, "", err
	}
	return true, id, nil
}

// attachSelfToControlNetwork inspects Relay's own container, attaches it to the
// validated network when it is not already a member, and confirms the endpoint
// has a usable address and belongs to the validated network ID. It returns the
// full inspected container ID so readiness can re-verify the same container
// later. The process hostname is used only for the initial safe resolution;
// NODE_ID is never treated as a container ID.
func (m *Manager) attachSelfToControlNetwork(
	ctx context.Context,
	nw network.Inspect,
	self string,
) (string, error) {
	insp, err := m.cli.ContainerInspect(ctx, self, client.ContainerInspectOptions{})
	if err != nil {
		return "", fmt.Errorf(
			"control network %q: cannot inspect Relay's own container: %w",
			nw.Name, m.classifyDockerErr(ctx, err),
		)
	}
	if !isSelfContainer(insp.Container, self) {
		return "", fmt.Errorf(
			"control network %q: the container resolved from this process's hostname is not Relay; refusing to attach it",
			nw.Name,
		)
	}
	id := insp.Container.ID
	if id == "" {
		return "", fmt.Errorf(
			"control network %q: Relay's own container has no id", nw.Name,
		)
	}
	if endpointOnControlNetwork(containerNetworks(insp.Container)[nw.Name], nw) {
		return id, nil
	}

	// Pin the validated network ID, not the name, so a delete/recreate race
	// between validation and connect cannot attach this container to a
	// replacement network.
	target := nw.ID
	if target == "" {
		target = nw.Name
	}
	_, connectErr := m.cli.NetworkConnect(ctx, target, client.NetworkConnectOptions{
		Container:      id,
		EndpointConfig: &network.EndpointSettings{},
	})

	// Re-inspect regardless of the connect result: an "already attached"
	// response is a benign race, and the endpoint must be proven usable either
	// way.
	connected, err := m.selfControlNetworkEndpoint(ctx, id, nw)
	if err != nil {
		return "", err
	}
	if connected {
		return id, nil
	}
	if connectErr != nil {
		return "", fmt.Errorf(
			"control network %q: attach Relay's own container: %w",
			nw.Name, m.classifyDockerErr(ctx, connectErr),
		)
	}
	return "", fmt.Errorf(
		"control network %q: Relay's own container has no usable address after attach",
		nw.Name,
	)
}

// verifySelfControlEndpoint re-verifies Relay's actual endpoint on the control
// network using the full container ID resolved and verified at startup. It never
// inspects by NODE_ID or by name, so a replaced or unrelated container cannot
// satisfy the check.
func (m *Manager) verifySelfControlEndpoint(
	ctx context.Context,
	nw network.Inspect,
) error {
	if m.controlSelfID == "" {
		return errors.New("control network: Relay's own container identity is not recorded")
	}
	insp, err := m.cli.ContainerInspect(ctx, m.controlSelfID, client.ContainerInspectOptions{})
	if err != nil {
		return fmt.Errorf(
			"control network %q: inspect Relay's own container: %w",
			nw.Name, m.classifyDockerErr(ctx, err),
		)
	}
	if !isSelfContainer(insp.Container, m.selfHostname) {
		return fmt.Errorf(
			"control network %q: stored Relay container identity no longer matches this process",
			nw.Name,
		)
	}
	if !endpointOnControlNetwork(containerNetworks(insp.Container)[nw.Name], nw) {
		return fmt.Errorf(
			"control network %q: Relay's own container is no longer attached with a usable address",
			nw.Name,
		)
	}
	return nil
}

// selfControlNetworkEndpoint re-inspects Relay's container and reports whether
// its control-network endpoint now belongs to the validated network and carries
// a concrete unicast address.
func (m *Manager) selfControlNetworkEndpoint(
	ctx context.Context,
	id string,
	nw network.Inspect,
) (bool, error) {
	insp, err := m.cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return false, fmt.Errorf(
			"control network %q: verify Relay's own attachment: %w",
			nw.Name, m.classifyDockerErr(ctx, err),
		)
	}
	return endpointOnControlNetwork(containerNetworks(insp.Container)[nw.Name], nw), nil
}

// containerNetworks returns an inspected container's network endpoints,
// tolerating a daemon response without network settings.
func containerNetworks(c container.InspectResponse) map[string]*network.EndpointSettings {
	if c.NetworkSettings == nil {
		return nil
	}
	return c.NetworkSettings.Networks
}

// usableEndpointAddress reports whether an endpoint carries an address that can
// actually be dialed.
func usableEndpointAddress(ep *network.EndpointSettings) bool {
	if ep == nil {
		return false
	}
	return usableExecutionAddress(ep.IPAddress, true) ||
		usableExecutionAddress(ep.GlobalIPv6Address, false)
}

// endpointOnControlNetwork reports whether an existing endpoint both belongs to
// the validated network and has a usable address. A non-empty NetworkID that
// names a different network means the container's same-named endpoint is stale,
// so Relay must attach to the validated network rather than trust the address.
func endpointOnControlNetwork(
	ep *network.EndpointSettings,
	nw network.Inspect,
) bool {
	if ep == nil || !usableEndpointAddress(ep) {
		return false
	}
	if ep.NetworkID != "" && nw.ID != "" && ep.NetworkID != nw.ID {
		return false
	}
	return true
}

// verifyLocalControlNetworkRoute proves that THIS process namespace has a real
// data path to the control network, using only the inspected network and the
// local interfaces. Any local subnet address is NOT sufficient: a remote Engine
// whose bridge subnet overlaps a local interface (common with 172.x ranges)
// would falsely pass even though the dial goes elsewhere.
//
// Two exact proofs are accepted:
//   - Native Linux host (or host-network container): the Docker-generated bridge
//     interface for this exact network ID (`br-<first 12 id chars>`) is up and
//     carries the inspected IPAM gateway address.
//   - A custom-hostname Relay container already attached by the operator: one of
//     its local interfaces has exactly the endpoint address, prefix length, and
//     MAC address Docker reports for that endpoint on this network.
//
// Everything else fails closed: Docker Desktop host processes, remote Engines,
// and custom-hostname containers that were not pre-attached cannot be proven
// safe without guessing.
func verifyLocalControlNetworkRoute(nw network.Inspect) error {
	routes := controlNetworkRoutes(nw)
	if len(routes) == 0 {
		return fmt.Errorf(
			"control network %q has no IPAM subnet/gateway; Relay cannot verify a direct route from this process",
			nw.Name,
		)
	}
	local, err := localInterfaceAddresses()
	if err != nil {
		return fmt.Errorf(
			"control network %q: cannot enumerate local interfaces: %w",
			nw.Name, err,
		)
	}
	if hostBridgeRoute(nw, routes, local) {
		return nil
	}
	if preattachedContainerRoute(nw, local) {
		return nil
	}
	return fmt.Errorf(
		"control network %q: Relay cannot prove a route to it from this process; "+
			"a Linux host needs the local Docker bridge %s with its IPAM gateway, "+
			"and a custom-hostname Relay container must already be attached to that network "+
			"with an interface matching its endpoint address, prefix, and MAC "+
			"(remote Docker Engines and Docker Desktop host processes are not supported)",
		nw.Name, controlBridgeInterfaceName(nw.ID),
	)
}

// controlNetworkRoute is one inspected IPAM subnet/gateway pair.
type controlNetworkRoute struct {
	subnet  netip.Prefix
	gateway netip.Addr
}

// controlNetworkRoutes returns the valid IPAM subnet/gateway pairs Docker
// assigned to the network.
func controlNetworkRoutes(nw network.Inspect) []controlNetworkRoute {
	var routes []controlNetworkRoute
	for _, cfg := range nw.IPAM.Config {
		if !cfg.Subnet.IsValid() {
			continue
		}
		routes = append(routes, controlNetworkRoute{
			subnet:  cfg.Subnet,
			gateway: cfg.Gateway,
		})
	}
	return routes
}

// localAddress is one address assigned to a local interface.
type localAddress struct {
	name     string
	addr     netip.Addr
	prefix   int
	hardware net.HardwareAddr
	up       bool
}

// localInterfaceAddresses enumerates this process namespace's interface
// addresses with the interface name, prefix length, MAC, and up state needed for
// exact route proofs.
func localInterfaceAddresses() ([]localAddress, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var out []localAddress
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("interface %s: %w", iface.Name, err)
		}
		for _, address := range addresses {
			ipNet, ok := address.(*net.IPNet)
			if !ok {
				continue
			}
			addr, ok := netip.AddrFromSlice(ipNet.IP)
			if !ok {
				continue
			}
			addr = addr.Unmap()
			ones, bits := ipNet.Mask.Size()
			if bits != addr.BitLen() {
				continue
			}
			out = append(out, localAddress{
				name:     iface.Name,
				addr:     addr,
				prefix:   ones,
				hardware: iface.HardwareAddr,
				up:       iface.Flags&net.FlagUp != 0,
			})
		}
	}
	return out, nil
}

// controlBridgeInterfaceName is the Linux bridge interface Docker creates for a
// user-defined network: "br-" plus the first 12 hex characters of its ID.
func controlBridgeInterfaceName(networkID string) string {
	if len(networkID) < 12 {
		return "br-<network id>"
	}
	return "br-" + networkID[:12]
}

// hostBridgeRoute reports whether the local Docker bridge for this exact
// network ID is up and carries the inspected IPAM gateway. The network ID is
// random, so a remote Engine or an overlapping local subnet cannot satisfy this
// proof by coincidence.
func hostBridgeRoute(
	nw network.Inspect,
	routes []controlNetworkRoute,
	local []localAddress,
) bool {
	bridge := controlBridgeInterfaceName(nw.ID)
	if len(nw.ID) < 12 {
		return false
	}
	for _, route := range routes {
		if !route.gateway.IsValid() {
			continue
		}
		for _, candidate := range local {
			if !candidate.up || candidate.name != bridge {
				continue
			}
			if candidate.addr == route.gateway && route.subnet.Contains(candidate.addr) {
				return true
			}
		}
	}
	return false
}

// preattachedContainerRoute reports whether a local interface is exactly the
// endpoint Docker reports on this network: same address, prefix length, and
// MAC. That is only true inside the pre-attached custom-hostname Relay
// container's own network namespace; an arbitrary local interface in an
// overlapping subnet does not match the endpoint's MAC.
func preattachedContainerRoute(nw network.Inspect, local []localAddress) bool {
	for _, endpoint := range nw.Containers {
		if endpoint.IPv4Address.IsValid() &&
			matchesLocalEndpoint(
				local, endpoint.IPv4Address.Addr().Unmap(),
				endpoint.IPv4Address.Bits(), endpoint.MacAddress,
			) {
			return true
		}
		if endpoint.IPv6Address.IsValid() &&
			matchesLocalEndpoint(
				local, endpoint.IPv6Address.Addr().Unmap(),
				endpoint.IPv6Address.Bits(), endpoint.MacAddress,
			) {
			return true
		}
	}
	return false
}

// matchesLocalEndpoint reports whether an up local interface has exactly the
// endpoint address, prefix length, and MAC. Both MACs must be present and equal:
// an absent MAC never degrades the proof to address/prefix-only identification,
// which an overlapping remote interface could otherwise satisfy.
func matchesLocalEndpoint(
	local []localAddress,
	addr netip.Addr,
	prefix int,
	mac network.HardwareAddr,
) bool {
	if len(mac) == 0 {
		return false
	}
	for _, candidate := range local {
		if !candidate.up || candidate.addr != addr || candidate.prefix != prefix {
			continue
		}
		if len(candidate.hardware) == 0 {
			continue
		}
		if strings.EqualFold(candidate.hardware.String(), mac.String()) {
			return true
		}
	}
	return false
}
