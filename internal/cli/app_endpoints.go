package cli

import (
	"fmt"
	"net"
	"strconv"

	"relay/internal/worker"
)

// serviceEndpointView is the live service-publication view rendered by
// `relay app inspect`. It is nil when publication is not configured, which
// preserves the existing inspect output. Live false means the worker socket or
// Docker could not provide current facts; the configured bind/advertise
// addresses are still rendered, and no persisted or guessed endpoint is
// substituted.
type serviceEndpointView struct {
	BindAddr      string
	AdvertiseAddr string
	Endpoints     []worker.ServiceEndpoint
	Live          bool
}

// resolveServiceEndpoints resolves the effective publication configuration and,
// when it is enabled, asks the running worker for its current local Docker
// facts. A disabled publication returns nil so inspect output is unchanged. A
// configuration error is returned to the caller; an unreachable worker or a
// failed Docker query returns a non-live view with the configured addresses
// instead of failing inspect or falling back to persisted state.
func resolveServiceEndpoints(name string, deps Dependencies) (*serviceEndpointView, error) {
	if deps.LoadServicePublication == nil {
		return nil, nil
	}
	bindAddr, advertiseAddr, err := deps.LoadServicePublication()
	if err != nil {
		return nil, fmt.Errorf("app inspect: %w", err)
	}
	if bindAddr == "" {
		return nil, nil
	}
	result, err := worker.QueryServiceEndpoints(deps.SocketPath, name)
	if err != nil {
		return &serviceEndpointView{
			BindAddr:      bindAddr,
			AdvertiseAddr: advertiseAddr,
		}, nil
	}
	return &serviceEndpointView{
		BindAddr:      result.BindAddr,
		AdvertiseAddr: result.AdvertiseAddr,
		Endpoints:     result.Endpoints,
		Live:          true,
	}, nil
}

// renderAppServiceEndpoints renders the configured bind/advertise addresses and
// each replica's actual Docker state and host endpoint. A non-live view reports
// unavailability explicitly rather than falling back to persisted state.
func renderAppServiceEndpoints(out *output, view *serviceEndpointView) {
	if view == nil {
		return
	}
	out.blank()
	out.line("Service endpoints:")
	tw := out.table(3)
	fmt.Fprintf(tw, "  Bind:\t%s\n", displayEndpointAddress(view.BindAddr))
	fmt.Fprintf(tw, "  Advertise:\t%s\n", displayEndpointAddress(view.AdvertiseAddr))
	if !view.Live {
		fmt.Fprintln(tw, "  Endpoints:\tunavailable (worker or Docker unavailable)")
		out.flush(tw)
		return
	}
	if len(view.Endpoints) == 0 {
		fmt.Fprintln(tw, "  Endpoints:\tnone")
		out.flush(tw)
		return
	}
	for _, endpoint := range view.Endpoints {
		fmt.Fprintf(
			tw,
			"  %s\t%s\t%s\n",
			serviceEndpointName(endpoint),
			serviceEndpointStatus(endpoint),
			serviceEndpointBinding(view, endpoint),
		)
	}
	out.flush(tw)
}

// serviceEndpointName renders a replica identity, omitting an unknown replica
// label instead of printing a negative index.
func serviceEndpointName(endpoint worker.ServiceEndpoint) string {
	if endpoint.Replica < 0 {
		return endpoint.Service
	}
	return fmt.Sprintf("%s[%d]", endpoint.Service, endpoint.Replica)
}

// serviceEndpointStatus reports Docker's actual state and health. Health is
// rendered only while the container is running (or when it reports a
// non-healthy value), so an exited container that still carries a stale
// "healthy" health status is never presented as healthy.
func serviceEndpointStatus(endpoint worker.ServiceEndpoint) string {
	if endpoint.Missing {
		return "not found"
	}
	status := endpoint.State
	if status == "" {
		status = "-"
	}
	if endpoint.Health != "" &&
		endpoint.Health != "none" &&
		(endpoint.State == "running" || endpoint.Health != "healthy") {
		status += " health=" + endpoint.Health
	}
	return status
}

// serviceEndpointBinding renders the actual Docker-assigned host endpoint as
// both the BIND address and the advertised address, using the same host port.
// HostPort 0 means the live view proved no valid binding; it is never replaced
// with a configured or guessed port. A non-running container keeps its binding
// visible but is marked inactive, so its state is not confused with a serving
// endpoint.
func serviceEndpointBinding(view *serviceEndpointView, endpoint worker.ServiceEndpoint) string {
	if endpoint.HostPort == 0 || endpoint.HostIP == "" {
		return "not published"
	}
	port := strconv.Itoa(endpoint.HostPort)
	binding := "bind=" + net.JoinHostPort(endpoint.HostIP, port)
	if view.AdvertiseAddr != "" {
		binding += " advertise=" + net.JoinHostPort(view.AdvertiseAddr, port)
	}
	binding += fmt.Sprintf(" -> %d/tcp", endpoint.Port)
	if endpoint.State != "running" {
		binding += " (container not running)"
	}
	return binding
}

// displayEndpointAddress renders an optional configured address, distinguishing
// "not configured" from a real address without printing an empty value.
func displayEndpointAddress(addr string) string {
	if addr == "" {
		return "-"
	}
	return addr
}
