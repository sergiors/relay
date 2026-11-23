package discovery

import "relay/internal/state"

// routableEndpoint is the proxy-neutral discovery view of one verified service
// replica. It carries only the facts a routing provider needs: the logical
// identity, the effective route, and the reachable backend address. Registry
// persistence details (node, replica, container, generation) deliberately do
// not cross this boundary.
type routableEndpoint struct {
	app           string
	service       string
	host          string
	path          string
	advertiseAddr string
	hostPort      int
}

// projectRoutableEndpoints maps the state registry read model to the
// proxy-neutral discovery model. The Traefik transformer still performs the
// final usability checks, so a malformed projection cannot emit a partial
// route.
func projectRoutableEndpoints(
	endpoints []state.ServiceRegistryEndpoint,
) []routableEndpoint {
	out := make([]routableEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		out = append(out, routableEndpoint{
			app:           endpoint.App,
			service:       endpoint.Service,
			host:          endpoint.Host,
			path:          endpoint.Path,
			advertiseAddr: endpoint.AdvertiseAddr,
			hostPort:      endpoint.HostPort,
		})
	}
	return out
}
