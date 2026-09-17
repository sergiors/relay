// Package routing holds Traefik-specific routing logic for Relay's persistent
// services: the label set stamped on a routed service container, the
// deterministic Traefik-safe router/service id, and the small config surface
// (TRAEFIK_NETWORK) the worker-level reconciler consults. NOTHING here talks to
// Docker or knows about templates — Traefik is the only routing integration
// supported (no provider abstraction), and the reconciler owns applying these
// labels and validating the network before starting routed containers.
package routing

import (
	"fmt"
	"strconv"
	"strings"
)

// TraefikLabelPrefix is the label-name prefix Traefik requires for its
// container-configuration labels.
const TraefikLabelPrefix = "traefik."

// TraefikConfig is the worker-level Traefik routing configuration: the Docker
// network Traefik is attached to (from the TRAEFIK_NETWORK environment
// variable), plus the optional router-slice values TRAEFIK_ENTRYPOINTS,
// TRAEFIK_CERTRESOLVER, and TRAEFIK_PRIORITY. Empty/unset optional values mean
// the corresponding label is simply omitted — nothing is defaulted here
// (no implicit "websecure" entrypoint, "letsencrypt" resolver, or priority).
type TraefikConfig struct {
	Network string
	// EntryPoints is the optional TRAEFIK_ENTRYPOINTS value: one or more
	// comma-separated Traefik entrypoint names (e.g. "websecure" or
	// "web,websecure", passed through verbatim into the label). When set it
	// generates a router `entrypoints` label. Empty = no entrypoints label.
	EntryPoints string
	// CertResolver is the optional TRAEFIK_CERTRESOLVER value (e.g.
	// "letsencrypt"): when set it generates BOTH the router `tls=true` and
	// `tls.certresolver` labels (TLS on, certificate resolved by that
	// resolver). Empty = neither tls label.
	CertResolver string
	// Priority is the optional TRAEFIK_PRIORITY value: nil = unset (no
	// priority label; Traefik's own default priority behavior applies), and a
	// non-nil pointer carries the configured positive value. A POINTER is
	// needed deliberately: 0 is never a valid priority in Relay (env parsing
	// requires positive ints), but only nil-vs-non-nil distinguishes "no
	// priority configured" from any provided value in the Config struct — an
	// int cannot express that, and the ability to CLEAR the value
	// (converging the priority label away) is part of reconciliation.
	Priority *int
}

// Validate reports whether the config is usable for a routed service. The
// network is the only required piece: Traefik reads Docker labels per-network,
// so without the network name the labels would attach to the wrong (or no)
// provider-side network and routing would silently not work. The optional
// entrypoint/certresolver/priority values need no validation here — unset
// simply means the corresponding label is omitted.
func (c TraefikConfig) Validate() error {
	if c.Network == "" {
		return fmt.Errorf("TRAEFIK_NETWORK is required when Traefik routing is configured")
	}
	return nil
}

// MissingNetwork is the user-facing error for a configured Traefik network that
// does not exist on the Docker daemon. Relay never creates the network — it is
// infrastructure owned outside Relay — so a missing network is an operator
// condition, never something to fix silently.
func MissingNetwork(network string) error {
	return fmt.Errorf("Traefik network %q does not exist", network)
}

// TraefikLabels returns the Traefik label set for one routed service. A service
// with no host is unrouted (internal-only), and gets NO Traefik labels at all —
// otherwise Traefik would route it for whatever host rule its labels declare.
// Concretely: an unrouted service returns a NIL map (the caller-visible "no
// routing" value), and the returned map is read-only — callers merge it into a
// container spec, never mutate it, so a nil return is safe to range over but
// never safe to write to.
//
// A routed service always gets the four base labels (plus the shared
// router/service id):
//
//	traefik.enable                                              = true
//	traefik.docker.network                                      = <network>  (only when cfg.Network != "")
//	traefik.http.routers.<id>.rule                              = Host(`<host>`)
//	traefik.http.services.<id>.loadbalancer.server.port         = <port>
//
// The docker.network label pins which network Traefik resolves the container
// on; it is omitted without a configured network so the container's
// membership alone decides.
//
// The optional config values then add router-slice labels on the SAME <id>,
// exactly WHEN their value is set — nothing is defaulted (no implicit
// "websecure" entrypoint, "letsencrypt" resolver, or fallback priority; the
// Traefik-side default behavior applies whenever a label is omitted):
//
//	cfg.EntryPoints  != ""  → traefik.http.routers.<id>.entrypoints        = <cfg.EntryPoints>
//	cfg.CertResolver != ""  → traefik.http.routers.<id>.tls                = true
//	                            traefik.http.routers.<id>.tls.certresolver = <cfg.CertResolver>
//	cfg.Priority     != nil → traefik.http.routers.<id>.priority           = <cfg.Priority>
func TraefikLabels(functionName, entrypoint, host string, port int, cfg TraefikConfig) map[string]string {
	if host == "" {
		return nil
	}
	id := ServiceProviderID(functionName, entrypoint)
	labels := map[string]string{
		"traefik.enable": "true",
		fmt.Sprintf("traefik.http.routers.%s.rule", id):                      fmt.Sprintf("Host(`%s`)", host),
		fmt.Sprintf("traefik.http.services.%s.loadbalancer.server.port", id): strconv.Itoa(port),
	}
	if cfg.Network != "" {
		labels["traefik.docker.network"] = cfg.Network
	}
	if cfg.EntryPoints != "" {
		labels[fmt.Sprintf("traefik.http.routers.%s.entrypoints", id)] = cfg.EntryPoints
	}
	if cfg.CertResolver != "" {
		labels[fmt.Sprintf("traefik.http.routers.%s.tls", id)] = "true"
		labels[fmt.Sprintf("traefik.http.routers.%s.tls.certresolver", id)] = cfg.CertResolver
	}
	if cfg.Priority != nil {
		labels[fmt.Sprintf("traefik.http.routers.%s.priority", id)] = strconv.Itoa(*cfg.Priority)
	}
	return labels
}

// ServiceProviderID derives the deterministic, Traefik-safe router/service id
// for one service: `relay-<function>-<entrypoint>`.
//
// Traefik router/service names appearing in labels must be identifier-safe, but
// the service identity parts are NOT: a function name may contain dots, and an
// entrypoint is a file path ("app/main.py") containing "/" and "."
// characters that would break Traefik's label grammar. So every character
// outside [a-z0-9-] is sanitized to "-", consecutive "-" collapsed, and
// leading/trailing "-" trimmed, per part. The id must be deterministic so
// reconciliation produces stable labels across passes (identical labels keep a
// container a keep candidate instead of stale).
func ServiceProviderID(functionName, entrypoint string) string {
	raw := "relay-" + traefikSafePart(functionName) + "-" + traefikSafePart(entrypoint)
	if len(raw) > 100 {
		raw = raw[:100]
	}
	return raw
}

// traefikSafePart lowercases s, replaces every character outside [a-z0-9-] with
// "-", collapses consecutive "-", and trims leading/trailing "-".
func traefikSafePart(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	clean := b.String()
	for strings.Contains(clean, "--") {
		clean = strings.ReplaceAll(clean, "--", "-")
	}
	return strings.Trim(clean, "-")
}

// IsTraefikLabel reports whether key belongs to Traefik's label namespace. The
// service reconciler uses it to detect Traefik-owned keys on a running
// container without embedding the prefix string in the reconciler.
func IsTraefikLabel(key string) bool {
	return strings.HasPrefix(key, TraefikLabelPrefix)
}
