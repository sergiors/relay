// Package routing holds Relay's provider-neutral route-host policy: the
// worker-wide effective-host override and the hostname contract used by both
// service reconciliation and registry publication. Routing itself is provided
// by an external proxy reading Relay's discovery API; Relay does not generate
// proxy-specific container labels or participate in proxy configuration.
package routing

import (
	"fmt"
	"regexp"
	"strings"
)

// hostnamePattern mirrors the service `host` hostname rule in
// internal/app (RFC-1123-style labels): case-insensitive alphanumeric labels
// separated by dots, each label 1-63 chars and not hyphen-bounded. It is
// duplicated here rather than importing internal/app: routing is a
// template-unaware leaf, and app already depends on the source layer, so the
// reverse import would invert the dependency direction for one regexp.
var hostnamePattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$`)

// validateHostname reports whether host is a valid hostname, using the same
// rules as the service `host` validation in internal/app: non-empty, at most
// 253 characters, no whitespace, and matching hostnamePattern. Empty is NOT
// valid here — callers that allow the "no host" (unrouted) case exclude it
// first. This helper is the shared rule for the worker-wide override value and
// for an explicitly supplied effective host.
func validateHostname(host string) error {
	if host == "" {
		return fmt.Errorf("host is empty")
	}
	if len(host) > 253 {
		return fmt.Errorf("host %q exceeds the 253-character hostname limit", host)
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return fmt.Errorf("host %q contains whitespace", host)
	}
	if !hostnamePattern.MatchString(host) {
		return fmt.Errorf("host %q is not a valid hostname", host)
	}
	return nil
}

// EffectiveHost returns the routing hostname for a service: the declared host
// unchanged when override is unset or whitespace-only, otherwise the exact
// override hostname. The override is a single effective hostname, never a
// suffix or a rewrite of the declared host. The declared host is passed by
// value, so template/logging/fingerprinting code keeps the original.
func EffectiveHost(host, override string) string {
	if !HostOverrideConfigured(override) {
		return host
	}
	return override
}

// HostOverrideConfigured reports whether the optional worker-wide override is
// set. Empty and whitespace-only values mean "no override".
func HostOverrideConfigured(override string) bool {
	return strings.TrimSpace(override) != ""
}

// ValidateHostOverride validates the optional worker-wide ROUTING_HOST_OVERRIDE
// value using the service-host hostname contract (see validateHostname). Unset
// and whitespace-only values mean no override and are valid. The value is used
// verbatim as the effective route host, so a malformed value would silently
// never match; it is rejected before any container or network work.
func ValidateHostOverride(override string) error {
	if !HostOverrideConfigured(override) {
		return nil
	}
	if err := validateHostname(override); err != nil {
		return fmt.Errorf("invalid ROUTING_HOST_OVERRIDE: %w", err)
	}
	return nil
}
