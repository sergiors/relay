// Package routing holds Traefik-specific routing logic for Relay's persistent
// services: the label set stamped on a routed service container, the
// deterministic Traefik-safe router/service id, and the small config surface
// (TRAEFIK_NETWORK and the optional TRAEFIK_HOST_OVERRIDE) the worker-level
// reconciler consults. NOTHING here talks to Docker or knows about templates —
// Traefik is the only routing integration supported (no provider abstraction),
// and the reconciler owns applying these labels and validating the network
// before starting routed containers.
package routing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// serviceProviderIDMaxLen is the maximum length of a Traefik-safe
// router/service/middleware identifier appearing in a label name. It is well
// below Traefik's own limits and keeps generated keys comfortably short.
const serviceProviderIDMaxLen = 100

// identityHashLen is the number of hex characters in the collision-resistant
// suffix appended to every provider/middleware id. 16 hex chars = 64 bits,
// matching the short-handle convention used for Relay's content-addressed image
// tags (see runtime.tagPrefixLen): a birthday collision at Relay's scale is
// astronomically unlikely. The suffix is derived from the FULL, pre-sanitization
// identity, so two identities that sanitize or truncate to the same base are
// still distinguished.
const identityHashLen = 16

// TraefikLabelPrefix is the label-name prefix Traefik requires for its
// container-configuration labels.
const TraefikLabelPrefix = "traefik."

// TraefikConfig is the worker-level Traefik routing configuration: the Docker
// network Traefik is attached to (from the TRAEFIK_NETWORK environment
// variable), plus the optional router-slice values TRAEFIK_ENTRYPOINTS,
// TRAEFIK_CERTRESOLVER, and TRAEFIK_PRIORITY, and the optional host override
// TRAEFIK_HOST_OVERRIDE. Empty/unset optional values mean the corresponding
// label is simply omitted — nothing is defaulted here (no implicit
// "websecure" entrypoint, "letsencrypt" resolver, or priority).
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
	// HostOverride is the optional TRAEFIK_HOST_OVERRIDE value: the domain
	// suffix that replaces the domain part of every routed service's declared
	// host, keeping the host's left-most label. It is the operator escape
	// hatch for running a template whose hosts belong to a real domain (e.g.
	// "issuer.example.com") on a local/inner environment (e.g.
	// "issuer.localhost") without editing the template. Empty = no override:
	// the declared host is used verbatim, byte-for-byte the pre-override
	// behavior. When set it must itself be a valid hostname (see
	// validateHostname), and the host derived from it is validated per service
	// against the hostname length limit (see ValidateHost).
	HostOverride string
}

// hostnamePattern mirrors the service `host` hostname rule in
// internal/function (RFC-1123-style labels): case-insensitive alphanumeric
// labels separated by dots, each label 1-63 chars and not hyphen-bounded. It
// is duplicated here rather than importing internal/function: routing is a
// template-unaware leaf (see the package comment), and function already
// depends on the source layer, so the reverse import would invert the
// dependency direction for one regexp.
var hostnamePattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$`)

// validateHostname reports whether host is a valid hostname, using the same
// rules as the service `host` validation in internal/function: non-empty, at
// most 253 characters, no whitespace, and matching hostnamePattern. Empty is
// NOT valid here — callers that allow the "no host" (unrouted) case exclude it
// first. This helper is the shared rule for the override value (Validate) and
// for the effective host derived from it (ValidateHost).
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

// OverrideHost returns the effective host for a routed service: the declared
// host unchanged when cfg.HostOverride is empty, or the host's left-most label
// joined to the override suffix otherwise. The declared host is never mutated
// (strings are values), so callers keep using the template's original host for
// logs, fingerprinting, and stale-container detection.
//
// The mapping keeps ONLY the first dot-separated label of the declared host:
// "issuer.example.com" with override "localhost" becomes
// "issuer.localhost". A single-label host ("issuer") has no domain to replace,
// so it is treated the same way — the label is kept and the override appended
// ("issuer.localhost"). This is the only interpretation consistent with the
// multi-label case: the override supplies the domain (everything after the
// first label), and a label-only host has an empty domain, so that empty
// domain is exactly what gets replaced.
//
// OverrideHost itself does NOT re-validate: it stays a pure mapping function
// so the label builder needs no error path. Validation of the derived host
// belongs with the per-service routing check (see ValidateHost), which runs
// before any container/network work.
func (c TraefikConfig) OverrideHost(host string) string {
	if c.HostOverride == "" {
		return host
	}
	label := host
	if i := strings.IndexByte(host, '.'); i >= 0 {
		label = host[:i]
	}
	return label + "." + c.HostOverride
}

// Validate reports whether the config is usable for a routed service. The
// network is the only required piece: Traefik reads Docker labels per-network,
// so without the network name the labels would attach to the wrong (or no)
// provider-side network and routing would silently not work. The optional
// entrypoint/certresolver/priority values need no validation here — unset
// simply means the corresponding label is omitted.
//
// HostOverride is validated when set, using the same hostname rules as a
// service `host` (see validateHostname): the override becomes part of every
// routed rule, and a malformed suffix would silently produce hosts Traefik
// never matches. Unset (empty) means no override and needs no validation.
//
// Validate is deliberately host-independent: it cannot see a template host, so
// it cannot check the COMBINED length of a derived host. That check is
// per-service (ValidateHost) because a conservative global override length
// cap here would reject valid short-host cases (e.g. override "localhost"
// with host "issuer").
func (c TraefikConfig) Validate() error {
	if c.Network == "" {
		return fmt.Errorf("TRAEFIK_NETWORK is required when Traefik routing is configured")
	}
	if c.HostOverride != "" {
		if err := validateHostname(c.HostOverride); err != nil {
			return fmt.Errorf("invalid TRAEFIK_HOST_OVERRIDE: %w", err)
		}
	}
	return nil
}

// ValidateHost reports whether host is a usable EFFECTIVE host for a routed
// service under this config: the declared host unchanged when no override is
// set, or the host derived from the override otherwise (see OverrideHost). It
// applies exactly the same hostname rules the template parser applies to a
// declared service host (validateHostname), so an override can never smuggle
// in a value that is accepted here but rejected upstream — or vice versa.
//
// This is the per-service counterpart to Validate. It exists because the
// combined host is only knowable with the template host in hand: an override
// that is itself a valid hostname ("localhost") can still derive an overlong
// host from a long declared host (a 63-char left label + "." + the override
// can exceed 253). Validating the derived value here, with the actual host,
// accepts valid short-host cases a global conservative cap would reject.
//
// Empty host is VALID and returns nil: an empty host means unrouted (no
// Traefik labels at all), so there is no effective host to validate. Callers
// only run this for services declaring a host, but the empty case is harmless
// and keeps the helper total.
func (c TraefikConfig) ValidateHost(host string) error {
	if host == "" {
		return nil
	}
	return validateHostname(c.OverrideHost(host))
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
//	traefik.http.routers.<id>.rule                              = Host(`<effective host>`)
//	traefik.http.services.<id>.loadbalancer.server.port         = <port>
//
// <effective host> is the declared host when cfg.HostOverride is unset, or its
// override mapping otherwise (see cfg.OverrideHost). Only the host value in the
// rule changes under an override; the id, port, network, and optional router
// values are untouched.
//
// When path is non-empty the rule ALSO constrains the request path and a
// StripPrefix middleware is attached to the router, so the upstream service
// sees the request as if the prefix were not part of it:
//
//	traefik.http.routers.<id>.rule                              = Host(`<effective host>`) && PathPrefix(`<path>`)
//	traefik.http.routers.<id>.middlewares                       = <middleware>
//	traefik.http.middlewares.<middleware>.stripprefix.prefixes  = <path>
//
// <middleware> is PathMiddlewareID(functionName, identity): the deterministic
// service id plus the "-path" infix and the same collision-resistant hash
// suffix. It is distinct from <id>, so adding a path can never overwrite the
// router/service slices, and it is per-service (the id already encodes function
// + identity), so two services on the same host with different paths get
// distinct middleware names. An empty path adds NOTHING — the host-only label
// set is byte-for-byte the pre-path behavior.
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
//
// The path argument is expected to be canonical (see function.Template parsing):
// leading "/", no trailing slash except root, no "//". TraefikLabels treats it
// as opaque and does not re-validate it.
//
// When cfg.HostOverride is set, the declared host is mapped through
// cfg.OverrideHost (left-most label + override domain) before the rule is
// built; the path, id, port, network, and optional router values are
// unaffected. host itself is passed by value and never mutated, so the
// template's original host is preserved for callers. An empty host still
// yields a NIL map regardless of the override: unrouted means unrouted.
func TraefikLabels(functionName, identity, host, path string, port int, cfg TraefikConfig) map[string]string {
	if host == "" {
		return nil
	}
	id := ServiceProviderID(functionName, identity)
	rule := fmt.Sprintf("Host(`%s`)", cfg.OverrideHost(host))
	if path != "" {
		rule = fmt.Sprintf("%s && PathPrefix(`%s`)", rule, path)
	}
	labels := map[string]string{
		"traefik.enable": "true",
		fmt.Sprintf("traefik.http.routers.%s.rule", id):                      rule,
		fmt.Sprintf("traefik.http.services.%s.loadbalancer.server.port", id): strconv.Itoa(port),
	}
	if path != "" {
		middleware := PathMiddlewareID(functionName, identity)
		labels[fmt.Sprintf("traefik.http.routers.%s.middlewares", id)] = middleware
		labels[fmt.Sprintf("traefik.http.middlewares.%s.stripprefix.prefixes", middleware)] = path
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

// PathMiddlewareID derives the deterministic, Traefik-safe StripPrefix
// middleware name for a routed service with a path: the service identity id
// plus the stable "-path" infix and the same collision-resistant hash suffix as
// ServiceProviderID. The infix keeps the middleware name in a distinct
// namespace from the router/service slices on the same service (so adding a
// path never clobbers them) and makes the name self-describing.
//
// The name is capped at 100 characters like ServiceProviderID. The hash suffix
// is preserved by trimming the readable base first, so a long identity never
// absorbs the suffix at the cap and always remains collision-resistant (see
// ServiceProviderID). Only [a-z0-9-] characters result, so the name is safe in
// Traefik's label grammar.
func PathMiddlewareID(functionName, identity string) string {
	return serviceProviderID(functionName, identity, "path")
}

// ServiceProviderID derives the deterministic, Traefik-safe router/service id
// for one service: `relay-<function>-<identity>-<hash>`.
//
// Traefik router/service names appearing in labels must be identifier-safe, but
// the service identity parts are NOT: a function name may contain dots, and an
// identity is a file path ("app/main.py"), Dockerfile path, or image reference
// ("ghcr.io/acme/api:1.2") containing "/", ".", and ":" characters that would
// break Traefik's label grammar. So every character outside [a-z0-9-] is
// sanitized to "-" and consecutive "-" are collapsed, per part. The id must be
// deterministic so reconciliation produces stable labels across passes
// (identical labels keep a container a keep candidate instead of stale).
//
// Sanitizing and capping alone is NOT injective: distinct valid identities can
// collapse to the same readable base, either because differing characters
// sanitize to the same "-" (e.g. "ghcr.io/acme/a/b:1" and "ghcr.io/acme/a-b:1")
// or because a long identity is truncated at the cap. The id therefore ends
// with a fixed-length hex suffix derived from the FULL, unmodified function name
// and identity, which makes distinct identities distinct ids with overwhelming
// probability while leaving the readable prefix intact. The suffix is always
// preserved at the cap: the readable base is trimmed to make room.
func ServiceProviderID(functionName, identity string) string {
	return serviceProviderID(functionName, identity, "")
}

// serviceProviderID builds a Traefik-safe identifier from the readable
// `relay-<function>-<identity>` base plus a collision-resistant hash suffix.
// infix, when non-empty (e.g. "path"), is inserted between the base and the
// hash so the middleware namespace stays distinct from the router/service one
// while both remain collision-safe. The readable base is trimmed to fit the cap
// before the suffix is appended, so the suffix (and therefore collision
// resistance) is never lost to truncation.
func serviceProviderID(functionName, identity, infix string) string {
	base := strings.Trim("relay-"+traefikSafePart(functionName)+"-"+traefikSafePart(identity), "-")
	if base == "" {
		base = "relay"
	}
	suffix := "-" + identityHash(functionName, identity)
	if infix != "" {
		suffix = "-" + infix + suffix
	}
	maxBase := serviceProviderIDMaxLen - len(suffix)
	if maxBase < 0 {
		maxBase = 0
	}
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + suffix
}

// identityHash returns the fixed-length hex collision-resistant suffix for a
// service identity. It hashes the FULL function name and the FULL identity
// (never the sanitized or truncated base) with a NUL separator, so the two
// inputs cannot run together across the join. The separator and the full inputs
// are exactly what makes distinct identities hash apart even when their
// sanitized bases coincide.
func identityHash(functionName, identity string) string {
	sum := sha256.Sum256([]byte(functionName + "\x00" + identity))
	return hex.EncodeToString(sum[:])[:identityHashLen]
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
