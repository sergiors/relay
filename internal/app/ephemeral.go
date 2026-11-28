package app

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

// serviceEphemeralRoots is the narrow allowlist of container directories an
// additional ephemeral volume may cover. They are cache/run locations that a
// long-running service commonly needs to write. Everything else — especially
// the root, /tmp, /etc, /proc, /sys, /dev, and /var/run — is rejected rather
// than pattern-matched. /tmp is always provided by Relay and cannot be
// redeclared.
var serviceEphemeralRoots = []string{"/run", "/var/cache"}

// resolveServiceEphemeral validates a service's optional `ephemeral` list and
// returns its canonical form (sorted, so the generated configuration label and
// mount set are deterministic regardless of YAML ordering). An empty list yields
// nil. All validation is delegated to ValidateServiceEphemeral so the parse
// boundary and the runtime create boundary share one policy.
func resolveServiceEphemeral(destinations []string) ([]string, error) {
	if len(destinations) == 0 {
		return nil, nil
	}
	if err := ValidateServiceEphemeral(destinations); err != nil {
		return nil, err
	}
	out := append([]string(nil), destinations...)
	sort.Strings(out)
	return out, nil
}

// ValidateServiceEphemeral validates all additional ephemeral-volume
// destinations a service declares against the container path policy. It is the
// single policy implementation shared by template parsing and the runtime create
// path, so a direct ServiceSpec caller cannot bypass the parse-time rules.
//
// Every destination must be an absolute, canonical path under an allowed
// ephemeral root (see serviceEphemeralRoots). Duplicate or overlapping
// destinations are rejected, as is /tmp, which Relay always mounts itself.
func ValidateServiceEphemeral(destinations []string) error {
	for i, destination := range destinations {
		if err := ValidateServiceEphemeralDestination(destination); err != nil {
			return err
		}
		for _, previous := range destinations[:i] {
			if ephemeralPathsOverlap(previous, destination) {
				return fmt.Errorf(
					"ephemeral destinations %q and %q overlap",
					previous, destination)
			}
		}
	}
	return nil
}

// ValidateServiceEphemeralDestination validates one additional ephemeral
// destination. The path must be absolute and already canonical (path.Clean would
// not change it), must not contain whitespace, a backslash, or a comma, must not
// overlap Relay's always-present /tmp volume, and must lie at or below one of the
// allowed ephemeral roots.
func ValidateServiceEphemeralDestination(destination string) error {
	switch {
	case destination == "":
		return fmt.Errorf("ephemeral destination is empty")
	case strings.ContainsAny(destination, " \t\r\n"):
		return fmt.Errorf("ephemeral destination %q contains whitespace", destination)
	case strings.Contains(destination, `\`):
		return fmt.Errorf("ephemeral destination %q contains a backslash", destination)
	case strings.Contains(destination, ","):
		return fmt.Errorf("ephemeral destination %q must not contain a comma", destination)
	case !strings.HasPrefix(destination, "/"):
		return fmt.Errorf("ephemeral destination %q must be an absolute path", destination)
	case path.Clean(destination) != destination:
		return fmt.Errorf(
			"ephemeral destination %q must be canonical "+
				"(no trailing slash, empty segment, or dot segment)",
			destination)
	case ephemeralPathsOverlap(destination, "/tmp"):
		return fmt.Errorf(
			"ephemeral destination %q conflicts with Relay's /tmp mount",
			destination)
	case !underServiceEphemeralRoot(destination):
		return fmt.Errorf(
			"ephemeral destination %q is not allowed (allowed: /run, /var/cache, "+
				"and their descendants)",
			destination)
	}
	return nil
}

// underServiceEphemeralRoot reports whether destination is one of the allowed
// ephemeral roots or a descendant of one.
func underServiceEphemeralRoot(destination string) bool {
	for _, root := range serviceEphemeralRoots {
		if destination == root || strings.HasPrefix(destination, root+"/") {
			return true
		}
	}
	return false
}

// ephemeralPathsOverlap reports whether two canonical container paths are equal
// or one is an ancestor of the other.
func ephemeralPathsOverlap(a, b string) bool {
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
