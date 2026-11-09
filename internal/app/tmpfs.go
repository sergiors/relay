package app

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

// ServiceTmpfsMountBytes is the fixed size of each additional service tmpfs
// mount. A fixed per-mount size keeps the generated option string deterministic
// and makes the aggregate budget simple to reason about: a service may declare
// at most effective-memory/4 worth of tmpfs upper bounds.
const ServiceTmpfsMountBytes int64 = 16 << 20

// serviceTmpfsMemoryDivisor bounds the aggregate size of a service's additional
// tmpfs mounts to a fraction of the container's effective memory limit. tmpfs
// pages count against the cgroup memory limit, so the upper bounds must fit
// comfortably inside it.
const serviceTmpfsMemoryDivisor int64 = 4

// serviceTmpfsRoots is the narrow allowlist of container directories an
// additional service tmpfs may cover. They are ephemeral cache/run locations
// that a long-running service commonly needs to write. Everything else —
// especially the root, /tmp, /etc, /proc, /sys, /dev, and /var/run — is
// rejected rather than pattern-matched.
var serviceTmpfsRoots = []string{"/run", "/var/cache"}

// resolveServiceTmpfs validates a service's optional `tmpfs` list and returns
// its canonical form (sorted, so the generated configuration label and mount
// set are deterministic regardless of YAML ordering). An empty list yields nil.
//
// memoryBytes is the app's EFFECTIVE per-container memory limit; it bounds the
// aggregate tmpfs size. All validation is delegated to ValidateServiceTmpfs so
// the parse boundary and the runtime create boundary share one policy.
func resolveServiceTmpfs(destinations []string, memoryBytes int64) ([]string, error) {
	if len(destinations) == 0 {
		return nil, nil
	}
	if err := ValidateServiceTmpfs(destinations, memoryBytes); err != nil {
		return nil, err
	}
	out := append([]string(nil), destinations...)
	sort.Strings(out)
	return out, nil
}

// ValidateServiceTmpfs validates all additional tmpfs destinations a service
// declares against the container path policy and the per-container memory
// budget. It is the single policy implementation shared by template parsing and
// the runtime create path, so a direct ServiceSpec caller cannot bypass the
// parse-time rules.
//
// Every destination must be an absolute, canonical path under an allowed
// ephemeral root (see serviceTmpfsRoots). Duplicate or overlapping
// destinations are rejected, as is any overlap with Relay's own /tmp tmpfs. The
// aggregate upper bound (one fixed ServiceTmpfsMountBytes per mount) must not
// exceed memoryBytes/serviceTmpfsMemoryDivisor.
//
// memoryBytes is the EFFECTIVE per-container memory limit. A non-positive value
// is treated as the default, matching ResourceLimits.OrDefault.
func ValidateServiceTmpfs(destinations []string, memoryBytes int64) error {
	if memoryBytes <= 0 {
		memoryBytes = DefaultResourceMemoryBytes
	}
	for i, destination := range destinations {
		if err := ValidateServiceTmpfsDestination(destination); err != nil {
			return err
		}
		for _, previous := range destinations[:i] {
			if tmpfsPathsOverlap(previous, destination) {
				return fmt.Errorf(
					"tmpfs destinations %q and %q overlap",
					previous, destination)
			}
		}
	}
	aggregate := int64(len(destinations)) * ServiceTmpfsMountBytes
	budget := memoryBytes / serviceTmpfsMemoryDivisor
	if aggregate > budget {
		return fmt.Errorf(
			"%d tmpfs mounts of %d MiB exceed the %d MiB aggregate budget "+
				"for a %d MiB container",
			len(destinations),
			ServiceTmpfsMountBytes>>20,
			budget>>20,
			memoryBytes>>20,
		)
	}
	return nil
}

// ValidateServiceTmpfsDestination validates one additional tmpfs destination.
// The path must be absolute and already canonical (path.Clean would not change
// it), must not contain whitespace or a backslash, and must lie at or below one
// of the allowed ephemeral roots. Relay's own /tmp tmpfs is never a valid
// additional destination because it is always present exactly as configured.
func ValidateServiceTmpfsDestination(destination string) error {
	switch {
	case destination == "":
		return fmt.Errorf("tmpfs destination is empty")
	case strings.ContainsAny(destination, " \t\r\n"):
		return fmt.Errorf("tmpfs destination %q contains whitespace", destination)
	case strings.Contains(destination, `\`):
		return fmt.Errorf("tmpfs destination %q contains a backslash", destination)
	case strings.Contains(destination, ","):
		return fmt.Errorf("tmpfs destination %q must not contain a comma", destination)
	case !strings.HasPrefix(destination, "/"):
		return fmt.Errorf("tmpfs destination %q must be an absolute path", destination)
	case path.Clean(destination) != destination:
		return fmt.Errorf(
			"tmpfs destination %q must be canonical "+
				"(no trailing slash, empty segment, or dot segment)",
			destination)
	case tmpfsPathsOverlap(destination, "/tmp"):
		return fmt.Errorf(
			"tmpfs destination %q conflicts with Relay's /tmp mount",
			destination)
	case !underServiceTmpfsRoot(destination):
		return fmt.Errorf(
			"tmpfs destination %q is not allowed (allowed: /run, /var/cache, "+
				"and their descendants)",
			destination)
	}
	return nil
}

// underServiceTmpfsRoot reports whether destination is one of the allowed
// ephemeral roots or a descendant of one.
func underServiceTmpfsRoot(destination string) bool {
	for _, root := range serviceTmpfsRoots {
		if destination == root || strings.HasPrefix(destination, root+"/") {
			return true
		}
	}
	return false
}

// tmpfsPathsOverlap reports whether two canonical container paths are equal or
// one is an ancestor of the other.
func tmpfsPathsOverlap(a, b string) bool {
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
