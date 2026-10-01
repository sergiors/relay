package app

import (
	"fmt"
	"regexp"
)

// maxNameLen caps app-name length. It is deliberately chosen to satisfy a
// Docker tag component limit: a valid image tag must be ≤ 128 chars, and Relay
// builds tags as "relay-app-" + name, so a 63-char name leaves room well inside
// that budget. Directory names this long are pathological anyway, but the cap
// keeps the mapping to docker tags trivially safe.
const maxNameLen = 63

// namePattern restricts the characters an app name may contain. The first
// character must be a letter or digit (so a name cannot start with '.', '-', or
// '_', which would collide with a hidden file or an odd-but-legal docker tag);
// subsequent characters may be letters, digits, '.', '_', or '-'. Uppercase,
// spaces, and path separators are excluded because loaders derive names straight
// from directory names.
var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// secretNamePattern restricts the characters a secret name may contain. It is
// deliberately the same shape as an app name (lowercase letters, digits,
// '.', '_', '-', no leading '.', no trailing '.') so a secret reference is
// always a safe, single path component: it can never contain a path separator,
// an absolute path, or "..", which is what lets the local store resolve it to a
// file under the secrets directory without any escaping risk. The rule is
// duplicated in internal/secrets (which cannot import this leaf package); the
// two are pinned equivalent by a cross-check test in the secrets package.
var secretNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// ValidSecretName reports whether name is a legal secret reference, returning
// nil when it is and a descriptive error otherwise. It rejects empty names,
// names over maxNameLen characters, names containing path separators or "..",
// and names ending in '.'. The value is never included in the error.
func ValidSecretName(name string) error {
	if name == "" {
		return fmt.Errorf("invalid secret name: must not be empty")
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("invalid secret name: must be at most %d characters", maxNameLen)
	}
	if !secretNamePattern.MatchString(name) {
		return fmt.Errorf("invalid secret name %q: must match [a-z0-9][a-z0-9._-]*", name)
	}
	if name[len(name)-1] == '.' {
		return fmt.Errorf("invalid secret name %q: must not end with '.'", name)
	}
	return nil
}

// validateNameRule reports whether name satisfies the conservative resource
// name rule shared by app names and service/schedule names: non-empty, at
// most maxNameLen characters, matching namePattern, and not ending in '.'. It
// returns a rule description (without the offending value) so each caller can
// wrap it with its own kind label. The rule is deliberately identical across
// kinds so a valid app name is always a valid service/schedule name, and
// vice versa.
func validateNameRule(name string) error {
	if name == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("must be at most %d characters", maxNameLen)
	}
	if !namePattern.MatchString(name) {
		return fmt.Errorf("must match [a-z0-9][a-z0-9._-]*")
	}
	// The pattern permits a trailing dot, but docker rejects tags that end in a
	// dot, and a name like "jobs." is a namespace-with-empty-version in disguise.
	// Reject it explicitly so every valid name maps to a legal docker tag.
	if name[len(name)-1] == '.' {
		return fmt.Errorf("must not end with '.'")
	}
	return nil
}

// ValidName reports whether name is a legal app name, returning nil when it
// is and a descriptive error otherwise. Names are validated at load time (never
// sanitized), so the value used for the docker image tag is guaranteed to be
// already safe for that use — this is what lets imageRef stay a simple
// concatenation.
func ValidName(name string) error {
	if err := validateNameRule(name); err != nil {
		return fmt.Errorf("invalid app name %q: %w", name, err)
	}
	return nil
}

// validServiceScheduleName reports whether name is a legal service or schedule
// name. It applies the same conservative rule as an app name (see
// validateNameRule); kind ("service"/"schedule") only shapes the error message.
func validServiceScheduleName(kind, name string) error {
	if err := validateNameRule(name); err != nil {
		return fmt.Errorf("invalid %s name %q: %w", kind, name, err)
	}
	return nil
}
