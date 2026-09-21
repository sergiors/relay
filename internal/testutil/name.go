package testutil

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// nameMaxLen is the length cap Unique targets by default. It mirrors
// function.ValidName's maxNameLen (internal/function/name.go): Relay function
// names must be at most this many characters to remain legal docker tag
// components. It is kept as a local constant so testutil does not take a
// dependency on internal/function; internal/testutil's name_test.go pins the
// two caps together by validating Unique's output with function.ValidName.
const nameMaxLen = 63

// sanitizeNameComponent lowercases s and replaces every character outside
// [a-z0-9._-] with '-', yielding a component safe for a Relay function name,
// docker image tag, or container-name component. It consolidates the
// per-package sanitizeTestName/sanitizeName helpers the integration tests used
// to carry (slash and space -> dash, uppercase lowered).
func sanitizeNameComponent(s string) string {
	b := []byte(strings.ToLower(s))
	for i, c := range b {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			b[i] = '-'
		}
	}
	return string(b)
}

// Unique returns a per-run-unique name built from prefix and the sanitized
// rawName (typically testing.T.Name). The result is at most nameMaxLen
// characters and matches Relay's function-name pattern [a-z0-9][a-z0-9._-]*
// with no trailing '.', so it is safe as a function directory name, function
// name, docker image-repo component, or container-name component.
//
// The form is "<prefix>-<truncated rawName>-<decimal nanosecond stamp>". The
// rawName portion is truncated to whatever fits so the whole name stays within
// nameMaxLen; the stamp supplies uniqueness, so concurrent runs against one
// daemon cannot collide. prefix must itself be a short, lowercase,
// [a-z0-9._-]-safe component starting with a letter or digit.
func Unique(prefix, rawName string) string {
	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	// Reserve both separators and the full stamp; whatever remains bounds the
	// truncated test-name portion.
	budget := nameMaxLen - len(prefix) - len(stamp) - 2
	if budget < 0 {
		budget = 0
	}
	name := sanitizeNameComponent(rawName)
	if len(name) > budget {
		name = name[:budget]
	}
	// Truncation can land on a separator. The stamp already guarantees the name
	// ends in a digit, but trimming keeps the result tidy and honors the
	// "no trailing separator" contract.
	name = strings.TrimRight(name, "-.")
	if name == "" {
		return prefix + "-" + stamp
	}
	return prefix + "-" + name + "-" + stamp
}

// UniqueName is Unique(prefix, t.Name()): a per-test-unique name bounded by the
// Relay function-name cap. Callers pass a short semantic prefix such as
// "shutdown", "svc-rec", or "recon".
func UniqueName(t *testing.T, prefix string) string {
	t.Helper()
	return Unique(prefix, t.Name())
}
