package python

import (
	"fmt"
	"strings"
)

// ServiceCommand returns the command used to run a Python service entrypoint.
// Services run as modules so package-relative imports work correctly.
func ServiceCommand(entrypoint string) ([]string, error) {
	module, ok := strings.CutSuffix(entrypoint, ".py")
	if !ok {
		return nil, fmt.Errorf(
			"service entrypoint %q: python services require a .py module file",
			entrypoint,
		)
	}

	parts := strings.Split(module, "/")

	for _, part := range parts {
		if !validIdentifier(part) {
			return nil, fmt.Errorf(
				"service entrypoint %q: %q is not a valid Python module name",
				entrypoint,
				part,
			)
		}
	}

	return []string{"python", "-m", strings.Join(parts, ".")}, nil
}

func validIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c == '_' ||
			('a' <= c && c <= 'z') ||
			('A' <= c && c <= 'Z') ||
			(i > 0 && '0' <= c && c <= '9') {
			continue
		}

		return false
	}

	return true
}
