package python

import (
	"strings"
	"testing"
)

// TestBootstrapIsEmbeddedAndAddsAppToSysPath asserts the two things static
// source inspection is actually needed for. The integration process tests
// cover the rest of the invocation protocol (sentinel framing, module import,
// async handlers, env rotation, error handling), so they are no longer
// duplicated as brittle substring checks. The sys.path assertion is kept because
// it pins the specific import contract: app sources are importable by
// module name (the package-relative service entrypoint depends on it).
func TestBootstrapIsEmbeddedAndAddsAppToSysPath(t *testing.T) {
	if len(Bootstrap) == 0 {
		t.Fatal("embedded python bootstrap is empty")
	}
	if !strings.Contains(string(Bootstrap), `sys.path.insert(0, "/app")`) {
		t.Error(`python bootstrap must put function sources on sys.path via sys.path.insert(0, "/app")`)
	}
}
