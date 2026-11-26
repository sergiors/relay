package python

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
)

// otelPythonSpecs mirrors the Python engine's managed install set (floor
// ranges, resolved together so the four packages stay version-coupled). It is
// deliberately kept OUTSIDE the integration build tag: the telemetry
// integration harness uses it for its managed install fallback, and
// TestManagedOtelFloorRangesStayInSync checks it against the production
// engine constants in every plain unit run.
var otelPythonSpecs = []string{
	"opentelemetry-api>=1.16,<2",
	"opentelemetry-sdk>=1.16,<2",
	"opentelemetry-exporter-otlp-proto-grpc>=1.16,<2",
	"opentelemetry-exporter-otlp-proto-http>=1.16,<2",
}

// TestManagedOtelFloorRangesStayInSync pins the three mirrors of the four
// production OTel floor ranges (the engine constants in engine.go): the
// telemetry harness's install list and the CI test image's preinstall must
// each stay exactly the engine's set, in both directions, so a version bump in
// one place cannot silently leave the others behind.
func TestManagedOtelFloorRangesStayInSync(t *testing.T) {
	floors := []string{
		otelAPIFloor,
		otelSDKFloor,
		otelGRPCExporterFloor,
		otelHTTPExporterFloor,
	}

	// The harness install list mirrors the engine floors (same set).
	if len(otelPythonSpecs) != len(floors) {
		t.Fatalf(
			"otelPythonSpecs = %v, want exactly the engine floor ranges %v",
			otelPythonSpecs,
			floors,
		)
	}
	for _, spec := range otelPythonSpecs {
		if !slices.Contains(floors, spec) {
			t.Errorf(
				"otelPythonSpecs entry %q is not one of the engine floor ranges %v",
				spec,
				floors,
			)
		}
	}

	// The CI image preinstall mirrors the engine floors (same set, both
	// directions): every quoted opentelemetry spec in the Dockerfile is a
	// production floor, and every production floor is preinstalled.
	dockerfile := filepath.Join("..", "..", "..", ".ci", "local", "Dockerfile")
	raw, err := os.ReadFile(dockerfile)
	if err != nil {
		t.Fatalf("read the CI test image Dockerfile: %v", err)
	}
	quoted := regexp.MustCompile(`'([^']*opentelemetry[^']*)'`).
		FindAllStringSubmatch(string(raw), -1)
	preinstall := make([]string, 0, len(quoted))
	for _, match := range quoted {
		preinstall = append(preinstall, match[1])
	}
	if len(preinstall) == 0 {
		t.Fatal("no quoted opentelemetry spec found in the CI test image Dockerfile")
	}
	for _, spec := range preinstall {
		if !slices.Contains(floors, spec) {
			t.Errorf(
				"CI Dockerfile spec %q is not one of the engine floor ranges %v",
				spec,
				floors,
			)
		}
	}
	for _, floor := range floors {
		if !slices.Contains(preinstall, floor) {
			t.Errorf(
				"engine floor range %q is missing from the CI Dockerfile preinstall %v",
				floor,
				preinstall,
			)
		}
	}
}
