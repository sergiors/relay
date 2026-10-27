package worker

import (
	"context"
	"testing"

	"relay/internal/app"
)

// TestPrepareAppsWithoutStateSinkNeedsNoToken preserves the historical test/
// standalone shape: without a state sink there is no persistence to fence, so a
// nil token map is not a reason to suppress preparation.
func TestPrepareAppsWithoutStateSinkNeedsNoToken(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "solo", "def handler(e): return 1\n")
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())

	spy := &startupSpyPreparer{}
	prepared := prepareApps(context.Background(), spy, startup, nil, nil, discardLogger())
	if spy.calls != 1 || len(prepared) != 1 {
		t.Fatalf("calls/prepared = %d/%d, want 1/1 without a state sink", spy.calls, len(prepared))
	}
}
