package cli

import (
	"bytes"
	"strings"
	"testing"

	"relay/internal/runtime"
)

// TestAppInspectRuntimePoolStartingRow verifies the Starting row appears
// only when an in-flight lazy start is observed.
func TestAppInspectRuntimePoolStartingRow(t *testing.T) {
	var w bytes.Buffer
	printRuntimePool(&w, &poolGauges{Capacity: 2, Starting: 1}, 0, 0, 0)
	out := normWS(w.String())
	if !strings.Contains(out, "Starting: 1") {
		t.Errorf("starting row missing:\n%s", out)
	}
}

// fakePoolSnapshotter is an in-memory worker.PoolSnapshotter for CLI tests, so
// the live query socket can be exercised without Docker or a runtime.Manager.
type fakePoolSnapshotter struct {
	pools map[string]runtime.PoolSnapshot
}

func (f fakePoolSnapshotter) PoolSnapshot(name string) (runtime.PoolSnapshot, bool) {
	s, ok := f.pools[name]
	return s, ok
}
