package state

import (
	"testing"
	"time"
)

// TestFunctionSnapshotResourcesEffective pins that the persisted snapshot
// carries the function's EFFECTIVE per-container resource configuration (with
// defaults resolved) and round-trips through JSONB, so `relay function inspect`
// can render it from an offline state read.
func TestFunctionSnapshotResourcesEffective(t *testing.T) {
	c := openTestState(t)

	// Configured resources.
	tmpl := mustTemplate(t, `runtime: node24
resources:
  memory: 512MiB
  cpus: 2.5
  pids: 96
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	c.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := c.GetFunction("demo")
	if !ok {
		t.Fatal("expected demo")
	}
	if detail.Resources == nil {
		t.Fatal("resources must be persisted")
	}
	if detail.Resources.MemoryBytes != 512<<20 || detail.Resources.CPUs != 2.5 || detail.Resources.Pids != 96 {
		t.Fatalf("resources = %+v, want 512MiB/2.5CPU/96", *detail.Resources)
	}

	// Omitted resources persist the effective defaults.
	c.RecordReconcileSuccess("defaulted", "img", "fp", time.Now(), fnFor(t, "defaulted", mustTemplate(t, `runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)))
	d2, ok := c.GetFunction("defaulted")
	if !ok {
		t.Fatal("expected defaulted")
	}
	if d2.Resources == nil || d2.Resources.MemoryBytes != 128<<20 || d2.Resources.CPUs != 1 || d2.Resources.Pids != 128 {
		t.Fatalf("default resources = %+v, want 128MiB/1CPU/128", d2.Resources)
	}
}
