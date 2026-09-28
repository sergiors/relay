package runtime

import (
	"context"
	"sync"
	"testing"

	"relay/internal/function"
)

// TestExecutePublishesResourcesAndRotatesGeneration proves the runtime half of a
// resource-only hot change: Execute resolves the function's effective limits and
// passes them to the container create, a change to those limits rotates the
// container generation (new create) WITHOUT any image change, and an unchanged
// value does not churn. It drives the real Execute path with the injectable
// container-start seam (no Docker daemon).
func TestExecutePublishesResourcesAndRotatesGeneration(t *testing.T) {
	m := &Manager{maxConcurrency: 4}
	m.containers = newContainerCache()

	var mu sync.Mutex
	var started []*fakeContainer
	var seenLimits []function.ResourceLimits
	var seenImages []string
	m.startContainerFn = func(_ context.Context, _ string, img resolvedImage, _ []string, limits function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		c := &fakeContainer{}
		mu.Lock()
		started = append(started, c)
		seenLimits = append(seenLimits, limits)
		seenImages = append(seenImages, img.createImage())
		mu.Unlock()
		return c, nil
	}

	prepared := &Prepared{Name: "fn", Image: "img-1", Concurrency: 2}
	exec := func() {
		t.Helper()
		if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
			t.Fatalf("execute: %v", err)
		}
	}

	// Publish the default resources and run once: one container, default limits.
	m.SetFunctionResources("fn", function.DefaultResourceLimits())
	exec()
	if len(started) != 1 {
		t.Fatalf("starts = %d, want 1", len(started))
	}
	if seenLimits[0] != function.DefaultResourceLimits() {
		t.Fatalf("create limits = %+v, want defaults", seenLimits[0])
	}

	// A second execute with unchanged resources reuses the warm container.
	exec()
	if len(started) != 1 {
		t.Fatalf("started a new container for unchanged resources: starts = %d", len(started))
	}

	// Change resources only: the next execute must create a new container with
	// the new limits, and the old idle container must be discarded.
	newLimits := function.ResourceLimits{MemoryBytes: 512 << 20, NanoCPUs: 500_000_000, PidsLimit: 64}
	m.SetFunctionResources("fn", newLimits)
	if got := started[0].reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Fatalf("old container discard reasons = %v, want [%s]", got, reasonResourcesChanged)
	}
	exec()
	if len(started) != 2 {
		t.Fatalf("resource change did not create a new container: starts = %d", len(started))
	}
	if seenLimits[1] != newLimits {
		t.Fatalf("create limits after change = %+v, want %+v", seenLimits[1], newLimits)
	}

	// Setting the SAME limits again must not churn.
	m.SetFunctionResources("fn", newLimits)
	exec()
	if len(started) != 2 {
		t.Fatalf("an unchanged SetFunctionResources churned containers: starts = %d", len(started))
	}
}

// TestResourceGenerationDrainsBusyOldContainer proves the busy-drain contract
// for a resource-only change at the cache level: a container created under the
// old config that is BUSY when the config changes is never discarded
// mid-invocation, is moved to a draining generation, and is discarded on
// release; meanwhile a new execute for the same image under the new config
// starts its own container. No image is retired, so the image reference is
// untouched.
func TestResourceGenerationDrainsBusyOldContainer(t *testing.T) {
	cc, ff := newTestCache()
	oldLimits := function.DefaultResourceLimits()
	newLimits := function.ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 64}
	cc.setFunctionResources("fn-a", oldLimits)
	oldConfig := oldLimits.Fingerprint()
	newConfig := newLimits.Fingerprint()
	if oldConfig == newConfig {
		t.Fatal("test configs must differ")
	}

	busy := newBlockingContainer(1)
	busyDone := make(chan error, 1)
	ff.build = func() *fakeContainer { return busy }
	go func() {
		busyDone <- cc.executeVersion(context.Background(), "fn-a", "img-1", oldConfig, 2, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	// Change resources while the old-config container is busy: it must not be
	// discarded yet.
	newIdle := &fakeContainer{}
	ff.build = func() *fakeContainer { return newIdle }
	cc.setFunctionResources("fn-a", newLimits)
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("busy old-config container discarded mid-invocation: %v", got)
	}
	if err := cc.executeVersion(context.Background(), "fn-a", "img-1", newConfig, 2, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute under new config: %v", err)
	}

	// The busy old-config container finishes: it drains (discarded on release)
	// and is not reused.
	close(busy.release)
	if err := <-busyDone; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Fatalf("drained old-config discards = %v, want [%s]", got, reasonResourcesChanged)
	}

	// A stale acquire carrying the OLD config must not re-pool the old config:
	// it is served on a throwaway, and the new-config idle container still wins.
	// Use a distinct throwaway so the pooled new-config container is untouched.
	throwaway := &fakeContainer{}
	ff.build = func() *fakeContainer { return throwaway }
	before := ff.count()
	if err := cc.executeVersion(context.Background(), "fn-a", "img-1", oldConfig, 2, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("stale old-config execute: %v", err)
	}
	if ff.count() != before+1 {
		t.Fatalf("stale old-config acquire must start exactly one throwaway container: creations %d -> %d", before, ff.count())
	}
	if got := throwaway.reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Fatalf("throwaway discard reason = %v, want [%s] (config-only retirement)", got, reasonResourcesChanged)
	}
}

// TestSetFunctionResourcesNoPoolRecordsOnly pins that publishing resources for a
// function with no live pool records them (so the next acquire seeds the pool)
// and does not panic or create state.
func TestSetFunctionResourcesNoPoolRecordsOnly(t *testing.T) {
	cc := newContainerCache()
	limits := function.ResourceLimits{MemoryBytes: 64 << 20, NanoCPUs: 250_000_000, PidsLimit: 32}
	cc.setFunctionResources("fn-a", limits)
	if got := cc.functionResources("fn-a"); got != limits {
		t.Fatalf("recorded resources = %+v, want %+v", got, limits)
	}
	if got := cc.functionConfig("fn-a"); got != limits.Fingerprint() {
		t.Fatalf("config fingerprint = %q, want %q", got, limits.Fingerprint())
	}
	// An unknown function reports the defaults.
	if got := cc.functionResources("unknown"); got != function.DefaultResourceLimits() {
		t.Fatalf("unknown function resources = %+v, want defaults", got)
	}
}
