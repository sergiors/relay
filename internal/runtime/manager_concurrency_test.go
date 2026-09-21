package runtime

import (
	"testing"

	"relay/internal/function"
	"relay/internal/metrics"
)

// TestManagerEffectiveConcurrencyClipsToGlobal pins the effective-bound rule:
// a template asking for more than MAX_CONCURRENCY is clipped to it (15 -> 8),
// a template below the cap is untouched (4 -> 4), and both the unset-global and
// unset-template fallbacks match the runner's normalization.
func TestManagerEffectiveConcurrencyClipsToGlobal(t *testing.T) {
	clipped := &Manager{maxConcurrency: 8}

	fn15 := function.Function{Name: "f", Template: &function.Template{Concurrency: 15}}
	if got := clipped.effectiveConcurrency(fn15); got != 8 {
		t.Fatalf("effectiveConcurrency(concurrency 15, MAX_CONCURRENCY 8) = %d, want 8", got)
	}

	fn4 := function.Function{Name: "f", Template: &function.Template{Concurrency: 4}}
	if got := clipped.effectiveConcurrency(fn4); got != 4 {
		t.Fatalf("effectiveConcurrency(concurrency 4, MAX_CONCURRENCY 8) = %d, want 4 (below cap)", got)
	}

	// A Manager constructed directly (no WithMaxConcurrency) behaves like the
	// default runner rather than "uncapped".
	uncapped := &Manager{}
	if got := uncapped.effectiveConcurrency(fn15); got != DefaultMaxConcurrency {
		t.Fatalf("effectiveConcurrency with no global cap = %d, want default %d", got, DefaultMaxConcurrency)
	}
	if DefaultMaxConcurrency != 8 {
		t.Fatalf("DefaultMaxConcurrency = %d, want 8 (mirrors runner/config)", DefaultMaxConcurrency)
	}

	// A template that omits concurrency keeps function.DefaultConcurrency.
	fnDefault := function.Function{Name: "g", Template: &function.Template{}}
	if got := uncapped.effectiveConcurrency(fnDefault); got != function.DefaultConcurrency {
		t.Fatalf("effectiveConcurrency(default template) = %d, want %d", got, function.DefaultConcurrency)
	}
}

// TestResolveManagerOptionsMaxConcurrency pins the WithMaxConcurrency option
// contract: an explicit positive value is honored, and a non-positive or unset
// value falls back to DefaultMaxConcurrency (never "uncapped"), matching the
// runner's normalization. This is what a direct NewManager caller relies on.
func TestResolveManagerOptionsMaxConcurrency(t *testing.T) {
	if got := resolveManagerOptions(nil).maxConcurrency; got != DefaultMaxConcurrency {
		t.Fatalf("default maxConcurrency = %d, want %d", got, DefaultMaxConcurrency)
	}
	if got := resolveManagerOptions([]ManagerOption{WithMaxConcurrency(4)}).maxConcurrency; got != 4 {
		t.Fatalf("explicit maxConcurrency = %d, want 4", got)
	}
	for _, bad := range []int{0, -1} {
		optsBad := []ManagerOption{WithMaxConcurrency(bad)}
		if got := resolveManagerOptions(optsBad).maxConcurrency; got != DefaultMaxConcurrency {
			t.Errorf("non-positive maxConcurrency %d resolved to %d, want default %d", bad, got, DefaultMaxConcurrency)
		}
	}
}

// TestManagerClampedConcurrencyDrivesPoolAndSnapshot proves the capped effective
// bound is the one that actually drives the warm pool: a function with template
// concurrency 15 under MAX_CONCURRENCY 8 warms a pool of capacity 8 (gauge,
// snapshot, and acquisition), and a later reconcile to template concurrency 4
// shrinks it to 4. This is the runtime half of "function concurrency 15 with
// MAX_CONCURRENCY=8 => effective live pool capacity 8", with the same rule
// applied to the runner's per-function semaphore.
func TestManagerClampedConcurrencyDrivesPoolAndSnapshot(t *testing.T) {
	reg := metrics.New()
	m := &Manager{maxConcurrency: 8, metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}

	fn15 := function.Function{Name: "fn-cap", Template: &function.Template{Concurrency: 15}}
	max := m.effectiveConcurrency(fn15)
	if max != 8 {
		t.Fatalf("effective max = %d, want 8", max)
	}
	if err := runInvoke(t, m.containers, ff, "fn-cap", "img-1", max, "h"); err != nil {
		t.Fatalf("execute at capped bound: %v", err)
	}

	if got := poolMax(m.containers, "fn-cap"); got != 8 {
		t.Fatalf("pool max = %d, want 8 (clipped)", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-cap")); got != 8 {
		t.Fatalf("capacity gauge = %d, want 8 (clipped)", got)
	}
	if s, ok := m.PoolSnapshot("fn-cap"); !ok || s.Capacity != 8 {
		t.Fatalf("snapshot = %+v, ok=%v; want capacity 8", s, ok)
	}

	// A later reconcile to template concurrency 4 (still below the cap) resizes
	// the live pool to exactly 4.
	fn4 := function.Function{Name: "fn-cap", Template: &function.Template{Concurrency: 4}}
	m.containers.setFunctionConcurrency("fn-cap", m.effectiveConcurrency(fn4))
	if got := poolMax(m.containers, "fn-cap"); got != 4 {
		t.Fatalf("pool max after later reconcile = %d, want 4", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-cap")); got != 4 {
		t.Fatalf("capacity gauge after later reconcile = %d, want 4", got)
	}
	if s, ok := m.PoolSnapshot("fn-cap"); !ok || s.Capacity != 4 {
		t.Fatalf("snapshot after later reconcile = %+v, ok=%v; want capacity 4", s, ok)
	}
}

// TestManagerClipConcurrencyHandBuiltPrepared pins the Execute path's guard: a
// hand-built Prepared carrying the raw template value (15) is clipped to the
// worker-global cap (8), so a direct caller cannot warm a pool larger than the
// runner would admit. A zero/negative prepared value keeps the function default.
func TestManagerClipConcurrencyHandBuiltPrepared(t *testing.T) {
	m := &Manager{maxConcurrency: 8}
	if got := m.clipConcurrency(15); got != 8 {
		t.Fatalf("clipConcurrency(15) = %d, want 8", got)
	}
	if got := m.clipConcurrency(4); got != 4 {
		t.Fatalf("clipConcurrency(4) = %d, want 4", got)
	}
	if got := m.clipConcurrency(0); got != function.DefaultConcurrency {
		t.Fatalf("clipConcurrency(0) = %d, want %d", got, function.DefaultConcurrency)
	}
	// Direct construction (zero maxConcurrency) clips to the default global cap.
	var direct Manager
	if got := direct.clipConcurrency(15); got != DefaultMaxConcurrency {
		t.Fatalf("clipConcurrency(15) on a direct Manager = %d, want %d", got, DefaultMaxConcurrency)
	}
}
