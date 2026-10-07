package runtime

import (
	"testing"

	"relay/internal/app"
	"relay/internal/observability/metrics"
)

// TestManagerEffectiveConcurrencyClipsToGlobal pins the effective-bound rule:
// a template asking for more than MAX_CONCURRENT_INVOCATIONS is clipped to it (15 -> 8),
// a template below the cap is untouched (4 -> 4), and both the unset-global and
// unset-template fallbacks match the runner's normalization.
func TestManagerEffectiveConcurrencyClipsToGlobal(t *testing.T) {
	clipped := &Manager{maxConcurrentInvocations: 8}

	fn15 := app.App{Name: "f", Template: &app.Template{Concurrency: 15}}
	if got := clipped.effectiveConcurrency(fn15); got != 8 {
		t.Fatalf("effectiveConcurrency(concurrency 15, MAX_CONCURRENT_INVOCATIONS 8) = %d, want 8", got)
	}

	fn4 := app.App{Name: "f", Template: &app.Template{Concurrency: 4}}
	if got := clipped.effectiveConcurrency(fn4); got != 4 {
		t.Fatalf("effectiveConcurrency(concurrency 4, MAX_CONCURRENT_INVOCATIONS 8) = %d, want 4 (below cap)", got)
	}

	// A Manager constructed directly (no WithMaxConcurrentInvocations) behaves like the
	// default runner rather than "uncapped".
	uncapped := &Manager{}
	if got := uncapped.effectiveConcurrency(fn15); got != DefaultMaxConcurrentInvocations {
		t.Fatalf("effectiveConcurrency with no global cap = %d, want default %d", got, DefaultMaxConcurrentInvocations)
	}
	if DefaultMaxConcurrentInvocations != 8 {
		t.Fatalf("DefaultMaxConcurrentInvocations = %d, want 8 (mirrors runner/config)", DefaultMaxConcurrentInvocations)
	}

	// A template that omits concurrency keeps app.DefaultConcurrency.
	fnDefault := app.App{Name: "g", Template: &app.Template{}}
	if got := uncapped.effectiveConcurrency(fnDefault); got != app.DefaultConcurrency {
		t.Fatalf("effectiveConcurrency(default template) = %d, want %d", got, app.DefaultConcurrency)
	}
}

// TestResolveManagerOptionsMaxConcurrentInvocations pins the WithMaxConcurrentInvocations option
// contract: an explicit positive value is honored, and a non-positive or unset
// value falls back to DefaultMaxConcurrentInvocations (never "uncapped"), matching the
// runner's normalization. This is what a direct NewManager caller relies on.
func TestResolveManagerOptionsMaxConcurrentInvocations(t *testing.T) {
	if got := resolveManagerOptions(nil).maxConcurrentInvocations; got != DefaultMaxConcurrentInvocations {
		t.Fatalf("default maxConcurrentInvocations = %d, want %d", got, DefaultMaxConcurrentInvocations)
	}
	if got := resolveManagerOptions([]ManagerOption{WithMaxConcurrentInvocations(4)}).maxConcurrentInvocations; got != 4 {
		t.Fatalf("explicit maxConcurrentInvocations = %d, want 4", got)
	}
	for _, bad := range []int{0, -1} {
		optsBad := []ManagerOption{WithMaxConcurrentInvocations(bad)}
		if got := resolveManagerOptions(optsBad).maxConcurrentInvocations; got != DefaultMaxConcurrentInvocations {
			t.Errorf("non-positive maxConcurrentInvocations %d resolved to %d, want default %d", bad, got, DefaultMaxConcurrentInvocations)
		}
	}
}

// TestResolveManagerOptionsMaxConcurrentBuilds pins the WithMaxConcurrentBuilds
// option contract: an explicit positive value is honored, and a non-positive or
// unset value falls back to DefaultMaxConcurrentBuilds (never "unbounded"), so a
// direct NewManager caller is always bounded.
func TestResolveManagerOptionsMaxConcurrentBuilds(t *testing.T) {
	if got := resolveManagerOptions(nil).maxConcurrentBuilds; got != DefaultMaxConcurrentBuilds {
		t.Fatalf("default maxConcurrentBuilds = %d, want %d", got, DefaultMaxConcurrentBuilds)
	}
	if got := resolveManagerOptions([]ManagerOption{WithMaxConcurrentBuilds(4)}).maxConcurrentBuilds; got != 4 {
		t.Fatalf("explicit maxConcurrentBuilds = %d, want 4", got)
	}
	for _, bad := range []int{0, -1} {
		optsBad := []ManagerOption{WithMaxConcurrentBuilds(bad)}
		if got := resolveManagerOptions(optsBad).maxConcurrentBuilds; got != DefaultMaxConcurrentBuilds {
			t.Errorf("non-positive maxConcurrentBuilds %d resolved to %d, want default %d", bad, got, DefaultMaxConcurrentBuilds)
		}
	}
	if DefaultMaxConcurrentBuilds != 2 {
		t.Fatalf("DefaultMaxConcurrentBuilds = %d, want 2 (mirrors config)", DefaultMaxConcurrentBuilds)
	}
}

// TestResolveManagerOptionsNetworks pins the WithNetworks option contract: an
// omitted option resolves to no networks (nil), and an explicit list is copied
// so a later mutation of the caller's slice cannot change the manager's
// configuration.
func TestResolveManagerOptionsNetworks(t *testing.T) {
	if got := resolveManagerOptions(nil).networks; got != nil {
		t.Fatalf("default networks = %v, want nil", got)
	}

	configured := []string{"backend", "frontend"}
	resolved := resolveManagerOptions([]ManagerOption{WithNetworks(configured)})
	if len(resolved.networks) != 2 || resolved.networks[0] != "backend" || resolved.networks[1] != "frontend" {
		t.Fatalf("networks = %v, want [backend frontend]", resolved.networks)
	}
	configured[0] = "mutated"
	if resolved.networks[0] != "backend" {
		t.Fatalf("WithNetworks must copy its argument; got %q after caller mutation", resolved.networks[0])
	}
}

// TestManagerClampedConcurrencyDrivesPoolAndSnapshot proves the capped effective
// bound is the one that actually drives the warm pool: an app with template
// concurrency 15 under MAX_CONCURRENT_INVOCATIONS 8 warms a pool of capacity 8 (gauge,
// snapshot, and acquisition), and a later reconcile to template concurrency 4
// shrinks it to 4. This is the runtime half of "app concurrency 15 with
// MAX_CONCURRENT_INVOCATIONS=8 => effective live pool capacity 8", with the same rule
// applied to the runner's per-app semaphore.
func TestManagerClampedConcurrencyDrivesPoolAndSnapshot(t *testing.T) {
	reg := metrics.New()
	m := &Manager{maxConcurrentInvocations: 8, metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}

	fn15 := app.App{Name: "fn-cap", Template: &app.Template{Concurrency: 15}}
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
	fn4 := app.App{Name: "fn-cap", Template: &app.Template{Concurrency: 4}}
	m.containers.setAppConcurrency("fn-cap", m.effectiveConcurrency(fn4))
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
// runner would admit. A zero/negative prepared value keeps the app default.
func TestManagerClipConcurrencyHandBuiltPrepared(t *testing.T) {
	m := &Manager{maxConcurrentInvocations: 8}
	if got := m.clipConcurrency(15); got != 8 {
		t.Fatalf("clipConcurrency(15) = %d, want 8", got)
	}
	if got := m.clipConcurrency(4); got != 4 {
		t.Fatalf("clipConcurrency(4) = %d, want 4", got)
	}
	if got := m.clipConcurrency(0); got != app.DefaultConcurrency {
		t.Fatalf("clipConcurrency(0) = %d, want %d", got, app.DefaultConcurrency)
	}
	// Direct construction (zero maxConcurrentInvocations) clips to the default global cap.
	var direct Manager
	if got := direct.clipConcurrency(15); got != DefaultMaxConcurrentInvocations {
		t.Fatalf("clipConcurrency(15) on a direct Manager = %d, want %d", got, DefaultMaxConcurrentInvocations)
	}
}
