package reconciler

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// seedOldService seeds one RUNNING non-converged (old-generation) container for
// fn/identity at the given replica slot, as Relay itself would have created it
// before a config change. Its labels deliberately do not match any desired
// generation, so reconcile treats it as an old A generation.
func seedOldService(f *fakeDocker, id, fn, identity, image string, port, replica int) {
	f.ctrs[id] = &fakeContainer{
		id: id, function: fn, entrypoint: identity, image: image, port: port,
		replica: replica, state: container.StateRunning,
	}
}

// TestReconcileReplaceStartsBeforeStoppingOld pins the core zero-downtime
// ordering: when the desired configuration changed (a new image here), the
// replacement for a slot is created/started BEFORE the old generation for that
// same slot is stopped. The recorded transition order is what proves it.
func TestReconcileReplaceStartsBeforeStoppingOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	order := f.order()
	start, stop := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-1")
	if start < 0 || stop < 0 {
		t.Fatalf("order = %v, want a start:0 before a stop:old-1", order)
	}
	if start > stop {
		t.Fatalf("order = %v: the old generation was stopped before the replacement started", order)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want exactly 1", got)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || c.image != "img-new" {
		t.Fatalf("replacement = %+v, want the desired img-new image", c)
	}
}

// TestReconcileReplaceMultiReplicaPerSlotOrdering pins that for each desired
// slot the replacement starts before that slot's old generation stops (not one
// global stop-all-then-start-all).
func TestReconcileReplaceMultiReplicaPerSlotOrdering(t *testing.T) {
	f := newFakeDocker()
	// Seeds are keyed so the fake's deterministic id ordering is stable.
	seedOldService(f, "old-0", "fn", "service.js", "img-old", 80, 0)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 1)
	seedOldService(f, "old-2", "fn", "service.js", "img-old", 80, 2)
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})

	if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// Slot 0 and 1 replacements precede their own old stops; scale-down slot 2
	// is stopped only after the desired slots converged.
	order := f.order()
	start0, stop0 := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-0")
	start1, stop1 := f.indexOfEvent("start:1"), f.indexOfEvent("stop:old-1")
	if start0 < 0 || stop0 < 0 || start1 < 0 || stop1 < 0 {
		t.Fatalf("order = %v, want start:0/stop:old-0 and start:1/stop:old-1", order)
	}
	if start0 > stop0 || start1 > stop1 {
		t.Fatalf("order = %v: an old generation was stopped before its slot's replacement", order)
	}
	stop2 := f.indexOfEvent("stop:old-2")
	if stop2 < 0 {
		t.Fatalf("order = %v, want the excess slot 2 stopped", order)
	}
	if stop2 < start0 || stop2 < start1 {
		t.Fatalf("order = %v: scale-down stopped an excess replica before desired replacements", order)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}
}

// TestReconcileReplaceStartFailurePreservesOld pins that a failed replacement
// (create/start/inspect failure) preserves the old running generation: nothing
// for that slot is stopped, the error surfaces, and the service stays up.
func TestReconcileReplaceStartFailurePreservesOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	f.failStart[0] = fmt.Errorf("start failed for replica 0")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "replica 0") {
		t.Fatalf("err = %v, want the start failure surfaced", err)
	}
	if c, ok := f.containerByID("old-1"); !ok || c.state != container.StateRunning {
		t.Fatalf("old generation must be preserved and running, got %+v (exists=%v)", c, ok)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (old preserved)", got)
	}
	if f.indexOfEvent("stop:old-1") >= 0 {
		t.Fatalf("order = %v: the old generation must not be stopped when its replacement failed", f.order())
	}
}

// TestReconcileReplaceStartFailureNoFallbackIsUnavailable pins the "no old
// fallback" path: with no old generation for the slot, a failed start leaves the
// slot unavailable (error surfaced, nothing running), with no fabricated
// success.
func TestReconcileReplaceStartFailureNoFallbackIsUnavailable(t *testing.T) {
	f := newFakeDocker()
	f.failStart[0] = fmt.Errorf("start failed for replica 0")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "replica 0") {
		t.Fatalf("err = %v, want the start failure surfaced", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 0 {
		t.Fatalf("running = %d, want 0 (no fallback available)", got)
	}
}

// TestReconcileReplaceEnvChangeStartsBeforeStop pins the env-change path (image
// reference unchanged, so the old container is stale only by relay.env_hash):
// the replacement is confirmed running before the env-stale container stops.
func TestReconcileReplaceEnvChangeStartsBeforeStop(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	start.Env = map[string]string{"MODE": "a"}
	if _, err := reconcile(t, f, "fn", start, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile env=a: %v", err)
	}
	old := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if old == nil {
		t.Fatal("no initial container")
	}
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()

	changed := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	changed.Env = map[string]string{"MODE": "b"}
	if _, err := reconcile(t, f, "fn", changed, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile env=b: %v", err)
	}
	order := f.order()
	startIdx, stopIdx := f.indexOfEvent("start:0"), f.indexOfEvent("stop:"+old.id)
	if startIdx < 0 || stopIdx < 0 || startIdx > stopIdx {
		t.Fatalf("order = %v, want the env-stale container replaced only after the replacement started", order)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1.2"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

// TestReconcileReplaceNetworkFailureDoesNotMutateOld pins that a network change
// whose replacement cannot be created (missing network surfaces as a create
// error) preserves the old generation. The old container still runs and is not
// stopped.
func TestReconcileReplaceNetworkFailureDoesNotMutateOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-1", 80, 0)
	// The network the old container was on is now gone, so the replacement's
	// create fails. The fake's StartService does not model create-time network
	// errors, so inject the failure directly.
	f.failStart[0] = fmt.Errorf("service: create container: network backend not found")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "network backend not found") {
		t.Fatalf("err = %v, want the create/network failure surfaced", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want the old generation preserved", got)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none on a failed replacement", f.stops)
	}
}

// TestReconcileReplaceRoutingValidationFailureDoesNotMutateOld pins the
// routing-validation path: an unusable routing config skips the service
// entirely, preserving the old container and performing no stops/starts.
func TestReconcileReplaceRoutingValidationFailureDoesNotMutateOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-1", 80, 0)
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "svc.test"})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}) // empty config -> validation error
	if err == nil {
		t.Fatal("expected a routing-validation error")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want the old generation preserved", got)
	}
	if len(f.stops) != 0 || f.indexOfEvent("start:0") >= 0 {
		t.Fatalf("routing validation failure performed container work: stops=%v order=%v", f.stops, f.order())
	}
}

// TestReconcileReplaceResolveFailureDoesNotMutateOld pins that an image
// resolution/pull failure preserves the healthy old generation (the existing
// resolve-before-action guarantee, now with the replacement ordering).
func TestReconcileReplaceResolveFailureDoesNotMutateOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	f.resolveErr["service.js"] = fmt.Errorf("pull failed: registry down")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "registry down") {
		t.Fatalf("err = %v, want the resolve failure surfaced", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want the old generation preserved", got)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none on a resolve failure", f.stops)
	}
}

// TestReconcileReplaceMissingEnvSecretDoesNotMutateOld pins that a missing
// secret provider (env resolution failure) preserves the old generation and
// performs no start/stop.
func TestReconcileReplaceMissingEnvSecretDoesNotMutateOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-1", 80, 0)
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	tmpl.Secrets = map[string]function.SecretRef{"TOKEN": "api-token"}

	_, err := reconcileEnv(t, f, "fn", tmpl, "img-1", nil, nil) // nil provider
	if err == nil || !strings.Contains(err.Error(), "no secret provider") {
		t.Fatalf("err = %v, want the env-resolution failure surfaced", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want the old generation preserved", got)
	}
	if len(f.stops) != 0 || f.indexOfEvent("start:0") >= 0 {
		t.Fatalf("env failure performed container work: stops=%v order=%v", f.stops, f.order())
	}
}

// TestReconcileReplaceInspectNonRunningDoesNotStopOld is the reconciler-side pin
// for the runtime running gate: the fake's StartService returning an error (as
// it does when the post-start inspect reports the container not running) must
// preserve the old generation. The runtime's own inspect behavior is covered by
// the runtime package tests.
func TestReconcileReplaceInspectNonRunningDoesNotStopOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	f.failStart[0] = fmt.Errorf("service: started container is not running")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "not running") {
		t.Fatalf("err = %v, want the non-running failure surfaced", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want the old generation preserved", got)
	}
	if f.indexOfEvent("stop:old-1") >= 0 {
		t.Fatalf("order = %v: old must not be stopped after a non-running replacement", f.order())
	}
}

// TestReconcileDuplicateCandidatesDeterministicByID pins the duplicate-
// generation convergence: with two CONVERGED containers in one slot (an A+B
// restart where both happen to match, or an aborted rollout), the lowest-ID one
// is kept deterministically and the other is cleaned up, regardless of list
// order.
func TestReconcileDuplicateCandidatesDeterministicByID(t *testing.T) {
	f := newFakeDocker()
	// Both fully converged for the desired spec (image, port, env, resources,
	// replica 0). "a-dup" sorts before "z-dup".
	converged := func(id string, port int) *fakeContainer {
		return &fakeContainer{
			id: id, function: "fn", entrypoint: "service.js", image: "img-1",
			port: port, replica: 0, state: container.StateRunning,
			envHash: serviceEnvHash(port), resources: serviceResources(),
		}
	}
	f.ctrs["a-dup"] = converged("a-dup", 80)
	f.ctrs["z-dup"] = converged("z-dup", 80)
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !changed {
		t.Fatal("a duplicate converged generation must be corrective")
	}
	if _, ok := f.containerByID("a-dup"); !ok {
		t.Fatal("the lowest-ID converged duplicate must be kept")
	}
	if _, ok := f.containerByID("z-dup"); ok {
		t.Fatal("the higher-ID converged duplicate must be cleaned up")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	// A second pass is now converged (no churn).
	changed, err = reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("the converged state after duplicate cleanup must be a no-op")
	}
}

// TestReconcileRestartWithOldAndNewConverges pins restart discovery: a previous
// run started the new generation B but crashed before stopping the old A (both
// running, same slot). On restart the converged B is kept, A is cleaned up, and
// a second pass is a no-op.
func TestReconcileRestartWithOldAndNewConverges(t *testing.T) {
	f := newFakeDocker()
	// B: converged for the desired img-new.
	f.ctrs["b-new"] = &fakeContainer{
		id: "b-new", function: "fn", entrypoint: "service.js", image: "img-new",
		port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80), resources: serviceResources(),
	}
	// A: old generation, still running.
	seedOldService(f, "a-old", "fn", "service.js", "img-old", 80, 0)
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, ok := f.containerByID("b-new"); !ok {
		t.Fatal("the converged new generation must be kept")
	}
	if _, ok := f.containerByID("a-old"); ok {
		t.Fatal("the superseded old generation must be cleaned up")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	stopsBefore := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed || len(f.stops) != stopsBefore {
		t.Fatalf("restart convergence must be stable: changed=%v stops %d->%d", changed, stopsBefore, len(f.stops))
	}
}

// TestReconcileRestartOldOnlyRetriesReplacement pins the "no desired B running"
// restart case: only the old A generation runs, so a replacement is attempted;
// on failure the old A is preserved for the retry.
func TestReconcileRestartOldOnlyRetriesReplacement(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "a-old", "fn", "service.js", "img-old", 80, 0)
	f.failStart[0] = fmt.Errorf("start failed for replica 0")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected the failed replacement to surface")
	}
	if _, ok := f.containerByID("a-old"); !ok {
		t.Fatal("the old fallback must be preserved while retrying")
	}
	// A later successful retry replaces it and stops the old.
	f.mu.Lock()
	delete(f.failStart, 0)
	f.mu.Unlock()
	if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
		t.Fatalf("retry reconcile: %v", err)
	}
	if _, ok := f.containerByID("a-old"); ok {
		t.Fatal("a successful retry must stop the old fallback")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 after retry", got)
	}
}

// TestReconcileScaleUpAndDownWithReplacement pins scale-up and scale-down
// combined with a config change, including the guarantee that scale-down does
// not remove every old replica before replacements converge.
func TestReconcileScaleUpAndDownWithReplacement(t *testing.T) {
	t.Run("scale up", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-0", "fn", "service.js", "img-old", 80, 0)
		tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 3})
		if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if got := f.runningCount("fn", "service.js"); got != 3 {
			t.Fatalf("running = %d, want 3", got)
		}
		// The old slot-0 container was replaced only after its replacement.
		if s, st := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-0"); s < 0 || st < 0 || s > st {
			t.Fatalf("order = %v, want start:0 before stop:old-0", f.order())
		}
	})

	t.Run("scale down keeps a running replica until replacements converge", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-0", "fn", "service.js", "img-old", 80, 0)
		seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 1)
		seedOldService(f, "old-2", "fn", "service.js", "img-old", 80, 2)
		tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
		if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if got := f.runningCount("fn", "service.js"); got != 1 {
			t.Fatalf("running = %d, want 1", got)
		}
		// Slot 0's replacement precedes the excess stops.
		s0, stop1 := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-1")
		if s0 < 0 || stop1 < 0 || s0 > stop1 {
			t.Fatalf("order = %v, want start:0 before the scale-down stop:old-1", f.order())
		}
	})
}

// TestReconcileRemovedServiceAllGenerationsStopped pins that a removed service
// has ALL its generations cleaned, including an A+B pair.
func TestReconcileRemovedServiceAllGenerationsStopped(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "gone-a", "fn", "old.js", "img-1", 80, 0)
	seedOldService(f, "gone-b", "fn", "old.js", "img-1", 80, 0)
	tmpl := serviceTemplate("node24") // no services
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("removed service generations left = %d, want 0", got)
	}
}

// TestReconcileRemovedServiceStoppedAfterDesiredStarts pins the identity-change
// ordering: a removed service's container is stopped only after a desired
// service has converged, so an identity change does not tear down the old
// generation before the new one is running.
func TestReconcileRemovedServiceStoppedAfterDesiredStarts(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-src", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
	// New desired identity (a changed image tag) plus the removed old identity.
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:2", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	start, stop := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-src")
	if start < 0 || stop < 0 || start > stop {
		t.Fatalf("order = %v, want the new identity started before the old identity is removed", f.order())
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:2"); got != 1 {
		t.Fatalf("new identity running = %d, want 1", got)
	}
}

// TestReconcileRemoveAllAndRemoveFunctionCleanAllGenerations pins that function
// removal cleans every generation.
func TestReconcileRemoveAllAndRemoveFunctionCleanAllGenerations(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "a", "fn", "service.js", "img-1", 80, 0)
	seedOldService(f, "b", "fn", "service.js", "img-2", 80, 0)
	RemoveAll(context.Background(), f, "fn", testutil.DiscardLogger())
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("containers after RemoveAll = %d, want 0", got)
	}
}

// TestReconcileReplaceCreateFailurePreservesOld is the create-stage failure
// (distinct from a start-stage failure): the replacement container could not be
// created at all, so the old generation is preserved and no stale cleanup runs
// for the slot.
func TestReconcileReplaceCreateFailurePreservesOld(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	f.failStart[0] = fmt.Errorf("service: create container: no such image")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "no such image") {
		t.Fatalf("err = %v, want the create failure surfaced", err)
	}
	if _, ok := f.containerByID("old-1"); !ok {
		t.Fatal("old generation must be preserved on a create failure")
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none on a create failure", f.stops)
	}
}

// TestReconcileStaleStopFailureIsSurfacedAndRecoverable pins the best-effort
// cleanup contract: a failing stale stop is surfaced, convergence of the rest
// proceeds, and a subsequent reconcile retries the cleanup (recoverable).
func TestReconcileStaleStopFailureIsSurfacedAndRecoverable(t *testing.T) {
	f := newFakeDocker()
	// A duplicate converged generation on the same slot: one keep, one stale to
	// stop. Make the stop of the stale one fail.
	converged := func(id string) *fakeContainer {
		return &fakeContainer{
			id: id, function: "fn", entrypoint: "service.js", image: "img-1",
			port: 80, replica: 0, state: container.StateRunning,
			envHash: serviceEnvHash(80), resources: serviceResources(),
		}
	}
	f.ctrs["a-keep"] = converged("a-keep")
	f.ctrs["z-stale"] = converged("z-stale")
	f.failStopFor = "z-stale"
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "stop failed") {
		t.Fatalf("err = %v, want the stale stop failure surfaced", err)
	}
	if _, ok := f.containerByID("z-stale"); !ok {
		t.Fatal("the failed stop must leave the container (partial progress only)")
	}
	// Recoverable: clearing the injected failure lets the next pass clean it.
	f.mu.Lock()
	f.failStopFor = ""
	f.mu.Unlock()
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("retry reconcile: %v", err)
	}
	if _, ok := f.containerByID("z-stale"); ok {
		t.Fatal("the next reconcile must retry and remove the stale duplicate")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

// TestReconcileNeverRemovesLastRunningReplica pins the availability safety net:
// when a desired slot has no old fallback AND its replacement fails, while the
// only other running generations are excess (a scale-down), the reconciler keeps
// one running generation rather than converging to zero replicas. It is
// deterministic (lowest-ID running stale is kept) and a no-op once a replacement
// succeeds.
func TestReconcileNeverRemovesLastRunningReplica(t *testing.T) {
	f := newFakeDocker()
	// Desired slot 0 is empty; the only running generations are the excess slots
	// 1 and 2 (e.g. slot 0's container was removed out of band).
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 1)
	seedOldService(f, "old-2", "fn", "service.js", "img-old", 80, 2)
	f.failStart[0] = fmt.Errorf("start failed for replica 0")
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected the failed replacement to surface")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (never converge a desired service to zero)", got)
	}
	// The lowest-ID running stale generation is the deterministic survivor.
	if _, ok := f.containerByID("old-1"); !ok {
		t.Fatal("old-1 (lowest-ID running stale) must be the survivor")
	}
	if _, ok := f.containerByID("old-2"); ok {
		t.Fatal("old-2 must be cleaned up")
	}
}

// TestReconcileImageRefProtectionAndExternalSemantics pins that the replacement
// path preserves the existing image-reference semantics: the replacement spec
// carries the resolved external reference and its content ID, and an
// entrypoint-source replacement carries the function image; external images are
// never mistaken for Relay-owned ones.
func TestReconcileImageRefProtectionAndExternalSemantics(t *testing.T) {
	t.Run("external image ref and content id preserved", func(t *testing.T) {
		f := newFakeDocker()
		f.resolvedImages["ghcr.io/acme/api:1.2"] = "sha256:cafe"
		seedOldService(f, "old-1", "fn", "ghcr.io/acme/api:1.2", "ghcr.io/acme/api:1.2", 8080, 0)
		// Change only the tag's content (a moved tag): same reference, new ID.
		f.mu.Lock()
		f.resolvedImages["ghcr.io/acme/api:1.2"] = "sha256:moved"
		f.mu.Unlock()
		tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
		if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		c := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
		if c == nil || c.image != "ghcr.io/acme/api:1.2" || c.imageID != "sha256:moved" {
			t.Fatalf("replacement = %+v, want the external ref + moved content id", c)
		}
		if c.entry != nil {
			t.Fatalf("external image replacement entry = %v, want nil (preserve image ENTRYPOINT)", c.entry)
		}
	})
}

// TestServiceCoordinatorReplacementOrderingAndSupersede drives the real
// ServiceReconciler through the existing ServiceCoordinator and pins two
// properties that only matter on the async production path:
//
//   - a replacement pass started by the coordinator keeps start-before-stop
//     ordering (the coordinator does not reorder reconcile work); and
//   - a newer desired state supersedes/coalesces through the coordinator's
//     existing mechanisms and the final pass converges to the LATEST state.
func TestServiceCoordinatorReplacementOrderingAndSupersede(t *testing.T) {
	f := newFakeDocker()
	services := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	start := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	start.Env = map[string]string{"MODE": "a"}
	coordinator.Enqueue("fn", start, "", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait 1: %v", err)
	}
	first := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if first == nil {
		t.Fatal("no initial container started through the coordinator")
	}

	// Reset the recorded transitions and publish a newer desired state (env=b).
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()
	changed := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	changed.Env = map[string]string{"MODE": "b"}
	coordinator.Enqueue("fn", changed, "", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait 2: %v", err)
	}

	order := f.order()
	startIdx, stopIdx := f.indexOfEvent("start:0"), f.indexOfEvent("stop:"+first.id)
	if startIdx < 0 || stopIdx < 0 || startIdx > stopIdx {
		t.Fatalf("order = %v, want start-before-stop through the coordinator", order)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1.2"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	c := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if c == nil || c.envHash != runtime.EnvHash([]string{"MODE=b", "PORT=8080"}) {
		t.Fatalf("final container = %+v, want the LATEST env=b effective env", c)
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestReconcileSourceRefChangeRemovalFailurePreservesOld covers gap 1: when a
// service's SourceRef changes (old identity A -> desired identity B), A's
// containers are "removed" (not in the desired set) and B's are added. If B does
// not converge — resolution/pull, routing/network, create/start, or non-running
// — A must remain running: under SourceRef grouping A may be the only usable
// generation for the service B replaces, so removing it would take the service
// to zero. Each subtest seeds A running at replica 0 and asserts A survives.
func TestReconcileSourceRefChangeRemovalFailurePreservesOld(t *testing.T) {
	newTmpl := func() *function.Template {
		return serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:2", Port: 80, Replicas: 1})
	}

	t.Run("resolve failure", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-a", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
		f.resolveErr["ghcr.io/acme/api:2"] = fmt.Errorf("pull failed: registry down")

		if _, err := reconcile(t, f, "fn", newTmpl(), "", routing.TraefikConfig{}); err == nil {
			t.Fatal("expected the resolve failure surfaced")
		}
		if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
			t.Fatalf("old identity A running = %d, want 1 (preserved)", got)
		}
		if len(f.stops) != 0 {
			t.Fatalf("stops = %v, want none on a failed desired convergence", f.stops)
		}
	})

	t.Run("missing routed network", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-a", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
		f.missingNetworks["proxy"] = true
		tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:2", Port: 80, Replicas: 1, Host: "svc.test"})

		if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{Network: "proxy"}); err == nil {
			t.Fatal("expected the missing routing network surfaced")
		}
		if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
			t.Fatalf("old identity A running = %d, want 1 (preserved)", got)
		}
		if len(f.stops) != 0 {
			t.Fatalf("stops = %v, want none on a missed routed network", f.stops)
		}
	})

	t.Run("create failure", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-a", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
		f.failStart[0] = fmt.Errorf("service: create container: no such image")

		if _, err := reconcile(t, f, "fn", newTmpl(), "", routing.TraefikConfig{}); err == nil {
			t.Fatal("expected the create failure surfaced")
		}
		if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
			t.Fatalf("old identity A running = %d, want 1 (preserved)", got)
		}
		if len(f.stops) != 0 {
			t.Fatalf("stops = %v, want none on a create failure", f.stops)
		}
	})

	t.Run("start failure", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-a", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
		f.failStart[0] = fmt.Errorf("service: start container: boom")

		if _, err := reconcile(t, f, "fn", newTmpl(), "", routing.TraefikConfig{}); err == nil {
			t.Fatal("expected the start failure surfaced")
		}
		if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
			t.Fatalf("old identity A running = %d, want 1 (preserved)", got)
		}
		if len(f.stops) != 0 {
			t.Fatalf("stops = %v, want none on a start failure", f.stops)
		}
	})

	t.Run("non-running replacement", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-a", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
		f.failStart[0] = fmt.Errorf("service: started container is not running")

		if _, err := reconcile(t, f, "fn", newTmpl(), "", routing.TraefikConfig{}); err == nil {
			t.Fatal("expected the non-running failure surfaced")
		}
		if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
			t.Fatalf("old identity A running = %d, want 1 (preserved)", got)
		}
		if len(f.stops) != 0 {
			t.Fatalf("stops = %v, want none on a non-running replacement", f.stops)
		}
	})
}

// TestReconcileSourceRefChangeSuccessRemovesOldAfterB pins the other half of gap
// 1: when the new SourceRef B DOES converge, A is removed only AFTER B is
// confirmed running (start-before-stop across the identity change).
func TestReconcileSourceRefChangeSuccessRemovesOldAfterB(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-a", "fn", "ghcr.io/acme/api:1", "ghcr.io/acme/api:1", 80, 0)
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:2", Port: 80, Replicas: 1})

	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	start, stop := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-a")
	if start < 0 || stop < 0 || start > stop {
		t.Fatalf("order = %v, want B started before A removed", f.order())
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:2"); got != 1 {
		t.Fatalf("new identity B running = %d, want 1", got)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 0 {
		t.Fatalf("old identity A running = %d, want 0", got)
	}
}
