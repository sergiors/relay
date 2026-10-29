package reconciler

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestShutdownCleanupRemovesOwnContainers: cleanup(nodeID) selects exactly the
// containers whose NodeID == nodeID (2 replicas of one app) and stops
// them all, returning the count.
func TestShutdownCleanupRemovesOwnContainers(t *testing.T) {
	f := newFakeDocker()
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		App:       "fn",
		Name:      "svc.js",
		SourceRef: "svc.js", Port: 80, Image: "img-1",
	}, 0); err != nil {
		t.Fatalf("start replica 0: %v", err)
	}
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		App:       "fn",
		Name:      "svc.js",
		SourceRef: "svc.js", Port: 80, Image: "img-1",
	}, 1); err != nil {
		t.Fatalf("start replica 1: %v", err)
	}

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	n, err := c.ShutdownCleanup(context.Background(), defaultFakeNodeID)
	if err != nil {
		t.Fatalf("shutdown cleanup: %v", err)
	}
	if n != 2 {
		t.Fatalf("removed count = %d, want 2", n)
	}
	if got := len(f.stops); got != 2 {
		t.Fatalf("stops = %d, want 2", got)
	}
	if got := f.countForApp("fn"); got != 0 {
		t.Fatalf("containers left = %d, want 0", got)
	}
}

// TestShutdownCleanupPreservesOtherWorkers: cleanup("w1") removes only w1's
// containers; another worker's (w2) and a container without a nodeID label
// (empty NodeID — ownership unknowable) are preserved.
func TestShutdownCleanupPreservesOtherWorkers(t *testing.T) {
	f := newFakeDocker()
	seed := func(id, nodeID string) {
		f.ctrs[id] = &fakeContainer{
			id: id, appName: "fn", entrypoint: "svc.js", image: "img-1",
			port: 80, replica: 0, state: container.StateRunning, nodeID: nodeID,
		}
	}
	seed("w1-a", "w1")
	seed("w1-b", "w1")
	seed("w2-a", "w2")
	seed("none-a", "") // no nodeID label: never claimed by any worker

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	n, err := c.ShutdownCleanup(context.Background(), "w1")
	if err != nil {
		t.Fatalf("shutdown cleanup: %v", err)
	}
	if n != 2 {
		t.Fatalf("removed count = %d, want 2", n)
	}
	if got := len(f.stops); got != 2 || f.stops[0] != "w1-a" || f.stops[1] != "w1-b" {
		t.Fatalf("stops = %v, want [w1-a w1-b]", f.stops)
	}
	f.mu.Lock()
	_, w2Alive := f.ctrs["w2-a"]
	_, noneAlive := f.ctrs["none-a"]
	f.mu.Unlock()
	if !w2Alive || !noneAlive {
		t.Fatalf("other workers' containers must be preserved (w2Alive=%v noneAlive=%v)", w2Alive, noneAlive)
	}
	if got := f.countForApp("fn"); got != 2 {
		t.Fatalf("preserved count = %d, want 2", got)
	}
}

// TestShutdownCleanupEmptyNodeIDSelectsNothing verifies the defensive guard:
// an empty nodeID matches no container (ownership is unknowable), so nothing
// is listed or stopped.
func TestShutdownCleanupEmptyNodeIDSelectsNothing(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["a"] = &fakeContainer{
		id: "a", appName: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning, nodeID: "",
	}

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	n, err := c.ShutdownCleanup(context.Background(), "")
	if err != nil {
		t.Fatalf("shutdown cleanup: %v", err)
	}
	if n != 0 {
		t.Fatalf("removed count = %d, want 0 (empty nodeID selects nothing)", n)
	}
	if got := len(f.stops); got != 0 {
		t.Fatalf("stops = %v, want none", f.stops)
	}
	if got := f.countForApp("fn"); got != 1 {
		t.Fatalf("container count = %d, want 1 (untouched)", got)
	}
}

// TestShutdownCleanupStopFailureDoesNotBlock: a failing stop surfaces the
// error and logs the Warn, but the other own containers are still removed
// (partial progress is kept).
func TestShutdownCleanupStopFailureDoesNotBlock(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["fail-1"] = &fakeContainer{
		id: "fail-1", appName: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning, nodeID: "w1",
	}
	f.ctrs["ok-1"] = &fakeContainer{
		id: "ok-1", appName: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 1, state: container.StateRunning, nodeID: "w1",
	}
	f.failStopFor = "fail-1"

	logger, capture := newCaptureLogger(slog.LevelWarn)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger, testReconcileTimeout)
	n, err := c.ShutdownCleanup(context.Background(), "w1")
	if err == nil {
		t.Fatal("expected the stop failure to be surfaced")
	}
	if !strings.Contains(err.Error(), "stop failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("selected count = %d, want 2", n)
	}
	if !strings.Contains(capture.String(), "Service: shutdown cleanup failed") {
		t.Fatalf("expected the Warn \"Service: shutdown cleanup failed\", got:\n%s", capture.String())
	}
	// Partial progress: the non-failing own container is gone, the failing one remains.
	f.mu.Lock()
	_, failAlive := f.ctrs["fail-1"]
	_, okAlive := f.ctrs["ok-1"]
	f.mu.Unlock()
	if okAlive {
		t.Fatal("the non-failing own container must have been removed despite the failing stop")
	}
	if !failAlive {
		t.Fatal("the failing container must remain (it could not be stopped)")
	}
}

// TestDiscoverShutdownContainersSelectsOwnNodeID pins the split discovery half:
// it returns exactly the containers whose relay.node_id equals the argument
// (nodeID-scoped, empty nodeID selects nothing) and stops nothing — the
// caller decides removal.
func TestDiscoverShutdownContainersSelectsOwnNodeID(t *testing.T) {
	f := newFakeDocker()
	seed := func(id, nodeID string) {
		f.ctrs[id] = &fakeContainer{
			id: id, appName: "fn", entrypoint: "svc.js", image: "img-1",
			port: 80, replica: 0, state: container.StateRunning, nodeID: nodeID,
		}
	}
	seed("w1-b", "w1")
	seed("w1-a", "w1")
	seed("w2-a", "w2")
	seed("none-a", "")

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	own, err := c.DiscoverShutdownContainers(context.Background(), "w1")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(own) != 2 || own[0].ID != "w1-a" || own[1].ID != "w1-b" {
		t.Fatalf("discovered %+v, want [w1-a w1-b] (own nodeID only)", own)
	}
	if len(f.stops) != 0 {
		t.Fatalf("discovery stopped %v, want none (discovery must not remove)", f.stops)
	}

	none, err := c.DiscoverShutdownContainers(context.Background(), "")
	if err != nil {
		t.Fatalf("discover empty nodeID: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("empty nodeID discovered %+v, want none", none)
	}
}

// TestRemoveShutdownContainersRemovesCapturedOnlyAndPreservesReplacement pins the
// property the snapshot-before-proof ordering relies on: removal stops exactly
// the captured containers and a container created AFTER the snapshot (a
// replacement owner's fresh container, with a new immutable id) survives
// untouched because removal never re-lists the daemon.
func TestRemoveShutdownContainersRemovesCapturedOnlyAndPreservesReplacement(t *testing.T) {
	f := newFakeDocker()
	for _, id := range []string{"w1-a", "w1-b"} {
		f.ctrs[id] = &fakeContainer{
			id: id, appName: "fn", entrypoint: "svc.js", image: "img-1",
			port: 80, replica: 0, state: container.StateRunning, nodeID: "w1",
		}
	}
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	own, err := c.DiscoverShutdownContainers(context.Background(), "w1")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(own) != 2 {
		t.Fatalf("discovered %+v, want 2", own)
	}

	// A replacement owner reusing NODE_ID starts a NEW service container after
	// the snapshot. It must survive the removal of the captured set.
	f.ctrs["replacement"] = &fakeContainer{
		id: "replacement", appName: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning, nodeID: "w1",
	}

	if err := c.RemoveShutdownContainers(context.Background(), own); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := len(f.stops); got != 2 {
		t.Fatalf("stops = %v, want exactly the two captured containers", f.stops)
	}
	f.mu.Lock()
	_, alive := f.ctrs["replacement"]
	f.mu.Unlock()
	if !alive {
		t.Fatal("a container created after the snapshot must survive (removal must not re-list)")
	}
}
