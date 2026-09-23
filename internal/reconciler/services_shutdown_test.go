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

// TestShutdownCleanupRemovesOwnContainers: cleanup(hostname) selects exactly the
// containers whose Hostname == hostname (2 replicas of one function) and stops
// them all, returning the count.
func TestShutdownCleanupRemovesOwnContainers(t *testing.T) {
	f := newFakeDocker()
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "fn", Identity: "svc.js", Port: 80, Image: "img-1",
	}, 0); err != nil {
		t.Fatalf("start replica 0: %v", err)
	}
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "fn", Identity: "svc.js", Port: 80, Image: "img-1",
	}, 1); err != nil {
		t.Fatalf("start replica 1: %v", err)
	}

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
	n, err := c.ShutdownCleanup(context.Background(), defaultFakeHostname)
	if err != nil {
		t.Fatalf("shutdown cleanup: %v", err)
	}
	if n != 2 {
		t.Fatalf("removed count = %d, want 2", n)
	}
	if got := len(f.stops); got != 2 {
		t.Fatalf("stops = %d, want 2", got)
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("containers left = %d, want 0", got)
	}
}

// TestShutdownCleanupPreservesOtherWorkers: cleanup("w1") removes only w1's
// containers; another worker's (w2) and a container without a hostname label
// (empty Hostname — ownership unknowable) are preserved.
func TestShutdownCleanupPreservesOtherWorkers(t *testing.T) {
	f := newFakeDocker()
	seed := func(id, hostname string) {
		f.ctrs[id] = &fakeContainer{
			id: id, function: "fn", entrypoint: "svc.js", image: "img-1",
			port: 80, replica: 0, state: container.StateRunning, hostname: hostname,
		}
	}
	seed("w1-a", "w1")
	seed("w1-b", "w1")
	seed("w2-a", "w2")
	seed("none-a", "") // no hostname label: never claimed by any worker

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
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
	if got := f.countForFunction("fn"); got != 2 {
		t.Fatalf("preserved count = %d, want 2", got)
	}
}

// TestShutdownCleanupEmptyHostnameSelectsNothing verifies the defensive guard:
// an empty hostname matches no container (ownership is unknowable), so nothing
// is listed or stopped.
func TestShutdownCleanupEmptyHostnameSelectsNothing(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["a"] = &fakeContainer{
		id: "a", function: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning, hostname: "",
	}

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
	n, err := c.ShutdownCleanup(context.Background(), "")
	if err != nil {
		t.Fatalf("shutdown cleanup: %v", err)
	}
	if n != 0 {
		t.Fatalf("removed count = %d, want 0 (empty hostname selects nothing)", n)
	}
	if got := len(f.stops); got != 0 {
		t.Fatalf("stops = %v, want none", f.stops)
	}
	if got := f.countForFunction("fn"); got != 1 {
		t.Fatalf("container count = %d, want 1 (untouched)", got)
	}
}

// TestShutdownCleanupStopFailureDoesNotBlock: a failing stop surfaces the
// error and logs the Warn, but the other own containers are still removed
// (partial progress is kept).
func TestShutdownCleanupStopFailureDoesNotBlock(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["fail-1"] = &fakeContainer{
		id: "fail-1", function: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning, hostname: "w1",
	}
	f.ctrs["ok-1"] = &fakeContainer{
		id: "ok-1", function: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 1, state: container.StateRunning, hostname: "w1",
	}
	f.failStopFor = "fail-1"

	logger, capture := newCaptureLogger(slog.LevelWarn)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger)
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
