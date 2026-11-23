package reconciler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/config"
	"relay/internal/routing"
	"relay/internal/testutil"
)

func reconcileServiceBind(
	t *testing.T, f *fakeDocker, fn string, tmpl *app.Template, image, bind string,
) {
	t.Helper()
	if err := applyServiceBind(f, fn, tmpl, image, bind); err != nil {
		t.Fatalf("ApplyResult: %v", err)
	}
}

// applyServiceBind runs one reconcile pass with the given bind address and
// returns the pass error, for tests that assert on failure outcomes.
func applyServiceBind(f *fakeDocker, fn string, tmpl *app.Template, image, bind string) error {
	services := NewServiceReconciler(
		f, nil, routing.TraefikConfig{Network: "proxy"},
		testutil.DiscardLogger(), testReconcileTimeout,
		WithServiceBindAddr(bind),
		WithNodeID(defaultFakeNodeID),
	)
	return services.ApplyResult(
		context.Background(), fn, tmpl, image, nil, nil,
	).Err
}

func TestServiceBindAddressAppliesOnlyToRoutedServices(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 1},
		app.Service{Name: "worker", Entrypoint: "worker.js", Port: 9000, Replicas: 1},
	)

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")

	routed := f.lastStartedFor("fn", "web")
	if routed == nil {
		t.Fatal("no routed container started")
	}
	if routed.bindAddr != "127.0.0.1" {
		t.Fatalf("routed bind = %q, want 127.0.0.1", routed.bindAddr)
	}
	unrouted := f.lastStartedFor("fn", "worker")
	if unrouted == nil {
		t.Fatal("no unrouted container started")
	}
	if unrouted.bindAddr != "" {
		t.Fatalf("unrouted bind = %q, want none even when publication is configured",
			unrouted.bindAddr)
	}
}

func TestServiceBindAddressChangeReplacesStartBeforeStop(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 1},
	)

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")
	first := f.lastStartedFor("fn", "web")
	if first == nil || first.bindAddr != "127.0.0.1" {
		t.Fatalf("first container = %+v, want bind 127.0.0.1", first)
	}
	firstID := first.id
	eventsBefore := len(f.order())

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.2")

	second := f.lastStartedFor("fn", "web")
	if second == nil {
		t.Fatal("no replacement container")
	}
	if second.id == firstID {
		t.Fatalf("container %s was not replaced after a bind-only change", firstID)
	}
	if second.bindAddr != "127.0.0.2" {
		t.Fatalf("replacement bind = %q, want 127.0.0.2", second.bindAddr)
	}
	if second.image != "img-1" {
		t.Fatalf("replacement image = %q, want the unchanged app image %q",
			second.image, "img-1")
	}
	if _, ok := f.containerByID(firstID); ok {
		t.Fatalf("old container %s still exists after the replacement", firstID)
	}

	events := f.order()[eventsBefore:]
	start := -1
	stop := -1
	for i, event := range events {
		switch event {
		case "start:0":
			if start < 0 {
				start = i
			}
		case "stop:" + firstID:
			stop = i
		}
	}
	if start < 0 || stop < 0 || start > stop {
		t.Fatalf("events = %v, want the replacement started before the old container stopped",
			events)
	}
}

func TestServiceBindAddressUnchangedIsIdempotent(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 1},
	)

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")
	if c := f.lastStartedFor("fn", "web"); c == nil {
		t.Fatal("no container started by the first pass")
	}
	started, stopped := f.startedCount(), len(f.stopRequestLog())

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")

	if f.startedCount() != started || len(f.stopRequestLog()) != stopped {
		t.Fatalf("unchanged bind caused replacement: started %d->%d stops %d->%d",
			started, f.startedCount(), stopped, len(f.stopRequestLog()))
	}
}

// startedBindsFor returns the bind address stamped on every started container
// of one service, keyed by replica slot.
func startedBindsFor(f *fakeDocker, service string) map[int]string {
	binds := make(map[int]string)
	for _, c := range f.ctrs {
		if c.entrypoint == service {
			binds[c.replica] = c.bindAddr
		}
	}
	return binds
}

// A routed service publishes on every one of its replicas while an unrouted
// service of the same app stays internal-only, and all replicas coexist as
// separate containers.
func TestServiceBindAddressStampsEveryReplicaAndSparesUnrouted(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 2},
		app.Service{Name: "worker", Entrypoint: "worker.js", Port: 9000, Replicas: 1},
	)

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")

	routed := startedBindsFor(f, "web")
	if len(routed) != 2 || routed[0] != "127.0.0.1" || routed[1] != "127.0.0.1" {
		t.Fatalf("routed replica binds = %v, want both replicas on 127.0.0.1", routed)
	}
	unrouted := startedBindsFor(f, "worker")
	if len(unrouted) != 1 || unrouted[0] != "" {
		t.Fatalf("unrouted binds = %v, want one unpublished replica", unrouted)
	}
	if f.startedCount() != 3 {
		t.Fatalf("started = %d, want the two routed replicas and the unrouted one to coexist",
			f.startedCount())
	}
}

// Publication applies to a routed service only while SERVICE_BIND_ADDR is
// configured: enabling publication replaces the internal-only container with a
// bound one, and disabling it replaces the bound container with an
// internal-only one, each through the normal replacement path.
func TestServicePublicationToggleReplacesRoutedContainer(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 1},
	)

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "")
	first := f.lastStartedFor("fn", "web")
	if first == nil || first.bindAddr != "" {
		t.Fatalf("disabled publication container = %+v, want one with no bind address", first)
	}

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")
	second := f.lastStartedFor("fn", "web")
	if second == nil {
		t.Fatal("no container after enabling publication")
	}
	if second.id == first.id || second.bindAddr != "127.0.0.1" {
		t.Fatalf("replacement = %+v, want a new container bound to 127.0.0.1 (replacing %s)",
			second, first.id)
	}
	if _, ok := f.containerByID(first.id); ok {
		t.Fatalf("internal-only container %s still exists after enabling publication", first.id)
	}

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "")
	third := f.lastStartedFor("fn", "web")
	if third == nil {
		t.Fatal("no container after disabling publication")
	}
	if third.id == second.id || third.bindAddr != "" {
		t.Fatalf("replacement = %+v, want a new internal-only container (replacing %s)",
			third, second.id)
	}
	if _, ok := f.containerByID(second.id); ok {
		t.Fatalf("bound container %s still exists after disabling publication", second.id)
	}
}

// SERVICE_ADVERTISE_ADDR is presentation metadata resolved by config and never
// an input to container identity: changing it alone (bind unchanged) leaves
// every replica untouched — no replacement, no rebuild.
func TestServiceAdvertiseAddressChangeKeepsContainers(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 1},
	)

	// The worker wires the reconciler from the same configuration loader the
	// administrative commands use, so the test drives both passes through it.
	loadBind := func(advertise string) string {
		t.Setenv("SERVICE_BIND_ADDR", "127.0.0.1")
		t.Setenv("SERVICE_ADVERTISE_ADDR", advertise)
		bind, _, err := config.LoadServicePublication()
		if err != nil {
			t.Fatalf("LoadServicePublication: %v", err)
		}
		return bind
	}

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", loadBind("10.0.0.5"))
	first := f.lastStartedFor("fn", "web")
	if first == nil {
		t.Fatal("no container started by the first pass")
	}
	started, stopped := f.startedCount(), len(f.stopRequestLog())

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", loadBind("203.0.113.7"))

	if f.startedCount() != started || len(f.stopRequestLog()) != stopped {
		t.Fatalf("advertise-only change replaced containers: started %d->%d stops %d->%d",
			started, f.startedCount(), stopped, len(f.stopRequestLog()))
	}
	if second := f.lastStartedFor("fn", "web"); second == nil || second.id != first.id {
		t.Fatalf("container after advertise change = %+v, want the original %s kept",
			second, first.id)
	}
}

// A bind-address change whose replacement fails StartService's post-start
// mapping verification (the publish_unverified discard) surfaces the failure
// and preserves the OLD bound replica: it keeps running with its previous bind
// identity and is never stopped for a replacement that was never verified.
func TestServiceBindChangePreservesOldWhenMappingUnverified(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web", Entrypoint: "web.js", Port: 8080, Host: "web.test", Replicas: 1},
	)

	reconcileServiceBind(t, f, "fn", tmpl, "img-1", "127.0.0.1")
	old := f.lastStartedFor("fn", "web")
	if old == nil || old.bindAddr != "127.0.0.1" {
		t.Fatalf("initial container = %+v, want one bound to 127.0.0.1", old)
	}

	// The replacement starts but Docker never materializes the new mapping;
	// StartService discards it and reports exactly this verification failure.
	f.failStart[0] = errors.New(
		"service: container cid-new did not report exactly one " +
			"8080/tcp host binding on 127.0.0.2 after start")

	err := applyServiceBind(f, "fn", tmpl, "img-1", "127.0.0.2")
	if err == nil || !strings.Contains(err.Error(),
		"did not report exactly one 8080/tcp host binding on 127.0.0.2") {
		t.Fatalf("error = %v, want the mapping-verification failure surfaced", err)
	}
	kept, ok := f.containerByID(old.id)
	if !ok || kept.state != container.StateRunning || kept.bindAddr != "127.0.0.1" {
		t.Fatalf("old bound replica = %+v (exists=%v), want it running with its previous bind",
			kept, ok)
	}
	if f.indexOfEvent("stop:"+old.id) >= 0 {
		t.Fatalf("order = %v: the old replica must not stop when its replacement failed verification",
			f.order())
	}
	if got := f.runningCount("fn", "web"); got != 1 {
		t.Fatalf("running = %d, want 1 (old preserved)", got)
	}
}
