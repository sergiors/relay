package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"relay/internal/testutil"
)

// scriptedContainer is one container in a scripted /containers/json response:
// an explicit immutable id plus its label set.
type scriptedContainer struct {
	id     string
	labels map[string]string
}

// orphanListJSON renders the minimal /containers/json shape the orphan sweep
// reads (Id + Labels) for the given containers.
func orphanListJSON(t *testing.T, ctrs ...scriptedContainer) string {
	t.Helper()
	items := make([]string, 0, len(ctrs))
	for _, c := range ctrs {
		labels, err := json.Marshal(c.labels)
		if err != nil {
			t.Fatalf("marshal labels: %v", err)
		}
		items = append(items, fmt.Sprintf(`{"Id":%q,"Labels":%s}`, c.id, string(labels)))
	}
	return "[" + strings.Join(items, ",") + "]"
}

// TestDiscoverOrphanContainersStrictHostnameAndTypeFilter pins the discovery
// half's predicate: only invocation containers (strict relay.type event or
// schedule) bearing THIS hostname are returned; service containers, other
// hostnames, non-Relay containers, and typed-but-hostname-less/untyped Relay
// containers are all excluded.
func TestDiscoverOrphanContainersStrictHostnameAndTypeFilter(t *testing.T) {
	body := orphanListJSON(t,
		scriptedContainer{"evt-1", map[string]string{
			labelType: ContainerTypeEvent, labelApp: "app", labelHostname: "host", labelHandler: "index.hi",
		}},
		scriptedContainer{"sch-1", map[string]string{
			labelType: ContainerTypeSchedule, labelApp: "app", labelHostname: "host", labelHandler: "tick",
		}},
		// Persistent service on our hostname: reconciler-owned, never swept.
		scriptedContainer{"svc-1", map[string]string{
			labelType: ContainerTypeService, labelApp: "app", labelHostname: "host",
		}},
		// Another worker's live/crashed invocation: never ours.
		scriptedContainer{"other-1", map[string]string{
			labelType: ContainerTypeEvent, labelApp: "app", labelHostname: "other",
		}},
		// Non-Relay container.
		scriptedContainer{"foreign-1", map[string]string{"app": "whatever"}},
		// relay.app but NO relay.type: untyped, not Relay-owned.
		scriptedContainer{"untyped-1", map[string]string{labelApp: "app", labelHostname: "host"}},
	)
	cli := newScriptedDockerClient(t, dockerRoute{method: http.MethodGet, path: "/containers/json", body: body})
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	got, err := m.DiscoverOrphanContainers(context.Background(), "host")
	if err != nil {
		t.Fatalf("DiscoverOrphanContainers: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("discovered %+v, want exactly the two invocation orphans (evt-1, sch-1)", got)
	}
	byID := map[string]OrphanContainer{}
	for _, o := range got {
		byID[o.ID] = o
	}
	if o, ok := byID["evt-1"]; !ok || o.App != "app" || o.Handler != "index.hi" {
		t.Errorf("evt-1 = %+v (ok=%v), want {App:app Handler:index.hi}", o, ok)
	}
	if o, ok := byID["sch-1"]; !ok || o.App != "app" || o.Handler != "tick" {
		t.Errorf("sch-1 = %+v (ok=%v), want {App:app Handler:tick}", o, ok)
	}
}

// TestRemoveOrphanContainersTargetsCapturedIDsOnly pins the property the
// snapshot-before-proof ordering depends on: removal targets EXACTLY the
// captured immutable ids and never re-lists the daemon, so a replacement
// container that appears after the snapshot can never be selected.
func TestRemoveOrphanContainersTargetsCapturedIDsOnly(t *testing.T) {
	body := orphanListJSON(t,
		scriptedContainer{"a", map[string]string{labelType: ContainerTypeEvent, labelApp: "app", labelHostname: "host"}},
		scriptedContainer{"b", map[string]string{labelType: ContainerTypeEvent, labelApp: "app", labelHostname: "host"}},
	)

	var (
		mu        sync.Mutex
		deleted   []string
		listCalls int
	)
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/containers/json", body: body,
			onMatch: func() { mu.Lock(); listCalls++; mu.Unlock() },
		},
		dockerRoute{
			method: http.MethodDelete, path: "/containers/",
			onRequest: func(r *http.Request) {
				mu.Lock()
				deleted = append(deleted, r.URL.Path)
				mu.Unlock()
			},
			body: "{}",
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	orphans, err := m.DiscoverOrphanContainers(context.Background(), "host")
	if err != nil {
		t.Fatalf("DiscoverOrphanContainers: %v", err)
	}
	// After the snapshot a replacement owner's container ("replacement") would
	// exist on the daemon; removal must never discover it.
	n, err := m.RemoveOrphanContainers(context.Background(), orphans)
	if err != nil {
		t.Fatalf("RemoveOrphanContainers: %v", err)
	}
	if n != 2 {
		t.Fatalf("removed = %d, want 2", n)
	}

	mu.Lock()
	gotDeleted := append([]string(nil), deleted...)
	gotList := listCalls
	mu.Unlock()

	if gotList != 1 {
		t.Fatalf("container list calls = %d, want 1 (removal must not re-discover)", gotList)
	}
	if len(gotDeleted) != 2 {
		t.Fatalf("delete calls = %v, want exactly the two captured ids", gotDeleted)
	}
	want := map[string]bool{"a": true, "b": true}
	for _, p := range gotDeleted {
		id := p[strings.LastIndex(p, "/")+1:]
		if !want[id] {
			t.Fatalf("deleted %q (id %q); only captured ids a and b may be targeted", p, id)
		}
	}
}

// TestRemoveOrphanContainersBenignOnConcurrentRemoval pins that a container the
// daemon already removed (a 404, e.g. a concurrent remover or AutoRemove) is
// benign: the removal reports success and does not surface an error.
func TestRemoveOrphanContainersBenignOnConcurrentRemoval(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodDelete, path: "/containers/gone",
			status: http.StatusNotFound,
			body:   `{"message":"No such container: gone"}`,
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	n, err := m.RemoveOrphanContainers(context.Background(), []OrphanContainer{{ID: "gone", App: "app"}})
	if err != nil {
		t.Fatalf("RemoveOrphanContainers on an already-removed container = %v, want nil (benign)", err)
	}
	if n != 1 {
		t.Fatalf("removed = %d, want 1 (a benign not-found means the container will not outlive the call)", n)
	}
}

// TestRemoveOrphanContainersNoOpWithoutCandidates pins the empty-input fast
// path: nothing is removed and no Docker call is made (the scripted client would
// fail the test on any unhandled request).
func TestRemoveOrphanContainersNoOpWithoutCandidates(t *testing.T) {
	cli := newScriptedDockerClient(t)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}
	n, err := m.RemoveOrphanContainers(context.Background(), nil)
	if err != nil || n != 0 {
		t.Fatalf("RemoveOrphanContainers(nil) = (%d, %v), want (0, nil)", n, err)
	}
}
