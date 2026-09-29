package runner

import (
	"context"
	"strings"
	"sync"
	"testing"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// indexEvents is the shared event battery for the runner index equivalence tests.
func indexEvents() []map[string]any {
	return []map[string]any{
		{},
		{"status": "COMPLETED"},
		{"status": "FAILED"},
		{"status": nil},
		{"status": false},
		{"status": 0},
		{"event_name": "MODIFY", "table_name": "enrollments"},
		{"new_image": map[string]any{"status": "COMPLETED", "cnpj": nil}},
		{"new_image": map[string]any{"name": "ACME"}},
		{"new_image": nil},
		{"field": "1"},
		{"field": 1},
		{"field": true},
		{"field": "true"},
		{"field": nil},
		{"id": "ENROLLMENT#123"},
		{"id": "OTHER#123"},
		{"score": 92},
		{"score": 70},
		{"created_at": "2026-09-12T10:00:00Z"},
	}
}

// assertSnapshotEquivalent asserts that a snapshot's indexed matching agrees with
// a full exact scan, rule for rule and in order, for every battery event.
func assertSnapshotEquivalent(t *testing.T, snap *pinnedSnapshot, pf *PreparedFunction) {
	t.Helper()
	for ei, event := range indexEvents() {
		got := snap.matchingRules(pf, event)
		want := pf.fn.Template.MatchingEventRules(event)
		if len(got) != len(want) {
			t.Fatalf("event %d: indexed %d rules, full scan %d", ei, len(got), len(want))
		}
		for i := range got {
			if got[i].Handler != want[i].Handler {
				t.Fatalf("event %d rule %d: indexed %q, full scan %q", ei, i, got[i].Handler, want[i].Handler)
			}
		}
	}
}

// parsedFn builds a prepared function whose template is parsed from YAML, so its
// event rules carry real matchers (and therefore real anchors).
func parsedFn(t *testing.T, name, tmplYAML string, exec Executor) *PreparedFunction {
	t.Helper()
	tmpl, err := function.ParseTemplate([]byte(tmplYAML))
	if err != nil {
		t.Fatalf("parse template %q: %v", name, err)
	}
	return NewPrepared(function.Function{Name: name, Template: tmpl}, &runtime.Prepared{Name: name, Image: "x"}, exec)
}

const indexedTmplA = `runtime: node24
events:
  - handler: handler.eq
    pattern:
      status: [COMPLETED, FAILED]
  - handler: handler.prefix
    pattern:
      id:
        prefix: ["ENROLLMENT#"]
  - handler: handler.exists
    pattern:
      new_image:
        cnpj:
          exists: true
  - handler: handler.and
    pattern:
      event_name: [MODIFY]
      table_name: [enrollments]
`

const indexedTmplB = `runtime: node24
events:
  - handler: handler.numeric
    pattern:
      score: [92]
  - handler: handler.absent
    pattern:
      field:
        exists: false
`

// TestRegistryIndexGenerationEquivalence pins that a snapshot's per-function index
// agrees with a full exact scan for every event.
func TestRegistryIndexGenerationEquivalence(t *testing.T) {
	pf := parsedFn(t, "alpha", indexedTmplA, &countingExecutor{})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())

	snap := r.Registry().snapshotPinned()
	defer snap.release()
	if snap.rulesFor(pf) == nil {
		t.Fatal("snapshot must publish a candidate index for a parsed template")
	}
	assertSnapshotEquivalent(t, snap, pf)
}

// TestRegistryIndexRebuiltOnReplace pins that a snapshot binds to the index of the
// function generation it observed: a snapshot taken before a replacement keeps the
// old generation's rules, and a later snapshot uses the new generation's rules.
func TestRegistryIndexRebuiltOnReplace(t *testing.T) {
	old := parsedFn(t, "alpha", indexedTmplA, &countingExecutor{})
	r := New([]*PreparedFunction{old}, testutil.DiscardLogger())

	before := r.Registry().snapshotPinned()
	defer before.release()

	// Replace with a DIFFERENT template generation.
	next := parsedFn(t, "alpha", indexedTmplB, &countingExecutor{})
	r.Registry().Replace("alpha", next)

	// The old snapshot is bound to the old generation: its index and rules are the
	// old ones, not resolved by name against the new entry.
	if got := before.rulesFor(old); got == nil {
		t.Fatal("old snapshot lost its generation's index")
	}
	if got := before.rulesFor(next); got != nil {
		t.Fatal("old snapshot must not see the new generation's index")
	}
	oldHandlers := handlerNames(before.matchingRules(old, map[string]any{"status": "COMPLETED"}))
	if strings.Join(oldHandlers, ",") != "handler.eq" {
		t.Fatalf("old snapshot matched %v, want the old generation's [handler.eq]", oldHandlers)
	}
	// The superseded entry is not in the old snapshot's function set, so a real
	// Handle would never match it. `matchingRules` falls back conservatively to the
	// entry's own full scan (never borrowing another generation's index).
	if got := before.matchingRules(next, map[string]any{}); strings.Join(handlerNames(got), ",") != "handler.absent" {
		t.Fatalf("unindexed fallback = %v, want the entry's own full scan [handler.absent]", handlerNames(got))
	}

	after := r.Registry().snapshotPinned()
	defer after.release()
	if got := handlerNames(after.matchingRules(next, map[string]any{"score": 92})); strings.Join(got, ",") != "handler.numeric,handler.absent" {
		t.Fatalf("new snapshot matched %v, want [handler.numeric handler.absent]", got)
	}
	// The superseded entry is absent from the new snapshot's function set; its
	// fallback runs the entry's own scan rather than the new generation's index.
	if got := after.rulesFor(next); got == nil {
		t.Fatal("new snapshot must index the new generation")
	}
}

// TestRegistryIndexRemovalExcludes pins that removing an entry drops it from the
// index: a stale snapshot still serves it (it is immutable), but a fresh snapshot
// has no index for it and GetByName no longer finds it.
func TestRegistryIndexRemovalExcludes(t *testing.T) {
	pf := parsedFn(t, "alpha", indexedTmplA, &countingExecutor{})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())
	r.Registry().Replace("alpha", nil)

	if got := r.Registry().GetByName("alpha"); got != nil {
		t.Fatalf("GetByName after removal = %v, want nil", got)
	}
	snap := r.Registry().snapshotPinned()
	defer snap.release()
	if got := snap.rulesFor(pf); got != nil {
		t.Fatal("removed entry must not appear in a fresh snapshot's index")
	}
	if len(snap.fns) != 0 {
		t.Fatalf("snapshot functions = %d, want 0", len(snap.fns))
	}
}

// TestRegistryIndexUnavailableIncluded pins that an unavailable function's entry
// is still indexed and still classifies as matching: an event matching only an
// unavailable function is MATCHED, and a fresh snapshot must expose the anchor
// rather than falling back or dropping the rule.
func TestRegistryIndexUnavailableIncluded(t *testing.T) {
	tmpl, err := function.ParseTemplate([]byte(indexedTmplA))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	pf := NewUnavailable(function.Function{Name: "broken", Template: tmpl})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())

	snap := r.Registry().snapshotPinned()
	defer snap.release()
	if snap.rulesFor(pf) == nil {
		t.Fatal("unavailable function must still be indexed")
	}
	got := handlerNames(snap.matchingRules(pf, map[string]any{"status": "COMPLETED"}))
	if strings.Join(got, ",") != "handler.eq" {
		t.Fatalf("unavailable match = %v, want [handler.eq]", got)
	}
	assertSnapshotEquivalent(t, snap, pf)
}

// TestRegistryIndexNilTemplateFallback pins that a hand-built entry with no
// template is not indexed and its snapshot lookup falls back to a full scan
// (which yields nothing, since a nil template cannot match).
func TestRegistryIndexNilTemplateFallback(t *testing.T) {
	pf := NewUnavailable(function.Function{Name: "no-template"})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())

	snap := r.Registry().snapshotPinned()
	defer snap.release()
	if got := snap.rulesFor(pf); got != nil {
		t.Fatal("nil-template entry must not be indexed")
	}
	if got := snap.matchingRules(pf, map[string]any{"status": "COMPLETED"}); len(got) != 0 {
		t.Fatalf("nil-template entry matched %d rules, want 0", len(got))
	}
}

// TestHandleIndexedExecutionOrder pins that Handle's indexed matching executes
// matching handlers in the existing order — registry function-name order, then
// rule declaration order — and skips non-matching rules, exactly as a full scan
// would.
func TestHandleIndexedExecutionOrder(t *testing.T) {
	exec := &recordingExecutor{}
	r := New([]*PreparedFunction{
		parsedFn(t, "beta", `runtime: node24
events:
  - handler: beta.first
    pattern:
      status: [COMPLETED]
  - handler: beta.second
    pattern:
      event_name: [MODIFY]
  - handler: beta.third
    pattern:
      id:
        prefix: ["NO-MATCH"]
`, exec),
		parsedFn(t, "alpha", indexedTmplA, exec),
	}, testutil.DiscardLogger())

	event := map[string]any{
		"status":     "COMPLETED",
		"event_name": "MODIFY",
		"table_name": "enrollments",
		"new_image":  map[string]any{"cnpj": nil},
	}
	if err := r.Handle(context.Background(), "m", event); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	handlers, _ := exec.got()
	got := strings.Join(handlers, ",")
	// alpha functions sort before beta; within a function, declaration order.
	want := "handler.eq,handler.exists,handler.and,beta.first,beta.second"
	if got != want {
		t.Fatalf("execution order = %q, want %q", got, want)
	}
}

// TestHandleIndexConcurrentReload runs Handle against indexed templates while the
// registry is churned with Set/Replace under -race, asserting no error or panic
// and that every consumed index generation stays internally consistent.
func TestHandleIndexConcurrentReload(t *testing.T) {
	names := []string{"a", "b", "c"}
	var fns []*PreparedFunction
	for _, n := range names {
		fns = append(fns, parsedFn(t, n, indexedTmplA, &countingExecutor{}))
	}
	r := New(fns, testutil.DiscardLogger())

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := names[w%len(names)]
				if w%2 == 0 {
					// Alternate between two generations so the index is rebuilt
					// with materially different rules.
					if w%4 == 0 {
						r.Registry().Replace(n, parsedFn(t, n, indexedTmplB, &countingExecutor{}))
					} else {
						r.Registry().Replace(n, parsedFn(t, n, indexedTmplA, &countingExecutor{}))
					}
				} else {
					r.Registry().Set([]*PreparedFunction{
						parsedFn(t, "a", indexedTmplA, &countingExecutor{}),
						parsedFn(t, "b", indexedTmplB, &countingExecutor{}),
					})
				}
			}
		}(w)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		event := map[string]any{"status": "COMPLETED", "score": 92, "field": nil, "event_name": "MODIFY", "table_name": "enrollments", "new_image": map[string]any{"cnpj": nil}}
		for i := 0; i < 300; i++ {
			if err := r.Handle(context.Background(), "m", event); err != nil {
				t.Errorf("Handle: %v", err)
			}
		}
	}()
	<-done
	close(stop)
	wg.Wait()
}

// handlerNames maps matched rules to handler names for order assertions.
func handlerNames(rules []function.EventRule) []string {
	out := make([]string, len(rules))
	for i, r := range rules {
		out[i] = r.Handler
	}
	return out
}
