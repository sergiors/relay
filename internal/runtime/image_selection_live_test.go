package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/testutil"
)

// selectionMemoryStore is a deterministic stand-in for the shared selection
// store. The same mutex protects reads, CAS-like updates, and publication
// callbacks, matching the store contract without involving PostgreSQL.
type selectionMemoryStore struct {
	mu        sync.Mutex
	rows      []RuntimeImageSelection
	readErr   error
	readCount int
	lockCount int
}

func (s *selectionMemoryStore) ReadRuntimeImageSelections(context.Context) ([]RuntimeImageSelection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readCount++
	if s.readErr != nil {
		return nil, s.readErr
	}
	return append([]RuntimeImageSelection(nil), s.rows...), nil
}

func (s *selectionMemoryStore) WithRuntimeImageSelectionLock(
	ctx context.Context,
	runtimeID string,
	fn func(context.Context, *RuntimeImageSelection) error,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lockCount++
	var current *RuntimeImageSelection
	for i := range s.rows {
		if s.rows[i].RuntimeID == runtimeID {
			row := s.rows[i]
			current = &row
			break
		}
	}
	return fn(ctx, current)
}

func (s *selectionMemoryStore) replace(rows ...RuntimeImageSelection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append([]RuntimeImageSelection(nil), rows...)
}

func (s *selectionMemoryStore) setReadError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readErr = err
}

func TestRefreshRuntimeImageSelectionsReplacesCompleteSnapshotOnce(t *testing.T) {
	store := &selectionMemoryStore{rows: []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)}}
	pins, _, err := resolveImageSelections(store.rows)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{log: testutil.DiscardLogger(), selectionStore: store, imageSelections: pins}

	store.replace(
		pinnedPythonSelection(pinnedDigestB),
		RuntimeImageSelection{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestA},
	)
	changed, err := m.RefreshRuntimeImageSelections(context.Background())
	if err != nil {
		t.Fatalf("refresh changed snapshot: %v", err)
	}
	if want := []string{"nodejs24.x", "python3.14"}; !reflect.DeepEqual(changed, want) {
		t.Fatalf("changed runtimes = %v, want sorted %v", changed, want)
	}
	if got := m.RuntimeImageSelectionIdentity("python3.14"); got != "python:3.14-slim@"+pinnedDigestB {
		t.Fatalf("python identity = %q, want digest B", got)
	}

	// Rewriting generation/selection time for the same effective pin is not an
	// image identity change. A missing row, however, means unpinned.
	store.replace(RuntimeImageSelection{
		RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestA,
		Generation: 2, SelectedAt: "later",
	})
	changed, err = m.RefreshRuntimeImageSelections(context.Background())
	if err != nil || !reflect.DeepEqual(changed, []string{"python3.14"}) {
		t.Fatalf("refresh after removal = (%v, %v), want only python unpinned", changed, err)
	}
	if got := m.RuntimeImageSelectionIdentity("nodejs24.x"); got != "node:24-alpine@"+pinnedDigestA {
		t.Fatalf("generation-only rewrite changed node identity: %q", got)
	}

	store.replace()
	changed, err = m.RefreshRuntimeImageSelections(context.Background())
	if err != nil || !reflect.DeepEqual(changed, []string{"nodejs24.x"}) {
		t.Fatalf("refresh to absent selection = (%v, %v), want node unpinned", changed, err)
	}
	if store.readCount != 3 {
		t.Fatalf("selection reads = %d, want exactly one per refresh", store.readCount)
	}
}

func TestRefreshRuntimeImageSelectionsKeepsLastKnownGoodOnReadOrValidationError(t *testing.T) {
	store := &selectionMemoryStore{rows: []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)}}
	pins, _, err := resolveImageSelections(store.rows)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{log: testutil.DiscardLogger(), selectionStore: store, imageSelections: pins}
	want := "python:3.14-slim@" + pinnedDigestA

	store.setReadError(errors.New("database unavailable"))
	if _, err := m.RefreshRuntimeImageSelections(context.Background()); err == nil {
		t.Fatal("read failure was not returned")
	}
	if got := m.RuntimeImageSelectionIdentity("python3.14"); got != want {
		t.Fatalf("read failure changed last-known-good identity to %q", got)
	}

	store.setReadError(nil)
	store.replace(
		RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:wrong", Digest: pinnedDigestB},
		RuntimeImageSelection{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: "sha256:bad"},
	)
	if _, err := m.RefreshRuntimeImageSelections(context.Background()); err == nil {
		t.Fatal("invalid snapshot was not rejected")
	}
	if got := m.RuntimeImageSelectionIdentity("python3.14"); got != want {
		t.Fatalf("invalid snapshot partially changed last-known-good identity to %q", got)
	}
	if got := m.RuntimeImageSelectionIdentity("nodejs24.x"); got != "" {
		t.Fatalf("invalid snapshot partially installed unrelated node identity %q", got)
	}
}

func TestPublishPreparedRejectsStaleAndAbsentSelectionWithoutActivation(t *testing.T) {
	cases := []struct {
		name     string
		rows     []RuntimeImageSelection
		builtPin string
	}{
		{name: "different pin", rows: []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestB)}, builtPin: "python:3.14-slim@" + pinnedDigestA},
		{name: "pin disappeared", rows: nil, builtPin: "python:3.14-slim@" + pinnedDigestA},
		{name: "pin appeared after unpinned build", rows: []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)}, builtPin: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &selectionMemoryStore{rows: tc.rows}
			m := &Manager{log: testutil.DiscardLogger(), containers: newContainerCache(), selectionStore: store}
			prepared := &Prepared{Name: "stale-app", Image: "img-stale", Runtime: "python3.14", ImageSelection: tc.builtPin}
			published := false
			err := m.PublishPrepared(context.Background(), prepared, func() error {
				published = true
				return nil
			})
			if !errors.Is(err, ErrRuntimeImageSelectionStale) {
				t.Fatalf("PublishPrepared error = %v, want stale-selection error", err)
			}
			if published {
				t.Fatal("stale generation reached the publication callback")
			}
			if _, ok := m.PoolSnapshot("stale-app"); ok {
				t.Fatal("stale generation activated the warm pool")
			}
			if store.lockCount != 1 {
				t.Fatalf("publication lock count = %d, want one authoritative check", store.lockCount)
			}
		})
	}
}

func TestPrepareCapturesOneRuntimeSelectionAcrossRefresh(t *testing.T) {
	dir, fn := nodeHandlerFixture(t, "selection-capture")
	store := &selectionMemoryStore{rows: []RuntimeImageSelection{{
		RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestA,
	}}}
	pins, _, err := resolveImageSelections(store.rows)
	if err != nil {
		t.Fatal(err)
	}
	var builds []capturedBuild
	var m *Manager
	cl := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/", status: http.StatusNotFound,
			body: `{"message":"no such image"}`,
			onMatch: func() {
				// The selection is captured before image reuse is checked. Change
				// the manager snapshot at this precise boundary; this prepare must
				// continue using the original immutable value throughout its build.
				store.replace(RuntimeImageSelection{
					RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestB,
				})
				if _, refreshErr := m.RefreshRuntimeImageSelections(context.Background()); refreshErr != nil {
					t.Errorf("refresh during prepare: %v", refreshErr)
				}
			},
		},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(r *http.Request) { builds = append(builds, capturedBuild{request: r}) },
			onBody: func(b []byte) {
				last := &builds[len(builds)-1]
				last.tar = append([]byte(nil), b...)
				last.tag = last.request.URL.Query().Get("t")
				labels := last.request.URL.Query().Get("labels")
				if err := json.Unmarshal([]byte(labels), &last.labels); err != nil {
					t.Errorf("decode labels: %v", err)
				}
			},
		},
	)
	m = newLifecycleManager(t, cl, context.Background())
	m.selectionStore = store
	m.imageSelections = pins

	selection, fp, err := app.SelectAndFingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := m.PrepareWithFingerprintAndSelection(context.Background(), fn, fp, selection)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.ImageSelection != "node:24-alpine@"+pinnedDigestA {
		t.Fatalf("prepared selection = %q, want captured digest A", prepared.ImageSelection)
	}
	if got := m.RuntimeImageSelectionIdentity("nodejs24.x"); got != "node:24-alpine@"+pinnedDigestB {
		t.Fatalf("manager selection after refresh = %q, want digest B", got)
	}
	if len(builds) != 1 || !strings.HasPrefix(builds[0].dockerfile(t), "FROM node:24-alpine@"+pinnedDigestA+"\n") {
		t.Fatalf("build did not use its captured digest A: %+v", builds)
	}
}
