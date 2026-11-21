package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"relay/internal/testutil"
)

// stateMarkerManager builds a Manager whose Docker calls hit scripted routes,
// so the convergence-state volume paths run through the real client call path
// with no daemon.
func stateMarkerManager(t *testing.T, routes ...dockerRoute) *Manager {
	t.Helper()
	return &Manager{
		cli:    newScriptedDockerClient(t, routes...),
		log:    testutil.DiscardLogger(),
		nodeID: "state-test",
	}
}

// strictStateLabels is the label set a volume must carry to be recognized as
// Relay-owned service convergence state.
func strictStateLabels(appName, serviceName, nodeID string, replica int) map[string]string {
	return map[string]string{
		labelType:    labelServiceStateType,
		labelApp:     appName,
		labelService: serviceName,
		labelNodeID:  nodeID,
		labelReplica: strconv.Itoa(replica),
	}
}

// withLabels returns base overlaid with extra.
func withLabels(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// scriptedVolume is one daemon volume in a scripted GET /volumes response.
type scriptedVolume struct {
	Name      string            `json:"Name"`
	Labels    map[string]string `json:"Labels,omitempty"`
	CreatedAt string            `json:"CreatedAt,omitempty"`
}

// volumeListJSON renders a GET /volumes response over the given volumes.
func volumeListJSON(t *testing.T, volumes []scriptedVolume) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"Volumes": volumes})
	if err != nil {
		t.Fatalf("render volume list: %v", err)
	}
	return string(raw)
}

// markerVolumeScript is a tiny stateful daemon over Relay's marker volumes: a
// create appends the volume with its recorded labels, a list reflects the
// current set, and a removal deletes by name. It records every call so tests
// can assert serialization (create body), scope (which names were removed),
// and that read paths never mutate the daemon.
type markerVolumeScript struct {
	listCalls   int
	createCalls int
	deleted     []string
	listFilter  string
	volumes     []scriptedVolume
	created     struct {
		Name   string            `json:"Name"`
		Labels map[string]string `json:"Labels"`
	}
}

// newMarkerScript seeds the daemon with the given volumes.
func newMarkerScript(volumes ...scriptedVolume) *markerVolumeScript {
	return &markerVolumeScript{volumes: volumes}
}

func (s *markerVolumeScript) routes(t *testing.T) []dockerRoute {
	return []dockerRoute{
		{
			method: http.MethodPost, path: "/volumes/create",
			body: `{"Name":"scripted"}`,
			onBody: func(body []byte) {
				s.createCalls++
				if err := json.Unmarshal(body, &s.created); err != nil {
					t.Errorf("decode volume create body: %v", err)
					return
				}
				s.volumes = append(s.volumes, scriptedVolume{
					Name: s.created.Name, Labels: s.created.Labels,
				})
			},
		},
		{
			method: http.MethodGet, path: "/volumes",
			onRequest: func(req *http.Request) {
				s.listCalls++
				s.listFilter = req.URL.Query().Get("filters")
			},
			serve: func(*http.Request) (int, string) {
				return http.StatusOK, volumeListJSON(t, s.volumes)
			},
		},
		{
			method: http.MethodDelete, path: "/volumes/", status: http.StatusNoContent,
			onRequest: func(req *http.Request) {
				// The client prefixes the API version (e.g. /v1.56/volumes/x).
				name := req.URL.Path
				if i := strings.Index(name, "/volumes/"); i >= 0 {
					name = name[i+len("/volumes/"):]
				}
				s.deleted = append(s.deleted, name)
				s.volumes = slices.DeleteFunc(s.volumes, func(v scriptedVolume) bool {
					return v.Name == name
				})
			},
		},
	}
}

// WriteServiceConvergenceState serializes the marker into volume labels and a
// later list reads the exact same marker back: every committed, candidate, and
// retry field round-trips, the strict ownership label and scoped volume name
// are stamped, and a write with no older marker removes nothing.
func TestServiceConvergenceStateWriteRoundTripsThroughVolumeLabels(t *testing.T) {
	ctx := context.Background()

	t.Run("full candidate and retry metadata round-trips", func(t *testing.T) {
		script := newMarkerScript()
		m := stateMarkerManager(t, script.routes(t)...)
		updatedAt := time.Date(2030, 1, 2, 3, 4, 5, 6000000, time.UTC)
		retryAt := time.Date(2030, 5, 6, 7, 8, 9, 123456000, time.UTC)

		err := m.WriteServiceConvergenceState(ctx, ServiceConvergenceState{
			App: "fn", Service: "web", NodeID: "n1", Replica: 2,
			CommittedContainerID: "ctr-active", Generation: "gen-current",
			CandidateContainerID: "ctr-cand", CandidateGeneration: "gen-prior",
			CandidateAttempt: 3, RetryNotBefore: retryAt, UpdatedAt: updatedAt,
		})
		if err != nil {
			t.Fatalf("WriteServiceConvergenceState: %v", err)
		}
		if script.createCalls != 1 {
			t.Fatalf("volume creates = %d, want 1", script.createCalls)
		}

		want := map[string]string{
			labelType:                labelServiceStateType,
			labelApp:                 "fn",
			labelService:             "web",
			labelNodeID:              "n1",
			labelReplica:             "2",
			labelCommitted:           "ctr-active",
			labelGeneration:          "gen-current",
			labelUpdatedAt:           updatedAt.UTC().Format(time.RFC3339Nano),
			labelCandidateContainer:  "ctr-cand",
			labelCandidateGeneration: "gen-prior",
			labelCandidateAttempt:    "3",
			labelRetryNotBefore:      retryAt.UTC().Format(time.RFC3339Nano),
		}
		for key, value := range want {
			if got := script.created.Labels[key]; got != value {
				t.Errorf("label %s = %q, want %q", key, got, value)
			}
		}
		if len(script.created.Labels) != len(want) {
			t.Errorf("label count = %d, want exactly the %d documented keys: %v",
				len(script.created.Labels), len(want), script.created.Labels)
		}
		if !strings.HasPrefix(script.created.Name, "relay-svc-state-n1-fn-web-2") {
			t.Errorf("volume name = %q, want the node/app/service/replica-scoped prefix",
				script.created.Name)
		}
		if len(script.deleted) != 0 {
			t.Fatalf("volume removals = %v, want none (no older marker exists)", script.deleted)
		}

		listed, err := m.ListServiceConvergenceStates(ctx)
		if err != nil {
			t.Fatalf("ListServiceConvergenceStates: %v", err)
		}
		if len(listed) != 1 {
			t.Fatalf("listed markers = %d, want 1", len(listed))
		}
		got := listed[0]
		if got.App != "fn" || got.Service != "web" || got.NodeID != "n1" || got.Replica != 2 ||
			got.CommittedContainerID != "ctr-active" || got.Generation != "gen-current" ||
			got.CandidateContainerID != "ctr-cand" ||
			got.CandidateGeneration != "gen-prior" || got.CandidateAttempt != 3 {
			t.Fatalf("listed marker = %+v, want the written marker verbatim", got)
		}
		if !got.RetryNotBefore.Equal(retryAt) {
			t.Errorf("RetryNotBefore = %v, want %v", got.RetryNotBefore, retryAt)
		}
		if !got.UpdatedAt.Equal(updatedAt) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, updatedAt)
		}
	})

	t.Run("committed-only marker omits candidate labels and reads back as zero", func(t *testing.T) {
		script := newMarkerScript()
		m := stateMarkerManager(t, script.routes(t)...)
		updatedAt := time.Date(2030, 1, 2, 3, 4, 5, 6000000, time.UTC)

		err := m.WriteServiceConvergenceState(ctx, ServiceConvergenceState{
			App: "fn", Service: "web", NodeID: "n1", Replica: 0,
			CommittedContainerID: "ctr-active", Generation: "gen-current",
			UpdatedAt: updatedAt,
		})
		if err != nil {
			t.Fatalf("WriteServiceConvergenceState: %v", err)
		}
		for _, key := range []string{
			labelCandidateContainer, labelCandidateGeneration,
			labelCandidateAttempt, labelRetryNotBefore,
		} {
			if value, ok := script.created.Labels[key]; ok {
				t.Errorf("label %s = %q, want it omitted for a committed-only marker", key, value)
			}
		}

		listed, err := m.ListServiceConvergenceStates(ctx)
		if err != nil {
			t.Fatalf("ListServiceConvergenceStates: %v", err)
		}
		if len(listed) != 1 {
			t.Fatalf("listed markers = %d, want 1", len(listed))
		}
		got := listed[0]
		if got.CommittedContainerID != "ctr-active" || got.Generation != "gen-current" ||
			got.CandidateContainerID != "" || got.CandidateGeneration != "" ||
			got.CandidateAttempt != 0 || !got.RetryNotBefore.IsZero() {
			t.Fatalf("listed marker = %+v, want committed fields with zero candidate/retry fields", got)
		}
		if !got.UpdatedAt.Equal(updatedAt) {
			t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, updatedAt)
		}
	})

	t.Run("zero UpdatedAt is stamped at write time", func(t *testing.T) {
		script := newMarkerScript()
		m := stateMarkerManager(t, script.routes(t)...)
		if err := m.WriteServiceConvergenceState(ctx, ServiceConvergenceState{
			App: "fn", Service: "web", NodeID: "n1", Replica: 0,
			CommittedContainerID: "ctr-active", Generation: "gen-current",
		}); err != nil {
			t.Fatalf("WriteServiceConvergenceState: %v", err)
		}
		stamp := script.created.Labels[labelUpdatedAt]
		parsed, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || parsed.IsZero() {
			t.Fatalf("updated_at label = %q (err %v), want a stamped RFC3339Nano instant", stamp, err)
		}
		listed, err := m.ListServiceConvergenceStates(ctx)
		if err != nil || len(listed) != 1 {
			t.Fatalf("ListServiceConvergenceStates = (%v, %v), want one marker", listed, err)
		}
		if !listed[0].UpdatedAt.Equal(parsed) {
			t.Errorf("listed UpdatedAt = %v, want the stamped %v", listed[0].UpdatedAt, parsed)
		}
	})
}

// Discovery classifies volumes defensively: only strict Relay service-state
// volumes count, malformed replicas are skipped instead of guessed, and
// missing or unparseable timestamps degrade (UpdatedAt label → CreatedAt →
// zero; unparseable attempt/retry numbers → zero values) rather than fail the
// whole listing. Listing never creates or removes anything, and the request
// carries the strict server-side label filter.
func TestListServiceConvergenceStatesSkipsForeignAndMalformedVolumes(t *testing.T) {
	script := newMarkerScript(
		scriptedVolume{Name: "full", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 2), map[string]string{
				labelCommitted:           "ctr-a",
				labelGeneration:          "gen-a",
				labelUpdatedAt:           "2030-01-02T03:04:05.5Z",
				labelCandidateContainer:  "ctr-b",
				labelCandidateGeneration: "gen-b",
				labelCandidateAttempt:    "4",
				labelRetryNotBefore:      "2030-01-02T03:05:00.5Z",
			})},
		// Foreign volumes must never be classified as markers.
		scriptedVolume{Name: "unlabeled-local", Labels: map[string]string{labelApp: "fn"}},
		scriptedVolume{Name: "foreign-type", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 0),
			map[string]string{labelType: "cache"})},
		scriptedVolume{Name: "bad-replica", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 0),
			map[string]string{labelReplica: "many"})},
		// Degradation cases: strict volumes with stamping defects.
		scriptedVolume{Name: "created-fallback",
			Labels:    strictStateLabels("fn", "web", "n1", 0),
			CreatedAt: "2024-03-04T05:06:07Z"},
		scriptedVolume{Name: "bad-stamp", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 1),
			map[string]string{labelUpdatedAt: "not-a-time"}),
			CreatedAt: "2024-03-04T05:06:07Z"},
		scriptedVolume{Name: "no-stamp", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 3),
			map[string]string{labelUpdatedAt: "junk"}),
			CreatedAt: "junk"},
		scriptedVolume{Name: "bad-numbers", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 4), map[string]string{
				labelCommitted:          "ctr-c",
				labelCandidateAttempt:   "bogus",
				labelRetryNotBefore:     "bogus",
				labelCandidateContainer: "ctr-d",
			})},
	)
	m := stateMarkerManager(t, script.routes(t)...)

	listed, err := m.ListServiceConvergenceStates(context.Background())
	if err != nil {
		t.Fatalf("ListServiceConvergenceStates: %v", err)
	}
	gotReplicas := make([]int, 0, len(listed))
	for _, state := range listed {
		gotReplicas = append(gotReplicas, state.Replica)
	}
	if !slices.Equal(gotReplicas, []int{2, 0, 1, 3, 4}) {
		t.Fatalf("listed replicas = %v, want [2 0 1 3 4]: foreign and unparsable volumes excluded, strict ones kept in order",
			gotReplicas)
	}

	full := listed[0]
	wantCreated := time.Date(2030, 1, 2, 3, 4, 5, 500000000, time.UTC)
	wantRetry := time.Date(2030, 1, 2, 3, 5, 0, 500000000, time.UTC)
	if full.CommittedContainerID != "ctr-a" || full.Generation != "gen-a" ||
		full.CandidateContainerID != "ctr-b" || full.CandidateGeneration != "gen-b" ||
		full.CandidateAttempt != 4 ||
		!full.UpdatedAt.Equal(wantCreated) || !full.RetryNotBefore.Equal(wantRetry) {
		t.Fatalf("full marker = %+v, want every serialized field decoded verbatim", full)
	}

	createdAt := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	if got := listed[1]; !got.UpdatedAt.Equal(createdAt) ||
		got.CommittedContainerID != "" || got.CandidateAttempt != 0 ||
		!got.RetryNotBefore.IsZero() {
		t.Errorf("missing stamp marker = %+v, want the CreatedAt fallback with zero unset fields", got)
	}
	if got := listed[2]; !got.UpdatedAt.Equal(createdAt) {
		t.Errorf("unparseable stamp marker = %+v, want the CreatedAt fallback", got)
	}
	if got := listed[3]; !got.UpdatedAt.IsZero() {
		t.Errorf("no usable stamp marker = %+v, want a zero UpdatedAt", got)
	}
	if got := listed[4]; got.CommittedContainerID != "ctr-c" ||
		got.CandidateContainerID != "ctr-d" || got.CandidateAttempt != 0 ||
		!got.RetryNotBefore.IsZero() {
		t.Errorf("unparseable numbers marker = %+v, want identity fields kept and numbers zeroed", got)
	}

	if script.createCalls != 0 || len(script.deleted) != 0 {
		t.Fatalf("daemon mutations = create %d, delete %v, want none from a read path",
			script.createCalls, script.deleted)
	}
	if !strings.Contains(script.listFilter, "relay.type") ||
		!strings.Contains(script.listFilter, labelServiceStateType) {
		t.Errorf("list filter = %q, want the strict relay.type=%s server-side filter",
			script.listFilter, labelServiceStateType)
	}
}

// Removal is scoped: only strict service-state volumes of the requested
// app/service/node are deleted — other apps, nodes, services, foreign-typed,
// and unlabeled volumes are never touched — and an empty app short-circuits
// before any Docker call at all.
func TestDeleteServiceConvergenceStatesScopedToStrictAppServiceNodeVolumes(t *testing.T) {
	ctx := context.Background()
	seed := func() []scriptedVolume {
		return []scriptedVolume{
			{Name: "web-n1", Labels: strictStateLabels("fn", "web", "n1", 0)},
			{Name: "worker-n1", Labels: strictStateLabels("fn", "worker", "n1", 0)},
			{Name: "web-n2", Labels: strictStateLabels("fn", "web", "n2", 0)},
			{Name: "other-app", Labels: strictStateLabels("other", "web", "n1", 0)},
			{Name: "unlabeled", Labels: map[string]string{
				labelApp: "fn", labelService: "web", labelNodeID: "n1",
			}},
			{Name: "foreign-type", Labels: withLabels(
				strictStateLabels("fn", "web", "n1", 0),
				map[string]string{labelType: "cache"})},
		}
	}
	deletedOf := func(t *testing.T, appName, serviceName, nodeID string) *markerVolumeScript {
		t.Helper()
		script := newMarkerScript(seed()...)
		m := stateMarkerManager(t, script.routes(t)...)
		if err := m.DeleteServiceConvergenceStates(ctx, appName, serviceName, nodeID); err != nil {
			t.Fatalf("DeleteServiceConvergenceStates(%q, %q, %q): %v",
				appName, serviceName, nodeID, err)
		}
		got := append([]string(nil), script.deleted...)
		slices.Sort(got)
		return script
	}
	expectDeleted := func(t *testing.T, script *markerVolumeScript, want ...string) {
		t.Helper()
		got := append([]string(nil), script.deleted...)
		wantSorted := append([]string(nil), want...)
		slices.Sort(got)
		slices.Sort(wantSorted)
		if !slices.Equal(got, wantSorted) {
			t.Fatalf("removed volumes = %v, want exactly %v", got, wantSorted)
		}
	}

	t.Run("service and node scope", func(t *testing.T) {
		script := deletedOf(t, "fn", "web", "n1")
		expectDeleted(t, script, "web-n1")
		if !strings.Contains(script.listFilter, labelServiceStateType) {
			t.Errorf("list filter = %q, want the strict service-state filter", script.listFilter)
		}
	})
	t.Run("empty service spans the app's services on the node", func(t *testing.T) {
		script := deletedOf(t, "fn", "", "n1")
		expectDeleted(t, script, "web-n1", "worker-n1")
	})
	t.Run("empty node spans the app's nodes", func(t *testing.T) {
		script := deletedOf(t, "fn", "", "")
		expectDeleted(t, script, "web-n1", "worker-n1", "web-n2")
	})
	t.Run("unknown app removes nothing", func(t *testing.T) {
		script := deletedOf(t, "absent", "", "")
		expectDeleted(t, script)
		if script.listCalls != 1 {
			t.Fatalf("list calls = %d, want 1 (the daemon was consulted)", script.listCalls)
		}
	})
	t.Run("empty app makes no Docker call", func(t *testing.T) {
		script := newMarkerScript(seed()...)
		m := stateMarkerManager(t, script.routes(t)...)
		if err := m.DeleteServiceConvergenceStates(ctx, "", "web", "n1"); err != nil {
			t.Fatalf("DeleteServiceConvergenceStates: %v", err)
		}
		if script.listCalls != 0 || len(script.deleted) != 0 || script.createCalls != 0 {
			t.Fatalf("daemon calls = list %d, delete %v, create %d, want none for an empty app",
				script.listCalls, script.deleted, script.createCalls)
		}
	})
}

// Create-before-delete cleanup removes only the OLDER strict marker of the
// same scope: other apps, nodes, services, foreign-typed, and unlabeled
// volumes survive, and the just-created marker stays discoverable.
func TestWriteServiceConvergenceStateCleanupRemovesOnlySameScopeMarkers(t *testing.T) {
	script := newMarkerScript(
		scriptedVolume{Name: "old-same-scope", Labels: strictStateLabels("fn", "web", "n1", 1)},
		scriptedVolume{Name: "other-app", Labels: strictStateLabels("other", "web", "n1", 1)},
		scriptedVolume{Name: "other-node", Labels: strictStateLabels("fn", "web", "n2", 1)},
		scriptedVolume{Name: "other-service", Labels: strictStateLabels("fn", "worker", "n1", 1)},
		scriptedVolume{Name: "unlabeled", Labels: map[string]string{
			labelApp: "fn", labelService: "web", labelNodeID: "n1",
		}},
		scriptedVolume{Name: "foreign-type", Labels: withLabels(
			strictStateLabels("fn", "web", "n1", 1),
			map[string]string{labelType: "cache"})},
	)
	m := stateMarkerManager(t, script.routes(t)...)
	ctx := context.Background()

	err := m.WriteServiceConvergenceState(ctx, ServiceConvergenceState{
		App: "fn", Service: "web", NodeID: "n1", Replica: 1,
		CommittedContainerID: "ctr-new", Generation: "gen-2",
		UpdatedAt: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("WriteServiceConvergenceState: %v", err)
	}
	if script.createCalls != 1 {
		t.Fatalf("volume creates = %d, want 1 (the new marker first)", script.createCalls)
	}
	got := append([]string(nil), script.deleted...)
	slices.Sort(got)
	if !slices.Equal(got, []string{"old-same-scope"}) {
		t.Fatalf("removed volumes = %v, want exactly [old-same-scope] "+
			"(other scopes, foreign-typed, and unlabeled volumes must survive)", got)
	}
	remaining := make(map[string]bool, len(script.volumes))
	for _, volume := range script.volumes {
		remaining[volume.Name] = true
	}
	if !remaining[script.created.Name] {
		t.Fatal("the just-created marker was removed by its own cleanup")
	}
	if remaining["old-same-scope"] {
		t.Fatal("the older same-scope marker survived cleanup")
	}
	if len(script.volumes) != 6 {
		t.Fatalf("remaining volumes = %d, want 6 (the new marker plus the 5 must-survive volumes)",
			len(script.volumes))
	}

	listed, err := m.ListServiceConvergenceStates(ctx)
	if err != nil {
		t.Fatalf("ListServiceConvergenceStates: %v", err)
	}
	// Discovery is unscoped, so the surviving strict markers of other scopes
	// list too; exactly one marker may carry this write's own key.
	var mine []ServiceConvergenceState
	for _, state := range listed {
		if state.App == "fn" && state.Service == "web" && state.NodeID == "n1" {
			mine = append(mine, state)
		}
	}
	if len(mine) != 1 ||
		mine[0].CommittedContainerID != "ctr-new" || mine[0].Generation != "gen-2" {
		t.Fatalf("markers for the written key = %+v, want exactly the newly committed generation",
			mine)
	}
	if len(listed) != 4 {
		t.Fatalf("listed markers = %d, want 4 (the new one plus the 3 surviving strict markers; "+
			"unlabeled and foreign-typed volumes stay excluded)", len(listed))
	}
}

// Write cleanup is replica-exact: committing replica N versions away only the
// OLDER replica-N marker for the same app/service/node and retains a sibling
// replica's marker, so one replica's convergence can never destroy another
// replica's committed state.
func TestWriteServiceConvergenceStateCleanupRetainsSiblingReplicaMarker(t *testing.T) {
	script := newMarkerScript(
		// Same app/service/node: replica 1 is the version being replaced;
		// replica 0 is the sibling that must survive this write.
		scriptedVolume{Name: "svc-old-rep1", Labels: strictStateLabels("fn", "web", "n1", 1)},
		scriptedVolume{Name: "svc-sibling-rep0", Labels: strictStateLabels("fn", "web", "n1", 0)},
	)
	m := stateMarkerManager(t, script.routes(t)...)
	ctx := context.Background()

	if err := m.WriteServiceConvergenceState(ctx, ServiceConvergenceState{
		App: "fn", Service: "web", NodeID: "n1", Replica: 1,
		CommittedContainerID: "ctr-r1-new", Generation: "gen-r1",
		UpdatedAt: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
	}); err != nil {
		t.Fatalf("WriteServiceConvergenceState: %v", err)
	}

	removed := append([]string(nil), script.deleted...)
	slices.Sort(removed)
	if !slices.Equal(removed, []string{"svc-old-rep1"}) {
		t.Fatalf("removed volumes = %v, want exactly [svc-old-rep1] "+
			"(the sibling replica marker must be retained)", removed)
	}
	remaining := make(map[string]bool, len(script.volumes))
	for _, volume := range script.volumes {
		remaining[volume.Name] = true
	}
	if !remaining["svc-sibling-rep0"] {
		t.Fatalf("sibling replica-0 marker was destroyed by replica 1's write; remaining = %v",
			remaining)
	}
	if remaining["svc-old-rep1"] {
		t.Fatalf("older replica-1 marker survived its versioned replacement; remaining = %v",
			remaining)
	}
	if !remaining[script.created.Name] {
		t.Fatalf("new replica-1 marker missing after cleanup; remaining = %v", remaining)
	}
}

// An invalid marker key is rejected before any daemon work: no create, no
// list, no removal.
func TestWriteServiceConvergenceStateRejectsInvalidKeysWithoutDockerCalls(t *testing.T) {
	ctx := context.Background()
	for name, state := range map[string]ServiceConvergenceState{
		"empty app":        {App: "", Service: "web", Replica: 0},
		"empty service":    {App: "fn", Service: "", Replica: 0},
		"negative replica": {App: "fn", Service: "web", Replica: -1},
	} {
		t.Run(name, func(t *testing.T) {
			var createCalls, listCalls int
			m := stateMarkerManager(t,
				dockerRoute{method: http.MethodPost, path: "/volumes/create",
					body: `{}`, onMatch: func() { createCalls++ }},
				dockerRoute{method: http.MethodGet, path: "/volumes",
					body: `{"Volumes":[]}`, onMatch: func() { listCalls++ }},
			)
			err := m.WriteServiceConvergenceState(ctx, state)
			if err == nil || !strings.Contains(err.Error(), "invalid convergence state key") {
				t.Fatalf("error = %v, want the invalid-key rejection", err)
			}
			if createCalls != 0 || listCalls != 0 {
				t.Fatalf("daemon calls = create %d, list %d, want none before validation passes",
					createCalls, listCalls)
			}
		})
	}
}
