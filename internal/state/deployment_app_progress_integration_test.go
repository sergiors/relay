//go:build integration

package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// RecordDeploymentAppProgress write semantics beyond the fence: the
// INSERT ... SELECT ... ON CONFLICT write reports whether PostgreSQL actually
// inserted or changed a row. True means an insert or a meaningful change
// (instance or payload); false means a stale fence OR an accepted identical
// rewrite, which must leave the row untouched.
//
// "The database did not UPDATE" is asserted two independent ways, without
// sleeps. The injectable clock (nowFn) is pinned to a deterministic instant the
// test advances between steps, so a real UPDATE would move updated_at; and
// schema-local AFTER INSERT/UPDATE triggers count the physical row writes, so
// the database itself records which statements changed a row.
//
// Every test runs on its own isolated PostgreSQL schema (openTestState) and
// fails rather than skips when the database is unreachable.

const (
	deployProgressNode = "node-progress"
	deployProgressApp  = "fn"
	deployInstFirst    = "inst-1"
	deployInstSecond   = "inst-2"
)

// deployProgressBase is the shared deterministic clock origin; each test pins
// it through pinDeploymentClock and advances it by a whole step between writes.
var deployProgressBase = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

// pinDeploymentClock stamps State.nowFn with a fixed instant. Tests re-pin it
// between steps so updated_at changes only when a write actually lands; the
// value is never read or written concurrently with a running write.
func pinDeploymentClock(t *testing.T, st *State, now time.Time) {
	t.Helper()
	st.nowFn = func() time.Time { return now }
}

// deployAppProgress builds one recorded progress payload with the shared test
// node/app identity, mirroring how production callers construct
// DeploymentAppProgress.
func deployAppProgress(
	generation int64,
	instance, phase, fingerprint, templateIdentity, commit, errText string,
) DeploymentAppProgress {
	return DeploymentAppProgress{
		NodeID:           deployProgressNode,
		App:              deployProgressApp,
		Generation:       generation,
		Instance:         instance,
		Phase:            phase,
		Fingerprint:      fingerprint,
		TemplateIdentity: templateIdentity,
		Commit:           commit,
		Error:            errText,
	}
}

// acquireDeployNode claims the shared test node at one generation and returns
// the incarnation that fences its writes.
func acquireDeployNode(
	t *testing.T, st *State, instance string, generation int64, commit string,
) DeploymentNodeProgress {
	t.Helper()
	node, err := st.AcquireDeploymentNode(context.Background(), deployProgressNode,
		instance, generation, DeploymentNodeProgress{
			NodeID: deployProgressNode, Generation: generation,
			Phase: DeploymentNodeMaterializing, Commit: commit,
		})
	if err != nil {
		t.Fatalf("acquire deployment node %q generation %d: %v",
			deployProgressNode, generation, err)
	}
	return node
}

// writeDeployApp performs one app-progress write and fails the test on an
// unexpected error: every write in these scenarios is fenced, never invalid.
func writeDeployApp(
	t *testing.T, st *State,
	generation, incarnation int64,
	progress DeploymentAppProgress,
) bool {
	t.Helper()
	landed, err := st.RecordDeploymentAppProgress(context.Background(),
		deployProgressNode, deployProgressApp, generation, incarnation, progress)
	if err != nil {
		t.Fatalf("RecordDeploymentAppProgress %q/%q generation %d: %v",
			deployProgressNode, deployProgressApp, generation, err)
	}
	return landed
}

// installDeploymentAppWriteCounters adds schema-local AFTER INSERT/UPDATE
// triggers on deployment_apps that count physical row writes. The counts let a
// test prove at the database level that a call performed no UPDATE (and created
// no row), independent of what the call reported. Cleanup rides the schema drop.
func installDeploymentAppWriteCounters(t *testing.T, st *State) {
	t.Helper()
	ctx := context.Background()
	stmts := []string{
		`CREATE TABLE deployment_app_write_counters (
			inserts BIGINT NOT NULL DEFAULT 0,
			updates BIGINT NOT NULL DEFAULT 0
		)`,
		`INSERT INTO deployment_app_write_counters DEFAULT VALUES`,
		`CREATE FUNCTION relay_test_count_deployment_app_insert() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
			UPDATE deployment_app_write_counters SET inserts = inserts + 1;
			RETURN NEW;
		 END;
		 $$`,
		`CREATE FUNCTION relay_test_count_deployment_app_update() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
			UPDATE deployment_app_write_counters SET updates = updates + 1;
			RETURN NEW;
		 END;
		 $$`,
		`CREATE TRIGGER relay_test_count_deployment_app_insert
		 AFTER INSERT ON deployment_apps
		 FOR EACH ROW EXECUTE FUNCTION relay_test_count_deployment_app_insert()`,
		`CREATE TRIGGER relay_test_count_deployment_app_update
		 AFTER UPDATE ON deployment_apps
		 FOR EACH ROW EXECUTE FUNCTION relay_test_count_deployment_app_update()`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("install deployment app write counters: %v", err)
		}
	}
}

// requireDeployAppWrites fails unless the database observed exactly the given
// number of physical inserts and updates on deployment_apps.
func requireDeployAppWrites(t *testing.T, st *State, inserts, updates int64) {
	t.Helper()
	var gotInserts, gotUpdates int64
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT inserts, updates FROM deployment_app_write_counters`,
	).Scan(&gotInserts, &gotUpdates); err != nil {
		t.Fatalf("read deployment app write counters: %v", err)
	}
	if gotInserts != inserts || gotUpdates != updates {
		t.Fatalf("physical writes = (%d inserts, %d updates), "+
			"want (%d inserts, %d updates)",
			gotInserts, gotUpdates, inserts, updates)
	}
}

// setDeploymentAppUpdateCounterEnabled suspends or resumes the AFTER UPDATE
// counter trigger. Fixture rewrites suspend it so the physical-write counts
// judge only the RecordDeploymentAppProgress statements under test.
func setDeploymentAppUpdateCounterEnabled(t *testing.T, st *State, enabled bool) {
	t.Helper()
	verb := "ENABLE"
	if !enabled {
		verb = "DISABLE"
	}
	if _, err := st.db.ExecContext(context.Background(),
		`ALTER TABLE deployment_apps `+verb+` TRIGGER `+
			`relay_test_count_deployment_app_update`); err != nil {
		t.Fatalf("%s deployment app update counter trigger: %v", verb, err)
	}
}

// rewriteDeploymentAppData replaces one row's stored payload with the given
// JSON text while the update counter trigger is suspended, so this fixture
// write never counts as a physical update of the statements under test. The
// trigger is resumed even when the rewrite fails.
func rewriteDeploymentAppData(t *testing.T, st *State, generation int64, data string) {
	t.Helper()
	setDeploymentAppUpdateCounterEnabled(t, st, false)
	defer setDeploymentAppUpdateCounterEnabled(t, st, true)
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE deployment_apps SET data = $1
		 WHERE node_id = $2 AND app_name = $3 AND generation = $4`,
		data, deployProgressNode, deployProgressApp, generation,
	); err != nil {
		t.Fatalf("rewrite stored deployment app payload: %v", err)
	}
}

// readRawDeploymentApp returns the stored instance, updated_at instant, and raw
// JSON payload for one (node, app, generation) row; a missing row fails.
func readRawDeploymentApp(
	t *testing.T, st *State, generation int64,
) (instance string, updatedAt time.Time, data string) {
	t.Helper()
	err := st.db.QueryRowContext(context.Background(),
		`SELECT instance, updated_at, data::text
		 FROM deployment_apps
		 WHERE node_id = $1 AND app_name = $2 AND generation = $3`,
		deployProgressNode, deployProgressApp, generation,
	).Scan(&instance, &updatedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deployment app %q/%q generation %d: row missing",
			deployProgressNode, deployProgressApp, generation)
	}
	if err != nil {
		t.Fatalf("read deployment app %q/%q generation %d: %v",
			deployProgressNode, deployProgressApp, generation, err)
	}
	return instance, updatedAt, data
}

// deploymentAppRowCount counts the shared test node's rows for the shared app
// across every generation.
func deploymentAppRowCount(t *testing.T, st *State) int {
	t.Helper()
	var count int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM deployment_apps
		 WHERE node_id = $1 AND app_name = $2`,
		deployProgressNode, deployProgressApp,
	).Scan(&count); err != nil {
		t.Fatalf("count deployment app rows: %v", err)
	}
	return count
}

// requireDeployAppRow fails unless the stored row for one generation carries
// the given instance, the given updated_at instant, and the payload fields of
// the given progress.
func requireDeployAppRow(
	t *testing.T, st *State, generation int64,
	wantInstance string, wantUpdatedAt time.Time, want DeploymentAppProgress,
) {
	t.Helper()
	instance, updatedAt, data := readRawDeploymentApp(t, st, generation)
	if instance != wantInstance {
		t.Fatalf("generation %d instance = %q, want %q",
			generation, instance, wantInstance)
	}
	if !updatedAt.Equal(wantUpdatedAt) {
		t.Fatalf("generation %d updated_at = %s, want %s (an untouched row "+
			"keeps the instant it was written)",
			generation, updatedAt, wantUpdatedAt)
	}
	var stored deploymentAppPayload
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		t.Fatalf("decode stored payload %q: %v", data, err)
	}
	if stored.Phase != want.Phase ||
		stored.Fingerprint != want.Fingerprint ||
		stored.TemplateIdentity != want.TemplateIdentity ||
		stored.Commit != want.Commit || stored.Error != want.Error {
		t.Fatalf("generation %d payload = phase %q fingerprint %q template %q "+
			"commit %q error %q, want phase %q fingerprint %q template %q "+
			"commit %q error %q",
			generation,
			stored.Phase, stored.Fingerprint, stored.TemplateIdentity,
			stored.Commit, stored.Error,
			want.Phase, want.Fingerprint, want.TemplateIdentity,
			want.Commit, want.Error)
	}
}

// TestRecordDeploymentAppProgressFirstWriteInsertsRow pins the initial insert:
// the first fenced write for a generation creates the row, reports true, and
// shows up through the read API with the stamped instance, payload, and clock
// instant.
func TestRecordDeploymentAppProgressFirstWriteInsertsRow(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	pinDeploymentClock(t, st, deployProgressBase)

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	progress := deployAppProgress(1, deployInstFirst,
		DeploymentAppPending, "fp-1", "tid-1", deployCommitA, "")

	if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); !landed {
		t.Fatal("first app progress = false, want true (the row was inserted)")
	}
	requireDeployAppWrites(t, st, 1, 0)
	requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progress)

	rows, err := st.ReadDeploymentAppProgress(
		context.Background(), deployProgressNode, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ReadDeploymentAppProgress = (%d rows, %v), want 1 row",
			len(rows), err)
	}
	row := rows[0]
	if row.Instance != deployInstFirst || row.Phase != DeploymentAppPending ||
		row.Fingerprint != "fp-1" || row.TemplateIdentity != "tid-1" ||
		row.Commit != deployCommitA {
		t.Fatalf("presented progress = %+v, want the recorded payload", row)
	}
	if row.UpdatedAt != deployProgressBase.Format(time.RFC3339) {
		t.Fatalf("presented updated_at = %q, want %q",
			row.UpdatedAt, deployProgressBase.Format(time.RFC3339))
	}
}

// TestRecordDeploymentAppProgressIdenticalRewriteIsNoOp pins the documented
// accepted no-op: repeating the exact same fenced write reports false and
// changes nothing — updated_at stays at its original instant and the database
// records no UPDATE, even though the clock advanced. A control write from the
// same incarnation reports true, so the false results came from the identical
// payload, not from a stale fence.
func TestRecordDeploymentAppProgressIdenticalRewriteIsNoOp(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	pinDeploymentClock(t, st, deployProgressBase)

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	progress := deployAppProgress(1, deployInstFirst,
		DeploymentAppPending, "fp-1", "tid-1", deployCommitA, "")

	if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); !landed {
		t.Fatal("first app progress = false, want true (the row was inserted)")
	}
	requireDeployAppWrites(t, st, 1, 0)

	// An identical rewrite under a later clock must not touch the row.
	pinDeploymentClock(t, st, deployProgressBase.Add(2*time.Second))
	if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); landed {
		t.Fatal("identical rewrite = true, want false (an accepted no-op)")
	}
	requireDeployAppWrites(t, st, 1, 0)
	requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progress)

	// Repeating it again stays a no-op rather than drifting updated_at.
	pinDeploymentClock(t, st, deployProgressBase.Add(5*time.Second))
	if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); landed {
		t.Fatal("second identical rewrite = true, want false")
	}
	requireDeployAppWrites(t, st, 1, 0)
	requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progress)

	// Control: the incarnation is still current, so the false results above
	// came from the identical payload, not from a stale fence.
	pinDeploymentClock(t, st, deployProgressBase.Add(8*time.Second))
	changed := progress
	changed.Phase = DeploymentAppActivating
	if landed := writeDeployApp(t, st, 1, node.Incarnation, changed); !landed {
		t.Fatal("changed write from the current incarnation = false, want true")
	}
	requireDeployAppWrites(t, st, 1, 1)
	requireDeployAppRow(t, st, 1, deployInstFirst,
		deployProgressBase.Add(8*time.Second), changed)
}

// TestRecordDeploymentAppProgressSemanticallyIdenticalJSONBIsNoOp pins that
// the no-op decision compares JSONB values, never serialized bytes: a row
// whose stored payload was written from a differently serialized but
// semantically identical document (other key order, extra whitespace) still
// reports false for the canonical payload, keeps its original updated_at, and
// performs zero physical UPDATEs.
func TestRecordDeploymentAppProgressSemanticallyIdenticalJSONBIsNoOp(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	pinDeploymentClock(t, st, deployProgressBase)
	ctx := context.Background()

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	progress := deployAppProgress(1, deployInstFirst,
		DeploymentAppPending, "fp-1", "tid-1", deployCommitA, "")

	// Given: a normal fenced insert of the canonical payload.
	if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); !landed {
		t.Fatal("first app progress = false, want true (the row was inserted)")
	}
	requireDeployAppWrites(t, st, 1, 0)

	// The same payload members emitted with a different key order and
	// whitespace: different bytes, one jsonb value.
	original, err := json.Marshal(deploymentAppPayload{
		Phase:            progress.Phase,
		Fingerprint:      progress.Fingerprint,
		TemplateIdentity: progress.TemplateIdentity,
		Commit:           progress.Commit,
		Error:            progress.Error,
	})
	if err != nil {
		t.Fatalf("marshal API payload: %v", err)
	}
	equivalent := fmt.Sprintf(`{
		"commit": %q,
		"template_identity": %q,
		"phase": %q,
		"fingerprint": %q
	}`, progress.Commit, progress.TemplateIdentity, progress.Phase,
		progress.Fingerprint)
	if string(equivalent) == string(original) {
		t.Fatal("equivalent payload must serialize differently from the " +
			"API payload (key order and whitespace)")
	}
	var sameJSONB bool
	if err := st.db.QueryRowContext(ctx,
		`SELECT $1::jsonb = $2::jsonb`, string(original), equivalent,
	).Scan(&sameJSONB); err != nil {
		t.Fatalf("compare payloads as jsonb: %v", err)
	}
	if !sameJSONB {
		t.Fatal("equivalent payload differs as jsonb from the API payload")
	}

	// Replace the stored payload with the equivalent text, so the row was
	// persisted from a different serialization than the API will send.
	rewriteDeploymentAppData(t, st, 1, equivalent)
	requireDeployAppWrites(t, st, 1, 0)

	// When: the canonical payload is written again under a later clock.
	pinDeploymentClock(t, st, deployProgressBase.Add(4*time.Second))
	if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); landed {
		t.Fatal("write of the semantically identical payload = true, " +
			"want false (JSONB equality is semantic)")
	}

	// Then: no physical UPDATE, no touched timestamp, no extra row.
	requireDeployAppWrites(t, st, 1, 0)
	requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progress)
	if count := deploymentAppRowCount(t, st); count != 1 {
		t.Fatalf("row count = %d, want 1", count)
	}

	// Control: the incarnation is still current, so the false above came from
	// payload identity, not a dead fence.
	pinDeploymentClock(t, st, deployProgressBase.Add(9*time.Second))
	changed := progress
	changed.Phase = DeploymentAppActivating
	if landed := writeDeployApp(t, st, 1, node.Incarnation, changed); !landed {
		t.Fatal("changed write = false, want true")
	}
	requireDeployAppWrites(t, st, 1, 1)
	requireDeployAppRow(t, st, 1, deployInstFirst,
		deployProgressBase.Add(9*time.Second), changed)
}

// TestRecordDeploymentAppProgressMeaningfulChangesUpdateRow pins that every
// meaningful payload field, and an instance-only transition, land as a real
// row change: true, a refreshed updated_at, exactly one database UPDATE, and
// all untouched fields preserved.
func TestRecordDeploymentAppProgressMeaningfulChangesUpdateRow(t *testing.T) {
	baseline := deployAppProgress(1, deployInstFirst,
		DeploymentAppActivating, "fp-1", "tid-1", deployCommitA, "")

	mutations := []struct {
		name   string
		mutate func(*DeploymentAppProgress)
	}{
		{"phase advances", func(p *DeploymentAppProgress) {
			p.Phase = DeploymentAppActive
		}},
		{"fingerprint changes", func(p *DeploymentAppProgress) {
			p.Fingerprint = "fp-2"
		}},
		{"template identity changes", func(p *DeploymentAppProgress) {
			p.TemplateIdentity = "tid-2"
		}},
		{"commit changes", func(p *DeploymentAppProgress) {
			p.Commit = deployCommitB
		}},
		{"error changes", func(p *DeploymentAppProgress) {
			p.Error = "activation failed"
		}},
		{"instance changes", func(p *DeploymentAppProgress) {
			p.Instance = deployInstSecond
		}},
	}

	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestState(t)
			installDeploymentAppWriteCounters(t, st)
			pinDeploymentClock(t, st, deployProgressBase)

			node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
			if landed := writeDeployApp(t, st, 1, node.Incarnation, baseline); !landed {
				t.Fatal("seed write = false, want true (the row was inserted)")
			}
			requireDeployAppWrites(t, st, 1, 0)

			want := baseline
			tc.mutate(&want)
			pinDeploymentClock(t, st, deployProgressBase.Add(3*time.Second))
			if landed := writeDeployApp(t, st, 1, node.Incarnation, want); !landed {
				t.Fatal("changed write = false, want true (a meaningful change)")
			}
			requireDeployAppWrites(t, st, 1, 1)
			requireDeployAppRow(t, st, 1, want.Instance,
				deployProgressBase.Add(3*time.Second), want)
		})
	}
}

// TestRecordDeploymentAppProgressNewGenerationAddsRow pins generation scoping:
// a write for a newly observed generation inserts its own row instead of
// editing the previous generation's row, which keeps the older row byte-for-
// byte intact (it still serves as the previous tree's authorization until
// pruning).
func TestRecordDeploymentAppProgressNewGenerationAddsRow(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	pinDeploymentClock(t, st, deployProgressBase)
	ctx := context.Background()

	first := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	progressGen1 := deployAppProgress(1, deployInstFirst,
		DeploymentAppActive, "fp-1", "tid-1", deployCommitA, "")
	if landed := writeDeployApp(t, st, 1, first.Incarnation, progressGen1); !landed {
		t.Fatal("generation-1 write = false, want true")
	}
	requireDeployAppWrites(t, st, 1, 0)

	// The node observes the next generation with a fresh incarnation.
	next := acquireDeployNode(t, st, deployInstFirst, 2, deployCommitB)
	if next.Incarnation <= first.Incarnation {
		t.Fatalf("re-acquired incarnation %d, want a fresh one after %d",
			next.Incarnation, first.Incarnation)
	}
	pinDeploymentClock(t, st, deployProgressBase.Add(4*time.Second))
	progressGen2 := deployAppProgress(2, deployInstFirst,
		DeploymentAppPending, "fp-2", "tid-2", deployCommitB, "")
	if landed := writeDeployApp(t, st, 2, next.Incarnation, progressGen2); !landed {
		t.Fatal("generation-2 write = false, want true (a new row)")
	}

	requireDeployAppWrites(t, st, 2, 0)
	if count := deploymentAppRowCount(t, st); count != 2 {
		t.Fatalf("row count = %d, want 2 (one row per generation)", count)
	}
	requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progressGen1)
	requireDeployAppRow(t, st, 2, deployInstFirst,
		deployProgressBase.Add(4*time.Second), progressGen2)

	rowsGen1, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(rowsGen1) != 1 || rowsGen1[0].Phase != DeploymentAppActive {
		t.Fatalf("generation-1 rows = (%+v, %v), want the untouched active row",
			rowsGen1, err)
	}
	rowsGen2, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 2)
	if err != nil || len(rowsGen2) != 1 || rowsGen2[0].Fingerprint != "fp-2" {
		t.Fatalf("generation-2 rows = (%+v, %v), want the new pending row",
			rowsGen2, err)
	}
}

// TestRecordDeploymentAppProgressStaleFenceMutatesNothing pins that a write
// refused by the fence changes nothing at all: a stale incarnation rewriting
// either fenced generation (observed or applied) with a different payload, and
// a current incarnation writing a generation the node has neither observed nor
// applied, all report false, insert no row, update no row, and leave both
// stored rows byte-for-byte untouched.
func TestRecordDeploymentAppProgressStaleFenceMutatesNothing(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	pinDeploymentClock(t, st, deployProgressBase)
	ctx := context.Background()

	// A first process incarnation applies generation 1, then goes stale when
	// the node takes up generation 2 under a new incarnation.
	stale := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	if landed, err := st.RecordDeploymentNodeProgress(ctx, deployProgressNode,
		1, stale.Incarnation, DeploymentNodeProgress{
			NodeID: deployProgressNode, Generation: 1,
			AppliedGeneration: 1, AppliedIncarnation: stale.Incarnation,
			Phase: DeploymentNodeMaterialized, Commit: deployCommitA,
			AppliedCommit: deployCommitA,
		}); err != nil || !landed {
		t.Fatalf("record applied generation = (%v, %v), want landed", landed, err)
	}
	current := acquireDeployNode(t, st, deployInstSecond, 2, deployCommitB)
	if current.Incarnation <= stale.Incarnation {
		t.Fatalf("re-acquired incarnation %d, want a fresh one after %d",
			current.Incarnation, stale.Incarnation)
	}
	if current.AppliedGeneration != 1 {
		t.Fatalf("applied generation after re-acquire = %d, want 1 preserved",
			current.AppliedGeneration)
	}

	// Both fenced rows exist: generation 2 via the observed generation and
	// generation 1 via the applied generation.
	progressGen1 := deployAppProgress(1, deployInstSecond,
		DeploymentAppActive, "fp-1", "tid-1", deployCommitA, "")
	progressGen2 := deployAppProgress(2, deployInstSecond,
		DeploymentAppPending, "fp-2", "tid-2", deployCommitB, "")
	if landed := writeDeployApp(t, st, 1, current.Incarnation, progressGen1); !landed {
		t.Fatal("applied-generation write = false, want true")
	}
	if landed := writeDeployApp(t, st, 2, current.Incarnation, progressGen2); !landed {
		t.Fatal("observed-generation write = false, want true")
	}
	requireDeployAppWrites(t, st, 2, 0)

	// Even a byte-for-byte identical rewrite from the stale incarnation must
	// not touch the row: same instance, same payload as persisted, yet no
	// update, no reclaim, and updated_at keeps the insert instant while the
	// clock moves on.
	pinDeploymentClock(t, st, deployProgressBase.Add(4*time.Second))
	if landed := writeDeployApp(t, st, 2, stale.Incarnation, progressGen2); landed {
		t.Fatal("stale-incarnation rewrite identical to the stored row = " +
			"true, want false")
	}
	requireDeployAppWrites(t, st, 2, 0)
	requireDeployAppRow(t, st, 2, deployInstSecond, deployProgressBase, progressGen2)

	// Every changed-payload refused write carries a payload that WOULD change
	// the stored row, so a false result can only come from the fence.
	pinDeploymentClock(t, st, deployProgressBase.Add(6*time.Second))
	stalePayload := deployAppProgress(2, deployInstFirst,
		DeploymentAppFailed, "fp-9", "tid-9", deployCommitB, "fetch failed")
	if landed := writeDeployApp(t, st, 2, stale.Incarnation, stalePayload); landed {
		t.Fatal("stale-incarnation write for the observed generation = true, " +
			"want false")
	}
	stalePayload.Generation = 1
	stalePayload.Commit = deployCommitA
	if landed := writeDeployApp(t, st, 1, stale.Incarnation, stalePayload); landed {
		t.Fatal("stale-incarnation write for the applied generation = true, " +
			"want false")
	}
	foreign := deployAppProgress(7, deployInstSecond,
		DeploymentAppActive, "fp-7", "tid-7", deployCommitC, "")
	if landed := writeDeployApp(t, st, 7, current.Incarnation, foreign); landed {
		t.Fatal("write for a foreign generation = true, want false")
	}

	requireDeployAppWrites(t, st, 2, 0)
	if count := deploymentAppRowCount(t, st); count != 2 {
		t.Fatalf("row count = %d, want 2 (a refused write created no row)", count)
	}
	requireDeployAppRow(t, st, 1, deployInstSecond, deployProgressBase, progressGen1)
	requireDeployAppRow(t, st, 2, deployInstSecond, deployProgressBase, progressGen2)
}

// TestRecordDeploymentAppProgressConcurrentWriters pins the write contract
// under real concurrency: identical racing writers collapse to a single insert
// (exactly one reports true, the rest are accepted no-ops), identical writers
// against an existing row all report false without a single UPDATE, and
// conflicting writers each land their own payload with no torn row.
func TestRecordDeploymentAppProgressConcurrentWriters(t *testing.T) {
	const writers = 8

	runWriters := func(
		t *testing.T, st *State, incarnation int64, progress []DeploymentAppProgress,
	) []bool {
		t.Helper()
		results := make([]bool, len(progress))
		errs := make([]error, len(progress))
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range progress {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i], errs[i] = st.RecordDeploymentAppProgress(
					context.Background(), deployProgressNode, deployProgressApp,
					progress[i].Generation, incarnation, progress[i])
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("writer %d: %v", i, err)
			}
		}
		return results
	}

	t.Run("identical writers collapse to one insert", func(t *testing.T) {
		st := openTestState(t)
		installDeploymentAppWriteCounters(t, st)
		pinDeploymentClock(t, st, deployProgressBase)
		node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
		progress := deployAppProgress(1, deployInstFirst,
			DeploymentAppPending, "fp-1", "tid-1", deployCommitA, "")
		racing := make([]DeploymentAppProgress, writers)
		for i := range racing {
			racing[i] = progress
		}

		results := runWriters(t, st, node.Incarnation, racing)

		written := 0
		for _, landed := range results {
			if landed {
				written++
			}
		}
		if written != 1 {
			t.Fatalf("true results = %d, want exactly 1 (identical racing "+
				"writers collapse to one insert)", written)
		}
		requireDeployAppWrites(t, st, 1, 0)
		requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progress)
	})

	t.Run("identical writers against an existing row all report false",
		func(t *testing.T) {
			st := openTestState(t)
			installDeploymentAppWriteCounters(t, st)
			pinDeploymentClock(t, st, deployProgressBase)
			node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
			progress := deployAppProgress(1, deployInstFirst,
				DeploymentAppActive, "fp-1", "tid-1", deployCommitA, "")
			if landed := writeDeployApp(t, st, 1, node.Incarnation, progress); !landed {
				t.Fatal("seed write = false, want true")
			}

			pinDeploymentClock(t, st, deployProgressBase.Add(7*time.Second))
			racing := make([]DeploymentAppProgress, writers)
			for i := range racing {
				racing[i] = progress
			}

			results := runWriters(t, st, node.Incarnation, racing)

			for i, landed := range results {
				if landed {
					t.Fatalf("writer %d = true, want false (an identical "+
						"rewrite is an accepted no-op)", i)
				}
			}
			requireDeployAppWrites(t, st, 1, 0)
			requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase,
				progress)
		})

	t.Run("conflicting writers each land their own payload",
		func(t *testing.T) {
			st := openTestState(t)
			installDeploymentAppWriteCounters(t, st)
			pinDeploymentClock(t, st, deployProgressBase)
			node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)

			phases := []string{
				DeploymentAppPending, DeploymentAppActivating,
				DeploymentAppActive, DeploymentAppDegraded,
			}
			conflicting := make([]DeploymentAppProgress, len(phases))
			wantByInstance := make(map[string]DeploymentAppProgress, len(phases))
			for i, phase := range phases {
				instance := fmt.Sprintf("inst-%d", i)
				progress := deployAppProgress(1, instance, phase,
					fmt.Sprintf("fp-%d", i), fmt.Sprintf("tid-%d", i),
					deployCommitA, "")
				conflicting[i] = progress
				wantByInstance[instance] = progress
			}

			results := runWriters(t, st, node.Incarnation, conflicting)

			for i, landed := range results {
				if !landed {
					t.Fatalf("writer %d = false, want true (its instance and "+
						"payload differ from every stored state)", i)
				}
			}
			// One insert and one update per losing writer: no write is lost.
			requireDeployAppWrites(t, st, 1, int64(len(phases)-1))
			if count := deploymentAppRowCount(t, st); count != 1 {
				t.Fatalf("row count = %d, want 1 (one contested row)", count)
			}

			instance, updatedAt, data := readRawDeploymentApp(t, st, 1)
			want, ok := wantByInstance[instance]
			if !ok {
				t.Fatalf("stored instance = %q, want one of the writer instances",
					instance)
			}
			if !updatedAt.Equal(deployProgressBase) {
				t.Fatalf("updated_at = %s, want %s",
					updatedAt, deployProgressBase)
			}
			var stored deploymentAppPayload
			if err := json.Unmarshal([]byte(data), &stored); err != nil {
				t.Fatalf("decode stored payload %q: %v", data, err)
			}
			// The instance and payload must belong to the same writer: a single
			// UPDATE writes both columns, so a torn row is impossible.
			if stored.Phase != want.Phase ||
				stored.Fingerprint != want.Fingerprint ||
				stored.TemplateIdentity != want.TemplateIdentity {
				t.Fatalf("instance %q carries payload phase %q fingerprint %q "+
					"template %q, want its own writer's payload phase %q "+
					"fingerprint %q template %q",
					instance, stored.Phase, stored.Fingerprint,
					stored.TemplateIdentity,
					want.Phase, want.Fingerprint, want.TemplateIdentity)
			}
		})
}

// TestRecordDeploymentAppProgressRecoversInstanceAfterRestart pins restart
// recovery: after the node re-acquires its generation under a new process
// instance, the superseded incarnation cannot mutate the stored row, the new
// incarnation takes the row over with an instance-only transition (true), and
// repeating its own identical write becomes an accepted no-op again.
func TestRecordDeploymentAppProgressRecoversInstanceAfterRestart(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	pinDeploymentClock(t, st, deployProgressBase)

	first := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	progress := deployAppProgress(1, deployInstFirst,
		DeploymentAppPending, "fp-1", "tid-1", deployCommitA, "")
	if landed := writeDeployApp(t, st, 1, first.Incarnation, progress); !landed {
		t.Fatal("write before restart = false, want true")
	}
	requireDeployAppWrites(t, st, 1, 0)

	// Restart: the same node re-acquires the same generation with a fresh
	// incarnation and its own instance identity.
	restarted := acquireDeployNode(t, st, deployInstSecond, 1, deployCommitA)
	if restarted.Incarnation <= first.Incarnation {
		t.Fatalf("restarted incarnation %d, want a fresh one after %d",
			restarted.Incarnation, first.Incarnation)
	}

	// The superseded process cannot mutate the row even with a payload that
	// would change it.
	pinDeploymentClock(t, st, deployProgressBase.Add(2*time.Second))
	stale := deployAppProgress(1, deployInstFirst,
		DeploymentAppFailed, "fp-9", "tid-9", deployCommitB, "fetch failed")
	if landed := writeDeployApp(t, st, 1, first.Incarnation, stale); landed {
		t.Fatal("superseded incarnation write = true, want false")
	}
	requireDeployAppWrites(t, st, 1, 0)
	requireDeployAppRow(t, st, 1, deployInstFirst, deployProgressBase, progress)

	// The recovered process takes the row over: identical payload, its own
	// instance — an instance-only transition lands as a real change.
	recovered := progress
	recovered.Instance = deployInstSecond
	if landed := writeDeployApp(t, st, 1, restarted.Incarnation, recovered); !landed {
		t.Fatal("instance-only recovery write = false, want true")
	}
	requireDeployAppWrites(t, st, 1, 1)
	requireDeployAppRow(t, st, 1, deployInstSecond,
		deployProgressBase.Add(2*time.Second), recovered)

	// From here the recovered process's own repeats are accepted no-ops.
	pinDeploymentClock(t, st, deployProgressBase.Add(5*time.Second))
	if landed := writeDeployApp(t, st, 1, restarted.Incarnation, recovered); landed {
		t.Fatal("identical write after recovery = true, want false")
	}
	requireDeployAppWrites(t, st, 1, 1)
	requireDeployAppRow(t, st, 1, deployInstSecond,
		deployProgressBase.Add(2*time.Second), recovered)
}
