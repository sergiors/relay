//go:build integration

package worker

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/deployment"
	"relay/internal/state"
	"relay/internal/testutil"
)

const (
	listenerCommitOne   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	listenerCommitTwo   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	listenerCommitThree = "cccccccccccccccccccccccccccccccccccccccc"
	listenerCommitFour  = "dddddddddddddddddddddddddddddddddddddddd"
	listenerRepo        = "git@github.com:acme/listen.git"
)

// publishListenerRevision publishes one operator revision and fails unless it
// advanced the shared generation.
func publishListenerRevision(t *testing.T, st *state.State, commit string) {
	t.Helper()
	result, err := st.PublishDeploymentDesired(context.Background(),
		state.DeploymentPublishRequest{
			Desired: state.DeploymentDesired{
				Commit:     commit,
				Repository: listenerRepo,
				Ref:        "main",
				Publisher:  "test",
			},
			ExpectedGeneration: -1,
			AllowPinned:        true,
		})
	if err != nil || !result.Published {
		t.Fatalf("publish = (%+v, %v), want a published generation", result, err)
	}
}

// TestDeploymentListenerFailureIsNonfatal pins the listener-is-only-an-
// optimization contract: a State handle the supervisor cannot subscribe on
// makes every attempt fail, but the supervisor reports the failure, stays
// alive retrying (the coordinator's fallback polling remains the convergence
// path), and ends only when the worker lifecycle does. Nothing on this path
// can cancel or fail the worker.
func TestDeploymentListenerFailureIsNonfatal(t *testing.T) {
	st := testutil.NewState(t)
	logger, logs := captureStartupLog()
	// Closing the handle makes every subscription attempt fail.
	if err := st.Close(); err != nil {
		t.Fatalf("close state: %v", err)
	}
	coordinator := deployment.New(deployment.Config{
		State:         st,
		NodeID:        "node-listen-fail",
		InstanceID:    "inst-listen-fail",
		PollInterval:  time.Hour,
		RetryInterval: time.Hour,
		DriftInterval: time.Hour,
		Logger:        logger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	supervisorDone := make(chan struct{})
	go func() {
		defer close(supervisorDone)
		superviseDeploymentListener(ctx, logger, st, coordinator)
	}()

	testutil.WaitFor(t, 5*time.Second, "subscription failure report", func() bool {
		return strings.Contains(logs.String(), "subscription failed")
	})
	// The failure must not terminate the supervisor: it is inside its backoff
	// and still retrying.
	select {
	case <-supervisorDone:
		t.Fatal("the listener supervisor exited on a subscription failure")
	case <-time.After(300 * time.Millisecond):
	}
	// The lifecycle is the only thing that ends it — and it stops promptly.
	cancel()
	select {
	case <-supervisorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the listener supervisor did not stop with the lifecycle")
	}
}

// TestDeploymentListenerCatchUpHintWakeAndReconnect is the supervisor's
// end-to-end contract on a real PostgreSQL: it subscribes and issues one
// normal catch-up wake, delivered hints converge a published generation
// without the (disabled) fallback poll, a session killed mid-flight is
// re-LISTENed after bounded backoff with a fresh catch-up wake that converges
// a revision committed entirely while offline, hints work again on the new
// session, and the registered deployment-listen barrier joins the supervisor
// once the lifecycle ends.
func TestDeploymentListenerCatchUpHintWakeAndReconnect(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	// A per-test application name (<= 63 bytes, enforced by Unique) scopes the
	// reconnect trigger to THIS test's listener session, so a parallel suite's
	// LISTEN sessions can never be terminated by mistake.
	appName := testutil.Unique("relay-lstn", t.Name())
	dsn += "&application_name=" + appName
	st := testutil.OpenState(t, dsn)
	logger, logs := captureStartupLog()

	cfg := deployment.Config{
		State:      st,
		NodeID:     "node-listen",
		InstanceID: "inst-listen",
		// No local source file: each materialization attempt fails fast AFTER
		// the node-progress claim, so attempts stay observable.
		GitConfigPath: filepath.Join(t.TempDir(), "source.json"),
		AppsDir:       t.TempDir(),
		PollInterval:  time.Hour,
		RetryInterval: time.Hour,
		DriftInterval: time.Hour,
		Logger:        logger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	nodeProgress := func() (state.DeploymentNodeProgress, bool) {
		node, found, err := st.ReadDeploymentNode(ctx, "node-listen")
		if err != nil || !found {
			return state.DeploymentNodeProgress{}, false
		}
		return node, true
	}
	waitNode := func(what string, pred func(state.DeploymentNodeProgress) bool) {
		t.Helper()
		testutil.WaitFor(t, 10*time.Second, what, func() bool {
			node, found := nodeProgress()
			return found && pred(node)
		})
	}

	// A revision already exists when this node starts.
	publishListenerRevision(t, st, listenerCommitOne)
	coordinator := deployment.New(cfg)
	coordinatorDone := coordinator.Start(ctx, nil, nil)

	// The startup pass attempts the published revision before any listener
	// exists, so its claim is the untouched baseline.
	testutil.WaitFor(t, 5*time.Second, "startup materialization attempt", func() bool {
		return strings.Contains(logs.String(), "materialization failed")
	})
	baseline, found := nodeProgress()
	if !found {
		t.Fatal("startup attempt left no node progress row")
	}

	reg := &shutdownRegistry{}
	startDeploymentListener(ctx, logger, st, coordinator, reg)
	t.Cleanup(func() {
		cancel()
		select {
		case <-coordinatorDone:
		case <-time.After(5 * time.Second):
		}
	})

	// It subscribes...
	testutil.WaitFor(t, 5*time.Second, "subscription", func() bool {
		return strings.Contains(logs.String(),
			"subscribed to desired revision notifications")
	})
	// ...and immediately catches up with a fresh desired-state check: poll,
	// retry, and drift are an hour away, so ONLY the post-subscription wake
	// can run this next pass.
	waitNode("post-subscription catch-up wake", func(n state.DeploymentNodeProgress) bool {
		return n.Incarnation > baseline.Incarnation
	})

	// A delivered hint converges an online publication without the poll.
	publishListenerRevision(t, st, listenerCommitTwo)
	waitNode("hint-driven generation 2", func(n state.DeploymentNodeProgress) bool {
		return n.Generation == 2
	})

	// Kill the subscribed backend, scoped precisely by this test's
	// application name plus the LISTEN statement still visible as its last
	// query.
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	defer admin.Close()
	var listenerPID int32
	if err := admin.QueryRowContext(ctx,
		`SELECT pid FROM pg_stat_activity
		 WHERE datname = current_database()
		   AND application_name = $1
		   AND query LIKE 'LISTEN%'`, appName).Scan(&listenerPID); err != nil {
		t.Fatalf("locate the listener backend by application_name: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		"SELECT pg_terminate_backend($1)", listenerPID); err != nil {
		t.Fatalf("terminate listener backend: %v", err)
	}

	// The supervisor observes the drop and enters its bounded backoff...
	testutil.WaitFor(t, 5*time.Second, "session-loss report", func() bool {
		return strings.Contains(logs.String(), "subscription ended; reconnecting")
	})
	// ...during which a revision commits with NO session subscribed (its hint
	// can never arrive)...
	publishListenerRevision(t, st, listenerCommitThree)
	// ...then the supervisor re-LISTENs...
	testutil.WaitFor(t, 10*time.Second, "re-subscription", func() bool {
		return strings.Count(logs.String(),
			"subscribed to desired revision notifications") >= 2
	})
	// ...and its catch-up wake converges the commit made while offline.
	waitNode("offline commit converged after re-LISTEN",
		func(n state.DeploymentNodeProgress) bool {
			return n.Generation == 3
		})

	// The re-LISTENed session receives hints again.
	publishListenerRevision(t, st, listenerCommitFour)
	waitNode("hint after re-LISTEN", func(n state.DeploymentNodeProgress) bool {
		return n.Generation == 4
	})

	// Shutdown: the lifecycle ends the supervisor, and the registered
	// deployment-listen barrier joins it (a wedged join fails here).
	cancel()
	regDone := make(chan struct{})
	go func() {
		defer close(regDone)
		reg.run(discardLogger())
	}()
	select {
	case <-regDone:
	case <-time.After(10 * time.Second):
		t.Fatal("deployment-listen barrier did not join the supervisor after shutdown")
	}
}
