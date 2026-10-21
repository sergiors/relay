package state

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"relay/internal/app"
)

// snapshotApp returns the persisted detail and fails when the row is absent.
func snapshotApp(t *testing.T, c *State, name string) Detail {
	t.Helper()
	detail, ok := c.GetApp(name)
	if !ok {
		t.Fatalf("expected a persisted row for %q", name)
	}
	return detail
}

func assertGenerationState(
	t *testing.T,
	c *State,
	name string,
	want Detail,
) {
	t.Helper()
	got := snapshotApp(t, c, name)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("generation-guarded write changed state:\n got: %+v\nwant: %+v",
			got, want)
	}
}

// TestStaleGenerationWritesAreRejected is the state-level cross-worker fence:
// once a newer desired generation has advanced the token, every completion
// carrying the older token must update no row and publish no status, including
// success, failure, building, reconciling, and service failure.
func TestStaleGenerationWritesAreRejected(t *testing.T) {
	operations := []struct {
		name  string
		apply func(*State, appFixture, Token) (bool, error)
	}{
		{
			name: "success",
			apply: func(c *State, fx appFixture, token Token) (bool, error) {
				return c.RecordReconcileSuccess(context.Background(),
					fx.name, "img-stale", "fp-stale", time.Now(), fx.app, token)
			},
		},
		{
			name: "failure",
			apply: func(c *State, fx appFixture, token Token) (bool, error) {
				return c.RecordReconcileFailure(context.Background(),
					fx.name, errors.New("stale failure"), token)
			},
		},
		{
			name: "building",
			apply: func(c *State, fx appFixture, token Token) (bool, error) {
				return c.RecordReconcileBuilding(context.Background(), fx.name, token)
			},
		},
		{
			name: "reconciling",
			apply: func(c *State, fx appFixture, token Token) (bool, error) {
				return c.RecordReconciling(context.Background(), fx.name, token)
			},
		},
		{
			name: "service failure",
			apply: func(c *State, fx appFixture, token Token) (bool, error) {
				return c.RecordServiceFailure(context.Background(),
					fx.name, errors.New("stale service failure"), token)
			},
		},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			c := openTestState(t)
			fx := newAppFixture(t, "stale-"+op.name)
			prepared := time.Now().Add(-time.Hour)

			old := seedTokenWithFingerprint(t, c, fx.app, "fp-old")
			if !requireWrite(t, fx.name, func() (bool, error) {
				return c.RecordReconcileSuccess(context.Background(),
					fx.name, "img-old", "fp-old", prepared, fx.app, old)
			}) {
				t.Fatal("precondition: old success did not persist")
			}

			// A newer worker advances the desired generation. Preparing must
			// preserve the still-serving active generation.
			next := requireDesired(t, fx.name, func() (Token, error) {
				return c.RecordPreparingWithFingerprint(context.Background(), fx.app.Name, fx.app, "fp-next")
			})
			afterPreparing := snapshotApp(t, c, fx.name)
			if afterPreparing.Status != StatusPreparing {
				t.Fatalf("status = %q, want preparing", afterPreparing.Status)
			}
			if afterPreparing.Image != "img-old" ||
				afterPreparing.Fingerprint != "fp-old" {
				t.Fatalf("active generation = %q/%q, want preserved img-old/fp-old",
					afterPreparing.Image, afterPreparing.Fingerprint)
			}
			if afterPreparing.DesiredFingerprint != "fp-next" {
				t.Fatalf("desired_fingerprint = %q, want fp-next",
					afterPreparing.DesiredFingerprint)
			}

			observer, events := recordingObserver()
			c.SetStatusObserver(observer)
			*events = nil

			landed, err := op.apply(c, fx, old)
			if err != nil {
				t.Fatalf("stale generation write returned error: %v", err)
			}
			if landed {
				t.Fatal("stale generation write reported success")
			}
			assertGenerationState(t, c, fx.name, afterPreparing)
			if len(*events) != 0 {
				t.Fatalf("stale write published status events: %+v", *events)
			}

			// The current token still owns the row: a real completion lands.
			if !requireWrite(t, fx.name, func() (bool, error) {
				return c.RecordReconcileSuccess(context.Background(),
					fx.name, "img-next", "fp-next", time.Now(), fx.app, next)
			}) {
				t.Fatal("current success did not persist")
			}
			if got := snapshotApp(t, c, fx.name); got.Status != StatusReady ||
				got.Image != "img-next" || got.Fingerprint != "fp-next" {
				t.Fatalf("current outcome = %+v, want ready img-next/fp-next", got)
			}
		})
	}
}

// TestCurrentGenerationWritesPersist keeps the acceptance controls alongside
// the stale rejection: each generation-specific write must still land when the
// caller carries the token of the current desired generation.
func TestCurrentGenerationWritesPersist(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := openTestState(t)
		fx := newAppFixture(t, "current-success")
		token := seedTokenWithFingerprint(t, c, fx.app, "fp-v1")

		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordReconcileSuccess(context.Background(),
				fx.name, "img-v1", "fp-v1", time.Now(), fx.app, token)
		}) {
			t.Fatal("current success did not persist")
		}
		got := snapshotApp(t, c, fx.name)
		if got.Status != StatusReady || got.Image != "img-v1" ||
			got.Fingerprint != "fp-v1" || got.DesiredFingerprint != "fp-v1" {
			t.Fatalf("success state = %+v, want ready img-v1/fp-v1", got)
		}
	})

	t.Run("failure preserves active image", func(t *testing.T) {
		c := openTestState(t)
		fx := newAppFixture(t, "current-failure")
		prepared := time.Now().Add(-time.Hour)
		old := seedTokenWithFingerprint(t, c, fx.app, "fp-old")
		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordReconcileSuccess(context.Background(),
				fx.name, "img-active", "fp-active", prepared, fx.app, old)
		}) {
			t.Fatal("precondition: active success did not persist")
		}
		next := requireDesired(t, fx.name, func() (Token, error) {
			return c.RecordPreparingWithFingerprint(context.Background(), fx.app.Name, fx.app, "fp-next")
		})

		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordReconcileFailure(context.Background(),
				fx.name, errors.New("current build failed"), next)
		}) {
			t.Fatal("current failure did not persist")
		}
		got := snapshotApp(t, c, fx.name)
		if got.Status != StatusDegraded {
			t.Fatalf("status = %q, want degraded", got.Status)
		}
		if got.Image != "img-active" || got.Fingerprint != "fp-active" {
			t.Fatalf("active generation = %q/%q, want preserved img-active/fp-active",
				got.Image, got.Fingerprint)
		}
		if got.PreparedAt != prepared.UTC().Format(time.RFC3339) {
			t.Fatalf("prepared_at = %q, want preserved %q",
				got.PreparedAt, prepared.UTC().Format(time.RFC3339))
		}
		if got.LastReconcileStatus != ReconcileFailed ||
			got.LastError != "current build failed" {
			t.Fatalf("outcome = %q/%q, want failed/current build failed",
				got.LastReconcileStatus, got.LastError)
		}
	})

	t.Run("building and reconciling", func(t *testing.T) {
		c := openTestState(t)
		fx := newAppFixture(t, "current-status")
		token := seedToken(t, c, fx.app)

		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordReconcileBuilding(context.Background(), fx.name, token)
		}) {
			t.Fatal("current building status did not persist")
		}
		if got := snapshotApp(t, c, fx.name); got.Status != StatusBuilding {
			t.Fatalf("status = %q, want building", got.Status)
		}
		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordReconciling(context.Background(), fx.name, token)
		}) {
			t.Fatal("current reconciling status did not persist")
		}
		if got := snapshotApp(t, c, fx.name); got.Status != StatusReconciling {
			t.Fatalf("status = %q, want reconciling", got.Status)
		}
	})

	t.Run("service failure preserves active image", func(t *testing.T) {
		c := openTestState(t)
		fx := newAppFixture(t, "current-service-failure")
		token := seedToken(t, c, fx.app)
		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordReconcileSuccess(context.Background(),
				fx.name, "img-active", "fp-active", time.Now(), fx.app, token)
		}) {
			t.Fatal("precondition: active success did not persist")
		}

		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordServiceFailure(context.Background(),
				fx.name, errors.New("current service failed"), token)
		}) {
			t.Fatal("current service failure did not persist")
		}
		got := snapshotApp(t, c, fx.name)
		if got.Status != StatusDegraded || got.Image != "img-active" ||
			got.Fingerprint != "fp-active" {
			t.Fatalf("service failure state = %+v, want degraded img-active/fp-active",
				got)
		}
	})

	t.Run("removal deletes row and stats", func(t *testing.T) {
		c := openTestState(t)
		fx := newAppFixture(t, "current-removal")
		token := seedToken(t, c, fx.app)
		c.RecordAppStats(AppStats{App: fx.name, EventsMatchedTotal: 3})

		if !requireWrite(t, fx.name, func() (bool, error) {
			return c.RecordRemoved(context.Background(), fx.name, token)
		}) {
			t.Fatal("current removal did not delete the row")
		}
		if _, ok := c.GetApp(fx.name); ok {
			t.Fatal("apps row must be gone after a current removal")
		}
		if _, ok := c.AppStats(fx.name); ok {
			t.Fatal("app_stats row must be gone after a current removal")
		}
	})
}

// TestDesiredABAStillRejectsStaleToken proves the token, not the desired
// fingerprint text, is the fence: a desired A -> B -> A sequence allocates a
// fresh token for the second A, so A's original completion is still stale even
// though the desired fingerprint matches again.
func TestDesiredABAStillRejectsStaleToken(t *testing.T) {
	c := openTestState(t)
	fx := newAppFixture(t, "aba")

	tokenA := seedTokenWithFingerprint(t, c, fx.app, "fp-A")
	tokenB := requireDesired(t, fx.name, func() (Token, error) {
		return c.RecordPreparingWithFingerprint(context.Background(), fx.app.Name, fx.app, "fp-B")
	})
	tokenA2 := requireDesired(t, fx.name, func() (Token, error) {
		return c.RecordPreparingWithFingerprint(context.Background(), fx.app.Name, fx.app, "fp-A")
	})
	if tokenA == tokenA2 || tokenB == tokenA2 {
		t.Fatalf("tokens not freshly allocated: A=%d B=%d A2=%d",
			tokenA, tokenB, tokenA2)
	}

	before := snapshotApp(t, c, fx.name)
	if requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordReconcileSuccess(context.Background(),
			fx.name, "img-A", "fp-A", time.Now(), fx.app, tokenA)
	}) {
		t.Fatal("stale A completion persisted after A->B->A")
	}
	assertGenerationState(t, c, fx.name, before)

	if !requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordReconcileSuccess(context.Background(),
			fx.name, "img-A2", "fp-A", time.Now(), fx.app, tokenA2)
	}) {
		t.Fatal("current A2 completion did not persist")
	}
	got := snapshotApp(t, c, fx.name)
	if got.Status != StatusReady || got.Image != "img-A2" {
		t.Fatalf("current A2 state = %+v, want ready img-A2", got)
	}
}

// TestStaleRemovalCannotDeleteReaddedApp proves removal is fenced too: after a
// newer generation re-adds an app, an older worker's removal must not delete the
// new apps row or its app_stats row.
func TestStaleRemovalCannotDeleteReaddedApp(t *testing.T) {
	c := openTestState(t)
	fx := newAppFixture(t, "readd")

	old := seedToken(t, c, fx.app)
	if !requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordRemoved(context.Background(), fx.name, old)
	}) {
		t.Fatal("precondition: current removal did not delete the row")
	}

	// A newer generation re-adds the app and creates its stats row.
	fresh := seedToken(t, c, fx.app)
	c.RecordAppStats(AppStats{App: fx.name, EventsMatchedTotal: 12})
	before := snapshotApp(t, c, fx.name)

	observer, events := recordingObserver()
	c.SetStatusObserver(observer)
	*events = nil

	if requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordRemoved(context.Background(), fx.name, old)
	}) {
		t.Fatal("stale removal deleted the re-added row")
	}
	assertGenerationState(t, c, fx.name, before)
	if len(*events) != 0 {
		t.Fatalf("stale removal published status events: %+v", *events)
	}
	if stats, ok := c.AppStats(fx.name); !ok || stats.EventsMatchedTotal != 12 {
		t.Fatalf("stale removal changed app_stats: %+v ok=%v", stats, ok)
	}

	// A stale success from before the removal must not resurrect or overwrite.
	if requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordReconcileSuccess(context.Background(),
			fx.name, "img-old", "fp-old", time.Now(), fx.app, old)
	}) {
		t.Fatal("stale success from before removal persisted")
	}
	assertGenerationState(t, c, fx.name, before)
	if len(*events) != 0 {
		t.Fatalf("stale success published status events: %+v", *events)
	}

	// The fresh generation still owns the row.
	if !requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordReconcileSuccess(context.Background(),
			fx.name, "img-fresh", "fp-fresh", time.Now(), fx.app, fresh)
	}) {
		t.Fatal("fresh success did not persist")
	}
	if got := snapshotApp(t, c, fx.name); got.Image != "img-fresh" {
		t.Fatalf("image = %q, want img-fresh", got.Image)
	}
}

// TestDesiredWritesAllocateFreshTokens pins token allocation: every
// desired-generation write (discovery, preparing, invalid desired) returns a
// new positive token, strictly increasing within the schema, so two consecutive
// writes can never share an identity even when their desired fingerprint text
// repeats.
func TestDesiredWritesAllocateFreshTokens(t *testing.T) {
	c := openTestState(t)
	fx := newAppFixture(t, "tokens")

	discovered := seedTokenWithFingerprint(t, c, fx.app, "fp-1")
	preparing := requireDesired(t, fx.name, func() (Token, error) {
		return c.RecordPreparingWithFingerprint(context.Background(), fx.name, fx.app, "fp-1")
	})
	invalid := requireDesired(t, fx.name, func() (Token, error) {
		return c.RecordInvalidDesired(context.Background(), fx.name, errors.New("invalid template"))
	})

	if discovered >= preparing || preparing >= invalid {
		t.Fatalf("tokens not strictly increasing: discovered=%d preparing=%d invalid=%d",
			discovered, preparing, invalid)
	}
	if invalid <= 0 {
		t.Fatalf("invalid desired token = %d, want positive", invalid)
	}
}

// TestZeroTokenWritesNeverPersist pins the fail-closed contract: a zero token
// means the desired-generation write failed, so no generation-specific write
// may touch the row or publish a status.
func TestZeroTokenWritesNeverPersist(t *testing.T) {
	c := openTestState(t)
	fx := newAppFixture(t, "zero-token")
	token := seedToken(t, c, fx.app)
	c.RecordAppStats(AppStats{App: fx.name, EventsMatchedTotal: 4})
	before := snapshotApp(t, c, fx.name)

	observer, events := recordingObserver()
	c.SetStatusObserver(observer)
	*events = nil

	operations := []struct {
		name  string
		apply func() (bool, error)
	}{
		{"success", func() (bool, error) {
			return c.RecordReconcileSuccess(context.Background(),
				fx.name, "img", "fp", time.Now(), fx.app, 0)
		}},
		{"failure", func() (bool, error) {
			return c.RecordReconcileFailure(context.Background(), fx.name, errors.New("boom"), 0)
		}},
		{"building", func() (bool, error) { return c.RecordReconcileBuilding(context.Background(), fx.name, 0) }},
		{"reconciling", func() (bool, error) { return c.RecordReconciling(context.Background(), fx.name, 0) }},
		{"service failure", func() (bool, error) {
			return c.RecordServiceFailure(context.Background(), fx.name, errors.New("boom"), 0)
		}},
		{"removal", func() (bool, error) { return c.RecordRemoved(context.Background(), fx.name, 0) }},
	}
	for _, op := range operations {
		landed, err := op.apply()
		if err != nil {
			t.Fatalf("%s with a zero token returned error: %v", op.name, err)
		}
		if landed {
			t.Fatalf("%s with a zero token reported success", op.name)
		}
	}

	assertGenerationState(t, c, fx.name, before)
	if len(*events) != 0 {
		t.Fatalf("zero-token writes published status events: %+v", *events)
	}
	if stats, ok := c.AppStats(fx.name); !ok || stats.EventsMatchedTotal != 4 {
		t.Fatalf("zero-token removal changed app_stats: %+v ok=%v", stats, ok)
	}
	// The row still belongs to its real token.
	if !requireWrite(t, fx.name, func() (bool, error) {
		return c.RecordReconcileSuccess(context.Background(),
			fx.name, "img-current", "fp", time.Now(), fx.app, token)
	}) {
		t.Fatal("current token success did not persist after zero-token writes")
	}
}

// appFixture bundles the app and the name used by the token tests.
type appFixture struct {
	app  app.App
	name string
}

func newAppFixture(t *testing.T, name string) appFixture {
	t.Helper()
	tmpl := mustTemplate(t, twoHandlerTmpl)
	return appFixture{app: fnFor(t, name, tmpl), name: name}
}
