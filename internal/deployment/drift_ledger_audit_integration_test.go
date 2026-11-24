//go:build integration

package deployment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// expectDriftKick observes one coordinator's kick channel without blocking.
func expectDriftKick(t *testing.T, c *Coordinator, want bool, what string) {
	t.Helper()
	select {
	case <-c.kick:
		if !want {
			t.Fatalf("%s: checkDrift unexpectedly requested rematerialization", what)
		}
	default:
		if want {
			t.Fatalf("%s: checkDrift did not request rematerialization", what)
		}
	}
}

// loadAppsSeam fixes the loader's view of /apps: what the real loader would
// return for the tree under test, without a filesystem race between the seam
// and the check.
func loadAppsSeam(
	apps []app.App, issues []app.LoadIssue,
) func(string) ([]app.App, []app.LoadIssue, error) {
	return func(string) ([]app.App, []app.LoadIssue, error) {
		return apps, issues, nil
	}
}

// coordinatorAppFor builds the app.App the loader would return for a valid
// app directory under appsDir, carrying the raw-template identity of the
// exact bytes it parsed (as the real loader does).
func coordinatorAppFor(t *testing.T, appsDir, name string) app.App {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(coordinatorTemplate))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return app.App{
		Name: name, Dir: filepath.Join(appsDir, name), Template: tmpl,
		TemplateIdentity: app.TemplateIdentity([]byte(coordinatorTemplate)),
	}
}

// newDriftAuditEnv seeds an authoritative generation-1 authorization with a
// healthy authorized "fn" and, when badRow is non-nil, a desired-invalid "bad"
// row (the failed-empty-fingerprint class) built from the current tree.
func newDriftAuditEnv(
	t *testing.T, badRow func(appsDir string) seededApp,
) (*coordinatorEnv, app.App) {
	t.Helper()
	env := newCoordinatorEnv(t)
	fnFP, fnTID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	rows := []seededApp{{
		name: "fn", fingerprint: fnFP, templateID: fnTID,
		phase: state.DeploymentAppPending,
	}}
	if badRow != nil {
		rows = append(rows, badRow(env.cfg.AppsDir))
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne, rows...)
	return env, coordinatorAppFor(t, env.cfg.AppsDir, "fn")
}

// invalidBadRow is the persisted shape of a desired-but-unloadable app: a
// failed phase, no content fingerprint, and the materialized template identity.
func invalidBadRow(templateID string) seededApp {
	return seededApp{
		name: "bad", fingerprint: "", templateID: templateID,
		phase: state.DeploymentAppFailed, errMessage: "template invalid",
	}
}

// writeInvalidBad materializes "bad" with an unloadable template and returns
// the identity its bytes hash to — the value the authorization records.
func writeInvalidBad(t *testing.T, appsDir string) string {
	t.Helper()
	dir := filepath.Join(appsDir, "bad")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir bad: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	src, err := app.ReadTemplateSource(appsDir, "bad")
	if err != nil {
		t.Fatalf("template source bad: %v", err)
	}
	return src.Identity
}

// TestDriftCheckTrustedVerificationIsSkippedOnceThenHashed pins the recency
// cutoff of the independent backstop: a reconciliation verification is trusted
// by exactly ONE check (the window whose previous-check boundary contains it),
// so a verification made just before a missed edit defers the coordinator's
// hash to the next drift tick — the edit is still detected, bounded by one
// extra tick (about two intervals from the verification) — while the window
// itself advances monotonically so the same verification can never be
// re-trusted.
func TestDriftCheckTrustedVerificationIsSkippedOnceThenHashed(t *testing.T) {
	env, fn := newDriftAuditEnv(t, nil)
	ctx := context.Background()
	fnDir := filepath.Join(env.cfg.AppsDir, "fn")

	cfg := env.cfg
	cfg.LoadApps = loadAppsSeam([]app.App{fn}, nil)

	var sinceLog []time.Time
	builtAt := time.Now()
	c := New(cfg)
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	// The ledger models the reconciler's contract: an entry counts only when it
	// was recorded at or after the window boundary the check asks for.
	var verifiedAt time.Time
	c.SetVerificationLedger(func(since time.Time) map[string]struct{} {
		sinceLog = append(sinceLog, since)
		if verifiedAt.IsZero() || verifiedAt.Before(since) {
			return nil
		}
		return map[string]struct{}{"fn": {}}
	})

	// The reconciler verifies fn's content, then an edit lands immediately
	// after — the missed-edit window the cutoff can skip.
	verifiedAt = time.Now()
	if err := os.WriteFile(filepath.Join(fnDir, "index.js"),
		[]byte("export function hi(e, context){ /* missed edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit app: %v", err)
	}

	// Check 1: its window reaches back to construction, so it trusts the fresh
	// verification and must NOT hash the (already drifted) bytes.
	c.checkDrift(ctx)
	expectDriftKick(t, c, false, "first check trusting a fresh verification")

	// Check 2: the window now starts at check 1, which is after the
	// verification. The entry no longer counts, the bytes are hashed, and the
	// edit is detected — exactly one tick late, never silently.
	c.checkDrift(ctx)
	expectDriftKick(t, c, true, "next check after the window advanced")

	if len(sinceLog) != 2 {
		t.Fatalf("ledger windows = %d, want one query per check", len(sinceLog))
	}
	if sinceLog[0].Before(builtAt) || sinceLog[0].After(time.Now()) {
		t.Fatalf("first window since = %v, want the construction boundary in [%v, %v]",
			sinceLog[0], builtAt, time.Now())
	}
	if sinceLog[1].Before(sinceLog[0]) {
		t.Fatalf("second window since = %v, want a boundary no earlier than the "+
			"first (%v): a verification must never be re-trusted", sinceLog[1], sinceLog[0])
	}
}

// TestDriftCheckHashesOnlyAppsOutsideTheLedger pins the steady-state split of
// the backstop: authorized apps the reconciler verified in the current window
// are never hashed a second time — even when their bytes now differ, because
// the reconciler's own drift report already requested restoration — while an
// authorized app outside the ledger IS hashed and detected, no matter how many
// other apps the ledger covers.
func TestDriftCheckHashesOnlyAppsOutsideTheLedger(t *testing.T) {
	env := newCoordinatorEnv(t)
	fnFP, fnTID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	extraFP, extraTID := writeCoordinatorApp(t, env.cfg.AppsDir, "extra")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fnFP, templateID: fnTID,
			phase: state.DeploymentAppPending},
		seededApp{name: "extra", fingerprint: extraFP, templateID: extraTID,
			phase: state.DeploymentAppPending},
	)
	ctx := context.Background()
	fnApp := coordinatorAppFor(t, env.cfg.AppsDir, "fn")
	extraApp := coordinatorAppFor(t, env.cfg.AppsDir, "extra")

	cfg := env.cfg
	cfg.LoadApps = loadAppsSeam([]app.App{fnApp, extraApp}, nil)
	c := New(cfg)
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}

	// Both apps drift locally after the reconciler verified them.
	for _, name := range []string{"fn", "extra"} {
		if err := os.WriteFile(filepath.Join(env.cfg.AppsDir, name, "index.js"),
			[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
			t.Fatalf("edit %s: %v", name, err)
		}
	}

	// Ledger covers both within the window: no source hash, no kick — the
	// reconciler's own drift report is the requested restore, and re-hashing
	// here would duplicate that work.
	covered := map[string]struct{}{"fn": {}, "extra": {}}
	c.SetVerificationLedger(func(time.Time) map[string]struct{} { return covered })
	c.checkDrift(ctx)
	expectDriftKick(t, c, false, "steady state with every app verified")

	// One app falls out of the ledger (stale or never verified): only that app
	// is hashed, and its drift is detected despite the other skip.
	covered = map[string]struct{}{"fn": {}}
	c.checkDrift(ctx)
	expectDriftKick(t, c, true, "unverified app hashed despite a partially covered ledger")
}

// TestDriftCheckInvalidAndLocalOnlyIssueMatrix pins the loader-issue half of
// the independent check against applied.Invalid/InvalidTemplates: a missing
// desired-invalid app is drift, deterministic identity changes on a
// desired-invalid app are drift, while an unchanged or identity-less
// (transient) desired-invalid issue stays with the reconciler's retry, and a
// local-only unloadable entry is unauthorized content.
func TestDriftCheckInvalidAndLocalOnlyIssueMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("missing desired-invalid app is drift", func(t *testing.T) {
		env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
			return invalidBadRow(writeInvalidBad(t, appsDir))
		})
		// The loader no longer sees bad at all: its directory vanished from the
		// materialized tree, and only the authorization still names it.
		cfg := env.cfg
		cfg.LoadApps = loadAppsSeam([]app.App{fn}, nil)
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, true, "missing desired-invalid app")
	})

	t.Run("changed invalid identity is drift", func(t *testing.T) {
		env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
			return invalidBadRow(writeInvalidBad(t, appsDir))
		})
		badDir := filepath.Join(env.cfg.AppsDir, "bad")
		if err := os.WriteFile(filepath.Join(badDir, "template.yaml"),
			[]byte("runtime: python9.1\n"), 0o644); err != nil {
			t.Fatalf("edit invalid template: %v", err)
		}
		changed, err := app.ReadTemplateSource(env.cfg.AppsDir, "bad")
		if err != nil {
			t.Fatalf("template source bad: %v", err)
		}
		cfg := env.cfg
		cfg.LoadApps = loadAppsSeam([]app.App{fn}, []app.LoadIssue{{
			Name: "bad", Err: errors.New("runtime python9.1 is unknown"),
			Identity: changed.Identity,
		}})
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, true, "deterministically changed invalid identity")
	})

	t.Run("unchanged invalid identity stays with the reconciler", func(t *testing.T) {
		env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
			return invalidBadRow(writeInvalidBad(t, appsDir))
		})
		recorded, err := app.ReadTemplateSource(env.cfg.AppsDir, "bad")
		if err != nil {
			t.Fatalf("template source bad: %v", err)
		}
		cfg := env.cfg
		cfg.LoadApps = loadAppsSeam([]app.App{fn}, []app.LoadIssue{{
			Name: "bad", Err: errors.New("runtime python9.9 is unknown"),
			Identity: recorded.Identity,
		}})
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, false, "unchanged desired-invalid issue")
	})

	t.Run("transient empty-identity issue on a desired-invalid stays with the reconciler",
		func(t *testing.T) {
			env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
				return invalidBadRow(writeInvalidBad(t, appsDir))
			})
			cfg := env.cfg
			// An empty local identity says nothing about the bytes: even had the
			// deterministic bytes changed, a read failure must not request a
			// restore from evidence it never observed.
			cfg.LoadApps = loadAppsSeam([]app.App{fn}, []app.LoadIssue{{
				Name: "bad", Err: errors.New("permission denied"), Identity: "",
			}})
			c := New(cfg)
			if _, err := c.ObserveStartup(ctx); err != nil {
				t.Fatalf("ObserveStartup: %v", err)
			}
			c.checkDrift(ctx)
			expectDriftKick(t, c, false, "identity-less desired-invalid issue")
		})

	t.Run("unverifiable invalid identity requests re-authorization", func(t *testing.T) {
		env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
			// Materialization could not read/hash bad's template, so no
			// materialized identity was recorded (empty templateID on the row).
			writeInvalidBad(t, appsDir)
			return invalidBadRow("")
		})
		local, err := app.ReadTemplateSource(env.cfg.AppsDir, "bad")
		if err != nil {
			t.Fatalf("template source bad: %v", err)
		}
		if local.Identity == "" {
			t.Fatal("precondition: the local invalid bytes must hash to an identity")
		}
		cfg := env.cfg
		// The loader deterministically hashes local invalid bytes, but there is
		// nothing recorded to compare against: unverifiable local invalid content
		// must request re-authorization of the exact shared SHA.
		cfg.LoadApps = loadAppsSeam([]app.App{fn}, []app.LoadIssue{{
			Name: "bad", Err: errors.New("runtime python9.9 is unknown"),
			Identity: local.Identity,
		}})
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, true, "unverifiable desired-invalid identity")
	})

	t.Run("empty issue identity with nothing recorded stays transient", func(t *testing.T) {
		env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
			writeInvalidBad(t, appsDir)
			return invalidBadRow("")
		})
		cfg := env.cfg
		// A read failure produces no identity at all: with nothing recorded and
		// nothing observed, there is no evidence about the bytes either way, so
		// the transient stays with the reconciler's retry.
		cfg.LoadApps = loadAppsSeam([]app.App{fn}, []app.LoadIssue{{
			Name: "bad", Err: errors.New("permission denied"), Identity: "",
		}})
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, false, "identity-less issue with no recorded identity")
	})

	t.Run("transient empty-identity issue on an authorized app is drift", func(t *testing.T) {
		env, _ := newDriftAuditEnv(t, nil)
		cfg := env.cfg
		// The name audit never needs content evidence: an authorized app the
		// loader cannot read at all is drift regardless of identity.
		cfg.LoadApps = loadAppsSeam(nil, []app.LoadIssue{{
			Name: "fn", Err: errors.New("permission denied"), Identity: "",
		}})
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, true, "unreadable authorized app")
	})

	t.Run("local-only unloadable addition is drift", func(t *testing.T) {
		env, fn := newDriftAuditEnv(t, nil)
		junkDir := filepath.Join(env.cfg.AppsDir, "junk")
		if err := os.MkdirAll(junkDir, 0o755); err != nil {
			t.Fatalf("mkdir junk: %v", err)
		}
		if err := os.WriteFile(filepath.Join(junkDir, "template.yaml"),
			[]byte("runtime: python9.9\n"), 0o644); err != nil {
			t.Fatalf("write junk template: %v", err)
		}
		junk, err := app.ReadTemplateSource(env.cfg.AppsDir, "junk")
		if err != nil {
			t.Fatalf("template source junk: %v", err)
		}
		cfg := env.cfg
		cfg.LoadApps = loadAppsSeam([]app.App{fn}, []app.LoadIssue{{
			Name: "junk", Err: errors.New("runtime python9.9 is unknown"),
			Identity: junk.Identity,
		}})
		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		c.checkDrift(ctx)
		expectDriftKick(t, c, true, "local-only unloadable addition")
	})

	t.Run("desired-invalid that becomes loadable with changed bytes is drift",
		func(t *testing.T) {
			env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
				return invalidBadRow(writeInvalidBad(t, appsDir))
			})
			// bad is corrected locally: it now parses, so the loader returns it
			// as an app whose template identity no longer matches the recorded
			// materialized invalid identity.
			if err := os.RemoveAll(filepath.Join(env.cfg.AppsDir, "bad")); err != nil {
				t.Fatalf("remove bad: %v", err)
			}
			writeCoordinatorApp(t, env.cfg.AppsDir, "bad")
			badApp := coordinatorAppFor(t, env.cfg.AppsDir, "bad")
			cfg := env.cfg
			cfg.LoadApps = loadAppsSeam([]app.App{fn, badApp}, nil)
			c := New(cfg)
			if _, err := c.ObserveStartup(ctx); err != nil {
				t.Fatalf("ObserveStartup: %v", err)
			}
			c.checkDrift(ctx)
			expectDriftKick(t, c, true, "loadable desired-invalid with changed bytes")
		})

	t.Run("desired-invalid that becomes fingerprintable requests re-authorization",
		func(t *testing.T) {
			env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
				writeCoordinatorApp(t, appsDir, "bad")
				// No recorded materialized identity: the row carries none.
				return invalidBadRow("")
			})
			badApp := coordinatorAppFor(t, env.cfg.AppsDir, "bad")
			cfg := env.cfg
			cfg.LoadApps = loadAppsSeam([]app.App{fn, badApp}, nil)
			c := New(cfg)
			if _, err := c.ObserveStartup(ctx); err != nil {
				t.Fatalf("ObserveStartup: %v", err)
			}
			c.checkDrift(ctx)
			expectDriftKick(t, c, true, "now-fingerprintable desired-invalid app")
		})

	t.Run("loadable desired-invalid with matching identity stays with the reconciler",
		func(t *testing.T) {
			skipWithoutPosixPermissions(t)
			env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
				_, tid := writeCoordinatorApp(t, appsDir, "bad")
				return invalidBadRow(tid)
			})
			index := filepath.Join(env.cfg.AppsDir, "bad", "index.js")
			if err := os.Chmod(index, 0o000); err != nil {
				t.Fatalf("chmod index: %v", err)
			}
			t.Cleanup(func() { _ = os.Chmod(index, 0o644) })

			badApp := coordinatorAppFor(t, env.cfg.AppsDir, "bad")
			cfg := env.cfg
			cfg.LoadApps = loadAppsSeam([]app.App{fn, badApp}, nil)
			c := New(cfg)
			if _, err := c.ObserveStartup(ctx); err != nil {
				t.Fatalf("ObserveStartup: %v", err)
			}
			// The local template identity matches the recorded materialized one:
			// a restore would be a no-op, so the failing source fingerprint (and
			// the retry it needs) stays with the reconciler — even though this
			// backstop window has not verified the app itself.
			c.checkDrift(ctx)
			expectDriftKick(t, c, false,
				"loadable desired-invalid with a matching identity")
		})

	t.Run("loadable desired-invalid verified this window stays with the reconciler",
		func(t *testing.T) {
			env, fn := newDriftAuditEnv(t, func(appsDir string) seededApp {
				_, tid := writeCoordinatorApp(t, appsDir, "bad")
				return invalidBadRow(tid)
			})
			badApp := coordinatorAppFor(t, env.cfg.AppsDir, "bad")
			cfg := env.cfg
			cfg.LoadApps = loadAppsSeam([]app.App{fn, badApp}, nil)
			c := New(cfg)
			if _, err := c.ObserveStartup(ctx); err != nil {
				t.Fatalf("ObserveStartup: %v", err)
			}
			// The reconciler read this app under the current authorization since
			// the previous check: it either reported the changed identity as drift
			// or already requested re-authorization, so the backstop must not
			// duplicate that verdict with a second hash.
			c.SetVerificationLedger(func(time.Time) map[string]struct{} {
				return map[string]struct{}{"bad": {}}
			})
			c.checkDrift(ctx)
			expectDriftKick(t, c, false,
				"loadable desired-invalid verified within the window")
		})
}

// skipWithoutPosixPermissions guards permission-based failure injection: it
// only defines behavior where read permission is enforced.
func skipWithoutPosixPermissions(t *testing.T) {
	t.Helper()
	if goruntime.GOOS == "windows" {
		t.Skip("permission-based failure injection is not portable on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as uid 0: read permission is not enforced")
	}
}

// blockingBuildBuilder holds the first preparation until release closes.
type blockingBuildBuilder struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (b *blockingBuildBuilder) Prepare(
	_ context.Context, fn app.App,
) (*runtime.Prepared, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.release
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (b *blockingBuildBuilder) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

// TestIndependentDriftCheckHashesQueuedAppWhilePumpBlocked is the end-to-end
// composition of the design: the worker wires the coordinator's backstop to the
// reconciler's verification ledger, a build holds the reconciler's serial pump
// with a drifted app queued behind it, and the independent check — running on
// the coordinator loop, never through the pump — still hashes that queued
// (unverified) app from the ledger's point of view and requests restoration,
// while the blocked build's own already-verified content is not re-hashed.
func TestIndependentDriftCheckHashesQueuedAppWhilePumpBlocked(t *testing.T) {
	env := newCoordinatorEnv(t)
	blockedFP, blockedTID := writeCoordinatorApp(t, env.cfg.AppsDir, "01-blocked")
	queuedFP, queuedTID := writeCoordinatorApp(t, env.cfg.AppsDir, "zzz-queued")
	// The queued app drifts after authorization, before its reconcile runs.
	if err := os.WriteFile(filepath.Join(env.cfg.AppsDir, "zzz-queued", "index.js"),
		[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit queued app: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "01-blocked", fingerprint: blockedFP, templateID: blockedTID,
			phase: state.DeploymentAppPending},
		seededApp{name: "zzz-queued", fingerprint: queuedFP, templateID: queuedTID,
			phase: state.DeploymentAppPending},
	)
	ctx, cancel := context.WithCancel(context.Background())

	blocking := &blockingBuildBuilder{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	rec := reconciler.New(reconciler.Config{
		Root: env.cfg.AppsDir, Debounce: 5 * time.Millisecond, Interval: time.Hour,
	}, &runner.Registry{}, blocking, testutil.DiscardLogger())
	rec.SetDeploymentAuthorization(1,
		map[string]string{"01-blocked": blockedFP, "zzz-queued": queuedFP},
		map[string]string{"01-blocked": blockedTID, "zzz-queued": queuedTID}, nil)

	startDone := make(chan error, 1)
	go func() { startDone <- rec.Start(ctx) }()
	var releaseOnce sync.Once
	t.Cleanup(func() {
		// Unblock the pump before ending its lifecycle so the shutdown join
		// cannot hang inside the held build.
		releaseOnce.Do(func() { close(blocking.release) })
		cancel()
		select {
		case <-startDone:
		case <-time.After(5 * time.Second):
			t.Error("reconciler did not stop after cancellation")
		}
	})

	rec.Enqueue("01-blocked")
	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the leading build never entered the pump")
	}
	rec.Enqueue("zzz-queued")

	// While the pump is inside the blocked build: the blocked app's content was
	// fingerprinted before Prepare (so the ledger has it), the queued drifted
	// app was never reached — the ledger must show exactly that staleness.
	if _, ok := rec.VerifiedSince(time.Time{})["01-blocked"]; !ok {
		t.Fatal("the in-flight app's pre-build verification must be visible in the ledger")
	}
	if _, ok := rec.VerifiedSince(time.Time{})["zzz-queued"]; ok {
		t.Fatal("a queued app behind the blocked build must stay unverified in the ledger")
	}

	// The coordinator sees the tree with the exact production wiring.
	c := New(env.cfg)
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	c.SetVerificationLedger(rec.VerifiedSince)

	c.checkDrift(ctx)
	expectDriftKick(t, c, true,
		"independent check hashing the queued app while the pump is blocked")

	// The pump is still blocked: the detection above never traveled through it.
	if _, ok := rec.VerifiedSince(time.Time{})["zzz-queued"]; ok {
		t.Fatal("the queued app must still be unverified after the independent check")
	}
}
