package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// fakeBuildSession records the lifecycle of the session buildWithSession
// attaches, so the wiring can be asserted without a daemon.
type fakeBuildSession struct {
	id     string
	closed bool
}

func (f *fakeBuildSession) ID() string { return f.id }

func (f *fakeBuildSession) Close() error {
	f.closed = true
	return nil
}

// TestNewManagerInstallsBuildSessionHook pins the production wiring: only
// NewManager installs the hook that attaches a BuildKit session, so a manager
// created by the worker cannot silently regress to session-less builds.
func TestNewManagerInstallsBuildSessionHook(t *testing.T) {
	isolatedTmpdir(t)
	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", withPing(noopPing))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = m.Close() }()

	if m.startBuildSessionFn == nil {
		t.Fatal("NewManager did not install the BuildKit session hook")
	}
}

// TestBuildWithSessionAttachesForTheBuildAndClosesAfter pins the session
// contract: the build sees the live session's ID and the session is still open
// while it runs, then is closed before buildWithSession returns.
func TestBuildWithSessionAttachesForTheBuildAndClosesAfter(t *testing.T) {
	sess := &fakeBuildSession{id: "session-1"}
	m := &Manager{startBuildSessionFn: func(context.Context, *client.Client) (buildSessionHandle, error) {
		return sess, nil
	}}

	called := false
	err := m.buildWithSession(context.Background(), "app", func(_ context.Context, id string) error {
		called = true
		if id != sess.id {
			t.Errorf("build session ID = %q, want %q", id, sess.id)
		}
		if sess.closed {
			t.Error("session was closed before the build finished")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("buildWithSession: %v", err)
	}
	if !called {
		t.Fatal("buildWithSession did not run the build")
	}
	if !sess.closed {
		t.Fatal("session was not closed after the build")
	}
}

// TestBuildWithSessionClosesOnBuildError pins that a failed build still tears
// down its session and preserves the build error.
func TestBuildWithSessionClosesOnBuildError(t *testing.T) {
	sess := &fakeBuildSession{id: "session-1"}
	m := &Manager{startBuildSessionFn: func(context.Context, *client.Client) (buildSessionHandle, error) {
		return sess, nil
	}}
	buildErr := errors.New("build failed")

	err := m.buildWithSession(context.Background(), "app", func(context.Context, string) error {
		return buildErr
	})
	if !errors.Is(err, buildErr) {
		t.Fatalf("buildWithSession error = %v, want %v", err, buildErr)
	}
	if !sess.closed {
		t.Fatal("session was not closed after a failed build")
	}
}

// TestBuildWithSessionHookErrorSkipsBuild pins that a session that cannot be
// established fails the build attempt instead of silently building without it
// (which is the "no active sessions" failure mode this session prevents).
func TestBuildWithSessionHookErrorSkipsBuild(t *testing.T) {
	dialErr := errors.New("dial failed")
	m := &Manager{startBuildSessionFn: func(context.Context, *client.Client) (buildSessionHandle, error) {
		return nil, dialErr
	}}

	built := false
	err := m.buildWithSession(context.Background(), "app", func(context.Context, string) error {
		built = true
		return nil
	})
	if err == nil {
		t.Fatal("buildWithSession should fail when the session cannot be established")
	}
	if !errors.Is(err, dialErr) {
		t.Fatalf("buildWithSession error = %v, want %v", err, dialErr)
	}
	if !strings.Contains(err.Error(), `app "app"`) {
		t.Fatalf("buildWithSession error = %v, want the app name in the context", err)
	}
	if built {
		t.Fatal("buildWithSession ran the build without a session")
	}
}

// TestBuildWithSessionWithoutHookBuildsWithoutSession pins the direct
// construction fallback: in-package tests whose scripted daemon cannot perform
// the /session hijack build with no session ID.
func TestBuildWithSessionWithoutHookBuildsWithoutSession(t *testing.T) {
	m := &Manager{}

	called := false
	err := m.buildWithSession(context.Background(), "app", func(_ context.Context, id string) error {
		called = true
		if id != "" {
			t.Errorf("session ID = %q, want empty without a session hook", id)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("buildWithSession: %v", err)
	}
	if !called {
		t.Fatal("buildWithSession did not run the build")
	}
}

// TestBuildWithSessionNilSessionBuildsWithoutSession pins the nil-session
// fallback: a hook that intentionally attaches nothing yields an empty ID
// rather than a panic.
func TestBuildWithSessionNilSessionBuildsWithoutSession(t *testing.T) {
	m := &Manager{startBuildSessionFn: func(context.Context, *client.Client) (buildSessionHandle, error) {
		return nil, nil
	}}

	called := false
	err := m.buildWithSession(context.Background(), "app", func(_ context.Context, id string) error {
		called = true
		if id != "" {
			t.Errorf("session ID = %q, want empty for a nil session", id)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("buildWithSession: %v", err)
	}
	if !called {
		t.Fatal("buildWithSession did not run the build")
	}
}
