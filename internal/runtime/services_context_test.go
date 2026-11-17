package runtime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/testutil"
)

// TestStopServiceContainersUsesCallerDeadline pins the fix: the container-removal
// DELETE must carry the caller's deadline rather than a fresh background-rooted
// context, while still being capped by containerOpTimeout. The deadline is
// observed on the actual HTTP request through the real client call path.
func TestStopServiceContainersUsesCallerDeadline(t *testing.T) {
	var rmDeadline okDeadline
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodDelete, path: "/containers/c1", body: `{}`,
			onRequest: func(req *http.Request) { rmDeadline.capture(req) }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// An exited container: no stop call, only the removal.
	ctrs := []ServiceContainer{{ID: "c1", App: "fn", Name: "svc", State: container.StateExited}}
	if err := m.StopServiceContainers(ctx, ctrs); err != nil {
		t.Fatalf("StopServiceContainers: %v", err)
	}
	if !rmDeadline.seen || !rmDeadline.ok {
		t.Fatal("removal DELETE carried no deadline; it is not rooted in the caller ctx")
	}
	remaining := time.Until(rmDeadline.at)
	if remaining > 2*time.Second {
		t.Fatalf("removal deadline = %v, must not exceed the caller's", remaining)
	}
	if remaining > containerOpTimeout {
		t.Fatalf("removal deadline = %v, must be capped by containerOpTimeout %v", remaining, containerOpTimeout)
	}
}

// TestStopServiceContainersCancellationRespected pins that a cancelled caller
// context aborts the removal promptly (the DELETE observes cancellation) and the
// context error surfaces to the caller.
func TestStopServiceContainersCancellationRespected(t *testing.T) {
	entered := make(chan struct{})
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: `{}`,
			onRequest: func(_ *http.Request) { close(entered) },
			fail: func(req *http.Request) error {
				<-req.Context().Done()
				return req.Context().Err()
			}},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	ctrs := []ServiceContainer{{ID: "c1", State: container.StateExited}}
	done := make(chan error, 1)
	go func() { done <- m.StopServiceContainers(ctx, ctrs) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("removal DELETE was never entered")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StopServiceContainers = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not abort the removal promptly")
	}
}

func TestStopServiceContainersOnlySuppressesConfirmedCancellationWarnings(t *testing.T) {
	t.Run("cancelled request", func(t *testing.T) {
		entered := make(chan struct{})
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodDelete, path: "/containers/c1", body: `{}`,
				onRequest: func(_ *http.Request) { close(entered) },
				fail: func(req *http.Request) error {
					<-req.Context().Done()
					return req.Context().Err()
				}},
		)
		var logs bytes.Buffer
		m := &Manager{
			cli: cli,
			log: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- m.StopServiceContainers(ctx, []ServiceContainer{
				{ID: "c1", App: "fn", Name: "svc", State: container.StateExited},
			})
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("container removal request was not entered")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("StopServiceContainers error = %v, want context.Canceled", err)
		}
		if strings.Contains(logs.String(), "Service: remove container failed") {
			t.Fatalf("confirmed cancellation emitted a per-container warning:\n%s", logs.String())
		}
	})

	t.Run("operational failure remains a warning", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodDelete, path: "/containers/c1",
				status: http.StatusInternalServerError, body: `{"message":"daemon failure"}`},
		)
		var logs bytes.Buffer
		m := &Manager{
			cli: cli,
			log: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}
		err := m.StopServiceContainers(context.Background(), []ServiceContainer{
			{ID: "c1", App: "fn", Name: "svc", State: container.StateExited},
		})
		if err == nil {
			t.Fatal("StopServiceContainers unexpectedly succeeded")
		}
		if !strings.Contains(logs.String(), "Service: remove container failed") {
			t.Fatalf("operational removal failure did not emit a warning:\n%s", logs.String())
		}
	})
}

// TestStopServiceContainersLoopStopsOnExpiredContext pins that a loop over
// multiple containers does not issue further Docker calls once the caller's
// context is already done.
func TestStopServiceContainersLoopStopsOnExpiredContext(t *testing.T) {
	calls := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: `{}`,
			onMatch: func() { calls++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already expired before any call
	ctrs := []ServiceContainer{
		{ID: "c1", State: container.StateExited},
		{ID: "c2", State: container.StateExited},
	}
	err := m.StopServiceContainers(ctx, ctrs)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StopServiceContainers = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("DELETE calls = %d, want 0 (an expired context must short-circuit the loop)", calls)
	}
}

// TestStopServiceContainersBenignNotFound pins that the context-aware removal
// preserves the benign not-found semantics: removing a container the daemon no
// longer has is success.
func TestStopServiceContainersBenignNotFound(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodDelete, path: "/containers/", status: http.StatusNotFound,
			body: `{"message":"No such container"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}
	err := m.StopServiceContainers(context.Background(), []ServiceContainer{{ID: "gone", State: container.StateExited}})
	if err != nil {
		t.Fatalf("StopServiceContainers on a gone container = %v, want nil (not-found is benign)", err)
	}
}

func TestStopServiceContainersWithResultReportsPartialProgress(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodDelete, path: "/containers/c1", body: `{}`},
		dockerRoute{method: http.MethodDelete, path: "/containers/c2",
			status: http.StatusInternalServerError, body: `{"message":"remove failed"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}
	completed, err := m.StopServiceContainersWithResult(context.Background(), []ServiceContainer{
		{ID: "c1", State: container.StateExited},
		{ID: "c2", State: container.StateExited},
	})
	if err == nil {
		t.Fatal("partial removal failure was not returned")
	}
	if completed != 1 {
		t.Fatalf("completed removals = %d, want 1", completed)
	}
}

// okDeadline captures whether the observed request carried a deadline and its
// instant, reusing the shape of the build-deadline helper.
type okDeadline struct {
	seen bool
	at   time.Time
	ok   bool
}

func (d *okDeadline) capture(req *http.Request) {
	at, ok := req.Context().Deadline()
	d.seen = true
	d.at = at
	d.ok = ok
}
