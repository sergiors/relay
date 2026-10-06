package worker

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/cron"
	"relay/internal/state"
)

// fakeSchedulerStorage records the scheduler-storage wiring decisions so the
// worker's required-outbox-but-not-global behavior is provable without a real
// state DB or gocron.
type fakeSchedulerStorage struct {
	setOutbox      int
	startRetry     int
	markUnavail    int
	startBootstrap int
	opener         cron.OutboxOpener
}

func (f *fakeSchedulerStorage) SetOutbox(cron.Outbox)             { f.setOutbox++ }
func (f *fakeSchedulerStorage) StartPendingRetry(context.Context) { f.startRetry++ }
func (f *fakeSchedulerStorage) MarkStorageUnavailable()           { f.markUnavail++ }
func (f *fakeSchedulerStorage) StartStorageBootstrap(_ context.Context, open cron.OutboxOpener) {
	f.startBootstrap++
	f.opener = open
}

// TestWireSchedulerStorageWithSharedHandle pins the healthy path: an available
// shared state handle is installed and the durable retry worker is started; no
// unavailable marking or bootstrap is used.
func TestWireSchedulerStorageWithSharedHandle(t *testing.T) {
	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer st.Close()

	fake := &fakeSchedulerStorage{}
	wireSchedulerStorage(context.Background(), fake, st, nil)
	if fake.setOutbox != 1 || fake.startRetry != 1 {
		t.Fatalf("setOutbox=%d startRetry=%d, want 1/1", fake.setOutbox, fake.startRetry)
	}
	if fake.markUnavail != 0 || fake.startBootstrap != 0 {
		t.Fatalf("markUnavail=%d startBootstrap=%d, want 0/0 on the healthy path", fake.markUnavail, fake.startBootstrap)
	}
}

// TestWireSchedulerStorageWithoutSharedHandleMarksUnavailable pins the
// startup-unavailable path: a nil shared handle marks the scheduler unavailable
// (so it never fires), records the lifecycle, and starts a scheduler-owned
// bootstrap — but never installs an outbox it does not have.
func TestWireSchedulerStorageWithoutSharedHandleMarksUnavailable(t *testing.T) {
	fake := &fakeSchedulerStorage{}
	called := false
	open := func() (cron.Outbox, io.Closer, error) {
		called = true
		return nil, nil, errors.New("still unavailable")
	}
	wireSchedulerStorage(context.Background(), fake, nil, open)

	if fake.markUnavail != 1 {
		t.Fatalf("markUnavail=%d, want 1", fake.markUnavail)
	}
	if fake.startRetry != 1 {
		t.Fatalf("startRetry=%d, want 1 (lifecycle recorded so a later recovery can start the worker)", fake.startRetry)
	}
	if fake.startBootstrap != 1 {
		t.Fatalf("startBootstrap=%d, want 1", fake.startBootstrap)
	}
	if fake.setOutbox != 0 {
		t.Fatalf("setOutbox=%d, want 0 (no shared handle, no outbox to install)", fake.setOutbox)
	}
	if fake.opener == nil {
		t.Fatal("bootstrap was started without an opener")
	}
	_, _, _ = fake.opener()
	if !called {
		t.Fatal("wired opener was not the supplied opener")
	}
}

// TestReadinessDoesNotDependOnScheduler pins the isolation requirement: worker
// readiness (Redis consumer + Docker + NETWORKS) is unaffected by the
// scheduler's storage state, so an unavailable scheduler still leaves event
// processing ready.
func TestReadinessDoesNotDependOnScheduler(t *testing.T) {
	consumer := &fakeConsumerHealth{healthy: true}
	manager := &fakeDockerReadiness{netOK: true}
	ready, reason := probeReadiness(context.Background(), consumer, manager, nil, time.Second)
	if !ready {
		t.Fatalf("readiness = false (%q) with healthy consumer/manager; scheduler state must not gate it", reason)
	}
}
