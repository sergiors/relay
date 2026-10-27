//go:build integration

package worker

import (
	"context"
	"testing"

	"relay/internal/testutil"
)

// TestWireSchedulerStorageWithSharedHandle pins the healthy path: an available
// shared state handle is installed and the durable retry worker is started; no
// unavailable marking or bootstrap is used.
func TestWireSchedulerStorageWithSharedHandle(t *testing.T) {
	st := testutil.NewState(t)

	fake := &fakeSchedulerStorage{}
	wireSchedulerStorage(context.Background(), fake, st, nil)
	if fake.setOutbox != 1 || fake.startRetry != 1 {
		t.Fatalf("setOutbox=%d startRetry=%d, want 1/1", fake.setOutbox, fake.startRetry)
	}
	if fake.markUnavail != 0 || fake.startBootstrap != 0 {
		t.Fatalf("markUnavail=%d startBootstrap=%d, want 0/0 on the healthy path", fake.markUnavail, fake.startBootstrap)
	}
}
