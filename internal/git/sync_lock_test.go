package git

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"relay/internal/processlock"
)

// errLockProbeTransport is returned by the fake transport once a sync has
// acquired the writer lock. Reaching it is the proof that the lock wait ended
// before any checkout work began.
var errLockProbeTransport = errors.New("transport reached")

// lockProbeOps records transport attempts without cloning or opening anything.
type lockProbeOps struct {
	cloneCalls int
	openCalls  int
}

func (o *lockProbeOps) clone(
	context.Context, string, string, gitssh.AuthMethod,
) error {
	o.cloneCalls++
	return errLockProbeTransport
}

func (o *lockProbeOps) open(string) (gitRepo, error) {
	o.openCalls++
	return nil, errLockProbeTransport
}

// TestSyncLockPathIsCheckoutScoped pins the lock identity: the path is a
// sibling of the managed checkout (never inside it, which a re-clone removes),
// is cleaned, and distinct checkouts get distinct locks. Callers sharing only
// AppsDir with different checkouts therefore do not serialize; that limitation
// is documented by this behavior rather than hidden by a shared global lock.
func TestSyncLockPathIsCheckoutScoped(t *testing.T) {
	checkout := filepath.Join(t.TempDir(), "git", "checkout")
	want := filepath.Join(filepath.Dir(checkout),
		"relay-"+filepath.Base(checkout)+".sync.lock")
	if got := syncLockPath(checkout); got != want {
		t.Fatalf("syncLockPath(%q) = %q, want %q", checkout, got, want)
	}
	if got := syncLockPath(checkout + string(filepath.Separator)); got != want {
		t.Fatalf("trailing separator changed lock path: %q, want %q", got, want)
	}
	other := filepath.Join(t.TempDir(), "git", "checkout")
	if syncLockPath(other) == want {
		t.Fatal("distinct checkouts share one writer lock path")
	}
	if got := syncLockPath(""); got != "" {
		t.Fatalf("syncLockPath(\"\") = %q, want empty", got)
	}
}

// TestSyncWaitsForWriterLockBeforeTransport pins writer serialization without
// sleeps: while the checkout-scoped lock is held, a sync is bounded by its
// context and fails at the lock, without touching the checkout or transport.
// Releasing the lock lets the same sync reach the transport, proving the lock
// was the blocker.
func TestSyncWaitsForWriterLockBeforeTransport(t *testing.T) {
	e := fixture(t, false)
	opts := syncOpts(t, e, io.Discard)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}

	lockPath := syncLockPath(opts.CheckoutDir)
	held, err := processlock.Acquire(lockPath)
	if err != nil {
		t.Fatalf("hold writer lock: %v", err)
	}
	defer held.Close()

	ops := &lockProbeOps{}
	ctx, cancel := context.WithTimeout(context.Background(),
		150*time.Millisecond)
	defer cancel()
	err = syncWithGit(ctx, opts, cfg, ops, &countingBuilder{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sync err = %v, want context deadline while lock is held", err)
	}
	if !strings.Contains(err.Error(), "acquire sync lock") {
		t.Fatalf("sync err = %v, want acquire-sync-lock context", err)
	}
	if ops.cloneCalls != 0 || ops.openCalls != 0 {
		t.Fatalf("transport reached while writer lock held: clone=%d open=%d",
			ops.cloneCalls, ops.openCalls)
	}
	if _, statErr := os.Stat(opts.CheckoutDir); !os.IsNotExist(statErr) {
		t.Fatalf("checkout touched before acquiring writer lock: %v", statErr)
	}

	if err := held.Close(); err != nil {
		t.Fatalf("release writer lock: %v", err)
	}
	ops = &lockProbeOps{}
	err = syncWithGit(context.Background(), opts, cfg, ops, &countingBuilder{})
	if !errors.Is(err, errLockProbeTransport) {
		t.Fatalf("sync after release err = %v, want transport reached", err)
	}
	if ops.cloneCalls != 1 {
		t.Fatalf("clone calls after release = %d, want 1", ops.cloneCalls)
	}
}
