package reconciler

// Focused regression tests for the watch-recovery implementation in
// reconciler.go: retry state/timer, watch registration tracking, terminal
// Start errors, recoverable-error rescan, and watchEntry identity refresh.
//
// Determinism notes:
//   - Wherever possible the tests drive the real event paths (PrepareWatch,
//     eventLoop, ticker) and converge by polling observable state, never by
//     sleeping for fixed durations.
//   - The scenarios that inject REAL registration failures use POSIX
//     permissions (an unreadable directory makes fsnotify's Add fail for any
//     non-root unix user), matching helpers_test.go's
//     skipWithoutPosixPermissions convention.
//   - The exchange scenarios drive handleWatchEvent directly with the
//     Rename+Create notification pair an atomic per-app directory exchange
//     produces, against a real fsnotify watcher and a really swapped
//     directory, but WITHOUT a concurrently running event loop:
//     handleWatchEvent is the exact function the event loop runs, so
//     synchronous driving is observationally equivalent while making
//     inode-identity assertions race-free.
//   - A change landing between the event handler's inspection and the
//     registration (the inspect/register race the post-refresh enqueue closes)
//     cannot be interleaved deterministically inside one handler call without
//     a production seam. Both entry points that own the guarantee are pinned
//     instead: the retry path registers BEFORE it inspects (decisive), and the
//     exchange path reconciles changes that landed while the tree was
//     detached.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/testutil"
)

// watchSettleDeadline is the shared convergence deadline for event-driven
// watch state; it bounds every poll in this file.
const watchSettleDeadline = 5 * time.Second

// restorePermissionsForCleanup registers a cleanup that makes the planted
// permission failure readable again, so a failing test's TempDir removal
// cannot itself fail on the unreadable directory.
func restorePermissionsForCleanup(t *testing.T, path string) {
	t.Helper()
	t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
}

// pollUntil runs fn until it reports true, failing with desc when it never
// settles. All polling in this file converges on observable state.
func pollUntil(t *testing.T, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(watchSettleDeadline)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

// watchMapPaths copies the reconciler's in-process watch index.
func watchMapPaths(r *Reconciler) map[string]struct{} {
	out := map[string]struct{}{}
	r.watches.Range(func(key, _ any) bool {
		if p, ok := key.(string); ok {
			out[p] = struct{}{}
		}
		return true
	})
	return out
}

// watchListPaths copies the fsnotify backend's current watch list.
func watchListPaths(r *Reconciler) map[string]struct{} {
	out := map[string]struct{}{}
	for _, p := range r.w.WatchList() {
		out[p] = struct{}{}
	}
	return out
}

// pendingWatchRetries copies the retry index as path -> consecutive attempts.
func pendingWatchRetries(r *Reconciler) map[string]int {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	out := make(map[string]int, len(r.watchRetries))
	for path, retry := range r.watchRetries {
		if retry != nil {
			out[path] = retry.attempts
		}
	}
	return out
}

// watchEntryIsCurrent reports whether a registered watch entry refers to the
// path's CURRENT inode (the identity-refresh invariant).
func watchEntryIsCurrent(t *testing.T, r *Reconciler, path string) bool {
	t.Helper()
	v, ok := r.watches.Load(path)
	if !ok {
		return false
	}
	entry, ok := v.(watchEntry)
	if !ok || entry.info == nil {
		return true
	}
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return os.SameFile(entry.info, info)
}

// samePathSet reports whether both path sets hold exactly the same members.
func samePathSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for p := range a {
		if _, ok := b[p]; !ok {
			return false
		}
	}
	return true
}

// buildTree creates an app directory with template.yaml, the given index.js
// content, and empty subdirectories, and returns its path.
func buildTree(t *testing.T, base, name, index string, subdirs ...string) string {
	t.Helper()
	dir := filepath.Join(base, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(template), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte(index), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	for _, s := range subdirs {
		if err := os.MkdirAll(filepath.Join(dir, s), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Join(dir, s), err)
		}
	}
	return dir
}

// exchangeAppDirectory replaces the tree at target with srcDir, leaving the
// same path bound to a different inode the way an atomic per-app exchange
// does.
func exchangeAppDirectory(t *testing.T, target, srcDir string) {
	t.Helper()
	if err := os.RemoveAll(target); err != nil {
		t.Fatalf("replace %s: %v", target, err)
	}
	if err := os.Rename(srcDir, target); err != nil {
		t.Fatalf("rotate %s into place: %v", target, err)
	}
}

// driveExchangeNotification feeds the Rename+Create notification pair an
// atomic per-app directory exchange produces to the exact handler the event
// loop runs. The caller must not run an event loop concurrently.
func driveExchangeNotification(r *Reconciler, path string) {
	r.handleWatchEvent(fsnotify.Event{Name: path, Op: fsnotify.Rename})
	r.handleWatchEvent(fsnotify.Event{Name: path, Op: fsnotify.Create})
}

// fixtureWithoutLoops builds a reconciler seeded from initial WITHOUT running
// Start, then establishes the synchronous watch, so exchange notifications
// can be driven through handleWatchEvent without a concurrently running event
// loop.
func fixtureWithoutLoops(
	t *testing.T,
	root string,
	b Builder,
	initial []*runner.PreparedApp,
) *Reconciler {
	t.Helper()
	r, _ := newTestReconciler(t, root, b, initial, nil)
	if err := r.PrepareWatch(context.Background()); err != nil {
		t.Fatalf("prepare watch: %v", err)
	}
	return r
}

// shutdownSkeletonReconciler registers cleanup for a skeleton reconciler whose
// Start never ran: pending debounce timers are stopped and the done channel is
// closed so the test-started pump exits. It must NOT be used on a reconciler
// whose Start already ran (Start closes done itself).
func shutdownSkeletonReconciler(t *testing.T, r *Reconciler) {
	t.Helper()
	t.Cleanup(func() {
		r.mu.Lock()
		for name, timer := range r.timers {
			timer.Stop()
			delete(r.timers, name)
		}
		r.mu.Unlock()
		close(r.done)
	})
}

// assertRegistryPrepared polls until name is a registered, prepared entry.
func assertRegistryPrepared(t *testing.T, reg *runner.Registry, name string) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("registry entry %q to become prepared", name), func() bool {
		pf := reg.GetByName(name)
		return pf != nil && pf.Prepared() != nil
	})
}

// drainReconcileQueue polls until the pump would find no work: no queued
// names and no armed debounce timers.
func drainReconcileQueue(t *testing.T, r *Reconciler) {
	t.Helper()
	pollUntil(t, "reconcile queue to drain", func() bool {
		r.mu.Lock()
		armed := len(r.timers)
		r.mu.Unlock()
		return armed == 0 && len(r.incoming) == 0
	})
}

// injectWatchError delivers one recoverable backend error to the event loop's
// Errors reader, like an fsnotify overflow. The Errors channel is unbuffered,
// so the send completes only when the loop takes the error.
func injectWatchError(t *testing.T, r *Reconciler, err error) {
	t.Helper()
	select {
	case r.w.Errors <- err:
	case <-time.After(watchSettleDeadline):
		t.Fatal("event loop never consumed the injected watch error")
	}
}

// --- scenarios 1 and 4 ---

// TestWatchInitialRegistrationFailureRecoversAndReconciles pins the
// initial-watch seam: a registration failure during the synchronous
// PrepareWatch walk is not fatal (PrepareWatch still returns nil and the
// watcher keeps working), the failed path is tracked for one bounded-backoff
// retry, and once the filesystem becomes readable again the retry registers
// the tree and the affected app is reconciled through the normal policy.
func TestWatchInitialRegistrationFailureRecoversAndReconciles(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	alphaPath := writeFnDir(t, root, "alpha")
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The app directory is unreadable at watch-establishment time: the root
	// registers, the app does not, and the failure is tracked — not fatal.
	if err := os.Chmod(alphaPath, 0o000); err != nil {
		t.Fatalf("chmod alpha: %v", err)
	}
	restorePermissionsForCleanup(t, alphaPath)
	if err := r.PrepareWatch(ctx); err != nil {
		t.Fatalf("PrepareWatch = %v; individual registration failures must not be fatal", err)
	}
	if !allWatched(r, []string{root}) || !noneWatched(r, []string{alphaPath}) {
		t.Fatal("an unreadable app directory must leave the root watched and the app unwatched")
	}
	if got := pendingWatchRetries(r); got[alphaPath] < 1 {
		t.Fatalf("pending watch retries = %v; want the failed path tracked for retry", got)
	}

	// The filesystem heals; the retry must register the tree and the first
	// reconcile must discover the app.
	if err := os.Chmod(alphaPath, 0o755); err != nil {
		t.Fatalf("restore alpha: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	pollUntil(t, "recovered watch on the failed app path", func() bool {
		return allWatched(r, []string{root, alphaPath}) &&
			watchEntryIsCurrent(t, r, alphaPath) &&
			len(pendingWatchRetries(r)) == 0
	})
	waitForPrepare(t, b, 1)
	assertRegistryPrepared(t, reg, "alpha")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// TestWatchNestedModificationObservedAfterNestedRecovery pins nested
// recovery: a failure inside a nested subtree is retried per-path, the
// recovery itself does not rebuild an unchanged app, and once the nested
// registration is restored a modification beneath it maps to the app and
// triggers reconciliation (the restored watch actually delivers).
func TestWatchNestedModificationObservedAfterNestedRecovery(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	papaDir := writeFnDir(t, root, "papa")
	nestedPath := filepath.Join(papaDir, "nested")
	if err := os.MkdirAll(nestedPath, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b,
		[]*runner.PreparedApp{initialFn("papa", papaDir)}, nil)
	if reg.GetByName("papa") == nil || reg.GetByName("papa").Prepared() == nil {
		t.Fatal("precondition: papa must be registered as prepared")
	}

	// The seed must see the FULL tree: the subtree becomes unreadable only
	// after seeding (as after a real permission regression), so the recovery
	// reconcile can genuinely find the app unchanged.
	if err := os.Chmod(nestedPath, 0o000); err != nil {
		t.Fatalf("chmod nested: %v", err)
	}
	restorePermissionsForCleanup(t, nestedPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	// Nested failure: the app dir itself registers, the nested subtree does
	// not, and only the nested path is pending retry.
	pollUntil(t, "initial walk to finish with only the nested subtree unwatched", func() bool {
		return allWatched(r, []string{root, papaDir}) &&
			noneWatched(r, []string{nestedPath}) &&
			pendingWatchRetries(r)[nestedPath] >= 1
	})

	// The subtree becomes readable; the retry registers it from the current
	// tree. Recovery alone must not rebuild an unchanged app.
	if err := os.Chmod(nestedPath, 0o755); err != nil {
		t.Fatalf("restore nested: %v", err)
	}
	pollUntil(t, "recovered watch on the nested subtree", func() bool {
		return allWatched(r, []string{root, papaDir, nestedPath}) &&
			watchEntryIsCurrent(t, r, nestedPath) &&
			len(pendingWatchRetries(r)) == 0
	})
	drainReconcileQueue(t, r)
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d after recovery; the unchanged app must not rebuild", b.prepares())
	}

	// A modification BENEATH the recovered registration reaches the app.
	if err := os.WriteFile(filepath.Join(nestedPath, "new.js"), []byte("// extra\n"), 0o644); err != nil {
		t.Fatalf("write nested modification: %v", err)
	}
	waitForPrepare(t, b, 1)
	if reg.GetByName("papa") == nil || reg.GetByName("papa").Prepared() == nil {
		t.Fatal("papa must remain registered and prepared after the nested rebuild")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenario 2 ---

// TestWatchPartialFailureRetriesOnlyFailedPath pins per-path retry isolation:
// one unreadable subtree fails its registration, the walk continues past it so
// the rest of the tree is watched and keeps working, only the failed path is
// pending retry, and after recovery only the failed path's app is reconciled.
func TestWatchPartialFailureRetriesOnlyFailedPath(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	// "aaa-broken" sorts FIRST so the walk demonstrably continues past its
	// failure to register the later sibling's whole subtree.
	brokenPath := writeFnDir(t, root, "aaa-broken")
	zzzDir := writeFnDir(t, root, "zzz")
	zzzNested := filepath.Join(zzzDir, "deep")
	if err := os.MkdirAll(zzzNested, 0o755); err != nil {
		t.Fatalf("mkdir deep: %v", err)
	}
	if err := os.Chmod(brokenPath, 0o000); err != nil {
		t.Fatalf("chmod aaa-broken: %v", err)
	}
	restorePermissionsForCleanup(t, brokenPath)

	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	// Partial failure: everything else stays watched in both the in-process
	// index and the backend, and only the broken path is pending retry.
	want := []string{root, zzzDir, zzzNested}
	pollUntil(t, "initial walk to finish with only the broken path unwatched", func() bool {
		return allWatched(r, want) && noneWatched(r, []string{brokenPath})
	})
	if got := watchListPaths(r); !samePathSet(got, watchMapPaths(r)) {
		t.Fatalf("backend watch list %v disagrees with the watch index %v", got, watchMapPaths(r))
	}
	if got := pendingWatchRetries(r); got[brokenPath] < 1 || len(got) != 1 {
		t.Fatalf("pending watch retries = %v; want only the failed path", got)
	}

	// Recovery: the failed path is retried from its current tree, the failed
	// subtree's app is reconciled, and the healthy sibling is not touched.
	if err := os.Chmod(brokenPath, 0o755); err != nil {
		t.Fatalf("restore aaa-broken: %v", err)
	}
	pollUntil(t, "recovered watch on the failed path", func() bool {
		return allWatched(r, append(want, brokenPath)) &&
			watchEntryIsCurrent(t, r, brokenPath) &&
			len(pendingWatchRetries(r)) == 0
	})
	assertRegistryPrepared(t, reg, "aaa-broken")
	waitForPrepare(t, b, 1)
	if reg.GetByName("zzz") != nil {
		t.Fatal("zzz must not be reconciled by the failed path's recovery")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenario 3 ---

// TestWatchExchangeRegistrationFailureRetriedOnCurrentTree pins the exchange
// hand-off: after the site maps to a new inode (an exchange-shaped replace),
// the refresh drops the detached old-tree watches, registers the watchable
// part of the CURRENT tree under the new identity, tracks exactly the
// unreadable subtree for retry, and the retry then registers that subtree from
// its current tree without resurrecting stale registrations.
func TestWatchExchangeRegistrationFailureRetriedOnCurrentTree(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	golfPath := filepath.Join(root, "golf")
	golfAttic := filepath.Join(golfPath, "attic")
	buildTree(t, root, "golf", "export function hi(e){}\n", "attic")

	r := fixtureWithoutLoops(t, root, &fakeBuilder{}, nil)
	b := r.builder.(*fakeBuilder)
	if !allWatched(r, []string{root, golfPath, golfAttic}) {
		t.Fatal("precondition: the old tree must be fully watched before the exchange")
	}

	// The exchange brings a tree whose "vault" subtree cannot be watched: the
	// permission failure is planted while the replacement tree is still in the
	// staging root and travels with the rotation.
	staging := t.TempDir()
	newTree := buildTree(t, staging, "golf", "export function hi(e){ console.log('v2'); }\n", "vault")
	if err := os.Chmod(filepath.Join(newTree, "vault"), 0o000); err != nil {
		t.Fatalf("chmod vault: %v", err)
	}
	// After the rotation the vault subtree lives at the EXCHANGED path; the
	// walk keys retry entries by the live path under the apps root.
	vaultPath := filepath.Join(golfPath, "vault")
	exchangeAppDirectory(t, golfPath, newTree)
	restorePermissionsForCleanup(t, vaultPath)
	driveExchangeNotification(r, golfPath)

	// After the refresh: the new site identity is watched, the detached attic
	// subtree is fully released, vault is unwatched but tracked for retry.
	want := map[string]struct{}{
		root:     {},
		golfPath: {},
	}
	pollUntil(t, "converged post-exchange watch state", func() bool {
		m := watchMapPaths(r)
		return samePathSet(m, want) && watchEntryIsCurrent(t, r, golfPath)
	})
	if got := watchListPaths(r); !samePathSet(got, want) {
		t.Fatalf("backend watch list after exchange = %v; want %v (no detached handles)", got, want)
	}
	if got := pendingWatchRetries(r); got[vaultPath] < 1 || len(got) != 1 {
		t.Fatalf("pending watch retries after exchange = %v; want only the failed subtree", got)
	}

	// Recovery: the subdir becomes watchable and the retry registers it
	// against the CURRENT tree; the reconstructed app is reconciled.
	if err := os.Chmod(vaultPath, 0o755); err != nil {
		t.Fatalf("restore vault: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	full := map[string]struct{}{
		root:      {},
		golfPath:  {},
		vaultPath: {},
	}
	pollUntil(t, "retry to register the recovered subtree on the current tree", func() bool {
		return samePathSet(watchMapPaths(r), full) && samePathSet(watchListPaths(r), full) &&
			watchEntryIsCurrent(t, r, vaultPath) &&
			len(pendingWatchRetries(r)) == 0
	})
	drainReconcileQueue(t, r)
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d after recovery; want exactly the exchange-triggered reconcile", b.prepares())
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenario 5 ---

// TestWatchRetryPendingDiscardedWhenPathRemoved pins the discard path: a
// pending registration retry for a path that disappears must be dropped when
// due (no infinite spin, no watch resurrection), and the removed non-app path
// must not be reconciled.
func TestWatchRetryPendingDiscardedWhenPathRemoved(t *testing.T) {
	root := t.TempDir()
	gonePath := writeFnDir(t, root, "gone")
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, nil, nil)

	// Synthetic registration failure against a real, watchable directory: the
	// origin does not matter for the discard behavior under test.
	r.recordWatchFailure(gonePath, errors.New("synthetic watcher add failure"))
	if got := pendingWatchRetries(r); got[gonePath] < 1 {
		t.Fatalf("pending watch retries = %v; want the failed path tracked", got)
	}

	if err := os.RemoveAll(gonePath); err != nil {
		t.Fatalf("remove gone: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	// The pending retry must be discarded rather than retried forever.
	pollUntil(t, "the removed path's retry to be discarded", func() bool {
		return len(pendingWatchRetries(r)) == 0 && noneWatched(r, []string{gonePath})
	})
	drainReconcileQueue(t, r)
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d; a removed non-app path must never be reconciled", b.prepares())
	}
	if reg.GetByName("gone") != nil {
		t.Fatal("a removed non-app path must never enter the registry")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenarios 6 and 11 ---

// mustWatchSet polls until both the watch index and the backend list hold
// exactly want, failing with a diff otherwise.
func mustWatchSet(t *testing.T, r *Reconciler, want map[string]struct{}) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("watch set to converge on %d paths", len(want)), func() bool {
		return samePathSet(watchMapPaths(r), want) && samePathSet(watchListPaths(r), want)
	})
}

// TestWatchRepeatedExchangeNotificationsConvergeWithoutDuplicates pins the
// duplicate-notification convergence: a repeated exchange notification pair
// for an already-current site converges on the new tree's watch set without
// retaining old watches or accumulating duplicate registrations, and ALL the
// notifications coalesce into a single debounced reconcile.
func TestWatchRepeatedExchangeNotificationsConvergeWithoutDuplicates(t *testing.T) {
	root := t.TempDir()
	golfPath := filepath.Join(root, "golf")
	golfAttic := filepath.Join(golfPath, "attic")
	buildTree(t, root, "golf", "export function hi(e){}\n", "attic")

	b := &fakeBuilder{}
	r := fixtureWithoutLoops(t, root, b, nil)
	if !allWatched(r, []string{root, golfPath, golfAttic}) {
		t.Fatal("precondition: the old tree must be fully watched before the exchange")
	}

	// The replacement tree is a different inode at the same path, with a
	// different subtree layout (attic gone, annex added).
	staging := t.TempDir()
	newTree := buildTree(t, staging, "golf", "export function hi(e){}\n", "annex")
	golfAnnex := filepath.Join(golfPath, "annex")
	exchangeAppDirectory(t, golfPath, newTree)

	// Two duplicate Rename+Create notification pairs for the same new inode.
	// After the first pair the site is already current; the second must be a
	// no-op side effect exactly like the first, never a stale or duplicated
	// registration.
	driveExchangeNotification(r, golfPath)
	driveExchangeNotification(r, golfPath)

	want := map[string]struct{}{
		root:      {},
		golfPath:  {},
		golfAnnex: {},
	}
	mustWatchSet(t, r, want)
	if !watchEntryIsCurrent(t, r, golfPath) || !watchEntryIsCurrent(t, r, golfAnnex) {
		t.Fatal("watch entries must refer to the current (new tree) inodes")
	}
	if len(pendingWatchRetries(r)) != 0 {
		t.Fatalf("retry state = %v; a converged refresh must clear its paths",
			pendingWatchRetries(r))
	}

	// The arming reconciliation runs through the real lifecycle so the pumped
	// name is joined before the test returns. All four handler enqueues
	// coalesce on the same debounce timer: exactly one reconcile fires.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	waitForPrepare(t, b, 1)
	drainReconcileQueue(t, r)
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d; repeated exchange notifications must coalesce into one reconcile", b.prepares())
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// TestWatchExchangeRefreshReconcilesDetachedChanges pins the exchange-path
// close of the inspect/register race: content that landed on the replacement
// tree while the old tree was still watched is reconciled through the handler
// run that re-registers the current tree — with one coalesced rebuild — even
// though no event for that content can ever be delivered on the watches that
// existed when it landed.
func TestWatchExchangeRefreshReconcilesDetachedChanges(t *testing.T) {
	root := t.TempDir()
	golfDir := writeFnDir(t, root, "golf") // v1 content, flat (no subdirs)
	golfPath := golfDir

	b := &fakeBuilder{}
	r := fixtureWithoutLoops(t, root, b, []*runner.PreparedApp{initialFn("golf", golfDir)})

	// The replacement tree carries v2 content that the currently installed
	// watches can never notify about: it existed before the site was rotated.
	staging := t.TempDir()
	newTree := buildTree(t, staging, "golf", "export function hi(e){ console.log('v2'); }\n", "annex")
	golfAnnex := filepath.Join(golfPath, "annex")
	exchangeAppDirectory(t, golfPath, newTree)

	// Only the site-level notification pair is delivered: every per-file
	// notification the old watches would have produced is lost with the
	// detached tree. The reconcile must still observe v2, driven by the
	// enqueues that follow the actual re-registration of the current tree.
	driveExchangeNotification(r, golfPath)

	// The arming reconciliation runs through the real lifecycle so the pumped
	// name and the watch maintenance are joined before the test ends.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	waitForPrepare(t, b, 1)
	drainReconcileQueue(t, r)
	if b.prepares() != 1 {
		t.Fatalf("prepares = %d; the re-registration enqueue must reconcile the changed inode once", b.prepares())
	}
	mustWatchSet(t, r, map[string]struct{}{
		root:      {},
		golfPath:  {},
		golfAnnex: {},
	})

	tmpl := mustParse(template)
	wantFP, fpErr := app.FingerprintApp(golfPath, tmpl)
	if fpErr != nil {
		t.Fatalf("fingerprint v2: %v", fpErr)
	}
	pollUntil(t, "the recorded built identity to be the replacement tree's content", func() bool {
		r.mu.Lock()
		stored := r.fingerprints["golf"]
		r.mu.Unlock()
		return stored == wantFP
	})
	if pf := r.reg.GetByName("golf"); pf == nil || pf.Prepared() == nil {
		t.Fatal("golf must be registered as prepared after the detached-change reconcile")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenario 7 (decisive retry-path form) ---

// TestWatchRecoveryReconcilesChangeLandedDuringDegradation pins the decisive
// order at the recovery entry point: when a watch registration retries, the
// registration of the CURRENT tree happens BEFORE the affected app is
// inspected and enqueued, so desired state that landed while the site was
// degraded and can only be seen through the new registration is reconciled
// immediately. A retry that only re-armed watches (and reconciled from a later
// event that never arrives, because the change hit no existing watch) would
// silently miss it until the next periodic audit.
//
// The construction is race-free by design: the replacement content is fully in
// place BEFORE any watch exists — no fsnotify event describes it and no debounced
// enqueue could race the recovery drive — and the seed is deliberately behind
// (empty), like an app whose startup scan could not hash the tree that finally
// arrived. Driving r.retryWatch is the exact recovery step the event loop's
// retry timer takes.
func TestWatchRecoveryReconcilesChangeLandedDuringDegradation(t *testing.T) {
	root := t.TempDir()
	hotelPath := writeFnDir(t, root, "hotel")
	hotel := initialFn("hotel", hotelPath)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{hotel}, nil)

	// The desired generation is ALREADY in place while the site's registration
	// is degraded: no watch ever delivered these bytes.
	if err := os.WriteFile(filepath.Join(hotelPath, "index.js"),
		[]byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write degraded-window content: %v", err)
	}
	// The seed is seeded from the FULL current tree first (as the worker's
	// seed step does), then the built identity is made stale. The failed
	// site's registration failure is only then tracked; the walk must have
	// completed before the failure is recorded (recordWatchFailure during the
	// walk would immediately be cleared by the same walk's success).
	r.Seed(hotel.App(), "")

	// The registration failure is tracked; the origin of one Add error is not
	// what this scenario pins, only the recovery that follows it.
	if err := r.PrepareWatch(context.Background()); err != nil {
		t.Fatalf("prepare watch: %v", err)
	}
	r.recordWatchFailure(hotelPath, errors.New("synthetic watcher add failure"))
	if got := pendingWatchRetries(r); got[hotelPath] != 1 || len(got) != 1 {
		t.Fatalf("pending watch retries = %v; want the degraded site tracked exactly once", got)
	}

	// Recovery retryWatch: the CURRENT tree is registered FIRST, and only then
	// is the app inspected and enqueued.
	r.retryWatch(hotelPath)
	if !allWatched(r, []string{hotelPath}) {
		t.Fatal("the retry must register the current tree at the degraded site")
	}
	if got := pendingWatchRetries(r); len(got) != 0 {
		t.Fatalf("retry state = %v; a successful registration must clear the retry", got)
	}

	// The recovery enqueue observes the degraded-window content immediately:
	// the only queued dispatch exists because of that enqueue.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	waitForPrepare(t, b, 1)
	assertRegistryPrepared(t, reg, "hotel")

	r.mu.Lock()
	stored := r.fingerprints["hotel"]
	r.mu.Unlock()
	wantFP, fpErr := app.FingerprintApp(hotelPath, mustParse(template))
	if fpErr != nil {
		t.Fatalf("fingerprint of the degraded-window content: %v", fpErr)
	}
	if stored != wantFP {
		t.Fatalf("recorded identity %q is not the degraded-window content's fingerprint %q", stored, wantFP)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenario 8 ---

// TestWatchGapErrorRestoresWatchAndReconciles pins recoverable-gap recovery:
// one rescan after a burst of backend errors restores a watch lost behind the
// backend's back with its current identity, one coalesced full-sweep request
// observes the delivery gap's changes, and the restored backend watch really
// delivers again.
func TestWatchGapErrorRestoresWatchAndReconciles(t *testing.T) {
	root := t.TempDir()
	indiaDir := writeFnDir(t, root, "india")
	nestedPath := filepath.Join(indiaDir, "nested")
	if err := os.MkdirAll(nestedPath, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	india := initialFn("india", indiaDir)
	b := &fakeBuilder{}
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{india})
	var logs testutil.SyncBuffer
	// A custom logger so the coalescing behavior of the recoverable-error
	// path is observable by counting the rescan warning.
	r := New(Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour},
		reg, b, slog.New(slog.NewTextHandler(&logs, nil)))
	seedCurrent(r, india.App())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	waitAllWatched(t, r, []string{root, indiaDir, nestedPath})

	// A delivery gap: the nested subtree's backend watch is lost behind the
	// rescaler's back (both in the in-process index and in the backend).
	if err := r.w.Remove(nestedPath); err != nil {
		t.Fatalf("simulate lost watch: %v", err)
	}
	r.watches.Delete(nestedPath)
	if _, ok := watchListPaths(r)[nestedPath]; ok {
		t.Fatal("precondition: the backend watch must actually be gone")
	}
	// Content changes during the gap: a plain overwrite is invisible to the
	// remaining watches, so observing it at all requires the gap recovery.
	if err := os.WriteFile(filepath.Join(indiaDir, "index.js"),
		[]byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2 during the gap: %v", err)
	}

	// An error storm (three recoverable backend errors in one burst) coalesces
	// into ONE rescan plus ONE full-sweep request.
	for i := 0; i < 3; i++ {
		injectWatchError(t, r, fsnotify.ErrEventOverflow)
	}

	// The rescan re-registers the missing subtree from its current inode.
	pollUntil(t, "the rescan to restore the lost watch", func() bool {
		m := watchMapPaths(r)
		_, okMap := m[nestedPath]
		_, okList := watchListPaths(r)[nestedPath]
		return okMap && okList && watchEntryIsCurrent(t, r, nestedPath)
	})
	// The gap's change is observed without waiting for a periodic tick.
	waitForPrepare(t, b, 1)
	drainReconcileQueue(t, r)
	if got := strings.Count(logs.String(), "Reconciler: watch error; rescanning"); got != 1 {
		t.Fatalf("rescan warnings = %d; one burst must coalesce into exactly one rescan", got)
	}

	// The restored backend watch really delivers again: a nested creation
	// reaches the app's reconcile.
	if err := os.WriteFile(filepath.Join(nestedPath, "later.js"), []byte("// later\n"), 0o644); err != nil {
		t.Fatalf("write nested file after recovery: %v", err)
	}
	waitForPrepare(t, b, 2)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on lifecycle cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}
}

// --- scenario 9 ---

// TestStartReturnsTerminalErrorOnWatcherClose pins the terminal path: an
// unexpected (non-context) close of the watcher's channels after Start is
// returned by Start as one of the terminal channel sentinels, and Start has
// already joined its loops when it does (its return is the join barrier).
func TestStartReturnsTerminalErrorOnWatcherClose(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "echo")
	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	waitAllWatched(t, r, []string{root})

	// The backend closes both channels outside a lifecycle cancellation.
	if err := r.w.Close(); err != nil {
		t.Fatalf("close watcher: %v", err)
	}

	select {
	case watchErr := <-errCh:
		if watchErr == nil {
			t.Fatal("Start returned nil after a terminal watcher close")
		}
		if watchErr != errWatchEventsClosed && watchErr != errWatchErrorsClosed {
			t.Fatalf("Start error = %v; want a terminal watcher/channel-close sentinel", watchErr)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return after the watcher closed")
	}

	// After Start returns, teardown is closed-channel safe (no send on a
	// closed channel from a late timer).
	r.Enqueue("echo")
}

// --- scenario 10 ---

// TestShutdownJoinsLoopsWithPendingWatchRetries pins shutdown with an
// unsatisfied retry: cancellation returns nil (normal cancellation, not a
// terminal failure), every loop is joined at Start's return, the pending
// retry state is preserved (never spun up nor silently dropped), and the
// watcher leases are released.
func TestShutdownJoinsLoopsWithPendingWatchRetries(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	limaPath := writeFnDir(t, root, "lima")
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, nil, nil)

	if err := os.Chmod(limaPath, 0o000); err != nil {
		t.Fatalf("chmod lima: %v", err)
	}
	restorePermissionsForCleanup(t, limaPath)
	if err := r.PrepareWatch(context.Background()); err != nil {
		t.Fatalf("prepare watch: %v", err)
	}
	if got := pendingWatchRetries(r); got[limaPath] < 1 {
		t.Fatalf("pending watch retries = %v; want the failed path tracked", got)
	}

	// The retries keep failing while the site stays unreadable, proving the
	// retry timer is live and looping right up to cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	pollUntil(t, "at least one in-loop retry attempt", func() bool {
		return pendingWatchRetries(r)[limaPath] >= 2
	})

	attemptsBefore := pendingWatchRetries(r)[limaPath]
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on cancellation with retries pending; shutdown must not be terminal", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}

	if n := len(r.w.WatchList()); n != 0 {
		t.Fatalf("backend watches = %d after shutdown; the leases must be released", n)
	}
	if got := pendingWatchRetries(r)[limaPath]; got < attemptsBefore {
		t.Fatalf("retry attempts = %d (before: %d); pending retry state must be preserved, not dropped",
			got, attemptsBefore)
	}
	r.mu.Lock()
	armed := len(r.timers)
	r.mu.Unlock()
	if armed != 0 {
		t.Fatalf("armed debounce timers = %d after shutdown; the timers must be stopped", armed)
	}
}

// --- scenario 11 ---

// TestWatchNoDuplicateRegistrationsOrGoroutineLeaks pins hygiene across
// repeated exchanges: successive synchronized rotations never retain old
// subtree watches, never accumulate duplicate registrations (index and backend
// agree exactly, one watch per live path), and after a full lifecycle the
// reconciler's goroutines are all joined by Start's return.
func TestWatchNoDuplicateRegistrationsOrGoroutineLeaks(t *testing.T) {
	root := t.TempDir()
	golfPath := filepath.Join(root, "golf")
	golfAttic := filepath.Join(golfPath, "attic")
	oldTree := buildTree(t, root, "golf", "export function hi(e){}\n", "attic")
	_ = oldTree

	r := fixtureWithoutLoops(t, root, &fakeBuilder{}, nil)
	baseline := runtime.NumGoroutine()

	const rounds = 3
	for round := 1; round <= rounds; round++ {
		staging := t.TempDir()
		sub := fmt.Sprintf("annex-%d", round)
		newTree := buildTree(t, staging, "golf", "export function hi(e){}\n", sub)
		exchangeAppDirectory(t, golfPath, newTree)
		driveExchangeNotification(r, golfPath)
	}

	// After the last round: exactly the current tree is watched — no attic
	// handle from the original tree, and no annex-1/annex-2 handles from the
	// replaced intermediate trees.
	last := filepath.Join(golfPath, fmt.Sprintf("annex-%d", rounds))
	for i := 1; i <= rounds; i++ {
		if i == rounds {
			continue
		}
		stale := filepath.Join(golfPath, fmt.Sprintf("annex-%d", i))
		if _, ok := watchMapPaths(r)[stale]; ok {
			t.Fatalf("round %d subtree %s still watched after its tree was replaced", i, stale)
		}
	}
	want := map[string]struct{}{
		root:     {},
		golfPath: {},
		last:     {},
	}
	mustWatchSet(t, r, want)
	if _, ok := watchMapPaths(r)[golfAttic]; ok {
		t.Fatal("the original tree's attic handle must not survive the rotations")
	}
	if !watchEntryIsCurrent(t, r, golfPath) {
		t.Fatal("the site watch must refer to the current inode, not a detached one")
	}

	// The live loop settles to the same exact set over the buffered real
	// events, then joins everything on cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()
	mustWatchSet(t, r, want)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start = %v on cancellation", err)
		}
	case <-time.After(watchSettleDeadline):
		t.Fatal("Start did not return on cancellation")
	}

	// All reconciler goroutines are joined: the count must settle at the
	// pre-test baseline (small tolerance for unrelated runtime goroutines).
	pollUntil(t, "goroutines to settle after Start joined its loops", func() bool {
		return runtime.NumGoroutine() <= baseline+3
	})
}

// --- scenario 12 ---

// TestWatchAmbiguousInspectionErrorKeepsRetryAndApp pins the ambiguous-error
// branch of retry eligibility: an inspection error that is NOT a definite
// absence (here a permission failure on the path's parent, the same class a
// transient I/O fault produces) keeps the retry pending instead of discarding
// it, does not destructively remove the surrounding watches, and lets the
// retry recover once the filesystem heals — while the active app entry is
// untouched the whole time.
func TestWatchAmbiguousInspectionErrorKeepsRetryAndApp(t *testing.T) {
	skipWithoutPosixPermissions(t)

	root := t.TempDir()
	kiloPath := writeFnDir(t, root, "kilo")
	newPath := filepath.Join(kiloPath, "unregistered")

	kilo := initialFn("kilo", kiloPath)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{kilo}, nil)
	// No Start here: the retries below are driven directly, so register the
	// closed-channel cleanup for the debounce timers this test arms.
	shutdownSkeletonReconciler(t, r)

	// The kilo site itself is watched synchronously; the fresh nested dir is
	// created only AFTER the walk, so nothing has registered it.
	if err := r.PrepareWatch(context.Background()); err != nil {
		t.Fatalf("prepare watch: %v", err)
	}
	if err := os.MkdirAll(newPath, 0o755); err != nil {
		t.Fatalf("mkdir unregistered: %v", err)
	}
	if !allWatched(r, []string{root, kiloPath}) || !noneWatched(r, []string{newPath}) {
		t.Fatal("precondition: kilo watched, the unregistered nested dir not")
	}

	// An ambiguous inspection failure appears (the parent dir becomes
	// unreadable, so even Lstat of the child fails with EACCES).
	if err := os.Chmod(kiloPath, 0o000); err != nil {
		t.Fatalf("chmod kilo: %v", err)
	}
	restorePermissionsForCleanup(t, kiloPath)
	_, openErr := os.Open(newPath)
	if openErr == nil {
		t.Skip("the platform did not enforce the permission failure")
	}
	r.recordWatchFailure(newPath, openErr)
	if got := pendingWatchRetries(r)[newPath]; got != 1 {
		t.Fatalf("retry attempts = %d after the ambiguous failure; the retry must be retained", got)
	}

	// A retry against the ambiguous site keeps the retry and increments it,
	// never discards it, and removes no existing watch.
	r.retryWatch(newPath)
	attempts, pending := r.watchAttempts(newPath)
	if attempts != 2 || !pending {
		t.Fatalf("watchAttempts = (%d, %v); an ambiguous failure must keep the retry pending", attempts, pending)
	}
	if !allWatched(r, []string{kiloPath}) {
		t.Fatal("the surrounding site's watch must not be destructively removed by an ambiguous retry")
	}
	if pf := reg.GetByName("kilo"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the active app entry must be untouched by watch-recovery bookkeeping")
	}

	// Once the filesystem heals, the retained retry registers the subtree
	// from its current tree and clears the pending retry.
	if err := os.Chmod(kiloPath, 0o755); err != nil {
		t.Fatalf("restore kilo: %v", err)
	}
	r.retryWatch(newPath)
	if _, pending := r.watchAttempts(newPath); pending {
		t.Fatal("the retry must be cleared once the subtree registers")
	}
	if !allWatched(r, []string{root, kiloPath, newPath}) ||
		!watchEntryIsCurrent(t, r, newPath) {
		t.Fatal("the retained retry must recover the subtree's watch")
	}
	if pf := reg.GetByName("kilo"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the active app entry must survive recovery")
	}
}
