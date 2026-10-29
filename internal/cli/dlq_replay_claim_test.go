package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/runtime"
	"relay/internal/worker"
)

// This file pins the CLI-side replay-claim contract `relay dlq replay` carries
// against the worker socket: admission happens before any dial and refuses a
// concurrent claimant, a success delete is fenced by the current claim token,
// definitive outcomes release the claim (so the entry is immediately
// re-replayable), ambiguous outcomes retain it until expiry, `rm` respects an
// active claim, and a disconnect never cancels the worker-side execution. The
// lease clock itself is exercised against real PostgreSQL (see the state
// package's replay-claim suite); here the fake store's claim model plus a
// blocking runner executor make the interleavings observable deterministically.

// blockingReplayExecutor is a runner.Executor that records the handler it ran
// and blocks inside Execute until Release is called, so a test can hold one
// CLI's replay mid-execution while another operation races the held claim. The
// executor deliberately ignores its context: the worker does not cancel an
// executing replay when the CLI disconnects, so the block is broken only by the
// test's release, never by cancellation.
type blockingReplayExecutor struct {
	mu          sync.Mutex
	handlers    []string
	completed   bool
	entered     chan struct{}
	release     chan struct{}
	onceEnter   sync.Once
	onceRelease sync.Once
}

func newBlockingReplayExecutor() *blockingReplayExecutor {
	return &blockingReplayExecutor{entered: make(chan struct{}), release: make(chan struct{})}
}

func (e *blockingReplayExecutor) Execute(
	_ context.Context, _ *runtime.Prepared, handler string, _ []byte, _ []string,
) error {
	e.mu.Lock()
	e.handlers = append(e.handlers, handler)
	e.mu.Unlock()
	e.onceEnter.Do(func() { close(e.entered) })
	<-e.release
	e.mu.Lock()
	e.completed = true
	e.mu.Unlock()
	return nil
}

// Release unblocks the executing handler (idempotent: every caller shares one
// close, so double cleanup is safe).
func (e *blockingReplayExecutor) Release() { e.onceRelease.Do(func() { close(e.release) }) }

func (e *blockingReplayExecutor) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-e.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the replay handler was never entered; nothing is executing")
	}
}

func (e *blockingReplayExecutor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.handlers)
}

func (e *blockingReplayExecutor) hasCompleted() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.completed
}

// runCLIContext is runCLIWithDeps with a caller-supplied context, so a test can
// cancel mid-command (the Ctrl-C path) instead of ending only with the test.
func runCLIContext(
	t *testing.T, ctx context.Context, deps Dependencies, stdin string, args ...string,
) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &out, deps)
	cmd.Reader = strings.NewReader(stdin)
	cmd.ErrWriter = &errOut
	err = cmd.Run(ctx, append([]string{"relay"}, args...))
	return out.String(), errOut.String(), err
}

// garbageResponseListener binds path with a raw socket that answers every
// exchange with an unparseable frame: the worker accepted the request, but the
// CLI cannot know whether a handler ran (a lost/malformed response). The
// response is written synchronously after reading one request line, so the test
// never depends on an exchange deadline.
func garbageResponseListener(t *testing.T, path string) {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen garbage socket: %v", err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = ln.Close()
		close(done)
		wg.Wait()
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				select {
				case <-done:
					return
				default:
				}
				_, _ = bufio.NewReader(conn).ReadString('\n')
				_, _ = io.WriteString(conn, "not-json\n")
			}()
		}
	}()
}

// TestDLQReplayNonReplayablePlaceholderReleasesClaim verifies the admission
// guard for a non-replayable entry happens AFTER the claim (so it can be
// released) and the entry is kept for inspection or removal.

// TestDLQReplayConcurrentSecondClaimRejectedBeforeExecution proves concurrent
// admission end to end: while the first CLI's replay executes (a handler
// blocked mid-flight), a second `relay dlq replay` of the same entry is refused
// by the store claim BEFORE it dials the worker, so the handler can never
// execute twice. The first replay then completes and deletes the entry.
func TestDLQReplayConcurrentSecondClaimRejectedBeforeExecution(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
	}}
	exec := newBlockingReplayExecutor()
	deps := dlqReplayRunnerWithExecutor(t, store, exec)
	t.Cleanup(exec.Release)

	// Given: the first CLI holds its claim; the handler is executing and
	// blocked.
	firstDone := make(chan error, 1)
	firstOut := make(chan string, 1)
	go func() {
		out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
		firstOut <- out
		firstDone <- err
	}()
	exec.waitEntered(t)
	if _, ok := store.heldToken("1-0"); !ok {
		t.Fatal("a claim must be held while the handler is executing")
	}

	// When: a second CLI replays the same entry concurrently.
	_, _, secondErr := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")

	// Then: the second claimant is refused without dialing — the exact handler
	// still executes exactly once.
	if secondErr == nil || !strings.Contains(secondErr.Error(), "replay is already in progress") {
		t.Fatalf("second replay err = %v, want the replay-in-progress refusal", secondErr)
	}
	if got := exec.count(); got != 1 {
		t.Fatalf("executed handlers = %d, want exactly 1 (the second claimant never executed)", got)
	}
	if _, ok := store.heldToken("1-0"); !ok {
		t.Fatal("the refused claimant must leave the first claimant's claim held")
	}

	// Finally: the first replay finishes and deletes the entry.
	exec.Release()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first replay err = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first replay never finished after release")
	}
	if out := <-firstOut; strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("first replay stdout = %q", out)
	}
	if got := exec.count(); got != 1 {
		t.Fatalf("executed handlers = %d after completion, want exactly 1", got)
	}
	if len(store.entries) != 0 {
		t.Fatalf("a successful replay must delete the entry: %+v", store.entries)
	}
}

// TestDLQReplaySuccessDeleteRequiresCurrentClaimToken proves the success delete
// is fenced by the claim token: when the claim is superseded while the handler
// is in flight (the lease expired and a newer claimant re-claimed), the
// successful handler must not delete the row a newer claimant owns — the entry
// is retained and the CLI says so.
func TestDLQReplaySuccessDeleteRequiresCurrentClaimToken(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
	}}
	rep := &recordingReplayer{onReplay: func() { store.supersedeClaim("1-0") }}
	deps := withFakeDLQ(t, store)
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil {
		t.Fatal("a superseded claim must not delete the entry on success")
	}
	for _, want := range []string{
		"handler succeeded but the DLQ entry was retained",
		dlq.ErrReplayClaimLost.Error(),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want it to mention %q", err, want)
		}
	}
	if calls, _, _ := rep.got(); calls != 1 {
		t.Fatalf("replayer calls = %d, want the single superseded execution", calls)
	}
	if len(store.entries) != 1 {
		t.Fatalf("the superseded claimant must retain the entry: %+v", store.entries)
	}
}

// TestDLQReplayDefiniteWorkerFailureReleasesClaim proves a worker-reported
// failure (the handler ran and failed) releases the claim: the entry stays, and
// the correct token was presented so a follow-up replay can be admitted again
// immediately (no lease-expiry wait).
func TestDLQReplayDefiniteWorkerFailureReleasesClaim(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{fail: true}
	deps := dlqReplayRunner(t, store, exec)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "handler boom") {
		t.Fatalf("err = %v, want the handler failure surfaced", err)
	}
	if got := store.releasedTokensFor("1-0"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("released tokens = %v, want exactly the held token [1]", got)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a failed handler must keep the entry: %+v", store.entries)
	}
	// Release makes the entry re-admittable without any expiry wait.
	if _, err := store.ClaimDLQReplay(context.Background(), "1-0", time.Minute); err != nil {
		t.Fatalf("re-claim after release: %v, want immediate admittance", err)
	}
}

// TestDLQReplayNotStartedReleasesClaim proves a request that definitively never
// started (the worker socket is absent, so the dial failed) releases the claim
// too: nothing can have executed, and the entry must be immediately
// re-replayable once a worker exists.
func TestDLQReplayNotStartedReleasesClaim(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	// No socket server is started at deps.SocketPath: the dial fails.
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "replay 1-0") {
		t.Fatalf("err = %v, want the unavailable-worker replay error", err)
	}
	if got := store.releasedTokensFor("1-0"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("released tokens = %v, want a release of the claim token [1]", got)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a not-started replay must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayNoReplayerAnswerReleasesClaim proves the worker's explicit
// no-replayer answer (invoke_unavailable, refused before any dispatch) is the
// other definite not-started outcome: the claim is released and the entry kept.
func TestDLQReplayNoReplayerAnswerReleasesClaim(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)
	startTestSocketWithReplayer(t, deps.SocketPath, nil) // the socket is up but serves no replayer

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "replay 1-0") {
		t.Fatalf("err = %v, want the unavailable-replayer replay error", err)
	}
	if got := store.releasedTokensFor("1-0"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("released tokens = %v, want a release of the claim token [1]", got)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a refused-before-dispatch replay must keep the entry: %+v", store.entries)
	}
}

// TestDLQReplayMalformedResponseRetainsClaim proves an ambiguous exchange — a
// worker that accepted the request but whose response is lost/malformed —
// retains the claim: nothing is released, the entry stays, and the entry is
// STILL blocked, so the operator cannot start a second execution while the
// first may be running.
func TestDLQReplayMalformedResponseRetainsClaim(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)
	garbageResponseListener(t, deps.SocketPath)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "replay 1-0") {
		t.Fatalf("err = %v, want the replay exchange error", err)
	}
	if got := store.releasedTokensFor("1-0"); len(got) != 0 {
		t.Fatalf("an ambiguous exchange must not release the claim: released %v", got)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an ambiguous exchange must keep the entry: %+v", store.entries)
	}
	if _, ok := store.heldToken("1-0"); !ok {
		t.Fatal("the claim must still be held after an ambiguous exchange")
	}
	// The retained claim blocks a retry until the lease expires.
	_, _, retryErr := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if retryErr == nil || !strings.Contains(retryErr.Error(), "replay is already in progress") {
		t.Fatalf("retry err = %v, want the replay-in-progress refusal while held", retryErr)
	}
}

// TestDLQReplayClientCancelRetainsClaimButHandlerStillExecutes proves the
// Ctrl-C path end to end: cancelling the CLI mid-exchange retains the claim
// (the outcome is ambiguous), reports the cancellation, and — because the
// worker does not cancel execution on disconnect — the handler keeps running on
// the worker side. Release unblocks it during the same test so coverage stays
// leak-free.
func TestDLQReplayClientCancelRetainsClaimButHandlerStillExecutes(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
	}}
	exec := newBlockingReplayExecutor()
	deps := dlqReplayRunnerWithExecutor(t, store, exec)
	t.Cleanup(exec.Release)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := runCLIContext(t, ctx, deps, "", "dlq", "replay", "1-0")
		done <- err
	}()
	exec.waitEntered(t)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, worker.ErrInvokeUnavailable) {
			t.Fatalf("err = %v, want the ambiguous-unavailable classification", err)
		}
		// The exchange error carries the cancellation in its text (the worker
		// side may still be running, so the classification is the ambiguous
		// unavailable one). NOTE: it is formatted with %v, so the chain does
		// not unwrap to context.Canceled; that formatting is implementation
		// detail, surfaced to an operator verbatim.
		if !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("err = %v, want the cancellation surfaced verbatim", err)
		}
		if errors.Is(err, worker.ErrInvokeNotStarted) || errors.Is(err, worker.ErrInvokeFailed) {
			t.Fatalf("err = %v, a cancelled in-flight replay is neither not-started nor failed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the replay did not abort promptly on client cancellation")
	}

	// The disconnect must not have cancelled the worker-side execution.
	time.Sleep(100 * time.Millisecond)
	if exec.hasCompleted() {
		t.Fatal("a client disconnect must not cancel the worker-side execution")
	}

	// The ambiguous outcome retains the claim: no release, entry kept, retry
	// blocked.
	if got := store.releasedTokensFor("1-0"); len(got) != 0 {
		t.Fatalf("a client-cancelled replay must not release the claim: released %v", got)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a client-cancelled replay must keep the entry: %+v", store.entries)
	}
	if _, ok := store.heldToken("1-0"); !ok {
		t.Fatal("the claim must still be held while the ambiguous execution may run")
	}

	// Unblock the (still running) handler so the socket handler goroutine
	// exits cleanly before cleanup closes the socket server.
	exec.Release()
}

// TestDLQReplayRmRefusesActiveClaimAndFreesAfterRelease proves `relay dlq rm`
// refuses an entry whose replay claim is active (the replay may still be
// executing) and that a released claim makes the entry removable again.
func TestDLQReplayRmRefusesActiveClaimAndFreesAfterRelease(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry("1-0", "1-0", "user-events", "events.created.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	// Given: a concurrent replay holds the claim (what an in-flight
	// `relay dlq replay` leaves behind).
	claim, err := store.ClaimDLQReplay(context.Background(), "1-0", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	_, _, err = runCLIWithDeps(t, deps, "", "dlq", "rm", "1-0")
	if err == nil || !strings.Contains(err.Error(), "has an active replay claim") {
		t.Fatalf("rm err = %v, want the active-claim refusal", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("the claimed entry must survive the refused rm: %+v", store.entries)
	}

	// When the claim is released, the entry is removable again.
	if err := store.ReleaseDLQReplay(context.Background(), "1-0", claim.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", "1-0")
	if err != nil || strings.TrimSpace(out) != "DLQ entry removed" {
		t.Fatalf("rm after release = (%q, %v), want removal", out, err)
	}
	if len(store.entries) != 0 {
		t.Fatalf("the released entry must be removable: %+v", store.entries)
	}
}

// TestDLQReplayNonReplayablePlaceholderReleasesClaim verifies the admission
// guard for a non-replayable entry happens AFTER the claim (so it can be
// released) and the entry is kept for inspection or removal.
func TestDLQReplayNonReplayablePlaceholderReleasesClaim(t *testing.T) {
	placeholder := dlqTestEntry("1-0", "1-0", "-", "-", "-", "2026-09-23T10:00:00Z")
	placeholder.HandlerAttempts = 0
	store := &fakeDLQStore{entries: []dlq.Entry{placeholder}}
	deps := withFakeDLQ(t, store)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", "1-0")
	if err == nil || !strings.Contains(err.Error(), "is not replayable") {
		t.Fatalf("err = %v, want the non-replayable error", err)
	}
	if got := store.releasedTokensFor("1-0"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("released tokens = %v, want the rejected claim released [1]", got)
	}
	if len(store.entries) != 1 {
		t.Fatalf("a non-replayable entry must be kept: %+v", store.entries)
	}
}
