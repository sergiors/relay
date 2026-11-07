// Fixture-build leak-check helpers for the runtime suite, behind the same
// fail-closed contract the runner suite's helper implements. They are
// deliberately NOT behind the integration tag: the bounded-poll verdict is
// daemon-independent (its container reads are behind a lister seam), so its
// contract is unit-tested without a daemon, while the client wrapper carries
// the real read for the integration-tagged no-leak contract test.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"
)

// awaitNoFixtureIntermediate polls (bounded) until an IN-WINDOW successful
// daemon read resolves whether any container whose command carries nonce
// exists, returning the observed matches and, when the window closed without
// a single successful read, a fail-closed error for the caller to fail on. It
// is the leak check behind the runtime no-leak fixture-build tests.
//
// Verdict semantics are fail-closed:
//
//   - a successful read with matches returns them immediately: nonce matches
//     are terminal state (the nonce is unique per build and nothing removes a
//     just-built leak), so a regression fails fast instead of paying the
//     window;
//   - a successful read with no matches returns (nil, nil) — the only verdict
//     that lets a caller pass. "No leak" is a positive observation, never an
//     inference from unreadable silence;
//   - container-read failures never satisfy the wait: they are retried until
//     the window elapses, so a daemon that cannot be read errors the test
//     instead of passing or failing it misleadingly;
//   - every read is issued with a context bounded by min(parent deadline,
//     this window) and canceled after the attempt. The read is synchronous
//     and MUST honor its context — the real ContainerList does. WithDeadline
//     signals cancellation at the bound but cannot preempt a read that
//     ignores it; the guard is that any result such a read still returns is
//     gated once it returns on the same effective bound (the window, or an
//     earlier parent deadline) AND on parent termination, and anything
//     obtained after either is rejected — so a late read can never deliver a
//     passing verdict, though only a context-honoring read is prevented from
//     holding the check open past the stated timeout.
func awaitNoFixtureIntermediate(ctx context.Context, cli *client.Client, nonce string, timeout time.Duration) ([]string, error) {
	return awaitFixtureListVerdict(ctx, timeout, func(readCtx context.Context) ([]string, error) {
		list, err := cli.ContainerList(readCtx, client.ContainerListOptions{All: true})
		if err != nil {
			return nil, err
		}
		var matched []string
		for _, c := range list.Items {
			if strings.Contains(c.Command, nonce) {
				matched = append(matched, fmt.Sprintf("id=%s command=%q", c.ID[:12], c.Command))
			}
		}
		return matched, nil
	})
}

// fixtureWindowClosed is the leak-check window's single boundary predicate:
// a read result is unobservable-in-window (rejected) exactly when the window
// deadline has been reached, inclusive — now == deadline is closed, anything
// strictly before is an in-window observation. Being a pure comparison, both
// inclusive-boundary sides are unit-testable without wall-clock or sleeps.
func fixtureWindowClosed(now, deadline time.Time) bool {
	return !now.Before(deadline)
}

// awaitFixtureListVerdict is the daemon-independent core of the leak check:
// it polls read until it yields a successful IN-WINDOW verdict or the window
// elapses. A nil error is always a verdict — non-nil matches fail the caller,
// nil matches pass it — and verdicts are only ever read from the window's
// inside:
//
//   - each read is issued with a context whose deadline IS the window edge
//     via WithDeadline(parent, window deadline) — the earlier of the two when
//     the parent has one — and canceled as soon as the attempt returns. The
//     read is synchronous and MUST honor its context (the real Docker
//     ContainerList does): WithDeadline signals cancellation at the window
//     edge but cannot preempt a read that ignores it — the guard against such
//     a read is the late-result rejection below, which applies once it
//     returns;
//   - any result is rejected once it arrives outside the EFFECTIVE bound —
//     the window edge or an earlier parent deadline — or after the parent
//     context ended: a read that went past its bounded context must not
//     deliver a passing verdict by outliving either. Matches — and any read
//     error — stay visible through the returned error;
//   - a read error is never a verdict: the poll retries it with context-aware
//     waits (the suite's shared pollInterval), and only when the window closes
//     without a single in-window successful read does it surface that for the
//     caller to fail closed on.
//
// The read runs on the calling goroutine only.
func awaitFixtureListVerdict(parent context.Context, timeout time.Duration, read func(context.Context) ([]string, error)) ([]string, error) {
	const windowClosed = "no in-window container read: the fixture leak-check window elapsed"
	deadline := time.Now().Add(timeout)
	// The binding bound is the EFFECTIVE deadline: the earlier of this window
	// and any parent deadline — exactly what the read context carries — and
	// the parent ending (canceled or past its own deadline) ends the check as
	// well. WithDeadline applies the bound to every read's context, and every
	// RESULT is gated on the same effective bound identically, so a
	// successful-looking result can never be accepted from outside it.
	effective := deadline
	if pDeadline, ok := parent.Deadline(); ok && pDeadline.Before(deadline) {
		effective = pDeadline
	}
	var lastReadErr error // most recent in-window read failure, folded into the final window error
	for {
		if fixtureWindowClosed(time.Now(), effective) {
			if lastReadErr != nil {
				return nil, fmt.Errorf("%s (last read error: %w)", windowClosed, lastReadErr)
			}
			return nil, errors.New(windowClosed)
		}

		// WithDeadline(parent, deadline) is the helper window edge itself —
		// no sampled-remaining step to drift it — and automatically keeps an
		// earlier parent deadline when one exists. The cancel is
		// unconditional so an abandoned read's context never outlives its
		// attempt.
		readCtx, cancel := context.WithDeadline(parent, deadline)
		matched, err := read(readCtx)
		cancel()

		// Gate the just-obtained result against the SAME effective bound the
		// read context carries, and against parent termination (canceled or
		// past its own deadline): a read that went past its bounded context
		// and returned afterward gets its result rejected here — never a
		// passing verdict; matches — and any read error — stay visible
		// through the returned error.
		if parentErr := parent.Err(); parentErr != nil || fixtureWindowClosed(time.Now(), effective) {
			if len(matched) > 0 {
				if err != nil {
					return nil, fmt.Errorf("read result rejected as unobservable-in-window, reporting %d matching container(s) (last read error: %w): %v", len(matched), err, matched)
				}
				return nil, fmt.Errorf("read result rejected as unobservable-in-window, reporting %d matching container(s): %v", len(matched), matched)
			}
			if parentErr != nil {
				return nil, fmt.Errorf("no in-window container read before the parent context ended: %w", parentErr)
			}
			if err != nil {
				return nil, fmt.Errorf("%s (last read error: %w)", windowClosed, err)
			}
			return nil, errors.New(windowClosed)
		}
		if err == nil {
			return matched, nil
		}
		lastReadErr = err

		// An in-window read failure is not a verdict: retry until the window
		// closes. The pre-read remaining window is stale here — this read
		// consumed part of it — so recompute; the gate above guarantees time
		// is left, and stopping explicitly when none remains keeps the retry
		// from sleeping past the window edge.
		remaining := time.Until(effective)
		if remaining <= 0 {
			return nil, fmt.Errorf("%s (last read error: %w)", windowClosed, lastReadErr)
		}
		wait := pollInterval
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-parent.Done():
			timer.Stop()
			return nil, fmt.Errorf("container list interrupted: %w", parent.Err())
		case <-timer.C:
		}
	}
}

func TestAwaitFixtureListVerdictCleanObservationPassesImmediately(t *testing.T) {
	reads := 0
	leaked, err := awaitFixtureListVerdict(context.Background(), 5*time.Second, func(context.Context) ([]string, error) {
		reads++
		return nil, nil
	})
	if err != nil {
		t.Fatalf("a successful read with no matches must be the clean verdict: %v", err)
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil", leaked)
	}
	if reads != 1 {
		t.Fatalf("read issued %d times, want 1: the first clean observation is the verdict", reads)
	}
}

func TestAwaitFixtureListVerdictLeakObservationFailsFastWithMatches(t *testing.T) {
	reads := 0
	want := []string{`id=0123456789ab command="/bin/sh -c '#(nop) ' 'CMD []'"`}
	leaked, err := awaitFixtureListVerdict(context.Background(), 5*time.Second, func(context.Context) ([]string, error) {
		reads++
		return want, nil
	})
	if err != nil {
		t.Fatalf("a successful read with matches must carry no list error: %v", err)
	}
	if len(leaked) != len(want) || leaked[0] != want[0] {
		t.Fatalf("leaked = %v, want %v", leaked, want)
	}
	if reads != 1 {
		t.Fatalf("read issued %d times, want 1: the first leak observation is the verdict", reads)
	}
}

func TestAwaitFixtureListVerdictFailsClosedWhenReadsNeverSucceed(t *testing.T) {
	// The window closes while every read failed: the check must surface an
	// error for the caller to fail on, not a nil matches slice that reads as
	// "no leak".
	reads := 0
	leaked, err := awaitFixtureListVerdict(context.Background(), 80*time.Millisecond, func(context.Context) ([]string, error) {
		reads++
		return nil, errors.New("daemon unreachable")
	})
	if err == nil {
		t.Fatal("a window with only failed reads must fail closed with an error, got nil")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil: a failed read never observed any match", leaked)
	}
	if reads < 1 {
		t.Fatalf("read issued %d times, want >= 1", reads)
	}
}

func TestAwaitFixtureListVerdictRetriesAfterFailedReads(t *testing.T) {
	// A failed read is never a verdict: the wait must keep polling until a
	// successful in-window read resolves clean or leak. Both continuations are
	// pinned so a future change that stops on the first error or treats it as
	// clean fails here.
	t.Run("error then clean", func(t *testing.T) {
		reads := 0
		leaked, err := awaitFixtureListVerdict(context.Background(), 5*time.Second, func(context.Context) ([]string, error) {
			reads++
			if reads == 1 {
				return nil, errors.New("daemon unreachable")
			}
			return nil, nil
		})
		if err != nil {
			t.Fatalf("a clean read after failed reads must be the verdict: %v", err)
		}
		if leaked != nil {
			t.Fatalf("leaked = %v, want nil", leaked)
		}
	})
	t.Run("error then leak", func(t *testing.T) {
		reads := 0
		leaked, err := awaitFixtureListVerdict(context.Background(), 5*time.Second, func(context.Context) ([]string, error) {
			reads++
			if reads == 1 {
				return nil, errors.New("daemon unreachable")
			}
			return []string{`id=0123456789ab command="leak"`}, nil
		})
		if err != nil {
			t.Fatalf("a leak read after failed reads must carry no list error: %v", err)
		}
		if len(leaked) != 1 {
			t.Fatalf("leaked = %v, want the observed match", leaked)
		}
	})
}

func TestAwaitFixtureListVerdictBoundsStalledReadsToTheWindow(t *testing.T) {
	// A read that waits only for its context cancellation must be cut off at
	// the window edge and must not hold the check open past the helper's
	// timeout. The read's context must carry the helper's deadline (this
	// parent has none, so only the helper can supply it), and the 10s fallback
	// timer below keeps a regressed helper — one that passed the unbounded
	// parent through — failing in this test instead of hanging to the package
	// timeout.
	const timeout = 80 * time.Millisecond
	var gotDeadline bool
	reads := 0
	start := time.Now()
	leaked, err := awaitFixtureListVerdict(context.Background(), timeout, func(readCtx context.Context) ([]string, error) {
		reads++
		if dl, ok := readCtx.Deadline(); ok {
			gotDeadline = dl.Before(time.Now().Add(10 * time.Second))
		}
		select {
		case <-readCtx.Done():
			// A context-honoring stalled read: released at the window edge.
			return nil, readCtx.Err()
		case <-time.After(10 * time.Second):
			// A regressed helper would land here unbounded.
			return nil, nil
		}
	})
	if err == nil {
		t.Fatal("a stalled bounded read must end in a window error, not a verdict")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil", leaked)
	}
	if !gotDeadline {
		t.Fatal("the read received a context with no (near-term) deadline; the helper must bound every read by min(parent, window)")
	}
	if reads != 1 {
		t.Fatalf("read issued %d times, want 1: the window error is returned at the deadline, not by retrying past it", reads)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("the check ran for %v, far beyond its %v window; a stalled read must be cut off, not waited out", elapsed, timeout)
	}
}

func TestAwaitFixtureListVerdictRejectsALateCleanRead(t *testing.T) {
	// A read that ignores its context entirely and returns a clean-looking
	// result only after the window has closed must NOT pass the caller: there
	// was never an in-window observation, so "no leak" cannot be inferred from
	// it. Deterministic: the read's 150ms return always lands after the 80ms
	// window.
	const timeout = 80 * time.Millisecond
	reads := 0
	start := time.Now()
	leaked, err := awaitFixtureListVerdict(context.Background(), timeout, func(context.Context) ([]string, error) {
		reads++
		<-time.After(150 * time.Millisecond) // ignores its bounded context
		return nil, nil                      // late, clean-looking
	})
	if err == nil {
		t.Fatal("a clean result returned after the window elapsed must be rejected; there was no in-window observation")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil", leaked)
	}
	if reads != 1 {
		t.Fatalf("read issued %d times, want 1: a late result is rejected outright, never retried past the window", reads)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("the check ran for %v for a deterministic 150ms late read", elapsed)
	}
}

func TestAwaitFixtureListVerdictKeepsALateLeakVisibleAsFailure(t *testing.T) {
	// A late read reporting matches can never pass either — the result is
	// rejected — but the matches stay visible in the returned error so the
	// leak is diagnosable rather than silently swallowed by the timeout.
	leaked, err := awaitFixtureListVerdict(context.Background(), 80*time.Millisecond, func(context.Context) ([]string, error) {
		<-time.After(150 * time.Millisecond) // ignores its bounded context
		return []string{`id=0123456789ab command="late leak"`}, nil
	})
	if err == nil {
		t.Fatal("a late read reporting matches must fail the caller, not pass")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil: a late result is rejected as a verdict, not returned", leaked)
	}
}

// TestAwaitFixtureListVerdictPerReadBoundaries pins the read-context boundary
// contract deterministically: WithDeadline(parent, window) gives every read A
// deadline that is exactly the earlier of the two (pinned by Time.Equal
// against the parent's, no wall-clock slop), and the window's closure is an
// inclusive boundary pinned by the pure fixtureWindowClosed predicate — a
// result at the deadline instant is rejected, one nanosecond before is an
// in-window observation — with no sleeps or live timing windows.
func TestAwaitFixtureListVerdictPerReadBoundaries(t *testing.T) {
	t.Run("read context deadline is exactly an earlier parent deadline", func(t *testing.T) {
		// Nothing here depends on a scheduling window: the parent's deadline
		// is generous (5s) and still the EARLIER bound (helper window 10s),
		// so min semantics must give the read context the parent's deadline
		// exactly; the read returns immediately, so the helper is never
		// racing to observe a short-lived deadline and the immediate clean
		// read is in-window by construction.
		parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
		defer cancel()
		parentDeadline, _ := parent.Deadline()
		reads := 0
		var firstReadDeadline time.Time
		var gotDeadline bool
		leaked, err := awaitFixtureListVerdict(parent, 10*time.Second, func(readCtx context.Context) ([]string, error) {
			reads++
			if dl, ok := readCtx.Deadline(); ok {
				firstReadDeadline = dl
				gotDeadline = true
			}
			return nil, nil
		})
		if err != nil {
			t.Fatalf("an immediate clean read inside both bounds must pass: %v", err)
		}
		if leaked != nil {
			t.Fatalf("leaked = %v, want nil", leaked)
		}
		if !gotDeadline {
			t.Fatal("the read received a context with no deadline; the helper must bound every read")
		}
		if !firstReadDeadline.Equal(parentDeadline) {
			t.Fatalf("read context deadline %v is not exactly the earlier parent deadline %v", firstReadDeadline, parentDeadline)
		}
		if reads != 1 {
			t.Fatalf("read issued %d times, want 1: the prompt clean read is the verdict", reads)
		}
	})
	t.Run("window closure is inclusive of the deadline instant", func(t *testing.T) {
		// Pure predicate, no wall time and no sleeps: the classification the
		// post-read rejection uses treats a result returned at the deadline
		// instant as unobservable-in-window and one nanosecond before as an
		// in-window observation.
		deadline := time.Now()
		if !fixtureWindowClosed(deadline, deadline) {
			t.Fatal("a result returned exactly at the window deadline must be classified closed (late/rejected), not in-window")
		}
		if fixtureWindowClosed(deadline.Add(-time.Nanosecond), deadline) {
			t.Fatal("a result returned one nanosecond before the deadline must be classified in-window")
		}
	})
}

func TestAwaitFixtureListVerdictKeepsALateMatchAndErrorDiagnosable(t *testing.T) {
	// A late read reporting BOTH matches and an error must preserve the read
	// error in the returned diagnostic rather than dropping it: the matches
	// make the check fail either way, and the read error must survive for the
	// operator.
	leaked, err := awaitFixtureListVerdict(context.Background(), 80*time.Millisecond, func(context.Context) ([]string, error) {
		<-time.After(150 * time.Millisecond) // ignores its bounded context
		return []string{`id=0123456789ab command="late leak"`}, errors.New("daemon unreachable mid-read")
	})
	if err == nil {
		t.Fatal("a late read must be rejected as a verdict, not pass")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil: the late result is rejected, matches go into the returned error", leaked)
	}
	if !strings.Contains(err.Error(), "daemon unreachable mid-read") {
		t.Fatalf("the returned diagnostic dropped the read error: %v", err)
	}
	if !strings.Contains(err.Error(), "1 matching container(s)") {
		t.Fatalf("the returned diagnostic dropped the matches: %v", err)
	}
}

// TestAwaitFixtureListVerdictStopsPollingWhenParentIsCanceled verifies that
// the retry wait must be context-aware: with the parent already canceled, the
// first read fails and the wait must end the check immediately instead of
// sleeping out the window.
func TestAwaitFixtureListVerdictStopsPollingWhenParentIsCanceled(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	reads := 0
	start := time.Now()
	leaked, err := awaitFixtureListVerdict(parent, 5*time.Second, func(readCtx context.Context) ([]string, error) {
		reads++
		select {
		case <-readCtx.Done():
			// A canceled parent cancels the read context too.
			return nil, readCtx.Err()
		case <-time.After(5 * time.Second):
			return nil, nil
		}
	})
	if err == nil {
		t.Fatal("a parent-canceled check must fail, not pass")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil", leaked)
	}
	if reads != 1 {
		t.Fatalf("read issued %d times after parent cancellation, want 1: the retry wait must honor parent.Done", reads)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the check ran for %v after an already-canceled parent", elapsed)
	}
}

// TestAwaitFixtureListVerdictRejectsCleanAfterParentEnded pins the
// effective-bound gate: a result is rejected whenever the parent context has
// ended — canceled or past its own deadline — even when the read ignored its
// context and returned a clean-looking result while the helper's own window
// is still open. Both subtests are deterministic with no sleeps and no wall
// clocks: ordering is pinned by channel happens-before (or by canceling
// before the call), never by elapsed time.
func TestAwaitFixtureListVerdictRejectsCleanAfterParentEnded(t *testing.T) {
	t.Run("already-canceled parent, context-ignoring clean read", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		leaked, err := awaitFixtureListVerdict(parent, 5*time.Second, func(context.Context) ([]string, error) {
			// Ignores its context: returns a clean-looking result although
			// the parent was already canceled before the read was issued.
			return nil, nil
		})
		if err == nil {
			t.Fatal("a clean result obtained after the parent ended must be rejected, not pass")
		}
		if leaked != nil {
			t.Fatalf("leaked = %v, want nil", leaked)
		}
		if !strings.Contains(err.Error(), "parent context ended") {
			t.Fatalf("the failure must name the parent as the reason, got: %v", err)
		}
	})
	t.Run("parent canceled mid-read, context-ignoring clean read", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		release := make(chan struct{})
		go func() {
			<-started
			cancel()
			close(release)
		}()
		leaked, err := awaitFixtureListVerdict(parent, time.Minute, func(context.Context) ([]string, error) {
			close(started)
			<-release
			// A clean-looking result, but obtained strictly after the parent
			// was canceled (channel ordering) — must never pass.
			return nil, nil
		})
		if err == nil {
			t.Fatal("a clean result obtained after the parent was canceled mid-read must be rejected, not pass")
		}
		if leaked != nil {
			t.Fatalf("leaked = %v, want nil", leaked)
		}
		if !strings.Contains(err.Error(), "parent context ended") {
			t.Fatalf("the failure must name the parent as the reason, got: %v", err)
		}
	})
}

// TestAwaitFixtureListVerdictStopsRetryingAtTheWindow pins the retry
// expiration: the wait between attempts must be recomputed AFTER each
// in-window failed read, and the loop must end with the window's error — the
// last read error folded in — when the window closes, never with an
// indefinite retry around the edge or a fabricated pass. Margins are
// generous (a 400ms read inside a 500ms window), so no assertion is a tight
// wall-clock comparison.
func TestAwaitFixtureListVerdictStopsRetryingAtTheWindow(t *testing.T) {
	reads := 0
	leaked, err := awaitFixtureListVerdict(context.Background(), 500*time.Millisecond, func(context.Context) ([]string, error) {
		reads++
		if reads == 1 {
			// Consume most of the window, then fail inside it.
			<-time.After(400 * time.Millisecond)
		}
		return nil, errors.New("daemon unreachable")
	})
	if err == nil {
		t.Fatal("a window that closes during failing reads must fail closed with the window error")
	}
	if leaked != nil {
		t.Fatalf("leaked = %v, want nil", leaked)
	}
	if !strings.Contains(err.Error(), "no in-window container read") {
		t.Fatalf("the final failure must be the window error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "daemon unreachable") {
		t.Fatalf("the final window error must fold the last read error, got: %v", err)
	}
	if reads > 20 {
		t.Fatalf("read issued %d times: retries must stop at the window edge", reads)
	}
}
