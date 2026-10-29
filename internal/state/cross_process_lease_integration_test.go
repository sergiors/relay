//go:build integration

// Cross-process proof for the State-backed dependency-image lease (IR-004).
//
// An in-process fake cannot prove this contract: two OS-level processes must
// observe ONE lease state through the database. A subprocess test helper (the
// test binary re-executing itself) holds a shared dependency-image lease in
// process A while the parent — using an INDEPENDENT State handle on the same
// database — must be refused the exclusive GC lease. When process A releases
// (the parent sends the explicit "release" protocol line and closes the
// child's stdin: a deterministic hand-back, never a timing sleep), the parent
// acquires. There is no in-memory registry involved: refusal and grant couple
// only through PostgreSQL.
//
// Method names follow the landed production contract (internal/state/
// dep_image_lease.go); adjust only these call sites if the spellings change.
package state_test

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"relay/internal/state"
	"relay/internal/testutil"
)

const (
	ir004ChildMarker = "IR004_LEASE_CHILD"
	// ir004ChildRunFlag names the subprocess body so the child test binary
	// runs ONLY the helper (the parent's own -test.run flag is deliberately
	// NOT passed through, which would re-run the parent selection).
	ir004ChildRunFlag = "^TestIntegrationDependencyLeaseChildHelper$"
	// ir004ChildBound is the deadlock bound for every subprocess sync step. On
	// the happy path each signal is delivered by the counterpart's exact
	// transition, never by the timer.
	ir004ChildBound = 30 * time.Second
)

// ir004Await receives one child-protocol line with a failure bound and, on any
// exiting error path, terminates the child first so a broken protocol wedges
// nothing in the suite. It is called from the test goroutine only.
func ir004Await(t *testing.T, what string, lines <-chan string, cmd *exec.Cmd) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(ir004ChildBound):
		if cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		t.Fatalf("the lease-holding child never reported %s", what)
		return ""
	}
}

// ir004StartedChild bundles the live subprocess plumbing the parent drives:
// the command (for status/lifecycle), the stdin writer (release side), and the
// asynchronously scanned stdout lines (the child's protocol output).
type ir004StartedChild struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan string
	stdout io.ReadCloser
}

// ir004StartChild re-executes the test binary running ONLY the child helper
// test, taking an explicit -test.run selection instead of inheriting the
// parent's own test flags (the parent's os.Args[1:] could otherwise recursively
// select the parent test again).
func ir004StartChild(t *testing.T, dsn, ref string) *ir004StartedChild {
	t.Helper()
	childExe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	c := &ir004StartedChild{lines: make(chan string, 8)}
	c.cmd = exec.Command(childExe, "-test.run", ir004ChildRunFlag)
	c.cmd.Env = append(os.Environ(),
		ir004ChildMarker+"=1",
		"IR004_LEASE_DSN="+dsn,
		"IR004_LEASE_REF="+ref,
		// Process A's own node identity: unique per run, DIFFERENT from the
		// parent's (process B) — two ownerful handles never share a NodeID, and
		// different identities on one database is the production branch.
		"IR004_LEASE_OWNER=ir004-proc-a-"+ir004RunToken,
		"IR004_LEASE_INSTANCE=inst-a-"+ir004RunToken,
	)
	c.stdin, err = c.cmd.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin pipe: %v", err)
	}
	c.stdout, err = c.cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout pipe: %v", err)
	}
	if err := c.cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	go func() {
		defer close(c.lines)
		scanner := bufio.NewScanner(c.stdout)
		for scanner.Scan() {
			c.lines <- scanner.Text()
		}
	}()
	t.Cleanup(func() {
		// Never let the child outlive the test: the release line then close
		// stdin makes even a wedged child release and exit.
		c.release()
		_ = c.cmd.Wait()
	})
	return c
}

func (c *ir004StartedChild) release() {
	_, _ = c.stdin.Write([]byte("release\n"))
	_ = c.stdin.Close()
}

// TestIntegrationDependencyLeaseCrossProcess proves the cross-process lease
// contract end to end, on the PRODUCTION connection branch: both processes use
// OWNERFUL State handles (unique NodeID/Instance per process, exactly like two
// different NODE_ID instances sharing one DATABASE_URL), so dependency-image
// coordination runs on each handle's pinned node-lock connection and the two
// identities coexist against the same database:
//
//  1. process A (the test binary re-executed) holds a shared use lease;
//  2. process B (this parent, an independent ownerful handle on the SAME
//     database) is refused the exclusive GC lease while A holds it;
//  3. after A releases and exits cleanly, process B acquires.
func TestIntegrationDependencyLeaseCrossProcess(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	// Run-unique reference AND run-unique node identities (advisory locks are
	// database-wide — per-test schemas do not namespace them). The child
	// receives the EXACT ref via IR004_LEASE_REF, so both processes coordinate
	// on one lock, each with its own NODE_ID-like identity.
	ref := "relay-dep-ir004-cross-process-" + ir004RunToken
	ownerP := "ir004-proc-b-" + ir004RunToken

	child := ir004StartChild(t, dsn, ref)
	if got := ir004Await(t, "its lease hold", child.lines, child.cmd); got != "held" {
		t.Fatalf("child protocol = %q, want 'held' before the parent continues", got)
	}

	// Process B: a fresh OWNERFUL handle on the SAME database (different node
	// identity than process A's) observes process A's hold.
	stB, err := state.Open(context.Background(), state.Options{
		DSN:      dsn,
		NodeID:   ownerP,
		Instance: "inst-b-" + ir004RunToken,
	})
	if err != nil {
		t.Fatalf("parent state.Open: %v", err)
	}
	defer func() { _ = stB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), ir004ChildBound)
	defer cancel()
	if _, ok, err := stB.TryAcquireDependencyImageGC(ctx, ref); err != nil || ok {
		t.Fatalf("TryAcquireDependencyImageGC while process A holds a shared lease = (acquired=%v, err=%v), want refusal", ok, err)
	}

	// Release process A with the explicit "release" protocol line (stdin close
	// follows, so the child also unblocks should it lose that line).
	child.release()

	// Protocol acknowledgment: the child reports "released" AFTER it has
	// released its lease, so the parent's exclusive try below cannot race the
	// hand-back.
	if got := ir004Await(t, "its release acknowledgment", child.lines, child.cmd); got != "released" {
		t.Fatalf("child protocol = %q, want 'released' after the hand-back", got)
	}

	// The exclusive grant now succeeds for process B.
	gc, ok, err := stB.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil {
		t.Fatalf("TryAcquireDependencyImageGC after process A released: %v", err)
	}
	if !ok {
		t.Fatal("TryAcquireDependencyImageGC must succeed once the holding process released")
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// The child must also have finished its release side and exited cleanly.
	if err := child.cmd.Wait(); err != nil {
		t.Fatalf("child exited non-zero after release: %v", err)
	}
}

// TestIntegrationDependencyLeaseChildHelper is the subprocess body of
// TestIntegrationDependencyLeaseCrossProcess. It is a test only so the parent
// can select it by -test.run; it is never run against production data, and a
// mis-wired invocation fails loudly rather than reading green.
func TestIntegrationDependencyLeaseChildHelper(t *testing.T) {
	if os.Getenv(ir004ChildMarker) != "1" {
		t.Skip("subprocess lease holder; run only via its parent test")
	}
	dsn := os.Getenv("IR004_LEASE_DSN")
	ref := os.Getenv("IR004_LEASE_REF")
	worker := os.Getenv("IR004_LEASE_OWNER")
	instance := os.Getenv("IR004_LEASE_INSTANCE")
	if dsn == "" || ref == "" || worker == "" || instance == "" {
		t.Fatal("child helper requires IR004_LEASE_DSN, IR004_LEASE_REF, IR004_LEASE_OWNER and IR004_LEASE_INSTANCE")
	}

	ctx, cancel := context.WithTimeout(context.Background(), ir004ChildBound)
	defer cancel()
	// OWNERFUL handle, like a production worker: coordination runs on the
	// pinned node-lock connection (withDependencyCoordConn's owner branch),
	// not an ownerless lazily-pinned depConn.
	st, err := state.Open(ctx, state.Options{DSN: dsn, NodeID: worker, Instance: instance})
	if err != nil {
		t.Fatalf("child state.Open: %v", err)
	}
	// Releases the leases even on a failure path before the explicit Release
	// below; closing is scoped-clean, so PostgreSQL releases every session lock
	// the handle held.
	defer func() { _ = st.Close() }()

	l, err := st.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("child AcquireDependencyImageUse: %v", err)
	}
	if err := l.Verify(ctx); err != nil {
		t.Fatalf("child lease Verify while held: %v", err)
	}

	// Announce the hold; the parent's "release" line authorizes the let-through.
	// A plain EOF (stdin closed) is treated the SAME as an explicit release
	// line, so either version is a valid authorization signal, and there is no
	// lower-level arbitrary byte flow.
	if _, err := os.Stdout.WriteString("held\n"); err != nil {
		t.Fatalf("child announcing hold: %v", err)
	}

	// Wait for the parent's authorization: the "release" line, or stdin EOF
	// (blocking, not polling — no sleeps). Both forms authorize the release.
	buf := bufio.NewReader(os.Stdin)
	_, _ = buf.ReadString('\n')

	if err := l.Release(); err != nil {
		t.Fatalf("child Release: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("child Close: %v", err)
	}
	// Acknowledge the completed release: the parent requires this exact line
	// before it asserts that the exclusive view is available, so the protocol
	// cannot race the parent ahead of the child's hand-back.
	if _, err := os.Stdout.WriteString("released\n"); err != nil {
		t.Fatalf("child acknowledging release: %v", err)
	}
}
