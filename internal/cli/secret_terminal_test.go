//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// waitForTerminalEchoOff blocks until the terminal driver reports echo disabled
// on slave, proving the prompt takes the hidden-read path rather than a plain
// line read.
func waitForTerminalEchoOff(t *testing.T, slave *os.File) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		echo, err := testTerminalEcho(slave)
		if err == nil && !echo {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("terminal echo was not disabled; the prompt must read with echo off")
}

// readPTYAvailable returns whatever bytes the master side has queued, without
// blocking. With echo disabled the typed value must never appear here.
func readPTYAvailable(master *os.File) string {
	var out []byte
	for {
		fds := []unix.PollFd{{Fd: int32(master.Fd()), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, 0)
		if err != nil || n == 0 || fds[0].Revents&unix.POLLIN == 0 {
			return string(out)
		}
		buf := make([]byte, 64)
		read, err := unix.Read(int(master.Fd()), buf)
		if read > 0 {
			out = append(out, buf[:read]...)
		}
		if err != nil || read == 0 {
			return string(out)
		}
	}
}

// TestSecretSetPromptsOnTerminalWithoutEcho verifies the default source: with a
// real terminal the value is read after echo is disabled, the typed bytes never
// come back to the master side, and the stored value has its line ending
// removed without being printed.
func TestSecretSetPromptsOnTerminalWithoutEcho(t *testing.T) {
	store := seedSecretStore(t)
	master, slave := mustOpenTestPTY(t)

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- secretSetValue(context.Background(), store, "db", slave, &out, false)
	}()

	waitForTerminalEchoOff(t, slave)
	if _, err := master.WriteString("s3cr3t"); err != nil {
		t.Fatalf("write terminal input: %v", err)
	}
	if echoed := readPTYAvailable(master); echoed != "" {
		t.Fatalf("terminal echoed %q; the prompt must disable echo", echoed)
	}
	if _, err := master.WriteString("\n"); err != nil {
		t.Fatalf("write terminal newline: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("secretSetValue: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the terminal prompt to finish")
	}

	got, err := store.Resolve(context.Background(), "db")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "s3cr3t" {
		t.Fatalf("resolved length = %d, want the typed value without its line ending", len(got))
	}
	if strings.Contains(out.String(), "s3cr3t") {
		t.Fatal("output leaked the secret value")
	}
	if !strings.Contains(out.String(), `Successfully set secret "db"`) {
		t.Fatalf("output missing success message: %q", out.String())
	}
}

// TestSecretSetTerminalEmptyValueRejected verifies an empty line at the
// interactive prompt (just Enter) is rejected: it must fail, print no success
// line, and leave no secret stored.
func TestSecretSetTerminalEmptyValueRejected(t *testing.T) {
	store := seedSecretStore(t)
	master, slave := mustOpenTestPTY(t)

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- secretSetValue(context.Background(), store, "db", slave, &out, false)
	}()

	// The prompt is active once the hidden read has disabled echo; pressing
	// Enter then delivers an empty line.
	waitForTerminalEchoOff(t, slave)
	if _, err := master.WriteString("\n"); err != nil {
		t.Fatalf("write terminal newline: %v", err)
	}

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "secret set: empty value") {
			t.Fatalf("err = %v, want an empty-value error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the terminal prompt to finish")
	}

	if out.Len() != 0 {
		t.Fatalf("output = %q, want no success line", out.String())
	}
	requireSecretMissing(t, store, "db")
}

// TestSecretSetTerminalReadError verifies a failing terminal read is reported as
// a read error without storing a secret. The platform helper makes the pending
// read fail immediately (or skips where that cannot be induced).
func TestSecretSetTerminalReadError(t *testing.T) {
	store := seedSecretStore(t)
	master, slave := mustOpenTestPTY(t)
	breakTerminalRead(t, master, slave)

	var out bytes.Buffer
	err := secretSetValue(context.Background(), store, "db", slave, &out, false)
	if err == nil || !strings.Contains(err.Error(), "secret set: read terminal") {
		t.Fatalf("err = %v, want a terminal read error", err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
	requireSecretMissing(t, store, "db")
}
