//go:build linux

package cli

import (
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// testTerminalEcho reports whether echo is enabled on the terminal.
func testTerminalEcho(f *os.File) (bool, error) {
	state, err := unix.IoctlGetTermios(int(f.Fd()), unix.TCGETS)
	if err != nil {
		return false, err
	}
	return state.Lflag&unix.ECHO != 0, nil
}

// breakTerminalRead makes the prompt's pending slave read fail. Closing the
// master up front would hang the terminal up before the prompt even starts: the
// slave then fails TCGETS with EIO, so it no longer looks like a terminal and
// the pre-read guard rejects it with the non-interactive message instead of
// exercising the read error. Wait until the prompt has disabled echo — the step
// immediately before it reads — and only then close the master, which makes that
// pending Linux PTY slave read return EIO. The timeout closes the master anyway
// so a prompt that never starts its hidden read cannot hang the test.
func breakTerminalRead(t *testing.T, master, slave *os.File) {
	t.Helper()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case <-stop:
				return
			default:
			}
			if echo, err := testTerminalEcho(slave); err == nil && !echo {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		_ = master.Close()
	}()
}
