//go:build linux

package cli

import (
	"os"
	"testing"

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

// breakTerminalRead returns a terminal whose reads fail deterministically.
// Closing the slave leaves the master a terminal for TCGETS while reads return
// EIO. Closing the master after the prompt has disabled echo is not a safe
// alternative: a slave read that starts after the hangup returns (0, nil), and
// term's readPasswordLine retries zero-byte reads forever, hanging the test.
func breakTerminalRead(t *testing.T, master, slave *os.File) *os.File {
	t.Helper()
	if err := slave.Close(); err != nil {
		t.Fatalf("close PTY slave: %v", err)
	}
	return master
}
