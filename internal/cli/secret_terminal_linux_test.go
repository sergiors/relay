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

// breakTerminalRead makes a pending slave read fail: closing the master makes a
// Linux PTY slave read return EIO immediately.
func breakTerminalRead(t *testing.T, master, slave *os.File) {
	t.Helper()
	if err := master.Close(); err != nil {
		t.Fatalf("close master: %v", err)
	}
}
