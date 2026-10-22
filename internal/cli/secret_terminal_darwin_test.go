//go:build darwin

package cli

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// testTerminalEcho reports whether echo is enabled on the terminal.
func testTerminalEcho(f *os.File) (bool, error) {
	state, err := unix.IoctlGetTermios(int(f.Fd()), unix.TIOCGETA)
	if err != nil {
		return false, err
	}
	return state.Lflag&unix.ECHO != 0, nil
}

// breakTerminalRead makes a pending slave read fail. Darwin PTY slaves block
// (or return EOF loops) instead of failing when the master closes, and macOS
// ignores O_NONBLOCK on PTY reads, so this path is not inducible here.
func breakTerminalRead(t *testing.T, master, slave *os.File) {
	t.Helper()
	t.Skip("darwin PTY reads cannot be failed deterministically")
}
