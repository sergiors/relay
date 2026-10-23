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

// breakTerminalRead returns a terminal whose reads fail. Darwin cannot induce
// that failure with the Linux strategy either: closing the slave leaves the
// master passing TCGETS, but reads return (0, nil) and term's readPasswordLine
// retries zero-byte reads forever. Closing the master instead makes the slave
// block or loop on zero-byte reads, because macOS ignores O_NONBLOCK on PTY
// slave reads. Skip rather than hang the test.
func breakTerminalRead(t *testing.T, master, slave *os.File) *os.File {
	t.Helper()
	t.Skip("darwin PTY reads cannot be failed deterministically")
	return nil
}
