package cli

import (
	"os"
	"testing"

	"github.com/creack/pty"
)

// mustOpenTestPTY opens a pseudo-terminal pair, skipping the test when the
// platform cannot provide one (for example a restricted CI sandbox). The slave
// is a real terminal file, so the hidden-prompt path is exercised for real
// instead of through a stub.
func mustOpenTestPTY(t *testing.T) (master, slave *os.File) {
	t.Helper()
	master, slave, err := pty.Open()
	if err != nil {
		t.Skipf("PTY unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = master.Close()
		_ = slave.Close()
	})
	return master, slave
}
