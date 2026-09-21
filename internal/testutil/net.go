package testutil

import (
	"fmt"
	"net"
	"testing"
)

// FreePort returns an available TCP port on the loopback interface by
// probe-listening on 127.0.0.1:0 and closing the listener.
func FreePort(t *testing.T) int {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer probe.Close()
	return probe.Addr().(*net.TCPAddr).Port
}

// FreeAddr returns a free loopback address ("127.0.0.1:port"). It exists for
// servers whose Start accepts a concrete pre-known address but does not expose
// the kernel-assigned port (e.g. a test that must later scrape the server).
func FreeAddr(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("127.0.0.1:%d", FreePort(t))
}
