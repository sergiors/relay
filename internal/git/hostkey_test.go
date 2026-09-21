package git

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/skeema/knownhosts"
	"golang.org/x/crypto/ssh"
)

// hostKeyFixture returns a temp sshDir with a generated deploy key and the
// Relay-owned known_hosts path (which does NOT yet exist — the initial TOFU
// state). It also returns a synthesized server host key (a distinct ed25519
// public key) and a TCP remote address so tests can invoke the TOFU callback
// directly, exactly as the transport would.
func hostKeyFixture(t *testing.T) (
	sshDir, khPath string, serverKey ssh.PublicKey, remote net.Addr, cb ssh.HostKeyCallback,
) {
	t.Helper()
	sshDir = filepath.Join(t.TempDir(), "ssh")
	if _, err := GenerateKey(sshDir); err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	khPath = knownHostsPath(sshDir)

	serverPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate server ed25519: %v", err)
	}
	serverKey, err = ssh.NewPublicKey(serverPub)
	if err != nil {
		t.Fatalf("serialize server key: %v", err)
	}
	remote = &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 22}

	ep, err := parseEndpoint("git@github.com:acme/r.git")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	var out bytes.Buffer
	logBuf := bytes.Buffer{}
	logr := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth, err := sshAuthFor(sshDir, ep, &out, logr)
	if err != nil {
		t.Fatalf("sshAuthFor: %v", err)
	}
	cb = auth.(*gitssh.PublicKeys).HostKeyCallback
	return sshDir, khPath, serverKey, remote, cb
}

// freshTOFUCallback rebuilds the TOFU callback from the CURRENT known_hosts
// file at sshDir (which the first trust will have populated). Each real sync
// builds its DB from the file, so a subsequent connection must use a fresh
// callback — this helper simulates exactly that.
func freshTOFUCallback(t *testing.T, sshDir string) ssh.HostKeyCallback {
	t.Helper()
	ep, err := parseEndpoint("git@github.com:acme/r.git")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	auth, err := sshAuthFor(sshDir, ep, nil, nil)
	if err != nil {
		t.Fatalf("sshAuthFor(rebuild): %v", err)
	}
	return auth.(*gitssh.PublicKeys).HostKeyCallback
}

// TestTOFUFirstConnectionTrustsUnknownHost verifies the heart of TOFU: the very
// first callback invocation with an unknown host returns nil (trusted) and
// persists the host's key to Relay's own known_hosts file, which is created with
// mode 0600 and carries the normalized host form. sshAuthFor already created the
// file (empty, the "no hosts trusted yet" state); the first trust appends the
// host's line.
func TestTOFUFirstConnectionTrustsUnknownHost(t *testing.T) {
	_, khPath, serverKey, remote, cb := hostKeyFixture(t)

	info, err := os.Stat(khPath)
	if err != nil {
		t.Fatalf("stat known_hosts before trust: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("known_hosts mode = %o, want 0600", info.Mode().Perm())
	}

	// The remote's IP is 10.0.0.1:22; pass hostname and remote exactly as the
	// transport would (hostname in host:port form, per Go's knownhosts callback
	// contract and go-git's getHostWithPort).
	err = cb("github.com:22", remote, serverKey)
	if err != nil {
		t.Fatalf("first-trust callback err = %v, want nil (TOFU accepts the unknown host)", err)
	}

	info, err = os.Stat(khPath)
	if err != nil {
		t.Fatalf("stat known_hosts after trust: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("known_hosts mode = %o, want 0600", info.Mode().Perm())
	}
	data, err := os.ReadFile(khPath)
	if err != nil {
		t.Fatalf("read known_hosts: %v", err)
	}
	// The persisted line must carry the normalized host form (OpenSSH format).
	if !strings.Contains(string(data), "github.com") {
		t.Fatalf("known_hosts line = %q, want normalized host form 'github.com'", string(data))
	}
	// The line must NOT contain raw key material as a recognizable private key
	// or the raw marshaled key blob of the deploy key.
	if strings.Contains(strings.ToUpper(string(data)), "BEGIN OPENSSH PRIVATE KEY") {
		t.Fatalf("known_hosts leaked private key material: %q", string(data))
	}
}

// TestTOFUPersistsKnownHostsEntry verifies that after the first-trust write, a
// fresh knownhosts.NewDB over the same file reports the host's keys (persistence
// is durable and re-parseable), proving the pin survives across processes.
func TestTOFUPersistsKnownHostsEntry(t *testing.T) {
	_, khPath, serverKey, remote, cb := hostKeyFixture(t)
	if err := cb("github.com:22", remote, serverKey); err != nil {
		t.Fatalf("first trust: %v", err)
	}

	db, err := knownhosts.NewDB(khPath)
	if err != nil {
		t.Fatalf("re-load known_hosts: %v", err)
	}
	if got := db.HostKeys("github.com:22"); len(got) == 0 {
		t.Fatal("HostKeys(github.com:22) empty after first trust; entry was not persisted")
	}
}

// TestTOFUSubsequentConnectionVerifies pins that a later connection (a fresh
// callback built from the persisted file, as the next sync would) with the SAME
// key verifies cleanly and returns nil.
func TestTOFUSubsequentConnectionVerifies(t *testing.T) {
	sshDir, _, serverKey, remote, cb := hostKeyFixture(t)
	if err := cb("github.com:22", remote, serverKey); err != nil {
		t.Fatalf("first trust: %v", err)
	}
	cb2 := freshTOFUCallback(t, sshDir)
	if err := cb2("github.com:22", remote, serverKey); err != nil {
		t.Fatalf("subsequent verification err = %v, want nil (same key must verify)", err)
	}
}

// TestTOFUHostKeyMismatchFails pins the changed-key guard: with a DIFFERENT key
// for the same host the callback fails loudly with an operator-actionable MITM
// message that names the host and mentions the change — but never exposes raw
// key material — and the known_hosts file is left byte-identical (never silently
// replaced).
func TestTOFUHostKeyMismatchFails(t *testing.T) {
	sshDir, khPath, serverKey, remote, cb := hostKeyFixture(t)
	if err := cb("github.com:22", remote, serverKey); err != nil {
		t.Fatalf("first trust: %v", err)
	}
	before, err := os.ReadFile(khPath)
	if err != nil {
		t.Fatalf("read known_hosts before mismatch: %v", err)
	}

	// A different server key for the same host, seen by a fresh callback (the
	// next sync) => must fail loudly.
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}
	otherKey, err := ssh.NewPublicKey(otherPub)
	if err != nil {
		t.Fatalf("serialize other key: %v", err)
	}
	cb2 := freshTOFUCallback(t, sshDir)
	err = cb2("github.com:22", remote, otherKey)
	if err == nil {
		t.Fatal("mismatched host key: nil error, want changed-key MITM failure")
	}
	if !strings.Contains(err.Error(), "github.com") {
		t.Fatalf("mismatch err = %q, want it to name the host", err)
	}
	if !strings.Contains(err.Error(), "changed") {
		t.Fatalf("mismatch err = %q, want 'changed' MITM wording", err)
	}
	// Never leak key material (a base64 blob or the private key header).
	if strings.Contains(err.Error(), "BEGIN OPENSSH PRIVATE KEY") {
		t.Fatalf("mismatch err leaked private key material: %v", err)
	}

	// The known_hosts file must be UNCHANGED (no silent replacement of the pin).
	after, err := os.ReadFile(khPath)
	if err != nil {
		t.Fatalf("read known_hosts after mismatch: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("known_hosts changed after a host-key mismatch; a changed key must never auto-update the pin")
	}
}

// TestTOFUFirstTrustNotifySurfaced pins the first-trust surfacing: the out/out
// channel line ("Trusted new host ... fingerprint") and the Debug log attrs are
// both emitted, and neither contains a raw key blob or private-key header.
func TestTOFUFirstTrustNotifySurfaced(t *testing.T) {
	sshDir := filepath.Join(t.TempDir(), "ssh")
	if _, err := GenerateKey(sshDir); err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	serverPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverKey, err := ssh.NewPublicKey(serverPub)
	if err != nil {
		t.Fatalf("serialize server key: %v", err)
	}
	remote := &net.TCPAddr{IP: net.ParseIP("203.0.113.5"), Port: 22}
	ep, err := parseEndpoint("ssh://git@gitlab.example.com/acme/r.git")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}

	var outBuf bytes.Buffer
	logBuf := bytes.Buffer{}
	logr := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth, err := sshAuthFor(sshDir, ep, &outBuf, logr)
	if err != nil {
		t.Fatalf("sshAuthFor: %v", err)
	}
	cb := auth.(*gitssh.PublicKeys).HostKeyCallback
	if err := cb("gitlab.example.com:22", remote, serverKey); err != nil {
		t.Fatalf("first trust: %v", err)
	}

	// Out gets the sentence-style user line with the fingerprint.
	if !strings.Contains(outBuf.String(), "Trusted new host gitlab.example.com") {
		t.Fatalf("out = %q, want 'Trusted new host' line", outBuf.String())
	}
	if !strings.Contains(outBuf.String(), "SHA256:") {
		t.Fatalf("out = %q, want SHA256 fingerprint", outBuf.String())
	}
	// Debug log carries host + fingerprint attrs with its own distinct message
	// (not the writer sentence).
	if !strings.Contains(logBuf.String(), "host=gitlab.example.com") ||
		!strings.Contains(logBuf.String(), "fingerprint=") ||
		!strings.Contains(logBuf.String(), "TOFU host key trusted") {
		t.Fatalf("log = %q, want TOFU-host-key-trusted Debug with host+fingerprint attrs", logBuf.String())
	}
	// Neither channel exposes raw key bytes or a private key header.
	for name, s := range map[string]string{"out": outBuf.String(), "log": logBuf.String()} {
		if strings.Contains(strings.ToUpper(s), "BEGIN OPENSSH PRIVATE KEY") {
			t.Fatalf("%s leaked private key material: %q", name, s)
		}
	}
}

// TestSSHAuthForErrorPaths verifies sshAuthFor's error handling directly: a
// missing deploy key errors with keygen guidance, while a valid key + temp dir
// yields a non-nil auth with a set (never nil) HostKeyCallback and no error.
func TestSSHAuthForErrorPaths(t *testing.T) {
	ep, err := parseEndpoint("git@github.com:acme/r.git")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}

	// Missing deploy key -> keygen guidance.
	emptyDir := filepath.Join(t.TempDir(), "ssh")
	_, err = sshAuthFor(emptyDir, ep, nil, nil)
	if err == nil {
		t.Fatal("sshAuthFor with missing key: nil error, want keygen guidance")
	}
	if !strings.Contains(err.Error(), "keygen") {
		t.Fatalf("err = %v, want keygen guidance", err)
	}

	// Valid key + temp dir -> auth non-nil, HostKeyCallback set.
	goodDir := filepath.Join(t.TempDir(), "ssh")
	if _, err := GenerateKey(goodDir); err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	auth, err := sshAuthFor(goodDir, ep, nil, nil)
	if err != nil {
		t.Fatalf("sshAuthFor with valid key: %v", err)
	}
	if auth == nil {
		t.Fatal("auth is nil, want non-nil")
	}
	if cb := auth.(*gitssh.PublicKeys).HostKeyCallback; cb == nil {
		t.Fatal("HostKeyCallback is nil; host-key verification must never be disabled")
	}
}
