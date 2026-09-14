package git

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

// TestGenerateKey verifies keygen creates a 0700 dir, a 0600 valid ed25519 PEM,
// a parseable public key, and prints an authorized_keys line with the
// "ssh-ed25519 " prefix.
func TestGenerateKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ssh")
	pub, err := GenerateKey(dir)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if !strings.HasPrefix(pub, "ssh-ed25519 ") {
		t.Fatalf("public key line = %q, want prefixssh-ed25519", pub)
	}
	keyPath := filepath.Join(dir, PrivateKeyFile)
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("key mode = %o, want 0600", info.Mode().Perm())
	}
	ddir, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if ddir.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode = %o, want 0700", ddir.Mode().Perm())
	}
	// The persisted PEM must parse and the public key must parse too.
	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	if _, err := ssh.ParsePrivateKey(data); err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(pub + "\n")); err != nil {
		t.Fatalf("parse authorized key: %v", err)
	}
	// keyExists must now report true.
	if !keyExists(dir) {
		t.Fatal("keyExists(dir) = false after keygen")
	}
}

// TestGenerateKeyRefusesOverwrite verifies a second keygen errors and leaves the
// existing key byte-identical (a silently regenerated key would orphan the
// public key registered with a provider).
func TestGenerateKeyRefusesOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ssh")
	if _, err := GenerateKey(dir); err != nil {
		t.Fatalf("first GenerateKey: %v", err)
	}
	keyPath := filepath.Join(dir, PrivateKeyFile)
	before, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read first key: %v", err)
	}
	if _, err := GenerateKey(dir); err == nil {
		t.Fatal("second GenerateKey: nil, want 'already exists' error")
	} else if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second GenerateKey error = %v, want 'already exists'", err)
	}
	after, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read after: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("existing key file changed on refused regeneration")
	}
}
