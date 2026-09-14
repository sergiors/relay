package git

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStatusNoConfig(t *testing.T) {
	e := fixture(t, false)
	// No config written yet: Status must return a non-configured snapshot and a
	// nil error (it is not an error to have nothing configured).
	s, err := Status(filepath.Join(e.gitDir, "source.json"), e.sshDir, e.checkout)
	if err != nil {
		t.Fatalf("Status err = %v, want nil", err)
	}
	if s.Configured {
		t.Fatal("Configured = true, want false with no config")
	}
	// Rendering prints the standalone message.
	var buf bytes.Buffer
	PrintStatus(&buf, s)
	if !strings.Contains(buf.String(), "No git source configured.") {
		t.Fatalf("status output = %q, want 'No git source configured'", buf.String())
	}
}

func TestStatusKeyMissingAndConfigured(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main", Path: "pkg/fn"}
	if err := writeConfig(cfg, filepath.Join(e.gitDir, "source.json")); err != nil {
		t.Fatalf("writeConfig: %v", err)
	}
	s, err := Status(filepath.Join(e.gitDir, "source.json"), e.sshDir, e.checkout)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !s.Configured || s.Repository != cfg.Repository || s.Ref != "main" || s.Path != "pkg/fn" {
		t.Fatalf("status = %+v, want configured repo/ref/path", s)
	}
	if s.KeyExists {
		t.Fatal("KeyExists = true, want false (no key generated)")
	}
	if s.Checkout || s.Commit != "" {
		t.Fatal("Checkout/Commit should be empty before any sync")
	}

	// After a sync, checkout+commit+synced are reported. The sync uses a config
	// without the monorepo Path (the fixture has no pkg/fn subdir); Path itself
	// was asserted above in the configured-status fields.
	syncCfg := Config{Repository: cfg.Repository, Ref: "main"}
	mustSync(t, e, syncCfg)
	s2, err := Status(filepath.Join(e.gitDir, "source.json"), e.sshDir, e.checkout)
	if err != nil {
		t.Fatalf("Status after sync: %v", err)
	}
	if !s2.Checkout || s2.Commit == "" || !s2.Synced || s2.LastSynced == "" {
		t.Fatalf("status after sync = %+v, want checkout+commit+synced", s2)
	}
}

func TestStatusPrintsNoKeyMaterial(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg)
	s, _ := Status(filepath.Join(e.gitDir, "source.json"), e.sshDir, e.checkout)
	var buf bytes.Buffer
	PrintStatus(&buf, s)
	// No private key material or key identity lines may appear. The repository
	// URL legitimately contains "git@...", which is public configuration, so it
	// is deliberately not in this list.
	for _, secret := range []string{"ssh-ed25519", "BEGIN OPENSSH PRIVATE"} {
		if strings.Contains(buf.String(), secret) {
			t.Fatalf("status leaked %q:\n%s", secret, buf.String())
		}
	}
}

// TestStatusKeyExistsAfterKeygen verifies KeyExists flips true after GenerateKey.
func TestStatusKeyExistsAfterKeygen(t *testing.T) {
	e := fixture(t, false)
	if _, err := GenerateKey(e.sshDir); err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	s, _ := Status(filepath.Join(e.gitDir, "source.json"), e.sshDir, e.checkout)
	if !s.KeyExists {
		t.Fatal("KeyExists = false, want true after keygen")
	}
}

// TestRemoveIdempotent verifies remove drops config+checkout and leaves
// /functions and the SSH key untouched, and that a second remove succeeds
// (idempotent), reporting nothing to remove.
func TestRemoveIdempotent(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg)

	// Plant a function file in /functions and generate a key before remove.
	writeFile(t, filepath.Join(e.functions, "saved", "note.txt"), "keep me\n")
	if _, err := GenerateKey(e.sshDir); err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	cfgPath := filepath.Join(e.gitDir, "source.json")
	var buf bytes.Buffer
	if err := Remove(cfgPath, e.checkout, &buf); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(cfgPath); !os.IsNotExist(err) {
		t.Fatal("config file not removed")
	}
	if _, err := os.Stat(e.checkout); !os.IsNotExist(err) {
		t.Fatal("checkout dir not removed")
	}
	// /functions and the key survive.
	if _, err := os.Stat(filepath.Join(e.functions, "saved", "note.txt")); err != nil {
		t.Fatalf("functions touched by remove: %v", err)
	}
	if _, err := os.Stat(filepath.Join(e.sshDir, PrivateKeyFile)); err != nil {
		t.Fatalf("SSH key removed by remove: %v", err)
	}
	// Second remove is idempotent.
	var buf2 bytes.Buffer
	if err := Remove(cfgPath, e.checkout, &buf2); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if !strings.Contains(buf2.String(), "Not configured") {
		t.Fatalf("second remove output = %q, want 'Not configured'", buf2.String())
	}
}

// TestRemoveMissingDirsNotError verifies remove tolerates a missing checkout.
func TestRemoveMissingDirsNotError(t *testing.T) {
	e := fixture(t, false)
	cfgPath := filepath.Join(e.gitDir, "source.json")
	// Nothing configured, checkout absent: must succeed.
	if err := Remove(cfgPath, e.checkout, io.Discard); err != nil {
		t.Fatalf("Remove with missing dirs: %v", err)
	}
}

// TestStatusErrOnCorruptConfig verifies Status surfaces a corrupt/unreadable
// config as an error (as opposed to "nothing configured").
func TestStatusErrOnCorruptConfig(t *testing.T) {
	e := fixture(t, false)
	cfgPath := filepath.Join(e.gitDir, "source.json")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Status(cfgPath, e.sshDir, e.checkout)
	if err == nil {
		t.Fatal("Status with corrupt config: nil error, want error")
	}
}
