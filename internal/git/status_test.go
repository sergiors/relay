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
// /apps and the SSH key untouched, and that a second remove succeeds
// (idempotent), reporting nothing to remove.
func TestRemoveIdempotent(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg)

	// Plant an app file in /apps and generate a key before remove.
	writeFile(t, filepath.Join(e.apps, "saved", "note.txt"), "keep me\n")
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
	// /apps and the key survive.
	if _, err := os.Stat(filepath.Join(e.apps, "saved", "note.txt")); err != nil {
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

// TestRemoveInvalidConfigStillRemoves pins finding #1: `git remove` is the escape
// hatch for a hand-edited source.json that LoadConfig now rejects. Removal must
// decide existence by stat (never by parsing), so it drops both an unparseable
// file and a parseable-but-invalid one, removes the checkout alongside it, and
// stays idempotent — even though LoadConfig would error on the same file.
func TestRemoveInvalidConfigStillRemoves(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "unparseable json", raw: "{not json"},
		{name: "non-ssh repository", raw: `{"repository":"https://github.com/a/r","ref":"main"}`},
		{name: "invalid scp host", raw: `{"repository":"git@:a/r.git","ref":"main"}`},
		{name: "bad ref", raw: `{"repository":"git@github.com:a/r.git","ref":"has space"}`},
		{name: "traversal path", raw: `{"repository":"git@github.com:a/r.git","ref":"main","path":"../x"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := fixture(t, false)
			// Create a real checkout via a sync, then poison the config on disk.
			mustSync(t, e, Config{Repository: "git@github.com:acme/r.git", Ref: "main"})
			cfgPath := filepath.Join(e.gitDir, "source.json")
			if err := os.WriteFile(cfgPath, []byte(c.raw), 0o600); err != nil {
				t.Fatalf("poison config: %v", err)
			}
			// Sanity: the canonical loader rejects this file (remove must not care).
			if _, err := LoadConfig(cfgPath); err == nil {
				t.Fatal("LoadConfig accepted the poisoned config; test precondition broken")
			}

			var buf bytes.Buffer
			if err := Remove(cfgPath, e.checkout, &buf); err != nil {
				t.Fatalf("Remove invalid config: %v", err)
			}
			if _, err := os.Stat(cfgPath); !os.IsNotExist(err) {
				t.Fatal("invalid config file not removed")
			}
			if _, err := os.Stat(e.checkout); !os.IsNotExist(err) {
				t.Fatal("checkout dir not removed alongside invalid config")
			}
			if !strings.Contains(buf.String(), "Removed git source config") {
				t.Fatalf("remove output = %q, want config-removed report", buf.String())
			}

			// Idempotent second run: nothing configured, no checkout.
			var buf2 bytes.Buffer
			if err := Remove(cfgPath, e.checkout, &buf2); err != nil {
				t.Fatalf("second Remove: %v", err)
			}
			if !strings.Contains(buf2.String(), "Not configured") {
				t.Fatalf("second remove output = %q, want 'Not configured'", buf2.String())
			}
		})
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
