package git

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
)

// countingOps is a gitOps fake that records whether any transport operation was
// attempted. Validation must short-circuit before the seam is touched, so every
// call count stays zero; a non-zero count means an invalid source reached the
// network/filesystem seam.
type countingOps struct {
	cloneCalls int
	openCalls  int
}

func (o *countingOps) clone(context.Context, string, string, gitssh.AuthMethod) error {
	o.cloneCalls++
	return nil
}

func (o *countingOps) open(string) (gitRepo, error) {
	o.openCalls++
	return nil, nil
}

// countingBuilder is a builder fake recording auth-build attempts. Validation
// must run before authFor, so a non-zero count means a bad source reached the
// SSH/TOFU path.
type countingBuilder struct{ calls int }

func (b *countingBuilder) authFor(string) (gitssh.AuthMethod, error) {
	b.calls++
	return nil, nil
}

// writeRawConfig writes a raw (hand-edited) JSON config and returns its path.
func writeRawConfig(t *testing.T, raw string) string {
	t.Helper()
	p := configPath(t)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte(raw), 0o600); err != nil {
		t.Fatalf("write raw config: %v", err)
	}
	return p
}

// TestNormalizeAndValidateConfigDefaultsRef pins that the canonical normalizer
// keeps the documented empty-ref default and accepts a valid SSH config, while
// leaving all bookkeeping fields untouched.
func TestNormalizeAndValidateConfigDefaultsRef(t *testing.T) {
	got, err := NormalizeAndValidateConfig(Config{
		Repository:       "git@github.com:acme/r.git",
		Synced:           true,
		LastSyncedCommit: "abc",
		LastSyncedAt:     "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("NormalizeAndValidateConfig: %v", err)
	}
	if got.Ref != DefaultRef {
		t.Fatalf("Ref = %q, want default %q", got.Ref, DefaultRef)
	}
	if !got.Synced || got.LastSyncedCommit != "abc" || got.LastSyncedAt != "2026-01-01T00:00:00Z" {
		t.Fatalf("bookkeeping changed: %+v", got)
	}
}

// TestLoadConfigPersistedValidation pins that LoadConfig validates the persisted
// source: a valid SSH config loads (with the default ref applied) while an
// http/file/local repository is rejected, so a hand-edited source.json cannot
// smuggle a non-SSH transport into the system.
func TestLoadConfigPersistedValidation(t *testing.T) {
	t.Run("valid ssh accepted", func(t *testing.T) {
		p := writeRawConfig(t, `{"repository":"ssh://git@github.com/acme/r.git"}`)
		got, err := LoadConfig(p)
		if err != nil {
			t.Fatalf("LoadConfig valid: %v", err)
		}
		if got.Ref != DefaultRef {
			t.Fatalf("Ref = %q, want default %q", got.Ref, DefaultRef)
		}
	})
	for _, repo := range []string{
		"https://github.com/acme/r.git",
		"http://github.com/acme/r.git",
		"file:///tmp/r",
		"/tmp/local-repo",
		// Malformed SSH syntax must be rejected by the canonical validator, so a
		// hand-edited source.json can never smuggle it past LoadConfig.
		"ssh://git@github.com:acme/r.git", // non-numeric port => parse error
		"ssh://git@/acme/r.git",           // ssh:// with no host
		"git@:acme/r.git",                 // invalid scp host (user not split)
		"git@github.com:",                 // empty scp repo path
	} {
		t.Run("reject "+repo, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"repository": repo, "ref": "main"})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			p := writeRawConfig(t, string(raw))
			if _, err := LoadConfig(p); err == nil {
				t.Fatalf("LoadConfig repository %q: nil error, want rejection", repo)
			}
		})
	}
}

// TestValidationParitySetSourceWriteLoad pins that the SAME canonical validators
// run for the operator path (SetSource), the internal persistence path
// (writeConfig), and persisted loading (LoadConfig): for every malformed field
// the three paths return the same non-nil error, byte-for-byte. That is what
// guarantees a value rejected by `relay git set` is also rejected when it is
// hand-edited into source.json.
func TestValidationParitySetSourceWriteLoad(t *testing.T) {
	cases := []struct {
		name            string
		repository, ref string
		path, secretRef string
	}{
		{name: "empty repository", repository: "", ref: "main"},
		{name: "non-ssh repository", repository: "https://github.com/a/r", ref: "main"},
		{name: "malformed ssh port", repository: "ssh://git@github.com:acme/r.git", ref: "main"},
		{name: "ssh missing host", repository: "ssh://git@/a/r.git", ref: "main"},
		{name: "invalid scp host", repository: "git@:a/r.git", ref: "main"},
		{name: "empty scp repo", repository: "git@github.com:", ref: "main"},
		{name: "whitespace ref", repository: "git@github.com:a/r.git", ref: "has space"},
		{name: "leading dash ref", repository: "git@github.com:a/r.git", ref: "-leading"},
		{name: "traversal path", repository: "git@github.com:a/r.git", ref: "main", path: "../x"},
		{name: "absolute path", repository: "git@github.com:a/r.git", ref: "main", path: "/abs"},
		{name: "dot path", repository: "git@github.com:a/r.git", ref: "main", path: "."},
		{name: "invalid secret ref", repository: "git@github.com:a/r.git", ref: "main", secretRef: "GH_secret"},
		{name: "slash secret ref", repository: "git@github.com:a/r.git", ref: "main", secretRef: "a/b"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setErr := SetSource(configPath(t), c.repository, c.ref, c.path, c.secretRef)
			if setErr == nil {
				t.Fatal("SetSource: nil error, want rejection")
			}

			writeErr := writeConfig(Config{
				Repository:       c.repository,
				Ref:              c.ref,
				Path:             c.path,
				WebhookSecretRef: c.secretRef,
			}, configPath(t))
			if writeErr == nil {
				t.Fatal("writeConfig: nil error, want rejection")
			}

			raw, err := json.Marshal(map[string]any{
				"repository":       c.repository,
				"ref":              c.ref,
				"path":             c.path,
				"webhookSecretRef": c.secretRef,
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			p := writeRawConfig(t, string(raw))
			_, loadErr := LoadConfig(p)
			if loadErr == nil {
				t.Fatal("LoadConfig: nil error, want rejection")
			}

			// Same canonical validator => identical error text on every path.
			if setErr.Error() != writeErr.Error() {
				t.Fatalf("SetSource err = %q, writeConfig err = %q; want identical", setErr, writeErr)
			}
			if loadErr.Error() != writeErr.Error() {
				t.Fatalf("LoadConfig err = %q, writeConfig err = %q; want identical", loadErr, writeErr)
			}
		})
	}
}

// TestSyncFromConfigRejectsInvalidConfigBeforeOps pins that a directly
// constructed invalid Config is refused by syncWithGit before the timeout, auth
// build, transport ops, or filesystem materialization: the fake ops and builder
// record zero calls and the target dirs are never created.
func TestSyncFromConfigRejectsInvalidConfigBeforeOps(t *testing.T) {
	for _, cfg := range []Config{
		{Repository: "", Ref: "main"},
		{Repository: "https://github.com/a/r", Ref: "main"},
		{Repository: "ssh://git@github.com:acme/r.git", Ref: "main"},
		{Repository: "ssh://git@/a/r.git", Ref: "main"},
		{Repository: "git@:a/r.git", Ref: "main"},
		{Repository: "git@github.com:", Ref: "main"},
		{Repository: "git@github.com:a/r.git", Ref: "has space"},
		{Repository: "git@github.com:a/r.git", Ref: "main", Path: "../x"},
		{Repository: "git@github.com:a/r.git", Ref: "main", WebhookSecretRef: "GH_secret"},
	} {
		t.Run(cfg.Repository+"/"+cfg.Ref+"/"+cfg.Path+"/"+cfg.WebhookSecretRef, func(t *testing.T) {
			dir := t.TempDir()
			o := NewSyncOptions()
			o.CheckoutDir = filepath.Join(dir, "checkout")
			o.AppsDir = filepath.Join(dir, "functions")
			o.CloneURL = filepath.Join(dir, "remote.git")
			ops := &countingOps{}
			auth := &countingBuilder{}

			err := syncWithGit(context.Background(), o, cfg, ops, auth)
			if err == nil {
				t.Fatal("syncWithGit with invalid config: nil error, want rejection")
			}
			if ops.cloneCalls != 0 || ops.openCalls != 0 || auth.calls != 0 {
				t.Fatalf("ops/auth reached before validation: clone=%d open=%d auth=%d",
					ops.cloneCalls, ops.openCalls, auth.calls)
			}
			if _, serr := os.Stat(o.CheckoutDir); !os.IsNotExist(serr) {
				t.Fatalf("checkout dir was touched: %v", serr)
			}
			if _, serr := os.Stat(o.AppsDir); !os.IsNotExist(serr) {
				t.Fatalf("functions dir was touched: %v", serr)
			}
		})
	}
}

// TestSyncFromConfigRejectsInvalidOverridesBeforeOps pins that the effective
// overrides (RepositoryURL, Ref, Path) are validated with the same rules as the
// persisted config and refused before any ops/auth. A valid config with a bad
// override must not reach the seam.
func TestSyncFromConfigRejectsInvalidOverridesBeforeOps(t *testing.T) {
	valid := Config{Repository: "git@github.com:a/r.git", Ref: "main"}
	badPath := "../x"
	cases := []struct {
		name string
		opts func(o *SyncOptions)
	}{
		{"non-ssh RepositoryURL", func(o *SyncOptions) { o.RepositoryURL = "https://github.com/a/r" }},
		{"whitespace Ref", func(o *SyncOptions) { o.Ref = "has space" }},
		{"leading dash Ref", func(o *SyncOptions) { o.Ref = "-leading" }},
		{"traversal Path", func(o *SyncOptions) { o.Path = &badPath }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			o := NewSyncOptions()
			o.CheckoutDir = filepath.Join(dir, "checkout")
			o.AppsDir = filepath.Join(dir, "functions")
			c.opts(&o)
			ops := &countingOps{}
			auth := &countingBuilder{}

			err := syncWithGit(context.Background(), o, valid, ops, auth)
			if err == nil {
				t.Fatal("syncWithGit with invalid override: nil error, want rejection")
			}
			if ops.cloneCalls != 0 || ops.openCalls != 0 || auth.calls != 0 {
				t.Fatalf("ops/auth reached before override validation: clone=%d open=%d auth=%d",
					ops.cloneCalls, ops.openCalls, auth.calls)
			}
		})
	}
}

// TestSyncRejectsHandEditedInvalidConfigFile pins the end-to-end file path: a
// hand-edited invalid source.json is rejected by Sync before it constructs any
// transport, creates a checkout, or materializes /apps.
func TestSyncRejectsHandEditedInvalidConfigFile(t *testing.T) {
	for _, raw := range []string{
		`{"repository":"https://github.com/a/r","ref":"main"}`,
		`{"repository":"ssh://git@github.com:acme/r.git","ref":"main"}`,
		`{"repository":"ssh://git@/a/r.git","ref":"main"}`,
		`{"repository":"git@:a/r.git","ref":"main"}`,
		`{"repository":"git@github.com:","ref":"main"}`,
		`{"repository":"git@github.com:a/r.git","ref":"has space"}`,
		`{"repository":"git@github.com:a/r.git","ref":"main","path":"../x"}`,
		`{"repository":"git@github.com:a/r.git","ref":"main","webhookSecretRef":"GH_secret"}`,
	} {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "source.json")
		if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		o := NewSyncOptions()
		o.ConfigPath = cfgPath
		o.CheckoutDir = filepath.Join(dir, "checkout")
		o.AppsDir = filepath.Join(dir, "functions")
		o.CloneURL = filepath.Join(dir, "remote.git")

		if err := Sync(context.Background(), o); err == nil {
			t.Fatalf("Sync with %s: nil error, want rejection", raw)
		}
		if _, serr := os.Stat(o.CheckoutDir); !os.IsNotExist(serr) {
			t.Fatalf("checkout created for invalid config: %v", serr)
		}
		if _, serr := os.Stat(o.AppsDir); !os.IsNotExist(serr) {
			t.Fatalf("functions dir created for invalid config: %v", serr)
		}
	}
}

// TestValidationErrorsNeverLeakKeyMaterial pins that a validation failure never
// surfaces private key contents or a secret's VALUE. The key file and the secret
// store hold canaries; the config is invalid (so an error is returned) and also
// references the secret by name. The returned error must not contain either
// canary.
func TestValidationErrorsNeverLeakKeyMaterial(t *testing.T) {
	const keyCanary = "CANARY-PRIVATE-KEY-MATERIAL-DO-NOT-LEAK"
	const secretCanary = "CANARY-SECRET-VALUE-DO-NOT-LEAK"

	dir := t.TempDir()
	sshDir := filepath.Join(dir, "ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		t.Fatalf("mkdir ssh: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sshDir, PrivateKeyFile), []byte(keyCanary), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	// A stored secret whose value is the canary, referenced by the config. The
	// name is valid; only the value is sensitive.
	secretsDir := filepath.Join(dir, "secrets")
	if err := os.MkdirAll(secretsDir, 0o700); err != nil {
		t.Fatalf("mkdir secrets: %v", err)
	}
	if err := os.WriteFile(filepath.Join(secretsDir, "gh_secret"), []byte(secretCanary), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}

	raw := `{"repository":"git@github.com:a/r.git","ref":"main","path":"../escape","webhookSecretRef":"gh_secret"}`
	cfgPath := filepath.Join(dir, "source.json")
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Persisted-load path.
	_, loadErr := LoadConfig(cfgPath)
	if loadErr == nil {
		t.Fatal("LoadConfig invalid config: nil error, want rejection")
	}
	assertNoCanary(t, loadErr.Error(), keyCanary, secretCanary)

	// Sync path (would build SSH auth from the key if validation did not run).
	o := NewSyncOptions()
	o.ConfigPath = cfgPath
	o.CheckoutDir = filepath.Join(dir, "checkout")
	o.AppsDir = filepath.Join(dir, "functions")
	o.SSHDir = sshDir
	o.CloneURL = filepath.Join(dir, "remote.git")
	syncErr := Sync(context.Background(), o)
	if syncErr == nil {
		t.Fatal("Sync invalid config: nil error, want rejection")
	}
	assertNoCanary(t, syncErr.Error(), keyCanary, secretCanary)

	// Direct-construction path.
	_, cfgErr := NormalizeAndValidateConfig(Config{
		Repository:       "git@github.com:a/r.git",
		Ref:              "main",
		Path:             "../escape",
		WebhookSecretRef: "gh_secret",
	})
	if cfgErr == nil {
		t.Fatal("NormalizeAndValidateConfig invalid config: nil error, want rejection")
	}
	assertNoCanary(t, cfgErr.Error(), keyCanary, secretCanary)
}

// assertNoCanary fails when any canary string appears in msg.
func assertNoCanary(t *testing.T, msg string, canaries ...string) {
	t.Helper()
	for _, c := range canaries {
		if strings.Contains(msg, c) {
			t.Fatalf("error leaked a canary value: %q", msg)
		}
	}
}
