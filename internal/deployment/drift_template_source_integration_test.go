//go:build integration

package deployment

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
)

// The drift backstop's ReadTemplateSource classification end to end: a
// TRANSIENT template read failure (permissions) requests nothing — it says
// nothing about the bytes, so the reconciler's retry owns it — while every
// deterministic outcome (the template is missing, replaced by a symlink, or
// its bytes no longer match the authorization) requests re-materialization.
func TestDriftCheckKicksOnDeterministicTemplateReadsOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as uid 0: read permission is not enforced")
	}
	env, fn := newDriftAuditEnv(t, nil)
	ctx := context.Background()
	templatePath := filepath.Join(fn.Dir, "template.yaml")
	original, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	restore := func() {
		if err := os.Remove(templatePath); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove template: %v", err)
		}
		if err := os.WriteFile(templatePath, original, 0o644); err != nil {
			t.Fatalf("restore template: %v", err)
		}
	}

	cfg := env.cfg
	// The seam fixes the loader's view so each check reaches the per-app
	// ReadTemplateSource call under test, whatever the filesystem currently
	// looks like.
	cfg.LoadApps = loadAppsSeam([]app.App{fn}, nil)
	c := New(cfg)
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}

	// Control: the tree still matches the authorization byte-for-byte.
	c.checkDrift(ctx)
	expectDriftKick(t, c, false, "matching tree")

	// A transient read failure requests nothing: an unreadable template says
	// nothing about its bytes.
	if err := os.Chmod(templatePath, 0o000); err != nil {
		t.Fatalf("chmod template: %v", err)
	}
	c.checkDrift(ctx)
	expectDriftKick(t, c, false, "transient template read failure")
	if err := os.Chmod(templatePath, 0o644); err != nil {
		t.Fatalf("restore template mode: %v", err)
	}
	c.checkDrift(ctx)
	expectDriftKick(t, c, false, "tree after the permissions were restored")

	// A missing template is deterministic drift: the authorized bytes are gone.
	if err := os.Remove(templatePath); err != nil {
		t.Fatalf("remove template: %v", err)
	}
	c.checkDrift(ctx)
	expectDriftKick(t, c, true, "missing template")
	restore()

	// A symlinked template is deterministic invalid input (ErrInvalidPath).
	if err := os.Remove(templatePath); err != nil {
		t.Fatalf("remove template: %v", err)
	}
	if err := os.Symlink(filepath.Join(fn.Dir, "elsewhere.yaml"), templatePath); err != nil {
		t.Fatalf("symlink template: %v", err)
	}
	c.checkDrift(ctx)
	expectDriftKick(t, c, true, "symlinked template")
	restore()

	// Changed bytes under the same path are drift even when the content
	// fingerprint would not notice (runtime-only template fields).
	if err := os.WriteFile(templatePath,
		append(append([]byte(nil), original...), []byte("# local drift\n")...),
		0o644); err != nil {
		t.Fatalf("edit template: %v", err)
	}
	c.checkDrift(ctx)
	expectDriftKick(t, c, true, "changed template bytes")
}
