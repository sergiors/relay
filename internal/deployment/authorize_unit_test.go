package deployment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
)

// discardLog returns a logger that never writes, for loader-default tests.
func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestAuthorizeReportsRootLoadFailure pins the root/global loader failure
// contract: when the /apps root cannot be loaded at all (a non-directory root,
// or an injected loader error), authorize must NOT present the result as a
// valid empty deployment — it returns a root authErr for every map, so the
// caller aborts the attempt, retains the previous applied authorization,
// records node failure, and retries instead of pruning prior progress.
func TestAuthorizeReportsRootLoadFailure(t *testing.T) {
	t.Run("apps root is a file", func(t *testing.T) {
		appsPath := filepath.Join(t.TempDir(), "apps")
		if err := os.WriteFile(appsPath, []byte("not a directory"), 0o644); err != nil {
			t.Fatalf("write apps file: %v", err)
		}
		c := New(Config{AppsDir: appsPath, Logger: discardLog()})

		fingerprints, templates, invalid, appErrors, err := c.authorize(context.Background())

		if err == nil || !strings.Contains(err.Error(), "load materialized apps") {
			t.Fatalf("authorize err = %v, want the root-load failure", err)
		}
		if fingerprints != nil || templates != nil || invalid != nil || appErrors != nil {
			t.Fatalf("authorize maps = (%v, %v, %v, %v), want all nil on a root failure "+
				"(never a valid empty set)", fingerprints, templates, invalid, appErrors)
		}
	})

	t.Run("injected loader error", func(t *testing.T) {
		rootErr := errors.New("permission denied reading /apps")
		c := New(Config{
			AppsDir: t.TempDir(),
			LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
				return nil, nil, rootErr
			},
		})

		_, _, _, _, err := c.authorize(context.Background())
		if !errors.Is(err, rootErr) {
			t.Fatalf("authorize err = %v, want it to wrap the root loader error", err)
		}
	})
}

// TestAuthorizeEmptyWithoutErrorIsValid pins the other side of the contract: a
// loader that SUCCEEDS with zero apps is a genuinely empty revision
// (authErr nil, empty non-nil maps) — only a root failure aborts. Per-app load
// issues are diagnostics, not root failures, and land in appErrors /
// invalidTemplates while authorization continues.
func TestAuthorizeEmptyWithoutErrorIsValid(t *testing.T) {
	t.Run("empty tree is a valid empty deployment", func(t *testing.T) {
		c := New(Config{
			AppsDir: t.TempDir(),
			LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
				return []app.App{}, nil, nil
			},
		})

		fingerprints, templates, invalid, appErrors, err := c.authorize(context.Background())
		if err != nil {
			t.Fatalf("authorize err = %v, want nil for a valid empty tree", err)
		}
		if fingerprints == nil || templates == nil || invalid == nil || appErrors == nil {
			t.Fatalf("authorize maps = (%v, %v, %v, %v), want empty non-nil maps",
				fingerprints, templates, invalid, appErrors)
		}
		if len(fingerprints) != 0 {
			t.Fatalf("fingerprints = %v, want empty", fingerprints)
		}
	})

	t.Run("per-app issues are not root failures", func(t *testing.T) {
		c := New(Config{
			AppsDir: t.TempDir(),
			LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
				return nil, []app.LoadIssue{{
					Name:     "bad",
					Err:      errors.New("template invalid"),
					Identity: "tid-bad",
				}}, nil
			},
		})

		fingerprints, _, invalid, appErrors, err := c.authorize(context.Background())
		if err != nil {
			t.Fatalf("authorize err = %v, want nil (a per-app issue is not a root failure)",
				err)
		}
		if appErrors["bad"] == "" {
			t.Fatalf("appErrors = %v, want the per-app diagnostic", appErrors)
		}
		if invalid["bad"] == "" {
			t.Fatalf("invalid templates = %v, want the materialized identity", invalid)
		}
		if len(fingerprints) != 0 {
			t.Fatalf("fingerprints = %v, want the invalid app excluded", fingerprints)
		}
	})
}

// TestAuthorizePersistsIdentityForRecoverableFingerprintError pins that a
// per-app FINGERPRINT failure (a recoverable read error on one source file,
// not a root failure) keeps authorization running while persisting the
// app's materialized template identity alongside the diagnostic. On restart
// the persisted identity lets the reconciler RETRY the app while its template
// bytes still match the recorded identity; invalid template bytes, by
// contrast, never activate — they stay desired-invalid diagnostics.
func TestAuthorizePersistsIdentityForRecoverableFingerprintError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read permission is not enforced as root")
	}
	// A runtime-backed template (same shape the coordinator materializes).
	const authorizeTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n" +
		"    pattern:\n      event_name: [INSERT]\n"
	appsDir := t.TempDir()
	fnDir := filepath.Join(appsDir, "fpfail")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	templatePath := filepath.Join(fnDir, "template.yaml")
	if err := os.WriteFile(templatePath, []byte(authorizeTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	unreadable := filepath.Join(fnDir, "a-unreadable.txt")
	if err := os.WriteFile(unreadable, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatalf("chmod source file: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o644) })

	tmpl, err := app.ParseTemplate([]byte(authorizeTemplate))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	c := New(Config{
		AppsDir: appsDir,
		LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
			return []app.App{{
				Name: "fpfail", Dir: fnDir, Template: tmpl,
			}}, nil, nil
		},
	})

	fingerprints, templateIdentities, invalid, appErrors, err := c.authorize(context.Background())
	if err != nil {
		t.Fatalf("authorize err = %v, want nil (a fingerprint error is not a root failure)",
			err)
	}
	if appErrors["fpfail"] == "" {
		t.Fatalf("appErrors = %v, want the fingerprint diagnostic", appErrors)
	}
	src, err := app.ReadTemplateSource(appsDir, "fpfail")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	if invalid["fpfail"] != src.Identity {
		t.Fatalf("invalid templates = %v, want the persisted materialized identity %q",
			invalid, src.Identity)
	}
	if _, ok := fingerprints["fpfail"]; ok {
		t.Fatalf("fingerprints = %v, want the unfingerprintable app excluded", fingerprints)
	}
	if _, ok := templateIdentities["fpfail"]; ok {
		t.Fatalf("template identities = %v, want the app excluded until it fingerprints",
			templateIdentities)
	}
}
