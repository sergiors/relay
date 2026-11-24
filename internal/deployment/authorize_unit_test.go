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

// TestAuthorizeFingerprintFailureFormsNoIncompleteAuthorization replaces the
// obsolete "persists identity for a recoverable fingerprint error"
// expectation: a parseable app whose SOURCE cannot be fingerprinted is no
// longer recorded as a per-app appErrors/invalidTemplates row. That committed
// an incomplete applied authorization with no fingerprint for a parseable app:
// loadApplied put the app in Snapshot.Invalid, startup dropped its valid
// candidate, and a matching delivery could be ACKed unmatched. The whole pass
// now fails with every map nil, so the caller keeps the prior applied
// authorization and retries. Cancellation fails the pass the same way. (An
// end-to-end materialization of such a tree is impractical here: every
// coordinator test uses the fake Git remote fixture, so no test runs
// SyncCommitFromConfig to completion. The prior-applied-auth preservation on a
// root authorize error is the caller contract already pinned by
// TestAuthorizeReportsRootLoadFailure and the
// TestObserveStartupSourceMismatchRetainsPreviousGeneration atomic path.)
// Ordinary image BUILD failures with a known fingerprint are unaffected: they
// stay unavailable/matched at the worker's startup publication boundary.
func TestAuthorizeFingerprintFailureFormsNoIncompleteAuthorization(t *testing.T) {
	// A runtime-backed template (same shape the coordinator materializes).
	const authorizeTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n" +
		"    pattern:\n      event_name: [INSERT]\n"
	writeApp := func(t *testing.T) (string, string, *app.Template) {
		t.Helper()
		appsDir := t.TempDir()
		fnDir := filepath.Join(appsDir, "fpfail")
		if err := os.MkdirAll(fnDir, 0o755); err != nil {
			t.Fatalf("mkdir app: %v", err)
		}
		templatePath := filepath.Join(fnDir, "template.yaml")
		if err := os.WriteFile(templatePath, []byte(authorizeTemplate), 0o644); err != nil {
			t.Fatalf("write template: %v", err)
		}
		tmpl, err := app.ParseTemplate([]byte(authorizeTemplate))
		if err != nil {
			t.Fatalf("parse template: %v", err)
		}
		return appsDir, fnDir, tmpl
	}
	seamFor := func(appsDir, fnDir string, tmpl *app.Template) Config {
		return Config{
			AppsDir: appsDir,
			LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
				return []app.App{{
					Name: "fpfail", Dir: fnDir, Template: tmpl,
					// The loaded record carries the identity of the exact bytes
					// it parsed, as the real loader does.
					TemplateIdentity: app.TemplateIdentity([]byte(authorizeTemplate)),
				}}, nil, nil
			},
		}
	}

	t.Run("source fingerprint failure is a root failure", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("read permission is not enforced as root")
		}
		appsDir, fnDir, tmpl := writeApp(t)
		unreadable := filepath.Join(fnDir, "a-unreadable.txt")
		if err := os.WriteFile(unreadable, []byte("secret"), 0o644); err != nil {
			t.Fatalf("write source file: %v", err)
		}
		if err := os.Chmod(unreadable, 0o000); err != nil {
			t.Fatalf("chmod source file: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(unreadable, 0o644) })
		c := New(seamFor(appsDir, fnDir, tmpl))

		fingerprints, templates, invalid, appErrors, err := c.authorize(context.Background())

		if err == nil ||
			!strings.Contains(err.Error(), `app "fpfail"`) ||
			!strings.Contains(err.Error(), "fingerprint materialized source") {
			t.Fatalf("authorize err = %v, want the root fingerprint failure "+
				"naming the app", err)
		}
		if fingerprints != nil || templates != nil || invalid != nil || appErrors != nil {
			t.Fatalf("authorize maps = (%v, %v, %v, %v), want all nil — a "+
				"fingerprint failure must never form an incomplete authorization "+
				"(no appErrors row, no fingerprint-less invalid entry)",
				fingerprints, templates, invalid, appErrors)
		}
	})

	t.Run("cancellation is a root failure", func(t *testing.T) {
		appsDir, fnDir, tmpl := writeApp(t)
		c := New(seamFor(appsDir, fnDir, tmpl))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		fingerprints, templates, invalid, appErrors, err := c.authorize(ctx)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("authorize err = %v, want the cancellation as a root "+
				"failure", err)
		}
		if fingerprints != nil || templates != nil || invalid != nil || appErrors != nil {
			t.Fatalf("authorize maps = (%v, %v, %v, %v), want all nil on "+
				"cancellation (never a per-app definition)", fingerprints,
				templates, invalid, appErrors)
		}
	})
}

// TestAuthorizeRefusesRecordWithoutTemplateIdentity pins the conservative
// boundary of the carried-identity contract: a valid loaded record that cannot
// prove which template bytes produced its parse is NOT authorized — the whole
// attempt fails closed (root-failure semantics, all maps nil) instead of
// pairing the fingerprint with unprovable template bytes or silently re-reading
// template.yaml after the scan.
func TestAuthorizeRefusesRecordWithoutTemplateIdentity(t *testing.T) {
	const authorizeTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n" +
		"    pattern:\n      event_name: [INSERT]\n"
	appsDir := t.TempDir()
	fnDir := filepath.Join(appsDir, "no-identity")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "template.yaml"),
		[]byte(authorizeTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	tmpl, err := app.ParseTemplate([]byte(authorizeTemplate))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	c := New(Config{
		AppsDir: appsDir,
		LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
			// A valid record whose carried identity is absent — the only shape
			// the real loader can no longer produce, but a seam or a future
			// caller could.
			return []app.App{{Name: "no-identity", Dir: fnDir, Template: tmpl}},
				nil, nil
		},
	})

	fingerprints, templates, invalid, appErrors, err := c.authorize(context.Background())

	if err == nil || !strings.Contains(err.Error(), "has no template identity") {
		t.Fatalf("authorize err = %v, want the missing-identity refusal", err)
	}
	if fingerprints != nil || templates != nil || invalid != nil || appErrors != nil {
		t.Fatalf("authorize maps = (%v, %v, %v, %v), want all nil on a missing "+
			"identity refusal (never an authorization with unprovable content)",
			fingerprints, templates, invalid, appErrors)
	}
}

// TestAuthorizePersistsCarriedIdentityFromLoadedRecord pins that the
// authorization records the identity carried by the loaded parse itself, not a
// later re-read of template.yaml: when the file changes after the scan, the
// persisted identity still describes the exact bytes that produced the parsed
// template and fingerprint the authorization pairs it with.
func TestAuthorizePersistsCarriedIdentityFromLoadedRecord(t *testing.T) {
	const authorizeTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n" +
		"    pattern:\n      event_name: [INSERT]\n"
	appsDir := t.TempDir()
	fnDir := filepath.Join(appsDir, "fn")
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	templatePath := filepath.Join(fnDir, "template.yaml")
	if err := os.WriteFile(templatePath, []byte(authorizeTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fnDir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	tmpl, err := app.ParseTemplate([]byte(authorizeTemplate))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	carried := app.TemplateIdentity([]byte(authorizeTemplate))
	c := New(Config{
		AppsDir: appsDir,
		LoadApps: func(string) ([]app.App, []app.LoadIssue, error) {
			return []app.App{{
				Name: "fn", Dir: fnDir, Template: tmpl,
				TemplateIdentity: carried,
			}}, nil, nil
		},
	})

	// when template.yaml changes after the loaded record was produced
	editedTemplate := authorizeTemplate + "resources:\n  memory: 1GiB\n"
	if err := os.WriteFile(templatePath, []byte(editedTemplate), 0o644); err != nil {
		t.Fatalf("rewrite template: %v", err)
	}
	editedIdentity := app.TemplateIdentity([]byte(editedTemplate))
	if editedIdentity == carried {
		t.Fatal("precondition: the edit must change the raw template identity")
	}

	fingerprints, templateIdentities, invalid, appErrors, err := c.authorize(context.Background())

	// then the carried identity — the bytes the parse actually saw — is the
	// one persisted alongside the fingerprint, never the re-read value
	if err != nil {
		t.Fatalf("authorize err = %v, want nil", err)
	}
	if len(appErrors) != 0 || len(invalid) != 0 {
		t.Fatalf("app errors/invalid = %v/%v, want none", appErrors, invalid)
	}
	if fingerprints["fn"] == "" {
		t.Fatalf("fingerprints = %v, want the loaded app fingerprinted",
			fingerprints)
	}
	if templateIdentities["fn"] != carried {
		t.Fatalf("template identity = %q, want the carried parse identity %q, "+
			"not a re-read of %q", templateIdentities["fn"], carried, editedIdentity)
	}
}
