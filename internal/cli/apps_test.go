package cli

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v3"

	"relay/internal/app"
)

// appListExitCode extracts the process exit code urfave's cli.Exit carries, so
// alias tests can assert the usage-error exit code (2) matches `app ls`, not
// only the message. A non-ExitCoder error reports -1.
func appListExitCode(err error) int {
	var ec cli.ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return -1
}

// seedListState creates a temp state DB holding several reconciled apps whose
// prepared_at is a fixed instant in the past, so the UPDATED column renders a
// stable relative age across the two command invocations the alias tests
// compare (no second-boundary flake). Names are deliberately unsorted so a test
// can assert both commands apply the same name ordering.
func seedListState(t *testing.T) Dependencies {
	t.Helper()
	st, deps := openTempState(t)

	tmpl, err := app.ParseTemplate([]byte(`runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	preparedAt := time.Now().Add(-2 * time.Hour)
	for _, name := range []string{"zeta-app", "alpha-app", "mid-app", "beta-app"} {
		st.RecordReconcileSuccess(name, "img-"+name, "fp-"+name, preparedAt,
			app.App{Name: name, Dir: filepath.Join(t.TempDir(), name), Template: tmpl})
	}
	return deps
}

// TestAppsAliasMatchesAppLsEmptyDB pins that the top-level `relay apps` short
// form is observably identical to the canonical `relay app ls` on a fresh,
// empty state database: same stdout (header only), same empty stderr, same nil
// error. The two entry points share one Action, so this guards against a future
// divergence such as a second formatting path.
func TestAppsAliasMatchesAppLsEmptyDB(t *testing.T) {
	deps := testDeps(t)

	canonicalOut, canonicalErrOut, canonicalErr := runCLIWithDeps(t, deps, "", "app", "ls")
	aliasOut, aliasErrOut, aliasErr := runCLIWithDeps(t, deps, "", "apps")

	if aliasOut != canonicalOut {
		t.Fatalf("stdout differs:\n apps:    %q\n app ls: %q", aliasOut, canonicalOut)
	}
	if aliasErrOut != canonicalErrOut {
		t.Fatalf("stderr differs:\n apps:    %q\n app ls: %q", aliasErrOut, canonicalErrOut)
	}
	if (aliasErr == nil) != (canonicalErr == nil) {
		t.Fatalf("error presence differs: apps=%v, app ls=%v", aliasErr, canonicalErr)
	}
}

// TestAppsAliasMatchesAppLsPopulatedDB pins that `relay apps` renders exactly
// the same table as `relay app ls` for a populated database with several apps,
// and that both sort rows by name. Comparing the two live outputs (rather than
// a copied fixture) keeps the alias honest without duplicating the list format.
func TestAppsAliasMatchesAppLsPopulatedDB(t *testing.T) {
	deps := seedListState(t)

	canonicalOut, _, canonicalErr := runCLIWithDeps(t, deps, "", "app", "ls")
	aliasOut, _, aliasErr := runCLIWithDeps(t, deps, "", "apps")
	if canonicalErr != nil {
		t.Fatalf("app ls: err = %v, want nil", canonicalErr)
	}
	if aliasErr != nil {
		t.Fatalf("apps: err = %v, want nil", aliasErr)
	}
	if aliasOut != canonicalOut {
		t.Fatalf("stdout differs:\n apps:\n%s\n app ls:\n%s", aliasOut, canonicalOut)
	}

	// Both must list every seeded app in name order.
	last := -1
	for _, name := range []string{"alpha-app", "beta-app", "mid-app", "zeta-app"} {
		idx := strings.Index(aliasOut, name)
		if idx < 0 {
			t.Fatalf("apps output missing %q:\n%s", name, aliasOut)
		}
		if idx < last {
			t.Fatalf("apps output not sorted by name (found %q out of order):\n%s", name, aliasOut)
		}
		last = idx
	}
}

// TestAppsAliasRejectsArguments pins that the alias is not a namespace: every
// positional token is a usage error with the same message and exit code as
// `app ls extra`, and in particular `apps inspect` / `apps invoke` do NOT
// resolve to `relay app` subcommands.
func TestAppsAliasRejectsArguments(t *testing.T) {
	deps := seedListState(t)

	canonicalOut, _, canonicalErr := runCLIWithDeps(t, deps, "", "app", "ls", "extra")
	if canonicalErr == nil {
		t.Fatal("app ls extra: err = nil, want usage error")
	}

	for _, args := range [][]string{
		{"apps", "extra"},
		{"apps", "inspect"},
		{"apps", "invoke"},
		{"apps", "inspect", "alpha-app"},
		{"apps", "invoke", "alpha-app"},
	} {
		out, _, err := runCLIWithDeps(t, deps, "", args...)
		if err == nil {
			t.Fatalf("%v: err = nil, want usage error", args)
		}
		if err.Error() != canonicalErr.Error() {
			t.Fatalf("%v: err = %q, want %q (same as app ls)", args, err, canonicalErr)
		}
		if got := appListExitCode(err); got != appListExitCode(canonicalErr) {
			t.Fatalf("%v: exit code = %d, want %d", args, got, appListExitCode(canonicalErr))
		}
		if out != canonicalOut {
			t.Fatalf("%v: stdout = %q, want %q", args, out, canonicalOut)
		}
	}
}

// TestAppsAliasErrorsMatchAppLs compares the alias's argument error against the
// canonical command's error verbatim, so a change to one is caught by the other.
func TestAppsAliasErrorsMatchAppLs(t *testing.T) {
	deps := testDeps(t)

	_, _, canonicalErr := runCLIWithDeps(t, deps, "", "app", "ls", "extra")
	_, _, aliasErr := runCLIWithDeps(t, deps, "", "apps", "extra")
	if canonicalErr == nil || aliasErr == nil {
		t.Fatalf("want both errors, got app ls=%v apps=%v", canonicalErr, aliasErr)
	}
	if aliasErr.Error() != canonicalErr.Error() {
		t.Fatalf("error differs:\n apps:    %q\n app ls: %q", aliasErr, canonicalErr)
	}
	if want := 2; appListExitCode(aliasErr) != want {
		t.Fatalf("apps exit code = %d, want %d", appListExitCode(aliasErr), want)
	}
}

// TestAppsHelpDescribesList pins that the alias's own top-level help makes its
// purpose clear: it lists apps and is the short form of `relay app ls`.
func TestAppsHelpDescribesList(t *testing.T) {
	out, _, err := runCLI(t, "", "apps", "--help")
	if err != nil {
		t.Fatalf("apps --help: err = %v, want nil", err)
	}
	for _, want := range []string{"List apps", "relay app ls"} {
		if !strings.Contains(out, want) {
			t.Fatalf("apps help missing %q:\n%s", want, out)
		}
	}
}

// TestRootHelpListsApps pins that the top-level help advertises the short form
// alongside the canonical app group.
func TestRootHelpListsApps(t *testing.T) {
	out, _, err := runCLI(t, "", "--help")
	if err != nil {
		t.Fatalf("--help: err = %v, want nil", err)
	}
	cmds := out
	if i := strings.Index(cmds, "COMMANDS:"); i >= 0 {
		cmds = cmds[i:]
	}
	if !strings.Contains(cmds, "apps") {
		t.Fatalf("COMMANDS block must list the apps alias:\n%s", out)
	}
}
