//go:build integration

package cli

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/testutil"
)

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
		recordSuccess(t, st, name, "img-"+name, "fp-"+name, preparedAt,
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
	// `app ls` reads the state database, so this needs the real-state fixture
	// even though the database is fresh and empty.
	testutil.RequireStateDatabase(t)
	deps := stateTestDeps(t)

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
	testutil.RequireStateDatabase(t)
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
