package worker

import (
	"errors"
	"testing"

	"relay/internal/app"
)

// startupCandidate builds one startup record for the authorization filter.
func startupCandidate(name, fingerprint string) startupApp {
	return startupApp{
		App:         app.App{Name: name},
		Fingerprint: fingerprint,
	}
}

func appNames(records []startupApp) []string {
	names := make([]string, 0, len(records))
	for _, record := range records {
		names = append(names, record.App.Name)
	}
	return names
}

// TestFilterAuthorizedStartupAppsDropsLocalOnlyCandidates pins the startup half
// of the local-/apps invariant: on a coordinator-active node only candidates
// whose fingerprint matches the materialized generation load — a local
// addition and a locally edited app are both dropped and never prepared or
// recorded as desired.
func TestFilterAuthorizedStartupAppsDropsLocalOnlyCandidates(t *testing.T) {
	records := []startupApp{
		startupCandidate("materialized", "fp-match"),
		startupCandidate("local-addition", "fp-local"),
		startupCandidate("locally-edited", "fp-edited"),
	}
	authorized := map[string]string{
		"materialized":   "fp-match",
		"locally-edited": "fp-materialized",
	}

	filtered := filterAuthorizedStartupApps(records, authorized, discardLogger())

	got := appNames(filtered)
	if len(got) != 1 || got[0] != "materialized" {
		t.Fatalf("filtered = %v, want [materialized]", got)
	}
	if filtered[0].Fingerprint != "fp-match" {
		t.Fatalf("fingerprint = %q, want the materialized one", filtered[0].Fingerprint)
	}
}

// TestFilterAuthorizedStartupEmptyAuthorizationDropsEverything pins the
// configured-Git-but-nothing-published case: a participating node with no
// desired row yet authorizes NOTHING, so every local candidate is dropped and
// /apps stays inert until a revision is published.
func TestFilterAuthorizedStartupEmptyAuthorizationDropsEverything(t *testing.T) {
	records := []startupApp{
		startupCandidate("one", "fp-1"),
		startupCandidate("two", "fp-2"),
	}

	filtered := filterAuthorizedStartupApps(records, map[string]string{}, discardLogger())

	if len(filtered) != 0 {
		t.Fatalf("filtered = %v, want no authorized candidates", appNames(filtered))
	}
}

// TestFilterAuthorizedStartupIssuesKeepsOnlyMaterializedInvalid pins that only
// invalid apps that were MATERIALIZED for the generation keep their startup
// issue: a local edit that breaks an app is drift, never a persisted invalid
// desired state.
func TestFilterAuthorizedStartupIssuesKeepsOnlyMaterializedInvalid(t *testing.T) {
	materializedBroken := errors.New("template invalid at the published revision")
	locallyBroken := errors.New("broken by a local edit")
	issues := []app.LoadIssue{
		{Name: "materialized-bad", Err: materializedBroken},
		{Name: "locally-broken", Err: locallyBroken},
	}

	filtered := filterAuthorizedStartupIssues(issues,
		map[string]string{"materialized-bad": "template invalid at the published revision"})

	if len(filtered) != 1 || filtered[0].Name != "materialized-bad" {
		t.Fatalf("filtered = %+v, want only the materialized invalid app", filtered)
	}

	// With no materialized invalid apps, every issue is dropped.
	if kept := filterAuthorizedStartupIssues(issues, nil); len(kept) != 0 {
		t.Fatalf("kept = %+v, want none (a local breakage is drift)", kept)
	}
}

// TestFilterAuthorizedStartupRetainsFailedAppWithMaterializedFingerprint pins
// the startup half of the failed-row recovery contract: when loadApplied keeps
// a build/service-failed app AUTHORIZED (its materialized fingerprint is in the
// authorization map), the startup filter retains the matching local candidate —
// so the normal startup prepare path installs the unavailable matched
// placeholder instead of dropping the app and ACKing events unmatched.
func TestFilterAuthorizedStartupRetainsFailedAppWithMaterializedFingerprint(t *testing.T) {
	// The authorization exactly as ObserveStartup reconstructs it from a
	// phase=failed row that carries a materialized fingerprint.
	authorized := map[string]string{"build-fail": "fp-bf"}
	records := []startupApp{
		startupCandidate("build-fail", "fp-bf"),
		startupCandidate("locally-added", "fp-other"),
	}

	filtered := filterAuthorizedStartupApps(records, authorized, discardLogger())

	if len(filtered) != 1 || filtered[0].App.Name != "build-fail" ||
		filtered[0].Fingerprint != "fp-bf" {
		t.Fatalf("filtered = %v, want the failed-but-materialized app retained",
			appNames(filtered))
	}
}
