package deployment

import "testing"

// TestMissingAppsExcludesDesiredInvalidFromRemoved pins the removal
// classification rule: an app that belonged to the previous generation and is
// still present in the new revision — but unloadable (a DESIRED invalid app) —
// is never recorded as removed, so its per-app progress row cannot be
// overwritten from failed to removed and its previous active generation may
// keep serving. Only names absent from both the new fingerprint set and the
// invalid set are genuine removals.
func TestMissingAppsExcludesDesiredInvalidFromRemoved(t *testing.T) {
	previous := map[string]string{
		"kept":       "fp-kept",
		"now-broken": "fp-old",
		"gone":       "fp-gone",
	}
	current := map[string]string{
		"kept": "fp-kept",
	}
	invalid := map[string]string{
		"now-broken": "template invalid",
	}

	removed := missingApps(previous, current, invalid)

	got := map[string]bool{}
	for _, name := range removed {
		got[name] = true
	}
	if len(removed) != 1 || !got["gone"] {
		t.Fatalf("removed = %v, want exactly [gone] "+
			"(a desired invalid app must not be classified removed)", removed)
	}
	if got["now-broken"] || got["kept"] {
		t.Fatalf("removed = %v, want neither the invalid nor the kept app", removed)
	}
}
