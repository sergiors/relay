package runtime

import (
	"context"
	"net/http"
	"testing"

	"relay/internal/testutil"
)

// TestImageRef verifies the fingerprint-versioned format
// "relay-app-<name>:<16hex>" and that it is deterministic.
func TestImageRef(t *testing.T) {
	fp := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cases := []struct {
		name, fp, want string
	}{
		{"user-events", fp, "relay-app-user-events:0123456789abcdef"},
		{"welcome_email", fp, "relay-app-welcome_email:0123456789abcdef"},
		{"jobs.v2", fp, "relay-app-jobs.v2:0123456789abcdef"},
		// The tag is only the first 16 hex chars; the rest is dropped.
		{"a", "abcdef1234567890xyz", "relay-app-a:abcdef1234567890"},
	}
	for _, tc := range cases {
		if got := ImageRef(tc.name, tc.fp); got != tc.want {
			t.Errorf("ImageRef(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestImageRefDeterministic asserts the same (name, fingerprint) always maps to
// the same reference, and distinct fingerprints map to distinct references (so
// two source versions can never collide on one image tag).
func TestImageRefDeterministic(t *testing.T) {
	fp1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fp2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if got, again := ImageRef("fn", fp1), ImageRef("fn", fp1); got != again {
		t.Fatalf("same fingerprint yielded %q then %q, want identical", got, again)
	}
	if ImageRef("fn", fp1) == ImageRef("fn", fp2) {
		t.Fatal("distinct fingerprints must yield distinct references")
	}
}

// TestImageRefShortFingerprint verifies no panic and a sensible prefix when the
// fingerprint is shorter than 16 chars (defensive; production fingerprints are
// always 64 hex chars).
func TestImageRefShortFingerprint(t *testing.T) {
	if got, want := ImageRef("fn", "abc"), "relay-app-fn:abc"; got != want {
		t.Errorf("ImageRef(fn, abc) = %q, want %q", got, want)
	}
}

// TestRepoForNameRoundTripsThroughNameFromRepo verifies the name<->repo mapping
// is exact and that nameFromRepo rejects anything outside the Relay namespace
// or an empty name.
func TestRepoForNameRoundTripsThroughNameFromRepo(t *testing.T) {
	for _, name := range []string{"user-events", "welcome_email", "jobs.v2", "a"} {
		repo := repoForName(name)
		if got, ok := nameFromRepo(repo); !ok || got != name {
			t.Errorf("nameFromRepo(repoForName(%q)) = %q, %v; want the round trip", name, got, ok)
		}
	}

	for _, repo := range []string{
		"python:3.14-slim", // foreign repo
		"relay-app-",       // prefix with an empty name
		"relay-dep-abc",    // the dependency namespace, not apps
	} {
		if got, ok := nameFromRepo(repo); ok || got != "" {
			t.Errorf("nameFromRepo(%q) = %q, %v; want rejection", repo, got, ok)
		}
	}
}

// TestAppNameFromImage verifies the image -> app scoping guard: only a
// tagged relay-app-<name>:<tag> reference yields a name; an untagged reference or
// a foreign namespace is rejected. This is what limits a re-activation to the
// activating app's own image.
func TestAppNameFromImage(t *testing.T) {
	cases := []struct {
		image  string
		want   string
		wantOK bool
	}{
		{"relay-app-user-events:0123456789abcdef", "user-events", true},
		{"relay-app-a:tag", "a", true},
		{"relay-app-a", "", false},          // no tag
		{"python:3.14-slim", "", false},     // foreign namespace
		{"relay-dep-abc:latest", "", false}, // dependency namespace
	}
	for _, tc := range cases {
		got, ok := appNameFromImage(tc.image)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("appNameFromImage(%q) = %q, %v; want %q, %v", tc.image, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestNameFromRepoRejectsBarePrefix pins that the bare "relay-app-" prefix (no
// app name) is not a valid Relay repo.
func TestNameFromRepoRejectsBarePrefix(t *testing.T) {
	if _, ok := nameFromRepo(relayRepoPrefix); ok {
		t.Fatalf("nameFromRepo(%q) accepted an empty function name", relayRepoPrefix)
	}
}

// TestRelayTagsFiltersToStrictManagedImages verifies the client-side listing
// filter is strict and label-derived: only images carrying relay.type=app
// AND a relay.app agreeing with their relay-app-<name> repository are
// collected, grouped by app name. A prefix-only image (no managed labels),
// a mislabeled image (label names another app), a dependency image, and a
// foreign image are all ignored. It drives the real relayTags call over a
// scripted daemon so the URL/decoding path is exercised too.
func TestRelayTagsFiltersToStrictManagedImages(t *testing.T) {
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/images/json",
		body: labeledImageListJSON(
			scriptedImage{tags: []string{"relay-app-a:aaaaaaaaaaaaaaaa"}, labels: appImageLabelsForTest("a")},
			scriptedImage{tags: []string{"relay-app-a:bbbbbbbbbbbbbbbb"}, labels: appImageLabelsForTest("a")},
			scriptedImage{tags: []string{"relay-app-b:cccccccccccccccc"}, labels: appImageLabelsForTest("b")},
			// Prefix-only: no managed labels -> never Relay-owned.
			scriptedImage{tags: []string{"relay-app-legacy:dddddddddddddddd"}},
			// Mislabeled: relay.app says "other" but the repo is a -> not
			// the repository's owner.
			scriptedImage{tags: []string{"relay-app-a:eeeeeeeeeeeeeeee"}, labels: appImageLabelsForTest("other")},
			// relay.type=app but no relay.app -> not owned.
			scriptedImage{tags: []string{"relay-app-a:ffffffffffffffff"}, labels: map[string]string{labelType: ImageTypeApp}},
			scriptedImage{tags: []string{"relay-dep-deadbeef:latest"}, labels: map[string]string{labelType: ImageTypeDependency}},
			scriptedImage{tags: []string{"python:3.14-slim"}},
		),
	})
	m := &Manager{cli: cli}

	byName, err := m.relayTags(context.Background())
	if err != nil {
		t.Fatalf("relayTags: %v", err)
	}
	if len(byName) != 2 {
		t.Fatalf("collected apps = %v, want only a and b", byName)
	}
	if len(byName["a"]) != 2 {
		t.Errorf("fn a tags = %v, want two", byName["a"])
	}
	if len(byName["b"]) != 1 {
		t.Errorf("fn b tags = %v, want one", byName["b"])
	}
}

// appImageLabels builds the strict managed app-image label set for name
// (relay.type=app + relay.app=name), matching what the builder stamps.
func appImageLabelsForTest(name string) map[string]string {
	return map[string]string{labelType: ImageTypeApp, labelApp: name}
}

// TestManagedAppImageName pins the single ownership predicate directly:
// prefix + relay.type=app + a matching relay.app are all required.
func TestManagedAppImageName(t *testing.T) {
	cases := []struct {
		name   string
		repo   string
		labels map[string]string
		want   string
		wantOK bool
	}{
		{"managed", "relay-app-a", appImageLabelsForTest("a"), "a", true},
		{"prefix only", "relay-app-a", nil, "", false},
		{"untyped labels", "relay-app-a", map[string]string{labelApp: "a"}, "", false},
		{"mislabeled app", "relay-app-a", appImageLabelsForTest("b"), "", false},
		{"missing app label", "relay-app-a", map[string]string{labelType: ImageTypeApp}, "", false},
		{"dependency type", "relay-dep-x", map[string]string{labelType: ImageTypeDependency}, "", false},
		{"foreign repo", "python", appImageLabelsForTest("python"), "", false},
	}
	for _, tc := range cases {
		got, ok := managedAppImageName(tc.repo, tc.labels)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("managedAppImageName(%q) = %q, %v; want %q, %v", tc.repo, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestAppImageTagsScopesRetirementToRelayOwnedAppImages pins the GC
// ownership rule at the production retirement primitives (AppImageTags ->
// RemoveImageNow, the sequence the runner's app-removal path drives):
// AppImageTags returns only Relay's own relay-app-<name> tags carrying the
// strict managed-image labels, so an external `image`-source service's reference
// (referenced by a running service container), another app's repo, and a
// prefix-only "relay-app-*" image with no labels are never removal candidates.
// The scripted daemon has no DELETE route for the external reference; the
// transport fails the test if one is sent.
func TestAppImageTagsScopesRetirementToRelayOwnedAppImages(t *testing.T) {
	const (
		fn      = "svc-ext"
		extRef  = "ghcr.io/acme/api:1.2"
		ownTag  = "relay-app-svc-ext:0000000000000000"
		otherFn = "relay-app-other:1111111111111111"
		legacy  = "relay-app-svc-ext:9999999999999999"
	)
	containers := `[{"Id":"c1","Labels":{"relay.type":"service","relay.app":"` + fn + `",` +
		`"relay.identity":"` + extRef + `","relay.image":"` + extRef + `"}}]`
	dels := 0
	cli := newScriptedDockerClient(t,
		// RemoveImageNow's reference guard lists containers; it sees c1
		// referencing extRef, so a mistaken attempt to remove extRef is refused
		// before any DELETE.
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: containers},
		dockerRoute{method: http.MethodGet, path: "/images/json", body: labeledImageListJSON(
			scriptedImage{tags: []string{ownTag}, labels: appImageLabelsForTest(fn)},
			scriptedImage{tags: []string{otherFn}, labels: appImageLabelsForTest("other")},
			// A prefix-only image named for the SAME app but with no
			// managed labels: never a candidate.
			scriptedImage{tags: []string{legacy}},
			scriptedImage{tags: []string{extRef}},
			scriptedImage{tags: []string{"python:3.14-slim"}},
		)},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	tags, err := m.AppImageTags(context.Background(), fn)
	if err != nil {
		t.Fatalf("AppImageTags: %v", err)
	}
	// The only candidate is the app's own LABELED tag (ownTag); extRef is a
	// running service's external reference, otherFn is another app's repo,
	// and legacy is a prefix-only image, so none is ever returned.
	if len(tags) != 1 || tags[0] != ownTag {
		t.Fatalf("AppImageTags = %v, want exactly [%s]", tags, ownTag)
	}

	removed := 0
	for _, tag := range tags {
		if err := m.RemoveImageNow(context.Background(), tag); err != nil {
			t.Fatalf("RemoveImageNow(%s): %v", tag, err)
		}
		removed++
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want exactly the function's own unreferenced tag", removed)
	}
	if dels != 1 {
		t.Fatalf("DELETE calls = %d, want exactly 1 (the function's own tag)", dels)
	}
}

// TestRemoveImagesExceptStrictOwnership pins the startup sweep's ownership
// scope: only strict managed app images are removal candidates, an active
// labeled image in the keep set is preserved, a stale labeled image is removed,
// and prefix-only or external images are never touched.
func TestRemoveImagesExceptStrictOwnership(t *testing.T) {
	const (
		active   = "relay-app-a:0000000000000000"
		stale    = "relay-app-a:1111111111111111"
		legacy   = "relay-app-legacy:2222222222222222"
		external = "ghcr.io/acme/api:1.2"
	)
	var deleted []string
	cli := newScriptedDockerClient(t,
		// The container-reference guard lists containers; none reference the
		// stale image, so it is removable.
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodGet, path: "/images/json", body: labeledImageListJSON(
			scriptedImage{tags: []string{active}, labels: appImageLabelsForTest("a")},
			scriptedImage{tags: []string{stale}, labels: appImageLabelsForTest("a")},
			scriptedImage{tags: []string{legacy}},
			scriptedImage{tags: []string{external}},
		)},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { deleted = append(deleted, "x") }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	removed, err := m.RemoveImagesExcept(context.Background(), map[string]bool{active: true})
	if err != nil {
		t.Fatalf("RemoveImagesExcept: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want exactly the one stale labeled image", removed)
	}
	if len(deleted) != 1 {
		t.Fatalf("DELETE calls = %d, want exactly 1 (the stale labeled image)", len(deleted))
	}
}

// TestRemoveImagesExceptKeepsLabeledActive pins that a labeled app image in
// the keep set is never removed even though it matches the ownership predicate.
func TestRemoveImagesExceptKeepsLabeledActive(t *testing.T) {
	const active = "relay-app-a:0000000000000000"
	deletes := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/json", body: labeledImageListJSON(
			scriptedImage{tags: []string{active}, labels: appImageLabelsForTest("a")},
		)},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { deletes++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	removed, err := m.RemoveImagesExcept(context.Background(), map[string]bool{active: true})
	if err != nil {
		t.Fatalf("RemoveImagesExcept: %v", err)
	}
	if removed != 0 || deletes != 0 {
		t.Fatalf("removed=%d deletes=%d, want 0/0 (active image must be kept)", removed, deletes)
	}
}
