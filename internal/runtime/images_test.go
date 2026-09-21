package runtime

import (
	"context"
	"net/http"
	"testing"
)

// TestImageRef verifies the fingerprint-versioned format
// "relay-fn-<name>:<16hex>" and that it is deterministic.
func TestImageRef(t *testing.T) {
	fp := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cases := []struct {
		name, fp, want string
	}{
		{"user-events", fp, "relay-fn-user-events:0123456789abcdef"},
		{"welcome_email", fp, "relay-fn-welcome_email:0123456789abcdef"},
		{"jobs.v2", fp, "relay-fn-jobs.v2:0123456789abcdef"},
		// The tag is only the first 16 hex chars; the rest is dropped.
		{"a", "abcdef1234567890xyz", "relay-fn-a:abcdef1234567890"},
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
	if got, want := ImageRef("fn", "abc"), "relay-fn-fn:abc"; got != want {
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
		"relay-fn-",        // prefix with an empty name
		"relay-dep-abc",    // the dependency namespace, not functions
	} {
		if got, ok := nameFromRepo(repo); ok || got != "" {
			t.Errorf("nameFromRepo(%q) = %q, %v; want rejection", repo, got, ok)
		}
	}
}

// TestFunctionNameFromImage verifies the image -> function scoping guard: only a
// tagged relay-fn-<name>:<tag> reference yields a name; an untagged reference or
// a foreign namespace is rejected. This is what limits a re-activation to the
// activating function's own image.
func TestFunctionNameFromImage(t *testing.T) {
	cases := []struct {
		image  string
		want   string
		wantOK bool
	}{
		{"relay-fn-user-events:0123456789abcdef", "user-events", true},
		{"relay-fn-a:tag", "a", true},
		{"relay-fn-a", "", false},           // no tag
		{"python:3.14-slim", "", false},     // foreign namespace
		{"relay-dep-abc:latest", "", false}, // dependency namespace
	}
	for _, tc := range cases {
		got, ok := functionNameFromImage(tc.image)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("functionNameFromImage(%q) = %q, %v; want %q, %v", tc.image, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestNameFromRepoRejectsBarePrefix pins that the bare "relay-fn-" prefix (no
// function name) is not a valid Relay repo.
func TestNameFromRepoRejectsBarePrefix(t *testing.T) {
	if _, ok := nameFromRepo(relayRepoPrefix); ok {
		t.Fatalf("nameFromRepo(%q) accepted an empty function name", relayRepoPrefix)
	}
}

// TestRelayTagsFiltersToRelayNamespace verifies the client-side listing filter:
// only relay-fn-<name> RepoTags are collected, grouped by function name, and a
// foreign or untagged tag is ignored. It drives the real relayTags call over a
// scripted daemon so the URL/decoding path is exercised too.
func TestRelayTagsFiltersToRelayNamespace(t *testing.T) {
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/images/json",
		body: imageListJSON(
			"relay-fn-a:aaaaaaaaaaaaaaaa",
			"relay-fn-a:bbbbbbbbbbbbbbbb",
			"relay-fn-b:cccccccccccccccc",
			"relay-dep-deadbeef:latest",
			"python:3.14-slim",
			"untagged",
		),
	})
	m := &Manager{cli: cli}

	byName, err := m.relayTags(context.Background())
	if err != nil {
		t.Fatalf("relayTags: %v", err)
	}
	if len(byName) != 2 {
		t.Fatalf("collected functions = %v, want only a and b", byName)
	}
	if len(byName["a"]) != 2 {
		t.Errorf("fn a tags = %v, want two", byName["a"])
	}
	if len(byName["b"]) != 1 {
		t.Errorf("fn b tags = %v, want one", byName["b"])
	}
}
