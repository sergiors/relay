package runtime

import (
	"testing"

	"github.com/moby/moby/api/types/image"
)

// TestImageLabelsApp verifies the app label builder produces exactly
// the managed app-image label set (type + app + fingerprint plus an
// optional dependency reference), and that an empty dependency is omitted.
func TestImageLabelsApp(t *testing.T) {
	got := appImageLabels("my-fn", "fpb1c2d3e4f5a6b7c", "relay-dep-abcdef1234567890", "bootstrap1")
	if got[labelType] != ImageTypeApp {
		t.Errorf("relay.type = %q, want app", got[labelType])
	}
	if got[labelApp] != "my-fn" {
		t.Errorf("relay.app = %q, want my-fn", got[labelApp])
	}
	if got[labelFingerprint] != "fpb1c2d3e4f5a6b7c" {
		t.Errorf("relay.fingerprint = %q", got[labelFingerprint])
	}
	if got[labelDependency] != "relay-dep-abcdef1234567890" {
		t.Errorf("relay.dependency = %q", got[labelDependency])
	}
	// A runtime label is a dependency-only field and must not appear on a
	// app image.
	if _, ok := got[labelRuntime]; ok {
		t.Error("function image must not carry a relay.runtime label")
	}

	// No dependency: omitted entirely.
	noDep := appImageLabels("my-fn", "fp", "", "")
	if _, ok := noDep[labelDependency]; ok {
		t.Error("a function without deps must not carry a relay.dependency label")
	}
}

// TestImageLabelsDependency verifies the dependency label builder produces
// exactly the managed dependency-image label set (type + runtime + fingerprint)
// and no app/dependency-owner labels.
func TestImageLabelsDependency(t *testing.T) {
	got := dependencyImageLabels("python3.14", "d1c2d3e4f5a6b7c8")
	if got[labelType] != ImageTypeDependency {
		t.Errorf("relay.type = %q, want dependency", got[labelType])
	}
	if got[labelRuntime] != "python3.14" {
		t.Errorf("relay.runtime = %q, want python3.14", got[labelRuntime])
	}
	if got[labelFingerprint] != "d1c2d3e4f5a6b7c8" {
		t.Errorf("relay.fingerprint = %q", got[labelFingerprint])
	}
	for _, key := range []string{labelApp, labelDependency} {
		if _, ok := got[key]; ok {
			t.Errorf("dependency image must not carry %q", key)
		}
	}
}

// TestPartitionManagedImagesDependencyRef verifies the partition app turns
// label-classified images into the right candidate set and referenced set: a
// app image's relay.dependency references its parent dependency, and a
// dependency image becomes a removal candidate.
func TestPartitionManagedImagesDependencyRef(t *testing.T) {
	depA := "relay-dep-aaaaaaaaaaaaaaaa"
	depB := "relay-dep-bbbbbbbbbbbbbbbb"
	items := []image.Summary{
		// A managed app image referencing depA (tagged, as a real
		// daemon-listed app image always is).
		{RepoTags: []string{"relay-app-a:aaaa"}, Labels: map[string]string{
			labelType: ImageTypeApp, labelApp: "a",
			labelDependency: depA,
		}},
		// A managed app image (no deps) — reference holder only.
		{RepoTags: []string{"relay-app-b:bbbb"}, Labels: map[string]string{
			labelType: ImageTypeApp, labelApp: "b",
		}},
		// A managed dependency image with a tagged relay-dep- reference: a
		// removal candidate.
		{RepoTags: []string{depA}, Labels: map[string]string{
			labelType: ImageTypeDependency, labelRuntime: "python3.14",
		}},
		// A managed dependency image tagged with another relay-dep- reference.
		{RepoTags: []string{depB}, Labels: map[string]string{
			labelType: ImageTypeDependency,
		}},
	}

	candidates, referenced := partitionManagedImages(items)
	if len(candidates) != 2 {
		t.Fatalf("candidates = %v, want the two tagged dependency images", candidates)
	}
	for _, c := range []string{depA, depB} {
		found := false
		for _, cand := range candidates {
			if cand == c {
				found = true
			}
		}
		if !found {
			t.Errorf("candidate %s missing", c)
		}
	}
	if !referenced[depA] {
		t.Error("depA must be referenced by function a")
	}
	if referenced[depB] {
		t.Error("depB must NOT be referenced")
	}
}

// TestPartitionManagedImagesIgnoresLabels verifies the partition app's
// strict, label-only classification: an image with no relay.type label is never
// a candidate nor a reference holder, regardless of its repository name. This
// pins the "repository names are never the sole classification signal" contract
// for unmanaged (e.g. pre-labels, or foreign) images.
func TestPartitionManagedImagesIgnoresLabels(t *testing.T) {
	items := []image.Summary{
		// A relay-app- and a relay-dep- named image with NO relay.type label are
		// unmanaged: never candidates, never reference holders. (A legacy
		// relay-dep- image from a pre-labels build, or a foreign image tagged
		// under a relay name, is inert garbage the GC deliberately leaves alone.)
		{RepoTags: []string{"relay-dep-oooooooooooooooo"}},
		{RepoTags: []string{"relay-app-user:abc"}, Labels: map[string]string{"app": "x"}},
		// A managed app image with a relay.dependency value that is NOT a
		// present candidate: its reference is still collected (there is just no
		// matching candidate to remove).
		{RepoTags: []string{"relay-app-a:cccc"}, Labels: map[string]string{
			labelType: ImageTypeApp, labelApp: "a",
			labelDependency: "relay-dep-cccccccccccccccc",
		}},
	}
	candidates, referenced := partitionManagedImages(items)
	if len(candidates) != 0 {
		t.Errorf("unlabeled images must never be removal candidates, got %v", candidates)
	}
	if !referenced["relay-dep-cccccccccccccccc"] {
		t.Error("the managed function image's dependency reference must be collected even though no candidate matches it")
	}
}

// TestPartitionManagedImagesMultiTagAndDangling verifies edge cases: a
// dependency image carrying multiple relay-dep- tags yields one candidate per
// tag, and a managed dependency image with NO RepoTag (a dangling image left by
// a failed build) never becomes a candidate.
func TestPartitionManagedImagesMultiTagAndDangling(t *testing.T) {
	items := []image.Summary{
		{Labels: map[string]string{
			labelType: ImageTypeDependency, labelRuntime: "python3.14",
		}},
		// Multi-tagged: both tags surface, but the non-relay-dep tag does not.
		{RepoTags: []string{"relay-dep-aaaaaaaaaaaaaaaa", "relay-dep-bbbbbbbbbbbbbbbb", "user-thing:v1"},
			Labels: map[string]string{labelType: ImageTypeDependency}},
	}
	candidates, _ := partitionManagedImages(items)
	if len(candidates) != 2 {
		t.Fatalf("candidates = %v, want exactly the two relay-dep tags (dangling and non-prefixed excluded)", candidates)
	}
	for _, c := range []string{"relay-dep-aaaaaaaaaaaaaaaa", "relay-dep-bbbbbbbbbbbbbbbb"} {
		found := false
		for _, cand := range candidates {
			if cand == c {
				found = true
			}
		}
		if !found {
			t.Errorf("candidate %s missing", c)
		}
	}
}

// TestPartitionManagedImagesDanglingAppNotReference pins that a DANGLING
// (untagged) labeled app image does NOT hold its relay.dependency as a
// reference. Racing same-content builds (or a tag removal the daemon would not
// turn into a full delete) leave the underlying image ID behind with its labels;
// such residue must never pin a dependency image forever. The dependency GC
// derives references from TAGGED app images only, with the daemon's
// force-free parent-layer refusal as the safety net for the edge where the
// dangling image genuinely still backs something live.
func TestPartitionManagedImagesDanglingAppNotReference(t *testing.T) {
	items := []image.Summary{
		// Dangling labeled app image: build residue, no tags, still
		// carrying a dependency label. Must NOT hold that dependency.
		{Labels: map[string]string{
			labelType:       ImageTypeApp,
			labelApp:        "residue",
			labelDependency: "relay-dep-aaaaaaaaaaaaaaaa",
		}},
		// Dangling labeled dependency image: likewise never a candidate.
		{Labels: map[string]string{
			labelType:        ImageTypeDependency,
			labelRuntime:     "python3.14",
			labelFingerprint: "aaaaaaaaaaaaaaaa",
		}},
	}
	candidates, referenced := partitionManagedImages(items)
	if len(candidates) != 0 {
		t.Errorf("dangling images must never be candidates, got %v", candidates)
	}
	if referenced["relay-dep-aaaaaaaaaaaaaaaa"] {
		t.Error("a dangling (untagged) function image must not hold its dependency as a reference")
	}
}
