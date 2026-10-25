package runtime

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// runningInIntermediateID matches the classic builder's "Running in <id>"
// stream line, whose <id> is a container ID the failed build leaves behind.
var runningInIntermediateID = regexp.MustCompile(`Running in ([0-9a-fA-F]{12,})`)

// failedBuildIntermediateIDs extracts every classic-builder intermediate
// container ID the propagated build stream text names, in stream order,
// deduplicated. It deliberately matches ONLY the builder's "Running in"
// syntax — never stray hex substrings of step content — so the returned IDs
// are the daemon's own debug leftovers of THE failed build, and inspecting
// them (exact-ID probe) is what distinguishes a REQUIRED missing intermediate
// (its NotFound must fail the test) from any unrelated container's absence on
// a shared daemon.
func failedBuildIntermediateIDs(errText string) []string {
	var (
		out  []string
		seen = map[string]bool{}
	)
	for _, m := range runningInIntermediateID.FindAllStringSubmatch(errText, -1) {
		id := strings.ToLower(m[1])
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// TestFailedBuildIntermediateIDsParsing pins the parse seam the failed-build
// integration test uses to identify the daemon's kept debug intermediates: it
// captures ONLY the classic builder's "Running in <id>" lines of the
// propagated build stream text, returns each ID exactly once in stream order,
// and ignores hex substrings in unrelated build content. The integration test
// then inspects exactly these IDs with NO NotFound suppression — a missing
// required intermediate fails, while unrelated containers on the shared daemon
// are never enumerated at all.
func TestFailedBuildIntermediateIDsParsing(t *testing.T) {
	stream := `Step 3/7 : RUN pnpm install --prod --frozen-lockfile
 ---> Running in 1a2b3c4d5e6f
4M7AJHSZ+g0PfTOUXcSt4e0l b4116abce4b45e2f (unrelated hex in step content)
 ---> Running in 9999bbbbbbbb
Error: EISDIR: illegal operation on a directory`
	got := failedBuildIntermediateIDs(stream)
	want := []string{"1a2b3c4d5e6f", "9999bbbbbbbb"}
	if !slices.Equal(got, want) {
		t.Fatalf("parsed intermediates = %v, want %v", got, want)
	}
	// Duplicated lines collapse (one entry per physical container).
	if again := failedBuildIntermediateIDs(stream + stream); !slices.Equal(again, want) {
		t.Fatalf("deduplicated parse = %v, want %v", again, want)
	}
	if none := failedBuildIntermediateIDs("error: malformed package.json (no intermediates)"); none != nil {
		t.Fatalf("non-matching stream = %v, want none", none)
	}
}

// TestFailedBuildIntermediateIDsParsingNormalizesCase covers the daemon ID
// casing edge: mixed-case stream IDs normalize to lowercase so the inspect
// needle is stable against the daemon's canonical container-ID form.
func TestFailedBuildIntermediateIDsParsingNormalizesCase(t *testing.T) {
	got := failedBuildIntermediateIDs("Running in AAAABBBBCCCC")
	if len(got) != 1 || got[0] != "aaaabbbbcccc" {
		t.Fatalf("mixed-case normalization = %v, want [aaaabbbbcccc]", got)
	}
}
