package runtime

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// runningInIntermediateID matches one classic-builder stream LINE that STARTS
// an intermediate container: the bare "Running in <id>" form or the daemon's
// " ---> Running in <id>" prefix form. The phrase is anchored to the whole line
// so stray hex or text elsewhere in step output can never be mistaken for an
// ID, and the ID must be at least 12 hex characters (the daemon's truncated
// form, up to a full container ID).
var runningInIntermediateID = regexp.MustCompile(`^[ \t]*(?:--->[ \t]+)?Running in ([0-9a-fA-F]{12,})[ \t]*$`)

// removedIntermediateID matches one classic-builder stream LINE that REMOVES a
// previously started intermediate container: "Removed intermediate container
// <id>", optionally with the daemon's " ---> " prefix. It is line-anchored for
// the same reason as runningInIntermediateID. A FAILED removal instead prints
// "Removing intermediate container <id>: <err>" — different phrase, and the
// container was not removed, so it stays a retained leftover.
var removedIntermediateID = regexp.MustCompile(`^[ \t]*(?:--->[ \t]+)?Removed intermediate container ([0-9a-fA-F]{12,})[ \t]*$`)

// failedBuildIntermediateIDs extracts the classic-builder intermediate
// containers a FAILED build RETAINS, processing the propagated stream text as
// ordered lines. It records every ID a "Running in <id>" line STARTS, in first
// appearance order, case-normalized and deduplicated. A "Removed intermediate
// container <id>" line retires ONLY a started ID whose exact normalized value it
// names and only when that start was already seen, so a removal can never
// suppress a candidate whose start line comes later (or that was never started
// at all). The returned IDs are therefore started-minus-subsequently-removed:
// the daemon removes each successful step's container as the build proceeds, so
// a late failure propagates a stream naming already-deleted containers; those
// are not leftovers, and probing them would falsely report a missing required
// intermediate. Both full and truncated (12+ hex) IDs are accepted, and
// association is exact normalized equality, never a prefix match. Only the
// builder's two recognizable phrases are matched — never arbitrary hex
// substrings of step content — so the returned IDs are the daemon's own debug
// leftovers of THE failed build, and inspecting them (exact-ID probe) is what
// distinguishes a REQUIRED missing intermediate (its NotFound must fail the
// test) from any unrelated container's absence on a shared daemon. A stream
// with no removal lines yields every started ID, so a build that retained all
// of them is preserved.
func failedBuildIntermediateIDs(errText string) []string {
	var (
		started []string
		seen    = map[string]bool{}
		removed = map[string]bool{}
	)
	for line := range strings.SplitSeq(errText, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if m := removedIntermediateID.FindStringSubmatch(line); m != nil {
			id := strings.ToLower(m[1])
			// Retire only an already-started ID: a removal line is meaningful
			// solely for a container this stream started earlier.
			if seen[id] {
				removed[id] = true
			}
			continue
		}
		if m := runningInIntermediateID.FindStringSubmatch(line); m != nil {
			id := strings.ToLower(m[1])
			if !seen[id] {
				seen[id] = true
				started = append(started, id)
			}
		}
	}
	var out []string
	for _, id := range started {
		if !removed[id] {
			out = append(out, id)
		}
	}
	return out
}

// TestFailedBuildIntermediateIDsParsing pins the parse seam the failed-build
// integration test uses to identify the daemon's RETAINED debug intermediates:
// the stream's "Running in <id>" started IDs MINUS those later retired by an
// exact-ID "Removed intermediate container" line, returned once each in stream
// order, with hex substrings of unrelated build content ignored. The
// integration test then inspects exactly these retained IDs with NO NotFound
// suppression — a missing required intermediate fails, while unrelated
// containers on the shared daemon are never enumerated at all.
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

// TestFailedBuildIntermediateIDsRetainedAfterRemovals pins the retained-ID
// contract behind the failed-build integration test: the stream's started IDs
// minus ONLY subsequent exact-ID "Removed intermediate container <id>"
// retirements — never any other hex, wording, line form, or association.
func TestFailedBuildIntermediateIDsRetainedAfterRemovals(t *testing.T) {
	// A daemon names intermediates truncated to 12 hex characters; the full
	// 64-hex form is part of the same grammar.
	full := "aa11bb22cc33dd44ee55ff6600778899a1b2c3d4e5f60718a9b0c1d2e3f4a5b6"
	for _, tt := range []struct {
		name   string
		stream string
		want   []string
	}{
		// CI reproducer: the earlier step's intermediate was removed by the
		// daemon; the failing step's was left behind.
		{
			name: "only the failing step container is retained",
			stream: `Step 3/7 : RUN pnpm install --prod --frozen-lockfile
 ---> Running in 4eed31ed4eb3
 ---> Removed intermediate container 4eed31ed4eb3
Step 4/7 : RUN node dist/index.js
 ---> Running in 53d7a14157b7
The command '/bin/sh -c node dist/index.js' returned a non-zero code: 1`,
			want: []string{"53d7a14157b7"},
		},
		{
			name: "multiple removals retire only their exact started IDs",
			stream: `Running in aaaabbbbcccc
Removed intermediate container aaaabbbbcccc
Running in ddddeeee1111
Running in ffff9999abcd
Removed intermediate container ffff9999abcd`,
			want: []string{"ddddeeee1111"},
		},
		{
			name:   "no removals yield every started ID",
			stream: "Running in aaaabbbbcccc\nRunning in ddddeeee1111",
			want:   []string{"aaaabbbbcccc", "ddddeeee1111"},
		},
		{
			name: "all started IDs removed yields none retained",
			stream: `Running in aaaabbbbcccc
Removed intermediate container aaaabbbbcccc`,
			want: nil,
		},
		{
			name: "duplicate start lines collapse to one entry",
			stream: `Running in aaaabbbbcccc
Running in aaaabbbbcccc
Removed intermediate container aaaabbbbcccc`,
			want: nil,
		},
		{
			name: "unrelated hex and mid-line text never match",
			stream: `Unrelated hash b4116abce4b45e2f in step output
log: Running in ffff11112222
Running in aaaabbbbcccc
msg: Removed intermediate container aaaabbbbcccc
Removed intermediate container?
 ---> 53d7a14157b7`,
			want: []string{"aaaabbbbcccc"},
		},
		{
			name: "removal IDs are case-normalized",
			stream: `Running in aaaabbbbcccc
 ---> Removed intermediate container AAAABBBBCCCC`,
			want: nil,
		},
		{
			name: "unstarted removals suppress no started IDs",
			stream: `Removed intermediate container ffff9999abcd
Running in ddddeeee1111
Removed intermediate container eeee12341234`,
			want: []string{"ddddeeee1111"},
		},
		{
			name: "removal before its start line is not subsequent",
			stream: `Removed intermediate container aaaabbbbcccc
Running in aaaabbbbcccc`,
			want: []string{"aaaabbbbcccc"},
		},
		{
			name: "association is exact equality, never a prefix",
			stream: `Running in aaaabbbbcccc
Removed intermediate container aaaabbbbccccdddd
Running in ddddeeee11112222
Removed intermediate container ddddeeee1111`,
			want: []string{"aaaabbbbcccc", "ddddeeee11112222"},
		},
		{
			name: "a failed removal keeps the container retained",
			stream: `Running in aaaabbbbcccc
Removing intermediate container aaaabbbbcccc: rmdir: resource busy`,
			want: []string{"aaaabbbbcccc"},
		},
		{
			name: "prefixed and bare line forms are both recognized",
			stream: ` ---> Running in aaaabbbbcccc
Removed intermediate container aaaabbbbcccc
Running in ddddeeee1111
 ---> Removed intermediate container ddddeeee1111
Running in ffff9999abcd`,
			want: []string{"ffff9999abcd"},
		},
		{
			name:   "truncated and full-length IDs are both retained",
			stream: "Running in ffff9999abcd\n ---> Running in " + full,
			want:   []string{"ffff9999abcd", full},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := failedBuildIntermediateIDs(tt.stream)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("retained intermediates = %q, want %q", got, tt.want)
			}
		})
	}
}
