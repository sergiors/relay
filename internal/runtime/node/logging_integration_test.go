//go:build integration

// Application logging tests for the split Node bootstrap: every console
// method is wrapped with one format — ISO-8601 UTC timestamp, level label,
// then the util.format rendering — routed through the method's native stream
// with no level filtering. Direct stdout/stderr writes and the lifecycle
// lines stay plain native output beside them. See bootstrap.mjs and
// lifecycle.mjs; the Python mirror lives in the python package.
package node

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// nodeConsoleLine matches one wrapped console line: an ISO-8601 UTC
// timestamp, the level label, then the formatted message.
var nodeConsoleLine = regexp.MustCompile(
	`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) (INFO|WARN|ERROR|DEBUG) (.*)$`)

// requireConsoleLine asserts out carries exactly one line produced by the
// wrapped console for the given message: full timestamp + level + message
// shape with the expected level label, and a timestamp that parses as a real
// instant. Returns the parsed timestamp.
func requireConsoleLine(t *testing.T, out, wantLevel, wantMsg string) time.Time {
	t.Helper()
	matches := 0
	var stamp time.Time
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, wantMsg) {
			continue
		}
		matches++
		m := nodeConsoleLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("line %q must be <iso-timestamp> <LEVEL> <message>", line)
		}
		if m[2] != wantLevel {
			t.Errorf("line %q level = %q, want %q", line, m[2], wantLevel)
		}
		if m[3] != wantMsg {
			t.Errorf("line message = %q, want %q", m[3], wantMsg)
		}
		parsed, err := time.Parse(time.RFC3339Nano, m[1])
		if err != nil {
			t.Errorf("timestamp %q is not a real instant: %v", m[1], err)
		} else {
			stamp = parsed
		}
	}
	if matches != 1 {
		t.Fatalf("lines carrying %q = %d, want exactly 1; output:\n%s", wantMsg, matches, out)
	}
	return stamp
}

// requireAbsent asserts none of the markers appears in out.
func requireAbsent(t *testing.T, stream, out string, markers ...string) {
	t.Helper()
	for _, marker := range markers {
		if strings.Contains(out, marker) {
			t.Errorf("%s must not carry %q; output:\n%s", stream, marker, out)
		}
	}
}

// awaitStderrContains waits until the child's stderr carries frag within
// bound: console writes to a pipe flush independently of the result frame's
// arrival on the connection. Once the LAST stderr marker of an invocation
// arrives, every earlier same-stream write has arrived too (pipe writes are
// ordered), which anchors the stderr absence checks.
func awaitStderrContains(t *testing.T, p *nodeProc, frag string) {
	t.Helper()
	bound := 2 * time.Second
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if strings.Contains(p.stderrString(), frag) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("stderr never carried %q within %v; stderr:\n%s", frag, bound, p.stderrString())
}

// TestNodeRuntimeLoggingConsoleLevelsAndRouting pins the level labels and
// the native stream routing: console.log is labeled INFO on stdout while
// console.warn/console.error are labeled WARN/ERROR on stderr, each emitted
// exactly once, with nothing from the other stream leaking across.
func TestNodeRuntimeLoggingConsoleLevelsAndRouting(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function levels(event, context) {
  console.log("node-routing-log");
  console.warn("node-routing-warn");
  console.error("node-routing-error");
}
`)
	p := startNode(t, root)

	r := p.invoke(t, "index.levels", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	requireConsoleLine(t, r.Out, "INFO", "node-routing-log")
	requireAbsent(t, "stdout", r.Out, "node-routing-warn", "node-routing-error")

	// The error marker is stderr's LAST write for this invocation; once it
	// arrives, the earlier warn write is observable too.
	awaitStderrContains(t, p, "node-routing-error")
	stderr := p.stderrString()
	requireConsoleLine(t, stderr, "WARN", "node-routing-warn")
	requireConsoleLine(t, stderr, "ERROR", "node-routing-error")
	requireAbsent(t, "stderr", stderr, "node-routing-log")
}

// TestNodeRuntimeLoggingNoLevelFiltering pins the no-filtering contract:
// console.debug and console.info — the levels a level-gated logger would
// drop — still reach the native stdout, labeled DEBUG and INFO.
func TestNodeRuntimeLoggingNoLevelFiltering(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function noisy(event, context) {
  console.debug("node-filter-debug");
  console.info("node-filter-info");
  console.error("node-filter-anchor");
}
`)
	p := startNode(t, root)

	r := p.invoke(t, "index.noisy", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	requireConsoleLine(t, r.Out, "DEBUG", "node-filter-debug")
	requireConsoleLine(t, r.Out, "INFO", "node-filter-info")

	// The anchor is stderr's only write when routing is correct; observing it
	// proves the stream drained, so a leaked debug/info line would be visible.
	awaitStderrContains(t, p, "node-filter-anchor")
	requireAbsent(t, "stderr", p.stderrString(), "node-filter-debug", "node-filter-info")
}

// TestNodeRuntimeLoggingPrefixesTimestamp pins the timestamp: every wrapped
// line begins with a real ISO-8601 UTC instant taken around the invocation,
// separated by single spaces from the level label and the message.
func TestNodeRuntimeLoggingPrefixesTimestamp(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function stamp(event, context) {
  console.log("node-ts-stdout");
  console.error("node-ts-stderr");
}
`)
	before := time.Now().UTC()
	p := startNode(t, root)

	r := p.invoke(t, "index.stamp", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	awaitStderrContains(t, p, "node-ts-stderr")

	for _, found := range []struct {
		stream string
		stamp  time.Time
	}{
		{"stdout", requireConsoleLine(t, r.Out, "INFO", "node-ts-stdout")},
		{"stderr", requireConsoleLine(t, p.stderrString(), "ERROR", "node-ts-stderr")},
	} {
		if found.stamp.Before(before.Add(-time.Minute)) || found.stamp.After(time.Now().UTC().Add(time.Minute)) {
			t.Errorf("%s timestamp %v not taken around the invocation (before %v)",
				found.stream, found.stamp, before)
		}
	}
}

// TestNodeRuntimeLoggingFormatting pins the util.format rendering behind the
// wrapper: printf-style tokens substitute, extra arguments space-join with
// inspected objects, and %j serializes JSON — exactly as a native console
// would format them, plus the timestamp/level prefix.
func TestNodeRuntimeLoggingFormatting(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function fmt(event, context) {
  console.log("node-fmt %s=%d", "k", 3);
  console.info("node-fmt-multi", 42, { a: 1 });
  console.error("node-fmt-json %j", { code: 7 });
  console.error(new Error("node-fmt-error"));
}
`)
	p := startNode(t, root)

	r := p.invoke(t, "index.fmt", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	requireConsoleLine(t, r.Out, "INFO", "node-fmt k=3")
	requireConsoleLine(t, r.Out, "INFO", "node-fmt-multi 42 { a: 1 }")

	awaitStderrContains(t, p, `node-fmt-json {"code":7}`)
	requireConsoleLine(t, p.stderrString(), "ERROR", `node-fmt-json {"code":7}`)

	// An Error argument keeps its "Error: ..." text behind the wrapped
	// prefix; util.format renders the stack, so the record is multiline —
	// only the first line carries the timestamp/level prefix and the
	// "Error: ..." message, and the stack frames follow it.
	awaitStderrContains(t, p, "Error: node-fmt-error")
	lines := strings.Split(p.stderrString(), "\n")
	first := -1
	for i, line := range lines {
		if strings.Contains(line, "Error: node-fmt-error") {
			first = i
			break
		}
	}
	if first == -1 {
		t.Fatalf("stderr never carried the Error record; stderr:\n%s", p.stderrString())
	}
	m := nodeConsoleLine.FindStringSubmatch(lines[first])
	if m == nil {
		t.Errorf("Error line %q must be <iso-timestamp> <LEVEL> <message>", lines[first])
	} else {
		if m[2] != "ERROR" {
			t.Errorf("Error line level = %q, want %q", m[2], "ERROR")
		}
		if m[3] != "Error: node-fmt-error" {
			t.Errorf("Error line message = %q, want %q", m[3], "Error: node-fmt-error")
		}
	}
	if first+1 >= len(lines) ||
		!strings.HasPrefix(strings.TrimLeft(lines[first+1], " \t"), "at ") {
		t.Errorf("the Error record must carry its multiline stack after the prefixed line; stderr:\n%s",
			p.stderrString())
	}
}

// TestNodeRuntimeLoggingDirectStreamWritesUnchanged pins the raw-stream
// contract: process.stdout.write / process.stderr.write pass through
// byte-exact with no timestamp/level decoration, while the console line
// beside them on stdout stays wrapped.
func TestNodeRuntimeLoggingDirectStreamWritesUnchanged(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function raw(event, context) {
  process.stdout.write("node-raw-out\n");
  process.stderr.write("node-raw-err\n");
  console.log("node-raw-console");
}
`)
	p := startNode(t, root)

	r := p.invoke(t, "index.raw", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	awaitStderrContains(t, p, "node-raw-err")

	for _, raw := range []struct {
		stream string
		out    string
		want   string
	}{
		{"stdout", r.Out, "node-raw-out"},
		{"stderr", p.stderrString(), "node-raw-err"},
	} {
		for _, line := range strings.Split(raw.out, "\n") {
			if strings.Contains(line, raw.want) && line != raw.want {
				t.Errorf("%s line %q must stay the raw write %q (no prefix)",
					raw.stream, line, raw.want)
			}
		}
	}
	requireConsoleLine(t, r.Out, "INFO", "node-raw-console")
	requireAbsent(t, "stdout", r.Out, "node-raw-err")
	requireAbsent(t, "stderr", p.stderrString(), "node-raw-out", "node-raw-console")
}

// TestNodeRuntimeLoggingLeavesLifecycleLinesUnchanged pins the lifecycle
// exemption: START/END and the post-END REPORT stay exact plain lines with no
// timestamp/level prefix, even though console.log is wrapped — and even when
// the application itself replaces console.log entirely, because the lifecycle
// and report writers bound the native method before any handler could be
// imported.
func TestNodeRuntimeLoggingLeavesLifecycleLinesUnchanged(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
console.log = (...args) =>
  process.stdout.write("node-hijacked " + args.join(" ") + "\n");

export function hijack(event, context) {
  console.log("node-hijacked-app-call");
}
`)
	p := startNode(t, root)

	r := p.invoke(t, "index.hijack", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	// Lifecycle and report lines stay exact plain lines: a decorated variant
	// would appear as a line that merely contains the RequestId marker.
	for _, line := range strings.Split(r.Out, "\n") {
		if !strings.Contains(line, "RequestId:") {
			continue
		}
		switch {
		case line == "START RequestId: test-request":
		case line == "END RequestId: test-request":
		case strings.HasPrefix(line, "REPORT RequestId: "):
			// Validated below against the full report contract.
		default:
			t.Errorf("lifecycle line %q must stay an exact plain line:\n%s", line, r.Out)
		}
	}
	if lifecycleLineIndex(r.Out, "START RequestId: test-request") == -1 ||
		lifecycleLineIndex(r.Out, "END RequestId: test-request") == -1 {
		t.Errorf("window must carry the exact START/END lines:\n%s", r.Out)
	}
	// Exactly one REPORT, in the exact report grammar, after END — its
	// plain-line exemption is pinned here, its format by the helper.
	requireInvocationReport(t, r.Out, "test-request", "t0")

	// The application's own replacement took effect for application code,
	// beside the untouched lifecycle lines.
	if !strings.Contains(r.Out, "node-hijacked node-hijacked-app-call") {
		t.Errorf("window must carry the application's replaced console output:\n%s", r.Out)
	}
}
