//go:build integration

// Lifecycle-line and runtime-metadata tests for the split Node bootstrap: the
// INIT_START report from the generated runtime metadata document, the
// START/END invocation boundary lines on the process's native stdout, and the
// guarantee that application stdout/stderr stay unchanged native logs beside
// them. The lifecycle lines carry the invocation context's STABLE LOGICAL
// requestId — deliberately different from the transport correlation id the
// frames carry — while the result frame still echoes the transport id. See
// lifecycle.mjs; the Python mirrors live in the python package.
package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// lifecycleLineIndex returns the index of the FIRST stdout line equal to want,
// or -1. Lines are split on the exact "\n" the lifecycle writer emits.
func lifecycleLineIndex(out, want string) int {
	for i, line := range strings.Split(out, "\n") {
		if line == want {
			return i
		}
	}
	return -1
}

// lifecycleLineCount returns how many stdout lines equal want.
func lifecycleLineCount(out, want string) int {
	count := 0
	for _, line := range strings.Split(out, "\n") {
		if line == want {
			count++
		}
	}
	return count
}

// writeLifecycleMetadata stages the runtime metadata document beside the
// bootstrap. The materialized lifecycle module (mirrored in-image layout)
// reads it from root/relay at startup like the image's /relay copy does.
func writeLifecycleMetadata(t *testing.T, root, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "relay"), 0o755); err != nil {
		t.Fatalf("mkdir relay: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "relay", "runtime-metadata.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("write runtime metadata: %v", err)
	}
}

// logicalRequestContext renders the context frame of an invocation whose
// lifecycle identity is the STABLE LOGICAL requestId — deliberately different
// from the transport correlation id Relay carries on the frame's own "id"
// field. The handler must name the invoked handler (the protocol rejects a
// mismatch), and the attempt/budget pair is what a retry reuses so the
// lifecycle lines of every attempt stay under one logical id.
func logicalRequestContext(handler, requestID string, attempt, maxAttempts int) map[string]any {
	return map[string]any{
		"requestId":   requestID,
		"attempt":     attempt,
		"maxAttempts": maxAttempts,
		"deadline":    time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"app":         "fn",
		"handler":     handler,
	}
}

// requireTransportEcho asserts the result frame echoes the TRANSPORT
// correlation id (never the logical requestId), the contract Relay uses to
// match a result to its in-flight invocation.
func requireTransportEcho(t *testing.T, r invokeResult, transportID string) {
	t.Helper()
	if r.Frame == "" {
		t.Fatal("no result frame to inspect")
	}
	var resp respFrame
	if err := json.Unmarshal([]byte(r.Frame), &resp); err != nil {
		t.Fatalf("result frame is not parseable JSON: %q", r.Frame)
	}
	if resp.ID != transportID {
		t.Errorf("result frame id = %q, want the transport correlation id %q (never the logical requestId)", resp.ID, transportID)
	}
}

// requireLogicalLifecycleLines asserts one invocation window's START/END lines
// carry the LOGICAL requestId — and never the transport correlation id — with
// START before the handler's app output and END after it (appOut empty skips
// the ordering half for handlers that print nothing).
func requireLogicalLifecycleLines(t *testing.T, window, logical, transportID, appOut string) {
	t.Helper()
	start := lifecycleLineIndex(window, "START RequestId: "+logical)
	end := lifecycleLineIndex(window, "END RequestId: "+logical)
	if start == -1 || end == -1 {
		t.Fatalf("window must carry START/END for the logical requestId %q:\n%s", logical, window)
	}
	if appOut != "" && !(start < lifecycleLineIndex(window, appOut) && lifecycleLineIndex(window, appOut) < end) {
		t.Fatalf("window must order START < app output < END:\n%s", window)
	}
	for _, field := range []string{"START RequestId: ", "END RequestId: "} {
		if n := lifecycleLineCount(window, field+transportID); n != 0 {
			t.Errorf("lifecycle lines must not carry the transport correlation id %q (got %d %q line(s)):\n%s", transportID, n, field+transportID, window)
		}
	}
}

// TestNodeRuntimeLifecycleInitStartAndInvocationLines pins the full lifecycle
// contract on one warm process: the native INIT_START carries exactly the
// generated runtime metadata document's pair (runtime + base image, the pinned
// reference verbatim) ONCE for the process lifetime; every accepted invocation
// gets a START line before its handler runs and an END line after it completes
// — on a handled SUCCESS and a handled FAILURE alike — with each line carrying
// the invocation context's LOGICAL requestId, never the transport correlation
// id; the result frame still echoes the transport id; and application
// stdout/stderr flow through unchanged as plain native lines beside them, with
// no lifecycle or protocol noise on either stream.
func TestNodeRuntimeLifecycleInitStartAndInvocationLines(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function show(event, context) {
  console.log("app out " + event.msg);
  console.error("app err " + event.msg);
}
export function boom(event, context) {
  console.log("failing shortly");
  throw new Error("handler-kaboom");
}
`)
	const pinnedBase = "node:24-alpine@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	writeLifecycleMetadata(t, root, `{"runtime": "nodejs24.x", "base_image": "`+pinnedBase+`"}`)
	p := startNode(t, root)
	p.awaitStdoutQuiet()

	wantInitStart := "INIT_START Runtime: nodejs24.x BaseImage: " + pinnedBase
	out := p.stdoutString()
	if n := lifecycleLineCount(out, wantInitStart); n != 1 {
		t.Fatalf("INIT_START lines = %d, want exactly 1 on startup; stdout:\n%s", n, out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "START RequestId:") || strings.HasPrefix(line, "END RequestId:") {
			t.Fatalf("an invocation line must precede the first request: %q", line)
		}
	}
	if strings.Contains(out, "app out") {
		t.Fatalf("no application output before the first invocation: %q", out)
	}

	// First invocation: the transport id is t0 (the harness's correlation id),
	// the LOGICAL id is deliberately different. START precedes the handler's
	// app output and END follows it.
	logical1 := "logical-success-1"
	r1 := p.invokeFrame(t, "index.show", `{"msg":"a"}`, nil, nil, logicalRequestContext("index.show", logical1, 1, 3))
	if !r1.OK {
		t.Fatalf("invoke 1 failed: %s", r1.Err)
	}
	requireTransportEcho(t, r1, "t0")
	requireLogicalLifecycleLines(t, r1.Out, logical1, "t0", "app out a")
	// The handler's stderr print stays exactly that plain native line: no
	// lifecycle lines and no protocol data may flow through stderr.
	stderr := p.stderrString()
	if !strings.Contains(stderr, "app err a") {
		t.Errorf("stderr = %q, want the handler's plain error print", stderr)
	}
	if strings.Contains(stderr, "RequestId:") {
		t.Errorf("stderr must not carry lifecycle lines: %q", stderr)
	}

	// Warm reuse: a second invocation on the SAME process gets its own pair
	// under its own logical id, and the process still reports INIT_START
	// exactly once.
	logical2 := "logical-success-2"
	r2 := p.invokeFrame(t, "index.show", `{"msg":"b"}`, nil, nil, logicalRequestContext("index.show", logical2, 1, 3))
	if !r2.OK {
		t.Fatalf("invoke 2 failed: %s", r2.Err)
	}
	requireTransportEcho(t, r2, "t1")
	requireLogicalLifecycleLines(t, r2.Out, logical2, "t1", "app out b")
	if got := lifecycleLineCount(p.stdoutString(), wantInitStart); got != 1 {
		t.Errorf("INIT_START lines after warm reuse = %d, want exactly 1", got)
	}

	// A handled failure completes its invocation too: the result still arrives
	// (ok:false, echoing the transport id) and END is written under the
	// logical id — a caught error is a completed invocation, not an
	// interruption.
	logical3 := "logical-handled-failure"
	r3 := p.invokeFrame(t, "index.boom", `{}`, nil, nil, logicalRequestContext("index.boom", logical3, 1, 3))
	if r3.OK {
		t.Fatal("expected the failing handler to fail its invocation")
	}
	requireTransportEcho(t, r3, "t2")
	requireLogicalLifecycleLines(t, r3.Out, logical3, "t2", "failing shortly")
	if got := lifecycleLineCount(p.stdoutString(), wantInitStart); got != 1 {
		t.Errorf("INIT_START lines after the failure = %d, want exactly 1", got)
	}
}

// TestNodeRuntimeLifecycleRequestIdStableAcrossRetries pins the retry shape:
// one logical requestId is retried with FRESH transport correlation ids and
// an advancing attempt, so the warm process serves two invocations whose
// lifecycle lines each carry the SAME logical RequestId in two separate
// START/END pairs, while both result frames echo their own transport id.
func TestNodeRuntimeLifecycleRequestIdStableAcrossRetries(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function show(event, context) {
  console.log("attempt " + context.attempt);
}
`)
	p := startNode(t, root)

	const logical = "logical-retry-id"
	pairs := 0
	for attempt := 1; attempt <= 2; attempt++ {
		// The transport correlation id is fresh per invocation (the harness's
		// t<N> sequence); the logical id never changes.
		transportID := fmt.Sprintf("t%d", p.seq)
		r := p.invokeFrame(t, "index.show", `{}`, nil, nil, logicalRequestContext("index.show", logical, attempt, 3))
		if !r.OK {
			t.Fatalf("retry attempt %d failed: %s", attempt, r.Err)
		}
		requireTransportEcho(t, r, transportID)
		requireLogicalLifecycleLines(t, r.Out, logical, transportID, "attempt "+fmt.Sprint(attempt))
		pairs++
	}

	// Two lifecycle pairs, one logical id: the whole-process stdout carries
	// START/END exactly twice under the logical id and never under either
	// transport correlation id.
	out := p.stdoutString()
	for _, field := range []string{"START RequestId: ", "END RequestId: "} {
		if got := lifecycleLineCount(out, field+logical); got != pairs {
			t.Errorf("%s%s lines = %d, want %d (one pair per attempt)", field, logical, got, pairs)
		}
		for attempt := 1; attempt <= pairs; attempt++ {
			if n := lifecycleLineCount(out, field+fmt.Sprintf("t%d", attempt-1)); n != 0 {
				t.Errorf("the transport correlation id t%d must not appear on any lifecycle line (%d %q line(s));\n%s", attempt-1, n, field+fmt.Sprintf("t%d", attempt-1), out)
			}
		}
	}
}

// TestNodeRuntimeLifecycleMissingMetadataSkipsInitStart pins the best-effort
// metadata contract: an image without the runtime metadata document starts and
// serves invocations with no INIT_START line at all, while START/END lines
// carry the logical requestId exactly as before.
func TestNodeRuntimeLifecycleMissingMetadataSkipsInitStart(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function show(event, context) {
  console.log("app out " + event.msg);
}
`)
	p := startNode(t, root)
	p.awaitStdoutQuiet()

	if strings.Contains(p.stdoutString(), "INIT_START") {
		t.Fatalf("no INIT_START may be emitted without runtime metadata, stdout:\n%s", p.stdoutString())
	}
	const logical = "logical-without-metadata"
	r := p.invokeFrame(t, "index.show", `{"msg":"a"}`, nil, nil, logicalRequestContext("index.show", logical, 1, 3))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	requireTransportEcho(t, r, "t0")
	requireLogicalLifecycleLines(t, r.Out, logical, "t0", "app out a")
	if strings.Contains(p.stdoutString(), "INIT_START") {
		t.Error("no INIT_START may appear after invocations either")
	}
}

// TestNodeRuntimeLifecycleMalformedMetadataSkipsInitStart pins the defensive
// parse branch: a metadata document that is malformed, not an object, or typed
// wrong produces NO INIT_START and never fails startup or an invocation, while
// a document with extra unknown fields tolerates them and reports only its
// runtime/base_image pair.
func TestNodeRuntimeLifecycleMalformedMetadataSkipsInitStart(t *testing.T) {
	requireNode(t)
	cases := []struct {
		name      string
		body      string
		wantStart bool
		wantLine  string
	}{
		{name: "invalid json", body: `{not json`},
		{name: "not an object", body: `"nodejs24.x"`},
		{name: "wrong field types", body: `{"runtime": 24, "base_image": null}`},
		{name: "extra fields are tolerated",
			body:      `{"runtime": "nodejs24.x", "base_image": "node:24-alpine", "digest": "sha256:deadbeef"}`,
			wantStart: true, wantLine: "INIT_START Runtime: nodejs24.x BaseImage: node:24-alpine"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := appRoot(t)
			writeModule(t, root, "index.mjs", `export function show(event, context) { console.log("app out"); }`)
			writeLifecycleMetadata(t, root, tc.body)
			p := startNode(t, root)
			p.awaitStdoutQuiet()

			out := p.stdoutString()
			if !tc.wantStart && strings.Contains(out, "INIT_START") {
				t.Fatalf("metadata %q must produce no INIT_START, stdout:\n%s", tc.body, out)
			}
			if tc.wantStart && lifecycleLineCount(out, tc.wantLine) != 1 {
				t.Fatalf("metadata %q must produce exactly %q, stdout:\n%s", tc.body, tc.wantLine, out)
			}
			if n := lifecycleLineCount(out, "INIT_START"); n > 1 {
				t.Errorf("INIT_START lines = %d, want at most 1", n)
			}
			r := p.invokeFrame(t, "index.show", `{}`, nil, nil, logicalRequestContext("index.show", "logical-id", 1, 3))
			if !r.OK {
				t.Fatalf("a startup with malformed metadata must still serve: %s", r.Err)
			}
		})
	}
}

// TestNodeRuntimeInterruptedInvocationWritesNoEnd pins the interrupted
// contract: an invocation whose handler never completes (the discarded-
// container/timeout case) gets a START line under its logical requestId but is
// NEVER reported as completed — no result frame is fabricated and no END line
// ever appears. The process must terminate promptly once the channel is
// discarded, without either line implying a completed invocation.
func TestNodeRuntimeInterruptedInvocationWritesNoEnd(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function hang(event, context) {
  return new Promise(() => {});
}
`)
	p := startNode(t, root)

	// Drive the raw exchange with a deliberately split identity: the frame
	// carries its own transport id while the context declares the logical one.
	const transportID = "interrupted-transport"
	const logical = "interrupted-logical"
	frame, err := json.Marshal(reqFrame{
		Type: "invoke", ID: transportID, Handler: "index.hang",
		Event: json.RawMessage(`{}`), Context: logicalRequestContext("index.hang", logical, 1, 1),
	})
	if err != nil {
		t.Fatalf("build request frame: %v", err)
	}
	if err := channelWriteFrame(p.conn, frame); err != nil {
		t.Fatalf("write invoke frame: %v", err)
	}
	if err := p.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := channelReadFrame(p.conn, channelResultCap); err == nil {
		t.Fatal("an incomplete handler must not receive a result frame")
	}
	if err := p.conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}
	p.awaitStdoutQuiet()
	out := p.stdoutString()
	if lifecycleLineIndex(out, "START RequestId: "+logical) == -1 {
		t.Fatalf("the interrupted invocation must have STARTed under its logical requestId, stdout:\n%s", out)
	}
	for _, field := range []string{"START RequestId: ", "END RequestId: "} {
		if n := lifecycleLineCount(out, field+transportID); n != 0 {
			t.Fatalf("lifecycle lines must not carry the transport correlation id %q:\n%s", transportID, out)
		}
	}

	// Discard the channel the way Relay does for a discarded container. The
	// bootstrap must terminate promptly; no END may have been written for the
	// interrupted invocation.
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Logf("bootstrap exit after discarding the channel: %v; stderr:\n%s", err, p.stderrString())
	}
	if out := p.stdoutString(); lifecycleLineIndex(out, "END RequestId: "+logical) != -1 {
		t.Fatalf("END must never be fabricated for an interrupted invocation; stdout:\n%s", out)
	}
}
