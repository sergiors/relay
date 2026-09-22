package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
)

// frame returns a stdcopy-multiplexed frame: an 8-byte header (stream byte at
// offset 0, big-endian payload length at bytes 4..7) followed by payload. It is
// the exact wire format stdcopy demultiplexes (see stdcopy.StdCopy).
func frame(stream byte, payload string) []byte {
	const prefixLen = 8
	buf := make([]byte, prefixLen+len(payload))
	buf[0] = stream
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[prefixLen:], payload)
	return buf
}

// newFunctionOutputSink installs a bytes.Buffer as the function-output sink and
// returns it, registering restoration of the previous sink. Container
// stdout/stderr is transport-forwarded here (not to the logger), so every
// handler-output assertion reads from this buffer. It lives in a plain _test.go
// file (no integration tag) so both the unit and integration-tagged tests in
// this package share one helper.
func newFunctionOutputSink(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := SetFunctionOutput(buf)
	t.Cleanup(func() { SetFunctionOutput(prev) })
	return buf
}

// awaitFunctionOutput polls the function-output sink until every wanted
// substring is present and returns the full buffer contents. Handler output is
// forwarded to the sink asynchronously: it is usually complete when Execute
// returns, but under load the lines can trail the invocation response, so an
// immediate read is not a reliable assertion basis.
func awaitFunctionOutput(t *testing.T, ctx context.Context, out *bytes.Buffer, wants ...string) string {
	t.Helper()
	if !pollUntil(ctx, 10*time.Second, func() bool {
		logs := out.String()
		for _, want := range wants {
			if !strings.Contains(logs, want) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("function output missing %v within 10s; got:\n%s", wants, out.String())
	}
	return out.String()
}

// runForwarders drives stdcopy.StdCopy with the given forwarded stdout/stderr
// forwarders and source frames, flushes both forwarders after EOF, and waits for
// completion — the exact lifecycle runContainer gives the reader goroutine.
func runForwarders(t *testing.T, out, errW *streamForwarder, frames []byte) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = stdcopy.StdCopy(out, errW, bytes.NewReader(frames))
		out.flush()
		errW.flush()
	}()
	<-done
}

func TestForwardStdout(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	runForwarders(t, out, errW, bytes.Join([][]byte{
		frame(1, "line one\n"),
		frame(1, "line two\n"),
	}, nil))
	if got, want := sink.String(), "[fn/h] stdout: line one\n[fn/h] stdout: line two\n"; got != want {
		t.Fatalf("stdout forwarding = %q, want %q", got, want)
	}
}

func TestForwardStderr(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	runForwarders(t, out, errW, bytes.Join([][]byte{
		frame(2, "bad thing\n"),
	}, nil))
	if got, want := sink.String(), "[fn/h] stderr: bad thing\n"; got != want {
		t.Fatalf("stderr forwarding = %q, want %q", got, want)
	}
}

func TestForwardInterleaved(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	// Alternate stdout/stderr; per-stream frame order must be preserved even
	// though complete lines may interleave between the two streams (allowed).
	runForwarders(t, out, errW, bytes.Join([][]byte{
		frame(1, "out1\n"),
		frame(2, "err1\n"),
		frame(1, "out2\n"),
		frame(2, "err2\n"),
	}, nil))
	got := sink.String()
	// Both streams present.
	if !strings.Contains(got, "stdout: out1") || !strings.Contains(got, "stderr: err1") {
		t.Fatalf("interleaving missing a stream, got: %q", got)
	}
	// Per-stream order, ignoring the other stream's interleaved lines.
	if order := extractPrefixed(t, got, "stdout"); !matches(order, []string{"out1", "out2"}) {
		t.Errorf("stdout order lost in interleaving, got %v (full %q)", order, got)
	}
	if order := extractPrefixed(t, got, "stderr"); !matches(order, []string{"err1", "err2"}) {
		t.Errorf("stderr order lost in interleaving, got %v (full %q)", order, got)
	}
}

// extractPrefixed returns the sequence of line contents for a given stream
// (e.g. "stdout") in the order they appear in the sink, ignoring other streams.
func extractPrefixed(t *testing.T, out, stream string) []string {
	t.Helper()
	var seq []string
	for _, line := range strings.Split(out, "\n") {
		marker := stream + ": "
		if i := strings.Index(line, marker); i >= 0 {
			seq = append(seq, line[i+len(marker):])
		}
	}
	return seq
}

// matches reports whether got begins with want in the given order.
func matches(got, want []string) bool {
	if len(got) < len(want) {
		return false
	}
	for i, w := range want {
		if got[i] != w {
			return false
		}
	}
	return true
}

func TestForwardMultiLineSingleFrame(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	runForwarders(t, out, errW, frame(1, "a\nb\nc\n"))
	if got, want := sink.String(), "[fn/h] stdout: a\n[fn/h] stdout: b\n[fn/h] stdout: c\n"; got != want {
		t.Fatalf("multi-line frame = %q, want %q", got, want)
	}
}

func TestForwardTrailingPartialLine(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	// No trailing newline: the flush at EOF must emit it as a single line
	// without appending a newline (the content is partial).
	runForwarders(t, out, errW, frame(1, "partial tail"))
	if got, want := sink.String(), "[fn/h] stdout: partial tail"; got != want {
		t.Fatalf("trailing partial = %q, want %q", got, want)
	}
}

func TestForwardOverCapPartialLine(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	// A single line larger than maxPending with no newline must be emitted as a
	// partial line (truncated conceptually by flushing) rather than held
	// unboundedly; after flush nothing extra remains.
	line := strings.Repeat("x", maxPending+4096)
	runForwarders(t, out, errW, frame(1, line))
	got := sink.String()
	if !strings.Contains(got, "[fn/h] stdout: "+strings.Repeat("x", maxPending)) {
		t.Errorf("over-cap line not flushed, got %d bytes", len(got))
	}
	// Nothing emitted after flush: exactly one partial line.
	if strings.Count(got, "[fn/h] stdout: ") != 1 {
		t.Errorf("expected exactly one partial line, got %q", got)
	}
}

func TestForwardEmptyStream(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	runForwarders(t, out, errW, nil)
	if got := sink.String(); got != "" {
		t.Fatalf("empty stream must emit nothing, got %q", got)
	}
}

func TestForwardBlankLinesPreserved(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	runForwarders(t, out, errW, frame(1, "a\n\nb\n"))
	if got, want := sink.String(), "[fn/h] stdout: a\n[fn/h] stdout: \n[fn/h] stdout: b\n"; got != want {
		t.Fatalf("blank lines = %q, want %q", got, want)
	}
}

func TestForwardTrimsCR(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	runForwarders(t, out, errW, frame(1, "line\r\nnext\r\n"))
	if got, want := sink.String(), "[fn/h] stdout: line\n[fn/h] stdout: next\n"; got != want {
		t.Fatalf("CRLF trimming = %q, want %q", got, want)
	}
}

// stripForwardingPrefix removes the per-line "[<fn>/<handler>...] <stream>: "
// forwarding prefix from each line of forwarded output, then joins the lines
// with '\n'. It reconstructs the raw payload a multi-line handler write is split
// from, so an assertion can check the value's exact bytes independent of the
// incidental prefix wording.
func stripForwardingPrefix(forwarded string) string {
	lines := strings.Split(forwarded, "\n")
	for i, line := range lines {
		if j := strings.Index(line, "] stdout: "); j >= 0 {
			lines[i] = line[j+len("] stdout: "):]
			continue
		}
		if j := strings.Index(line, "] stderr: "); j >= 0 {
			lines[i] = line[j+len("] stderr: "):]
		}
	}
	return strings.Join(lines, "\n")
}

// lineHasAll reports whether any single newline-separated line of forwarded
// output contains every one of the given substrings. Integration tests use it to
// assert that a payload and its attribution (handler/message id/stream) land on
// the SAME forwarded line without pinning the exact prefix formatting.
func lineHasAll(forwarded string, subs ...string) bool {
	for _, line := range strings.Split(forwarded, "\n") {
		all := true
		for _, sub := range subs {
			if !strings.Contains(line, sub) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

func TestForwardPrefixIncludesMessageID(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{MessageID: "1791234567890-0", EventID: "evt_x"})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{MessageID: "1791234567890-0", EventID: "evt_x"})
	runForwarders(t, out, errW, frame(1, "hi\n"))
	// MessageID is preferred over EventID, so the prefix carries @1791234567890-0.
	if got, want := sink.String(), "[fn/h@1791234567890-0] stdout: hi\n"; got != want {
		t.Fatalf("message-id prefix = %q, want %q", got, want)
	}
}

func TestForwardPrefixUsesEventIDWhenNoMessageID(t *testing.T) {
	sink := newFunctionOutputSink(t)
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{EventID: "evt_777"})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{EventID: "evt_777"})
	runForwarders(t, out, errW, frame(1, "hi\n"))
	if got, want := sink.String(), "[fn/h@evt_777] stdout: hi\n"; got != want {
		t.Fatalf("event-id prefix = %q, want %q", got, want)
	}
}

func TestForwardPrefersMetaFunctionHandler(t *testing.T) {
	sink := newFunctionOutputSink(t)
	// Direct params say fn/h but meta (authoritative when set) says meta-fn/meta-h.
	out := newStreamForwarder("stdout", "fn", "h", RunMeta{Function: "meta-fn", Handler: "meta-h"})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{Function: "meta-fn", Handler: "meta-h"})
	runForwarders(t, out, errW, frame(1, "hi\n"))
	if got, want := sink.String(), "[meta-fn/meta-h] stdout: hi\n"; got != want {
		t.Fatalf("meta function/handler prefix = %q, want %q", got, want)
	}
}

// panicWriter is an io.Writer whose Write always panics, used to prove a broken
// sink never breaks forwarding (a transport swallows it) and, in the
// integration suite, never breaks an invocation. It lives in a plain _test.go
// file so both suites share one definition.
type panicWriter struct{}

func (panicWriter) Write([]byte) (int, error) { panic("sink exploded") }

func TestForwardPanickingSinkDoesNotPanic(t *testing.T) {
	prev := SetFunctionOutput(panicWriter{})
	defer SetFunctionOutput(prev)

	out := newStreamForwarder("stdout", "fn", "h", RunMeta{})
	errW := newStreamForwarder("stderr", "fn", "h", RunMeta{})
	// Must complete without a panic propagating to the test.
	runForwarders(t, out, errW, bytes.Join([][]byte{
		frame(1, "boom\n"),
		frame(2, "err\n"),
	}, nil))
}

func TestSetFunctionOutputNilFallsBack(t *testing.T) {
	// SetFunctionOutput(nil) must fall back to os.Stdout without panicking; the
	// os.Stdout identity can't be asserted, so this only proves the call returns
	// the previous sink and does not crash.
	prev := SetFunctionOutput(&bytes.Buffer{})
	restored := SetFunctionOutput(nil)
	if restored == nil {
		t.Fatal("SetFunctionOutput(nil) returned nil previous sink")
	}
	SetFunctionOutput(prev)
}

func TestWriteFunctionOutputNilWriterNoPanic(t *testing.T) {
	// A nil sink must fall back to os.Stdout: the write succeeds, does not panic,
	// and reports the full length.
	prev := SetFunctionOutput(nil)
	defer SetFunctionOutput(prev)
	if n, err := writeFunctionOutput([]byte("x")); err != nil || n != 1 {
		t.Fatalf("writeFunctionOutput nil sink: n=%d err=%v", n, err)
	}
}
