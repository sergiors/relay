package runtime

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// testResponse builds one wire format response line (no newline).
func testResponse(id string, ok bool, errMsg string) string {
	b, _ := json.Marshal(invokeResponse{ID: id, OK: ok, Error: errMsg})
	return relayProtocolSentinel + string(b)
}

func newTestDemuxer(t *testing.T, fn string) (*protocolDemuxer, *[]string, func()) {
	t.Helper()
	var lines []string
	// Install a recording sink wrapper: every forwarded line (with its
	// streamForwarder prefix) is recorded, newline-split.
	rec := &recordingSink{lines: &lines}
	prev := SetFunctionOutput(rec)
	t.Cleanup(func() { SetFunctionOutput(prev) })
	d := newProtocolDemuxer(fn)
	return d, &lines, func() { rec.mu.Lock(); defer rec.mu.Unlock() }
}

type recordingSink struct {
	mu    sync.Mutex
	lines *[]string
	buf   []byte
}

func (r *recordingSink) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	for {
		i := strings.IndexByte(string(r.buf), '\n')
		if i < 0 {
			break
		}
		line := append([]byte(nil), r.buf[:i]...)
		*r.lines = append(*r.lines, string(line))
		r.buf = r.buf[i+1:]
	}
	return len(p), nil
}

func TestDemuxerForwardsUserLines(t *testing.T) {
	d, lines, restore := newTestDemuxer(t, "fn")
	defer restore()
	d.begin(RunMeta{Function: "fn", Handler: "mod.h", MessageID: "m1"})
	if _, err := d.Write([]byte("hello\nworld\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := strings.Join(*lines, "|"); got != "[fn/mod.h@m1] stdout: hello|[fn/mod.h@m1] stdout: world" {
		t.Errorf("forwarded = %q, want per-invocation prefixed lines", got)
	}
	d.end()
	// After end() the fallback (idle) prefix applies to later output.
	if _, err := d.Write([]byte("library print\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, want := (*lines)[2], "[fn/] stdout: library print"; got != want {
		t.Errorf("idle forward = %q, want %q", got, want)
	}
}

func TestDemuxerDeliversMatchingResponse(t *testing.T) {
	d, _, restore := newTestDemuxer(t, "fn")
	defer restore()
	ch := make(chan invokeResponse, 1)
	d.setPending("abc", ch)
	if _, err := d.Write([]byte(testResponse("abc", true, "") + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case resp := <-ch:
		if !resp.OK || resp.ID != "abc" {
			t.Fatalf("delivered %+v, want ok for id abc", resp)
		}
	default:
		t.Fatal("no response delivered")
	}
}

func TestDemuxerWrongIDIsProtocolError(t *testing.T) {
	d, _, restore := newTestDemuxer(t, "fn")
	defer restore()
	var fired int
	d.protoFail = func() { fired++ }

	// A parseable response with a different (or absent) pending id.
	d.setPending("registered", make(chan invokeResponse, 1))
	if _, err := d.Write([]byte(testResponse("other", true, "") + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if fired != 1 {
		t.Fatalf("mismatched id must trigger the protocol-error path, fired=%d", fired)
	}

	// A late response when NOTHING is pending: still a protocol error, never
	// silently multiplexed.
	d.clearPending()
	fired = 0
	if _, err := d.Write([]byte(testResponse("whatever", true, "") + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if fired != 1 {
		t.Fatalf("unexpected response when idle must be a protocol error, fired=%d", fired)
	}
}

// TestDemuxerDoubleResponseIsProtocolError verifies the duplicate-response
// guard: a second frame for the SAME pending id before the Invoke consumed the
// first must be dropped with a protocol error, never block the sole reader
// goroutine on a buffered channel that is already full.
func TestDemuxerDoubleResponseIsProtocolError(t *testing.T) {
	d, _, restore := newTestDemuxer(t, "fn")
	defer restore()
	fired := make(chan struct{}, 8)
	d.protoFail = func() { fired <- struct{}{} }

	ch := make(chan invokeResponse, 1)
	d.setPending("dup", ch)
	line := testResponse("dup", true, "") + "\n"
	if _, err := d.Write([]byte(line + line)); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case resp := <-ch:
		if !resp.OK {
			t.Fatalf("first frame = %+v, want ok", resp)
		}
	default:
		t.Fatal("the first response must be delivered")
	}
	select {
	case <-fired:
	default:
		t.Fatal("a duplicate response must trigger the protocol-error path")
	}
	// The demuxer must still accept writes (the reader goroutine never
	// deadlocked behind the full channel).
	if _, err := d.Write([]byte("still alive\n")); err != nil {
		t.Fatalf("write after protocol error: %v", err)
	}
}

func TestDemuxerUnparseableSentinelIsUserOutput(t *testing.T) { //nolint
	d, lines, restore := newTestDemuxer(t, "fn")
	defer restore()
	failed := false
	d.protoFail = func() { failed = true }
	userPrint := relayProtocolSentinel + "definitely not a response"
	if _, err := d.Write([]byte(userPrint + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if failed {
		t.Error("an unparseable sentinel line must be forwarded as user output, not a protocol error")
	}
	if got, want := (*lines)[0], "[fn/] stdout: "+userPrint; got != want {
		t.Errorf("forwarded = %q, want %q", got, want)
	}
}

func TestDemuxerPartialLineAcrossWrites(t *testing.T) {
	d, lines, restore := newTestDemuxer(t, "fn")
	defer restore()
	if _, err := d.Write([]byte("hel")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(*lines) != 0 {
		t.Errorf("a partial line must not be forwarded yet, got %v", *lines)
	}
	if _, err := d.Write([]byte("lo\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, want := (*lines)[0], "[fn/] stdout: hello"; got != want {
		t.Errorf("forwarded = %q, want %q", got, want)
	}
	// A response split across writes is still delivered.
	ch := make(chan invokeResponse, 1)
	d.setPending("split", ch)
	full := testResponse("split", false, "boom") + "\n"
	if _, err := d.Write([]byte(full[:len(full)/2])); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := d.Write([]byte(full[len(full)/2:])); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case resp := <-ch:
		if resp.OK || resp.Error != "boom" {
			t.Fatalf("delivered %+v, want ok:false boom", resp)
		}
	default:
		t.Fatal("split response not delivered")
	}
}

func TestDemuxerBoundsLongLines(t *testing.T) {
	d, lines, restore := newTestDemuxer(t, "fn")
	defer restore()
	long := strings.Repeat("x", maxPending+100)
	if _, err := d.Write([]byte(long + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The oversized line was flushed (as user output) once the buffer crossed
	// the cap; the remainder continued to stream.
	found := false
	for _, l := range *lines {
		if strings.Contains(l, "xxxxx") {
			found = true
		}
	}
	if !found {
		t.Errorf("oversized line must be flushed as bounded user output, got %v", *lines)
	}
}

func TestDemuxerFlushesTrailingPartialAtEOF(t *testing.T) {
	d, lines, restore := newTestDemuxer(t, "fn")
	defer restore()
	if _, err := d.Write([]byte("no newline at end")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(*lines) != 0 {
		t.Fatalf("trailing partial must not be forwarded before EOF flush, got %v", *lines)
	}
	d.flushPending()
	if got, want := (*lines)[0], "[fn/] stdout: no newline at end"; got != want {
		t.Errorf("flushed = %q, want %q", got, want)
	}
}

func TestDemuxerStderrForwarding(t *testing.T) {
	d, lines, restore := newTestDemuxer(t, "fn")
	defer restore()
	d.begin(RunMeta{Function: "fn", Handler: "mod.h", EventID: "evt"})
	sink := stderrSink{d: d}
	if _, err := sink.Write([]byte("bad\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, want := (*lines)[0], "[fn/mod.h@evt] stderr: bad"; got != want {
		t.Errorf("stderr forwarded = %q, want %q", got, want)
	}
}
