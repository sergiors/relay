package runtime

import (
	"encoding/json"
	"sync"
)

// The persistent invocation protocol. Execution containers are
// REUSED across invocations for the same function: each pooled container (up to
// the function's concurrency, per image version) stays alive as a long-running
// bootstrap process and is leased to one invocation at a time, and each
// invocation is one request/response frame exchange over that container's stdin
// and stdout. The bootstrap is a line-JSON server side:
//
//	Relay → stdin:   {"id":"<8-16 hex>","handler":"mod.func","event":<raw>,"env":{"K":"V"}}
//	stdin → stdout:  @@RELAY@@{"id":"<id>","ok":true}            (line)
//	                 @@RELAY@@{"id":"<id>","ok":false,"error":"…"}
//
// stdout lines that do NOT start with the sentinel are user output and are
// forwarded to the function-output sink exactly as before (a raw transport);
// sentinel lines that do not parse as a response frame are likewise treated as
// user output (a program that happens to print the sentinel is not a framing
// failure, but a PARSEABLE response carrying an unexpected id is a protocol
// error and discards the container).
//
// Error strings in the response are operator-facing log text only (truncated
// bootstrap-side to a bounded size); handler failures keep the container, so
// the protocol never turns a user error into an infrastructure failure.

// relayProtocolSentinel prefixes every bootstrap protocol response line on
// stdout. It must stay in sync with the bootstraps in python/ and node/ (the
// interpreter tests hardcode it; a shared Go constant cannot be imported there
// without an import cycle through internal/runtime).
const relayProtocolSentinel = "@@RELAY@@"

// maxResponseFrame is the size bound every response frame is guaranteed to
// respect. The bootstraps truncate their error strings to maxResponseError
// (3 KiB) so a frame always stays below the demuxer's maxPending (4 KiB) line
// cap: an oversized line would be split off as user output mid-frame and the
// response would be destroyed. Keeping frame < maxPending is therefore a
// protocol invariant, not an optimization.
const (
	maxResponseFrame = 3 << 10 // 3 KiB — bootstrap-side truncation cap
	maxResponseError = maxResponseFrame - 256
)

// invokeRequest is one request frame Relay writes to the container's stdin as a
// single JSON line. Event is the raw event JSON passed through verbatim
// (json.RawMessage), so Relay never re-encodes or mutates the payload.
type invokeRequest struct {
	ID      string            `json:"id"`
	Handler string            `json:"handler"`
	Event   json.RawMessage   `json:"event"`
	Env     map[string]string `json:"env"`
}

// invokeResponse is one response frame line the bootstrap writes on stdout.
type invokeResponse struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// clampResponseError bounds a response error string to maxResponseError so the
// whole frame stays below the demuxer's 4 KiB line cap (see maxResponseFrame).
// The Go side truncates what the bootstraps already bound: belt and braces for
// hand-written or future bootstraps.
func clampResponseError(s string) string {
	if len(s) > maxResponseError {
		return s[:maxResponseError]
	}
	return s
}

// protocolDemuxer is the stdout demultiplexer for a reused execution container:
// it splits the container's stdout into lines, forwards user output to the
// stdout stream forwarder (wrapped by output.go, which prefixes it), and routes
// sentinel-prefixed response frames to the pending invocation. It implements
// io.Writer feeding stdcopy.StdCopy from the container's long-lived reader
// goroutine. All state is mutex-guarded because the swap in/out of the
// per-invocation forwarder happens on the Invoke goroutine while the reader
// goroutine writes lines.
//
// A line starting with the sentinel is a protocol CANDIDATE, not an automatic
// protocol frame: it must parse as JSON response AND match the pending
// invocation's registered id to be delivered. An unparseable sentinel line is
// forwarded as user output (it cannot be a response — reporting it as a failure
// would let handler noise break invocations). A parseable response that does
// not match the registered id is a protocol violation: it triggers protoFail
// (the container discard path) instead of being silently dropped, so
// multiplexing can never silently mis-route responses.
//
// The line buffer is bounded exactly like streamForwarder's (maxPending): a
// line longer than the cap is flushed as user output, bounding memory; a
// trailing partial line at EOF is flushed too.
type protocolDemuxer struct {
	fn string // function name for the idle fallback prefix

	// mu guards everything below. The reader goroutine (Write/processLine) and
	// the Invoke goroutine (setPending/clearPending/begin/end) both take it;
	// contention is one swap per invocation plus one lock per stdout chunk.
	mu sync.Mutex

	// pendingID/pendingCh register the invocation currently awaiting a
	// response. Registered by Invoke just before the request is written and
	// cleared when it returns; the single-reader goroutine is the only sender.
	pendingID string
	pendingCh chan invokeResponse
	hasPend   bool

	// stdoutFwd/stderrFwd are the CURRENT forwarders: swapped to
	// per-invocation prefixed instances (begin) while an Invoke is in flight
	// and back to the idle fallback (end) after. One forwarder pair exists per
	// stream; swapping whole instances is how the per-invocation prefix
	// (function/handler[@id]) is applied without mutating shared state.
	stdoutFwd *streamForwarder
	stderrFwd *streamForwarder

	// idleStdout/idleStderr are the STABLE fallback forwarders used while no
	// invocation is in flight (asynchronous user output, e.g. daemon threads).
	// They are created once: a fresh forwarder per write would drop the
	// buffered partial line of a stderr line split across stdcopy chunks.
	idleStdout *streamForwarder
	idleStderr *streamForwarder

	// pendingBuf holds an incomplete trailing line between Write calls.
	pendingBuf []byte

	// protoFail is invoked on the reader goroutine when a parseable response
	// arrives with no registered (or with a mismatched) id. Nil in tests.
	protoFail func()
}

// newProtocolDemuxer returns a demuxer whose idle forwarding falls back to
// stable "[<fn>/] stdout/stderr: " prefixed forwarders.
func newProtocolDemuxer(fn string) *protocolDemuxer {
	idle := RunMeta{Function: fn}
	return &protocolDemuxer{
		fn:         fn,
		idleStdout: newStreamForwarder("stdout", fn, "", idle),
		idleStderr: newStreamForwarder("stderr", fn, "", idle),
	}
}

// begin swaps in per-invocation forwarders so forwarded lines carry the
// [function/handler[@id]] prefix for this invocation.
func (d *protocolDemuxer) begin(meta RunMeta) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stdoutFwd = newStreamForwarder("stdout", d.fn, meta.Handler, meta)
	d.stderrFwd = newStreamForwarder("stderr", d.fn, meta.Handler, meta)
}

// end removes the per-invocation forwarders, falling back to the stable idle
// "[function/] stream:" prefixes for asynchronous output between invocations.
// The outgoing forwarders are flushed first so any buffered partial line they
// hold (a line split across stdcopy chunks) is delivered under their own
// prefix rather than leaking into the next prefix.
func (d *protocolDemuxer) end() {
	d.mu.Lock()
	stdout, stderr := d.stdoutFwd, d.stderrFwd
	d.stdoutFwd, d.stderrFwd = nil, nil
	d.mu.Unlock()
	if stdout != nil {
		stdout.flush()
	}
	if stderr != nil {
		stderr.flush()
	}
}

// setPending registers the pending invocation (id → response channel). It must
// be called before the request line is written.
func (d *protocolDemuxer) setPending(id string, ch chan invokeResponse) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pendingID = id
	d.pendingCh = ch
	d.hasPend = true
}

// clearPending unregisters the pending invocation. Buffered undelivered
// responses are dropped: after a timeout the container is discarded anyway.
func (d *protocolDemuxer) clearPending() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pendingID = ""
	d.pendingCh = nil
	d.hasPend = false
}

// curForwarders returns the forwarder pair for user output, falling back to
// the stable idle forwarders (create-time identity prefix with an empty
// handler) when no invocation is in flight. Called with d.mu held.
func (d *protocolDemuxer) curForwardersW() (stdout, stderr *streamForwarder) {
	if d.stdoutFwd != nil {
		return d.stdoutFwd, d.stderrFwd
	}
	return d.idleStdout, d.idleStderr
}

// stderrSink adapts the demuxer's stderr forwarder into the io.Writer
// stdcopy.StdCopy wants for the stderr stream: the demuxer holds the CURRENT
// stderr forwarder and delegates writes to it.
type stderrSink struct{ d *protocolDemuxer }

// Write delegates to the current (or idle-fallback) stderr forwarder. stderr
// is pure user forwarding — no protocol lines ever appear on it.
func (s *stderrSink) Write(p []byte) (int, error) {
	s.d.mu.Lock()
	defer s.d.mu.Unlock()
	_, fwd := s.d.curForwardersW()
	_, _ = fwd.Write(p)
	return len(p), nil
}

// Write implements io.Writer for stdcopy: it splits p into lines and routes
// each. It always consumes the whole buffer (nil error) so stdcopy never
// aborts on forwarding.
func (d *protocolDemuxer) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.pendingBuf = append(d.pendingBuf, p...)
	for {
		i := indexOfByte(d.pendingBuf, '\n')
		if i < 0 {
			break
		}
		line := d.pendingBuf[:i]
		d.pendingBuf = d.pendingBuf[i+1:]
		d.route(append([]byte(nil), line...))
	}
	if len(d.pendingBuf) > maxPending {
		// Line longer than the cap: flush it as user output so memory stays
		// bounded (the remainder of the line continues streaming below).
		d.fwdUser(d.pendingBuf)
		d.pendingBuf = d.pendingBuf[:0]
	}
	return len(p), nil
}

// flushPending delivers any buffered trailing partial line as user output. It
// is called when the stdout stream reaches EOF.
func (d *protocolDemuxer) flushPending() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pendingBuf) > 0 {
		d.fwdUser(d.pendingBuf)
		d.pendingBuf = d.pendingBuf[:0]
	}
}

// route classifies one complete stdout line. Called with d.mu held; line must
// not be aliased.
func (d *protocolDemuxer) route(line []byte) {
	if !hasPrefix(line, relayProtocolSentinel) {
		d.fwdUser(line)
		return
	}
	payload := line[len(relayProtocolSentinel):]
	var resp invokeResponse
	if err := unmarshalLine(payload, &resp); err != nil {
		// A sentinel-looking line that does not parse is user output — do NOT
		// fail on it.
		d.fwdUser(line)
		return
	}
	// Parseable response: it must match the registered pending id.
	if !d.hasPend || resp.ID != d.pendingID {
		d.protocolError()
		return
	}
	ch := d.pendingCh
	// Non-blocking send: the channel is buffered (cap 1) and the pending
	// Invoke is its sole receiver. A second frame with the same id before the
	// Invoke consumed the first means the bootstrap sent two responses for one
	// request — a protocol violation (never silently multiplexed): drop the
	// frame and signal the discard path instead of deadlocking the sole reader
	// goroutine on a blocking send under d.mu.
	select {
	case ch <- resp:
	default:
		d.protocolError()
	}
}

// protocolError reports an unexpected protocol frame (a parseable response
// with no matching pending invocation). The callback (wired by
// executionContainer) discards the container; nothing is silently allowed.
func (d *protocolDemuxer) protocolError() {
	if d.protoFail != nil {
		d.protoFail()
	}
}

// fwdUser forwards a raw line (without the trailing newline) to the current
// stdout forwarder, falling back to the idle forwarder when no invocation is
// in flight. The swap is done under d.mu; a user print landing exactly across
// the swap boundary may be attributed to the adjacent prefix — acceptable
// diagnostic imprecision (see the comment on swap-attribution in
// execution_container.go).
func (d *protocolDemuxer) fwdUser(line []byte) {
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	fwd, _ := d.curForwardersW()
	fwd.emit(line, true)
}

// flushForwarders flushes the trailing partial buffers of both current stream
// forwarders. It is called when the container's streams reach EOF, after
// flushPending delivered the stdout remainder.
func (d *protocolDemuxer) flushForwarders() {
	d.mu.Lock()
	stdout, stderr := d.curForwardersW()
	d.mu.Unlock()
	stdout.flush()
	stderr.flush()
}

// hasPendingIdle reports whether an invocation currently has a pending
// response registered. It is read by the exit monitor to decide whether a
// process-death event belongs to the invocation or to the idle container.
func (d *protocolDemuxer) pending() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hasPend
}

// indexOfByte is bytes.IndexByte for a single byte without importing bytes at
// call sites (the demuxer only ever splits on '\n').
func indexOfByte(b []byte, c byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == c {
			return i
		}
	}
	return -1
}

func hasPrefix(b []byte, s string) bool {
	return len(b) >= len(s) && string(b[:len(s)]) == s
}

func unmarshalLine(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
