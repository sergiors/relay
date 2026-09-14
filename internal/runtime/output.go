package runtime

import (
	"bytes"
	"io"
	"os"
	"sync"
)

// Function output forwarding: container stdout/stderr is forwarded to the Relay
// process output. Forwarding is a raw transport, deliberately NOT routed
// through slog — it must stream live while the container runs (not buffered
// until exit), must work regardless of Relay's LOG_LEVEL, and must NOT be
// inferred into log severity levels. Relay acts as a passthrough; the exact
// bytes the function writes (Python print/logging, Node console.log/console.error,
// anything) arrive verbatim, line by line, on the process output.

// outputMu guards functionOut; SetFunctionOutput and writeFunctionOutput both
// take it so the sink can be redirected from tests without racing in-flight
// forwarders from other goroutines.
var (
	outputMu    sync.Mutex
	functionOut io.Writer = os.Stdout
)

// maxPending bounds the per-stream line buffer. A container line longer than
// this (no '\n' within the cap) is flushed as an incomplete line rather than
// growing the buffer unboundedly, bounding per-invocation memory.
const maxPending = 4 << 10 // 4 KiB

// SetFunctionOutput redirects function container output to w. It returns the
// previous sink so callers can restore it (used by tests). A nil w falls back
// to os.Stdout.
//
// Sink contract: every forwarded line is written to w synchronously from the
// container's output reader goroutine. A sink whose Write blocks will
// back-pressure the container's output streaming and delay the reader's EOF
// join (and hence the invocation's return). The default sink (os.Stdout) and
// ordinary in-memory test buffers are non-blocking. A slow destination (a
// network writer, a bounded channel) must wrap itself with its own buffering or
// async flush before calling SetFunctionOutput.
func SetFunctionOutput(w io.Writer) io.Writer {
	outputMu.Lock()
	defer outputMu.Unlock()
	prev := functionOut
	if w == nil {
		w = os.Stdout
	}
	functionOut = w
	return prev
}

// writeFunctionOutput copies p to the current function-output sink, serializing
// on outputMu because multiple in-flight invocations may forward concurrently.
// It is a best-effort raw transport: both write errors and sink panics are
// swallowed (recovered) so a broken or panicking sink can never fail an
// invocation or break the container lifecycle. It always returns a nil error:
// stdcopy aborts its whole copy on a writer error, which would break the reader
// goroutine and its cleanup joins.
func writeFunctionOutput(p []byte) (int, error) {
	outputMu.Lock()
	defer outputMu.Unlock()
	defer func() { _ = recover() }()

	sink := functionOut
	if sink == nil {
		sink = os.Stdout
	}
	n, _ := sink.Write(p)
	return n, nil
}

// streamForwarder demultiplexes one container stream's raw bytes into completed
// lines, prefixing each with a static [...] context and emitting it through
// the function-output sink in a single write. One forwarder is used per stream
// (stdout and stderr) so interleaving between the two never merges partial
// lines. It buffers at most maxPending bytes of an incomplete trailing line.
type streamForwarder struct {
	// mu guards pending and is held for the whole Write/flush so the buffer
	// stays coherent even if a caller drives the forwarder concurrently.
	mu      sync.Mutex
	prefix  []byte // static per-line prefix, e.g. "[fn/handler@id] stdout: "
	pending []byte // incomplete trailing line awaiting a '\n'
}

// newStreamForwarder returns a streamForwarder for stream ("stdout"/"stderr").
// The function/handler are taken from meta when non-empty, else from the direct
// fn/handler params — Execute always sets both identically, so they agree; the
// override just keeps direct callers that pass a meta working. A non-empty
// message or event id from meta is appended to the prefix.
func newStreamForwarder(stream, fn, handler string, meta RunMeta) *streamForwarder {
	if meta.Function != "" {
		fn = meta.Function
	}
	if meta.Handler != "" {
		handler = meta.Handler
	}
	return &streamForwarder{prefix: outputPrefix(fn, handler, stream, meta)}
}

// outputPrefix builds the static line prefix for one stream:
//
//	[<function>/<handler>[@<message_id|event_id>]] <stream>:
//
// The function/handler come from the direct params (always set by Execute). A
// non-empty message or event id on the metadata is appended after '@' when
// present (message id preferred over event id); otherwise the bracket holds just
// function/handler. The prefix is computed once and reused for every line.
func outputPrefix(fn, handler, stream string, meta RunMeta) []byte {
	prefix := make([]byte, 0, 32)
	prefix = append(prefix, "["...)
	prefix = append(prefix, fn...)
	prefix = append(prefix, '/')
	prefix = append(prefix, handler...)
	if id := meta.MessageID; id != "" {
		prefix = append(prefix, '@')
		prefix = append(prefix, id...)
	} else if id := meta.EventID; id != "" {
		prefix = append(prefix, '@')
		prefix = append(prefix, id...)
	}
	prefix = append(prefix, ']', ' ')
	prefix = append(prefix, stream...)
	prefix = append(prefix, ':')
	prefix = append(prefix, ' ')
	return prefix
}

// Write implements io.Writer for stdcopy. It splits p on '\n', emitting each
// complete line immediately (streaming live) and keeping any trailing partial
// line buffered. If the buffered partial exceeds maxPending without a newline,
// it is flushed as-is (truncated conceptually) so memory stays bounded. It
// always returns len(p) with a nil error so stdcopy never aborts on forwarding.
func (f *streamForwarder) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pending = append(f.pending, p...)
	for {
		i := bytes.IndexByte(f.pending, '\n')
		if i < 0 {
			break
		}
		line := f.pending[:i]
		f.pending = f.pending[i+1:]
		f.emit(line, true)
	}
	if len(f.pending) > maxPending {
		f.emit(f.pending, false)
		f.pending = f.pending[:0]
	}
	return len(p), nil
}

// flush emits any buffered trailing partial line (with no trailing newline, as
// it is incomplete) and clears the buffer. It is called when the stream reaches
// EOF — after stdcopy returns, before readerDone closes — so a trailing partial
// line is still delivered and every consumer's buffer is drained.
func (f *streamForwarder) flush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) > 0 {
		f.emit(f.pending, false)
		f.pending = f.pending[:0]
	}
}

// emit writes prefix + content (+ optional trailing newline) to the
// function-output sink. A trailing '\r' (CRLF) on the content is trimmed. The
// write is best-effort and panic-safe via writeFunctionOutput.
func (f *streamForwarder) emit(content []byte, newline bool) {
	if len(content) > 0 && content[len(content)-1] == '\r' {
		content = content[:len(content)-1]
	}
	buf := make([]byte, 0, len(f.prefix)+len(content)+1)
	buf = append(buf, f.prefix...)
	buf = append(buf, content...)
	if newline {
		buf = append(buf, '\n')
	}
	_, _ = writeFunctionOutput(buf)
}
