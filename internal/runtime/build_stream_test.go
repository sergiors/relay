package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/jsonstream"

	"relay/internal/observability/metrics"
)

// drainResponse builds a Docker build-response body from stream chunks and an
// optional trailing error message. Chunks are emitted as separate JSON message
// objects so a test can exercise the message-boundary handling (the retention cap
// applies across messages).
func drainResponse(streams []string, errMsg string) string {
	var b strings.Builder
	for _, s := range streams {
		msg, _ := json.Marshal(map[string]string{"stream": s})
		b.Write(msg)
		b.WriteByte('\n')
	}
	if errMsg != "" {
		msg, _ := json.Marshal(map[string]any{"errorDetail": map[string]string{"message": errMsg}})
		b.Write(msg)
		b.WriteByte('\n')
	}
	return b.String()
}

// TestDrainBuildResponseNormalOutput pins ordinary behavior: all stream text is
// retained, with no error and no truncation.
func TestDrainBuildResponseNormalOutput(t *testing.T) {
	out, truncated, err := drainBuildResponse(strings.NewReader(drainResponse([]string{"step 1\n", "step 2\n"}, "")))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if truncated {
		t.Fatal("normal output must not report truncation")
	}
	if out != "step 1\nstep 2\n" {
		t.Fatalf("retained output = %q", out)
	}
}

// TestDrainBuildResponseSurfacesFirstErrorAndRetainsOutput pins that the first
// Docker error is returned with the output retained, and that the returned
// diagnostic is not truncated for an ordinary-sized stream.
func TestDrainBuildResponseSurfacesFirstErrorAndRetainsOutput(t *testing.T) {
	out, truncated, err := drainBuildResponse(strings.NewReader(drainResponse([]string{"installing...\n", "boom happened\n"}, "the real cause")))
	if err == nil {
		t.Fatal("expected the Docker error to surface")
	}
	if !strings.Contains(err.Error(), "the real cause") {
		t.Fatalf("error = %v, want the first Docker error", err)
	}
	if truncated {
		t.Fatal("a small failure output must not be marked truncated")
	}
	if !strings.Contains(out, "boom happened") {
		t.Fatalf("retained failure output = %q, want the useful diagnostic", out)
	}
}

// TestDrainBuildResponseRetainsAtMostCapAndMarksTruncation pins the bound: a
// stream far larger than the retention cap yields at most cap bytes of output
// (plus the explicit marker), the truncation flag is set, and the ENTIRE reader
// is consumed (nothing is left undrained).
func TestDrainBuildResponseRetainsAtMostCapAndMarksTruncation(t *testing.T) {
	// Three chunks each larger than a third of the cap, so the cap is crossed
	// mid-stream and the rest must be discarded, not retained.
	chunk := strings.Repeat("x", buildOutputRetention/2)
	body := drainResponse([]string{chunk, chunk, chunk, chunk}, "")
	counting := &countingReader{r: strings.NewReader(body)}

	out, truncated, err := drainBuildResponse(counting)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !truncated {
		t.Fatal("output beyond the retention cap must report truncation")
	}
	if len(out) > buildOutputRetention+len(buildOutputTruncatedMarker)+1 {
		t.Fatalf("retained output length %d exceeds cap %d plus marker", len(out), buildOutputRetention)
	}
	if !strings.HasSuffix(out, buildOutputTruncatedMarker) {
		t.Fatalf("retained output must end with the truncation marker, got suffix %q", out[len(out)-40:])
	}
	if counting.remaining() != 0 {
		t.Fatalf("drain left %d response bytes unread; the stream must be fully consumed", counting.remaining())
	}
}

// TestDrainBuildResponseBoundsOversizedDockerError pins that an oversized Docker
// error message is itself capped: the returned diagnostic is at most the
// retention budget plus the fixed marker, the first cause is still surfaced (its
// code and message prefix survive), truncation is marked, and the whole response
// is drained. The payload is generated so the bound is exercised without a large
// literal.
func TestDrainBuildResponseBoundsOversizedDockerError(t *testing.T) {
	payload := strings.Repeat("e", buildOutputRetention+4096)
	// Encode a code alongside the oversized message so the test can prove the
	// first error's code survives the cap. Built inline rather than through the
	// shared drainResponse helper, which carries only a message.
	raw, _ := json.Marshal(map[string]any{"errorDetail": map[string]any{"code": 42, "message": payload}})
	body := string(raw) + "\n"
	counting := &countingReader{r: strings.NewReader(body)}

	out, truncated, err := drainBuildResponse(counting)
	if err == nil {
		t.Fatal("expected the oversized Docker error to surface")
	}
	if !truncated {
		t.Fatal("an oversized Docker error message must report truncation")
	}
	if !strings.Contains(out, buildOutputTruncatedMarker) {
		t.Fatalf("retained diagnostic %q must mark the truncation explicitly", out)
	}
	if got := len(err.Error()) + len(out); got > buildOutputRetention+len(buildOutputTruncatedMarker)+1 {
		t.Fatalf("error diagnostic length %d exceeds cap %d plus marker", got, buildOutputRetention)
	}
	// The first cause is retained and useful: the error still carries the message
	// prefix and the original Docker error code.
	var de *jsonstream.Error
	if !errors.As(err, &de) {
		t.Fatalf("error = %T, want a *jsonstream.Error", err)
	}
	if de.Code != 42 {
		t.Fatalf("error code = %d, want 42 (the first Docker error code)", de.Code)
	}
	if !strings.Contains(err.Error(), strings.Repeat("e", 32)) {
		t.Fatalf("error = %q, want the retained first Docker error message", err.Error())
	}
	// The stream is still fully consumed, cap or not.
	if counting.remaining() != 0 {
		t.Fatalf("drain left %d response bytes unread; the stream must be fully consumed", counting.remaining())
	}
}

// TestDrainBuildResponseBoundsErrorAndStreamTogether pins the combined bound: a
// Docker error message plus stream output that together exceed the retention
// budget are trimmed to the budget as a whole, so the error message reserves its
// share and the returned diagnostic (message plus stream text) never exceeds the
// cap plus marker. The first error survives, truncation is marked, and the rest
// of the stream is still drained.
func TestDrainBuildResponseBoundsErrorAndStreamTogether(t *testing.T) {
	// The error message alone consumes three quarters of the budget, leaving a
	// small stream share; the stream is a full budget on its own, so it must be
	// cut to the remainder.
	errMsg := strings.Repeat("E", (buildOutputRetention*3)/4)
	chunk := strings.Repeat("s", buildOutputRetention)
	body := drainResponse([]string{chunk}, errMsg)
	counting := &countingReader{r: strings.NewReader(body)}

	out, truncated, err := drainBuildResponse(counting)
	if err == nil {
		t.Fatal("expected the Docker error to surface")
	}
	if !truncated {
		t.Fatal("error plus stream over the budget must report truncation")
	}
	budget := buildStreamBudget(errMsg)
	if len(out) > budget+len(buildOutputTruncatedMarker)+1 {
		t.Fatalf("retained stream length %d exceeds remaining budget %d plus marker", len(out), budget)
	}
	combined := len(errMsg) + len(out)
	if combined > buildOutputRetention+len(buildOutputTruncatedMarker)+1 {
		t.Fatalf("combined diagnostic length %d exceeds cap %d plus marker", combined, buildOutputRetention)
	}
	if !strings.HasSuffix(out, buildOutputTruncatedMarker) {
		t.Fatalf("retained stream output must be marked truncated")
	}
	// Both components are useful: the error keeps its first-cause text and the
	// stream kept its leading bytes.
	if !strings.Contains(err.Error(), strings.Repeat("E", 32)) {
		t.Fatalf("error = %q, want the first Docker error message", err.Error())
	}
	if !strings.HasPrefix(out, strings.Repeat("s", 32)) {
		t.Fatalf("retained stream output = %q..., want the leading stream bytes", out[:32])
	}
	if counting.remaining() != 0 {
		t.Fatalf("drain left %d response bytes unread after the cap", counting.remaining())
	}
}

// TestDrainBuildResponseConcurrentBounds pins per-build bounded retention when
// multiple builds overlap: several concurrent drains read the SAME immutable
// synthetic oversized responses, and each independently retains no more than the
// documented cap plus marker, reports truncation, surfaces the expected
// error/success, and consumes its whole reader. Each goroutine gets its own
// strings.Reader over shared read-only bytes, so no cursor is shared.
func TestDrainBuildResponseConcurrentBounds(t *testing.T) {
	// Built once and only read concurrently; three half-cap chunks cross the cap
	// mid-stream without materializing an oversized literal.
	chunk := strings.Repeat("c", buildOutputRetention/2)
	successBody := drainResponse([]string{chunk, chunk, chunk}, "")
	errorBody := drainResponse([]string{chunk, chunk, chunk}, "concurrent failure")

	const workers = 8
	type result struct {
		out       string
		truncated bool
		err       error
		remaining int
	}
	results := make([]result, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := successBody
			if i%2 == 1 {
				body = errorBody
			}
			counting := &countingReader{r: strings.NewReader(body)}
			out, truncated, err := drainBuildResponse(counting)
			results[i] = result{out: out, truncated: truncated, err: err, remaining: counting.remaining()}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if !r.truncated {
			t.Errorf("worker %d: oversized response must report truncation", i)
		}
		if len(r.out) > buildOutputRetention+len(buildOutputTruncatedMarker)+1 {
			t.Errorf("worker %d: retained %d bytes, want <= cap %d plus marker", i, len(r.out), buildOutputRetention)
		}
		if !strings.HasSuffix(r.out, buildOutputTruncatedMarker) {
			t.Errorf("worker %d: retained output must end with the truncation marker", i)
		}
		if r.remaining != 0 {
			t.Errorf("worker %d: left %d response bytes unread; each drain must consume its reader", i, r.remaining)
		}
		if i%2 == 1 {
			if r.err == nil || !strings.Contains(r.err.Error(), "concurrent failure") {
				t.Errorf("worker %d: error = %v, want the scripted first failure", i, r.err)
			}
		} else if r.err != nil {
			t.Errorf("worker %d: error = %v, want nil", i, r.err)
		}
	}
}

// TestDrainBuildResponseDrainsAfterCapAndError pins that the stream is fully
// consumed even after the retention cap AND after the first error: the reader is
// exhausted and the first error is still the one returned.
func TestDrainBuildResponseDrainsAfterCapAndError(t *testing.T) {
	chunk := strings.Repeat("y", buildOutputRetention/2)
	// Oversized output, then an early error, then more output after the error.
	body := drainResponse([]string{chunk, chunk, chunk}, "first failure") + drainResponse([]string{strings.Repeat("z", 4096)}, "")
	counting := &countingReader{r: strings.NewReader(body)}

	out, truncated, err := drainBuildResponse(counting)
	if err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("error = %v, want the first Docker failure", err)
	}
	if !truncated {
		t.Fatal("oversized output must report truncation even when an error follows")
	}
	if !strings.HasSuffix(out, buildOutputTruncatedMarker) {
		t.Fatalf("retained output must be marked truncated")
	}
	if counting.remaining() != 0 {
		t.Fatalf("drain left %d response bytes unread after the cap/error", counting.remaining())
	}
}

// TestDrainBuildResponseIgnoresMalformedAfterError pins that a malformed trailing
// message after the first error does not replace the real cause.
func TestDrainBuildResponseIgnoresMalformedAfterError(t *testing.T) {
	body := drainResponse(nil, "the real cause") + "{not json\n"
	_, _, err := drainBuildResponse(strings.NewReader(body))
	if err == nil || !strings.Contains(err.Error(), "the real cause") {
		t.Fatalf("error = %v, want the first Docker error even with a malformed trailing message", err)
	}
}

// TestDrainBuildResponseMalformedBeforeErrorSurfacesDecodeError pins that a
// malformed message before any error is surfaced (it is the only failure signal).
func TestDrainBuildResponseMalformedBeforeErrorSurfacesDecodeError(t *testing.T) {
	if _, _, err := drainBuildResponse(strings.NewReader("{not json\n")); err == nil {
		t.Fatal("a malformed response before any error must surface the decode error")
	}
}

// TestDrainBuildResponseDrainsAfterMalformedMessage pins that a malformed
// message does not abandon the response body: parsing stops, but the decoder's
// buffered remainder and the underlying reader are still drained to EOF. The
// trailing payload is far larger than the decoder's initial buffer, so without
// the explicit drain the reader would be left substantially unread.
func TestDrainBuildResponseDrainsAfterMalformedMessage(t *testing.T) {
	// 256 KiB of non-JSON trailing bytes: too large for the decoder to have
	// buffered before it failed on the malformed leading token.
	trailing := strings.Repeat("t", 256<<10)

	t.Run("malformed before any error preserves the decode error", func(t *testing.T) {
		body := "{not json\n" + trailing
		counting := &countingReader{r: strings.NewReader(body)}
		_, _, err := drainBuildResponse(counting)
		if err == nil {
			t.Fatal("a malformed response before any error must surface the decode error")
		}
		if counting.remaining() != 0 {
			t.Fatalf("drain left %d response bytes unread after a malformed message; the body must be fully consumed", counting.remaining())
		}
	})

	t.Run("malformed after a Docker error preserves the first error", func(t *testing.T) {
		body := drainResponse(nil, "the real cause") + "{not json\n" + trailing
		counting := &countingReader{r: strings.NewReader(body)}
		_, _, err := drainBuildResponse(counting)
		if err == nil || !strings.Contains(err.Error(), "the real cause") {
			t.Fatalf("error = %v, want the first Docker error even with a malformed trailing message", err)
		}
		if counting.remaining() != 0 {
			t.Fatalf("drain left %d response bytes unread after a malformed message; the body must be fully consumed", counting.remaining())
		}
	})
}

// countingReader counts the bytes it has not yet returned, so a test can prove a
// drain consumed the whole stream.
type countingReader struct {
	r         io.Reader
	delivered int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.delivered += n
	return n, err
}

// remaining reports how many bytes of the original reader are still unread. It
// only works for a strings.Reader, which is what the tests use.
func (c *countingReader) remaining() int {
	if sr, ok := c.r.(*strings.Reader); ok {
		return sr.Len()
	}
	return -1
}

// TestRunImageBuildStreamsOutputAndDrains pins the end-to-end runImageBuild path
// with a scripted daemon: a successful build whose response is ordinary output
// returns nil, and a large response is bounded and counted without hanging.
func TestRunImageBuildStreamsOutputAndDrains(t *testing.T) {
	ctxDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctxDir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	t.Run("normal", func(t *testing.T) {
		reg := metrics.New()
		cli := newScriptedDockerClient(t,
			dockerRoute{method: "POST", path: "/build", body: drainResponse([]string{"ok\n"}, "")},
		)
		if err := runImageBuild(context.Background(), cli, "app", ctxDir, "relay-app-x:fp", nil, reg); err != nil {
			t.Fatalf("runImageBuild: %v", err)
		}
		if got := reg.Counter(metrics.MetricBuildOutputTruncated); got != 0 {
			t.Fatalf("truncation counter = %d, want 0", got)
		}
	})

	t.Run("oversized-output", func(t *testing.T) {
		reg := metrics.New()
		chunk := strings.Repeat("q", buildOutputRetention/2)
		cli := newScriptedDockerClient(t,
			dockerRoute{method: "POST", path: "/build", body: drainResponse([]string{chunk, chunk, chunk}, "")},
		)
		if err := runImageBuild(context.Background(), cli, "app", ctxDir, "relay-app-x:fp2", nil, reg); err != nil {
			t.Fatalf("runImageBuild oversized: %v", err)
		}
		if got := reg.Counter(metrics.MetricBuildOutputTruncated); got != 1 {
			t.Fatalf("truncation counter = %d, want 1", got)
		}
	})
}

// TestRunImageBuildProducerErrorFailsBuild pins that a context-tar producer
// failure reaches the Docker build operation end to end, not just the pipe seam
// (TestContextTarProducerErrorReachesConsumer). The scripted daemon reads the
// request body — exactly as a real transport does — and returns a nominal 200
// response; a dangling symlink makes the tar walk fail, so runImageBuild must
// report the tar/context producer error instead of accepting the nominal build
// response as success.
func TestRunImageBuildProducerErrorFailsBuild(t *testing.T) {
	ctxDir := t.TempDir()
	if err := os.Symlink(filepath.Join(ctxDir, "does-not-exist"), filepath.Join(ctxDir, "dangling")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	var reached atomic.Bool
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: "POST", path: "/build",
			body:    drainResponse([]string{"ok\n"}, ""),
			onMatch: func() { reached.Store(true) },
		},
	)
	err := runImageBuild(context.Background(), cli, "app", ctxDir, "relay-app-x:fp", nil, nil)
	if !reached.Load() {
		t.Fatal("the scripted daemon was not reached; the producer-error assertion would be vacuous")
	}
	if err == nil {
		t.Fatal("a failed tar producer must fail the build, not pass on the nominal daemon response")
	}
	if !strings.Contains(err.Error(), "tar build context") {
		t.Fatalf("error = %v, want the tar/context producer error", err)
	}
}

// TestContextTarStreamsIncrementally proves the context tar is streamed, not
// buffered: with a file much larger than the pipe can hold, the function blocks
// after the reader has taken only a prefix, and only completes once the reader
// consumes the rest.
func TestContextTarStreamsIncrementally(t *testing.T) {
	ctxDir := t.TempDir()
	// 4 MiB of content: far more than an io.Pipe can absorb unpaused.
	content := bytes.Repeat([]byte("a"), 4<<20)
	if err := os.WriteFile(filepath.Join(ctxDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("write big file: %v", err)
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := writeContextTar(context.Background(), ctxDir, pw)
		_ = pw.CloseWithError(err)
		done <- err
	}()

	// Read only a small prefix; the producer must still be running (blocked on
	// the pipe) because the tar body dwarfs what has been read.
	prefix := make([]byte, 512)
	if _, err := io.ReadFull(pr, prefix); err != nil {
		t.Fatalf("read prefix: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("writeContextTar completed after only %d bytes were read; it must stream, not buffer (err=%v)", len(prefix), err)
	case <-time.After(50 * time.Millisecond):
	}

	// Drain the rest and confirm a clean finish and a round-trippable tar.
	rest, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("writeContextTar: %v", err)
	}
	raw := append(prefix, rest...)
	if got := tarEntry(t, raw, "big.bin"); got != len(content) {
		t.Fatalf("tar entry big.bin size = %d, want %d", got, len(content))
	}
}

// TestContextTarHonorsCancellation proves the producer stops promptly when the
// build context is cancelled rather than running the whole walk.
func TestContextTarHonorsCancellation(t *testing.T) {
	ctxDir := t.TempDir()
	for _, name := range []string{"a.js", "b.js", "c.js"} {
		if err := os.WriteFile(filepath.Join(ctxDir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pr, pw := io.Pipe()
	err := writeContextTar(ctx, ctxDir, pw)
	_ = pr.Close()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writeContextTar cancelled = %v, want context.Canceled", err)
	}
}

// TestContextTarProducerErrorReachesConsumer proves a producer failure is
// surfaced through the pipe (never silently swallowed): a dangling symlink makes
// the tar walk fail, and the reader observes that error rather than io.EOF.
func TestContextTarProducerErrorReachesConsumer(t *testing.T) {
	ctxDir := t.TempDir()
	if err := os.Symlink(filepath.Join(ctxDir, "does-not-exist"), filepath.Join(ctxDir, "dangling")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	pr, pw := io.Pipe()
	go func() { _ = pw.CloseWithError(writeContextTar(context.Background(), ctxDir, pw)) }()
	_, err := io.ReadAll(pr)
	if err == nil {
		t.Fatal("expected the producer's walk error to reach the consumer")
	}
}

// TestContextTarStopsOnEarlyConsumerClose proves an early consumer close does not
// leave the producer spinning: the writer unblocks with an error promptly.
func TestContextTarStopsOnEarlyConsumerClose(t *testing.T) {
	ctxDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctxDir, "big.bin"), bytes.Repeat([]byte("b"), 4<<20), 0o644); err != nil {
		t.Fatalf("write big file: %v", err)
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- writeContextTar(context.Background(), ctxDir, pw) }()

	prefix := make([]byte, 512)
	if _, err := io.ReadFull(pr, prefix); err != nil {
		t.Fatalf("read prefix: %v", err)
	}
	_ = pr.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("producer must observe the early consumer close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("producer did not stop after the consumer closed the pipe")
	}
}

// tarEntry returns the size of the named entry in a tar stream, or -1.
func tarEntry(t *testing.T, raw []byte, name string) int {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return -1
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		if hdr.Name == name {
			return int(hdr.Size)
		}
	}
}

// TestRunImageBuildJoinsProducerOnDaemonFailure proves the producer goroutine is
// joined (no leak, no hang) when the daemon request fails before a response: the
// call returns the transport error promptly.
func TestRunImageBuildJoinsProducerOnDaemonFailure(t *testing.T) {
	ctxDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctxDir, "index.js"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: "POST", path: "/build",
			fail: func(*http.Request) error { return errors.New("daemon unreachable") },
		},
	)
	done := make(chan error, 1)
	go func() {
		done <- runImageBuild(context.Background(), cli, "app", ctxDir, "relay-app-x:fp", nil, nil)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the daemon failure to surface")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runImageBuild did not return promptly after a daemon failure (producer not joined)")
	}
}

// TestRunImageBuildCancelsBlockedTarProducer proves the explicit requirement
// that cancelling an in-flight build unblocks and joins a context-tar producer
// that is actively blocked on writes. The scripted daemon enters /build and
// blocks on the request context WITHOUT ever reading the request body, so the
// producer is parked on the unbuffered pipe mid-tar. Cancelling the build
// context must reach runImageBuild's error path, which closes the pipe reader,
// joins the producer, and returns context.Canceled promptly — a producer left
// blocked on a full pipe would instead hang the call.
func TestRunImageBuildCancelsBlockedTarProducer(t *testing.T) {
	ctxDir := t.TempDir()
	// A sparse multi-MiB file: its size dwarfs any pipe buffer so the producer
	// is unambiguously blocked mid-stream, without materializing duplicate bytes
	// in the test heap.
	big := filepath.Join(ctxDir, "big.bin")
	f, err := os.Create(big)
	if err != nil {
		t.Fatalf("create big file: %v", err)
	}
	if err := f.Truncate(4 << 20); err != nil {
		_ = f.Close()
		t.Fatalf("truncate big file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close big file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{})
	readErr := make(chan error, 1)
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodPost, path: "/build",
			onRequest: func(req *http.Request) {
				// Consume exactly one tar block: this cannot complete until the
				// producer has actively streamed it, so it proves the producer is
				// mid-tar (and it parks on the next write once we stop reading).
				_, err := io.ReadFull(req.Body, make([]byte, 512))
				readErr <- err
				close(entered)
			},
			fail: func(req *http.Request) error {
				// Do not read the request body: the tar producer stays blocked on
				// the unbuffered pipe until the build context is cancelled.
				<-req.Context().Done()
				return req.Context().Err()
			},
		},
	)

	done := make(chan error, 1)
	go func() {
		done <- runImageBuild(ctx, cli, "app", ctxDir, "relay-app-x:fp", nil, nil)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ImageBuild was not entered")
	}
	if err := <-readErr; err != nil {
		t.Fatalf("producer did not stream the first tar block; the blocked-producer assertion would be vacuous: %v", err)
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runImageBuild cancelled = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runImageBuild did not return after cancellation while its tar producer was blocked (producer not joined)")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("runImageBuild took %v to cancel; want prompt cancellation", elapsed)
	}
}
