package runtime

// Unit tests for the persistent execution-channel wire format (protocol.go):
// 4-byte big-endian length-prefixed UTF-8 JSON frames in both directions, the
// HMAC challenge primitives, the per-container credential shape, and the
// request-frame encoding Relay sends. The old stdin/stdout sentinel demuxer
// inventory was removed with the protocol it pinned; channel/session behavior
// lives in execution_channel_test.go and execution_container_test.go, and
// native Docker log observation lives in the integration suite.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// fragmentedReader yields at most n bytes per Read, so a codec that assumes a
// frame arrives in one Read would fail.
type fragmentedReader struct {
	data []byte
	n    int
}

func (r *fragmentedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.n
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// shortWriter accepts at most n bytes per Write, exercising the partial-write
// loop in writeFull.
type shortWriter struct {
	buf bytes.Buffer
	n   int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.n {
		p = p[:w.n]
	}
	return w.buf.Write(p)
}

// stalledWriter accepts zero bytes without an error, the degenerate write
// writeFull must refuse rather than loop on.
type stalledWriter struct{}

func (stalledWriter) Write([]byte) (int, error) { return 0, nil }

// failingWriter writes n bytes once, then fails.
type failingWriter struct {
	remaining int
	err       error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, w.err
	}
	if len(p) > w.remaining {
		p = p[:w.remaining]
	}
	w.remaining -= len(p)
	return len(p), nil
}

func isLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte(`{"type":"result","id":"abc","ok":true}`)
	var buf bytes.Buffer
	if err := writeFrame(&buf, payload, maxResponseFrame); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	got, err := readFrame(&buf, maxResponseFrame)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round trip = %q, want %q", got, payload)
	}
	if buf.Len() != 0 {
		t.Errorf("%d bytes left after reading the frame, want 0", buf.Len())
	}
}

func TestFrameHeaderIsFourByteBigEndianLength(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 300)
	var buf bytes.Buffer
	if err := writeFrame(&buf, payload, maxResponseFrame); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	raw := buf.Bytes()
	if len(raw) != 4+len(payload) {
		t.Fatalf("framed length = %d, want %d", len(raw), 4+len(payload))
	}
	if got := binary.BigEndian.Uint32(raw[:4]); got != 300 {
		t.Errorf("length header = %d, want 300 (big-endian)", got)
	}
}

func TestWriteFrameRejectsEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, nil, maxResponseFrame); err == nil {
		t.Fatal("an empty payload must be rejected")
	}
	if buf.Len() != 0 {
		t.Errorf("a rejected write emitted %d bytes, want 0", buf.Len())
	}
}

func TestWriteFrameRejectsOversizePayload(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, make([]byte, 9), 8); err == nil {
		t.Fatal("a payload over the limit must be rejected")
	} else if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error = %v, want it to name the limit", err)
	}
	if buf.Len() != 0 {
		t.Errorf("a rejected write emitted %d bytes, want 0", buf.Len())
	}
}

func TestWriteFrameHandlesPartialWrites(t *testing.T) {
	payload := []byte(`{"type":"invoke","id":"partial"}`)
	w := &shortWriter{n: 3}
	if err := writeFrame(w, payload, maxResponseFrame); err != nil {
		t.Fatalf("writeFrame over a 3-byte writer: %v", err)
	}
	raw := w.buf.Bytes()
	got, err := readFrame(bytes.NewReader(raw), maxResponseFrame)
	if err != nil {
		t.Fatalf("readFrame of the partially written frame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload = %q, want %q", got, payload)
	}
}

func TestWriteFrameStalledWriterIsShortWriteError(t *testing.T) {
	err := writeFrame(stalledWriter{}, []byte("x"), maxResponseFrame)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want io.ErrShortWrite", err)
	}
}

func TestWriteFrameWriteErrorPropagates(t *testing.T) {
	sentinel := errors.New("sink exploded")
	err := writeFrame(&failingWriter{remaining: 2, err: sentinel}, []byte("abcdef"), maxResponseFrame)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want the writer's error", err)
	}
}

func TestReadFrameFragmentedReads(t *testing.T) {
	payload := []byte(`{"type":"result","id":"frag","ok":false,"error":"boom"}`)
	var buf bytes.Buffer
	if err := writeFrame(&buf, payload, maxResponseFrame); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	got, err := readFrame(&fragmentedReader{data: buf.Bytes(), n: 1}, maxResponseFrame)
	if err != nil {
		t.Fatalf("readFrame one byte at a time: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload = %q, want %q", got, payload)
	}
}

func TestReadFrameRejectsZeroLength(t *testing.T) {
	header := make([]byte, 4) // big-endian 0
	_, err := readFrame(bytes.NewReader(header), maxResponseFrame)
	if err == nil || !strings.Contains(err.Error(), "empty frame") {
		t.Fatalf("error = %v, want the empty-frame rejection", err)
	}
}

func TestReadFrameRejectsOversizeLength(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 9)
	// The reader carries only the header: if readFrame tried to consume the
	// declared payload it would report EOF instead of naming the limit.
	_, err := readFrame(bytes.NewReader(header[:]), 8)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want the size-limit rejection", err)
	}
}

func TestReadFrameTruncatedPayloadIsError(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 10)
	body := append(header[:], []byte("12345")...)
	_, err := readFrame(bytes.NewReader(body), maxResponseFrame)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want unexpected-EOF for a truncated frame", err)
	}
}

func TestReadFrameOnClosedConnectionIsEOF(t *testing.T) {
	_, err := readFrame(bytes.NewReader(nil), maxResponseFrame)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error = %v, want io.EOF at connection close", err)
	}
}

func TestClampResponseErrorBoundsAndKeepsUTF8(t *testing.T) {
	if short := "boom"; clampResponseError(short) != short {
		t.Errorf("clampResponseError(%q) = %q, want unchanged", short, clampResponseError(short))
	}
	clamped := clampResponseError(strings.Repeat("x", 1<<20))
	if len(clamped) != maxResponseError {
		t.Fatalf("clamped length = %d, want maxResponseError (%d)", len(clamped), maxResponseError)
	}
	// A multi-megabyte multibyte error must clamp on a code-point boundary:
	// the result is always valid UTF-8 and still bounded.
	multi := clampResponseError(strings.Repeat("\U0001F600", 1<<18))
	if len(multi) > maxResponseError {
		t.Errorf("clamped multibyte length %d exceeds maxResponseError %d", len(multi), maxResponseError)
	}
	for i, r := range multi {
		_ = i
		if r == 0xFFFD {
			t.Fatalf("clamped multibyte error contains a replacement rune (split rune) at %d", i)
		}
	}
	if !json.Valid([]byte(`{"error":"` + strings.ReplaceAll(multi, `"`, "") + `"}`)) {
		t.Error("clamped multibyte error must be encodable in a result frame")
	}
}

func TestExecutionCredentialAndNonceShape(t *testing.T) {
	cred1, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("newExecutionCredential: %v", err)
	}
	cred2, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("newExecutionCredential: %v", err)
	}
	if !isLowerHex64(cred1) {
		t.Errorf("credential %q is not 64 lowercase hex chars", cred1)
	}
	if cred1 == cred2 {
		t.Error("two container credentials must be distinct")
	}
	nonce1, err := newHandshakeNonce()
	if err != nil {
		t.Fatalf("newHandshakeNonce: %v", err)
	}
	if !isLowerHex64(nonce1) {
		t.Errorf("nonce %q is not 64 lowercase hex chars", nonce1)
	}
	if nonce1 == cred1 {
		t.Error("a nonce must not collide with a credential")
	}
}

func TestExecutionChallengeMACComposition(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	nonce := strings.Repeat("a", 64)

	// Pin the exact key and message composition: HMAC-SHA256 keyed with the
	// ASCII bytes of the credential over the direction context plus nonce.
	want := func(contextPrefix string) string {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(contextPrefix + nonce))
		return hex.EncodeToString(mac.Sum(nil))
	}
	gotBootstrap := executionChallengeHex(secret, executionBootstrapAuthContext, nonce)
	if gotBootstrap != want(executionBootstrapAuthContext) {
		t.Errorf("bootstrap MAC = %s, want %s", gotBootstrap, want(executionBootstrapAuthContext))
	}
	gotClient := executionChallengeHex(secret, executionClientAuthContext, nonce)
	if gotClient != want(executionClientAuthContext) {
		t.Errorf("client MAC = %s, want %s", gotClient, want(executionClientAuthContext))
	}
	if gotBootstrap == gotClient {
		t.Error("the two directions must not produce the same MAC for the same nonce")
	}
	if !isLowerHex64(gotBootstrap) {
		t.Errorf("MAC %q is not 64 lowercase hex chars", gotBootstrap)
	}
}

func TestVerifyExecutionChallenge(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	nonce := strings.Repeat("b", 64)
	mac := executionChallengeHex(secret, executionBootstrapAuthContext, nonce)

	if !verifyExecutionChallenge(secret, executionBootstrapAuthContext, nonce, mac) {
		t.Error("the correct MAC must verify")
	}
	if verifyExecutionChallenge(secret, executionBootstrapAuthContext, strings.Repeat("c", 64), mac) {
		t.Error("a MAC over a different nonce must not verify")
	}
	otherSecret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	if verifyExecutionChallenge(otherSecret, executionBootstrapAuthContext, nonce, mac) {
		t.Error("a MAC under a different credential must not verify")
	}
	if verifyExecutionChallenge(secret, executionClientAuthContext, nonce, mac) {
		t.Error("a MAC computed under the other direction's context must not verify")
	}
	if verifyExecutionChallenge(secret, executionBootstrapAuthContext, nonce, strings.ToUpper(mac)) {
		t.Error("the wire form is lowercase hex; an uppercase MAC must not verify")
	}
	if verifyExecutionChallenge(secret, executionBootstrapAuthContext, nonce, mac[:63]) {
		t.Error("a truncated MAC must not verify")
	}
}

func TestEncodeInvokeRequestShape(t *testing.T) {
	event := []byte(`{"event_name":"INSERT","id":9}`)
	env := map[string]string{"TOKEN": "v1"}
	ctx := context.WithValue(context.Background(), runMetaKey{}, RunMeta{App: "app-fn"})
	frame, err := encodeInvokeRequest(ctx, "req-1", "index.hi", event, env)
	if err != nil {
		t.Fatalf("encodeInvokeRequest: %v", err)
	}
	var req invokeRequest
	if err := json.Unmarshal(frame, &req); err != nil {
		t.Fatalf("frame is not valid JSON: %v", err)
	}
	if req.Type != frameTypeInvoke {
		t.Errorf("type = %q, want %q", req.Type, frameTypeInvoke)
	}
	if req.ID != "req-1" {
		t.Errorf("id = %q, want the echoed request id", req.ID)
	}
	if req.Handler != "index.hi" {
		t.Errorf("handler = %q, want index.hi", req.Handler)
	}
	var want, got map[string]any
	if err := json.Unmarshal(event, &want); err != nil {
		t.Fatalf("fixture event: %v", err)
	}
	if err := json.Unmarshal(req.Event, &got); err != nil {
		t.Fatalf("frame event is not valid JSON: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("event = %v, want the verbatim payload %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("event[%q] = %v, want %v", k, got[k], v)
		}
	}
	if req.Env["TOKEN"] != "v1" || len(req.Env) != 1 {
		t.Errorf("env = %v, want exactly the per-invocation map", req.Env)
	}
	if req.Context == nil {
		t.Fatal("context must always be present on a request frame")
	}
	if req.Context.RequestID == "" || req.Context.Handler != "index.hi" || req.Context.App != "app-fn" {
		t.Errorf("context = %+v, want the execution identity from RunMeta", req.Context)
	}
	if req.Context.Attempt < 1 || req.Context.MaxAttempts < 1 {
		t.Errorf("context attempt budget = %d/%d, want at least 1/1",
			req.Context.Attempt, req.Context.MaxAttempts)
	}
	if req.Context.Deadline == "" {
		t.Error("context deadline must always render (the zero instant when unset)")
	}
	if req.Trace != nil {
		t.Errorf("trace = %+v, want no trace object without a trace context", req.Trace)
	}
}
