package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"

	"relay/internal/observability/tracing"
)

// The persistent execution-channel protocol.
//
// Each execution container runs ONE long-lived bootstrap process. Relay opens
// ONE TCP connection to that process when the container starts, authenticates
// it, and then reuses the same connection for every invocation served by the
// container. There is no stdin/stdout attach, no sentinel, and no line-oriented
// demultiplexing: a frame is length-prefixed JSON in both directions. The
// bootstrap's stdout/stderr are application output owned by the container; Relay
// neither reads nor forwards them.
//
// # Transport
//
// The bootstrap listens on 0.0.0.0:<RELAY_EXECUTION_PORT> (currently 7000). The
// port is internal to the container's private network namespace, is never
// published, and is reserved for the bootstrap (handler code must not bind it).
// Relay reaches it over one of the worker-global shared Docker
// networks (NETWORKS) the container was created on and the worker is expected
// to be attached to (see execution_channel.go). Each container gets a fresh
// random credential in RELAY_EXECUTION_SECRET; it is never logged and never
// appears in a frame. The bootstrap should read both variables at startup (and
// may remove the secret from its own environment before running handlers, though
// handler code is not isolated from its own bootstrap process).
//
// # Framing
//
// Every message is:
//
//	4-byte big-endian uint32 N, then N bytes of UTF-8 JSON
//
// N must be non-zero and at most the limit for that message (below). There is no
// newline or other in-band delimiter; readers use io.ReadFull and writers loop
// over partial writes. A frame larger than its limit, a zero length, or malformed
// JSON is a fatal protocol error and the container is discarded.
//
// Handshake (once per connection, before any invocation)
//
//	Relay    -> bootstrap: {"type":"hello","version":1,
//	                        "nonce":"<64 lowercase hex chars>"}
//	bootstrap -> Relay:    {"type":"auth","version":1,
//	                        "mac":"<64 lowercase hex chars>",
//	                        "nonce":"<64 lowercase hex chars>"}
//	Relay    -> bootstrap: {"type":"ready","version":1,
//	                        "mac":"<64 lowercase hex chars>"}
//
// Both MACs use HMAC-SHA256 with the SAME key: the ASCII bytes of
// RELAY_EXECUTION_SECRET (64 lowercase hex chars). The messages differ:
//
//	auth.mac  = HMAC("relay-bootstrap-auth-v1:" + Relay hello nonce)
//	ready.mac = HMAC("relay-client-auth-v1:" + bootstrap auth nonce)
//
// Both values are lowercase hex. The bootstrap must send "auth" only after it
// has accepted the connection and is ready to read invocation frames; a verified
// auth response is Relay's readiness check. The bootstrap must verify the
// "ready" MAC before accepting any invocation frame, so a sibling container on
// the control network cannot invoke handlers without the credential. It may close
// unauthenticated connections (with a short handshake deadline) and accept
// again, but it serves exactly one authenticated connection at a time. A wrong
// MAC, wrong version, unexpected frame, or handshake timeout fails the start.
//
// Invocation (sequential; one request/response at a time on the connection)
//
//	Relay    -> bootstrap: {"type":"invoke","id":"<id>","handler":"mod.func",
//	                        "event":<raw JSON>,"env":{"K":"V"},
//	                        "trace":{"traceparent":"…","tracestate":"…",
//	                                 "baggage":"…"},
//	                        "context":{"requestId":"…","attempt":1,
//	                                   "maxAttempts":3,
//	                                   "deadline":"<RFC 3339>",
//	                                   "messageId":"…","traceId":"…",
//	                                   "app":"…","handler":"mod.func"}}
//	bootstrap -> Relay:    {"type":"result","id":"<id>","ok":true}
//	                       {"type":"result","id":"<id>","ok":false,"error":"…"}
//
// event is passed through verbatim. trace is omitted when there is no trace
// context; context is always present. The bootstrap must echo the request id
// exactly and must not put Docker/container identifiers into any frame. A result
// must carry type "result", a nonempty id, and an explicit boolean "ok"; a
// missing/null "ok" is a malformed frame, never an implied handler failure.
//
// A handler exception is a normal ok:false result and the container stays
// healthy. A malformed frame, a duplicate result, a result with no pending
// invocation, or a result whose id does not match the in-flight request is a
// protocol violation: Relay discards the container and never reports success for
// that invocation. Transport loss while an invocation is unresolved is the same
// discard. Relay cancellation closes the connection and discards the container.
//
// Limits
//
//	maxHandshakeFrame 4 KiB
//	maxRequestFrame   8 MiB
//	maxResponseFrame  1 MiB
//	maxResponseError  64 KiB (Relay's diagnostic truncation of an error string)
//
// Limits are the JSON payload size, excluding the 4-byte header.
const (
	// executionPort is the fixed internal bootstrap port. Network namespaces are
	// isolated, so no port publishing or per-container allocation is involved.
	executionPort = 7000

	// executionEnvPort and executionEnvSecret are the bootstrap's two Relay-owned
	// environment variables. They are appended after the app's plan env (and any
	// same-named plan entry is removed), so an app cannot override the channel
	// address or credential.
	executionEnvPort   = "RELAY_EXECUTION_PORT"
	executionEnvSecret = "RELAY_EXECUTION_SECRET"

	// executionProtocolVersion is carried by every handshake frame.
	executionProtocolVersion = 1

	maxHandshakeFrame = 4 << 10  // 4 KiB
	maxRequestFrame   = 8 << 20  // 8 MiB
	maxResponseFrame  = 1 << 20  // 1 MiB
	maxResponseError  = 64 << 10 // 64 KiB

	frameTypeHello  = "hello"
	frameTypeAuth   = "auth"
	frameTypeReady  = "ready"
	frameTypeInvoke = "invoke"
	frameTypeResult = "result"

	executionBootstrapAuthContext = "relay-bootstrap-auth-v1:"
	executionClientAuthContext    = "relay-client-auth-v1:"
)

// handshakeRequest is Relay's hello frame.
type handshakeRequest struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	Nonce   string `json:"nonce"`
}

// handshakeResponse is the bootstrap's challenge response. MAC authenticates
// the bootstrap over Relay's hello nonce; Nonce is the bootstrap's own challenge
// that Relay must answer with the ready frame.
type handshakeResponse struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	MAC     string `json:"mac"`
	Nonce   string `json:"nonce"`
}

// handshakeReady is Relay's answer to the bootstrap's challenge.
type handshakeReady struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	MAC     string `json:"mac"`
}

// invokeRequest is one request frame. Event is the raw event JSON passed through
// verbatim (json.RawMessage), so Relay never re-encodes or mutates the payload.
//
// Trace is the optional W3C trace-context carrier (traceparent/tracestate/
// baggage) injected from the invocation's context under the single "trace"
// object field. It is omitempty, so no trace object is emitted when there is no
// trace context. The Node bootstrap extracts it via
// propagation.extract(context.active(), req.trace ?? {}) and the Python
// bootstrap does the same through its global propagator.
//
// Context is the always-present per-invocation execution metadata (see
// HandlerContext), including the stable request ID, attempt/budget, effective
// deadline, and the optional source message and trace IDs.
type invokeRequest struct {
	Type    string               `json:"type"`
	ID      string               `json:"id"`
	Handler string               `json:"handler"`
	Event   json.RawMessage      `json:"event"`
	Env     map[string]string    `json:"env"`
	Trace   *traceFrame          `json:"trace,omitempty"`
	Context *handlerContextFrame `json:"context"`
}

// handlerContextFrame is the per-invocation execution metadata nested under the
// request frame's "context" field (see HandlerContext). Deadline is the
// invocation context's effective deadline formatted as RFC 3339/ISO-8601; a
// context with no deadline carries the zero instant, which the handler side
// treats as already expired. MessageID and TraceID are omitted when absent, so
// state-free executions do not invent broker or trace identities.
type handlerContextFrame struct {
	RequestID   string `json:"requestId"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"maxAttempts"`
	Deadline    string `json:"deadline"`
	MessageID   string `json:"messageId,omitempty"`
	TraceID     string `json:"traceId,omitempty"`
	App         string `json:"app"`
	Handler     string `json:"handler"`
}

// newHandlerContextFrame renders the HandlerContext carried by ctx into the
// request frame's "context" object. Missing fields are normalized so the frame
// object is always complete: direct runtime callers that never injected a
// HandlerContext get a fresh state-free identity (attempt 1, maxAttempts 1),
// while the runner's broker-backed paths supply the stable values. The deadline
// is read from ctx itself (not from HandlerContext) so it always matches the
// context that cancels the execution, and the trace ID is the current span's
// W3C trace ID so it agrees with the "trace" carrier.
func newHandlerContextFrame(ctx context.Context, handler string) *handlerContextFrame {
	hc := HandlerContextFrom(ctx).normalized(RunMetaFrom(ctx).App, handler)
	deadline, _ := ctx.Deadline()
	frame := &handlerContextFrame{
		RequestID:   hc.RequestID,
		Attempt:     hc.Attempt,
		MaxAttempts: hc.MaxAttempts,
		Deadline:    deadline.UTC().Format(time.RFC3339Nano),
		MessageID:   hc.MessageID,
		App:         hc.App,
		Handler:     hc.Handler,
	}
	if spanCtx := oteltrace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		frame.TraceID = spanCtx.TraceID().String()
	}
	return frame
}

// traceFrame is the W3C trace-context carrier nested under the request frame's
// "trace" field. It deliberately uses the exact header names as JSON keys so the
// container side can hand the object to its propagator directly (the Node
// bootstrap does exactly this). It is a pointer so an absent context omits the
// whole object rather than emitting empty fields.
type traceFrame struct {
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
	Baggage     string `json:"baggage,omitempty"`
}

// newTraceFrame renders the current trace context into the request-frame "trace"
// object, or returns nil when there is nothing to carry (tracing disabled, or a
// context with no recording span). It is the Go-side injection point for the
// execution-container boundary; the carrier convention lives in the tracing
// package so the frame and the stream message never disagree on field names.
func newTraceFrame(ctx context.Context) *traceFrame {
	carrier := tracing.CarrierFromContext(ctx)
	if len(carrier) == 0 {
		return nil
	}
	return &traceFrame{
		Traceparent: carrier[tracing.TraceparentKey],
		Tracestate:  carrier[tracing.TracestateKey],
		Baggage:     carrier[tracing.BaggageKey],
	}
}

// encodeInvokeRequest renders one request frame payload from the invocation
// identity, the verbatim event payload, the per-invocation env, the trace context
// carried by ctx, and ctx's HandlerContext metadata. The event JSON is passed
// through untouched (json.RawMessage) and the trace object is omitted when ctx
// carries no trace context. The context object is always present: it is
// execution metadata, not payload, and a handler side may depend on its identity
// fields.
func encodeInvokeRequest(ctx context.Context, id, handler string, eventJSON []byte, env map[string]string) ([]byte, error) {
	return json.Marshal(invokeRequest{
		Type:    frameTypeInvoke,
		ID:      id,
		Handler: handler,
		Event:   json.RawMessage(eventJSON),
		Env:     env,
		Trace:   newTraceFrame(ctx),
		Context: newHandlerContextFrame(ctx, handler),
	})
}

// invokeResponse is one result frame payload.
type invokeResponse struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// UnmarshalJSON decodes a result frame strictly enough that a missing required
// field can never be mistaken for a handler failure. In particular, a missing or
// null "ok" would otherwise leave the bool false and look like a normal
// ok:false response, keeping a malformed frame's container alive. Type, a
// nonempty id, and a present boolean ok are required; unknown fields remain
// forward-compatible.
func (r *invokeResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Type  *string `json:"type"`
		ID    *string `json:"id"`
		OK    *bool   `json:"ok"`
		Error string  `json:"error,omitempty"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	switch {
	case wire.Type == nil || *wire.Type != frameTypeResult:
		return errors.New("result frame has an invalid type")
	case wire.ID == nil || *wire.ID == "":
		return errors.New("result frame has no id")
	case wire.OK == nil:
		return errors.New("result frame has no ok field")
	}
	*r = invokeResponse{
		Type:  *wire.Type,
		ID:    *wire.ID,
		OK:    *wire.OK,
		Error: wire.Error,
	}
	return nil
}

// clampResponseError bounds a response error string to maxResponseError bytes.
// The Go side truncates what the bootstraps already bound: belt and braces for
// hand-written or future bootstraps. Truncation happens on a Unicode code-point
// boundary so the result is always valid UTF-8 (a byte-slice could otherwise
// split a multibyte character).
func clampResponseError(s string) string {
	if len(s) <= maxResponseError {
		return s
	}
	return strings.ToValidUTF8(s[:maxResponseError], "")
}

// writeFrame writes one length-prefixed frame. It loops over partial writes, so
// a caller never loses the tail of a frame on a short write.
func writeFrame(w io.Writer, payload []byte, max int) error {
	if len(payload) == 0 {
		return errors.New("protocol: empty frame")
	}
	if len(payload) > max {
		return fmt.Errorf("protocol: frame of %d bytes exceeds the %d-byte limit", len(payload), max)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, payload)
}

// writeFull writes all of p, handling short writes.
func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// readFrame reads one length-prefixed frame and enforces max. io.ReadFull makes
// partial reads harmless; a zero length, an oversized length, or a truncated
// frame is an error.
func readFrame(r io.Reader, max int) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		return nil, errors.New("protocol: empty frame")
	}
	if int64(size) > int64(max) {
		return nil, fmt.Errorf("protocol: frame of %d bytes exceeds the %d-byte limit", size, max)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// newExecutionCredential returns a fresh 32-byte credential rendered as 64
// lowercase hex chars. It fails closed: a credential must never fall back to a
// predictable value.
func newExecutionCredential() (string, error) {
	return newRandomHex(32)
}

// newHandshakeNonce returns a fresh challenge nonce rendered as 64 lowercase hex
// chars.
func newHandshakeNonce() (string, error) {
	return newRandomHex(32)
}

func newRandomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("protocol: generate random value: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// executionChallengeMAC computes one handshake MAC. The key is the ASCII bytes
// of the 64-character credential; the message is the direction-specific context
// prefix plus the nonce string exactly as sent.
func executionChallengeMAC(secret, authContext, nonce string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(authContext + nonce))
	return mac.Sum(nil)
}

// executionChallengeHex renders one challenge MAC in the protocol's lowercase
// hex form.
func executionChallengeHex(secret, authContext, nonce string) string {
	return hex.EncodeToString(executionChallengeMAC(secret, authContext, nonce))
}

// verifyExecutionChallenge compares a received MAC with the expected value in
// constant time. The received value is compared as the lowercase-hex ASCII form
// the contract requires.
func verifyExecutionChallenge(secret, authContext, nonce, got string) bool {
	expected := executionChallengeHex(secret, authContext, nonce)
	return hmac.Equal([]byte(expected), []byte(got))
}
