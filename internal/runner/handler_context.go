package runner

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"relay/internal/runtime"
)

// requestIDSource names the broker-backed execution family a request ID is
// derived from. Relay consumes one stream today, so the message ID is already
// unique across families; keeping the source in the identity makes that
// uniqueness explicit and survives a shared message ID if entries are ever
// delivered through more than one source.
type requestIDSource string

const (
	requestIDSourceEvent    requestIDSource = "event"
	requestIDSourceSchedule requestIDSource = "schedule"
)

var stateFreeRequestIDSequence atomic.Uint64

// brokerRequestID derives the deterministic handler-context request ID for one
// broker-backed invocation from its stable source identity: the source family,
// the stream message ID (or schedule occurrence message ID), and the
// invocation's app and handler. It deliberately excludes delivery and attempt
// counts, so every retry, redelivery, and worker restart computes the same ID
// for the same unit of work. The invocation suffix keeps handlers of one
// message distinct.
func brokerRequestID(source requestIDSource, msgID, app, handler string) string {
	return string(source) + ":" + msgID + ":" + app + "/" + handler
}

// stateFreeRequestID returns a fresh request ID when there is no broker
// invocation identity to derive one from (manual invocation, DLQ replay, or
// another direct caller). Each call is a distinct logical execution, even when
// a replay carries its original source message ID as provenance.
func stateFreeRequestID() string {
	return stateFreeRequestIDWithRandom(rand.Read)
}

func stateFreeRequestIDWithRandom(readRandom func([]byte) (int, error)) string {
	var b [16]byte
	if _, err := readRandom(b[:]); err != nil {
		sequence := stateFreeRequestIDSequence.Add(1)
		return fmt.Sprintf("%x-%x", time.Now().UnixNano(), sequence)
	}
	return hex.EncodeToString(b[:])
}

// newHandlerContext builds the runtime.HandlerContext stamped on an invocation
// context. attempt is the confirmed 1-based handler attempt for a broker-backed
// execution (1 when there is no confirmed claim), and maxAttempts is the
// configured retry budget as a total attempt count (initial attempt plus
// retries). The attempt is floored at 1 so a state-free execution still reports
// a well-formed attempt; maxAttempts is passed through unchanged, because a
// config change can leave the claimed attempt above the current budget and the
// handler must see the budget that actually applies.
func newHandlerContext(requestID, app, handler, msgID string, attempt, maxAttempts int) runtime.HandlerContext {
	if attempt < 1 {
		attempt = 1
	}
	return runtime.HandlerContext{
		RequestID:   requestID,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		MessageID:   msgID,
		App:         app,
		Handler:     handler,
	}
}
