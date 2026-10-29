package state

import (
	"strings"
	"testing"

	"relay/internal/dlq"
)

// TestDLQDataRoundTripsArbitraryIdentityAndPayloadBytes pins the private JSONB
// representation independently of PostgreSQL: identity, event, and reason bytes
// must survive even when they are not valid UTF-8 or contain NUL. encoding/json
// base64-encodes the []byte fields, so JSONB never sees the forbidden bytes.
func TestDLQDataRoundTripsArbitraryIdentityAndPayloadBytes(t *testing.T) {
	const lossy = "\x00nul \xff\xfe invalid \n"
	entry := dlq.Entry{
		ID:              "entry",
		Source:          lossy,
		SourceGroup:     "\xffgroup",
		SourceMessageID: "\x00message",
		Consumer:        "\xffworker",
		App:             "fn",
		Handler:         "h",
		Event:           `{"a":1}` + lossy,
		Reason:          "exhausted" + lossy,
		Deliveries:      7,
		HandlerAttempts: 3,
		Trace:           "trace",
	}

	text, err := marshalDLQData(entry)
	if err != nil {
		t.Fatalf("marshalDLQData: %v", err)
	}
	if strings.ContainsAny(text, "\x00") {
		t.Fatalf("private JSON must not contain raw NUL: %q", text)
	}
	got, err := unmarshalDLQData(entry.ID, text)
	if err != nil {
		t.Fatalf("unmarshalDLQData: %v", err)
	}
	if got.Source != entry.Source || got.SourceGroup != entry.SourceGroup ||
		got.SourceMessageID != entry.SourceMessageID || got.Consumer != entry.Consumer {
		t.Fatalf("identity round-trip mismatch: %+v", got)
	}
	if got.Event != entry.Event || got.Reason != entry.Reason {
		t.Fatalf("payload round-trip mismatch: event=%q reason=%q", got.Event, got.Reason)
	}
}
