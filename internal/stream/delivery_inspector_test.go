package stream

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestDeliveryInspectorRefusesUnwiredAndIncomplete pins the fail-closed guard
// rails without any broker: a nil client (or a nil inspector) fails every query
// — reconciliation must preserve unsettled rows when the adapter is unwired —
// and an incomplete identity is refused before any command is sent.
func TestDeliveryInspectorRefusesUnwiredAndIncomplete(t *testing.T) {
	ctx := context.Background()
	var nilInspector *DeliveryInspector
	if _, err := nilInspector.Pending(ctx, "s", "g", "1-0"); err == nil {
		t.Fatal("a nil inspector must fail closed")
	}
	unwired := NewDeliveryInspector(nil)
	if _, err := unwired.Pending(ctx, "s", "g", "1-0"); err == nil {
		t.Fatal("an inspector without a Redis client must fail closed")
	}
	for _, tc := range [][3]string{
		{"", "g", "1-0"},
		{"s", "", "1-0"},
		{"s", "g", ""},
	} {
		if _, err := NewDeliveryInspector(&redis.Client{}).Pending(ctx, tc[0], tc[1], tc[2]); err == nil {
			t.Fatalf("incomplete identity %+v must be refused before any Redis I/O", tc)
		}
	}
}
