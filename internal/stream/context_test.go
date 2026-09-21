package stream

import (
	"context"
	"testing"
)

// TestDeliveryAttemptContextRoundTrip pins the WithDeliveryAttempt /
// DeliveryAttemptFrom contract: a set attempt round-trips, and an absent value
// defaults to 1.
func TestDeliveryAttemptContextRoundTrip(t *testing.T) {
	ctx := WithDeliveryAttempt(context.Background(), 7)
	if got := DeliveryAttemptFrom(ctx); got != 7 {
		t.Fatalf("DeliveryAttemptFrom = %d, want 7", got)
	}
	if got := DeliveryAttemptFrom(context.Background()); got != 1 {
		t.Fatalf("absent attempt = %d, want default 1", got)
	}
}

// TestDeliveryAttemptFromClampsBelowOne pins the clamp: any stored value below 1
// (0 or negative) is reported as 1, so a caller can always treat the result as a
// 1-based attempt number.
func TestDeliveryAttemptFromClampsBelowOne(t *testing.T) {
	for _, n := range []int64{0, -1, -100} {
		if got := DeliveryAttemptFrom(WithDeliveryAttempt(context.Background(), n)); got != 1 {
			t.Errorf("attempt %d => %d, want clamp to 1", n, got)
		}
	}
}
