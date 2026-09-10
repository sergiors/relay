package stream

import (
	"testing"

	"github.com/redis/go-redis/v9"

	"relay/internal/function"
)

// TestNewConsumerDerivedDefaults pins the default derivation for the reclaim
// MinPendingIdle threshold and the reclaim interval. These defaults are what
// pace message-level retry/recovery, so they are locked down here.
func TestNewConsumerDerivedDefaults(t *testing.T) {
	// A client is required for the consumer to be constructed; the address is
	// never dialed by NewConsumer.
	cfg := func() ConsumerConfig {
		return ConsumerConfig{Client: redis.NewClient(&redis.Options{Addr: "localhost:6379"})}
	}

	// Unset MinPendingIdle → 3 * MaxRuleTimeout (15m).
	c := NewConsumer(cfg())
	if got := c.minPendingIdle; got != 3*MaxRuleTimeout {
		t.Errorf("minPendingIdle = %s, want 3*MaxRuleTimeout = %s", got, 3*MaxRuleTimeout)
	}

	// Unset ReclaimInterval → DefaultReclaimInterval (1m).
	if got := c.reclaimInterval; got != DefaultReclaimInterval {
		t.Errorf("reclaimInterval = %s, want DefaultReclaimInterval = %s", got, DefaultReclaimInterval)
	}
}

// TestMaxRuleTimeoutMatchesFunctionMaxTimeout pins the cross-package invariant
// that stream.MaxRuleTimeout and function.MaxTimeout are the same value. The
// stream layer derives its reclaim threshold from this cap, so a divergence
// would break the reclaim-while-in-flight guard.
func TestMaxRuleTimeoutMatchesFunctionMaxTimeout(t *testing.T) {
	if MaxRuleTimeout != function.MaxTimeout {
		t.Errorf("stream.MaxRuleTimeout = %s, function.MaxTimeout = %s; they must match", MaxRuleTimeout, function.MaxTimeout)
	}
}
