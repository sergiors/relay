package stream

import (
	"testing"

	"github.com/redis/go-redis/v9"

	"relay/internal/function"
)

// TestNewConsumerDerivedDefaults pins the default derivation for the reclaim
// MinPendingIdle threshold, the reclaim interval, and the bounded local buffer
// (MaxBufferedEvents). These defaults are what pace message-level retry/recovery
// and backpressure, so they are locked down here.
func TestNewConsumerDerivedDefaults(t *testing.T) {
	// A client is required for the consumer to be constructed; the address is
	// never dialed by NewConsumer.
	cfg := func() ConsumerConfig {
		return ConsumerConfig{Client: redis.NewClient(&redis.Options{Addr: "localhost:6379"})}
	}

	// Unset MinPendingIdle → DefaultReclaimInterval (1m).
	c := NewConsumer(cfg())
	if got := c.minPendingIdle; got != DefaultReclaimInterval {
		t.Errorf("minPendingIdle = %s, want DefaultReclaimInterval = %s", got, DefaultReclaimInterval)
	}

	// Unset ReclaimInterval → DefaultReclaimInterval (1m).
	if got := c.reclaimInterval; got != DefaultReclaimInterval {
		t.Errorf("reclaimInterval = %s, want DefaultReclaimInterval = %s", got, DefaultReclaimInterval)
	}

	// Unset MaxBufferedEvents → DefaultMaxBufferedEvents (16).
	if got := c.capacity; got != DefaultMaxBufferedEvents {
		t.Errorf("capacity = %d, want DefaultMaxBufferedEvents = %d", got, DefaultMaxBufferedEvents)
	}

	// A zero or negative MaxBufferedEvents also falls back to the default (a
	// value of 0 must not mean unbounded).
	for _, bad := range []int{0, -1} {
		badCfg := cfg()
		badCfg.MaxBufferedEvents = bad
		if got := NewConsumer(badCfg).capacity; got != DefaultMaxBufferedEvents {
			t.Errorf("MaxBufferedEvents=%d capacity = %d, want default %d", bad, got, DefaultMaxBufferedEvents)
		}
	}

	// An explicit positive value is honored.
	okCfg := cfg()
	okCfg.MaxBufferedEvents = 4
	if got := NewConsumer(okCfg).capacity; got != 4 {
		t.Errorf("explicit MaxBufferedEvents=4 capacity = %d, want 4", got)
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
