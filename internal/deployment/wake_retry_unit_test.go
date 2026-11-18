package deployment

import (
	"testing"
	"time"
)

// TestDefaultCadenceValues pins the three independent coordinator cadences:
// the 60s fallback desired-state poll (the notification path normally beats
// it), the 5s retry cadence used only while work is pending, and the unchanged
// 30s drift backstop.
func TestDefaultCadenceValues(t *testing.T) {
	if DefaultPollInterval != 60*time.Second {
		t.Errorf("DefaultPollInterval = %s, want a 60s fallback poll", DefaultPollInterval)
	}
	if DefaultRetryInterval != 5*time.Second {
		t.Errorf("DefaultRetryInterval = %s, want 5s", DefaultRetryInterval)
	}
	if DefaultDriftInterval != 30*time.Second {
		t.Errorf("DefaultDriftInterval = %s, want 30s (unchanged)", DefaultDriftInterval)
	}
}

// TestNewAppliesAndOverridesCadences pins that a coordinator built without
// overrides runs all three timers at their package defaults, and that each
// override is honored independently of the others.
func TestNewAppliesAndOverridesCadences(t *testing.T) {
	fallback := New(Config{})
	if fallback.poll != DefaultPollInterval ||
		fallback.retry != DefaultRetryInterval ||
		fallback.drift != DefaultDriftInterval {
		t.Fatalf("default cadences = (poll %s, retry %s, drift %s), want (%s, %s, %s)",
			fallback.poll, fallback.retry, fallback.drift,
			DefaultPollInterval, DefaultRetryInterval, DefaultDriftInterval)
	}

	custom := New(Config{
		PollInterval:  7 * time.Second,
		RetryInterval: 3 * time.Second,
		DriftInterval: 9 * time.Second,
	})
	if custom.poll != 7*time.Second || custom.retry != 3*time.Second ||
		custom.drift != 9*time.Second {
		t.Fatalf("override cadences = (poll %s, retry %s, drift %s), want (7s, 3s, 9s)",
			custom.poll, custom.retry, custom.drift)
	}
}

// TestWakeAndKickCoalesceWithoutDowngradingForce pins the wake-signal
// contract: normal wakes queue exactly one non-forcing pass (coalescing while
// the loop is still busy with the active sync, never blocking, never
// duplicating), a forced drift request marks that pending pass forced, and a
// hint coalesced into a forced request can never downgrade it — while a hint
// that arrives first is upgraded when drift then asks for a forced pass.
func TestWakeAndKickCoalesceWithoutDowngradingForce(t *testing.T) {
	var nilCoordinator *Coordinator
	nilCoordinator.Wake()
	nilCoordinator.Kick()

	c := New(Config{})
	forcePending := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.forceWake
	}

	// A normal wake queues one non-forcing signal.
	c.Wake()
	if got := len(c.kick); got != 1 {
		t.Fatalf("pending wake signals = %d, want 1", got)
	}
	if forcePending() {
		t.Fatal("Wake marked the pending pass forced; it must stay non-forcing")
	}
	// A second wake while the loop has not drained the channel coalesces into
	// the same pending pass — the state of a wake arriving during an active
	// sync: represented, not lost, and never queued twice.
	c.Wake()
	if got := len(c.kick); got != 1 {
		t.Fatalf("pending wake signals after a coalesced wake = %d, want 1", got)
	}
	<-c.kick

	// A forced drift request marks its pending pass forced...
	c.Kick()
	if got := len(c.kick); got != 1 || !forcePending() {
		t.Fatalf("after Kick: signals = %d forced = %v, want one forced pass",
			got, forcePending())
	}
	// ...and a database hint coalesced into it cannot downgrade the force.
	c.Wake()
	if got := len(c.kick); got != 1 {
		t.Fatalf("pending signals after the coalesced hint = %d, want 1", got)
	}
	if !forcePending() {
		t.Fatal("a coalesced hint downgraded a forced drift pass")
	}
	<-c.kick

	// Model the loop consuming that pass, then hint-first: drift arriving
	// later upgrades the pending pass instead of being dropped.
	c.mu.Lock()
	c.forceWake = false
	c.mu.Unlock()
	c.Wake()
	c.Kick()
	if got := len(c.kick); got != 1 || !forcePending() {
		t.Fatalf("after hint-then-kick: signals = %d forced = %v, want one forced pass",
			got, forcePending())
	}
}
