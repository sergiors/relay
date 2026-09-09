package stream

import (
	"math/rand/v2"
	"sync"
	"time"
)

// defaultBackoffTable is the fixed, bounded retry progression. After the last
// entry the delay stays capped at the final value forever.
var defaultBackoffTable = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	15 * time.Second,
	30 * time.Second,
}

// defaultJitter scales a base delay by a factor in [0.8, 1.2). Jitter exists so
// that replicas observing the same outage do not retry in lockstep: without it,
// synchronized retries would re-hit Redis simultaneously the moment it returns.
func defaultJitter(base float64) float64 {
	return base * (0.8 + 0.4*rand.Float64())
}

// backoff implements bounded exponential backoff with jitter. It is safe for
// concurrent use. Waits are context-aware at the call site (not here) so a
// pending retry can be interrupted promptly on shutdown.
type backoff struct {
	mu     sync.Mutex
	step   int
	table  []time.Duration
	jitter func(float64) float64 // injectable for tests; default rand-based
}

func newBackoff(table []time.Duration, jitter func(float64) float64) *backoff {
	if len(table) == 0 {
		table = defaultBackoffTable
	}
	if jitter == nil {
		jitter = defaultJitter
	}
	return &backoff{table: table, jitter: jitter}
}

// next returns the current table value (jittered) and advances the step,
// capped at the final table entry.
func (b *backoff) next() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	base := b.table[b.step]
	if b.step < len(b.table)-1 {
		b.step++
	}
	return time.Duration(b.jitter(float64(base)))
}

// peek returns the current table value without advancing the step. Used by the
// recovery loop, which is paced by its own ticker and must not advance the
// main-loop backoff.
func (b *backoff) peek() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.table[b.step]
}

func (b *backoff) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.step = 0
}
