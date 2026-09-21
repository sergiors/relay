package runner

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/testutil"
)

// Set and Replace keep the function set sorted by name, so Names() and iteration
// order never depend on the order functions were discovered or swapped in.
func TestRegistryNamesDeterministic(t *testing.T) {
	r := New(nil, testutil.DiscardLogger())
	reg := r.Registry()

	// Inserting "zeta" before "alpha": the slice must come out sorted anyway.
	reg.Set([]*PreparedFunction{newFn(t, "zeta"), newFn(t, "alpha")})
	reg.Replace("mid", newFn(t, "mid"))
	reg.Replace("beta", newFn(t, "beta"))

	if got := strings.Join(reg.Names(), ","); got != "alpha,beta,mid,zeta" {
		t.Fatalf("Names order not deterministic: got %q", got)
	}

	// Removal keeps the remainder sorted.
	reg.Replace("alpha", nil)
	if got := strings.Join(reg.Names(), ","); got != "beta,mid,zeta" {
		t.Fatalf("Names order after removal: got %q", got)
	}
}

// TestRegistryReplaceAbsentNameAdds covers Replace's append path: replacing a
// name that is not present adds the entry (sorted) and GetByName finds it.
func TestRegistryReplaceAbsentNameAdds(t *testing.T) {
	reg := New(nil, testutil.DiscardLogger()).Registry()
	reg.Set(nil)

	reg.Replace("new", newFn(t, "new"))

	if got := reg.GetByName("new"); got == nil || got.Name() != "new" {
		t.Fatalf("GetByName(new) = %v, want the added function", got)
	}
	if got := strings.Join(reg.Names(), ","); got != "new" {
		t.Fatalf("Names = %q, want [new]", got)
	}
}

// Runs Handle concurrently with registry swaps under -race and asserts the
// runner neither panics nor errors while the set is being replaced mid-iteration.
func TestHandleVsSwapSnapshotConsistency(t *testing.T) {
	r := New(nil, testutil.DiscardLogger())

	names := []string{"a", "b", "c"}
	var fns []*PreparedFunction
	for _, n := range names {
		fns = append(fns, alwaysMatchFn(t, n, &countingExecutor{hold: time.Millisecond}))
	}
	r.Registry().Set(fns)

	var swaps atomic.Int64
	stop := make(chan struct{})

	var swappers sync.WaitGroup
	for w := 0; w < 4; w++ {
		swappers.Add(1)
		go func() {
			defer swappers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Replace a single entry; occasionally mark it unavailable to
				// exercise the skip path.
				i := int(swaps.Add(1)) % len(names)
				if i%2 == 0 {
					r.Registry().Replace(names[i], NewUnavailable(function.Function{Name: names[i]}))
				} else {
					r.Registry().Replace(names[i], alwaysMatchFn(t, names[i], &countingExecutor{hold: time.Millisecond}))
				}
			}
		}()
	}

	// Bounded iteration count instead of a fixed soak: the loop count is
	// independent of wall-clock time while still giving the swap goroutines
	// ample opportunity to interleave under -race.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			if err := r.Handle(context.Background(), "m", map[string]any{"a": 1}); err != nil {
				t.Errorf("handle returned error: %v", err)
			}
		}
	}()

	<-done
	close(stop)
	swappers.Wait()

	if swaps.Load() == 0 {
		t.Fatal("expected at least one swap during the test")
	}
}
