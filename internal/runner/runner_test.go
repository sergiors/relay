package runner

import (
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
)

// Set and Replace keep the function set sorted by name, so Names() and iteration
// order never depend on the order functions were discovered or swapped in.
func TestRegistryNamesDeterministic(t *testing.T) {
	r := New(nil, log.New(nil, "", 0))
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

// Records invocations without touching Docker, satisfying the runner's local
// executor interface.
type fakeExecutor struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	// Yield inside the invocation so registry swaps can interleave; the runner
	// must keep using its snapshot regardless.
	time.Sleep(time.Millisecond)
	return nil
}

func newFn(t *testing.T, name string) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{Name: name, Template: &function.Template{Runtime: "node24"}},
		&runtime.Prepared{Name: name, Image: "x"},
		&fakeExecutor{},
	)
}

// Runs Handle concurrently with registry swaps under -race and asserts the
// runner neither panics nor errors while the set is being replaced mid-iteration.
func TestHandleVsSwapSnapshotConsistency(t *testing.T) {
	r := New(nil, log.New(nil, "", 0))

	names := []string{"a", "b", "c"}
	var fns []*PreparedFunction
	for _, n := range names {
		fns = append(fns, newFn(t, n))
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
					r.Registry().Replace(names[i], newFn(t, names[i]))
				}
			}
		}()
	}

	var running atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			running.Add(1)
			err := r.Handle(context.Background(), "m", map[string]any{"a": 1})
			running.Add(-1)
			if err != nil {
				t.Errorf("handle returned error: %v", err)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	swappers.Wait()
	<-done

	if swaps.Load() == 0 {
		t.Fatal("expected at least one swap during the test")
	}
}
