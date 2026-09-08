package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	"relay/internal/function"
	"relay/internal/runtime"
)

// Executor is the subset of the runtime Manager that invocations need. It is a
// small interface so Handle and PreparedFunction construction can be exercised
// in tests without a Docker daemon; the concrete *runtime.Manager satisfies it.
type Executor interface {
	Execute(ctx context.Context, prepared *runtime.Prepared, handler string, eventJSON []byte) error
}

// Registry holds the current set of prepared functions behind a lock so swaps
// are atomic: Handle takes one snapshot per call and keeps it for the whole
// invocation, so an in-flight execution never sees a half-replaced set. It is
// exported so the reconciler can swap functions live from its own package.
type Registry struct {
	mu  sync.RWMutex
	fns []*PreparedFunction
}

func (r *Registry) snapshot() []*PreparedFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Copy the slice header only; the underlying elements are immutable once
	// published, so in-flight Handles keep using the snapshot even if Swap runs.
	return append([]*PreparedFunction(nil), r.fns...)
}

// Set replaces the entire registry contents in one atomic step. Functions are
// kept sorted by name so iteration order (and Names) is deterministic.
func (r *Registry) Set(fns []*PreparedFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fns = append([]*PreparedFunction(nil), fns...)
	sortFn(r.fns)
}

// Replace swaps the entry for name, adding it if absent. A nil pf removes the
// entry (used when a function directory disappears). The slice stays name-sorted.
func (r *Registry) Replace(name string, pf *PreparedFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, cur := range r.fns {
		if cur.fn.Name == name {
			if pf == nil {
				r.fns = append(r.fns[:i], r.fns[i+1:]...)
			} else {
				r.fns[i] = pf
			}
			sortFn(r.fns)
			return
		}
	}
	if pf != nil {
		r.fns = append(r.fns, pf)
		sortFn(r.fns)
	}
}

// sortFn orders the registry by function name so Names() and iteration are
// deterministic regardless of the order functions were discovered or swapped in.
func sortFn(fns []*PreparedFunction) {
	sort.Slice(fns, func(i, j int) bool { return fns[i].fn.Name < fns[j].fn.Name })
}

// GetByName returns the prepared function for the given name, or nil if absent,
// without disturbing the running snapshot.
func (r *Registry) GetByName(name string) *PreparedFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pf := range r.fns {
		if pf.fn.Name == name {
			return pf
		}
	}
	return nil
}

// Names returns a snapshot of the currently registered function names, so the
// reconciler can enumerate what is active without pinning the whole slice.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.fns))
	for _, pf := range r.fns {
		out = append(out, pf.fn.Name)
	}
	return out
}

// Runner orchestrates the flow: for each decoded event it evaluates all loaded
// functions and, for every matching rule, executes the corresponding handler in
// a container. It contains no Redis, matcher, or docker details; execution is
// delegated to the runtime executor. The function set is an atomic snapshot so
// it can be reconciled (swapped) live without disrupting in-flight invocations.
type Runner struct {
	reg *Registry
	log *log.Logger
}

// PreparedFunction pairs a loaded function with its prepared image and the
// executor used to run invocations. A function whose image could not be built
// is marked unavailable and skipped during execution.
type PreparedFunction struct {
	fn        function.Function
	prepared  *runtime.Prepared
	executor  Executor
	available bool
}

// Name returns the function name.
func (p *PreparedFunction) Name() string { return p.fn.Name }

// Function returns the underlying loaded function.
func (p *PreparedFunction) Function() function.Function { return p.fn }

// Prepared returns the built image handle, or nil for an unavailable function.
func (p *PreparedFunction) Prepared() *runtime.Prepared { return p.prepared }

func NewPrepared(
	fn function.Function,
	prepared *runtime.Prepared,
	executor Executor,
) *PreparedFunction {
	return &PreparedFunction{fn: fn, prepared: prepared, executor: executor, available: true}
}

// NewUnavailable wraps a function whose image could not be built so the runner
// can skip it without losing the function's identity.
func NewUnavailable(fn function.Function) *PreparedFunction {
	return &PreparedFunction{fn: fn, available: false}
}

// New creates a Runner over the given prepared functions. Each invocation is
// bounded by the matching rule's own timeout.
func New(prepared []*PreparedFunction, logger *log.Logger) *Runner {
	if logger == nil {
		logger = log.Default()
	}
	r := &Runner{reg: &Registry{}, log: logger}
	r.reg.Set(prepared)
	return r
}

// Registry exposes the runner's mutable snapshot set so the reconciler can swap
// functions live without round-tripping through New.
func (r *Runner) Registry() *Registry { return r.reg }

// Handle evaluates the event against all loaded functions and executes every
// matching rule's handler. It returns nil only when every invocation succeeded
// (or nothing matched); otherwise it returns an error so the stream layer does
// not acknowledge the message.
func (r *Runner) Handle(ctx context.Context, msgID string, event map[string]any) error {
	// Take one consistent snapshot for the whole call so a concurrent registry
	// swap mid-execution cannot reorder or drop functions under us.
	for _, pf := range r.reg.snapshot() {
		if !pf.available {
			continue
		}
		rules := pf.fn.Template.MatchingRules(event)
		for _, rule := range rules {
			r.log.Printf("function %q rule %q matched event %q", pf.fn.Name, rule.Handler, msgID)
			eventJSON, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("function %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
			}
			invokeCtx, cancel := context.WithTimeout(ctx, rule.Timeout)
			err = pf.executor.Execute(invokeCtx, pf.prepared, rule.Handler, eventJSON)
			cancel()
			if err != nil {
				r.log.Printf("function %q handler %q execution failed for event %q: %v", pf.fn.Name, rule.Handler, msgID, err)
				return err
			}
			r.log.Printf("function %q handler %q executed for event %q", pf.fn.Name, rule.Handler, msgID)
		}
	}
	return nil
}
