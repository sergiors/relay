package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
)

// Runner orchestrates the flow: for each decoded event it evaluates all loaded
// functions and, for every matching rule, executes the corresponding handler in
// a container. It contains no Redis, matcher, or docker details; execution is
// delegated to the runtime executor.
type Runner struct {
	functions []*PreparedFunction
	timeout   time.Duration
	log       *log.Logger
}

// PreparedFunction pairs a loaded function with its prepared image and the
// executor used to run invocations. A function whose image could not be built
// is marked unavailable and skipped during execution.
type PreparedFunction struct {
	fn        function.Function
	prepared  *runtime.Prepared
	executor  *runtime.Manager
	available bool
}

func NewPrepared(
	fn function.Function,
	prepared *runtime.Prepared,
	executor *runtime.Manager,
) *PreparedFunction {
	return &PreparedFunction{fn: fn, prepared: prepared, executor: executor, available: true}
}

// NewUnavailable wraps a function whose image could not be built so the runner
// can skip it without losing the function's identity.
func NewUnavailable(fn function.Function) *PreparedFunction {
	return &PreparedFunction{fn: fn, available: false}
}

// New creates a Runner over the given prepared functions. timeout bounds each
// individual handler invocation.
func New(prepared []*PreparedFunction, timeout time.Duration, logger *log.Logger) *Runner {
	if logger == nil {
		logger = log.Default()
	}
	return &Runner{functions: prepared, timeout: timeout, log: logger}
}

// Handle evaluates the event against all loaded functions and executes every
// matching rule's handler. It returns nil only when every invocation succeeded
// (or nothing matched); otherwise it returns an error so the stream layer does
// not acknowledge the message.
func (r *Runner) Handle(ctx context.Context, msgID string, event map[string]any) error {
	for _, pf := range r.functions {
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
			invokeCtx, cancel := context.WithTimeout(ctx, r.timeout)
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
