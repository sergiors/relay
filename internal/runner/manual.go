package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"relay/internal/app"
	eventmatch "relay/internal/event"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// InvokeApp executes every event rule of the named app whose pattern
// matches event, in declaration order, and returns the number of handlers
// invoked together with a concise error describing any failures. It is the
// synchronous manual-invocation primitive behind `relay app invoke`,
// driven by the worker over its query socket against the LIVE runner/runtime
// pool.
//
// It deliberately reuses the event execution path: the current registry
// snapshot, the worker-global + per-app concurrency slots, the rule
// timeout (capped at the configured maximum exactly like Handle), per-invocation
// env/secret resolution, the image in-flight reference, the panic boundary, the
// normal runtime executor (Manager.Execute), and the handler success/failure
// execution metrics. It deliberately does NOT reuse the broker lifecycle: it
// never consults stream.InvocationState, never claims or counts the event
// classification counters (events_received/matched/unmatched, and not
// app_events_matched_total), never schedules a retry, and never
// dead-letters. A manual invocation is an operator action, not a stream
// delivery, so no broker state is written.
//
// Failure semantics mirror Handle's aggregate behavior where it is sensible: a
// failure in one matching handler does NOT prevent the later matching handlers
// from running, and the returned error is the first failure after every matching
// handler has been attempted. Matching zero rules is not an error: it returns
// (0, nil) so the caller renders "no matching handlers".
//
// An app absent from the current registry returns ErrAppNotFound; a
// registered but unrunnable app (its image could not be built) returns
// ErrAppUnavailable. Both are wrapped with the app name so the socket
// can map them onto stable wire codes.
//
// Tracing: the worker-side root operation span is opened BEFORE registry and
// match validation, so an invalid app, an unavailable app, or a
// no-match invocation is still traced (with its error recorded) without changing
// any return semantics.
func (r *Runner) InvokeApp(
	ctx context.Context, name string, event map[string]any,
) (count int, err error) {
	// A worker-side root operation span around the whole manual invocation, so
	// an operator `relay app invoke` is traced on the worker without the
	// CLI carrying any trace context. It is a NEW root regardless of the
	// caller's context; every matching rule's function.invoke runs as its child
	// (the root span's context is the base for each rule's invokeCtx). It is
	// opened before validation and finalized by the deferred finalizer on every
	// return path (success, validation error, or handler failure).
	opCtx, opSpan := startManualInvokeSpan(ctx, name)
	ctx = opCtx
	defer func() { finishOperationSpan(opSpan, err) }()

	pf, pin := r.reg.getByNamePinned(name)
	if pin != nil {
		defer pin.Release()
	}
	if pf == nil {
		return 0, fmt.Errorf("%w: %q", ErrAppNotFound, name)
	}
	if pf.Prepared() == nil {
		return 0, fmt.Errorf("%w: %q", ErrAppUnavailable, name)
	}

	// Matching is pure and in declaration order. An app with no matching
	// rule is a successful no-op.
	rules := eventmatch.MatchingEventRules(pf.fn.Template.Events, event)
	if len(rules) == 0 {
		return 0, nil
	}

	// Marshal the supplied event once for every matching rule. A marshal
	// failure is a programming error in the caller's decoded value; report it
	// rather than executing a handler with a corrupt payload.
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("app %q: marshal event: %w", name, err)
	}
	eventID, eventName := eventFields(event)

	var firstErr error
	for _, rule := range rules {
		// Cap the rule timeout at the configured maximum (defense in depth;
		// template validation enforces the cap at load), exactly like Handle.
		timeout := rule.Timeout
		if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
			timeout = cap
		}

		// Reserve the worker-global and per-app concurrency slots, exactly
		// like the event path. A slot timeout is not an execution: record the
		// first such error and keep trying the later rules (which may themselves
		// be blocked, in which case they are reported the same way).
		releaseSlots, _ := r.reserveSlots(ctx, name, pf.fn.Template.Concurrency)
		if releaseSlots == nil {
			r.log.Warn("App invoke: concurrency slot wait timed out",
				"app", name,
				"handler", rule.Handler,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("app %q handler %q: concurrency slot wait timed out", name, rule.Handler)
			}
			continue
		}

		// Pre-admit the worker-global warm-container budget before claiming an
		// attempt. A manual invocation is an operator action, not a stream
		// delivery, so a saturated budget is reported as an error (no pending
		// semantics) and the later rules are still tried.
		warmCtx, warmOK := r.admitWarm(ctx)
		if !warmOK {
			releaseSlots()
			r.log.Warn("App invoke: warm-container budget saturated",
				"app", name,
				"handler", rule.Handler,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("app %q handler %q: warm-container budget saturated", name, rule.Handler)
			}
			continue
		}

		err := func() error {
			defer releaseSlots()
			defer runtime.WarmPermitFrom(warmCtx).Release()

			// The handler attempt is about to begin: this is the
			// last_execution_at attribution point, exactly as Handle's rule loop
			// and InvokeHandler stamp it. A slot timeout above never reaches here
			// (it is not an execution).
			r.metrics.SetAppTimestamp(name, metrics.AppTimestampExecution, time.Now().Unix())

			extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
			if err != nil {
				r.recordHandlerFailure(name, rule.Handler, 0)
				return fmt.Errorf("app %q handler %q: %w", name, rule.Handler, err)
			}

			invokeCtx, cancel := context.WithTimeout(warmCtx, timeout)
			// Stamp the invocation's diagnostic metadata so the execution
			// container carries its owner and identity labels. The type is the
			// event one-shot type: a manual invocation runs an event rule against
			// an operator-supplied event, so it belongs to the same execution
			// population (and is swept by hostname if the worker dies mid-call).
			// MessageID is empty: there is no stream message.
			invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
				Type:      runtime.ContainerTypeEvent,
				App:       name,
				Handler:   rule.Handler,
				EventID:   eventID,
				EventName: eventName,
				Hostname:  r.hostname,
				Image:     toImage(pf),
			})

			start := time.Now()
			panicked, panicValue, err := r.runInvocation(
				pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv,
				pin, invocationTrace{},
			)
			elapsed := time.Since(start)
			if panicked {
				r.log.Error("App invoke: handler PANICKED",
					"app", name,
					"handler", rule.Handler,
					"panic_value", fmt.Sprintf("%v", panicValue),
					"stack", string(debug.Stack()),
				)
			}
			if warmSaturated(err) {
				// Backpressure, not a handler failure: a manual invocation is
				// synchronous, so report it without counting a handler failure.
				r.log.Warn("App invoke: warm-container budget saturated",
					"app", name,
					"handler", rule.Handler,
				)
				return fmt.Errorf("app %q handler %q: warm-container budget saturated", name, rule.Handler)
			}
			if err != nil {
				r.recordHandlerFailure(name, rule.Handler, elapsed)
				r.log.Warn("App invoke: handler execution failed",
					"app", name,
					"handler", rule.Handler,
					"duration", elapsed,
					"reason", err,
				)
				return fmt.Errorf("app %q handler %q: %w", name, rule.Handler, err)
			}
			r.recordHandlerSuccess(name, rule.Handler, elapsed)
			r.log.Info("App invoke: handler executed",
				"app", name,
				"handler", rule.Handler,
				"duration", elapsed,
			)
			return nil
		}()
		// Count every handler whose execution was attempted, including a failed
		// attempt: the count answers "how many handlers ran", and the caller
		// only prints it when there is no error anyway.
		count++
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return count, firstErr
	}
	return count, nil
}

// ReplayDLQ re-executes the exact app/handler recorded by one DLQ entry
// against the CURRENT registry, for the `relay dlq replay` command. It is the
// DLQ counterpart of InvokeApp: an operator action against the LIVE runner,
// never a stream delivery.
//
// It validates the current configuration and then delegates to InvokeHandler:
//
//   - an app absent from the current registry returns ErrAppNotFound;
//   - a registered but unrunnable app (image build failed) returns
//     ErrAppUnavailable;
//   - a handler no longer present in the app's current template (its event
//     rules or schedules) returns ErrHandlerNotFound — the entry is retained and
//     the operator sees that the configuration changed.
//
// The delegation runs with a context carrying NO stream.InvocationState (an
// explicit opt-out, so it holds even if the caller's context came from the stream
// delivery path), so InvokeHandler takes its state-free single-attempt path:
// exactly one synchronous execution of the named handler (never event matching,
// so no other rule can run), against the app's current runtime, env/secrets
// resolution, concurrency slots, and timeout resolution. It writes NO broker
// state — no invocation-state hash, no event classification counters, and no
// retry/DLQ counters (recordFailure is only reached on the state-carrying path).
// It DOES record the normal handler success/failure execution metrics, exactly as
// a manual invocation does, because those are execution outcomes rather than
// broker state.
//
// The handler timeout follows InvokeHandler's current resolution: the matching
// schedule entry's timeout when the handler has one, otherwise the CURRENT event
// rule's timeout for that exact handler, otherwise the app default — always
// capped by the configured maximum. Resolving the event rule is by exact handler
// string, never event matching, so the replay still runs exactly one handler.
// event is the entry's original payload, replayed verbatim.
//
// Tracing: a `dlq.replay` span wraps the whole replay as a NEW ROOT worker
// operation (independent of the caller's context), carrying a span link to the
// original failed invocation when lineage is supplied (the DLQ entry's optional
// compact trace reference, forwarded by the control path — never embedded in the
// event JSON). The replayed `function.invoke` runs as its child. An empty or
// malformed lineage simply omits the link; the replay still runs. The operation
// span is opened BEFORE registry/handler validation, so a removed app or
// handler is still traced (with its error recorded) without changing any return
// semantics.
func (r *Runner) ReplayDLQ(ctx context.Context, fnName, handler string, event []byte, lineage string) (err error) {
	// The replay operation is a new root worker span; its child function.invoke
	// (started via InvokeHandler below) inherits this context. It is opened
	// before validation and records the terminal outcome through the deferred
	// finalizer on every return path.
	opCtx, opSpan := startReplaySpan(ctx, fnName, handler, lineage)
	defer func() { finishOperationSpan(opSpan, err) }()

	pf := r.reg.GetByName(fnName)
	if pf == nil {
		return fmt.Errorf("%w: %q", ErrAppNotFound, fnName)
	}
	if pf.Prepared() == nil {
		return fmt.Errorf("%w: %q", ErrAppUnavailable, fnName)
	}
	if !templateHasHandler(pf.fn.Template, handler) {
		return fmt.Errorf("%w: app %q handler %q", ErrHandlerNotFound, fnName, handler)
	}
	// Strip any inherited invocation state and run the state-free path: no
	// TryStart/complete/retry/exhaustion, and no DLQ accounting. The opt-out
	// makes the guarantee explicit even when the caller's context came from the
	// stream delivery path.
	return r.InvokeHandler(stream.WithoutInvocationState(opCtx), "", fnName, "", handler, event)
}

// templateHasHandler reports whether handler is present in the template's
// current event rules or schedules. A DLQ entry attributes an exhausted
// invocation that came from one of those, so either counts as "still
// configured". A nil template has no handlers.
func templateHasHandler(tmpl *app.Template, handler string) bool {
	if tmpl == nil {
		return false
	}
	for _, rule := range tmpl.Events {
		if rule.Handler == handler {
			return true
		}
	}
	for _, sch := range tmpl.Schedules {
		if sch.Handler == handler {
			return true
		}
	}
	return false
}
