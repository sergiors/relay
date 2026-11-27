//go:build integration

package runtime

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"relay/internal/observability/tracing"
	"relay/internal/testutil"
)

// Real-container managed-OTel context acceptance for the Node engine: ONE
// nodejs26.x invocation image built and driven by the real Manager → Prepare
// → Execute path (production execEnv, production frame encoding, production
// /relay/otel install), proving the incoming W3C trace context is the ACTIVE
// context during the handler module's FIRST lazy evaluation and still active
// across an async boundary inside the reused warm process — and that the same
// warm process accepts a SECOND invocation carrying a different trace.
//
// No collector is required: the assertions are the handler's own log lines
// plus a handler failure message naming the observed context when it does not
// match the frame's expectation, so broken extraction surfaces as an
// immediate Execute error instead of a log-wait timeout.
const nodeOtelContextHandlerSource = `
import { trace, context as otelContext } from "@opentelemetry/api";

function activeTag() {
  const span = trace.getSpan(otelContext.active());
  if (!span) return "root";
  const sc = span.spanContext();
  return sc.traceId + "/" + sc.spanId;
}

// First lazy module evaluation: this module is imported exactly once inside
// the invocation's extracted context (loadHandler runs under the trace
// frame's parent context), so its top-level line is the module-evaluation
// proof — while LAZY_TAG itself is READ again by every per-invocation call.
const LAZY_TAG = activeTag();
console.log("module-load-active=" + LAZY_TAG);

export async function check(event, context) {
  console.log("lazy-active=" + LAZY_TAG);
  await new Promise((resolve) => setTimeout(resolve, 50));
  const afterAwait = activeTag();
  console.log("async-active=" + afterAwait);
  console.log("check-done id=" + event.event_id);
  if (!LAZY_TAG.startsWith(event.lazy_expect + "/")) {
    throw new Error("lazy trace mismatch: active=" + LAZY_TAG +
      " expect=" + event.lazy_expect);
  }
  if (!afterAwait.startsWith(event.expect + "/")) {
    throw new Error("async trace mismatch: active=" + afterAwait +
      " expect=" + event.expect);
  }
}
`

// TestIntegrationNode26ManagedOtelW3CContextActiveAcrossAsyncAndLazyImport
// builds the real managed-OTel Node invocation image and proves, through the
// production execution channel, that:
//
//   - Prepare resolves the managed provider identity and the execution-only
//     provider environment for the Node handler image;
//   - the incoming traceparent is the active context during the handler
//     module's first lazy evaluation (a mismatch throws, failing Execute
//     with the observed context in the message);
//   - the same context is still active after an async boundary; and
//   - the warm container/process serves a second invocation carrying a
//     DIFFERENT trace, with exactly one lazy evaluation overall.
func TestIntegrationNode26ManagedOtelW3CContextActiveAcrossAsyncAndLazyImport(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	appName := "node26-otel-ctx-" + runtime.GOARCH
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-"+appName+":"))

	dir := t.TempDir()
	fn := newAppFromTemplate(t, appName, dir, `
runtime: nodejs26.x
events:
  - handler: handler.check
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.js", nodeOtelContextHandlerSource)

	// A real sampled span context in every execution context: the frame
	// encoder injects its W3C traceparent, so the container receives genuine
	// incoming context exactly as the runner produces it.
	withSpanRecorder(t)

	m, _ := newManager(t)
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare nodejs26.x managed-otel app: %v", err)
	}
	// The Node handler image must be a managed-provider image: a private
	// config identity and the execution-only provider environment (never on
	// the service path) are what make the bootstrap start its context
	// manager and propagators in the first place.
	if prepared.otelConfig == "" {
		t.Fatal("prepared Node invocation image has no managed provider identity")
	}
	if !hasEntry(prepared.execEnv, "OTEL_SERVICE_NAME="+appName) {
		t.Errorf("execEnv = %v, want the injected service default %q",
			prepared.execEnv, "OTEL_SERVICE_NAME="+appName)
	}

	// First invocation: its own trace is the expectation for BOTH the lazy
	// module evaluation and the post-await read.
	runCtx1, span1 := tracing.Start(context.Background(), "function.invoke")
	traceOne := span1.SpanContext().TraceID().String()
	event1 := fmt.Sprintf(
		`{"event_id":"otc-1","status":"COMPLETED","expect":%q,"lazy_expect":%q}`,
		traceOne, traceOne)
	if err := m.Execute(runCtx1, prepared, "handler.check", []byte(event1), nil); err != nil {
		t.Fatalf("the first invocation must observe the incoming W3C context during lazy module evaluation and across await: %v", err)
	}
	span1.End()
	id1 := waitForContainerByLabel(ctx, m.cli, labelApp, fn.Name)
	if id1 == "" {
		t.Fatal("warm execution container not found after the first invocation")
	}

	// Second invocation on the SAME warm process with a DIFFERENT trace: the
	// async read must carry trace two, while the (already evaluated) lazy
	// tag stays trace one — proving one evaluation under invocation one.
	runCtx2, span2 := tracing.Start(context.Background(), "function.invoke")
	traceTwo := span2.SpanContext().TraceID().String()
	if traceTwo == traceOne {
		t.Fatalf("each execution must start its own trace, both are %s", traceOne)
	}
	event2 := fmt.Sprintf(
		`{"event_id":"otc-2","status":"COMPLETED","expect":%q,"lazy_expect":%q}`,
		traceTwo, traceOne)
	if err := m.Execute(runCtx2, prepared, "handler.check", []byte(event2), nil); err != nil {
		t.Fatalf("the warm process must accept a second, different trace: %v", err)
	}
	span2.End()
	id2 := waitForContainerByLabel(ctx, m.cli, labelApp, fn.Name)
	if id2 == "" {
		t.Fatal("warm execution container not found after the second invocation")
	}
	if id2 != id1 {
		t.Errorf("the second trace must run in the SAME warm container: id1=%s id2=%s", id1, id2)
	}

	logs := awaitAppLogsFor(t, ctx, m.cli, fn.Name,
		"module-load-active="+traceOne+"/",
		"lazy-active="+traceOne+"/",
		"async-active="+traceOne+"/",
		"check-done id=otc-1",
		"async-active="+traceTwo+"/",
		"check-done id=otc-2",
	)
	// The module was evaluated exactly ONCE — under the FIRST trace — proven
	// by a single top-level module-load line; the per-invocation lazy lines
	// may repeat (LAZY_TAG is re-read from the cached module every call).
	if n := strings.Count(logs, "module-load-active="); n != 1 {
		t.Errorf("module-load-active lines = %d, want exactly 1 (one module evaluation under the first trace); logs:\n%s",
			n, logs)
	}
}
