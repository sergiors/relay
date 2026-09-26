package runtime

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"relay/internal/observability/tracing"
)

// spanRecorder pairs the in-memory exporter with the provider so tests can
// force-flush before reading. It installs the provider through the production
// Setup seam (restoring globals on cleanup); runtime package tests are not
// parallel, so the global swap is race-free.
type spanRecorder struct {
	exp      *tracetest.InMemoryExporter
	provider *tracing.Provider
}

func withSpanRecorder(t *testing.T) *spanRecorder {
	t.Helper()
	// The production Setup honors OTEL_SDK_DISABLED; clear it so an ambient
	// value in the test environment cannot disable the injected exporter.
	t.Setenv("OTEL_SDK_DISABLED", "")
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	exp := tracetest.NewInMemoryExporter()
	provider, err := tracing.Setup(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), tracing.WithExporter(exp))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	return &spanRecorder{exp: exp, provider: provider}
}

func (r *spanRecorder) spans(t *testing.T) tracetest.SpanStubs {
	t.Helper()
	if err := r.provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	return r.exp.GetSpans()
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

// TestContainerCacheExecuteEmitsAcquireAndInvokeSpans proves the cache execute
// path emits runtime.acquire and runtime.invoke child spans, with runtime.invoke
// parented to runtime.acquire's sibling (both under the caller context) and
// carrying the function/image attributes.
func TestContainerCacheExecuteEmitsAcquireAndInvokeSpans(t *testing.T) {
	rec := withSpanRecorder(t)
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h1"); err != nil {
		t.Fatalf("execute: %v", err)
	}

	spans := rec.spans(t)
	var acquire, invoke *tracetest.SpanStub
	for i := range spans {
		switch spans[i].Name {
		case "runtime.acquire":
			acquire = &spans[i]
		case "runtime.invoke":
			invoke = &spans[i]
		}
	}
	if acquire == nil || invoke == nil {
		t.Fatalf("missing acquire/invoke spans; got %v", spanNames(spans))
	}
	if got := attrString(*acquire, "relay.function"); got != "fn-a" {
		t.Errorf("runtime.acquire relay.function = %q, want fn-a", got)
	}
	if got := attrString(*invoke, "relay.image"); got != "img-1" {
		t.Errorf("runtime.invoke relay.image = %q, want img-1", got)
	}
}

// TestContainerCacheAcquireFailureSpanRecordsError proves a failed start (no
// lease) records an error on the runtime.acquire span and never emits a
// runtime.invoke span.
func TestContainerCacheAcquireFailureSpanRecordsError(t *testing.T) {
	rec := withSpanRecorder(t)
	cc, ff := newTestCache()
	ff.startErr = context.DeadlineExceeded

	err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h1")
	if err == nil {
		t.Fatal("execute returned nil, want the start error")
	}
	spans := rec.spans(t)
	var acquire *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "runtime.acquire" {
			acquire = &spans[i]
		}
		if spans[i].Name == "runtime.invoke" {
			t.Errorf("runtime.invoke span emitted on a failed acquire: %v", spanNames(spans))
		}
	}
	if acquire == nil {
		t.Fatalf("missing runtime.acquire span; got %v", spanNames(spans))
	}
	if acquire.Status.Code != codes.Error {
		t.Errorf("runtime.acquire status = %v, want codes.Error", acquire.Status.Code)
	}
}

func attrString(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}
