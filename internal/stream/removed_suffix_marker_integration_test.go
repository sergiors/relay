//go:build integration

// This file pins the removed read-only legacy exhaustion suffix (":dlq") at the
// real Redis invocation-hash seam the stream validates claims on: no hidden
// legacy read path parses it, and the delivery becomes eligible again.
package stream

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/testutil"
)

// TestIntegrationAtomicRemovedDLQSuffixMarkerIsNotTerminal pins the removed
// read-only legacy suffix end to end on the real invocation hash: a marker
// still carrying the old ":dlq" exhaustion record is NOT parsed as the
// retained claim — not by the pre-persisting validation read and not as a
// terminal state — with no hidden legacy read path left anywhere.
func TestIntegrationAtomicRemovedDLQSuffixMarkerIsNotTerminal(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	// Given: an invocation hash holding the legacy-suffixed exhaustion marker.
	const legacy = "exhausted:2:deadbeef:dlq"
	seedMarker(t, cli, stream, group, msgID, "fn/h", legacy)

	// Then: the pre-persisting validation read (exhaustedClaim) does NOT
	// surface a claim for it, so the stream never treats it as a persisted
	// exhaustion it can skip.
	if claim, ok, err := store.exhaustedClaim(ctx, stream, group, msgID, "fn/h"); err != nil || ok {
		t.Fatalf("legacy-suffix exhaustedClaim = (%+v, %v, %v), want (zero claim, false, nil)", claim, ok, err)
	}

	// And: parse-level grammar for the suffix is invalid (the legacy terminal
	// read is not provided); every neighboring unsuffixed shape still parses.
	if kind, _, n, ok := parseInvocationState(legacy); ok {
		t.Fatalf("parseInvocationState(%q) = kind %v n %d ok, want not-ok", legacy, kind, n)
	}
	if kind, _, n, ok := parseInvocationState("exhausted:2:deadbeef"); !ok || kind != kindExhausted || n != 2 {
		t.Fatalf("parseInvocationState(exhausted:2:deadbeef) = kind %v n %d ok %v, want the canonical exhaustion", kind, n, ok)
	}

	// Finally: the delivery is ELIGIBLE again — the claim overwrite starts
	// attempt 1 with a fresh token, so the marker is no longer the legacy
	// value afterwards.
	now := time.Now()
	started, claim, wait, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started || claim.Attempt != 1 || claim.Token == "" || wait != 0 {
		t.Fatalf("tryStart over the legacy suffix = (%v, %+v, %v, %v), want (true, attempt 1 with a fresh token, 0, nil)",
			started, claim, wait, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); !strings.HasPrefix(got, "running:") || got == legacy {
		t.Fatalf("marker after the retake = %q, want the fresh running claim", got)
	}
}
