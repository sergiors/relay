package worker

import (
	"sync"
	"testing"
	"time"

	"relay/internal/state"
)

// The worker converts persisted image selections into the runtime Manager's
// immutable startup snapshot at the package boundary. state must never import
// runtime, so the narrow conversion lives here.

// TestToRuntimeImageSelectionsNilStaysNil pins the no-selected-rows startup
// state: an absent selection read is forwarded as nil, preserving the runtime
// Manager's exact historical (unpinned) construction.
func TestToRuntimeImageSelectionsNilStaysNil(t *testing.T) {
	if got := toRuntimeImageSelections(nil); got != nil {
		t.Fatalf("nil selections converted to %+v, want nil", got)
	}
	if got := toRuntimeImageSelections([]state.RuntimeImageSelection{}); got != nil {
		t.Fatalf("empty selections converted to %+v, want nil", got)
	}
}

// TestToRuntimeImageSelectionsConvertsFields pins the field-for-field
// conversion of the startup read: RuntimeID, SourceImage, Digest, Generation,
// and SelectedAt survive verbatim so the runtime layer validates exactly the
// persisted row.
func TestToRuntimeImageSelectionsConvertsFields(t *testing.T) {
	selectedAt := time.Date(2026, 11, 6, 8, 30, 0, 0, time.UTC).Format(time.RFC3339)
	rows := []state.RuntimeImageSelection{
		{RuntimeID: "python3.14", SourceImage: "python:3.14-slim",
			Digest: "sha256:" + repeatWorkerHex('a'), Generation: 3, SelectedAt: selectedAt},
		{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine",
			Digest: "sha256:" + repeatWorkerHex('b'), Generation: 1, SelectedAt: selectedAt},
	}

	got := toRuntimeImageSelections(rows)
	if len(got) != len(rows) {
		t.Fatalf("converted %d selections, want %d", len(got), len(rows))
	}
	for i, want := range rows {
		if got[i].RuntimeID != want.RuntimeID || got[i].SourceImage != want.SourceImage ||
			got[i].Digest != want.Digest || got[i].Generation != want.Generation ||
			got[i].SelectedAt != want.SelectedAt {
			t.Fatalf("selection %d = %+v, want %+v", i, got[i], want)
		}
	}

	// The conversion must not alias the caller's slice: a later mutation of its
	// rows cannot retroactively change what startup provided.
	rows[0].Digest = "sha256:" + repeatWorkerHex('c')
	if got[0].Digest == rows[0].Digest {
		t.Fatal("toRuntimeImageSelections aliased the state rows instead of copying")
	}
}

// TestToRuntimeImageSelectionsIsConcurrencySafe is a -race guard: the worker
// owns the read on the startup goroutine; the conversion (a copy) must be safe
// to exercise concurrently without shared mutable state.
func TestToRuntimeImageSelectionsIsConcurrencySafe(t *testing.T) {
	selectedAt := time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC).Format(time.RFC3339)
	rows := []state.RuntimeImageSelection{
		{RuntimeID: "python3.14", SourceImage: "python:3.14-slim",
			Digest: "sha256:" + repeatWorkerHex('d'), Generation: 2, SelectedAt: selectedAt},
	}
	want := rows[0].Digest

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := toRuntimeImageSelections(rows)
			if len(got) != 1 || got[0].Digest != want || got[0].RuntimeID != "python3.14" {
				t.Errorf("concurrent conversion = %+v, want the stable row", got)
			}
		}()
	}
	wg.Wait()
}

// repeatWorkerHex renders 64 repetitions of a lowercase hex character for
// shape-valid test digests.
func repeatWorkerHex(c byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = c
	}
	return string(out)
}
