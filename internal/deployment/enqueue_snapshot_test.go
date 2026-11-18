package deployment

import "testing"

// TestEnqueueSnapshotIncludesInvalidApps pins the post-materialization
// enqueue contract: a NEWLY materialized snapshot enqueues the valid
// fingerprints (they need normal reconciler passes), the desired-but-invalid
// names (their pending rules must be published too — a parseable invalid app
// is excluded from the startup app list and gets no rules otherwise), and the
// removed names (their removal passes).
func TestEnqueueSnapshotIncludesInvalidApps(t *testing.T) {
	c := New(Config{})
	var enqueued []string

	c.enqueueSnapshot(func(name string) {
		enqueued = append(enqueued, name)
	}, Snapshot{
		Fingerprints: map[string]string{"ok": "fp-ok"},
		Invalid:      map[string]string{"broken": "template invalid"},
		Removed:      []string{"gone"},
	})

	got := map[string]bool{}
	for _, name := range enqueued {
		got[name] = true
	}
	for _, want := range []string{"ok", "broken", "gone"} {
		if !got[want] {
			t.Fatalf("enqueued = %v, want %q included", enqueued, want)
		}
	}
	if len(enqueued) != 3 {
		t.Fatalf("enqueued = %v, want exactly the three snapshot names", enqueued)
	}
}
