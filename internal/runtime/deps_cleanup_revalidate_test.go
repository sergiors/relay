package runtime

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"relay/internal/testutil"
)

// revalidateDepImageList renders an image list for the under-lease revalidation
// cases below: one managed dependency image plus, when referenced is non-empty,
// one managed app image naming that dependency.
func revalidateDepImageList(dep string, depKind, referenced string) string {
	var items []string
	switch depKind {
	case "managed":
		items = append(items, `{"RepoTags":["`+dep+`"],"Id":"sha256:dep","Labels":{"relay.type":"dependency","relay.runtime":"python3.14"}}`)
	case "untagged":
		items = append(items, `{"RepoTags":[],"Id":"sha256:dep","Labels":{"relay.type":"dependency","relay.runtime":"python3.14"}}`)
	case "unmanaged":
		items = append(items, `{"RepoTags":["`+dep+`"],"Id":"sha256:dep","Labels":{}}`)
	}
	if referenced != "" {
		items = append(items, `{"RepoTags":["relay-app-demo:0000000000000000"],"Id":"sha256:app","Labels":{"relay.type":"app","relay.dependency":"`+referenced+`"}}`)
	}
	return "[" + strings.Join(items, ",") + "]"
}

// TestDependencyGCRevalidatesCandidateUnderLease pins the two-part revalidation
// performed after the cross-process exclusive lease is acquired. Confirm only
// the referenced set is not enough: another GC pass may have removed or untagged
// the target while this pass waited for the lease, in which case a second
// ImageRemove would report (and count) a no-op removal. A target that is no
// longer an eligible tagged managed dependency candidate must be skipped with no
// DELETE, no removed count, and the exclusive lease released.
func TestDependencyGCRevalidatesCandidateUnderLease(t *testing.T) {
	const dep = "relay-dep-cccccccccccccccc"

	cases := []struct {
		name        string
		secondList  string
		wantRemoved int
		wantDeletes int
	}{
		{
			name:        "removed while waiting for the lease",
			secondList:  "[]",
			wantRemoved: 0,
			wantDeletes: 0,
		},
		{
			name:        "untagged while waiting for the lease",
			secondList:  revalidateDepImageList(dep, "untagged", ""),
			wantRemoved: 0,
			wantDeletes: 0,
		},
		{
			name:        "no longer a managed dependency image",
			secondList:  revalidateDepImageList(dep, "unmanaged", ""),
			wantRemoved: 0,
			wantDeletes: 0,
		},
		{
			name:        "still a candidate but now referenced",
			secondList:  revalidateDepImageList(dep, "managed", dep),
			wantRemoved: 0,
			wantDeletes: 0,
		},
		{
			name:        "still a candidate and unreferenced",
			secondList:  revalidateDepImageList(dep, "managed", ""),
			wantRemoved: 1,
			wantDeletes: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var listings, deletes int
			cli := newScriptedDockerClient(t,
				dockerRoute{
					method: http.MethodGet,
					path:   "/images/json",
					serve: func(*http.Request) (int, string) {
						listings++
						if listings == 1 {
							// The initial candidate selection sees the layer.
							return 0, revalidateDepImageList(dep, "managed", "")
						}
						return 0, tc.secondList
					},
				},
				dockerRoute{
					method:  http.MethodDelete,
					path:    "/images/",
					body:    "[]",
					onMatch: func() { deletes++ },
				},
			)
			coord := newInProcessDepCoordinator()
			m := &Manager{cli: cli, log: testutil.DiscardLogger(), depCoord: coord}

			removed, err := m.CleanupUnusedDependencies(context.Background())
			if err != nil {
				t.Fatalf("CleanupUnusedDependencies: %v", err)
			}
			if removed != tc.wantRemoved {
				t.Fatalf("removed = %d, want %d", removed, tc.wantRemoved)
			}
			if deletes != tc.wantDeletes {
				t.Fatalf("DELETE calls = %d, want %d", deletes, tc.wantDeletes)
			}
			// Exactly the initial selection plus the under-lease revalidation; a
			// skipped candidate must never reach the removal path.
			if listings != 2 {
				t.Fatalf("image listings = %d, want initial selection + one revalidation", listings)
			}
			// The exclusive protection must be released on every outcome, so a
			// later pass (or another Manager sharing the coordinator) can decide
			// again.
			lease, acquired, err := coord.TryAcquireGC(context.Background(), dep)
			if err != nil {
				t.Fatalf("TryAcquireGC after the pass: %v", err)
			}
			if !acquired {
				t.Fatal("exclusive GC protection was not released after the pass")
			}
			if err := lease.Release(); err != nil {
				t.Fatalf("release probe lease: %v", err)
			}
		})
	}
}
