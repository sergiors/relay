package runtime

import (
	"errors"
	"testing"

	cerrdefs "github.com/containerd/errdefs"
)

// TestBenignRemovalErr verifies the benign-error decision for best-effort
// container removal. A removal is benign when the container will not outlive the
// call: no error, not-found (the daemon already removed it via AutoRemove or a
// previous remove), or conflict/409 (the daemon is removing it right now). Any
// other error is a genuine failure and must NOT be treated as benign. Matching
// is typed (errors.Is against the errdefs sentinels), never on the error string.
func TestBenignRemovalErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, true},
		{"not-found", cerrdefs.ErrNotFound, true},
		{"conflict", cerrdefs.ErrConflict, true},
		// A wrapped not-found must still be recognized (errors.Is unwraps).
		{"wrapped not-found", errors.Join(cerrdefs.ErrNotFound, errors.New("no such container")), true},
		{"wrapped conflict", errors.Join(cerrdefs.ErrConflict, errors.New("removal already in progress")), true},
		// Genuine failures are NOT benign.
		{"permission denied", cerrdefs.ErrPermissionDenied, false},
		{"internal", cerrdefs.ErrInternal, false},
		{"invalid argument", cerrdefs.ErrInvalidArgument, false},
		{"plain error", errors.New("permission denied"), false},
		{"unknown", cerrdefs.ErrUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := benignRemovalErr(tc.err); got != tc.want {
				t.Errorf("benignRemovalErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
