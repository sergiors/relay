package deployment

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"relay/internal/app"
)

// templateSourceDrift classifies ReadTemplateSource failures for the drift
// backstop: only a DETERMINISTIC outcome (the authorized app or its template is
// gone, or the path is no longer a valid app directory) says anything about
// the bytes and authorizes a rematerialization kick; every transient read
// failure — permissions, I/O, a read racing a replacement — leaves the decision
// to the reconciler's retry instead.
func TestTemplateSourceDriftClassifiesDeterministicReadFailures(t *testing.T) {
	const name = "fn"
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "missing app directory",
			err:  fmt.Errorf("app %q: %w", name, fs.ErrNotExist),
			want: true,
		},
		{
			name: "missing template",
			err:  fmt.Errorf("%w", app.ErrNotReady),
			want: true,
		},
		{
			name: "symlinked or non-regular template",
			err:  fmt.Errorf("%w: app %q template is not a regular file", app.ErrInvalidPath, name),
			want: true,
		},
		{
			name: "permission denied",
			err:  fmt.Errorf("app %q: read template: %w", name, os.ErrPermission),
			want: false,
		},
		{
			name: "read racing a replacement",
			err:  fmt.Errorf("app %q: template changed during read", name),
			want: false,
		},
		{
			name: "unclassified operational failure",
			err:  errors.New("daemon blip"),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := templateSourceDrift(tc.err); got != tc.want {
				t.Fatalf("templateSourceDrift(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
