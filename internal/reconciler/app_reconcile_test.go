package reconciler

import (
	"testing"

	"relay/internal/app"
)

// TestServiceDesiredConfigurationChanged pins that an ephemeral-only service
// edit is detected as a desired-configuration change. Ephemeral destinations
// are deliberately excluded from the image fingerprint, so this comparison is
// the only coordinator-level signal that such an edit must supersede pending
// state instead of being coalesced as a redundant observation.
func TestServiceDesiredConfigurationChanged(t *testing.T) {
	service := func(ephemeral ...string) app.Service {
		svc := app.Service{
			Name: "cache", Entrypoint: "service.js", Port: 8080, Replicas: 1,
		}
		if len(ephemeral) > 0 {
			svc.Ephemeral = ephemeral
		}
		return svc
	}
	base := serviceTemplate("nodejs24.x", service("/run/cache"))

	for _, tc := range []struct {
		name string
		next *app.Template
		want bool
	}{
		{
			name: "unchanged ephemeral is not a change",
			next: serviceTemplate("nodejs24.x", service("/run/cache")),
			want: false,
		},
		{
			name: "changed ephemeral destination is a change",
			next: serviceTemplate("nodejs24.x", service("/var/cache/nginx")),
			want: true,
		},
		{
			name: "added ephemeral destination is a change",
			next: serviceTemplate("nodejs24.x",
				service("/run/cache", "/var/cache/nginx")),
			want: true,
		},
		{
			name: "removed ephemeral list is a change",
			next: serviceTemplate("nodejs24.x", service()),
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := serviceDesiredConfigurationChanged(base, tc.next); got != tc.want {
				t.Fatalf("serviceDesiredConfigurationChanged = %v, want %v",
					got, tc.want)
			}
		})
	}
}
