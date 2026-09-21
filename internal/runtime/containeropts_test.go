package runtime

import (
	"errors"
	"testing"

	cerrdefs "github.com/containerd/errdefs"
)

// TestRemoveContainerErrorsTreatedAsBenign verifies the benign-error decision
// for best-effort container removal. A removal is benign when the container will
// not outlive the call: no error, not-found (the daemon already removed it via
// AutoRemove or a previous remove), or conflict/409 (the daemon is removing it
// right now). Any other error is a genuine failure and must NOT be treated as
// benign. Matching is typed (errors.Is against the errdefs sentinels), never on
// the error string.
func TestRemoveContainerErrorsTreatedAsBenign(t *testing.T) {
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

// TestHardenedHostConfigSecurityBaseline pins the complete security/resource
// baseline every Relay container is created with, so the contract is checked
// without a Docker daemon (the integration hardening test still proves the
// daemon applies it). autoRemove is the only field that differs between
// one-shot invocation containers (true) and persistent services (false).
func TestHardenedHostConfigSecurityBaseline(t *testing.T) {
	hc := hardenedHostConfig(true)
	if hc == nil {
		t.Fatal("hardenedHostConfig returned nil")
	}
	if hc.Memory != 128<<20 {
		t.Errorf("memory = %d, want 128 MiB (%d)", hc.Memory, 128<<20)
	}
	if hc.NanoCPUs != 1_000_000_000 {
		t.Errorf("NanoCPUs = %d, want 1 CPU", hc.NanoCPUs)
	}
	if hc.PidsLimit == nil || *hc.PidsLimit != 128 {
		t.Errorf("PidsLimit = %v, want 128", hc.PidsLimit)
	}
	if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
		t.Errorf("CapDrop = %v, want [ALL]", hc.CapDrop)
	}
	if !hc.ReadonlyRootfs {
		t.Error("ReadonlyRootfs = false, want true")
	}
	if got, want := hc.Tmpfs["/tmp"], "rw,nosuid,noexec,size=64m"; got != want {
		t.Errorf("Tmpfs[/tmp] = %q, want %q", got, want)
	}
	if !hc.AutoRemove {
		t.Error("AutoRemove = false for an invocation container, want true")
	}

	// The persistent-service variant differs ONLY in AutoRemove.
	svc := hardenedHostConfig(false)
	if svc.AutoRemove {
		t.Error("AutoRemove = true for a service container, want false (reconciler-owned)")
	}
	if svc.Memory != hc.Memory || svc.NanoCPUs != hc.NanoCPUs ||
		svc.ReadonlyRootfs != hc.ReadonlyRootfs || svc.PidsLimit == nil ||
		*svc.PidsLimit != *hc.PidsLimit {
		t.Errorf("service HostConfig = %+v; want the same baseline as the invocation variant except AutoRemove", svc)
	}
}
