package runtime

import (
	"errors"
	"testing"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"

	"relay/internal/app"
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
// daemon applies it). AutoRemove differs between one-shot invocation containers
// (true) and persistent services (false); a service may additionally carry only
// NET_BIND_SERVICE for a privileged declared port.
func TestHardenedHostConfigSecurityBaseline(t *testing.T) {
	hc := hardenedHostConfig(true, app.DefaultResourceLimits())
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
	if len(hc.SecurityOpt) != 1 || hc.SecurityOpt[0] != noNewPrivileges {
		t.Errorf("SecurityOpt = %v, want [%s]", hc.SecurityOpt, noNewPrivileges)
	}
	if hc.Privileged {
		t.Error("Privileged = true, want false")
	}
	if hc.PidMode != "" {
		t.Errorf("PidMode = %q, want the private default", hc.PidMode)
	}
	if hc.IpcMode != "" {
		t.Errorf("IpcMode = %q, want the private default", hc.IpcMode)
	}
	if hc.NetworkMode != "" {
		t.Errorf("NetworkMode = %q, want the default (no host/container sharing)", hc.NetworkMode)
	}
	if len(hc.CapAdd) != 0 {
		t.Errorf("CapAdd = %v, want none", hc.CapAdd)
	}

	// The persistent-service variant differs in AutoRemove, plus the single
	// low-port capability addition when the declared port is privileged.
	svc := hardenedServiceHostConfig(app.DefaultResourceLimits(), 3000)
	if svc.AutoRemove {
		t.Error("AutoRemove = true for a service container, want false (reconciler-owned)")
	}
	if svc.Memory != hc.Memory || svc.NanoCPUs != hc.NanoCPUs ||
		svc.ReadonlyRootfs != hc.ReadonlyRootfs || svc.PidsLimit == nil ||
		*svc.PidsLimit != *hc.PidsLimit {
		t.Errorf("service HostConfig = %+v; want the same baseline as the invocation variant except AutoRemove", svc)
	}
	if len(svc.SecurityOpt) != 1 || svc.SecurityOpt[0] != noNewPrivileges {
		t.Errorf("service SecurityOpt = %v, want [%s]", svc.SecurityOpt, noNewPrivileges)
	}
	if len(svc.CapAdd) != 0 {
		t.Errorf("service CapAdd = %v for an unprivileged port, want none", svc.CapAdd)
	}

	lowPort := hardenedServiceHostConfig(app.DefaultResourceLimits(), 80)
	if len(lowPort.CapAdd) != 1 || lowPort.CapAdd[0] != lowPortCapability {
		t.Errorf("low-port CapAdd = %v, want [%s]", lowPort.CapAdd, lowPortCapability)
	}
	if len(lowPort.CapDrop) != 1 || lowPort.CapDrop[0] != "ALL" {
		t.Errorf("low-port CapDrop = %v, want [ALL]", lowPort.CapDrop)
	}
}

// TestResourceHostConfigMapsEffectiveLimits pins the single Docker mapping point:
// an app's effective limits map exactly onto HostConfig.Resources (bytes to
// Memory, NanoCPUs through, PidsLimit as a pointer), and a zero/partial value is
// normalized to the defaults so a hand-built value can never create an
// "unlimited" container. The rest of the hardening baseline is untouched.
func TestResourceHostConfigMapsEffectiveLimits(t *testing.T) {
	limits := app.ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 500_000_000, PidsLimit: 64}
	hc := hardenedHostConfig(true, limits)
	if hc.Memory != 256<<20 || hc.NanoCPUs != 500_000_000 || hc.PidsLimit == nil || *hc.PidsLimit != 64 {
		t.Fatalf("HostConfig resources = mem %d nano %d pids %v; want 256MiB/0.5CPU/64",
			hc.Memory, hc.NanoCPUs, hc.PidsLimit)
	}
	if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" || !hc.ReadonlyRootfs ||
		hc.Tmpfs["/tmp"] != "rw,nosuid,noexec,size=64m" {
		t.Fatalf("hardening baseline must be preserved, got %+v", hc)
	}

	// A zero value normalizes field-by-field to the defaults.
	zero := resourceHostConfig(app.ResourceLimits{})
	def := app.DefaultResourceLimits()
	if zero.Memory != def.MemoryBytes || zero.NanoCPUs != def.NanoCPUs ||
		zero.PidsLimit == nil || *zero.PidsLimit != def.PidsLimit {
		t.Fatalf("zero limits = %+v; want the defaults %+v mapped", zero, def)
	}
}

// TestValidateContainerHostConfig pins the pre-create enforcement boundary: the
// exact shared baseline (including a read-only source mount and its read-only
// tmpfs mask) is accepted, while privileged containers, host namespace/network
// sharing, extra capabilities, devices, writable rootfs, a writable or socket
// bind, and any security-option override are rejected. This is what makes a
// future create path unable to bypass the hardening silently.
func TestValidateContainerHostConfig(t *testing.T) {
	base := func() *container.HostConfig {
		return hardenedServiceHostConfig(app.DefaultResourceLimits(), 3000)
	}
	safeMounts := []mount.Mount{
		{Type: mount.TypeBind, Source: "/apps/demo", Target: "/app/src", ReadOnly: true},
		{Type: mount.TypeTmpfs, Target: "/app/src/node_modules", ReadOnly: true},
	}
	if err := validateContainerHostConfig(base(), safeMounts, 3000, "/apps/demo"); err != nil {
		t.Fatalf("baseline host config rejected: %v", err)
	}
	// A privileged service port's single capability addition is still safe.
	lowPort := hardenedServiceHostConfig(app.DefaultResourceLimits(), 80)
	if err := validateContainerHostConfig(lowPort, nil, 80, ""); err != nil {
		t.Fatalf("low-port service host config rejected: %v", err)
	}
	// NET_BIND_SERVICE is scoped to services: an invocation container (port 0)
	// can never carry it.
	invocationWithCap := hardenedHostConfig(true, app.DefaultResourceLimits())
	invocationWithCap.CapAdd = []string{lowPortCapability}
	if err := validateContainerHostConfig(invocationWithCap, nil, 0, ""); err == nil {
		t.Fatal("an invocation container with NET_BIND_SERVICE was accepted")
	}
	// A HOT_RELOAD source legitimately mapped under Docker named-volume storage
	// keeps working because the check uses the app's recorded source root.
	volumeRoot := "/var/lib/docker/volumes/relay-apps/_data/demo"
	volumeMounts := []mount.Mount{
		{Type: mount.TypeBind, Source: volumeRoot, Target: "/app/src", ReadOnly: true},
	}
	if err := validateContainerHostConfig(base(), volumeMounts, 3000, volumeRoot); err != nil {
		t.Fatalf("named-volume source mount rejected: %v", err)
	}
	// A read-only bind of an arbitrary sensitive host directory is not trusted.
	sensitive := []mount.Mount{
		{Type: mount.TypeBind, Source: "/etc", Target: "/etc", ReadOnly: true},
	}
	if err := validateContainerHostConfig(base(), sensitive, 3000, "/apps/demo"); err == nil {
		t.Fatal("a read-only /etc bind was accepted")
	}

	for _, tc := range []struct {
		name   string
		mutate func(*container.HostConfig) []mount.Mount
	}{
		{
			name: "privileged",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.Privileged = true
				return nil
			},
		},
		{
			name: "host pid namespace",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.PidMode = "host"
				return nil
			},
		},
		{
			name: "host ipc namespace",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.IpcMode = "host"
				return nil
			},
		},
		{
			name: "host network",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.NetworkMode = "host"
				return nil
			},
		},
		{
			name: "container network",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.NetworkMode = "container:abc"
				return nil
			},
		},
		{
			name: "missing capability drop",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.CapDrop = nil
				return nil
			},
		},
		{
			name: "partial capability drop",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.CapDrop = []string{"ALL", "NET_ADMIN"}
				return nil
			},
		},
		{
			name: "extra capability",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.CapAdd = []string{"SYS_ADMIN"}
				return nil
			},
		},
		{
			name: "low-port capability without privileged port",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.CapAdd = []string{lowPortCapability}
				return nil
			},
		},
		{
			name: "host bind configuration",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.Binds = []string{"/var/run/docker.sock:/var/run/docker.sock"}
				return nil
			},
		},
		{
			name: "volumes from",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.VolumesFrom = []string{"other"}
				return nil
			},
		},
		{
			name: "device",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.Devices = []container.DeviceMapping{{PathOnHost: "/dev/kvm"}}
				return nil
			},
		},
		{
			name: "writable rootfs",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.ReadonlyRootfs = false
				return nil
			},
		},
		{
			name: "missing no-new-privileges",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.SecurityOpt = nil
				return nil
			},
		},
		{
			name: "seccomp override",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.SecurityOpt = []string{noNewPrivileges, "seccomp=unconfined"}
				return nil
			},
		},
		{
			name: "writable bind",
			mutate: func(*container.HostConfig) []mount.Mount {
				return []mount.Mount{{Type: mount.TypeBind, Source: "/apps/demo", Target: "/app/src"}}
			},
		},
		{
			name: "docker socket bind",
			mutate: func(*container.HostConfig) []mount.Mount {
				return []mount.Mount{{
					Type: mount.TypeBind, Source: "/var/run/docker.sock",
					Target: "/var/run/docker.sock", ReadOnly: true,
				}}
			},
		},
		{
			name: "writable tmpfs",
			mutate: func(*container.HostConfig) []mount.Mount {
				return []mount.Mount{{Type: mount.TypeTmpfs, Target: "/app/src"}}
			},
		},
		{
			name: "volume mount",
			mutate: func(*container.HostConfig) []mount.Mount {
				return []mount.Mount{{Type: mount.TypeVolume, Target: "/data", ReadOnly: true}}
			},
		},
		{
			name: "nested unsafe host config mount",
			mutate: func(hc *container.HostConfig) []mount.Mount {
				hc.Mounts = []mount.Mount{{
					Type: mount.TypeBind, Source: "/var/run/docker.sock",
					Target: "/sock", ReadOnly: true,
				}}
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hc := base()
			mounts := tc.mutate(hc)
			if err := validateContainerHostConfig(hc, mounts, 3000, "/apps/demo"); err == nil {
				t.Fatal("unsafe host config was accepted")
			}
		})
	}
}
