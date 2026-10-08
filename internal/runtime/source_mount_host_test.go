package runtime

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"

	"relay/internal/app"
	pyengine "relay/internal/runtime/python"
	"relay/internal/testutil"
)

// TestMapToDaemonPathNestedMount pins the core mapping: an app path under the
// inspected bind destination is rewritten to the mount's daemon-visible Source
// plus the slash-relative remainder, at the mount root, nested, and with a
// trailing slash. A path outside the mount is returned unchanged.
func TestMapToDaemonPathNestedMount(t *testing.T) {
	mounts := []container.MountPoint{
		{Type: mount.TypeBind, Source: "/host/project/examples/apps", Destination: "/apps"},
	}
	for _, tc := range []struct {
		name, in, want string
	}{
		{"nested", "/apps/foo/bar", "/host/project/examples/apps/foo/bar"},
		{"direct child", "/apps/foo", "/host/project/examples/apps/foo"},
		{"mount root", "/apps", "/host/project/examples/apps"},
		{"trailing slash", "/apps/foo/", "/host/project/examples/apps/foo"},
		{"uncovered", "/opt/foo", "/opt/foo"},
		{"boundary sibling", "/apps2/foo", "/apps2/foo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mapToDaemonPath(tc.in, mounts); got != tc.want {
				t.Fatalf("mapToDaemonPath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestMapToDaemonPathMostSpecific pins that the most-specific (longest)
// destination wins, so an app individually bind-mounted beneath a broader /apps
// mount is not shadowed by the parent.
func TestMapToDaemonPathMostSpecific(t *testing.T) {
	mounts := []container.MountPoint{
		{Type: mount.TypeBind, Source: "/host/apps", Destination: "/apps"},
		{Type: mount.TypeBind, Source: "/host/special", Destination: "/apps/special"},
	}
	if got := mapToDaemonPath("/apps/special/app", mounts); got != "/host/special/app" {
		t.Fatalf("most-specific mapping = %q, want /host/special/app", got)
	}
	if got := mapToDaemonPath("/apps/other", mounts); got != "/host/apps/other" {
		t.Fatalf("parent mapping = %q, want /host/apps/other", got)
	}
}

// TestMapToDaemonPathFallback pins the conservative cases: only bind mounts with
// a non-empty Source are mapped, and an uncovered path (including the filesystem
// root) is returned unchanged.
func TestMapToDaemonPathFallback(t *testing.T) {
	mounts := []container.MountPoint{
		{Type: mount.TypeVolume, Source: "/var/lib/docker/volumes/v/_data", Destination: "/apps"},
		{Type: mount.TypeTmpfs, Destination: "/tmp"},
		{Type: mount.TypeBind, Source: "", Destination: "/empty"},
		{Type: mount.TypeBind, Source: "/host/data", Destination: ""},
	}
	for _, in := range []string{"/apps/foo", "/tmp/x", "/empty/y", "/", "/unrelated"} {
		if got := mapToDaemonPath(in, mounts); got != in {
			t.Fatalf("mapToDaemonPath(%q) = %q, want unchanged", in, got)
		}
	}
}

// TestDaemonSourcePathMapsSelfContainerBindMount drives the real ContainerInspect
// client path against a scripted daemon whose self container carries the bundled
// Compose shape (a host dir bind-mounted at /apps) and proves the mapping is
// applied and cached: a second call must not inspect again.
func TestDaemonSourcePathMapsSelfContainerBindMount(t *testing.T) {
	const hostname = "abcdef012345"
	fullID := hostname + strings.Repeat("f", 52)
	inspected := 0
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet, path: "/containers/" + hostname + "/json",
		body: fmt.Sprintf(
			`{"Id":%q,"Config":{"Hostname":%q},"Mounts":[{"Type":"bind","Source":"/host/apps","Destination":"/apps"}]}`,
			fullID, hostname),
		onMatch: func() { inspected++ },
	})
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: hostname}

	if got := m.daemonSourcePath(context.Background(), "/apps/myapp"); got != "/host/apps/myapp" {
		t.Fatalf("daemonSourcePath = %q, want /host/apps/myapp", got)
	}
	if got := m.daemonSourcePath(context.Background(), "/apps/other"); got != "/host/apps/other" {
		t.Fatalf("second daemonSourcePath = %q, want /host/apps/other", got)
	}
	if inspected != 1 {
		t.Fatalf("inspect calls = %d, want exactly 1 (the resolution is cached)", inspected)
	}
}

// TestDaemonSourcePathRejectsForeignContainer pins the safe validation: an
// inspect that resolves to a container declaring a different hostname is not
// Relay's own container, so the path is left unchanged.
func TestDaemonSourcePathRejectsForeignContainer(t *testing.T) {
	const hostname = "abcdef012345"
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet, path: "/containers/" + hostname + "/json",
		body: fmt.Sprintf(
			`{"Id":%q,"Config":{"Hostname":"some-other-container"},"Mounts":[{"Type":"bind","Source":"/host/apps","Destination":"/apps"}]}`,
			hostname+strings.Repeat("f", 52)),
	})
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: hostname}
	if got := m.daemonSourcePath(context.Background(), "/apps/myapp"); got != "/apps/myapp" {
		t.Fatalf("a foreign container must not map; got %q", got)
	}
}

// TestDaemonSourcePathIgnoresNonBindMounts pins that a resolved self container
// whose /apps is a volume (not a bind) leaves the path unchanged.
func TestDaemonSourcePathIgnoresNonBindMounts(t *testing.T) {
	const hostname = "abcdef012345"
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet, path: "/containers/" + hostname + "/json",
		body: fmt.Sprintf(
			`{"Id":%q,"Config":{"Hostname":%q},"Mounts":[{"Type":"volume","Source":"/var/lib/docker/volumes/v/_data","Destination":"/apps"}]}`,
			hostname+strings.Repeat("f", 52), hostname),
	})
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: hostname}
	if got := m.daemonSourcePath(context.Background(), "/apps/myapp"); got != "/apps/myapp" {
		t.Fatalf("a volume mount must not map; got %q", got)
	}
}

// TestDaemonSourcePathNonContainerNoInspect pins that a non-container hostname
// (the production default detection) never issues a daemon call: the scripted
// daemon has no routes, so any inspect would fail the test through the harness.
func TestDaemonSourcePathNonContainerNoInspect(t *testing.T) {
	cli := newScriptedDockerClient(t)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "my-laptop"}
	if got := m.daemonSourcePath(context.Background(), "/apps/myapp"); got != "/apps/myapp" {
		t.Fatalf("non-container hostname must keep appDir; got %q", got)
	}
}

// TestDaemonSourcePathInspectErrorNotCached pins that a failed self-inspect is
// not cached, so a transient daemon error is retried by a later call instead of
// permanently disabling the mapping.
func TestDaemonSourcePathInspectErrorNotCached(t *testing.T) {
	const hostname = "abcdef012345"
	attempts := 0
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet, path: "/containers/" + hostname + "/json",
		status: http.StatusInternalServerError, body: `{"message":"daemon blip"}`,
		onMatch: func() { attempts++ },
	})
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: hostname}
	for i := 0; i < 2; i++ {
		if got := m.daemonSourcePath(context.Background(), "/apps/x"); got != "/apps/x" {
			t.Fatalf("attempt %d: got %q, want the appDir fallback", i, got)
		}
	}
	if attempts != 2 {
		t.Fatalf("inspect attempts = %d, want 2 (a failure must not be cached)", attempts)
	}
}

// TestIsContainerIDLike pins the default-hostname precondition: only a short or
// full lowercase hex container ID is treated as a reliable self identity.
func TestIsContainerIDLike(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"abcdef012345", true},
		{strings.Repeat("a", 64), true},
		{"ABCDEF012345", false},
		{"abcdef01234", false},
		{strings.Repeat("a", 65), false},
		{"abcdef0123456g", false},
		{"test-host", false},
		{"", false},
	} {
		if got := isContainerIDLike(tc.in); got != tc.want {
			t.Errorf("isContainerIDLike(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestIsSelfContainer pins the safe self-identification: the inspected container
// must declare the worker hostname and, for the default container-ID hostname,
// carry it as an ID prefix; a custom hostname is accepted on the hostname alone.
func TestIsSelfContainer(t *testing.T) {
	const short = "abcdef012345"
	full := short + strings.Repeat("f", 52)
	for _, tc := range []struct {
		name string
		c    container.InspectResponse
		host string
		want bool
	}{
		{"default short ID", container.InspectResponse{ID: full, Config: &container.Config{Hostname: short}}, short, true},
		{"hostname mismatch", container.InspectResponse{ID: full, Config: &container.Config{Hostname: "other"}}, short, false},
		{"short hostname not an ID prefix", container.InspectResponse{ID: strings.Repeat("f", 64), Config: &container.Config{Hostname: short}}, short, false},
		{"custom hostname", container.InspectResponse{ID: full, Config: &container.Config{Hostname: "relay"}}, "relay", true},
		{"nil config", container.InspectResponse{ID: full}, short, false},
		{"empty hostname", container.InspectResponse{ID: full, Config: &container.Config{Hostname: short}}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSelfContainer(tc.c, tc.host); got != tc.want {
				t.Fatalf("isSelfContainer = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPrepareRecordsDaemonVisibleSourcePath proves the resolver is wired into
// Prepare: when Relay is a container whose own filesystem maps the app directory
// to a daemon-visible host path, the recorded SourceMount.HostPath is the mapped
// path rather than fn.Dir. The self-container mount's Destination is the app's
// temp directory itself, so the mapping is exact and needs no /apps path on the
// test host.
func TestPrepareRecordsDaemonVisibleSourcePath(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "mapped", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	builtPlan, err := pyengine.Engine{}.Plan(spec, dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	present := fmt.Sprintf(`{"Id":"sha256:deadbeef","Config":{"Labels":{%q:%q}}}`,
		labelBootstrap, bootstrapHash(builtPlan))

	const hostname = "abcdef012345"
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", body: present},
		dockerRoute{
			method: http.MethodGet, path: "/containers/" + hostname + "/json",
			body: fmt.Sprintf(
				`{"Id":%q,"Config":{"Hostname":%q},"Mounts":[{"Type":"bind","Source":"/host/mapped","Destination":%q}]}`,
				hostname+strings.Repeat("f", 52), hostname, dir),
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true
	m.hostname = hostname

	if _, err := m.Prepare(context.Background(), fn); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	sm, ok := m.sourceMountFor(fn.Name)
	if !ok {
		t.Fatal("prepare must record a source mount")
	}
	if sm.HostPath != "/host/mapped" {
		t.Fatalf("recorded HostPath = %q, want the daemon-visible /host/mapped", sm.HostPath)
	}
}
