//go:build integration

// This file exercises the workload security boundary against a real Docker
// daemon: the control-network refusal on both create paths, the effective
// hardened HostConfig for a low-port managed service, and a legitimate
// unprivileged external image service (nginx on a non-privileged port).
package runtime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/testutil"
)

// uniqueControlNetwork creates a bridge network labelled as Relay's control
// plane and registers its removal. The test owns the infrastructure; Relay
// never creates or removes networks.
func uniqueControlNetwork(t *testing.T, ctx context.Context, cli *client.Client) string {
	t.Helper()
	name := "relay-test-control-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := cli.NetworkCreate(ctx, name, client.NetworkCreateOptions{
		Driver: "bridge",
		Labels: map[string]string{controlNetworkLabel: "true"},
	}); err != nil {
		t.Fatalf("create control network: %v", err)
	}
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = cli.NetworkRemove(cc, name, client.NetworkRemoveOptions{})
	})
	return name
}

// TestIntegrationServiceControlNetworkRejected proves the persistent-service
// create path refuses a network labelled relay.control before Docker
// ContainerCreate: no container is ever created for the rejected replica.
func TestIntegrationServiceControlNetworkRejected(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	networkName := uniqueControlNetwork(t, ctx, cli)
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, "ctl-svc")
	})

	_, err := m.StartService(ctx, ServiceSpec{
		App: "ctl-svc", Name: "svc", SourceRef: "alpine:3.20", Port: 3000,
		Image: "alpine:3.20", Networks: []string{networkName},
	}, 0)
	if err == nil {
		t.Fatal("a service on the control network must be rejected")
	}
	if !strings.Contains(err.Error(), "control network") {
		t.Fatalf("error = %v, want a control-network rejection", err)
	}
	if got := countServiceContainers(t, ctx, m, "ctl-svc"); got != 0 {
		t.Fatalf("created %d service container(s) despite the rejection, want 0", got)
	}
}

// TestIntegrationDeclaredVolumeImageRejectedBeforeCreate builds a tiny external
// image whose Config declares a Docker VOLUME, then proves StartService rejects
// it before a persistent service container can be created.
func TestIntegrationDeclaredVolumeImageRejectedBeforeCreate(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	image := "relay-test-declared-volume-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	buildTestImage(ctx, t, image, `FROM scratch
VOLUME ["/relay-data"]
CMD []
`)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(cli, cleanupCtx, image)
	})

	insp, err := cli.ImageInspect(ctx, image)
	if err != nil {
		t.Fatalf("inspect fixture image: %v", err)
	}
	if insp.Config == nil || len(insp.Config.Volumes) != 1 {
		t.Fatalf("fixture image Config.Volumes = %v, want /relay-data", insp.Config)
	}
	if _, ok := insp.Config.Volumes["/relay-data"]; !ok {
		t.Fatalf("fixture image Config.Volumes = %v, want /relay-data", insp.Config.Volumes)
	}

	m, _ := newManager(t)
	_, err = m.StartService(ctx, ServiceSpec{
		App: "declared-volume", Name: "external", SourceRef: image,
		Image: image, Port: 8080,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "declares writable volumes") {
		t.Fatalf("StartService error = %v, want declared-volume rejection", err)
	}
	if got := countServiceContainers(t, ctx, m, "declared-volume"); got != 0 {
		t.Fatalf("created %d service container(s) despite the declared-volume rejection, want 0", got)
	}
}

// TestIntegrationDeclaredVolumeCoveredByServiceTmpfs proves on a real Engine
// that an exactly configured tmpfs masks an image-declared VOLUME without
// creating an anonymous Docker volume. The non-root image writes beneath that
// tmpfs while its separately owned rootfs directory remains unwritable under
// ReadonlyRootfs.
func TestIntegrationDeclaredVolumeCoveredByServiceTmpfs(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	image := "relay-test-volume-tmpfs-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	buildTestImage(ctx, t, image, `FROM alpine:3.20
RUN mkdir -p /relay-root-check && chown 10001:10001 /relay-root-check
VOLUME ["/run/cache"]
USER 10001:10001
CMD ["sh", "-c", "printf tmpfs-ok > /run/cache/relay-proof || exit 21; test \"$(cat /run/cache/relay-proof)\" = tmpfs-ok || exit 22; if awk '$2 == \"/run/cache\" && $3 == \"tmpfs\" { found=1 } END { exit !found }' /proc/mounts; then echo TMPFS_PROC_MOUNTS_OK; else echo TMPFS_PROC_MOUNTS_MISSING; exit 24; fi; if printf bad > /relay-root-check/should-not-write 2>/dev/null; then echo ROOTFS_WRITABLE; exit 23; fi; echo TMPFS_WRITE_OK_ROOTFS_READONLY; exec sleep 3600"]
`)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(cli, cleanupCtx, image)
	})

	imageInfo, err := cli.ImageInspect(ctx, image)
	if err != nil {
		t.Fatalf("inspect fixture image: %v", err)
	}
	if imageInfo.Config == nil || len(imageInfo.Config.Volumes) != 1 {
		t.Fatalf("fixture image Config.Volumes = %v, want only /run/cache", imageInfo.Config)
	}
	if _, ok := imageInfo.Config.Volumes["/run/cache"]; !ok {
		t.Fatalf("fixture image Config.Volumes = %v, want /run/cache", imageInfo.Config.Volumes)
	}

	// Snapshot after the image build so the delta is specifically attributable
	// to creating the service container, not build-side effects.
	volumesBefore := dockerVolumeNames(t, ctx, cli)
	m, _ := newManager(t)
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, "svc-volume-tmpfs")
	})

	id, err := m.StartService(ctx, ServiceSpec{
		App: "svc-volume-tmpfs", Name: "external", SourceRef: image,
		Image: image, Port: 8080, Tmpfs: []string{"/run/cache"},
	}, 0)
	if err != nil {
		t.Fatalf("start external service with exact VOLUME tmpfs coverage: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)

	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect service: %v", err)
	}
	assertHardenedHostConfig(t, insp.Container.HostConfig)
	if !insp.Container.HostConfig.ReadonlyRootfs {
		t.Error("ReadonlyRootfs = false, want true")
	}
	if got, want := insp.Container.HostConfig.Tmpfs["/run/cache"], serviceTmpfsOptions(); got != want {
		t.Errorf("HostConfig.Tmpfs[/run/cache] = %q, want %q", got, want)
	}
	if insp.Container.Config == nil || insp.Container.Config.User != "10001:10001" {
		t.Errorf("container user = %v, want image's non-root 10001:10001", insp.Container.Config)
	}
	var mountDescriptions []string
	for _, mounted := range insp.Container.Mounts {
		mountDescriptions = append(mountDescriptions, string(mounted.Type)+":"+mounted.Destination+":"+mounted.Name)
		if mounted.Destination == "/run/cache" && mounted.Type == "volume" {
			t.Errorf("ContainerInspect.Mounts contains Docker volume at /run/cache: %v", mountDescriptions)
		}
	}
	// This Engine's ContainerInspect.Mounts omits tmpfs entries. The inspected
	// HostConfig plus the in-container mount table and Docker volume inventory
	// establish the effective mount and absence of an anonymous volume.

	var logs string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		rc, logErr := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if logErr != nil {
			t.Fatalf("read service logs: %v", logErr)
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rc)
		_ = rc.Close()
		logs = buf.String()
		return strings.Contains(logs, "TMPFS_PROC_MOUNTS_OK") || strings.Contains(logs, "TMPFS_PROC_MOUNTS_MISSING") || strings.Contains(logs, "TMPFS_WRITE_OK_ROOTFS_READONLY") || strings.Contains(logs, "ROOTFS_WRITABLE")
	}) {
		t.Fatalf("service did not report tmpfs mount-table/write checks; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "TMPFS_PROC_MOUNTS_OK") {
		t.Errorf("in-container /proc/mounts did not show tmpfs mounted exactly at /run/cache; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "TMPFS_WRITE_OK_ROOTFS_READONLY") {
		t.Errorf("service did not prove writable tmpfs plus unwritable rootfs; logs:\n%s", logs)
	}
	t.Logf("non-root service write checks passed; logs=%q", logs)

	volumesAfter := dockerVolumeNames(t, ctx, cli)
	var createdVolumes []string
	for name := range volumesAfter {
		if _, existed := volumesBefore[name]; !existed {
			createdVolumes = append(createdVolumes, name)
		}
	}
	t.Logf("Docker volume inventory around service create: before=%d after=%d newly-created=%v", len(volumesBefore), len(volumesAfter), createdVolumes)
	if len(createdVolumes) != 0 {
		t.Errorf("Docker volume inventory gained %v after service create; exact tmpfs coverage must suppress anonymous VOLUME creation (before=%v after=%v)", createdVolumes, sortedVolumeNames(volumesBefore), sortedVolumeNames(volumesAfter))
	}
}

func dockerVolumeNames(t *testing.T, ctx context.Context, cli *client.Client) map[string]struct{} {
	t.Helper()
	listed, err := cli.VolumeList(ctx, client.VolumeListOptions{})
	if err != nil {
		t.Fatalf("list Docker volumes: %v", err)
	}
	names := make(map[string]struct{}, len(listed.Items))
	for _, item := range listed.Items {
		names[item.Name] = struct{}{}
	}
	return names
}

func sortedVolumeNames(names map[string]struct{}) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// TestIntegrationExecutionControlNetworkRejected proves the shared execution
// create path applies the same control-network guard: with the worker-global
// network set pointing at a control network, an invocation fails before any
// container exists.
func TestIntegrationExecutionControlNetworkRejected(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	networkName := uniqueControlNetwork(t, ctx, cli)
	m, err := newCoordinatedRealManager(t,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		WithNetworks([]string{networkName}),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-ctl-exec:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function run(e, context){ console.log('must not run'); }\n")
	fn := app.App{Name: "ctl-exec", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{NodeID: "test-host", App: "ctl-exec", Image: prepared.Image})
	err = m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	if err == nil {
		t.Fatal("an execution on the control network must be rejected")
	}
	if !strings.Contains(err.Error(), "control network") {
		t.Fatalf("error = %v, want a control-network rejection", err)
	}
	if got := countContainersByLabel(ctx, m.cli, labelApp, "ctl-exec"); got != 0 {
		t.Fatalf("created %d execution container(s) despite the rejection, want 0", got)
	}
}

// TestIntegrationManagedServiceLowPort proves a managed (non-root UID 10001)
// service can bind a privileged declared port with the shared hardening and the
// single scoped NET_BIND_SERVICE addition. It also rechecks the effective
// inspect-level security baseline for a persistent service.
func TestIntegrationManagedServiceLowPort(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, "svc-low-port")
		cleanupImagePrefixes(cli, "relay-app-svc-low-port:")()
	})

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function hi(e, context){ console.log('hi'); }\n")
	writeFile(t, dir, "service.js", `
import http from "node:http";
process.on("SIGTERM", () => process.exit(0));
http.createServer((req, res) => res.end("ok")).listen(80, () => {
  console.log("listening on 80");
});
`)
	fn := app.App{Name: "svc-low-port", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
	prepared, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	id, err := m.StartService(ctx, ServiceSpec{
		App: "svc-low-port", Name: "svc", SourceRef: "service.js",
		Port: 80, Image: prepared.Image, Entry: []string{"node", "/app/service.js"},
		Env: []string{"PORT=80"},
	}, 0)
	if err != nil {
		t.Fatalf("start low-port service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)

	var logs string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		rc, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if err != nil {
			t.Fatalf("logs: %v", err)
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rc)
		rc.Close()
		logs = buf.String()
		return strings.Contains(logs, "listening on 80")
	}) {
		t.Fatalf("service never bound port 80; logs:\n%s", logs)
	}

	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	assertHardenedHostConfig(t, insp.Container.HostConfig)
	if len(insp.Container.HostConfig.CapAdd) != 1 ||
		!isLowPortCapability(insp.Container.HostConfig.CapAdd[0]) {
		t.Fatalf("cap add = %v, want the single low-port capability", insp.Container.HostConfig.CapAdd)
	}
	if insp.Container.Config == nil || insp.Container.Config.User != "10001:10001" {
		t.Fatalf("config user = %v, want the managed runtime user 10001:10001", insp.Container.Config)
	}
}

// TestIntegrationUnprivilegedExternalImageService keeps a legitimate external
// image service working under the hardened HostConfig. nginx-unprivileged runs
// as a non-root user and listens on 8080, so it needs no added capability and no
// writable rootfs path.
func TestIntegrationUnprivilegedExternalImageService(t *testing.T) {
	cli := testutil.RequireDocker(t)
	const image = "nginxinc/nginx-unprivileged:1.27-alpine"
	testutil.RequireImage(t, cli, image)

	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, "svc-nginx")
	})

	id, err := m.StartService(ctx, ServiceSpec{
		App: "svc-nginx", Name: "web", SourceRef: image, Port: 8080,
		Image: image, Env: []string{"PORT=8080"},
		Tmpfs: []string{"/var/cache/nginx", "/run"},
	}, 0)
	if err != nil {
		t.Fatalf("start external image service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)

	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	assertHardenedHostConfig(t, insp.Container.HostConfig)
	for _, destination := range []string{"/var/cache/nginx", "/run"} {
		if got, want := insp.Container.HostConfig.Tmpfs[destination], serviceTmpfsOptions(); got != want {
			t.Errorf("HostConfig.Tmpfs[%s] = %q, want fixed secure options %q", destination, got, want)
		}
	}
	if len(insp.Container.HostConfig.CapAdd) != 0 {
		t.Fatalf("external image service gained capabilities: %v", insp.Container.HostConfig.CapAdd)
	}
	if insp.Container.Config == nil || insp.Container.Config.User != "101" {
		t.Fatalf("config user = %v, want the image's own 101 user", insp.Container.Config)
	}
	if insp.Container.State == nil || insp.Container.State.Status != container.StateRunning {
		t.Fatalf("container state = %+v, want running", insp.Container.State)
	}
}
