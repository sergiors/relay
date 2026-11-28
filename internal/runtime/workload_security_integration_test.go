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
		Labels: map[string]string{networkRoleLabel: executionControlNetworkRole},
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
// create path refuses a network carrying only relay.network.role=execution-control
// before Docker ContainerCreate: no container is ever created for the rejected
// replica.
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

// TestIntegrationDeclaredVolumeCoveredByServiceEphemeral proves on a real Engine
// that an exactly configured ephemeral anonymous volume covers an image-declared
// VOLUME without creating a second implicit volume. The non-root image writes
// beneath that volume while its separately owned rootfs directory remains
// unwritable under ReadonlyRootfs, and an explicit container delete reclaims the
// anonymous volumes.
func TestIntegrationDeclaredVolumeCoveredByServiceEphemeral(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	image := "relay-test-volume-ephemeral-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	buildTestImage(ctx, t, image, `FROM alpine:3.20
RUN mkdir -p /run/cache /relay-root-check && chown 10001:10001 /run/cache /relay-root-check
VOLUME ["/run/cache"]
USER 10001:10001
CMD ["sh", "-c", "printf volume-ok > /run/cache/relay-proof || exit 21; test \"$(cat /run/cache/relay-proof)\" = volume-ok || exit 22; if printf bad > /relay-root-check/should-not-write 2>/dev/null; then echo ROOTFS_WRITABLE; exit 23; fi; echo VOLUME_WRITE_OK_ROOTFS_READONLY; exec sleep 3600"]
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
		_, _ = m.RemoveAppServiceContainers(cc, "svc-volume-ephemeral")
	})

	id, err := m.StartService(ctx, ServiceSpec{
		App: "svc-volume-ephemeral", Name: "external", SourceRef: image,
		Image: image, Port: 8080, Ephemeral: []string{"/run/cache"},
	}, 0)
	if err != nil {
		t.Fatalf("start external service with exact VOLUME ephemeral coverage: %v", err)
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
	if len(insp.Container.HostConfig.Tmpfs) != 0 {
		t.Errorf("HostConfig.Tmpfs = %v, want none", insp.Container.HostConfig.Tmpfs)
	}
	if insp.Container.Config == nil || insp.Container.Config.User != "10001:10001" {
		t.Errorf("container user = %v, want image's non-root 10001:10001", insp.Container.Config)
	}
	volumeNames := make(map[string]string)
	for _, mounted := range insp.Container.Mounts {
		if mounted.Type != "volume" {
			continue
		}
		volumeNames[mounted.Destination] = mounted.Name
		if !mounted.RW {
			t.Errorf("volume at %s is not writable", mounted.Destination)
		}
	}
	for _, destination := range []string{tmpVolumeTarget, "/run/cache"} {
		if volumeNames[destination] == "" {
			t.Errorf("ContainerInspect.Mounts = %+v, want an anonymous volume at %s", insp.Container.Mounts, destination)
		}
	}
	if len(volumeNames) != 2 {
		t.Errorf("container volume mounts = %v, want exactly /tmp and /run/cache", volumeNames)
	}

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
		return strings.Contains(logs, "VOLUME_WRITE_OK_ROOTFS_READONLY") || strings.Contains(logs, "ROOTFS_WRITABLE")
	}) {
		t.Fatalf("service did not report volume write/rootfs checks; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "VOLUME_WRITE_OK_ROOTFS_READONLY") {
		t.Errorf("service did not prove a writable ephemeral volume plus unwritable rootfs; logs:\n%s", logs)
	}
	t.Logf("non-root service write checks passed; logs=%q", logs)

	volumesAfter := dockerVolumeNames(t, ctx, cli)
	var createdVolumes []string
	for name := range volumesAfter {
		if _, existed := volumesBefore[name]; !existed {
			createdVolumes = append(createdVolumes, name)
		}
	}
	sort.Strings(createdVolumes)
	t.Logf("Docker volume inventory around service create: before=%d after=%d newly-created=%v", len(volumesBefore), len(volumesAfter), createdVolumes)
	if len(createdVolumes) != 2 {
		t.Errorf("Docker volume inventory gained %v after service create, want exactly the /tmp and /run/cache anonymous volumes", createdVolumes)
	}

	// Volume independence: a second replica of the same image and the same
	// ephemeral set must receive its OWN anonymous volumes rather than share
	// the first replica's, so one replica's state can never leak sideways.
	id2, err := m.StartService(ctx, ServiceSpec{
		App: "svc-volume-ephemeral", Name: "external", SourceRef: image,
		Image: image, Port: 8080, Ephemeral: []string{"/run/cache"},
	}, 1)
	if err != nil {
		t.Fatalf("start second replica: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id2)
	insp2, err := cli.ContainerInspect(ctx, id2, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect second replica: %v", err)
	}
	replica2Volumes := make(map[string]string)
	for _, mounted := range insp2.Container.Mounts {
		if mounted.Type == "volume" {
			replica2Volumes[mounted.Destination] = mounted.Name
		}
	}
	for _, destination := range []string{tmpVolumeTarget, "/run/cache"} {
		name2 := replica2Volumes[destination]
		if name2 == "" {
			t.Fatalf("second replica mounts = %+v, want an anonymous volume at %s",
				insp2.Container.Mounts, destination)
		}
		if name2 == volumeNames[destination] {
			t.Errorf("second replica shares anonymous volume %q at %s with the "+
				"first replica", name2, destination)
		}
	}
	volumesAfterReplica2 := dockerVolumeNames(t, ctx, cli)
	gainedAfterReplica2 := 0
	for name := range volumesAfterReplica2 {
		if _, existed := volumesBefore[name]; !existed {
			gainedAfterReplica2++
		}
	}
	if gainedAfterReplica2 != 4 {
		t.Errorf("volume inventory gained %d volumes after two replicas, want 4 "+
			"(an independent /tmp and /run/cache pair per replica)", gainedAfterReplica2)
	}

	// Explicit removal must reclaim the anonymous volumes of BOTH replicas, not
	// only the containers. AutoRemove covers one-shot containers; this covers
	// services.
	if _, err := m.RemoveAppServiceContainers(ctx, "svc-volume-ephemeral"); err != nil {
		t.Fatalf("remove service containers: %v", err)
	}
	volumesRemoved := dockerVolumeNames(t, ctx, cli)
	for _, volumes := range []map[string]string{volumeNames, replica2Volumes} {
		for destination, name := range volumes {
			if _, exists := volumesRemoved[name]; exists {
				t.Errorf("anonymous volume %q at %s still exists after explicit service removal", name, destination)
			}
		}
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
		// Both explicit writable mounts the shipped examples have used for
		// this image: the cache directory nginx itself owns and the runtime
		// directory.
		Ephemeral: []string{"/run", "/var/cache/nginx"},
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
	for _, destination := range []string{tmpVolumeTarget, "/run", "/var/cache/nginx"} {
		if _, ok := mountAt(insp.Container.HostConfig.Mounts, destination); !ok {
			t.Errorf("HostConfig.Mounts = %v, want an anonymous volume at %s", insp.Container.HostConfig.Mounts, destination)
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

	// The image's own non-root user must be able to write the paths Relay made
	// writable for it: the cache mount nginx owns and the baseline /tmp. /run
	// is root-owned in the image, so only its mounted presence is asserted
	// above — Docker copies the image directory's ownership into the volume.
	writeOut, writeCode := execInContainer(t, ctx, cli, id, "/bin/sh", "-c",
		`printf cache > /var/cache/nginx/relay-proof || exit 71; `+
			`printf tmp > /tmp/relay-proof || exit 72; `+
			`printf 'UID=%s CACHE=%s TMP=%s' "$(id -u)" `+
			`"$(cat /var/cache/nginx/relay-proof)" "$(cat /tmp/relay-proof)"`)
	if writeCode != 0 {
		t.Fatalf("non-root write exec failed (exit %d): %s", writeCode, writeOut)
	}
	if !strings.Contains(writeOut, "UID=101") {
		t.Errorf("write exec = %q, want the image's non-root uid 101", writeOut)
	}
	if !strings.Contains(writeOut, "CACHE=cache TMP=tmp") {
		t.Errorf("write exec = %q, want both writes read back", writeOut)
	}

	// The service must actually serve HTTP on its declared port inside the
	// container. nginx may still be starting when the container first reports
	// running, so the request is polled.
	var httpBody string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		out, code := execInContainer(t, ctx, cli, id, "/bin/sh", "-c",
			"wget -qO- http://127.0.0.1:8080/")
		httpBody = out
		return code == 0
	}) {
		t.Fatalf("in-container HTTP request never succeeded; last response: %q",
			httpBody)
	}
	if !strings.Contains(httpBody, "nginx") {
		t.Errorf("HTTP response = %q, want it to identify nginx", httpBody)
	}
}

// TestIntegrationImageValidationRejectsHostileTmpTargets proves the baseline
// /tmp target validation against REAL daemon image archives: the scripted
// fixtures cannot prove the daemon's actual save format. A real image whose
// /tmp is a symlink — which Docker would follow when mounting the anonymous
// volume, redirecting it over /etc despite ReadonlyRootfs — and a real image
// with no /tmp at all both fail closed before any container exists, on both
// the service and execution create paths, while a normal image passes.
//
// The execution path is driven through the Manager's execution-image
// validation seam: the full create entry requires a provisioned control
// network whose route proof needs a Linux Docker host (see
// ensureControlNetwork), so this test deliberately avoids newManager and runs
// on hosts where those manager-driven integration tests cannot.
func TestIntegrationImageValidationRejectsHostileTmpTargets(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-node"}

	buildFixture := func(name, dockerfile string) string {
		ref := "relay-test-tmp-target-" + name + "-" +
			strconv.FormatInt(time.Now().UnixNano(), 36)
		buildTestImage(ctx, t, ref, dockerfile)
		t.Cleanup(func() {
			cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer ccancel()
			cleanupImage(cli, cc, ref)
		})
		return ref
	}
	symlinkImage := buildFixture("symlink", `FROM alpine:3.20
RUN rm -rf /tmp && ln -s /etc /tmp
CMD ["sleep", "3600"]
`)
	missingImage := buildFixture("missing", `FROM alpine:3.20
RUN rm -rf /tmp
CMD ["sleep", "3600"]
`)
	normalImage := buildFixture("normal", `FROM alpine:3.20
CMD ["sleep", "3600"]
`)

	for _, tc := range []struct{ name, image, want string }{
		{"symlinked /tmp fails closed", symlinkImage,
			`redirected through symlink "/tmp"`},
		{"missing /tmp fails closed", missingImage,
			`must already exist as a directory`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Service path: the full StartService create path against the
			// real archive.
			_, err := m.StartService(ctx, ServiceSpec{
				App: "svc-tmp-target", Name: "web", SourceRef: tc.image,
				Image: tc.image, Port: 8080,
			}, 0)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("StartService error = %v, want it to contain %q",
					err, tc.want)
			}
			// Execution path: the Manager's execution image validation.
			if _, err := m.validateExecutionImage(ctx, tc.image); err == nil ||
				!strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateExecutionImage error = %v, want it to contain %q",
					err, tc.want)
			}
		})
	}

	// Both rejections happened before ContainerCreate: the daemon never held a
	// container for this app.
	containers, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("list containers: %v", err)
	}
	for _, listed := range containers.Items {
		if listed.Labels[labelApp] == "svc-tmp-target" {
			t.Fatalf("rejected service still has container %s", listed.ID)
		}
	}

	t.Run("image with a real /tmp directory passes", func(t *testing.T) {
		id, err := m.validateExecutionImage(ctx, normalImage)
		if err != nil {
			t.Fatalf("validation of a normal image: %v", err)
		}
		if !strings.HasPrefix(id, "sha256:") {
			t.Fatalf("validated image ID = %q, want an immutable sha256 ID", id)
		}
	})
}

// TestIntegrationTmpVolumeHolds256MiBUnderMemoryLimit proves Relay's /tmp is
// disk-backed, not memory-backed: a non-root, read-only-rootfs container
// capped at the default 128 MiB streams a 256 MiB file into the anonymous /tmp
// volume — dd reads /dev/zero in 1 MiB blocks, so neither the container nor
// this test ever holds the payload in memory — and the resulting size is
// verified inside the container. A tmpfs /tmp could not do this: its pages are
// unreclaimable and would exhaust both a size-capped mount and the memory
// cgroup. The container is created from the production hardenedHostConfig
// baseline exactly as service creates use it.
func TestIntegrationTmpVolumeHolds256MiBUnderMemoryLimit(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	image := "relay-test-tmp256-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	buildTestImage(ctx, t, image, `FROM alpine:3.20
USER 10001:10001
CMD ["sh", "-c", "dd if=/dev/zero of=/tmp/big bs=1048576 count=256 2>&1 || exit 41; sz=$(stat -c %s /tmp/big) || exit 42; [ \"$sz\" = 268435456 ] || { echo SIZE_MISMATCH=$sz; exit 43; }; echo TMP256_OK size=$sz; exec sleep 3600"]
`)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(cli, cleanupCtx, image)
	})

	hc := hardenedHostConfig(false, app.DefaultResourceLimits())
	if hc.Memory != 128<<20 {
		t.Fatalf("HostConfig memory = %d, want the 128 MiB default", hc.Memory)
	}
	if hc.AutoRemove {
		t.Fatal("AutoRemove = true, want the persistent shape so the test owns removal")
	}
	assertBaselineTmpVolume(t, hc.Mounts)

	resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: image},
		HostConfig: hc,
	})
	if err != nil {
		t.Fatalf("create container: %v", err)
	}
	id := resp.ID
	// The production removal helper reclaims the anonymous volume together
	// with the container (Force + RemoveVolumes).
	t.Cleanup(func() {
		_ = removeContainer(cli, id)
	})
	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start container: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)

	var logs string
	if !pollUntil(ctx, 2*time.Minute, func() bool {
		stdout, stderr, logErr := fetchAppLogs(ctx, cli, id)
		if logErr != nil {
			return false
		}
		logs = stdout + stderr
		return strings.Contains(logs, "TMP256_OK") ||
			strings.Contains(logs, "SIZE_MISMATCH")
	}) {
		t.Fatalf("container did not report the 256 MiB /tmp write; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "TMP256_OK size=268435456") {
		t.Fatalf("write result = logs %q, want the verified 268435456-byte file", logs)
	}

	// The size is verified a second time through an in-container exec, running
	// as the image's non-root user, against the live volume.
	out, code := execInContainer(t, ctx, cli, id, "/bin/sh", "-c",
		"stat -c %s /tmp/big")
	if code != 0 {
		t.Fatalf("in-container stat exit code = %d, output: %s", code, out)
	}
	if !strings.Contains(out, "268435456") {
		t.Fatalf("in-container stat = %q, want 268435456", strings.TrimSpace(out))
	}

	// The effective runtime hardening still holds with the payload resident on
	// the volume: 128 MiB cap, read-only rootfs, no HostConfig.Tmpfs anywhere,
	// the image's non-root user, and exactly the baseline /tmp volume.
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect container: %v", err)
	}
	assertHardenedHostConfig(t, insp.Container.HostConfig)
	if insp.Container.Config == nil || insp.Container.Config.User != "10001:10001" {
		t.Errorf("container user = %v, want the image's non-root 10001:10001",
			insp.Container.Config)
	}
	volumeMounts := 0
	for _, mounted := range insp.Container.Mounts {
		if mounted.Type != "volume" {
			continue
		}
		volumeMounts++
		if !mounted.RW {
			t.Errorf("volume at %s is not writable", mounted.Destination)
		}
	}
	if volumeMounts != 1 {
		t.Errorf("volume mounts = %d, want exactly the baseline /tmp volume",
			volumeMounts)
	}
}
