//go:build integration

// This file exercises the reconciler's hot-reload flow end to end against a
// real Docker daemon: discover -> change & rebuild -> broken template retained
// -> fix -> remove.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES a reachable Docker
// daemon; a missing dependency fails the affected tests rather than skipping
// them. Start Docker, or point DOCKER_HOST at a running daemon. The daemon is
// located via client.FromEnv, so DOCKER_HOST, the local socket, and a socket
// proxy are all respected.
package reconciler

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/testutil"
)

// createReconcilerNetwork creates a DEDICATED user-defined bridge network for
// this test — tests act as the operator that owns the worker-global NETWORKS
// set, which the persistent execution channel requires (every execution start
// fails closed without a configured shared network). The label is relay.test
// (never the execution-control role, which the workload guard rejects) and no
// ports are published. Removal is registered FIRST so it runs LAST
// (t.Cleanup is LIFO), after the deferred Manager.Close has discarded attached
// containers; a removal failure is logged, never fatal.
func createReconcilerNetwork(t *testing.T, cli *client.Client) string {
	t.Helper()
	name := fmt.Sprintf("relay-test-exec-%08x", rand.Uint32())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := cli.NetworkCreate(ctx, name, client.NetworkCreateOptions{
		Driver: "bridge",
		Labels: map[string]string{"relay.test": "execution"},
	}); err != nil {
		t.Fatalf("create reconciler test network %s: %v", name, err)
	}
	t.Cleanup(func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rcancel()
		if _, err := cli.NetworkRemove(rctx, name, client.NetworkRemoveOptions{}); err != nil {
			t.Logf("cleanup: remove reconciler test network %s: %v", name, err)
		}
	})
	return name
}

// awaitReconcilerAppLogs polls the app's NATIVE Docker logs (stdout+stderr
// across every container carrying relay.app=appName, resolved fresh each poll)
// until want appears at least min times, then returns the combined text. The
// count is the freshness proof for stages that reuse a warm container: re-
// execution must ADD an occurrence rather than satisfy the assertion from
// stale logs.
func awaitReconcilerAppLogs(t *testing.T, ctx context.Context, cli *client.Client, appName, want string, min int) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	combined := ""
	for time.Now().Before(deadline) {
		list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err == nil {
			combined = ""
			for _, c := range list.Items {
				if c.Labels["relay.app"] != appName {
					continue
				}
				res, lerr := cli.ContainerLogs(ctx, c.ID, client.ContainerLogsOptions{
					ShowStdout: true,
					ShowStderr: true,
				})
				if lerr != nil {
					continue
				}
				var buf bytes.Buffer
				_, _ = stdcopy.StdCopy(&buf, &buf, res)
				_ = res.Close()
				combined += buf.String()
			}
			if strings.Count(combined, want) >= min {
				return combined
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("app %q logs missing %q x%d within 10s; logs:\n%s", appName, want, min, combined)
	return ""
}

// dockerManagerAdapter wraps a runtime.Manager to satisfy the reconciler's
// Builder interface (the Manager already has the methods). It also implements
// the optional selectionPreparer seam so the integration test exercises the same
// fingerprint + selection handoff production uses.
type dockerManagerAdapter struct{ m *runtime.Manager }

func (a dockerManagerAdapter) Prepare(
	ctx context.Context,
	fn app.App,
) (*runtime.Prepared, error) {
	return a.m.Prepare(ctx, fn)
}

func (a dockerManagerAdapter) PrepareWithFingerprintAndSelection(
	ctx context.Context,
	fn app.App,
	fingerprint string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	return a.m.PrepareWithFingerprintAndSelection(ctx, fn, fingerprint, selection)
}
func (a dockerManagerAdapter) Execute(
	ctx context.Context,
	prepared *runtime.Prepared,
	handler string,
	eventJSON []byte,
	extraEnv []string,
) error {
	return a.m.Execute(ctx, prepared, handler, eventJSON, extraEnv)
}

// The adapter must forward BOTH halves of the fenced preparation/publication
// seam. With only selectionPreparer, the reconciler would use the Manager's
// deferred preparation and never run its matching fenced publication/activation.
func (a dockerManagerAdapter) RefreshRuntimeImageSelections(ctx context.Context) ([]string, error) {
	return a.m.RefreshRuntimeImageSelections(ctx)
}

func (a dockerManagerAdapter) PublishPrepared(
	ctx context.Context,
	prepared *runtime.Prepared,
	publish func() error,
) error {
	return a.m.PublishPrepared(ctx, prepared, publish)
}

// SetAppResources implements the optional resourceSetter seam so the
// adapter exercises the same resource-only hot-change path production uses.
func (a dockerManagerAdapter) SetAppResources(name string, limits app.ResourceLimits) {
	a.m.SetAppResources(name, limits)
}

// writeFn writes a node app directory: template + handler.
func writeNodeFn(t *testing.T, root, name, output string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	tmpl := "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	src := "export function hi(e, context){ console.log(\"" + output + "\"); }\n"
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte(src), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
}

// runHandlerWith builds the image and executes one event; handler output is
// then observed through the container's native Docker logs by the caller.
func runHandlerWith(
	t *testing.T,
	m *runtime.Manager,
	fn app.App,
	hndlr,
	eventJSON string,
) {
	t.Helper()
	prepared, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare %s: %v", fn.Name, err)
	}
	if err := m.Execute(context.Background(), prepared, hndlr, []byte(eventJSON), nil); err != nil {
		t.Fatalf("execute %s: %v", fn.Name, err)
	}
}

// TestReconcilerReloadIntegration drives the full live-reload flow against a real
// Docker daemon: discover -> change & rebuild -> broken template retained ->
// fix -> remove. It mirrors the compose verification from the task.
func TestReconcilerReloadIntegration(t *testing.T) {
	testutil.RequireDocker(t)

	root := t.TempDir()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	// Derive a unique app name from the test name and a nanosecond stamp so
	// concurrent runs against one daemon cannot collide on the image repo or the
	// app directory, and scope cleanup to that prefix; testutil.UniqueName
	// keeps it within app.ValidName's 63-char cap even for a long test name.
	name := testutil.UniqueName(t, "recon")

	var buf bytes.Buffer
	// Relay-operational logger. App stdout/stderr are the container's NATIVE
	// Docker logs — Relay never forwards them — so the handler-output
	// assertions below read those logs (see awaitReconcilerAppLogs), not from
	// this operational log buffer (which the secret-leak assertions read).
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// The worker-global NETWORKS set is optional application connectivity;
	// this test owns a dedicated one as the operator would. The execution
	// channel instead runs over the manager's internal control network,
	// provisioned below exactly like the worker preflight.
	networkName := createReconcilerNetwork(t, testutil.RequireDocker(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	m, err := runtime.NewManager(logger, nil, "test-host",
		runtime.WithNetworks([]string{networkName}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	if err := m.EnsureControlNetwork(ctx); err != nil {
		t.Fatalf("ensure control network: %v", err)
	}

	// Track the relay-app-<name>:* images this test builds (v1/v2/v3 via
	// fingerprint-tagged refs) so t.Cleanup removes them; the reconciler's
	// rebuilds leave superseded versions behind. Removal is scoped strictly to
	// the derived name, never unrelated images.
	cleanupCli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker client for cleanup: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		defer cleanupCli.Close()
		// Daemon-side ownership filter: only THIS app's managed app images
		// (relay.type=app AND relay.app=<name>), so cleanup never enumerates
		// the whole shared daemon and the repo-prefix match below can only ever
		// see candidates the label filter already narrowed to this app.
		imgs, err := cleanupCli.ImageList(cleanupCtx, client.ImageListOptions{
			Filters: make(client.Filters).
				Add("label", "relay.type="+runtime.ImageTypeApp, "relay.app="+name),
		})
		if err != nil {
			t.Logf("cleanup: image list: %v", err)
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-app-"+name+":") {
					if _, err := cleanupCli.ImageRemove(cleanupCtx, tag, client.ImageRemoveOptions{Force: true}); err != nil {
						t.Logf("cleanup: remove %s: %v", tag, err)
					}
					break
				}
			}
		}
	})

	adapter := dockerManagerAdapter{m}

	// a) Discover a brand-new app via reconcile.
	writeNodeFn(t, root, name, "hello-v1")
	reg := &runner.Registry{}
	reg.Set(nil)
	r := New(
		Config{
			Root:     root,
			Debounce: 20 * time.Millisecond,
			Interval: time.Hour,
		},
		reg,
		adapter,
		logger,
	)
	r.reconcileApp(name)

	if pf := reg.GetByName(name); pf == nil || pf.Prepared() == nil {
		t.Fatal("function should be discovered and prepared")
	}
	// Execute the freshly built image for v1.
	fn := app.App{Name: name, Dir: filepath.Join(root, name), Template: mustParse(templateWithInsert())}
	runHandlerWith(t, m, fn, "index.hi", `{"event_name":"INSERT"}`)
	_ = awaitReconcilerAppLogs(t, ctx, cleanupCli, name, "hello-v1", 1)

	// b) Change source; unchanged template. Fingerprint changes -> rebuild.
	writeNodeFn(t, root, name, "hello-v2")
	r.reconcileApp(name)
	runHandlerWith(t, m, fn, "index.hi", `{"event_name":"INSERT"}`)
	_ = awaitReconcilerAppLogs(t, ctx, cleanupCli, name, "hello-v2", 1)
	if pf := reg.GetByName(name); pf == nil || pf.Prepared() == nil {
		t.Fatal("function must stay prepared after reload")
	}

	// c) Break template -> old version retained, not removed.
	tmplPath := filepath.Join(root, name, "template.yaml")
	if err := os.WriteFile(tmplPath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp(name)
	if pf := reg.GetByName(name); pf == nil || pf.Prepared() == nil {
		t.Fatal("broken template must not drop the active version")
	}
	// Old image still runs (v2) because the running snapshot is unchanged.
	// The WARM container already logged hello-v2 in stage (b): the count>=2
	// requirement proves THIS stage re-executed the old version rather than
	// satisfying the assertion from stale logs.
	runHandlerWith(t, m, fn, "index.hi", `{"event_name":"INSERT"}`)
	_ = awaitReconcilerAppLogs(t, ctx, cleanupCli, name, "hello-v2", 2)

	// d) Fix template -> rebuild succeeds.
	writeNodeFn(t, root, name, "hello-v3")
	r.reconcileApp(name)
	runHandlerWith(t, m, fn, "index.hi", `{"event_name":"INSERT"}`)
	_ = awaitReconcilerAppLogs(t, ctx, cleanupCli, name, "hello-v3", 1)

	// e) Remove dir -> app dropped.
	if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
		t.Fatalf("remove function dir: %v", err)
	}
	r.reconcileApp(name)
	if reg.GetByName(name) != nil {
		t.Fatal("function should be removed from the registry when its dir vanishes")
	}
}

func templateWithInsert() string {
	return "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"
}
