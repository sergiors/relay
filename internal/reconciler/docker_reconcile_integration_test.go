//go:build integration

// This file exercises the reconciler's hot-reload flow end to end against a
// real Docker daemon: discover -> change & rebuild -> broken template retained
// -> fix -> remove.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES a reachable Docker
// daemon; a missing dependency fails the affected tests rather than skipping
// them. Start the documented dev dependencies with
// `docker compose -f compose.dev.yaml up -d`. The daemon is located via
// client.FromEnv, so DOCKER_HOST, the local socket, and a socket proxy are all
// respected.
package reconciler

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// dockerManagerAdapter wraps a runtime.Manager to satisfy the reconciler's
// Builder interface (the Manager already has both methods).
type dockerManagerAdapter struct{ m *runtime.Manager }

// requireDocker fails the test immediately when the Docker Engine API daemon
// cannot be reached via client.FromEnv (DOCKER_HOST, socket, socket proxy are
// all respected). Integration tests fundamentally require Docker; missing
// infrastructure fails rather than skips.
func requireDocker(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker integration test requires a Docker daemon (client: %v)", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Fatalf("docker integration test requires a reachable Docker daemon (ping: %v); start one or run `docker compose -f compose.dev.yaml up -d`", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

func (a dockerManagerAdapter) Prepare(
	ctx context.Context,
	fn function.Function,
) (*runtime.Prepared, error) {
	return a.m.Prepare(ctx, fn)
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

// writeFn writes a node function directory: template + handler.
func writeNodeFn(t *testing.T, root, name, output string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	tmpl := "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	src := "export function hi(e){ console.log(\"" + output + "\"); }\n"
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte(src), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
}

// runHandlerWith builds the image and executes one event, capturing stdout.
func runHandlerWith(
	t *testing.T,
	m *runtime.Manager,
	buf *bytes.Buffer,
	fn function.Function,
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
	requireDocker(t)

	root := t.TempDir()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	m, err := runtime.NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	// Track the relay-fn-example:* images this test builds (v1/v2/v3 via
	// fingerprint-tagged refs) so t.Cleanup removes them; the reconciler's
	// rebuilds leave superseded versions behind. Removal is scoped strictly to
	// the function name this test creates ("example"), never unrelated images.
	cleanupCli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker client for cleanup: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		defer cleanupCli.Close()
		imgs, err := cleanupCli.ImageList(cleanupCtx, client.ImageListOptions{All: true})
		if err != nil {
			t.Logf("cleanup: image list: %v", err)
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-example:") {
					if _, err := cleanupCli.ImageRemove(cleanupCtx, tag, client.ImageRemoveOptions{Force: true}); err != nil {
						t.Logf("cleanup: remove %s: %v", tag, err)
					}
					break
				}
			}
		}
	})

	adapter := dockerManagerAdapter{m}

	// a) Discover a brand-new function via reconcile.
	writeNodeFn(t, root, "example", "hello-v1")
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
	r.reconcileFunction("example")

	if pf := reg.GetByName("example"); pf == nil || pf.Prepared() == nil {
		t.Fatal("example should be discovered and prepared")
	}
	// Execute the freshly built image for v1.
	fn := function.Function{Name: "example", Dir: filepath.Join(root, "example"), Template: mustParse(templateWithInsert())}
	runHandlerWith(t, m, &buf, fn, "index.hi", `{"event_name":"INSERT"}`)
	if !bytes.Contains(buf.Bytes(), []byte("hello-v1")) {
		t.Fatalf("expected v1 output, got: %s", buf.String())
	}

	// b) Change source; unchanged template. Fingerprint changes -> rebuild.
	buf.Reset()
	writeNodeFn(t, root, "example", "hello-v2")
	r.reconcileFunction("example")
	runHandlerWith(t, m, &buf, fn, "index.hi", `{"event_name":"INSERT"}`)
	if !bytes.Contains(buf.Bytes(), []byte("hello-v2")) {
		t.Fatalf("expected v2 output after reload, got: %s", buf.String())
	}
	if pf := reg.GetByName("example"); pf == nil || pf.Prepared() == nil {
		t.Fatal("example must stay prepared after reload")
	}

	// c) Break template -> old version retained, not removed.
	buf.Reset()
	tmplPath := filepath.Join(root, "example", "template.yaml")
	if err := os.WriteFile(tmplPath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("example")
	if pf := reg.GetByName("example"); pf == nil || pf.Prepared() == nil {
		t.Fatal("broken template must not drop the active version")
	}
	// Old image still runs (v2) because the running snapshot is unchanged.
	runHandlerWith(t, m, &buf, fn, "index.hi", `{"event_name":"INSERT"}`)
	if !bytes.Contains(buf.Bytes(), []byte("hello-v2")) {
		t.Fatalf("old version must keep running after broken template, got: %s", buf.String())
	}

	// d) Fix template -> rebuild succeeds.
	buf.Reset()
	writeNodeFn(t, root, "example", "hello-v3")
	r.reconcileFunction("example")
	runHandlerWith(t, m, &buf, fn, "index.hi", `{"event_name":"INSERT"}`)
	if !bytes.Contains(buf.Bytes(), []byte("hello-v3")) {
		t.Fatalf("expected v3 output after fix, got: %s", buf.String())
	}

	// e) Remove dir -> function dropped.
	if err := os.RemoveAll(filepath.Join(root, "example")); err != nil {
		t.Fatalf("remove example: %v", err)
	}
	r.reconcileFunction("example")
	if reg.GetByName("example") != nil {
		t.Fatal("example should be removed from the registry when its dir vanishes")
	}
}

func templateWithInsert() string {
	return "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"
}
