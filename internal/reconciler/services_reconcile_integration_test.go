//go:build integration

// This file drives the service reconciler end to end through the reconciler's
// unexported reconcileFunction against a real Docker daemon: it creates a temp
// function dir with a services template, builds/reconciles 2 replicas, scales to
// 1, then removes the dir and asserts 0 containers + the image is removed. It
// mirrors reconciler_integration_test.go's dockerManagerAdapter pattern.
package reconciler

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// TestServicesReconcileIntegration reconciles a function's services end to end.
func TestServicesReconcileIntegration(t *testing.T) {
	requireDocker(t)

	root := t.TempDir()
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	m, err := runtime.NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	// Manager is both the reconcile Builder and the services Docker seam.
	adapter := dockerManagerAdapter{m}

	// Write a node function dir that declares one service (port 3000).
	name := "svc-reconcile"
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(serviceReconcileTemplate(3000, 2)), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app", "service.js"), []byte("setInterval(() => {}, 1 << 30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}

	// Cleanup: stop every service container belonging to this function and remove
	// every relay-fn-svc-reconcile:* image this test builds.
	cleanupCli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("cleanup client: %v", err)
	}
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		defer cleanupCli.Close()
		if c, err := m.ServiceContainerList(cc); err == nil {
			_ = m.StopServiceContainers(cc, c)
		}
		imgs, err := cleanupCli.ImageList(cc, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-"+name+":") {
					_, _ = cleanupCli.ImageRemove(cc, tag, client.ImageRemoveOptions{Force: true})
					break
				}
			}
		}
	})

	reg := &runner.Registry{}
	reg.Set(nil)

	// Wire the reconciler's UpdateServices/RemoveServices hooks to the services
	// reconciler, so discovery/update converge service containers and removal
	// stops them (before the images would be retired by the worker's removal
	// hook, which this test does not wire).
	svcCtrl := NewServiceReconciler(m, nil, logger)
	rec := New(
		Config{
			Root:     root,
			Debounce: 20 * time.Millisecond,
			Interval: time.Hour,
			UpdateServices: func(fnName string, tmpl *function.Template, image string) {
				ctx := context.Background()
				svcCtrl.Apply(ctx, fnName, tmpl, image, nil)
			},
			RemoveServices: func(fnName string) {
				svcCtrl.Remove(context.Background(), fnName)
			},
		},
		reg,
		adapter,
		logger,
	)

	// a) Discover + prepare; reconcile starts 2 running service replicas.
	rec.reconcileFunction(name)
	if pf := reg.GetByName(name); pf == nil || pf.Prepared() == nil {
		t.Fatal("function should be discovered and prepared")
	}
	assertServiceCounts(t, m, name, 2)

	// b) Scale down: edit template replicas 2 -> 1, re-reconcile.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(serviceReconcileTemplate(3000, 1)), 0o644); err != nil {
		t.Fatalf("write scaled template: %v", err)
	}
	rec.reconcileFunction(name)
	assertServiceCounts(t, m, name, 1)

	// c) Remove the dir; reconcile drops the function and the RemoveServices
	// hook stops+removes its service containers. The images would then be
	// retired by the worker's RemoveFunction hook (not wired here), so this test
	// asserts the container side converges to zero.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	rec.reconcileFunction(name)
	if reg.GetByName(name) != nil {
		t.Fatal("function should be removed from the registry")
	}
	assertServiceCounts(t, m, name, 0)
}

// serviceReconcileTemplate renders a node24 template declaring one service.
func serviceReconcileTemplate(port, replicas int) string {
	return "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\nservices:\n  - entrypoint: app/service.js\n    port: 3000\n    replicas: " + strconv.Itoa(replicas) + "\n"
}

// assertServiceCounts waits (up to 20s) until exactly want running service
// containers exist for name.
func assertServiceCounts(t *testing.T, m *runtime.Manager, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		list, err := m.ServiceContainerList(context.Background())
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		n := 0
		allRunning := true
		for _, c := range list {
			if c.Function == name {
				if c.State != container.StateRunning {
					allRunning = false
				}
				n++
			}
		}
		if n == want && allRunning {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	list, _ := m.ServiceContainerList(context.Background())
	got := 0
	for _, c := range list {
		if c.Function == name {
			got++
		}
	}
	t.Fatalf("expected %d running service containers for %s, got %d", want, name, got)
}
