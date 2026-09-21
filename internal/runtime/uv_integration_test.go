//go:build integration

// Python dependency integration tests for the uv-based install paths: the
// classic requirements.txt path (installed with uv, not pip) and the native
// uv-project path (pyproject.toml + a committed uv.lock, installed frozen), plus
// the guarantee that the uv binary is present even with no dependencies.
package runtime

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/testutil"
)

// runImageCommand runs a one-off container from image (overriding its
// ENTRYPOINT) and returns the combined stdout+stderr. It is used to probe the
// built image's filesystem (e.g. that the uv binary is present) without going
// through the execution-container protocol. The container is force-removed via
// t.Cleanup even on failure.
func runImageCommand(ctx context.Context, t *testing.T, image string, argv []string) (string, error) {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()

	createResp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:      image,
			Entrypoint: argv,
			Labels:     map[string]string{"relay.test": "uv-probe"},
		},
	})
	if err != nil {
		t.Fatalf("create probe container: %v", err)
	}
	id := createResp.ID
	t.Cleanup(func() { _ = removeContainer(cli, id) })

	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start probe container: %v", err)
	}
	wait := cli.ContainerWait(ctx, id, client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	select {
	case res := <-wait.Result:
		logs, _ := readContainerLogs(ctx, cli, id)
		if res.Error != nil {
			return logs, fmt.Errorf("probe wait: %s: %s", res.Error.Message, logs)
		}
		if res.StatusCode != 0 {
			return logs, fmt.Errorf("probe exited %d: %s", res.StatusCode, logs)
		}
		return logs, nil
	case err := <-wait.Error:
		return "", err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func readContainerLogs(ctx context.Context, cli *client.Client, id string) (string, error) {
	res, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return "", err
	}
	defer res.Close()
	// Without a TTY, ContainerLogs returns the multiplexed stream, so demux
	// stdout+stderr the same way Relay's execution container does.
	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, res); err != nil {
		return out.String(), err
	}
	return out.String(), nil
}

// uvLockFixture is a committed uv.lock resolved by the pinned uv 0.12.17 for a
// project whose only dependency is six==1.16.0. Embedding the exact lock (rather
// than resolving at test time) keeps the native test deterministic and proves
// the frozen workflow never re-resolves: uv must accept this lock as-is.
const uvLockFixture = `version = 1
revision = 3
requires-python = ">=3.12"

[[package]]
name = "relay-uv-fixture"
version = "0.1.0"
source = { virtual = "." }
dependencies = [
    { name = "six" },
]

[package.metadata]
requires-dist = [{ name = "six", specifier = "==1.16.0" }]

[[package]]
name = "six"
version = "1.16.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://files.pythonhosted.org/packages/71/39/171f1c67cd00715f190ba0b100d606d440a28c93c7714febeca8b79af85e/six-1.16.0.tar.gz", hash = "sha256:1e61c37477a1626458e36f7b1d82aa5c9b094fa4802892072e49de9c60c4c926", size = 34041, upload-time = "2021-05-05T14:18:18.379Z" }
wheels = [
    { url = "https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/six-1.16.0-py2.py3-none-any.whl", hash = "sha256:8abb2f1d86890a2dfb989f9a77cfcfd3e47c2a354b01111771326f8aa26e0254", size = 11053, upload-time = "2021-05-05T14:18:17.237Z" },
]
`

const uvFixturePyproject = `[project]
name = "relay-uv-fixture"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["six==1.16.0"]
`

// uvDependencyHandler imports the fixture dependency and prints its version, so
// a successful Execute proves the dependency was installed into the interpreter
// the bootstrap uses (system site-packages).
const uvDependencyHandler = `import six

def run(event):
    print("six version " + six.__version__)
`

// newUvPythonFunction builds a Python function directory whose handler imports
// six, with the given dependency manifests written into it.
func newUvPythonFunction(t *testing.T, name string, manifests map[string]string) function.Function {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", uvDependencyHandler)
	for fileName, content := range manifests {
		writeFile(t, dir, fileName, content)
	}
	return function.Function{Name: name, Dir: dir, Template: &function.Template{Runtime: "python3.14"}}
}

// TestIntegrationRequirementsInstalledWithUvAndExecutes verifies the classic
// requirements.txt path still works end to end, now installed with uv into the
// system environment, and that the existing `python -u /relay/bootstrap.py`
// runtime entrypoint sees the dependency.
func TestIntegrationRequirementsInstalledWithUvAndExecutes(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-uv-req:"))

	fn := newUvPythonFunction(t, "uv-req", map[string]string{
		"requirements.txt": "six==1.16.0\n",
	})
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !strings.HasPrefix(prepared.Dependency, depRepoPrefix) {
		t.Fatalf("requirements function dependency = %q, want a relay-dep-* layer", prepared.Dependency)
	}
	if err := m.Execute(ctx, prepared, "handler.run", []byte(`{"status":"COMPLETED"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if logs := out.String(); !strings.Contains(logs, "six version 1.16.0") {
		t.Errorf("expected the requirements dependency to be importable at runtime, got: %s", logs)
	}
}

// TestIntegrationNativeUvProjectExecutes verifies the native uv path end to end:
// pyproject.toml + a committed uv.lock builds a frozen dependency layer and the
// handler can import the locked dependency at runtime. A regression that
// re-resolved the set or used pip would fail the lock assertion or the import.
func TestIntegrationNativeUvProjectExecutes(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-uv-native:"))

	fn := newUvPythonFunction(t, "uv-native", map[string]string{
		"pyproject.toml": uvFixturePyproject,
		"uv.lock":        uvLockFixture,
	})
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !strings.HasPrefix(prepared.Dependency, depRepoPrefix) {
		t.Fatalf("native function dependency = %q, want a relay-dep-* layer", prepared.Dependency)
	}
	if err := m.Execute(ctx, prepared, "handler.run", []byte(`{"status":"COMPLETED"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if logs := out.String(); !strings.Contains(logs, "six version 1.16.0") {
		t.Errorf("expected the locked native dependency to be importable at runtime, got: %s", logs)
	}
}

// TestIntegrationPythonNoDepsHasUvBinary verifies uv is copied into a Python
// runtime image even when the function declares no dependencies, by building the
// function image and running `uv --version` in a one-off container from it.
func TestIntegrationPythonNoDepsHasUvBinary(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-uv-nodeps:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	fn := function.Function{Name: "uv-nodeps", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	m, _ := newManager(t)
	p, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if p.Dependency != "" {
		t.Errorf("a function with no deps must have no dependency layer, got %q", p.Dependency)
	}

	// Run the built function image with a uv version probe as the command; the
	// image's own ENTRYPOINT must be overridden so the probe runs.
	out, err := runImageCommand(ctx, t, p.Image, []string{"uv", "--version"})
	if err != nil {
		t.Fatalf("run uv --version: %v", err)
	}
	if !strings.Contains(out, "uv 0.12.17") {
		t.Errorf("expected the pinned uv 0.12.17 binary in the image, got: %q", out)
	}
}
