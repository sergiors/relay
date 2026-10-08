package runtime

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
)

// writeNodeTestFile writes a file under dir, failing the test on error. It is the
// unit-test (no integration build tag) counterpart of integration_helpers'
// writeFile, which is compiled only under -tags=integration.
func writeNodeTestFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// nodeMountedPlan returns the exact Node source-mounted build plan the Manager
// computes for fn, so a test can pin the relay.bootstrap label an existing image
// must carry to be reused. It mirrors the Manager's own resolution: the runtime
// spec with SourceMounted set and the template's declared handler modules.
func nodeMountedPlan(t *testing.T, fn app.App) plan.BuildPlan {
	t.Helper()
	if fn.Template == nil {
		t.Fatalf("function %q has no template", fn.Name)
	}
	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		t.Fatalf("lookup %q: %v", fn.Template.Runtime, err)
	}
	spec.SourceMounted = true
	p, err := node.Engine{}.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan %q: %v", fn.Name, err)
	}
	return p
}

// TestPrepareSourceMountNodeSourceOnlyChangeDoesNotBuildDependency is the Node
// counterpart of the Python source-only proof: after a SOURCE-ONLY edit the
// dependency fingerprint/reference and the (source-independent) app image are
// unchanged and NEITHER a dependency nor an app build is issued. Dependency and
// app builds are counted separately, so a source edit can never be mistaken for a
// dependency rebuild.
func TestPrepareSourceMountNodeSourceOnlyChangeDoesNotBuildDependency(t *testing.T) {
	dir := t.TempDir()
	writeNodeTestFile(t, dir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`)
	writeNodeTestFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeNodeTestFile(t, dir, "index.js", "import colors from 'picocolors';\nexport function handler(e){ console.log(colors.red('v1')); }\n")
	fn := app.App{Name: "node-source-only", Dir: dir, Template: &app.Template{Runtime: "node24"}}

	var first buildCounts
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		countBuildsRoute(&first, nil, nil),
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true
	preparedFirst, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if preparedFirst.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if first.dep != 1 || first.app != 1 {
		t.Fatalf("first prepare builds = dep %d app %d, want 1 and 1", first.dep, first.app)
	}

	// Source-only edit: package.json (the dependency input) is untouched.
	writeNodeTestFile(t, dir, "index.js", "import colors from 'picocolors';\nexport function handler(e){ console.log(colors.red('v2')); }\n")

	// Both the dependency image and the source-independent app image are present
	// with the current bootstrap label, so Prepare must reuse them.
	present := fmt.Sprintf(`{"Id":"sha256:deadbeef","Config":{"Labels":{%q:%q}}}`, labelBootstrap, bootstrapHash(nodeMountedPlan(t, fn)))

	var second buildCounts
	cli2 := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", body: present},
		countBuildsRoute(&second, nil, nil),
	)
	m2 := newLifecycleManager(t, cli2, context.Background())
	m2.sourceMount = true
	preparedSecond, err := m2.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if second.dep != 0 || second.app != 0 {
		t.Fatalf("source-only edit builds = dep %d app %d, want 0 and 0", second.dep, second.app)
	}
	if preparedSecond.Dependency != preparedFirst.Dependency {
		t.Fatalf("source-only edit changed the dependency reference: %q -> %q", preparedFirst.Dependency, preparedSecond.Dependency)
	}
	if preparedSecond.Image != preparedFirst.Image {
		t.Fatalf("source-only edit changed the app image: %q -> %q", preparedFirst.Image, preparedSecond.Image)
	}
	if preparedSecond.Fingerprint == preparedFirst.Fingerprint {
		t.Fatal("source-only edit must still advance the source fingerprint (the reconcile generation)")
	}
}

// TestPrepareSourceMountNodeDependencyChangeBuildsNewGeneration is the Node
// counterpart of the Python dependency-input rebuild proof: mutating
// package.json yields a DIFFERENT dependency fingerprint/reference and a NEW
// dependency image generation (built from the edited bytes), and the app image is
// rebuilt FROM that new dependency reference. Dependency and app builds are
// counted separately, so the new generation cannot be masked by a source-only
// reuse.
func TestPrepareSourceMountNodeDependencyChangeBuildsNewGeneration(t *testing.T) {
	dir := t.TempDir()
	writeNodeTestFile(t, dir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`)
	writeNodeTestFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeNodeTestFile(t, dir, "index.js", "import colors from 'picocolors';\nexport function handler(e){ console.log(colors.green('v1')); }\n")
	fn := app.App{Name: "node-dep-change", Dir: dir, Template: &app.Template{Runtime: "node24"}}

	var first buildCounts
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		countBuildsRoute(&first, nil, nil),
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true
	preparedFirst, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if preparedFirst.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if first.dep != 1 || first.app != 1 {
		t.Fatalf("first prepare builds = dep %d app %d, want 1 and 1", first.dep, first.app)
	}
	depFirst := preparedFirst.Dependency
	imageFirst := preparedFirst.Image

	// Mutate the manifest (a dependency-blocking change) AND the source.
	writeNodeTestFile(t, dir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0","ms":"2.1.3"}}`)
	writeNodeTestFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolorsMs)
	writeNodeTestFile(t, dir, "index.js", "import colors from 'picocolors';\nimport ms from 'ms';\nexport function handler(e){ console.log(colors.green('v2 ' + ms(1500))); }\n")

	var second buildCounts
	var depCtx, appCtx []byte
	cli2 := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		countBuildsRoute(&second, &depCtx, &appCtx),
	)
	m2 := newLifecycleManager(t, cli2, context.Background())
	m2.sourceMount = true
	preparedSecond, err := m2.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if preparedSecond.Dependency == depFirst {
		t.Fatalf("manifest change kept the dependency reference %q; want a new generation", depFirst)
	}
	if preparedSecond.Image == imageFirst {
		t.Fatalf("manifest change kept the app image %q; want a rebuild FROM the new dependency", imageFirst)
	}
	if second.dep != 1 || second.app != 1 {
		t.Fatalf("manifest change builds = dep %d app %d, want 1 and 1 (a new generation is BUILT)", second.dep, second.app)
	}
	if got := string(stagedFileBytes(t, depCtx, "package.json")); !strings.Contains(got, `"ms"`) || !strings.Contains(got, "2.1.3") {
		t.Fatalf("dependency build staged %q, want the edited manifest with ms 2.1.3", got)
	}
	df := string(stagedFileBytes(t, appCtx, "Dockerfile"))
	if !strings.Contains(df, "FROM "+preparedSecond.Dependency+"\n") {
		t.Fatalf("app image must build FROM the new dependency reference:\n%s", df)
	}
}
