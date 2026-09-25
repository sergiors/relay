package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/testutil"
)

// newClockManager builds a Manager directly (no Docker) with a scripted client,
// a worker identity, and a pinned clock, so service-source resolution is
// deterministic without a daemon or a mutable global.
func newClockManager(t *testing.T, cli *client.Client, now func() time.Time) *Manager {
	t.Helper()
	m := &Manager{
		log:        testutil.DiscardLogger(),
		cli:        cli,
		hostname:   "test-host",
		now:        now,
		pullChecks: map[string]time.Time{},
		containers: newContainerCache(),
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// TestResolveEntrypointServiceUsesFunctionImage: an `entrypoint` source resolves
// to the function's own image plus the runtime-specific launch command, touching
// no Docker source resolution.
func TestResolveEntrypointServiceUsesFunctionImage(t *testing.T) {
	m := newClockManager(t, nil, time.Now)
	tmpl := &function.Template{Runtime: "node24"}
	svc := function.Service{Entrypoint: "app/service.js", Port: 80, Replicas: 1}

	got, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, "relay-fn-fn:abc")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Ref != "relay-fn-fn:abc" {
		t.Fatalf("ref = %q, want the function image", got.Ref)
	}
	if len(got.Entry) != 2 || got.Entry[0] != "node" || got.Entry[1] != "/app/app/service.js" {
		t.Fatalf("entry = %v, want [node /app/app/service.js]", got.Entry)
	}
	if got.ID != "" {
		t.Fatalf("id = %q, want empty for a Relay content-addressed image", got.ID)
	}
}

// TestServiceBuildImageRefIdentityAndContentAddressed: the build image reference
// folds the service identity into the source fingerprint, so two build services
// with identical source but different Dockerfiles are distinct images, while an
// unchanged (identity, fingerprint) pair is stable.
func TestServiceBuildImageRefIdentityAndContentAddressed(t *testing.T) {
	a := serviceBuildImageRef("fn", "Dockerfile", "fp")
	b := serviceBuildImageRef("fn", "docker/Dockerfile.prod", "fp")
	c := serviceBuildImageRef("fn", "Dockerfile", "fp2")
	d := serviceBuildImageRef("fn", "Dockerfile", "fp")

	if a == b {
		t.Fatalf("distinct Dockerfile identities share a ref: %q", a)
	}
	if a == c {
		t.Fatalf("different fingerprints share a ref: %q", a)
	}
	if a != d {
		t.Fatalf("identical identity+fingerprint differ: %q vs %q", a, d)
	}
	if !strings.HasPrefix(a, "relay-fn-fn:") {
		t.Fatalf("ref %q not in the function's image repo", a)
	}
}

// TestResolveBuildServiceReusesExistingImage: when the content-addressed build
// image already exists locally, resolution short-circuits without a build.
func TestResolveBuildServiceReusesExistingImage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	svc := function.Service{Build: "Dockerfile", Port: 80, Replicas: 1}
	fp, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	ref := serviceBuildImageRef("fn", "Dockerfile", fp)

	// Inspect returns a present image; no build route exists, so a build attempt
	// would fail the scripted client loudly.
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", body: `{"Id":"sha256:abc"}`},
	)
	m := newClockManager(t, cli, time.Now)

	got, err := m.ResolveServiceImage(context.Background(), "fn", dir, &function.Template{Runtime: "node24"}, svc, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Ref != ref {
		t.Fatalf("ref = %q, want the content-addressed build image %q", got.Ref, ref)
	}
	if got.Entry != nil {
		t.Fatalf("entry = %v, want nil so the image ENTRYPOINT/CMD is preserved", got.Entry)
	}
}

// TestResolveBuildServiceFingerprintInvalidatesOnSelectedSourceChange: editing a
// file selected by the function's .gitignore-driven selection changes the build
// image reference, while editing an ignored file does not — the invalidation
// reuses the existing source/.gitignore selection policy.
func TestResolveBuildServiceFingerprintInvalidatesOnSelectedSourceChange(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("Dockerfile", "FROM scratch\n")
	write("app.js", "v1\n")
	write(".gitignore", "ignored.txt\n")
	write("ignored.txt", "junk\n")

	refFor := func() string {
		t.Helper()
		fp, err := function.Fingerprint(dir)
		if err != nil {
			t.Fatalf("fingerprint: %v", err)
		}
		return serviceBuildImageRef("fn", "Dockerfile", fp)
	}

	base := refFor()

	// An edited SELECTED file invalidates.
	write("app.js", "v2\n")
	afterSelected := refFor()
	if afterSelected == base {
		t.Fatal("editing a selected source file did not invalidate the build image")
	}

	// An edited IGNORED file does not invalidate.
	write("ignored.txt", "different junk\n")
	if got := refFor(); got != afterSelected {
		t.Fatalf("editing an ignored file changed the build image ref: %q -> %q", afterSelected, got)
	}

	// Editing the Dockerfile itself (selected) invalidates too.
	write("Dockerfile", "FROM scratch\n# changed\n")
	if got := refFor(); got == afterSelected {
		t.Fatal("editing the selected Dockerfile did not invalidate the build image")
	}
}

// TestPullDueClockSeam: the pull cadence is driven by the injected clock and the
// last SUCCESSFUL check, with no package-global time.
func TestPullDueClockSeam(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	m := newClockManager(t, nil, func() time.Time { return now })

	if !m.pullDue("fn", "nginx:1") {
		t.Fatal("a first sighting must be pull-due")
	}
	m.recordPullCheck("fn", "nginx:1", now)
	if m.pullDue("fn", "nginx:1") {
		t.Fatal("a just-recorded successful check must not be pull-due")
	}
	now = base.Add(serviceImagePullInterval - time.Second)
	if m.pullDue("fn", "nginx:1") {
		t.Fatal("pull-due before the interval elapsed")
	}
	now = base.Add(serviceImagePullInterval)
	if !m.pullDue("fn", "nginx:1") {
		t.Fatal("pull-due at the interval boundary")
	}
	// A different identity (independent service) has its own window.
	if !m.pullDue("fn", "nginx:2") {
		t.Fatal("an unseen identity must be pull-due")
	}
	// A different function has its own window.
	if !m.pullDue("other", "nginx:1") {
		t.Fatal("an unseen function must be pull-due")
	}
}

// TestResolveExternalServiceImagePullAtMostHourlyAndImmediateOnChange pins the
// full external-image policy with a counting scripted daemon: the first resolve
// pulls, an immediate second resolve does not, a source identity change pulls
// immediately, and after the interval another pull occurs.
func TestResolveExternalServiceImagePullAtMostHourlyAndImmediateOnChange(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	pulls := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/images/create", body: "{}", onMatch: func() { pulls++ }},
		dockerRoute{method: http.MethodGet, path: "/images/", body: `{"Id":"sha256:cafe"}`},
	)
	m := newClockManager(t, cli, func() time.Time { return now })

	tmpl := &function.Template{Runtime: "node24"}
	svc := function.Service{Image: "ghcr.io/acme/api:1.2", Port: 80, Replicas: 1}

	got, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, "")
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if got.Ref != "ghcr.io/acme/api:1.2" || got.ID != "sha256:cafe" {
		t.Fatalf("resolved = %+v, want the image ref with content id", got)
	}
	if pulls != 1 {
		t.Fatalf("pulls after first resolve = %d, want 1", pulls)
	}

	// Second resolve within the hour: no remote pull.
	if _, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, ""); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if pulls != 1 {
		t.Fatalf("pulls within the freshness window = %d, want still 1", pulls)
	}

	// A changed source identity (a different image) is checked immediately.
	changed := function.Service{Image: "ghcr.io/acme/api:1.3", Port: 80, Replicas: 1}
	if _, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, changed, ""); err != nil {
		t.Fatalf("changed resolve: %v", err)
	}
	if pulls != 2 {
		t.Fatalf("pulls after a source change = %d, want 2 (immediate)", pulls)
	}

	// After the interval, the original identity is checked again.
	now = base.Add(serviceImagePullInterval)
	if _, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, ""); err != nil {
		t.Fatalf("post-interval resolve: %v", err)
	}
	if pulls != 3 {
		t.Fatalf("pulls after the interval = %d, want 3", pulls)
	}
}

// TestResolveExternalServiceImageFailedPullDoesNotAdvance: a failed pull is
// surfaced and does NOT advance the freshness window, so the next pass retries.
func TestResolveExternalServiceImageFailedPullDoesNotAdvance(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pulls := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/images/create", status: http.StatusInternalServerError, body: `{"message":"registry down"}`, onMatch: func() { pulls++ }},
		dockerRoute{method: http.MethodGet, path: "/images/", body: `{"Id":"sha256:cafe"}`},
	)
	m := newClockManager(t, cli, func() time.Time { return now })
	tmpl := &function.Template{Runtime: "node24"}
	svc := function.Service{Image: "ghcr.io/acme/api:1.2", Port: 80, Replicas: 1}

	if _, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, ""); err == nil {
		t.Fatal("expected the failed pull to surface")
	}
	if pulls != 1 {
		t.Fatalf("pulls = %d, want 1", pulls)
	}
	if !m.pullDue("fn", svc.Image) {
		t.Fatal("a failed pull must not advance the freshness window")
	}
	// The next pass retries even though no time has advanced.
	if _, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, ""); err == nil {
		t.Fatal("expected the retry to fail too")
	}
	if pulls != 2 {
		t.Fatalf("pulls = %d, want 2 (retried without advancing time)", pulls)
	}
}

// TestResolveExternalServiceImageMissingLocalPullsImmediately: a locally absent
// image is pulled immediately even when the freshness window is satisfied — there
// is no healthy container to preserve, and a failed pull surfaces rather than
// fabricating a container.
func TestResolveExternalServiceImageMissingLocalPullsImmediately(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pulls := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/images/create", status: http.StatusInternalServerError, body: `{"message":"registry down"}`, onMatch: func() { pulls++ }},
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
	)
	m := newClockManager(t, cli, func() time.Time { return now })
	// A recorded successful check would suppress a remote check for a PRESENT
	// image, but a missing local image must still be pulled.
	m.recordPullCheck("fn", "ghcr.io/acme/api:1.2", now)
	tmpl := &function.Template{Runtime: "node24"}
	svc := function.Service{Image: "ghcr.io/acme/api:1.2", Port: 80, Replicas: 1}

	_, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, "")
	if err == nil || !strings.Contains(err.Error(), "registry down") {
		t.Fatalf("err = %v, want the pull failure surfaced", err)
	}
	if pulls != 1 {
		t.Fatalf("pulls = %d, want 1 (missing local image pulled despite the window)", pulls)
	}
}

// TestResolveExternalServiceImagePresentWithinWindowSkipsPull: a present image
// within the freshness window is not re-pulled.
func TestResolveExternalServiceImagePresentWithinWindowSkipsPull(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pulls := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/images/create", body: "{}", onMatch: func() { pulls++ }},
		dockerRoute{method: http.MethodGet, path: "/images/", body: `{"Id":"sha256:cafe"}`},
	)
	m := newClockManager(t, cli, func() time.Time { return now })
	m.recordPullCheck("fn", "ghcr.io/acme/api:1.2", now)
	tmpl := &function.Template{Runtime: "node24"}
	svc := function.Service{Image: "ghcr.io/acme/api:1.2", Port: 80, Replicas: 1}

	got, err := m.ResolveServiceImage(context.Background(), "fn", t.TempDir(), tmpl, svc, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Ref != "ghcr.io/acme/api:1.2" || got.ID != "sha256:cafe" {
		t.Fatalf("resolved = %+v, want the present image with content id", got)
	}
	if pulls != 0 {
		t.Fatalf("pulls = %d, want 0 for a present image within the window", pulls)
	}
}

// TestForgetServicePullChecks scopes removal to one function.
func TestForgetServicePullChecks(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := newClockManager(t, nil, func() time.Time { return now })
	m.recordPullCheck("a", "img:1", now)
	m.recordPullCheck("b", "img:1", now)

	m.forgetServicePullChecks("a")

	if m.pullDue("a", "img:1") != true {
		t.Fatal("forgotten function must be pull-due again")
	}
	if m.pullDue("b", "img:1") != false {
		t.Fatal("other function's window must be preserved")
	}
}

// TestPrepareRuntimeLessTemplateSucceeds: a template whose only services use
// build/image sources has no runtime and no function image; Prepare still
// succeeds (the function is available for service convergence) and carries the
// source fingerprint, without attempting a build.
func TestPrepareRuntimeLessTemplateSucceeds(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	tmpl := &function.Template{Services: []function.Service{{Build: "Dockerfile", Port: 80, Replicas: 1}}}
	m := newClockManager(t, nil, time.Now)

	got, err := m.Prepare(context.Background(), function.Function{Name: "fn", Dir: dir, Template: tmpl})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got.Image != "" {
		t.Fatalf("image = %q, want empty for a runtime-less template", got.Image)
	}
	if got.Fingerprint == "" {
		t.Fatal("fingerprint must still be computed for a runtime-less template")
	}
}
