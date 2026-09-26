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

	got, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "relay-fn-fn:abc")
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

	got, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "")
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
	if _, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, ""); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if pulls != 1 {
		t.Fatalf("pulls within the freshness window = %d, want still 1", pulls)
	}

	// A changed source identity (a different image) is checked immediately.
	changed := function.Service{Image: "ghcr.io/acme/api:1.3", Port: 80, Replicas: 1}
	if _, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, changed, ""); err != nil {
		t.Fatalf("changed resolve: %v", err)
	}
	if pulls != 2 {
		t.Fatalf("pulls after a source change = %d, want 2 (immediate)", pulls)
	}

	// After the interval, the original identity is checked again.
	now = base.Add(serviceImagePullInterval)
	if _, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, ""); err != nil {
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

	if _, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, ""); err == nil {
		t.Fatal("expected the failed pull to surface")
	}
	if pulls != 1 {
		t.Fatalf("pulls = %d, want 1", pulls)
	}
	if !m.pullDue("fn", svc.Image) {
		t.Fatal("a failed pull must not advance the freshness window")
	}
	// The next pass retries even though no time has advanced.
	if _, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, ""); err == nil {
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

	_, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "")
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

	got, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "")
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

// TestResolveServiceImageDispatchesBySource pins the two-source dispatch: an
// entrypoint source resolves through the runtime entry map against the function
// image, while an image source resolves to the external reference with its
// content ID. Both leave no spurious build.
func TestResolveServiceImageDispatchesBySource(t *testing.T) {
	entryTmpl := &function.Template{Runtime: "node24"}
	entrySvc := function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1}
	entry, err := newClockManager(t, nil, time.Now).ResolveServiceImage(context.Background(), "fn", entryTmpl, entrySvc, "relay-fn-fn:abc")
	if err != nil {
		t.Fatalf("entrypoint resolve: %v", err)
	}
	if entry.Ref != "relay-fn-fn:abc" || len(entry.Entry) != 2 {
		t.Fatalf("entrypoint resolved = %+v, want the function image and an entry override", entry)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cli := newScriptedDockerClient(t, dockerRoute{method: http.MethodGet, path: "/images/", body: `{"Id":"sha256:cafe"}`})
	m := newClockManager(t, cli, func() time.Time { return now })
	m.recordPullCheck("fn", "ghcr.io/acme/api:1.2", now)
	imgSvc := function.Service{Image: "ghcr.io/acme/api:1.2", Port: 80, Replicas: 1}
	img, err := m.ResolveServiceImage(context.Background(), "fn", &function.Template{}, imgSvc, "")
	if err != nil {
		t.Fatalf("image resolve: %v", err)
	}
	if img.Ref != "ghcr.io/acme/api:1.2" || img.ID != "sha256:cafe" {
		t.Fatalf("image resolved = %+v, want the external ref with its content ID", img)
	}
	if img.Entry != nil {
		t.Fatalf("image entry = %v, want nil (preserve image ENTRYPOINT/CMD)", img.Entry)
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
// external `image` sources has no runtime and no function image; Prepare still
// succeeds (the function is available for service convergence) and carries the
// fingerprint, without attempting a build.
func TestPrepareRuntimeLessTemplateSucceeds(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("services:\n  - image: nginx:1.27\n    port: 80\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	tmpl := &function.Template{Services: []function.Service{{Image: "nginx:1.27", Port: 80, Replicas: 1}}}
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

// TestPrepareRuntimeLessTemplateFingerprintsTemplateOnly pins the narrow-input
// optimization: an external-image-only function never builds an image from
// source, so its fingerprint is over template.yaml ALONE. An unrelated source
// file (even an unreadable one) must neither be read nor affect the digest, and
// a template edit must still change it.
func TestPrepareRuntimeLessTemplateFingerprintsTemplateOnly(t *testing.T) {
	dir := t.TempDir()
	const tmplYAML = "services:\n  - image: nginx:1.27\n    port: 80\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmplYAML), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	// An unreadable source file: if Prepare scanned the tree, the fingerprint
	// would fail (or read bytes that cannot matter).
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("x\n"), 0o200); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	tmpl, err := function.ParseTemplate([]byte(tmplYAML))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	m := newClockManager(t, nil, time.Now)

	got, err := m.Prepare(context.Background(), function.Function{Name: "fn", Dir: dir, Template: tmpl})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// A template edit changes the fingerprint.
	changed := strings.Replace(tmplYAML, "nginx:1.27", "nginx:1.28", 1)
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(changed), 0o644); err != nil {
		t.Fatalf("rewrite template: %v", err)
	}
	next, err := m.Prepare(context.Background(), function.Function{Name: "fn", Dir: dir, Template: tmpl})
	if err != nil {
		t.Fatalf("prepare after template edit: %v", err)
	}
	if next.Fingerprint == got.Fingerprint {
		t.Fatal("editing template.yaml must change the runtime-less fingerprint")
	}
}
