package runtime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"relay/internal/testutil"
)

// archiveEntry is one tar header used to build an image layer fixture. A zero
// Typeflag is written as a regular file; file contents are not needed because
// the validator inspects headers only.
type archiveEntry struct {
	name     string
	typ      byte
	linkname string
}

// archiveLayer is one layer in a Docker/OCI save fixture. gzip models the
// containerd image store's compressed blobs; an uncompressed layer models a
// legacy Docker archive.
type archiveLayer struct {
	name string
	body []byte
	gzip bool
}

func layerTar(t *testing.T, entries ...archiveEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		hdr := &tar.Header{
			Name: entry.name,
			Mode: 0o755,
		}
		hdr.Typeflag = entry.typ
		if hdr.Typeflag == 0 {
			hdr.Typeflag = tar.TypeReg
		}
		if hdr.Typeflag == tar.TypeSymlink {
			hdr.Linkname = entry.linkname
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write layer header %q: %v", entry.name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close layer tar: %v", err)
	}
	return buf.Bytes()
}

// imageSaveBody builds the archive returned by Docker's ImageSave API. Layers
// precede manifest.json, matching the real daemon's order, so the production
// parser must retain per-layer mutations until it learns their order.
func imageSaveBody(t *testing.T, layers ...archiveLayer) string {
	t.Helper()
	var outer bytes.Buffer
	tw := tar.NewWriter(&outer)
	names := make([]string, 0, len(layers))
	for _, layer := range layers {
		body := layer.body
		if layer.gzip {
			var compressed bytes.Buffer
			zw := gzip.NewWriter(&compressed)
			if _, err := zw.Write(body); err != nil {
				t.Fatalf("compress layer %q: %v", layer.name, err)
			}
			if err := zw.Close(); err != nil {
				t.Fatalf("close compressed layer %q: %v", layer.name, err)
			}
			body = compressed.Bytes()
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: layer.name, Typeflag: tar.TypeReg, Mode: 0o644,
			Size: int64(len(body)),
		}); err != nil {
			t.Fatalf("write layer %q: %v", layer.name, err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("write layer body %q: %v", layer.name, err)
		}
		names = append(names, layer.name)
	}
	manifest, err := json.Marshal([]imageSaveManifestEntry{{
		Config: "blobs/sha256/config",
		Layers: names,
	}})
	if err != nil {
		t.Fatalf("marshal image manifest: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: "manifest.json", Typeflag: tar.TypeReg, Mode: 0o644,
		Size: int64(len(manifest)),
	}); err != nil {
		t.Fatalf("write image manifest header: %v", err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatalf("write image manifest: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close image archive: %v", err)
	}
	return outer.String()
}

func imageSaveRoute(body string) dockerRoute {
	return dockerRoute{
		method: http.MethodGet,
		path:   "/images/get",
		body:   body,
	}
}

func TestValidateImageTmpfsTargetsRejectsSymlinkRedirection(t *testing.T) {
	tests := []struct {
		name   string
		layers []archiveLayer
		target string
		want   string
	}{
		{
			name: "var cache points at etc",
			layers: []archiveLayer{{
				name: "blobs/sha256/layer0", gzip: true,
				body: layerTar(t,
					archiveEntry{name: "var", typ: tar.TypeDir},
					archiveEntry{
						name: "var/cache", typ: tar.TypeSymlink,
						linkname: "/etc",
					},
				),
			}},
			target: "/var/cache/nginx",
			want:   `symlink "/var/cache"`,
		},
		{
			name: "run points at etc",
			layers: []archiveLayer{{
				name: "blobs/sha256/layer0", gzip: true,
				body: layerTar(t,
					archiveEntry{name: "run", typ: tar.TypeSymlink, linkname: "/etc"},
				),
			}},
			target: "/run/cache",
			want:   `symlink "/run"`,
		},
		{
			name: "var points at etc",
			layers: []archiveLayer{{
				name: "blobs/sha256/layer0", gzip: true,
				body: layerTar(t,
					archiveEntry{
						name: "var", typ: tar.TypeSymlink,
						linkname: "/etc",
					},
				),
			}},
			target: "/var/cache/nginx",
			want:   `symlink "/var"`,
		},
		{
			name: "target itself is a symlink",
			layers: []archiveLayer{{
				name: "blobs/sha256/layer0", gzip: true,
				body: layerTar(t,
					archiveEntry{name: "var", typ: tar.TypeDir},
					archiveEntry{name: "var/cache", typ: tar.TypeDir},
					archiveEntry{
						name: "var/cache/nginx", typ: tar.TypeSymlink,
						linkname: "/etc",
					},
				),
			}},
			target: "/var/cache/nginx",
			want:   `symlink "/var/cache/nginx"`,
		},
		{
			name: "ancestor is a regular file",
			layers: []archiveLayer{{
				name: "blobs/sha256/layer0", gzip: true,
				body: layerTar(t,
					archiveEntry{name: "run", typ: tar.TypeReg},
				),
			}},
			target: "/run/cache",
			want:   `non-directory component "/run"`,
		},
		{
			// An opaque whiteout clears children of its directory, not the
			// directory itself, so it must not hide an ancestor symlink.
			name: "opaque whiteout does not hide symlink ancestor",
			layers: []archiveLayer{
				{
					name: "blobs/sha256/layer0", gzip: true,
					body: layerTar(t,
						archiveEntry{
							name: "run", typ: tar.TypeSymlink,
							linkname: "/etc",
						},
					),
				},
				{
					name: "blobs/sha256/layer1", gzip: true,
					body: layerTar(t,
						archiveEntry{
							name: "run/.wh..wh..opq",
							typ:  tar.TypeReg,
						},
					),
				},
			},
			target: "/run/cache",
			want:   `symlink "/run"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli := newScriptedDockerClient(t, imageSaveRoute(
				imageSaveBody(t, tc.layers...)))
			err := validateImageTmpfsTargets(
				context.Background(), cli, "sha256:test-image",
				[]string{tc.target})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateImageTmpfsTargetsAllowsSafeLayouts(t *testing.T) {
	targetExists := []archiveLayer{{
		name: "blobs/sha256/layer0", gzip: true,
		body: layerTar(t,
			archiveEntry{name: "run", typ: tar.TypeDir},
			archiveEntry{name: "var", typ: tar.TypeDir},
			archiveEntry{name: "var/cache", typ: tar.TypeDir},
			archiveEntry{name: "var/cache/nginx", typ: tar.TypeDir},
		),
	}}
	targetMissing := []archiveLayer{{
		name: "blobs/sha256/layer0", gzip: true,
		body: layerTar(t,
			archiveEntry{name: "run", typ: tar.TypeDir},
			archiveEntry{name: "var", typ: tar.TypeDir},
			archiveEntry{name: "var/cache", typ: tar.TypeDir},
		),
	}}
	symlinkReplacedByDirectory := []archiveLayer{
		{
			name: "blobs/sha256/layer0", gzip: true,
			body: layerTar(t,
				archiveEntry{
					name: "run", typ: tar.TypeSymlink, linkname: "/etc",
				},
			),
		},
		{
			name: "blobs/sha256/layer1", gzip: true,
			body: layerTar(t,
				archiveEntry{name: ".wh.run", typ: tar.TypeReg},
				archiveEntry{name: "run", typ: tar.TypeDir},
			),
		},
	}
	legacyUncompressed := []archiveLayer{{
		name: "abc/layer.tar",
		body: layerTar(t,
			archiveEntry{name: "run", typ: tar.TypeDir},
		),
	}}
	for _, tc := range []struct {
		name   string
		layers []archiveLayer
		target string
	}{
		{"existing target", targetExists, "/var/cache/nginx"},
		{"missing target", targetMissing, "/var/cache/nginx"},
		{"symlink replaced by directory", symlinkReplacedByDirectory, "/run/cache"},
		{"legacy uncompressed layer", legacyUncompressed, "/run/cache"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := newScriptedDockerClient(t, imageSaveRoute(
				imageSaveBody(t, tc.layers...)))
			if err := validateImageTmpfsTargets(
				context.Background(), cli, "sha256:safe", []string{tc.target},
			); err != nil {
				t.Fatalf("safe image rejected: %v", err)
			}
		})
	}
}

func TestReadImageFilesystemBoundsRelevantRecordsAcrossLayers(t *testing.T) {
	const layers = maxImageArchiveRelevantRecords/maxImageLayerRecords + 1
	entries := make([]archiveEntry, maxImageLayerRecords-1)
	for i := range entries {
		entries[i] = archiveEntry{name: "run", typ: tar.TypeDir}
	}
	body := layerTar(t, entries...)
	imageLayers := make([]archiveLayer, layers)
	for i := range imageLayers {
		imageLayers[i] = archiveLayer{
			name: "blobs/sha256/layer" + strconv.Itoa(i),
			body: body,
		}
	}

	_, err := readImageFilesystem(
		context.Background(),
		strings.NewReader(imageSaveBody(t, imageLayers...)),
		map[string]struct{}{"run": {}},
	)
	if err == nil || !strings.Contains(err.Error(), errImageArchiveTooManyRecords.Error()) {
		t.Fatalf("readImageFilesystem error = %v, want aggregate record-limit failure", err)
	}
}

func TestValidateImageTmpfsTargetsFailsClosed(t *testing.T) {
	t.Run("image save failure", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/images/get",
			status: http.StatusInternalServerError,
			body:   `{"message":"daemon unavailable"}`,
		})
		err := validateImageTmpfsTargets(
			context.Background(), cli, "sha256:test", []string{"/run/cache"})
		if err == nil || !strings.Contains(err.Error(), "inspect image filesystem") {
			t.Fatalf("error = %v, want a filesystem-inspection failure", err)
		}
	})
	t.Run("missing manifest", func(t *testing.T) {
		var archive bytes.Buffer
		tw := tar.NewWriter(&archive)
		body := layerTar(t, archiveEntry{name: "run", typ: tar.TypeDir})
		if err := tw.WriteHeader(&tar.Header{
			Name: "blobs/sha256/layer0", Typeflag: tar.TypeReg,
			Size: int64(len(body)),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		cli := newScriptedDockerClient(t,
			imageSaveRoute(archive.String()))
		err := validateImageTmpfsTargets(
			context.Background(), cli, "sha256:test", []string{"/run/cache"})
		if err == nil || !strings.Contains(err.Error(), "manifest") {
			t.Fatalf("error = %v, want a missing-manifest failure", err)
		}
	})
	t.Run("missing immutable ID", func(t *testing.T) {
		cli := newScriptedDockerClient(t)
		err := validateImageTmpfsTargets(
			context.Background(), cli, "", []string{"/run/cache"})
		if err == nil || !strings.Contains(err.Error(), "immutable image ID") {
			t.Fatalf("error = %v, want a missing-image-ID failure", err)
		}
	})
}

func TestStartServiceRejectsSymlinkedTmpfsTargetBeforeCreate(t *testing.T) {
	const image = "registry.example/service:symlink"
	creates := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/" + image + "/json",
			body: `{"Id":"sha256:with-symlink","Config":{}}`,
		},
		imageSaveRoute(imageSaveBody(t, archiveLayer{
			name: "blobs/sha256/layer0", gzip: true,
			body: layerTar(t,
				archiveEntry{name: "var", typ: tar.TypeDir},
				archiveEntry{
					name: "var/cache", typ: tar.TypeSymlink,
					linkname: "/etc",
				},
			),
		})),
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			body: `{"Id":"unexpected"}`,
			onMatch: func() {
				creates++
			},
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	_, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
		Tmpfs: []string{"/var/cache/nginx"},
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("StartService error = %v, want a symlink rejection", err)
	}
	if creates != 0 {
		t.Fatalf("Docker ContainerCreate calls = %d, want none", creates)
	}
}

func TestStartServiceSkipsImageFilesystemWithoutTmpfs(t *testing.T) {
	const image = "registry.example/service:no-tmpfs"
	saves := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/" + image + "/json",
			body: `{"Id":"sha256:no-tmpfs","Config":{}}`,
		},
		dockerRoute{
			method: http.MethodGet, path: "/images/get",
			onMatch: func() { saves++ },
		},
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			status: http.StatusInternalServerError,
			body:   `{"message":"scripted create failure"}`,
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	_, _ = m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
	}, 0)
	if saves != 0 {
		t.Fatalf("ImageSave calls = %d, want none without configured tmpfs", saves)
	}
}

// safeTmpfsTargetArchive is an image archive whose /run and /var/cache trees
// are ordinary directories, so validating either as a tmpfs target succeeds.
func safeTmpfsTargetArchive(t *testing.T) string {
	t.Helper()
	return imageSaveBody(t, archiveLayer{
		name: "blobs/sha256/layer0", gzip: true,
		body: layerTar(t,
			archiveEntry{name: "run", typ: tar.TypeDir},
			archiveEntry{name: "run/cache", typ: tar.TypeDir},
			archiveEntry{name: "var", typ: tar.TypeDir},
			archiveEntry{name: "var/cache", typ: tar.TypeDir},
			archiveEntry{name: "var/cache/data", typ: tar.TypeDir},
		),
	})
}

func TestImageTmpfsValidationCacheKeysByImageAndTargetSet(t *testing.T) {
	var cache imageTmpfsValidationCache
	var calls atomic.Int32
	fn := func(context.Context) error {
		calls.Add(1)
		return nil
	}
	key := func(imageID string, destinations ...string) tmpfsImageValidationKey {
		return tmpfsImageValidationKey{
			imageID: imageID,
			targets: tmpfsTargetsKey(destinations),
		}
	}
	// A successful validation is reused for the same image and target set.
	for range 2 {
		if err := cache.validate(
			context.Background(),
			key("sha256:one", "/run/cache"), fn,
		); err != nil {
			t.Fatalf("same key: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("same key invoked validation %d times, want 1", got)
	}
	// Target declaration order is not part of the identity.
	if err := cache.validate(
		context.Background(),
		key("sha256:one", "/run/b", "/run/a"), fn,
	); err != nil {
		t.Fatal(err)
	}
	if err := cache.validate(
		context.Background(),
		key("sha256:one", "/run/a", "/run/b"), fn,
	); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("order-insensitive key invoked validation %d times, want 2", got)
	}
	// Different image content is a different validation even for the same set.
	if err := cache.validate(
		context.Background(),
		key("sha256:two", "/run/cache"), fn,
	); err != nil {
		t.Fatal(err)
	}
	// A different target set is a different validation for the same image.
	if err := cache.validate(
		context.Background(),
		key("sha256:one", "/var/cache/data"), fn,
	); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("distinct keys invoked validation %d times, want 4", got)
	}
}

func TestImageTmpfsValidationCacheCoalescesConcurrentCallers(t *testing.T) {
	var cache imageTmpfsValidationCache
	key := tmpfsImageValidationKey{
		imageID: "sha256:same",
		targets: tmpfsTargetsKey([]string{"/run/cache"}),
	}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	fn := func(context.Context) error {
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		return nil
	}

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			errs <- cache.validate(context.Background(), key, fn)
		}()
	}
	<-started
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent validation: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent callers invoked validation %d times, want 1", got)
	}
}

func TestImageTmpfsValidationCacheDoesNotRetainFailure(t *testing.T) {
	var cache imageTmpfsValidationCache
	key := tmpfsImageValidationKey{
		imageID: "sha256:flaky",
		targets: tmpfsTargetsKey([]string{"/run/cache"}),
	}
	transient := errors.New("transient image save failure")
	var calls atomic.Int32
	fn := func(context.Context) error {
		if calls.Add(1) == 1 {
			return transient
		}
		return nil
	}
	if err := cache.validate(
		context.Background(), key, fn,
	); !errors.Is(err, transient) {
		t.Fatalf("first validation error = %v, want %v", err, transient)
	}
	if err := cache.validate(context.Background(), key, fn); err != nil {
		t.Fatalf("validation after a failure: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("failed validation was reused: %d invocations, want 2", got)
	}
}

func TestValidateImageTmpfsTargetsCachedFailsClosedWithoutImageID(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}
	err := m.validateImageTmpfsTargetsCached(
		context.Background(), "", []string{"/run/cache"})
	if err == nil || !strings.Contains(err.Error(), "immutable image ID") {
		t.Fatalf("error = %v, want a missing-image-ID failure", err)
	}
}

// TestStartServiceReusesImageTmpfsValidationAcrossReplicas proves the
// Manager-scoped reuse against the real create path: replicas of one image and
// one target set stream the image archive once, while a different target set
// is validated separately.
func TestStartServiceReusesImageTmpfsValidationAcrossReplicas(t *testing.T) {
	const image = "registry.example/service:cached"
	saves := 0
	cli := newScriptedDockerClient(t,
		imageWithoutDeclaredVolumesRoute(image),
		dockerRoute{
			method: http.MethodGet, path: "/images/get",
			body:    safeTmpfsTargetArchive(t),
			onMatch: func() { saves++ },
		},
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			body: `{"Id":"cid-1"}`,
		},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{
			method: http.MethodGet, path: "/containers/cid-1/json",
			body: `{"Id":"cid-1","State":{"Running":true}}`,
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	for replica := 0; replica < 2; replica++ {
		if _, err := m.StartService(context.Background(), ServiceSpec{
			App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
			Tmpfs: []string{"/run/cache"},
		}, replica); err != nil {
			t.Fatalf("replica %d: %v", replica, err)
		}
	}
	if saves != 1 {
		t.Fatalf("ImageSave calls = %d, want 1 for repeated replicas", saves)
	}
	if _, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
		Tmpfs: []string{"/var/cache/data"},
	}, 2); err != nil {
		t.Fatalf("different target set: %v", err)
	}
	if saves != 2 {
		t.Fatalf("ImageSave calls = %d, want 2 for a distinct target set", saves)
	}
}

// TestStartServiceImageTmpfsValidationDistinguishesImageIDs pins that moving
// external content is validated again under its new immutable ID, never served
// from the previous ID's success entry.
func TestStartServiceImageTmpfsValidationDistinguishesImageIDs(t *testing.T) {
	const image = "registry.example/service:moving"
	saves := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/sha256:one/json",
			body: `{"Id":"sha256:one","Config":{}}`,
		},
		dockerRoute{
			method: http.MethodGet, path: "/images/sha256:two/json",
			body: `{"Id":"sha256:two","Config":{}}`,
		},
		dockerRoute{
			method: http.MethodGet, path: "/images/get",
			body:    safeTmpfsTargetArchive(t),
			onMatch: func() { saves++ },
		},
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			body: `{"Id":"cid-1"}`,
		},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{
			method: http.MethodGet, path: "/containers/cid-1/json",
			body: `{"Id":"cid-1","State":{"Running":true}}`,
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	for _, id := range []string{"sha256:one", "sha256:two"} {
		if _, err := m.StartService(context.Background(), ServiceSpec{
			App: "fn", Name: "svc", SourceRef: image, Image: image, ImageID: id,
			Port: 8080, Tmpfs: []string{"/run/cache"},
		}, 0); err != nil {
			t.Fatalf("image %s: %v", id, err)
		}
	}
	if saves != 2 {
		t.Fatalf("ImageSave calls = %d, want 2 for distinct image IDs", saves)
	}
}

// TestStartServiceRetriesImageTmpfsValidationAfterFailure proves a failed
// archive inspection is not cached: the next create retries and succeeds.
func TestStartServiceRetriesImageTmpfsValidationAfterFailure(t *testing.T) {
	const image = "registry.example/service:transient"
	saves := 0
	cli := newScriptedDockerClient(t,
		imageWithoutDeclaredVolumesRoute(image),
		dockerRoute{
			method: http.MethodGet, path: "/images/get",
			body: safeTmpfsTargetArchive(t),
			serve: func(*http.Request) (int, string) {
				saves++
				if saves == 1 {
					return http.StatusInternalServerError,
						`{"message":"transient image save failure"}`
				}
				return 0, ""
			},
		},
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			body: `{"Id":"cid-1"}`,
		},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{
			method: http.MethodGet, path: "/containers/cid-1/json",
			body: `{"Id":"cid-1","State":{"Running":true}}`,
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	if _, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
		Tmpfs: []string{"/run/cache"},
	}, 0); err == nil ||
		!strings.Contains(err.Error(), "inspect image filesystem") {
		t.Fatalf("first StartService error = %v, want an inspection failure", err)
	}
	if _, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
		Tmpfs: []string{"/run/cache"},
	}, 0); err != nil {
		t.Fatalf("StartService after a validation failure: %v", err)
	}
	if saves != 2 {
		t.Fatalf("ImageSave calls = %d, want 2 (a failure must not be reused)", saves)
	}
}
