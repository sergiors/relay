package runtime

import (
	"archive/tar"
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/mount"

	"relay/internal/app"
	"relay/internal/testutil"
)

// imageWithoutDeclaredVolumesRoute explicitly scripts ImageInspect for a
// workload image. Keeping this opt-in ensures a new image inspection cannot
// silently pass in tests whose daemon script forgot to model it.
func imageWithoutDeclaredVolumesRoute(image string) dockerRoute {
	id := "sha256:clean"
	if strings.HasPrefix(image, "sha256:") {
		id = image
	}
	return dockerRoute{
		method: http.MethodGet,
		path:   "/images/" + image + "/json",
		body:   `{"Id":"` + id + `","Config":{}}`,
	}
}

func TestExecutionContainerCreatesFromInspectedImageID(t *testing.T) {
	const ref, id = "relay-app-fn:mutable", "sha256:inspected"
	var createBody []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/" + ref + "/json",
			body: `{"Id":"` + id + `","Config":{}}`,
		},
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			status: http.StatusInternalServerError,
			body:   `{"message":"scripted create failure"}`,
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	_, err := startExecutionContainer(context.Background(),
		containerOps{controlNetwork: unitControlNetwork}, m.cli, m.log,
		"fn", ref, nil, nil, app.ResourceLimits{}, RunMeta{}, "", "")
	if err == nil {
		t.Fatal("expected the scripted create failure")
	}
	if len(createBody) == 0 {
		t.Fatal("no /containers/create body captured")
	}
	req := decodeCreateRequest(t, createBody)
	if req.Config.Image != id {
		t.Fatalf("Config.Image = %q, want inspected immutable image ID %q instead of mutable tag %q", req.Config.Image, id, ref)
	}
}

func TestValidateImageDeclaredVolumes(t *testing.T) {
	t.Run("exact configured ephemeral coverage is accepted", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet,
			path:   "/images/test-image/json",
			body:   `{"Id":"sha256:with-volume","Config":{"Volumes":{"/run/cache":{}}}}`,
		})
		if got, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image", "/run/cache"); err != nil || got != "sha256:with-volume" {
			t.Fatalf("exact ephemeral coverage: id=%q err=%v, want immutable ID and no error", got, err)
		}
	})
	t.Run("baseline /tmp mount covers a declared /tmp volume", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet,
			path:   "/images/test-image/json",
			body:   `{"Id":"sha256:with-tmp-volume","Config":{"Volumes":{"/tmp":{}}}}`,
		})
		if got, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image", tmpVolumeTarget); err != nil || got != "sha256:with-tmp-volume" {
			t.Fatalf("/tmp coverage: id=%q err=%v, want immutable ID and no error", got, err)
		}
	})
	t.Run("ancestor ephemeral destination does not cover a declared volume", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet,
			path:   "/images/test-image/json",
			body:   `{"Id":"sha256:with-volume","Config":{"Volumes":{"/run/cache":{}}}}`,
		})
		if _, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image", "/run"); err == nil || !strings.Contains(err.Error(), "/run/cache") {
			t.Fatalf("ancestor-only ephemeral coverage error = %v, want uncovered destination", err)
		}
	})

	t.Run("rejects destinations in sorted order", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet,
			path:   "/images/test-image/json",
			body:   `{"Id":"sha256:with-volumes","Config":{"Volumes":{"/zeta":{},"/alpha":{}}}}`,
		})
		_, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image")
		if err == nil {
			t.Fatal("image with declared volumes was accepted")
		}
		if got, want := err.Error(), `image "test-image" declares writable volumes [/alpha /zeta]`; !strings.Contains(got, want) {
			t.Fatalf("error = %q, want it to contain %q", got, want)
		}
	})

	t.Run("inspection error fails closed", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/images/test-image/json",
			status: http.StatusInternalServerError,
			body:   `{"message":"daemon unavailable"}`,
		})
		_, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image")
		if err == nil || !strings.Contains(err.Error(), `inspect image "test-image"`) {
			t.Fatalf("validateImageDeclaredVolumes error = %v, want an inspection failure", err)
		}
	})

	t.Run("missing config fails closed", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/images/test-image/json", body: `{"Id":"sha256:missing-config"}`,
		})
		if _, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image"); err == nil || !strings.Contains(err.Error(), "image config is missing") {
			t.Fatalf("missing image config error = %v, want fail-closed config error", err)
		}
	})
	t.Run("missing immutable ID fails closed", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/images/test-image/json", body: `{"Config":{}}`,
		})
		if _, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image"); err == nil || !strings.Contains(err.Error(), "immutable image ID is missing") {
			t.Fatalf("missing image ID error = %v, want fail-closed identity error", err)
		}
	})

	t.Run("missing or empty Config.Volumes is accepted", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			body string
		}{
			{name: "missing volumes", body: `{"Id":"sha256:empty","Config":{}}`},
			{name: "empty volumes", body: `{"Id":"sha256:empty","Config":{"Volumes":{}}}`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cli := newScriptedDockerClient(t, dockerRoute{
					method: http.MethodGet, path: "/images/test-image/json", body: tc.body,
				})
				if got, err := validateImageDeclaredVolumes(context.Background(), cli, "test-image"); err != nil || got != "sha256:empty" {
					t.Fatalf("empty image config rejected: %v", err)
				}
			})
		}
	})
}

func TestStartServiceAcceptsExactlyCoveredDeclaredVolumeAndSerializesEphemeral(t *testing.T) {
	const image = "registry.example/service:with-volume"
	var createBody []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet,
			path:   "/images/" + image + "/json",
			body:   `{"Id":"sha256:with-volume","Config":{"Volumes":{"/run/cache":{}}}}`,
		},
		imageSaveRoute(imageSaveBody(t, archiveLayer{
			name: "blobs/sha256/layer0", gzip: true,
			body: layerTar(t,
				archiveEntry{name: "tmp", typ: tar.TypeDir},
				archiveEntry{name: "run", typ: tar.TypeDir},
				archiveEntry{name: "run/cache", typ: tar.TypeDir},
			),
		})),
		dockerRoute{
			method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`,
			onBody: func(body []byte) { createBody = append([]byte(nil), body...) },
		},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json", body: `{"Id":"cid-1","State":{"Running":true}}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	if _, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
		Ephemeral: []string{"/run/cache"},
	}, 0); err != nil {
		t.Fatalf("StartService with exact declared-volume coverage: %v", err)
	}
	if len(createBody) == 0 {
		t.Fatal("no /containers/create request was captured")
	}
	req := decodeCreateRequest(t, createBody)
	volume, ok := mountAt(req.HostConfig.Mounts, "/run/cache")
	if !ok || volume.Type != mount.TypeVolume || volume.Source != "" ||
		volume.ReadOnly || volume.VolumeOptions != nil {
		t.Fatalf("serialized volume at /run/cache = %+v (present=%v), want an anonymous writable volume without options", volume, ok)
	}
	assertBaselineTmpVolume(t, req.HostConfig.Mounts)
	if len(req.HostConfig.Tmpfs) != 0 {
		t.Fatalf("serialized HostConfig.Tmpfs = %v, want none", req.HostConfig.Tmpfs)
	}
	if req.Config.Labels[labelEphemeral] != "/run/cache" {
		t.Fatalf("relay.ephemeral label = %q, want /run/cache", req.Config.Labels[labelEphemeral])
	}
	if !req.HostConfig.ReadonlyRootfs {
		t.Fatal("serialized service HostConfig must retain ReadonlyRootfs")
	}
}

func TestStartServiceRejectsInvalidDirectEphemeralSpecBeforeCreate(t *testing.T) {
	creates := 0
	const image = "registry.example/service:direct-spec"
	cli := newScriptedDockerClient(t,
		imageWithoutDeclaredVolumesRoute(image),
		dockerRoute{
			method: http.MethodPost, path: "/containers/create", body: `{"Id":"unexpected"}`,
			onMatch: func() { creates++ },
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
	_, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
		// Bypass template parsing deliberately: runtime create must enforce policy.
		Ephemeral: []string{"/etc"},
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("StartService error = %v, want direct-spec ephemeral rejection", err)
	}
	if creates != 0 {
		t.Fatalf("Docker ContainerCreate calls = %d, want none", creates)
	}
}

func TestWorkloadCreateRejectsImageDeclaredVolumesBeforeCreate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		image string
		start func(*testing.T, *Manager) error
	}{
		{
			name:  "execution",
			image: "volume-image",
			start: func(t *testing.T, m *Manager) error {
				_, err := startExecutionContainer(context.Background(),
					containerOps{controlNetwork: unitControlNetwork}, m.cli, m.log,
					"fn", "volume-image", nil, nil, app.ResourceLimits{}, RunMeta{}, "", "")
				return err
			},
		},
		{
			name:  "external image service",
			image: "registry.example/service:1",
			start: func(t *testing.T, m *Manager) error {
				_, err := m.StartService(context.Background(), ServiceSpec{
					App: "fn", Name: "external", SourceRef: "registry.example/service:1",
					Image: "registry.example/service:1", Port: 8080,
				}, 0)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creates := 0
			cli := newScriptedDockerClient(t,
				dockerRoute{
					method: http.MethodGet, path: "/images/" + tc.image + "/json",
					body: `{"Id":"sha256:bad","Config":{"Volumes":{"/z-volume":{},"/a-volume":{}}}}`,
				},
				dockerRoute{
					method: http.MethodPost, path: "/containers/create",
					body:    `{"Id":"unexpected"}`,
					onMatch: func() { creates++ },
				},
			)
			m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
			err := tc.start(t, m)
			if err == nil || !strings.Contains(err.Error(), `declares writable volumes [/a-volume /z-volume]`) {
				t.Fatalf("start error = %v, want deterministic declared-volume rejection", err)
			}
			if creates != 0 {
				t.Fatalf("Docker ContainerCreate was issued %d time(s), want none", creates)
			}
		})
	}
}

// TestWorkloadDeclaredTmpVolumeIsCoveredByBaselineMount pins the documented
// exception: an image-declared VOLUME at /tmp is accepted on both workload
// paths because Relay's baseline anonymous volume mounts that exact target,
// while any other declared destination still fails before create and is
// reported without the /tmp exemption.
func TestWorkloadDeclaredTmpVolumeIsCoveredByBaselineMount(t *testing.T) {
	const tmpOnly = `{"Id":"sha256:tmp-only","Config":{"Volumes":{"/tmp":{}}}}`
	const mixed = `{"Id":"sha256:mixed","Config":{"Volumes":{"/tmp":{},"/data":{}}}}`
	for _, tc := range []struct {
		name    string
		image   string
		inspect string
		start   func(*testing.T, *Manager) error
		wantErr string // "" means the create must be attempted and fail there
	}{
		{
			name:    "execution accepts declared /tmp",
			image:   "relay-app-fn:tmp-volume",
			inspect: tmpOnly,
			start: func(t *testing.T, m *Manager) error {
				_, err := startExecutionContainer(context.Background(),
					containerOps{controlNetwork: unitControlNetwork}, m.cli, m.log,
					"fn", "relay-app-fn:tmp-volume", nil, nil,
					app.ResourceLimits{}, RunMeta{}, "", "")
				return err
			},
		},
		{
			name:    "service accepts declared /tmp without ephemeral",
			image:   "registry.example/service:tmp-volume",
			inspect: tmpOnly,
			start: func(t *testing.T, m *Manager) error {
				_, err := m.StartService(context.Background(), ServiceSpec{
					App: "fn", Name: "svc",
					SourceRef: "registry.example/service:tmp-volume",
					Image:     "registry.example/service:tmp-volume", Port: 8080,
				}, 0)
				return err
			},
		},
		{
			name:    "service still rejects an uncovered declared volume",
			image:   "registry.example/service:mixed-volume",
			inspect: mixed,
			start: func(t *testing.T, m *Manager) error {
				_, err := m.StartService(context.Background(), ServiceSpec{
					App: "fn", Name: "svc",
					SourceRef: "registry.example/service:mixed-volume",
					Image:     "registry.example/service:mixed-volume", Port: 8080,
				}, 0)
				return err
			},
			wantErr: `declares writable volumes [/data]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creates := 0
			cli := newScriptedDockerClient(t,
				dockerRoute{
					method: http.MethodGet, path: "/images/" + tc.image + "/json",
					body: tc.inspect,
				},
				dockerRoute{
					method: http.MethodPost, path: "/containers/create",
					status:  http.StatusInternalServerError,
					body:    `{"message":"scripted create failure"}`,
					onMatch: func() { creates++ },
				},
			)
			m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
			err := tc.start(t, m)
			if tc.wantErr == "" {
				if err == nil || !strings.Contains(err.Error(), "scripted create failure") {
					t.Fatalf("start error = %v, want the declared /tmp volume to pass "+
						"validation and reach ContainerCreate", err)
				}
				if creates != 1 {
					t.Fatalf("Docker ContainerCreate calls = %d, want 1", creates)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("start error = %v, want it to contain %q", err, tc.wantErr)
			}
			if creates != 0 {
				t.Fatalf("Docker ContainerCreate calls = %d, want none", creates)
			}
		})
	}
}

func TestImageWithoutDeclaredVolumesStillReachesContainerCreate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		image string
		start func(*testing.T, *Manager, string) error
	}{
		{
			name:  "execution",
			image: "relay-app-fn:empty-volumes",
			start: func(_ *testing.T, m *Manager, image string) error {
				_, err := startExecutionContainer(context.Background(),
					containerOps{controlNetwork: unitControlNetwork}, m.cli, m.log,
					"fn", image, nil, nil, app.ResourceLimits{}, RunMeta{}, "", "")
				return err
			},
		},
		{
			name:  "service",
			image: "registry.example/service:empty-volumes",
			start: func(_ *testing.T, m *Manager, image string) error {
				_, err := m.StartService(context.Background(), ServiceSpec{
					App: "fn", Name: "svc", SourceRef: image, Image: image, Port: 8080,
				}, 0)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creates := 0
			cli := newScriptedDockerClient(t,
				imageWithoutDeclaredVolumesRoute(tc.image),
				dockerRoute{
					method: http.MethodPost, path: "/containers/create",
					status:  http.StatusInternalServerError,
					body:    `{"message":"expected scripted create failure"}`,
					onMatch: func() { creates++ },
				},
			)
			m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "test-host"}
			err := tc.start(t, m, tc.image)
			if err == nil || !strings.Contains(err.Error(), "scripted create failure") {
				t.Fatalf("start error = %v, want the scripted ContainerCreate failure", err)
			}
			if creates != 1 {
				t.Fatalf("Docker ContainerCreate calls = %d, want 1", creates)
			}
		})
	}
}
