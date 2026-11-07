//go:build integration

package runtime

// Live Docker Engine Distribution API coverage: the resolver's contract against
// REAL registries — an official public multi-platform tag (Docker Hub) and a
// local authenticated private registry backed by the real registryauth.Store
// config parsing. Both prove DistributionInspect is read-only (no image is
// pulled or stored on the daemon) and never hard-code a public digest: the
// digest SHAPE, media type, and platform structure are asserted, plus exact
// equality only against content this test itself pushed.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/authconfig"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/api/types/registry"
	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/crypto/bcrypt"

	"relay/internal/registryauth"
	"relay/internal/testutil"
)

// digestShape matches the persisted index-digest form Relay resolves.
var digestShape = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// imageSnapshot describes the daemon-local state of one image reference for
// the no-pull proof: either its local image ID, or its absence.
type imageSnapshot struct {
	ID      string
	Absent  bool
	Message string // the inspect error text when absent
}

func takeImageSnapshot(t *testing.T, ctx context.Context, cl *client.Client, ref string) imageSnapshot {
	t.Helper()
	insp, err := cl.ImageInspect(ctx, ref)
	if err == nil {
		return imageSnapshot{ID: insp.ID}
	}
	return imageSnapshot{Absent: true, Message: err.Error()}
}

// requireSnapshotUnchanged pins the read-only no-pull contract after a
// distribution inspection: a locally present image keeps its exact ID, an
// absent one is still absent (DistributionInspect never pulls).
func requireSnapshotUnchanged(
	t *testing.T, before, after imageSnapshot, ref string,
) {
	t.Helper()
	if before.Absent {
		if !after.Absent {
			t.Fatalf("distribution inspection PULLED %s: absent before, inspect error %q, now image %s",
				ref, before.Message, after.ID)
		}
		return
	}
	if after.Absent {
		t.Fatalf("present image %s disappeared during distribution inspection (before %s, after %q)",
			ref, before.ID, after.Message)
	}
	if before.ID != after.ID {
		t.Fatalf("image %s ID changed during distribution inspection (before %s, after %s)",
			ref, before.ID, after.ID)
	}
}

// TestIntegrationDistributionResolverResolvesPublicMultiPlatformTags drives the
// production resolver (anonymous) through the REAL Engine against official
// public multi-platform tags: the response names the queried reference, carries
// a valid sha256 index digest, an OCI-index or Docker-manifest-list media type,
// and at least two CONCRETE platforms (attestation descriptors excluded). No
// image is pulled: the daemon-local state of every reference is unchanged.
func TestIntegrationDistributionResolverResolvesPublicMultiPlatformTags(t *testing.T) {
	cl := testutil.RequireDocker(t)
	resolver := NewDistributionResolver(cl, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// hello-world:latest is the empirically-verified absent reference; node and
	// python are the official managed bases. Whether a CI runner has them
	// cached is irrelevant: both branches of the snapshot must hold.
	for _, ref := range []string{"node:24-alpine", "python:3.14-slim", "hello-world:latest"} {
		t.Run(ref, func(t *testing.T) {
			before := takeImageSnapshot(t, ctx, cl, ref)

			resolveCtx, resolveCancel := context.WithTimeout(ctx, 60*time.Second)
			defer resolveCancel()
			resolution, err := resolver.ResolveBaseImage(resolveCtx, ref)
			if err != nil {
				t.Fatalf("ResolveBaseImage(%s): %v", ref, err)
			}
			if resolution.Ref != ref {
				t.Fatalf("resolution Ref = %q, want the queried reference %q", resolution.Ref, ref)
			}
			if !digestShape.MatchString(resolution.Digest) {
				t.Fatalf("resolution Digest = %q, want sha256:<64 lowercase hex>", resolution.Digest)
			}
			if !isIndexMediaType(resolution.MediaType) {
				t.Fatalf("resolution MediaType = %q, want an OCI index or Docker manifest list",
					resolution.MediaType)
			}
			concrete := 0
			for _, platform := range resolution.Platforms {
				if platform.OS == "" || platform.Architecture == "" {
					continue
				}
				// Attestation descriptors report os/architecture "unknown";
				// they are not a target platform image consumers can build for.
				if strings.EqualFold(platform.OS, "unknown") ||
					strings.EqualFold(platform.Architecture, "unknown") {
					continue
				}
				concrete++
			}
			// Official multi-platform tags list their concrete platforms; the
			// resolver must not collapse or fabricate them.
			if concrete < 2 {
				t.Fatalf("resolution reports %d concrete platforms (%v); an official "+
					"multi-platform tag must carry at least two", concrete, resolution.Platforms)
			}

			after := takeImageSnapshot(t, ctx, cl, ref)
			requireSnapshotUnchanged(t, before, after, ref)
		})
	}
}

// registryCredentials is the fixture identity the private registry test uses.
type registryCredentials struct {
	username string
	password string
}

// startAuthTestRegistry starts a real registry:2 container with htpasswd
// authentication published on 127.0.0.1:<free port>, waits for readiness, and
// removes it (forced) on cleanup.
func startAuthTestRegistry(
	t *testing.T, ctx context.Context, cl *client.Client,
) (hostPort string, creds registryCredentials) {
	t.Helper()

	// The Engine contacts the registry, so the container must be reachable
	// from the daemon. 127.0.0.1 is in every daemon's default insecure/HTTP
	// set and works on Linux CI and Docker Desktop alike.
	hostPort = strconv.Itoa(testutil.FreePort(t))
	// The Engine contacts the registry, so the container must be reachable
	// from the daemon. 127.0.0.1 is in every daemon's default insecure/HTTP
	// set and works on Linux CI and Docker Desktop alike.
	bindAddr := netip.MustParseAddr("127.0.0.1")
	creds = registryCredentials{username: "relay-tester", password: "relay-distribution-secret"}

	if _, err := cl.ImageInspect(ctx, "registry:2"); err != nil {
		pull, err := cl.ImagePull(ctx, "registry:2", client.ImagePullOptions{})
		if err != nil {
			t.Fatalf("pull registry:2 for the private-registry fixture: %v", err)
		}
		if err := pull.Wait(ctx); err != nil {
			t.Fatalf("wait for registry:2 pull: %v", err)
		}
	}

	htpasswd := filepath.Join(t.TempDir(), "htpasswd")
	hashed, err := bcrypt.GenerateFromPassword([]byte(creds.password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt fixture credential: %v", err)
	}
	if err := os.WriteFile(htpasswd,
		[]byte(creds.username+":"+string(hashed)+"\n"), 0o600); err != nil {
		t.Fatalf("write htpasswd: %v", err)
	}

	name := testutil.UniqueName(t, "dist-reg")
	created, err := cl.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name:  name,
		Image: "registry:2",
		Config: &container.Config{Env: []string{
			"REGISTRY_AUTH=htpasswd",
			"REGISTRY_AUTH_HTPASSWD_REALM=relay-distribution-test",
			"REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
			// Required registry-side secret for upload tokens; never a
			// credential of the test.
			"REGISTRY_HTTP_SECRET=" + randomHexSecret(t),
		}},
		HostConfig: &container.HostConfig{
			Binds: []string{htpasswd + ":/auth/htpasswd:ro"},
			PortBindings: network.PortMap{
				network.MustParsePort("5000/tcp"): []network.PortBinding{{
					HostIP:   bindAddr,
					HostPort: hostPort,
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("create registry container: %v", err)
	}
	t.Cleanup(func() {
		removeCtx, removeCancel := context.WithTimeout(context.Background(), time.Minute)
		defer removeCancel()
		_, _ = cl.ContainerRemove(removeCtx, created.ID, client.ContainerRemoveOptions{Force: true})
	})
	if _, err := cl.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start registry container: %v", err)
	}

	// Readiness: unauthenticated GET /v2/ must be challenged, and basic auth
	// must succeed — proving both the container and the htpasswd fixture.
	// Every probe carries a short client timeout AND the test context, so a
	// stalled dial can never hang past the bounded poll loop.
	base := "http://127.0.0.1:" + hostPort
	readiness := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for {
		probe, probeErr := http.NewRequestWithContext(
			ctx, http.MethodGet, base+"/v2/", nil)
		if probeErr != nil {
			t.Fatalf("build registry readiness probe: %v", probeErr)
		}
		respChallenged, err := readiness.Do(probe)
		if err == nil {
			if respChallenged.StatusCode == http.StatusUnauthorized {
				respChallenged.Body.Close()
				break
			}
			respChallenged.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry container did not become ready (auth challenge) on %s", base)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for the registry: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	authProbe, authErr := http.NewRequestWithContext(
		ctx, http.MethodGet, base+"/v2/", nil)
	if authErr != nil {
		t.Fatalf("build registry auth probe: %v", authErr)
	}
	authProbe.SetBasicAuth(creds.username, creds.password)
	respOK, err := readiness.Do(authProbe)
	if err != nil {
		t.Fatalf("probe registry with credentials: %v", err)
	}
	defer respOK.Body.Close()
	if respOK.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(respOK.Body, 512))
		t.Fatalf("registry rejected fixture credentials: status %d body %s",
			respOK.StatusCode, body)
	}
	return hostPort, creds
}

func randomHexSecret(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("registry secret: %v", err)
	}
	return fmt.Sprintf("%x", raw)
}

// pushOCIIndex publishes a minimal two-platform OCI index for repo to the
// local registry at host (http://127.0.0.1:<port>) using Basic auth, and
// returns the index digest. The content is test-created, so its digest is a
// local fixture value, not a public one.
func pushOCIIndex(
	t *testing.T, ctx context.Context, host, repo, tag string, creds registryCredentials,
) string {
	t.Helper()

	do := func(method, url string, body []byte, contentType string) *http.Response {
		t.Helper()
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, reader)
		if err != nil {
			t.Fatalf("build registry request: %v", err)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.SetBasicAuth(creds.username, creds.password)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, url, err)
		}
		return resp
	}
	pushBlob := func(blob []byte) digest.Digest {
		d := digest.FromBytes(blob)
		resp := do(http.MethodPost, host+"/v2/"+repo+"/blobs/uploads/", nil, "")
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("blob upload start: status %d", resp.StatusCode)
		}
		location := resp.Header.Get("Location")
		if !strings.HasPrefix(location, "http") {
			location = host + location
		}
		separator := "?"
		if strings.Contains(location, "?") {
			separator = "&"
		}
		upload := do(http.MethodPut,
			location+separator+"digest="+d.String(), blob, "application/octet-stream")
		body, _ := io.ReadAll(io.LimitReader(upload.Body, 512))
		upload.Body.Close()
		if upload.StatusCode != http.StatusCreated {
			t.Fatalf("blob upload %s: status %d body %s", d, upload.StatusCode, body)
		}
		return d
	}
	putManifest := func(ref string, raw []byte, contentType string) digest.Digest {
		resp := do(http.MethodPut, host+"/v2/"+repo+"/manifests/"+ref, raw, contentType)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("manifest push %s: status %d body %s", ref, resp.StatusCode, body)
		}
		return digest.FromBytes(raw)
	}

	child := func(arch string) ocispec.Descriptor {
		config := mustJSON(t, ocispec.Image{
			OS:           "linux",
			Architecture: arch,
			RootFS:       ocispec.RootFS{Type: "layers"},
		})
		configDigest := pushBlob(config)
		manifest := mustJSON(t, ocispec.Manifest{
			SchemaVersion: 2,
			MediaType:     ocispec.MediaTypeImageManifest,
			Config: ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageConfig,
				Digest:    configDigest,
				Size:      int64(len(config)),
			},
			Layers: []ocispec.Descriptor{},
		})
		manifestDigest := putManifest(digest.FromBytes(manifest).String(), manifest,
			ocispec.MediaTypeImageManifest)
		return ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    manifestDigest,
			Size:      int64(len(manifest)),
			Platform:  &ocispec.Platform{OS: "linux", Architecture: arch},
		}
	}

	index := mustJSON(t, ocispec.Index{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageIndex,
		Manifests:     []ocispec.Descriptor{child("amd64"), child("arm64")},
	})
	d := putManifest(tag, index, ocispec.MediaTypeImageIndex)
	return d.String()
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal OCI fixture: %v", err)
	}
	return raw
}

// TestIntegrationDistributionResolverPrivateRegistryUsesConfiguredAuth drives
// the FULL production auth chain against a live authenticated registry: a real
// registryauth.Store (loaded via DOCKER_CONFIG) feeding the real resolver, so
// the X-Registry-Auth header the Engine sends is exactly what the Docker CLI
// would produce for the same config. Valid credentials resolve the pushed index;
// anonymous and wrong credentials fail WITHOUT leaking any secret.
func TestIntegrationDistributionResolverPrivateRegistryUsesConfiguredAuth(t *testing.T) {
	cl := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	hostPort, creds := startAuthTestRegistry(t, ctx, cl)
	registryHost := "127.0.0.1:" + hostPort
	repo := "relay-distribution-test/creds"
	ref := registryHost + "/" + repo + ":v1"

	// The local image must not exist before or after the inspection: no pull.
	before := takeImageSnapshot(t, ctx, cl, ref)
	if !before.Absent {
		t.Fatalf("fixture precondition: %s unexpectedly present locally (%s)", ref, before.ID)
	}

	indexDigest := pushOCIIndex(t, ctx, "http://"+registryHost, repo, "v1", creds)

	// The production configuration discovery: DOCKER_CONFIG directory with a
	// basic-auth entry for the private registry host.
	configDir := t.TempDir()
	configJSON := `{"auths": {"` + registryHost + `": {"auth": "` +
		base64.StdEncoding.EncodeToString([]byte(creds.username+":"+creds.password)) + `"}}}`
	if err := os.WriteFile(filepath.Join(configDir, "config.json"),
		[]byte(configJSON), 0o600); err != nil {
		t.Fatalf("write docker config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", configDir)
	store, err := registryauth.Load()
	if err != nil {
		t.Fatalf("registryauth.Load: %v", err)
	}
	resolver := NewDistributionResolver(cl, store)

	resolveCtx, resolveCancel := context.WithTimeout(ctx, 60*time.Second)
	defer resolveCancel()
	resolution, err := resolver.ResolveBaseImage(resolveCtx, ref)
	if err != nil {
		// Reachability from the daemon is a platform property (127.0.0.1 must
		// be in the daemon's insecure set and the published port reachable);
		// name that explicitly if it ever fails.
		t.Fatalf("authenticated ResolveBaseImage(%s) failed — if this error is a "+
			"connection/TLS failure, the Docker Engine cannot reach the host-published "+
			"127.0.0.1 HTTP registry on this platform: %v", ref, err)
	}
	if resolution.Digest != indexDigest {
		t.Fatalf("resolution Digest = %q, want the pushed index digest %q", resolution.Digest, indexDigest)
	}
	if !isIndexMediaType(resolution.MediaType) {
		t.Fatalf("resolution MediaType = %q, want an index/list", resolution.MediaType)
	}
	if resolution.Ref != ref {
		t.Fatalf("resolution Ref = %q, want %q", resolution.Ref, ref)
	}

	after := takeImageSnapshot(t, ctx, cl, ref)
	requireSnapshotUnchanged(t, before, after, ref)

	// Anonymous and wrong credentials must fail with the registry's challenge,
	// and neither failure message may carry any credential value.
	_, err = NewDistributionResolver(cl, nil).ResolveBaseImage(resolveCtx, ref)
	if err == nil {
		t.Fatalf("anonymous access resolved the authenticated registry %s; "+
			"want the registry auth challenge", ref)
	}
	challenge := strings.ToLower(err.Error())
	if !strings.Contains(challenge, "unauthor") &&
		!strings.Contains(challenge, "no basic auth credentials") {
		t.Fatalf("anonymous private resolve err = %v, want the registry auth challenge", err)
	}
	if strings.Contains(err.Error(), creds.password) {
		t.Fatalf("anonymous failure leaks the fixture password: %v", err)
	}
	wrongProvider := fixtureAuthProvider(t, registryHost,
		creds.username, "wrong-password-value")
	_, err = NewDistributionResolver(cl, wrongProvider).
		ResolveBaseImage(resolveCtx, ref)
	if err == nil {
		t.Fatal("wrong credentials resolved an authenticated registry")
	}
	if strings.Contains(err.Error(), "wrong-password-value") ||
		strings.Contains(err.Error(), creds.password) {
		t.Fatalf("wrong-credential failure leaks secrets: %v", err)
	}
}

// fixtureAuthProvider is a RegistryAuthProvider whose credentials for the
// registry host are the supplied basic-auth pair (the private-registry negative
// test). It reuses the shared stub provider so only the credential value
// differs from the success path.
func fixtureAuthProvider(t *testing.T, serverAddress, username, password string) RegistryAuthProvider {
	t.Helper()
	encoded, err := authconfig.Encode(registry.AuthConfig{
		Username:      username,
		Password:      password,
		ServerAddress: serverAddress,
	})
	if err != nil {
		t.Fatalf("encode wrong-credential auth: %v", err)
	}
	return &stubAuthProvider{encoded: encoded}
}
