package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/source"
)

// serviceImagePullInterval bounds how often Relay performs a REMOTE pull check
// for an external service image. A successful check is recorded in memory (per
// worker, per independent service) and suppresses further remote checks for this
// window; a failed check does not advance the clock, so it is retried at the
// next reconcile and a transient registry outage recovers promptly. Changing a
// service's configured source (a new identity) has no recorded check and is
// therefore checked immediately. The window is deliberately not configurable and
// never persisted: it is a runtime freshness policy, not state.
const serviceImagePullInterval = time.Hour

// ServiceImage is a resolved service source ready to start a container from.
//
// Ref is the image reference passed to the container create call. ID is the
// local image content ID (relay.image_id), used to detect that a moved tag now
// points at different bytes so the running container is replaced; it is empty
// when the reference itself already encodes the content (Relay-built images,
// whose tag embeds the source fingerprint) or when inspection is unavailable.
//
// Entry is the per-container entrypoint override. A nil Entry means the
// container preserves the image's own ENTRYPOINT/CMD — true for `build` and
// `image` sources. Only the runtime-managed `entrypoint` source overrides it.
type ServiceImage struct {
	Ref   string
	ID    string
	Entry []string
}

// ResolveServiceImage resolves the desired image for one service from its
// configured source. It is the single place that knows how a service's source
// becomes a runnable image, so the service reconciler stays source-agnostic and
// the three sources share one container lifecycle.
//
//	functionImage is the function's own prepared runtime image, used only by an
//	`entrypoint` service (which overrides that image's invocation bootstrap).
//
// Resolution is read-only except for the two lifecycle actions a source may
// need: building a `build` source's image (idempotent, content-addressed) and
// pulling an `image` source's reference. A build/pull failure is returned as an
// error WITHOUT resolving, so the caller can preserve healthy containers.
func (m *Manager) ResolveServiceImage(
	ctx context.Context,
	fnName, fnDir string,
	tmpl *function.Template,
	svc function.Service,
	functionImage string,
) (ServiceImage, error) {
	switch svc.Source() {
	case function.ServiceSourceBuild:
		return m.resolveBuildServiceImage(ctx, fnName, fnDir, svc)
	case function.ServiceSourceImage:
		return m.resolveExternalServiceImage(ctx, fnName, svc.SourceRef())
	default:
		entry, err := ServiceEntry(tmpl.Runtime, svc.Entrypoint)
		if err != nil {
			return ServiceImage{}, err
		}
		return ServiceImage{Ref: functionImage, Entry: entry}, nil
	}
}

// resolveBuildServiceImage resolves a `build` source: a Relay-owned image built
// from the user's Dockerfile over the function's SELECTED source. The image is
// content-addressed by a fingerprint that folds the service identity (the
// Dockerfile path) into the function's existing source fingerprint, so an edit
// to any selected source file — the Dockerfile included — yields a new tag and
// rebuilds, while an unchanged tree reuses the local image with no build.
//
// The build uses the Docker Engine API with the same .gitignore-driven selection
// the function fingerprint uses (resolved once and shared), so ignored files
// never enter the image and never affect its identity. The Dockerfile's own
// ENTRYPOINT/CMD are preserved (Entry stays nil).
func (m *Manager) resolveBuildServiceImage(
	ctx context.Context,
	fnName, fnDir string,
	svc function.Service,
) (ServiceImage, error) {
	selection, err := source.ForDir(fnDir)
	if err != nil {
		return ServiceImage{}, fmt.Errorf("select sources: %w", err)
	}
	// The image tag is derived from the IMAGE fingerprint (runtime-only template
	// keys such as `networks` removed), so a runtime-only edit does not rebuild
	// the Dockerfile image while the reconciler still replaces the stale service
	// container for the new networks. An actual source or Dockerfile edit still
	// changes the tag and rebuilds.
	fp, err := function.ImageFingerprintSelection(selection)
	if err != nil {
		return ServiceImage{}, fmt.Errorf("fingerprint: %w", err)
	}
	ref := serviceBuildImageRef(fnName, svc.SourceRef(), fp)
	if m.imageExists(ctx, ref) {
		m.log.Debug("Service: build image exists; reusing",
			"function", fnName, "service", svc.SourceRef(), "image", ref)
		return ServiceImage{Ref: ref}, nil
	}

	start := m.clock()
	// Pre-build structured log: emitted once, immediately before the Dockerfile
	// build is issued, so a slow build is visible while it runs (the completion
	// log below only appears on success). The generated image reference is
	// content-addressed and safe to log.
	m.log.Info("Service: building image",
		"function", fnName,
		"service", svc.SourceRef(),
		"image", ref,
	)
	// The build runs on an independent, lifecycle-rooted buildTimeout context,
	// NOT on the caller's ctx: ResolveServiceImage is reached from the service
	// reconciler, whose ctx is a short reconcile budget, and a slow Dockerfile
	// build must not be cut off by it (while still being cancelled at Relay
	// shutdown via the manager lifecycle). The imageExists probe above keeps the
	// caller's ctx: it is quick and must honor its cancellation.
	buildCtx, buildCancel := m.buildContext()
	defer buildCancel()
	if err := m.buildServiceImage(buildCtx, fnName, svc, selection, ref); err != nil {
		return ServiceImage{}, err
	}
	m.log.Info("Service: build image built",
		"function", fnName,
		"service", svc.SourceRef(),
		"image", ref,
		"duration", m.clock().Sub(start),
		"result", "success",
	)
	return ServiceImage{Ref: ref}, nil
}

// buildServiceImage stages the selected source into a transient context and runs
// a Docker Engine API build with the user's Dockerfile. The context is staged
// read-only from the shared selection (template.yaml excluded exactly as the
// function image build excludes it); the user's function directory is never
// modified.
func (m *Manager) buildServiceImage(
	ctx context.Context,
	fnName string,
	svc function.Service,
	selection *source.Selection,
	ref string,
) error {
	ctxDir, err := os.MkdirTemp("", "relay-svc-build-*")
	if err != nil {
		return fmt.Errorf("create build context: %w", err)
	}
	defer os.RemoveAll(ctxDir)

	if err := copySourceDir(selection, ctxDir); err != nil {
		return fmt.Errorf("copy sources: %w", err)
	}
	dockerfile := filepath.ToSlash(svc.Build)
	if _, err := os.Stat(filepath.Join(ctxDir, filepath.FromSlash(dockerfile))); err != nil {
		return fmt.Errorf("dockerfile %q: %w", svc.Build, err)
	}
	contextTar, err := tarContext(ctxDir)
	if err != nil {
		return fmt.Errorf("tar build context: %w", err)
	}
	resp, err := m.cli.ImageBuild(ctx, contextTar, serviceBuildImageOptions(ref, dockerfile, fnName))
	if err != nil {
		return fmt.Errorf("docker build: %w", err)
	}
	defer resp.Body.Close()
	out, err := drainBuildResponse(resp.Body)
	if err != nil {
		return fmt.Errorf("docker build: %w\n%s", err, strings.TrimSpace(out))
	}
	return nil
}

// resolveExternalServiceImage resolves an `image` source: inspect the local
// image and, when the freshness policy allows, pull it from its registry.
//
// The remote check runs at most once per serviceImagePullInterval per
// independent service, measured from the last SUCCESSFUL check; a failed check
// does not advance that instant (it is retried at the next reconcile). A source
// with no recorded check — a first sighting, a changed identity, or a worker
// restart (the map is in-memory only) — is checked immediately. A pull failure
// is always surfaced so the caller preserves whatever containers it already has;
// a missing local image with no successful pull cannot start a replica at all.
func (m *Manager) resolveExternalServiceImage(ctx context.Context, fnName, identity string) (ServiceImage, error) {
	insp, err := m.cli.ImageInspect(ctx, identity)
	present := err == nil
	if err != nil && !errors.Is(err, cerrdefs.ErrNotFound) {
		// Any non-not-found inspect failure is inconclusive: surface it rather
		// than guessing, so a broken daemon never leads to a spurious pull or a
		// container decision on unknown state.
		return ServiceImage{}, fmt.Errorf("inspect image %q: %w", identity, err)
	}

	// A missing local image is pulled immediately regardless of the freshness
	// window: there is no healthy container to preserve, so waiting would only
	// leave the service unable to start. For a present image the window governs,
	// so a healthy service is never re-pulled more than once per interval.
	if !present || m.pullDue(fnName, identity) {
		if err := m.pullImage(ctx, identity); err != nil {
			return ServiceImage{}, fmt.Errorf("pull image %q: %w", identity, err)
		}
		m.recordPullCheck(fnName, identity, m.clock())
		// Re-inspect after a successful pull: a moved tag may now point at
		// different bytes, and the container must be replaced when it does.
		if insp, err = m.cli.ImageInspect(ctx, identity); err != nil {
			return ServiceImage{}, fmt.Errorf("inspect image %q after pull: %w", identity, err)
		}
	}
	return ServiceImage{Ref: identity, ID: insp.ID}, nil
}

// pullDue reports whether a remote pull check is due for fnName's service image
// identity: true when no successful check is recorded (first sighting, changed
// identity, or worker restart) or when at least one interval has elapsed since
// the last successful one. The check record is updated only on success, so a
// failed pull keeps this true and retries next pass.
func (m *Manager) pullDue(fnName, identity string) bool {
	m.pullMu.Lock()
	defer m.pullMu.Unlock()
	last, ok := m.pullChecks[pullCheckKey(fnName, identity)]
	if !ok {
		return true
	}
	return m.clock().Sub(last) >= serviceImagePullInterval
}

// recordPullCheck stamps a successful pull check at t.
func (m *Manager) recordPullCheck(fnName, identity string, t time.Time) {
	m.pullMu.Lock()
	defer m.pullMu.Unlock()
	if m.pullChecks == nil {
		m.pullChecks = map[string]time.Time{}
	}
	m.pullChecks[pullCheckKey(fnName, identity)] = t
}

// forgetServicePullChecks drops every pull-check record for a function. It is
// called when a function is removed so a later re-added function starts fresh
// (immediate remote check) and the map does not grow without bound across
// removals. It is idempotent and safe with no records.
func (m *Manager) forgetServicePullChecks(fnName string) {
	m.pullMu.Lock()
	defer m.pullMu.Unlock()
	prefix := fnName + "\x00"
	for k := range m.pullChecks {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(m.pullChecks, k)
		}
	}
}

// pullCheckKey is the per-independent-service pull-check key: the function and
// the service identity, so two functions never share a freshness window and the
// two identity parts cannot collide (the NUL separator is not valid in either a
// function name or an image reference).
func pullCheckKey(fnName, identity string) string {
	return fnName + "\x00" + identity
}

// pullImage pulls exactly one image reference (`All` is false: only the tag or
// digest the service names is fetched, never every tag in the repository). The
// response stream is drained to completion so the pull's errors surface.
func (m *Manager) pullImage(ctx context.Context, ref string) error {
	resp, err := m.cli.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	return resp.Wait(ctx)
}

// serviceBuildImageRef maps a function name, service identity, and the
// function's source fingerprint to the Relay-owned build image reference. It
// lives in the function's OWN image repository (relay-fn-<name>) with a
// content tag that folds the identity into the source fingerprint, so:
//
//   - the function's existing image retirement (FunctionImageTags /
//     RemoveFunctionImages / RemoveImagesExcept) covers build-service images
//     with no separate lifecycle, and
//   - two build services of one function never share a tag even when their
//     selected source is identical (the Dockerfile identity differs).
func serviceBuildImageRef(fnName, identity, fingerprint string) string {
	sum := sha256.Sum256([]byte(identity + "\x00" + fingerprint))
	return ImageRef(fnName, hex.EncodeToString(sum[:]))
}

// serviceBuildImageOptions is the ImageBuildOptions for a service Dockerfile
// build: the caller-selected Dockerfile path within the context, the
// content-addressed tag, and the managed function-image labels so the build
// image is recognizable to image cleanup. It mirrors buildImageOptions' Remove
// policy (prune successful intermediates) deliberately.
func serviceBuildImageOptions(ref, dockerfile, fnName string) client.ImageBuildOptions {
	return client.ImageBuildOptions{
		Tags:       []string{ref},
		Dockerfile: dockerfile,
		Remove:     true,
		Labels:     imageLabels(ImageTypeFunction, fnName, "", "", ""),
	}
}
