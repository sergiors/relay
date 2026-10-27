package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime/plan"
)

// buildTimeout bounds one Dockerfile build: the ImageBuild request and draining
// its response stream. It is far longer than the worker's 30s reconcileTimeout:
// an install step can legitimately take minutes, while a normal service converge
// must stay short.
const buildTimeout = 10 * time.Minute

// buildContext returns the bounded context for one Dockerfile build: an
// independent buildTimeout rooted at the manager's lifecycle context, not the
// caller's reconcile context. A build must not be cut off by a much shorter
// reconcile budget (the worker's 30s timeout reaches the managed build path
// through Config.UpdateServices), yet Relay shutdown must cancel it. A Manager
// constructed directly by tests has no lifecycle; Background keeps the build
// bounded without leaking a caller's short deadline into it.
func (m *Manager) buildContext() (context.Context, context.CancelFunc) {
	parent := m.lifecycle
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, buildTimeout)
}

// buildImage stages one app image from the immutable source snapshot and the
// engine's build plan. copySource is true for the historical baked-source image
// and false for a HOT_RELOAD image, whose live source is bind-mounted at runtime
// instead of copied; either way the generated plan files and the build context
// come from the one captured snapshot.
func buildImage(
	ctx context.Context,
	cli *client.Client,
	name string,
	p plan.BuildPlan,
	image string,
	labels map[string]string,
	snapshot *app.SourceSnapshot,
	reg *metrics.Registry,
	copySource bool,
) error {
	// The snapshot is the single immutable read the caller already fingerprinted:
	// the image is staged from exactly the bytes its tag describes, with the
	// app's .gitignore policy applied at capture time. The snapshot's private root
	// IS the build context, so the source is not copied a second time; Discard
	// (the capturing caller's deferred release) owns removing it on every path.
	//
	// template.yaml is excluded at capture because it is Relay configuration
	// (runtime, rules, env values, secret references), not app source; baking it
	// into a layer would embed those values. Generated plan files are written only
	// into the transient context below, so the user's app directory is never
	// modified.
	ctxDir := snapshot.Root()
	if ctxDir == "" {
		return fmt.Errorf("app %q: build source snapshot is unavailable", name)
	}

	if err := writePlanFiles(ctxDir, name, p.Files); err != nil {
		return err
	}

	dockerfile, err := renderDockerfileWithSource(p, copySource)
	if err != nil {
		return fmt.Errorf("app %q: render dockerfile: %w", name, err)
	}
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("app %q: write dockerfile: %w", name, err)
	}

	return runImageBuild(ctx, cli, name, ctxDir, image, labels, reg)
}

// writePlanFiles writes the generated plan files (bootstrap, injected
// package.json) into the staged build context. The caller's app directory is
// never modified: generated files live only in the transient context.
func writePlanFiles(ctxDir, name string, files []plan.File) error {
	for _, f := range files {
		rel := strings.TrimPrefix(filepath.Clean(f.Path), "/")
		target := filepath.Join(ctxDir, rel)
		if dir := filepath.Dir(target); dir != ctxDir {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("app %q: mkdir for %s: %w", name, f.Path, err)
			}
		}
		mode := f.Mode
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(target, f.Content, mode); err != nil {
			return fmt.Errorf("app %q: write %s: %w", name, f.Path, err)
		}
	}
	return nil
}

// buildDependencyImage builds the reusable dependency layer for an app. The
// image installs the dependencies into Deps.Dir (e.g. /app) as root and carries
// NO user setup, entrypoint, or env: it is a base for the app image, so run-time
// concerns stay in the app image's own layer.
//
// snap is the immutable manifest snapshot the caller captured: the same bytes
// whose fingerprint names the tag are staged here, so the tag can never be
// applied to different content. Its private root IS the build context; the
// caller's deferred release removes it.
//
// Concurrent builders (two workers, or two Managers sharing a daemon) stage
// isolated contexts, run the same manifest, base, and install command, and tag
// the same content-equal reference. Docker lets the tag land on identical
// content either way, so no lockfile is needed.
func buildDependencyImage(
	ctx context.Context,
	cli *client.Client,
	spec plan.Spec,
	deps plan.Deps,
	snap dependencySnapshot,
	depRef, depFingerprint string,
	targetArch string,
	reg *metrics.Registry,
) error {
	ctxDir := snap.root
	if ctxDir == "" {
		return fmt.Errorf("dependency %s: build context is unavailable", depRef)
	}

	// Stage ONLY the manifest files, not the app's source tree: baking the whole
	// source would couple the layer to every source change and defeat reuse.
	//
	// Render via the single generic renderer with a synthetic plan: the runtime
	// base, WORKDIR = the install dir, the manifests already staged at their
	// relative context paths (so COPY . <workdir> copies exactly them), the
	// runtime's external tools, and the install command. Deps is intentionally
	// zero so the renderer emits the plain COPY . <workdir> path, not a nested
	// dependency base. No User/UserSetup/Env/Entrypoint: it is a base image.
	depPlan := plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    deps.Dir,
		TargetArch: targetArch,
		// The dependency base is built FROM the raw runtime base, not the app
		// image, so it must materialize the runtime's tools itself; the app image
		// inherits them through FROM.
		RuntimeTools: spec.RuntimeTools,
		Install:      []string{deps.Install},
	}
	if depPlan.Install[0] == "" {
		return fmt.Errorf("dependency %s: empty install command", depRef)
	}
	dockerfile, err := renderDockerfile(depPlan)
	if err != nil {
		return fmt.Errorf("dependency %s: render dockerfile: %w", depRef, err)
	}
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("dependency %s: write dockerfile: %w", depRef, err)
	}

	return runImageBuild(ctx, cli, "dependency "+depRef, ctxDir, depRef, dependencyImageLabels(spec.Name, depFingerprint), reg)
}

// buildImageOptions returns the ImageBuildOptions Relay uses for every app and
// dependency build. Remove is true because the moby client v0.6.0 emits rm=0
// when Remove is false (suppressing the daemon's default cleanup); it restores
// rm=1 so successful builds prune intermediate RUN/metadata containers.
// ForceRemove is deliberately not used: failed builds keep their intermediates
// for debugging.
//
// labels stamp managed-image identity onto the image config (the build backend
// applies them as LABEL equivalents); nil stamps nothing.
//
// Platform is explicit from the resolved target architecture rather than the
// daemon's default, so the image, the dependency fingerprint, and any artifact
// variant keyed on the same arch cannot disagree (e.g. Relay under emulation).
// Every base is a Linux image, so only the architecture varies.
func buildImageOptions(image string, labels map[string]string) client.ImageBuildOptions {
	return client.ImageBuildOptions{
		Tags:       []string{image},
		Dockerfile: "Dockerfile",
		Remove:     true,
		Labels:     labels,
		Platforms:  []ocispec.Platform{buildTargetPlatform()},
	}
}

// buildTargetPlatform is the OCI platform every Relay image is built for: Linux
// at the resolved target architecture, the same key the dependency fingerprint
// and artifact variant selection use.
func buildTargetPlatform() ocispec.Platform {
	return ocispec.Platform{OS: "linux", Architecture: arch}
}
