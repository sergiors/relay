package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runtime/plan"
	"relay/internal/source"
)

// buildTimeout bounds a single Dockerfile build: the ImageBuild request and
// draining its response stream. It is deliberately far longer than the worker's
// reconcileTimeout (30s): an install step can legitimately take minutes, while a
// normal service converge must stay short. Builds are NOT children of the
// caller's reconcile context — Manager.buildContext roots them in the manager
// lifecycle instead, so a slow build is bounded here AND still cancelled when
// Relay shuts down.
const buildTimeout = 10 * time.Minute

// buildContext returns the bounded context for one Dockerfile build: an
// independent buildTimeout rooted at the manager's lifecycle context. Rooting at
// the manager lifecycle (cancelled by Close) rather than the caller's reconcile
// context is deliberate — a build must not be cut off by a much shorter
// reconcile budget (the worker's 30s reconcileTimeout reaches the managed
// runtime build path through Config.UpdateServices), yet it must still be
// cancelled by Relay worker/reconciler shutdown. A Manager constructed directly
// by tests leaves lifecycle nil; context.Background keeps builds bounded without
// leaking a caller's short deadline into them.
func (m *Manager) buildContext() (context.Context, context.CancelFunc) {
	parent := m.lifecycle
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, buildTimeout)
}

// renderDockerfile is the ONLY Dockerfile renderer, shared by every engine; an
// engine must express its concerns as plan data rather than generate a
// Dockerfile. It emits FROM/WORKDIR/COPY/RUN/USER/ENTRYPOINT from the plan.
func renderDockerfile(p plan.BuildPlan) string {
	var b strings.Builder

	b.WriteString("FROM " + p.BaseImage + "\n")
	if p.WorkDir != "" {
		b.WriteString("WORKDIR " + p.WorkDir + "\n")
	}

	// Function sources are copied into WorkDir.
	dest := p.WorkDir
	if dest == "" {
		dest = "/"
	}
	b.WriteString("COPY . " + dest + "\n")

	// The builder mirrors each file's absolute Path into a relative context path
	// (leading '/' stripped), so COPY sources that same relative path and writes
	// it into the file's parent directory in the image.
	for _, f := range p.Files {
		rel := strings.TrimPrefix(filepath.Clean(f.Path), "/")
		dir := "/"
		if i := strings.LastIndex(f.Path, "/"); i > 0 {
			dir = f.Path[:i]
		}
		b.WriteString("COPY " + rel + " " + dir + "/\n")
	}

	// External tool copies pull a pinned binary out of another image (e.g. the
	// uv distroless image). They are emitted BEFORE the install RUN so the
	// dependency install can already use the tool.
	for _, tc := range p.ToolCopies {
		b.WriteString("COPY --from=" + tc.From + " " + tc.Source + " " + tc.Dest + "\n")
	}

	for _, cmd := range p.Install {
		if cmd != "" {
			b.WriteString("RUN " + cmd + "\n")
		}
	}

	// UserSetup runs as root after Install so dependency installation (which
	// needs root for site-packages/node_modules) is unaffected by the runtime
	// user. It creates the user and hands the writable paths to it.
	if p.UserSetup != "" {
		b.WriteString("RUN " + p.UserSetup + "\n")
	}
	if p.User != "" {
		b.WriteString("USER " + p.User + "\n")
	}

	if len(p.Entrypoint) > 0 {
		b.WriteString("ENTRYPOINT " + quoteEntrypointJSON(p.Entrypoint) + "\n")
	}
	return b.String()
}

// quoteEntrypointJSON renders an ENTRYPOINT as a JSON array so that arguments
// with special characters are preserved.
func quoteEntrypointJSON(args []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, a := range args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(a))
	}
	b.WriteByte(']')
	return b.String()
}

func buildImage(
	ctx context.Context,
	cli *client.Client,
	name string,
	fn function.Function,
	p plan.BuildPlan,
	image string,
	labels map[string]string,
	selection *source.Selection,
) error {
	ctxDir, err := os.MkdirTemp("", "relay-build-*")
	if err != nil {
		return fmt.Errorf("function %q: create build context: %w", name, err)
	}
	defer os.RemoveAll(ctxDir)

	// selection is the SAME source-selection policy the caller fingerprinted, so
	// the image contains exactly the selected source: files excluded by the
	// function's .gitignore rules are never baked into the image and the
	// applicable ignore files are. Resolving it once in the caller keeps the
	// fingerprint and the context from racing a concurrent rule edit. Staging is
	// read-only; the user's function directory is never modified.
	//
	// Copy the selected function sources into the context, EXCLUDING
	// template.yaml. The template is Relay configuration (runtime, rules, env
	// values, secret references), not function source: baking it into the image
	// would embed env values and secret references in the image layers. The
	// fingerprint still covers template.yaml (its content gates rebuilds), but the
	// image never contains it. Generated plan files are written separately, so the
	// user's function directory is never modified.
	if err := copySourceDir(selection, ctxDir); err != nil {
		return fmt.Errorf("function %q: copy sources: %w", name, err)
	}

	if err := writePlanFiles(ctxDir, name, p.Files); err != nil {
		return err
	}

	dockerfile := renderDockerfile(p)
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("function %q: write dockerfile: %w", name, err)
	}

	return runImageBuild(ctx, cli, name, ctxDir, image, labels)
}

// writePlanFiles writes the generated plan files (bootstrap, injected
// package.json) into the staged build context. The caller's function directory
// is never modified: generated files live only in the transient context.
func writePlanFiles(ctxDir, name string, files []plan.File) error {
	for _, f := range files {
		rel := strings.TrimPrefix(filepath.Clean(f.Path), "/")
		target := filepath.Join(ctxDir, rel)
		if dir := filepath.Dir(target); dir != ctxDir {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("function %q: mkdir for %s: %w", name, f.Path, err)
			}
		}
		mode := f.Mode
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(target, f.Content, mode); err != nil {
			return fmt.Errorf("function %q: write %s: %w", name, f.Path, err)
		}
	}
	return nil
}

// runImageBuild is the shared ImageBuild tail: write the Dockerfile is already
// done by the caller; this tars the context, builds, and drains the response.
// labels are the managed-image labels stamped onto the resulting image (see
// buildImageOptions / managed image labeling in labels.go); nil means no labels.
func runImageBuild(ctx context.Context, cli *client.Client, name, ctxDir, image string, labels map[string]string) error {
	// The daemon expects the build context as a tar stream; build it in memory
	// from the staged directory rather than shelling out to tar.
	contextTar, err := tarContext(ctxDir)
	if err != nil {
		return fmt.Errorf("function %q: tar build context: %w", name, err)
	}

	resp, err := cli.ImageBuild(ctx, contextTar, buildImageOptions(image, labels))
	if err != nil {
		return fmt.Errorf("function %q: docker build: %w", name, err)
	}
	defer resp.Body.Close()

	// The build API returns 200 even when the build fails; failure is signalled
	// by an "error" JSON message in the response stream, so drain it and treat
	// any such message as a failed build.
	out, err := drainBuildResponse(resp.Body)
	if err != nil {
		return fmt.Errorf("function %q: docker build: %w\n%s", name, err, strings.TrimSpace(out))
	}
	return nil
}

// buildDependencyImage builds the reusable dependency layer for a function. The
// image installs the dependencies into Deps.Dir (e.g. /app) as ROOT and carries
// NO user setup, entrypoint, or env — it is a BASE for the function image, not a
// runnable image, so run-time concerns (the runtime user, the entrypoint) stay in
// the function image's own layer.
//
// Concurrency: two processes may build the same dependency image concurrently
// (two Relay workers, or two Manager instances sharing a daemon). Both stage
// isolated temp contexts (per-call), run the exact same manifest + base + install
// command, and tag the same reference; Docker lets the tag land on the identical
// content either way (last tag wins, content-equal), so no lockfile is needed.
func buildDependencyImage(
	ctx context.Context,
	cli *client.Client,
	spec plan.Spec,
	fnDir string,
	deps plan.Deps,
	depRef, depFingerprint string,
) error {
	ctxDir, err := os.MkdirTemp("", "relay-dep-build-*")
	if err != nil {
		return fmt.Errorf("dependency %s: create build context: %w", depRef, err)
	}
	defer os.RemoveAll(ctxDir)

	// Stage ONLY the manifest files, not the function's source tree. The
	// dependency image exists to cache the install; baking the whole source
	// would couple the layer to every source change and defeat the reuse.
	for _, name := range deps.Files {
		src := filepath.Join(fnDir, filepath.FromSlash(name))
		content, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("dependency %s: read manifest %q: %w", depRef, name, err)
		}
		target := filepath.Join(ctxDir, filepath.FromSlash(name))
		if dir := filepath.Dir(target); dir != ctxDir {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("dependency %s: mkdir for %s: %w", depRef, name, err)
			}
		}
		if err := os.WriteFile(target, content, 0o644); err != nil {
			return fmt.Errorf("dependency %s: write manifest %s: %w", depRef, name, err)
		}
	}

	// Render via the single generic renderer with a synthetic plan: the runtime
	// base, WORKDIR = the install dir, the manifests already staged at their
	// relative context paths (so the generic `COPY . <workdir>` copies exactly
	// them), the runtime's external tool copies (the install may need the tool,
	// e.g. uv), and the install command. No User/UserSetup/Env/Entrypoint — it is
	// a base image.
	depPlan := plan.BuildPlan{
		BaseImage: spec.BaseImage,
		WorkDir:   deps.Dir,
		// The dependency base image is built FROM the raw runtime base (not the
		// function image), so it must copy the runtime's external tools itself:
		// the function image inherits them through FROM, but the dependency
		// build cannot.
		ToolCopies: spec.ToolCopies,
		// Note: plan.Deps is intentionally left zero here so the renderer emits
		// the plain `COPY . <workdir>` path, not a nested dependency base.
		Install: []string{deps.Install},
	}
	if depPlan.Install[0] == "" {
		return fmt.Errorf("dependency %s: empty install command", depRef)
	}
	dockerfile := renderDockerfile(depPlan)
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("dependency %s: write dockerfile: %w", depRef, err)
	}

	return runImageBuild(ctx, cli, "dependency "+depRef, ctxDir, depRef, dependencyImageLabels(spec.Name, depFingerprint))
}

// buildImageOptions returns the ImageBuildOptions Relay uses for every function
// and dependency build. Remove is set to true deliberately: the moby client
// v0.6.0 emits
// rm=0 when Remove is false (it only sends the value when opting out of the
// daemon's default), which suppresses the daemon's default cleanup of
// intermediate containers after a successful classic-builder build. Remove:true
// restores rm=1, so the daemon prunes the intermediate RUN and metadata-step
// containers (and the dangling parent-chain head image) once a build succeeds.
//
// labels are the managed-image labels stamped onto the resulting image config.
// The build backend (buildkit, and the classic builder) applies them as LABEL
// instructions equivalent — the same mechanism as a Dockerfile LABEL line — so
// the labels land on the image and are visible via ImageList's per-image
// Labels. Labels are nil when a build should stamp nothing (no managed-image
// identity to record).
//
// Failed builds intentionally keep their intermediates: the daemon only removes
// intermediates when the build completed successfully (Remove && retErr == nil),
// so a failed build leaves its intermediate state in place for debugging.
// ForceRemove is deliberately not used: it would also remove intermediates on
// failure, which we do not want.
func buildImageOptions(image string, labels map[string]string) client.ImageBuildOptions {
	return client.ImageBuildOptions{
		Tags:       []string{image},
		Dockerfile: "Dockerfile",
		Remove:     true,
		Labels:     labels,
	}
}

// drainBuildResponse reads the JSON message stream returned by ImageBuild,
// collecting the build output and failing on the first message that carries an
// error.
func drainBuildResponse(r io.Reader) (string, error) {
	var out strings.Builder
	dec := json.NewDecoder(r)
	for {
		var msg jsonstream.Message
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out.String(), err
		}
		if msg.Error != nil {
			return out.String(), msg.Error
		}
		if msg.Stream != "" {
			out.WriteString(msg.Stream)
		}
	}
	return out.String(), nil
}

func tarContext(ctxDir string) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := filepath.Walk(ctxDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(ctxDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		name := filepath.ToSlash(rel)
		if info.IsDir() {
			return tw.WriteHeader(&tar.Header{
				Name:     name + "/",
				Mode:     int64(info.Mode().Perm()),
				Typeflag: tar.TypeDir,
			})
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: int64(info.Mode().Perm()),
			Size: info.Size(),
		}); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		// Close explicitly here, not via defer: a deferred Close inside a walk
		// callback would not run until the whole walk completes, holding fds
		// open for the entire context.
		if _, err := io.Copy(tw, f); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// copySourceDir stages the SELECTED function source into the build context. It
// walks the shared selection (so files excluded by the function's .gitignore
// rules are never copied and the applicable ignore files are) and additionally
// skips any file named template.yaml anywhere in the tree. That exclusion is by
// base name so a nested template.yaml can never leak Relay configuration —
// including env values and secret references — into an image; the loader only
// ever reads the top-level one, so nested copies are dead weight at best.
func copySourceDir(selection *source.Selection, dst string) error {
	return selection.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(selection.Dir(), path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if filepath.Base(rel) == "template.yaml" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}
