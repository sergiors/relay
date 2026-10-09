package runtime

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime/plan"
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
// Dockerfile. It emits FROM/WORKDIR/COPY/RUN/USER/ENTRYPOINT from the plan, with
// the app source copied into WorkDir (the historical baked-source image). It
// fails for an artifact RuntimeTool with no variant for the plan's target
// architecture, so an unsupported target is a build-time error, never a silently
// missing tool.
func renderDockerfile(p plan.BuildPlan) (string, error) {
	return renderDockerfileWithSource(p, true)
}

// renderDockerfileWithSource is renderDockerfile with the source COPY made
// optional. copySource is false for a SOURCE_MOUNT app image: the app's live
// source is bind-mounted at runtime instead of being copied, so the generated
// plan files (the bootstrap, an injected package.json) and the install/user/
// entrypoint steps are emitted exactly as before but the `COPY . WorkDir` line
// is omitted. The build context still contains the selected source (it is the
// same immutable snapshot the fingerprint was derived from), so the tag and the
// mounted tree can never describe different bytes.
func renderDockerfileWithSource(p plan.BuildPlan, copySource bool) (string, error) {
	var b strings.Builder

	b.WriteString("FROM " + p.BaseImage + "\n")
	if p.WorkDir != "" {
		b.WriteString("WORKDIR " + p.WorkDir + "\n")
	}

	// App sources are copied into WorkDir.
	if copySource {
		dest := p.WorkDir
		if dest == "" {
			dest = "/"
		}
		b.WriteString("COPY . " + dest + "\n")
	}

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

	// Runtime tools materialize a pinned external binary. An image-copy tool
	// (e.g. uv) is copied out of a pinned image; a remote-archive artifact (e.g.
	// pnpm's standalone binary) is downloaded, checksum-verified, and extracted
	// from its per-architecture archive. Both are emitted BEFORE the install RUN
	// so the install can already use the tool.
	if err := renderRuntimeTools(&b, p); err != nil {
		return "", err
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
	return b.String(), nil
}

// renderRuntimeTools writes the Dockerfile instructions for every runtime tool,
// in plan order. An image-copy tool becomes a COPY --from; an artifact tool
// becomes one fail-closed RUN that downloads the pinned archive, verifies its
// SHA-256, extracts ONLY the validated member into a scratch dir, and moves it
// to its destination.
//
// Download and extraction share ONE RUN deliberately: the archive is removed
// before the layer is committed, so the final image never carries the download
// as a dead layer (an ADD would retain it even though a later RUN deletes it).
// Every command is `&&`-chained so a failed download, a bad checksum, a missing
// member, or a tar failure aborts the build instead of producing an image with a
// bogus tool. The archive is checksum-pinned and the member is a validated clean
// relative path, so extraction cannot traverse outside the scratch directory;
// only that one member is ever written.
func renderRuntimeTools(b *strings.Builder, p plan.BuildPlan) error {
	for i, tool := range p.RuntimeTools {
		if err := tool.ValidateForm(); err != nil {
			return fmt.Errorf("runtime tool %d: %w", i, err)
		}
		if tool.Artifact == nil {
			// Image-copy form: pull the pinned binary out of another image.
			if tool.From == "" || tool.Source == "" || tool.Destination == "" {
				return fmt.Errorf("runtime tool %d: incomplete image copy (from=%q source=%q destination=%q)",
					i, tool.From, tool.Source, tool.Destination)
			}
			b.WriteString("COPY --from=" + tool.From + " " + tool.Source + " " + tool.Destination + "\n")
			continue
		}
		if p.TargetArch == "" {
			return fmt.Errorf("runtime tool %q: artifact requires a target architecture", tool.Destination)
		}
		variant, ok := tool.Artifact.VariantForArch(p.TargetArch)
		if !ok {
			return fmt.Errorf("runtime tool %q: no artifact variant for architecture %q", tool.Destination, p.TargetArch)
		}
		if err := validateArtifactVariant(variant); err != nil {
			return fmt.Errorf("runtime tool %q: %w", tool.Destination, err)
		}
		if tool.Destination == "" || !strings.HasPrefix(tool.Destination, "/") {
			return fmt.Errorf("runtime tool: artifact destination %q must be an absolute path", tool.Destination)
		}
		b.WriteString(renderArtifactTool(i, tool.Destination, variant))
	}
	return nil
}

// renderArtifactTool renders one artifact tool as a single fail-closed RUN that
// downloads the pinned archive, verifies the SHA-256, extracts exactly the
// validated member, installs it at destination, and removes every scratch path
// before the layer commits. i makes the scratch paths unique per tool so two
// artifacts in one image cannot clash.
//
// The download tries wget then curl (a base image may ship either; the artifact
// is only declared by runtimes whose base provides one). If neither succeeds the
// RUN exits non-zero and the build fails closed.
func renderArtifactTool(i int, destination string, variant plan.ArtifactVariant) string {
	archive := fmt.Sprintf("/tmp/relay-tool-%d.tar.gz", i)
	unpack := fmt.Sprintf("/tmp/relay-tool-%d.unpack", i)
	destDir := path.Dir(destination)
	url := dockerfileShellArg(variant.URL)
	var b strings.Builder
	b.WriteString("RUN ( wget -q -O " + dockerfileShellArg(archive) + " " + url + " || curl -fsSL -o " + dockerfileShellArg(archive) + " " + url + " )" +
		" && echo " + dockerfileShellArg(variant.SHA256+"  "+archive) + " | sha256sum -c -" +
		" && rm -rf " + dockerfileShellArg(unpack) +
		" && mkdir -p " + dockerfileShellArg(unpack) +
		" && tar -xzf " + dockerfileShellArg(archive) + " -C " + dockerfileShellArg(unpack) + " " + dockerfileShellArg(variant.Member) +
		" && mkdir -p " + dockerfileShellArg(destDir) +
		" && mv " + dockerfileShellArg(path.Join(unpack, variant.Member)) + " " + dockerfileShellArg(destination) +
		" && chmod 0755 " + dockerfileShellArg(destination) +
		" && rm -rf " + dockerfileShellArg(unpack) + " " + dockerfileShellArg(archive) + "\n")
	return b.String()
}

// validateArtifactVariant rejects an artifact variant whose data could produce an
// unsafe or non-deterministic Dockerfile: a non-HTTPS URL, a malformed digest, a
// member that is not a clean relative path, or shell-hostile URL bytes. The
// plan data is trusted (registry constants), but validation is defense-in-depth
// against a future tool definition, and it makes path traversal impossible.
func validateArtifactVariant(v plan.ArtifactVariant) error {
	if v.Arch == "" {
		return fmt.Errorf("artifact variant has no architecture")
	}
	if !strings.HasPrefix(v.URL, "https://") {
		return fmt.Errorf("artifact variant for %s: url %q must be https", v.Arch, v.URL)
	}
	if strings.ContainsAny(v.URL, " \t\r\n\"'`\\") {
		return fmt.Errorf("artifact variant for %s: url %q contains unsafe characters", v.Arch, v.URL)
	}
	if len(v.SHA256) != 64 {
		return fmt.Errorf("artifact variant for %s: sha256 %q must be 64 hex characters", v.Arch, v.SHA256)
	}
	for _, c := range v.SHA256 {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("artifact variant for %s: sha256 %q must be lowercase hex", v.Arch, v.SHA256)
		}
	}
	return validateArtifactMember(v.Member)
}

// validateArtifactMember rejects a tar member that is not a clean, non-option
// relative path, so a crafted member can never escape the extraction directory
// or be mistaken for a tar flag: an empty member, an absolute path, a leading
// "-", a backslash, or any "."/".." segment is an error.
func validateArtifactMember(member string) error {
	if member == "" {
		return fmt.Errorf("artifact member is empty")
	}
	if strings.HasPrefix(member, "/") || strings.HasPrefix(member, "-") {
		return fmt.Errorf("artifact member %q must be a relative path without a leading %q", member, string(member[0]))
	}
	if strings.Contains(member, "\\") {
		return fmt.Errorf("artifact member %q must not contain a backslash", member)
	}
	if path.Clean(member) != member {
		return fmt.Errorf("artifact member %q must be a clean relative path", member)
	}
	for _, seg := range strings.Split(member, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("artifact member %q must not contain an empty, \".\", or \"..\" segment", member)
		}
	}
	return nil
}

// dockerfileShellArg renders s as exactly one POSIX shell word for a Dockerfile
// RUN. A string made only of conservative shell-safe bytes is emitted bare so
// ordinary paths and digests stay readable; anything else is single-quoted.
// Plan data is validated to be safe before it reaches here; the quoting is
// defense-in-depth so a future value cannot split or expand inside the RUN.
func dockerfileShellArg(s string) string {
	if s == "" {
		return "''"
	}
	if dockerfileShellSafe(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// dockerfileShellSafe reports whether every byte of s needs no quoting. It is
// deliberately conservative: a byte outside the set always triggers quoting.
func dockerfileShellSafe(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_', c == '@', c == '%', c == '+', c == '=', c == ':', c == ',', c == '.', c == '/', c == '-':
		default:
			return false
		}
	}
	return true
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

// buildImage stages one app image from the immutable source snapshot and the
// engine's build plan. copySource is true for the historical baked-source image
// and false for a SOURCE_MOUNT app image, whose live source is bind-mounted at
// runtime instead of copied; either way the generated plan files and the build
// context come from the one captured snapshot.
func buildImage(
	ctx context.Context,
	cli *client.Client,
	name string,
	fn app.App,
	p plan.BuildPlan,
	image string,
	labels map[string]string,
	snapshot *app.SourceSnapshot,
	reg *metrics.Registry,
	copySource bool,
) error {
	// snapshot is the SINGLE immutable read the caller already fingerprinted: the
	// image is staged from exactly the bytes its tag was derived from, so a
	// concurrent edit between the fingerprint and the build can no longer make the
	// tag and the baked content disagree. The app's .gitignore policy was applied
	// at capture time (ignored files and .git were never captured), so the staged
	// context contains exactly the selected source. The snapshot's private root IS
	// the build context: the source is not copied a second time, and Discard (the
	// capturing caller's deferred release) owns removing it on every path.
	//
	// template.yaml is excluded from the context even though it participates in
	// the fingerprint: the template is Relay configuration (runtime, rules, env
	// values, secret references), not app source, so baking it into the image
	// would embed env values and secret references in the image layers. Generated
	// plan files are written separately below, so the user's app directory is
	// never modified.
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
// package.json) into the staged build context. The caller's app directory
// is never modified: generated files live only in the transient context.
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

// buildOutputRetention bounds the whole retained Docker build diagnostic: the
// Docker error message and the retained stream text together. A build can stream
// an unbounded amount of output (an install step's progress) and the daemon's
// error message can itself be arbitrarily large, so both are capped rather than
// accumulated without limit; the remainder is drained and discarded. There is
// deliberately no user-facing configuration for this: it is a diagnostic bound,
// not a behavior knob.
const buildOutputRetention = 1 << 20 // 1 MiB

// buildOutputTruncatedMarker is appended to a retained build diagnostic when
// bytes were discarded, so an operator can tell a genuinely short failure output
// from one that was cut at the retention bound.
const buildOutputTruncatedMarker = "[build output truncated]"

// runImageBuild is the shared ImageBuild tail: the Dockerfile is already written
// by the caller; this streams the context tar, builds, and drains the response.
// labels are the managed-image labels stamped onto the resulting image (see
// buildImageOptions / managed image labeling in labels.go); nil means no labels.
//
// The context tar is STREAMED into the daemon through an io.Pipe rather than
// buffered in memory first, so an arbitrarily large build context never has to
// be materialized in the worker heap. The producer goroutine is joined on every
// path (success, daemon error, cancellation, and early consumer close), so a
// failed or cancelled build cannot leak it or block on a full pipe.
func runImageBuild(ctx context.Context, cli *client.Client, name, ctxDir, image string, labels map[string]string, reg *metrics.Registry) error {
	pr, pw := io.Pipe()
	producerDone := make(chan error, 1)
	go func() {
		err := writeContextTar(ctx, ctxDir, pw)
		// CloseWithError closes the write end: nil yields io.EOF so the consumer
		// sees a clean end, otherwise the reader observes the producer's error.
		_ = pw.CloseWithError(err)
		producerDone <- err
	}()

	resp, buildErr := cli.ImageBuild(ctx, pr, buildImageOptions(image, labels))
	if buildErr != nil {
		// The client did not produce a response (a transport failure or a
		// cancelled context). It may have abandoned the context body, so unblock
		// the producer before joining it; Close is idempotent and a no-op if the
		// producer already finished, so the join below cannot hang.
		_ = pr.Close()
		producerErr := <-producerDone
		// The transport failure is the build failure. A producer error is already
		// reflected through it when the daemon read the failing stream; on a
		// non-read failure the producer was abandoned, which is not itself a
		// second fault to report.
		if producerErr != nil && !errors.Is(producerErr, io.ErrClosedPipe) && !errors.Is(producerErr, context.Canceled) {
			return fmt.Errorf("app %q: docker build: %w", name, errors.Join(buildErr, producerErr))
		}
		return fmt.Errorf("app %q: docker build: %w", name, buildErr)
	}

	// Drain the response BEFORE joining the producer. The HTTP transport writes
	// the request body on its own goroutine while this goroutine reads the
	// response, so the daemon can always flush its output and finish reading the
	// context; waiting for the producer first could deadlock against a server
	// blocked writing a response nobody is reading. Draining also consumes the
	// full stream after a build failure.
	out, truncated, drainErr := drainBuildResponse(resp.Body)
	_ = resp.Body.Close()
	// If the response ended without the transport having consumed the whole
	// context (an early abort or a daemon that stopped reading), unblock the
	// producer so the join below cannot hang. Closing an already-closed pipe is a
	// no-op.
	_ = pr.Close()
	producerErr := <-producerDone

	// A producer error that is only the pipe closing under it (the consumer
	// returned early, or we closed the read end after the response) or the build
	// context being cancelled is not itself a tar fault: the real outcome is the
	// drained response below. Any OTHER producer error (a filesystem failure)
	// means the tar Relay sent was incomplete, which must never pass as a
	// successful build.
	if producerErr != nil && !errors.Is(producerErr, io.ErrClosedPipe) && !errors.Is(producerErr, context.Canceled) && !errors.Is(producerErr, context.DeadlineExceeded) {
		return fmt.Errorf("app %q: tar build context: %w", name, producerErr)
	}
	if truncated {
		reg.Inc(metrics.MetricBuildOutputTruncated)
	}
	// The build API returns 200 even when the build fails; failure is signalled
	// by an "error" JSON message in the response stream (drainErr), or by the
	// build context being cancelled. The returned output is a bounded diagnostic;
	// on success it is discarded by the caller.
	if drainErr != nil {
		return fmt.Errorf("app %q: docker build: %w\n%s", name, drainErr, strings.TrimSpace(out))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("app %q: docker build: %w", name, ctxErr)
	}
	return nil
}

// buildDependencyImage builds the reusable dependency layer for an app. The
// image installs the dependencies into Deps.Dir (e.g. /app) as ROOT and carries
// NO user setup, entrypoint, or env — it is a BASE for the app image, not a
// runnable image, so run-time concerns (the runtime user, the entrypoint) stay in
// the app image's own layer.
//
// snap is the immutable manifest snapshot captured by the caller (see
// snapshotDependency): the SAME bytes whose fingerprint names the tag are the
// bytes staged here, so the image can never be tagged for one manifest content
// while baking another.
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
	deps plan.Deps,
	snap dependencySnapshot,
	depRef, depFingerprint string,
	targetArch string,
	reg *metrics.Registry,
) error {
	// snap is the immutable manifest snapshot the caller captured (see
	// snapshotDependency): the SAME bytes whose fingerprint names the tag are the
	// bytes staged here, so the image can never be tagged for one manifest content
	// while baking another. The snapshot's private root IS the dependency build
	// context: the manifests are not copied a second time, and the caller's
	// deferred release removes it on every path.
	//
	// Stage ONLY the manifest files, not the app's source tree. The dependency
	// image exists to cache the install; baking the whole source would couple the
	// layer to every source change and defeat the reuse.
	ctxDir := snap.root
	if ctxDir == "" {
		return fmt.Errorf("dependency %s: build context is unavailable", depRef)
	}

	// Render via the single generic renderer with a synthetic plan: the runtime
	// base, WORKDIR = the install dir, the manifests already staged at their
	// relative context paths (so the generic `COPY . <workdir>` copies exactly
	// them), the runtime's external tools (the install may need the tool, e.g.
	// uv), and the install command. No User/UserSetup/Env/Entrypoint — it is a
	// base image.
	depPlan := plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    deps.Dir,
		TargetArch: targetArch,
		// The dependency base image is built FROM the raw runtime base (not the
		// app image), so it must materialize the runtime's external tools
		// itself: the app image inherits them through FROM, but the dependency
		// build cannot.
		RuntimeTools: spec.RuntimeTools,
		// Note: plan.Deps is intentionally left zero here so the renderer emits
		// the plain `COPY . <workdir>` path, not a nested dependency base.
		Install: []string{deps.Install},
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

// buildImageOptions returns the ImageBuildOptions Relay uses for every app
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
//
// Platform is set EXPLICITLY from the resolved target architecture instead of
// letting the builder default to the daemon's own platform: the dependency
// fingerprint and any artifact RuntimeTool variant are keyed on the same arch,
// and an implicit default could let the built image and that key disagree (e.g.
// a Relay process running under emulation on a differently-architected daemon).
// Relay only ever builds LINUX container images (every base is a Linux image),
// so the target OS is linux regardless of the Relay process host OS (a macOS
// dev process talks to a Linux Docker VM); only the architecture varies.
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
// at the resolved target architecture (the same `arch` key the dependency
// fingerprint and artifact-tool variant selection use). It is the single source
// of truth passed to Docker's ImageBuild, so the platform the builder targets
// can never silently diverge from the architecture Relay keyed the build on.
func buildTargetPlatform() ocispec.Platform {
	return ocispec.Platform{OS: "linux", Architecture: arch}
}

// drainBuildResponse reads the JSON message stream returned by ImageBuild,
// retaining a BOUNDED diagnostic and failing on the first message that carries
// an error.
//
// The whole response is always consumed, even after the retention cap is reached
// and even after the first Docker error, so the daemon's response body is
// drained rather than abandoned mid-stream (which can stall the connection).
// The first error is remembered; draining continues so the stream is not left
// half-read. The retained diagnostic as a whole (the first Docker error message
// plus the retained stream text) is bounded by buildOutputRetention: an
// oversized Docker error message is itself capped (copied, never left backed by
// the original decoded string), and the stream budget is reduced by the error
// message it must accompany. Whatever was cut is marked with
// buildOutputTruncatedMarker, so an operator can tell a short failure from a
// truncated one. On a successful stream the (possibly bounded) output is
// returned with a nil error; the caller discards it.
//
// A malformed message ends parsing: JSON cannot be resynchronized, so the
// remaining messages are not interpreted. After the first error the malformed
// message is ignored (the first error already explains the failure); before any
// error the decode error is returned. Either way the decoder's buffered
// remainder and the underlying reader are drained to io.Discard first, so the
// daemon's response body is still fully consumed rather than abandoned (which
// can stall the connection).
//
// It also reports whether bytes were discarded (truncation), so the caller can
// record the bounded-diagnostic anomaly.
func drainBuildResponse(r io.Reader) (string, bool, error) {
	var out strings.Builder
	truncated := false
	var firstErr error
	var decodeErr error
	// errMsg is the bounded first Docker error message. It reserves part of the
	// retention budget so the combined diagnostic (error message plus retained
	// stream text) stays bounded.
	errMsg := ""

	dec := json.NewDecoder(r)
	for {
		var msg jsonstream.Message
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Parsing stops here, but the body must still be drained: consume
			// whatever the decoder buffered and then the rest of the underlying
			// reader before reporting the preserved error.
			drainDecoderRemainder(dec, r)
			// The build already failed with a Docker error; a malformed trailing
			// message adds no signal and must not replace the real cause.
			if firstErr == nil {
				decodeErr = err
			}
			break
		}
		if msg.Error != nil {
			if firstErr == nil {
				capped, cut := capBuildError(msg.Error)
				firstErr = capped
				errMsg = capped.Message
				if cut {
					truncated = true
				}
			}
			// Keep draining: the remainder of the response must still be read.
			continue
		}
		if msg.Stream == "" {
			continue
		}
		if truncated {
			continue
		}
		remaining := buildStreamBudget(errMsg) - out.Len()
		if remaining <= 0 {
			truncated = true
			continue
		}
		if len(msg.Stream) <= remaining {
			out.WriteString(msg.Stream)
			continue
		}
		out.WriteString(msg.Stream[:remaining])
		truncated = true
	}
	// Finalize once: an error message decoded late may have shrunk the stream
	// budget after text was already retained, so the combined diagnostic is
	// trimmed here too and the truncation flag is refreshed.
	text, cut := finishBuildOutput(out, truncated, errMsg)
	if firstErr != nil {
		return text, cut, firstErr
	}
	return text, cut, decodeErr
}

// capBuildError bounds the first Docker error message to buildOutputRetention and
// reports whether the message was cut. An oversized message is copied, so the
// retained diagnostic never keeps the original, arbitrarily large decoded string
// alive. The error's code and an in-budget message are preserved verbatim.
func capBuildError(e *jsonstream.Error) (*jsonstream.Error, bool) {
	if len(e.Message) <= buildOutputRetention {
		return e, false
	}
	capped := *e
	capped.Message = strings.Clone(e.Message[:buildOutputRetention])
	return &capped, true
}

// buildStreamBudget returns how many bytes of stream output may be retained
// alongside a first Docker error message of errMsg bytes, so the combined
// retained diagnostic never exceeds buildOutputRetention.
func buildStreamBudget(errMsg string) int {
	remaining := buildOutputRetention - len(errMsg)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// drainDecoderRemainder consumes everything a json.Decoder has already buffered
// but not yet consumed, then the rest of the underlying reader, so a caller that
// stops parsing mid-stream still reads the body to EOF. It must be called only
// after a decode error, when the decoder's position no longer matters. Errors
// from the underlying reader are intentionally ignored: the caller already has
// the parse failure (or the first Docker error) to report, and draining is
// best-effort cleanup of a response that will be closed anyway.
func drainDecoderRemainder(dec *json.Decoder, r io.Reader) {
	_, _ = io.Copy(io.Discard, dec.Buffered())
	_, _ = io.Copy(io.Discard, r)
}

// finishBuildOutput renders the retained build text, appending the truncation
// marker when bytes were discarded and a separating newline when the retained
// text does not already end with one. errMsg is the retained first Docker error
// message, whose bytes reserve part of the retention budget: the returned stream
// text is itself trimmed to the remaining budget, so the combined diagnostic
// (error message plus stream text) never exceeds buildOutputRetention before the
// fixed marker. It returns the rendered text and whether it truncated, which
// covers the case where the budget only shrank when a later error message was
// decoded. It never returns more than the stream budget bytes of build output
// before the marker.
func finishBuildOutput(out strings.Builder, truncated bool, errMsg string) (string, bool) {
	s := out.String()
	if budget := buildStreamBudget(errMsg); len(s) > budget {
		s = s[:budget]
		truncated = true
	}
	if !truncated {
		return s, false
	}
	if s != "" && !strings.HasSuffix(s, "\n") {
		s += "\n"
	}
	return s + buildOutputTruncatedMarker, true
}

// writeContextTar streams a tar of ctxDir into w. It is the producer half of the
// streamed build context (see runImageBuild): it never buffers the whole tar, so
// an arbitrarily large context is bounded by the pipe, not by heap. It honors
// ctx: a cancelled build stops the walk at the next entry and returns the
// context error, so the producer cannot outlive its build.
func writeContextTar(ctx context.Context, ctxDir string, w io.Writer) error {
	tw := tar.NewWriter(w)
	err := filepath.Walk(ctxDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
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
		return err
	}
	// Flush the tar trailer, but only when the consumer is still reading: if the
	// daemon already closed the pipe, the tar writer's final write fails and that
	// is the consumer's early return, not a build fault of ours.
	if err := tw.Close(); err != nil {
		return err
	}
	return nil
}
