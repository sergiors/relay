package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runtime/plan"
)

// tagPrefixLen is the number of hex fingerprint characters used as the docker
// tag. The full fingerprint is a 64-hex SHA-256 over the function directory;
// that remains authoritative everywhere it is persisted (SQLite, Prepared). The
// tag prefix is an opaque, collision-safe short handle: 16 hex chars = 64 bits,
// and a birthday collision at the function-version counts Relay deals with (a
// handful per function, tens of functions) is astronomically unlikely. Git's
// default short-hash length is 7–12 chars (28–48 bits); 16 chars is comfortably
// beyond that while staying well inside docker's tag length limit combined with
// a validated name (name ≤ 63 chars, "relay-fn-" prefix, ":<16hex>" suffix → ≤
// ~89 chars < 128). A full-fingerprint tag would add nothing but length: the
// fingerprint still uniquely determines the tag, so equality on the tag is
// equality on the source.
const tagPrefixLen = 16

// ptr returns a pointer to v. It is a tiny helper for the pointer-typed fields
// in the container HostConfig (e.g. PidsLimit) so the hardening values read as
// literals rather than requiring a local variable.
func ptr[T any](v T) *T { return &v }

// ImageRef maps a validated function name and its content fingerprint to the
// docker image reference for that exact source version. No sanitizing is needed
// for the name: function names are validated at load time (internal/function) to
// be [a-z0-9][a-z0-9._-]* and not end in '.', so they are already legal docker
// repository names. The reference always carries the short fingerprint tag, so
// every distinct source version of a function is a distinct docker image and can
// be built, reused, and retired independently without ever clobbering a sibling
// version. The "relay-fn-" prefix namespaces all of Relay's images so they never
// collide with unrelated images on the same daemon.
//
// Defensive on short input: fingerprints are always 64 chars in practice, but a
// caller (or a truncated persisted value) passing fewer hex chars must not panic;
// the tag is simply the first min(len, tagPrefixLen) chars.
func ImageRef(name, fingerprint string) string {
	if len(fingerprint) > tagPrefixLen {
		fingerprint = fingerprint[:tagPrefixLen]
	}
	return "relay-fn-" + name + ":" + fingerprint
}

func buildImage(
	ctx context.Context,
	cli *client.Client,
	name string,
	fn function.Function,
	p plan.BuildPlan,
	image string,
) error {
	ctxDir, err := os.MkdirTemp("", "relay-build-*")
	if err != nil {
		return fmt.Errorf("function %q: create build context: %w", name, err)
	}
	defer os.RemoveAll(ctxDir)

	// Copy the function directory into the context, EXCLUDING template.yaml.
	// The template is Relay configuration (runtime, rules, env values, secret
	// references), not function source: baking it into the image would embed env
	// values and secret references in the image layers. The fingerprint still
	// covers template.yaml (its content gates rebuilds), but the image never
	// contains it. Generated plan files are written separately, so the user's
	// function directory is never modified.
	if err := copyDir(fn.Dir, ctxDir, map[string]bool{"template.yaml": true}); err != nil {
		return fmt.Errorf("function %q: copy sources: %w", name, err)
	}

	for _, f := range p.Files {
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

	dockerfile := renderDockerfile(p)
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("function %q: write dockerfile: %w", name, err)
	}

	// The daemon expects the build context as a tar stream; build it in memory
	// from the staged directory rather than shelling out to tar.
	contextTar, err := tarContext(ctxDir)
	if err != nil {
		return fmt.Errorf("function %q: tar build context: %w", name, err)
	}

	resp, err := cli.ImageBuild(ctx, contextTar, buildImageOptions(image))
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

// buildImageOptions returns the ImageBuildOptions Relay uses for every function
// build. Remove is set to true deliberately: the moby client v0.6.0 emits
// rm=0 when Remove is false (it only sends the value when opting out of the
// daemon's default), which suppresses the daemon's default cleanup of
// intermediate containers after a successful classic-builder build. Remove:true
// restores rm=1, so the daemon prunes the intermediate RUN and metadata-step
// containers (and the dangling parent-chain head image) once a build succeeds.
//
// Failed builds intentionally keep their intermediates: the daemon only removes
// intermediates when the build completed successfully (Remove && retErr == nil),
// so a failed build leaves its intermediate state in place for debugging.
// ForceRemove is deliberately not used: it would also remove intermediates on
// failure, which we do not want.
func buildImageOptions(image string) client.ImageBuildOptions {
	return client.ImageBuildOptions{
		Tags:       []string{image},
		Dockerfile: "Dockerfile",
		Remove:     true,
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

// killContainer force-kills a container with a detached, bounded context so it
// works even when the invocation ctx is already cancelled.
func killContainer(cli *client.Client, id string) {
	killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = cli.ContainerKill(killCtx, id, client.ContainerKillOptions{})
}

// benignRemovalErr reports whether a container-removal error means the container
// will not outlive the call, which is the only contract a best-effort removal
// has. Two daemon responses are benign:
//
//   - not-found (cerrdefs.ErrNotFound): the daemon already removed the container
//     (AutoRemove finished, or a previous remove succeeded). Nothing left to do.
//   - conflict (cerrdefs.ErrConflict, HTTP 409): the daemon is removing the
//     container right now (AutoRemove in flight). The container still exists in
//     the daemon's map until the removal completes, so a DELETE issued in that
//     window races the daemon's own removal and answers 409 rather than 404; the
//     container will still be gone.
//
// A nil error (no error) is trivially benign. Any other error (permission
// denied, internal, ...) is a genuine failure and is NOT benign. Matching is
// typed (errors.Is against the errdefs sentinels), never on the error string.
func benignRemovalErr(err error) bool {
	return err == nil || errors.Is(err, cerrdefs.ErrNotFound) || errors.Is(err, cerrdefs.ErrConflict)
}

// removeContainer removes a container, treating an already-removed container and
// a removal already in progress as success. With AutoRemove the daemon removes
// the container as soon as it exits, so Relay's best-effort removal routinely
// races the daemon: it either finds the container already gone (not-found) or
// already being removed (conflict/409). Both mean "the container will not outlive
// this call", which is the only contract a best-effort remove has, so neither is
// logged as noise. Only a real (non-benign) removal failure is surfaced so the
// caller can log it.
func removeContainer(cli *client.Client, id string) error {
	rmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ContainerRemove(rmCtx, id, client.ContainerRemoveOptions{Force: true})
	if benignRemovalErr(err) {
		return nil
	}
	return err
}

// removeBackstop best-effort removes a container and logs a genuine failure. It
// is called only on the paths where the container may still be alive or never
// exited on its own (failed start, kill/timeout/cancel cleanup, wait error). On
// the normal path the container exits on its own and AutoRemove removes it, so
// no explicit remove is issued there. removeContainer is idempotent w.r.t. the
// benign races (not-found, removal-in-progress), so this is safe to call even
// when the daemon is already removing the container.
func removeBackstop(cli *client.Client, log func(format string, args ...any), id string) {
	if err := removeContainer(cli, id); err != nil {
		log("docker run: remove container %s: %v", id, err)
	}
}

// drainWait consumes the eventual ContainerWait delivery on either channel so
// the client's per-request goroutine (which writes to these channels) can exit
// instead of blocking forever. It is called only after the container has been
// killed or otherwise stopped; the bound guards against a broken daemon that
// never delivers, so runContainer cannot block indefinitely.
func drainWait(wait client.ContainerWaitResult, timeout time.Duration) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-wait.Result:
	case <-wait.Error:
	case <-t.C:
	}
}

// runContainer runs the image once for a single handler invocation, writing the
// event JSON to stdin. Container stdout/stderr are forwarded to Relay logs. The
// container exit status is the result: 0 is success, non-zero is failure. A
// cancelled context is reported as cancellation, not as a docker error. meta is
// the diagnostic metadata stamped as container labels (purely for triage; see
// RunMeta). env are the function's runtime environment variables (from the
// engine's plan), merged after the base RELAY_HANDLER var. extraEnv are the
// per-invocation variables (template env values + resolved secret values),
// merged after env; later entries win on duplicate names. The container is
// created with AutoRemove so the daemon removes it the moment it exits; Relay's
// explicit removal is only a backstop for the paths where the container never
// exits on its own (failed start, kill/timeout/cancel cleanup, wait error). The
// normal path — the container exits on its own — has no explicit remove and
// relies entirely on AutoRemove.
//
// Every execution container is hardened: it runs as a non-root user (set at
// build time via the plan's USER), drops all Linux capabilities, is memory/CPU/
// pids-limited, has a read-only rootfs, and gets a bounded /tmp tmpfs. These are
// internal defaults, not configuration. Networking is left enabled: outbound
// access is a legitimate function need, and network policy is a documented
// residual limitation rather than something this layer enforces.
func runContainer(
	ctx context.Context,
	cli *client.Client,
	log func(format string, args ...any),
	name, image string,
	env []string,
	extraEnv []string,
	handler string,
	eventJSON []byte,
	meta RunMeta,
) error {
	// Merge the base handler var, the function's plan env, and the per-invocation
	// extra env. The plan env is per-function (not per-run), so it is applied
	// uniformly to every invocation; the extra env carries this invocation's
	// template env values and resolved secrets. Later entries win on duplicates
	// (container env semantics), so a template env var may intentionally override
	// a runtime default.
	containerEnv := append([]string{"RELAY_HANDLER=" + handler}, env...)
	containerEnv = append(containerEnv, extraEnv...)
	createResp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:        image,
			Env:          containerEnv,
			OpenStdin:    true,
			StdinOnce:    true,
			AttachStdin:  true,
			AttachStdout: true,
			AttachStderr: true,
			Tty:          false,
			Labels:       runLabels(meta),
		},
		// AutoRemove removes the container as soon as it exits (attach and wait
		// still deliver output and exit code first; the daemon removes it once
		// the process has stopped). This is the normal cleanup path: Relay no
		// longer needs to remove normally-exited containers itself.
		//
		// Hardening: non-root user (set in the image), all capabilities dropped,
		// memory/CPU/pids limits, read-only rootfs, and a bounded /tmp tmpfs.
		// /tmp is the one writable path handlers get: Python's tempfile and
		// Node's temp-file helpers default to it, so a bounded tmpfs keeps
		// normal library behavior working without giving the container a
		// writable rootfs. The tmpfs is size-bounded and mounted nosuid/noexec
		// so it cannot be used to escalate or execute dropped binaries.
		HostConfig: &container.HostConfig{
			AutoRemove: true,
			Resources: container.Resources{
				Memory:    512 << 20,     // 512 MiB
				NanoCPUs:  1_000_000_000, // 1 CPU
				PidsLimit: ptr(int64(128)),
			},
			CapDrop:        []string{"ALL"},
			ReadonlyRootfs: true,
			Tmpfs:          map[string]string{"/tmp": "rw,nosuid,noexec,size=64m"},
		},
	})
	if err != nil {
		return fmt.Errorf("docker run: create container: %w", err)
	}
	id := createResp.ID
	// Normal exits rely on AutoRemove. The backstop is armed only while an
	// abnormal return could leave the container behind, and is disarmed once
	// wait.Result confirms the container has exited.
	armedRemove := false
	defer func() {
		if armedRemove {
			removeBackstop(cli, log, id)
		}
	}()

	// Attach before start so the hijacked stream is ready to receive stdin and
	// relay output the moment the container runs.
	attach, err := cli.ContainerAttach(ctx, id, client.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		// Created but never started: AutoRemove can never fire, so the backstop
		// must remove the container before this path returns.
		armedRemove = true
		return fmt.Errorf("docker run: attach: %w", err)
	}
	defer attach.Close()

	// From this point onward any abnormal exit (or panic unwind) requires
	// explicit cleanup: the container is started or about to start, and only a
	// confirmed exit via wait.Result hands cleanup back to AutoRemove.
	armedRemove = true

	// Demultiplex the non-TTY attach stream (stdout/stderr are multiplexed) into
	// separate buffers. Reading runs concurrently so a chatty container cannot
	// deadlock on a full socket while we write stdin.
	var stdout, stderr bytes.Buffer
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_, _ = stdcopy.StdCopy(&stdout, &stderr, attach.Reader)
	}()

	// Start the container first: the not-running wait below is only meaningful
	// once the container is actually running.
	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		// Start failed; the container never ran and no wait request was issued.
		// Close the hijacked conn so the reader goroutine can join; the armed
		// backstop removes the created-but-never-started container on return.
		attach.Close()
		<-readerDone
		return fmt.Errorf("docker run: start: %w", err)
	}

	// Open the wait request while the container is running. Every path below
	// that returns after this point must drain both channels so the client's
	// per-request goroutine can exit.
	wait := cli.ContainerWait(ctx, id, client.ContainerWaitOptions{
		Condition: container.WaitConditionNotRunning,
	})

	// Write the event JSON to the container's stdin, then half-close so the
	// process sees EOF and can exit. On an early failure, kill the container
	// (bounded context, detached from a possibly-cancelled ctx), close the
	// attach conn to unblock the reader, drain the wait channels, then join the
	// reader goroutine so it cannot leak. defer attach.Close() still runs as a
	// backstop on the normal path.
	if _, err := attach.Conn.Write(eventJSON); err != nil {
		killContainer(cli, id)
		attach.Close()
		drainWait(wait, 5*time.Second)
		<-readerDone
		return fmt.Errorf("write event to stdin: %w", err)
	}
	if err := attach.CloseWrite(); err != nil {
		killContainer(cli, id)
		attach.Close()
		drainWait(wait, 5*time.Second)
		<-readerDone
		return fmt.Errorf("close stdin: %w", err)
	}

	// Wait for the container to stop, but also react to context cancellation
	// (the per-invocation timeout) by force-killing the container.
	var exitCode int64
	var waitErr error
	select {
	case <-ctx.Done():
		// ctx is already cancelled, so use a detached, bounded context for the
		// kill, then close the attach conn and drain the wait channels so that
		// path's client goroutine unblocks and the reader goroutine can join.
		// The armed backstop removes the killed container on return.
		killContainer(cli, id)
		attach.Close()
		drainWait(wait, 3*time.Second)
		<-readerDone
		return fmt.Errorf("docker run: %w", ctx.Err())
	case err := <-wait.Error:
		waitErr = err
		// The wait request failed and the container state is uncertain. Kill it and
		// keep the backstop armed so cleanup remains best-effort and idempotent.
		killContainer(cli, id)
		attach.Close()
		drainWait(wait, 5*time.Second)
	case res := <-wait.Result:
		exitCode = res.StatusCode
		if res.Error != nil {
			waitErr = errors.New(res.Error.Message)
		}
		// The container exited on its own (success, non-zero exit, or a wait
		// error it reported): AutoRemove owns the cleanup from here, so the
		// backstop is disarmed and no explicit remove is issued.
		armedRemove = false
	}
	<-readerDone

	// Forward container output to Relay logs with a function/handler prefix.
	if stdout.Len() > 0 {
		log("function %q handler %q: %s", name, handler, strings.TrimRight(stdout.String(), "\n"))
	}
	if stderr.Len() > 0 {
		log("function %q handler %q: stderr: %s", name, handler, strings.TrimRight(stderr.String(), "\n"))
	}

	if waitErr != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("docker run: %w", ctx.Err())
		}
		return fmt.Errorf("docker run: %w", waitErr)
	}
	if exitCode != 0 {
		return fmt.Errorf("docker run: container exited with status %d", exitCode)
	}
	return nil
}

// copyDir copies src into dst, skipping any file whose slash-separated relative
// path is in skip AND any file named template.yaml anywhere in the tree (the
// exclusion is by base name so a nested template.yaml can never leak Relay
// configuration — including env values and secret references — into an image;
// the loader only ever reads the top-level one, so nested copies are dead
// weight at best). It is used to stage a function directory into a build
// context.
func copyDir(src, dst string, skip map[string]bool) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if skip[filepath.ToSlash(rel)] || filepath.Base(rel) == "template.yaml" {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
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
