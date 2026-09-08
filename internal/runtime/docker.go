// Package runtime executes Relay functions in containers: one image per
// function, one container per handler invocation, talking to the Docker Engine
// API directly via the moby client. Runtime engines (python, node) produce
// generic build plans; this package turns those plans into images and
// invocations without knowing about Python imports or Node module resolution.
package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runtime/plan"
)

// imageRef maps a function name to a docker tag: lowercase, disallowed
// characters replaced by '-', prefixed "relay-fn-" to avoid reserved names, and
// suffixed with a short deterministic hash so that different function names
// that sanitize to the same image name still produce distinct refs.
func imageRef(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return "relay-fn-" + b.String() + "-" + fnv1a8(name)
}

// fnv1a8 is the deterministic 8-hex hash suffix used by imageRef.
func fnv1a8(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("%08x", h.Sum32())
}

func buildImage(ctx context.Context, cli *client.Client, name string, fn function.Function, p plan.BuildPlan, image string) error {
	ctxDir, err := os.MkdirTemp("", "relay-build-*")
	if err != nil {
		return fmt.Errorf("function %q: create build context: %w", name, err)
	}
	defer os.RemoveAll(ctxDir)

	// Copy the function directory into the context. Generated plan files are
	// written separately, so the user's function directory is never modified.
	if err := copyDir(fn.Dir, ctxDir); err != nil {
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

	resp, err := cli.ImageBuild(ctx, contextTar, client.ImageBuildOptions{
		Tags:       []string{image},
		Dockerfile: "Dockerfile",
	})
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

// tarContext walks a staged build-context directory and returns it as a tar
// stream suitable for ImageBuild.
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
			return tw.WriteHeader(&tar.Header{Name: name + "/", Mode: int64(info.Mode().Perm()), Typeflag: tar.TypeDir})
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: int64(info.Mode().Perm()), Size: info.Size()}); err != nil {
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
// cancelled context is reported as cancellation, not as a docker error.
func runContainer(ctx context.Context, cli *client.Client, log func(format string, args ...any), name, image, handler string, eventJSON []byte) error {
	createResp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:        image,
			Env:          []string{"RELAY_HANDLER=" + handler},
			OpenStdin:    true,
			StdinOnce:    true,
			AttachStdin:  true,
			AttachStdout: true,
			AttachStderr: true,
			Tty:          false,
		},
	})
	if err != nil {
		return fmt.Errorf("docker run: create container: %w", err)
	}
	id := createResp.ID
	// Best-effort cleanup on every path; a leaked container is worse than a
	// failed remove.
	defer func() {
		rmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = cli.ContainerRemove(rmCtx, id, client.ContainerRemoveOptions{Force: true})
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
		return fmt.Errorf("docker run: attach: %w", err)
	}
	defer attach.Close()

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
		// Close the hijacked conn so the reader goroutine can join.
		attach.Close()
		<-readerDone
		return fmt.Errorf("docker run: start: %w", err)
	}

	// Open the wait request while the container is running. Every path below
	// that returns after this point must drain both channels so the client's
	// per-request goroutine can exit.
	wait := cli.ContainerWait(ctx, id, client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})

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
		killContainer(cli, id)
		attach.Close()
		drainWait(wait, 3*time.Second)
		<-readerDone
		return fmt.Errorf("docker run: %w", ctx.Err())
	case err := <-wait.Error:
		waitErr = err
		// The wait request failed; the container may still be running. Kill it,
		// close the attach stream so the reader goroutine can exit, and drain
		// the other wait channel so no client goroutine is left blocked.
		killContainer(cli, id)
		attach.Close()
		drainWait(wait, 5*time.Second)
	case res := <-wait.Result:
		exitCode = res.StatusCode
		if res.Error != nil {
			waitErr = errors.New(res.Error.Message)
		}
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

func copyDir(src, dst string) error {
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
