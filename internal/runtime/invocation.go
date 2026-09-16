package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// killContainer force-kills a container with a detached, bounded context so it
// works even when the invocation ctx is already cancelled.
func killContainer(cli *client.Client, id string) {
	killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = cli.ContainerKill(killCtx, id, client.ContainerKillOptions{})
}

// removeBackstop best-effort removes a container and logs a genuine failure. It
// is called only on the paths where the container may still be alive or never
// exited on its own (failed start, kill/timeout/cancel cleanup, wait error). On
// the normal path the container exits on its own and AutoRemove removes it, so
// no explicit remove is issued there. removeContainer is idempotent w.r.t. the
// benign races (not-found, removal-in-progress), so this is safe to call even
// when the daemon is already removing the container.
func removeBackstop(cli *client.Client, log *slog.Logger, id string) {
	if err := removeContainer(cli, id); err != nil {
		log.Warn(fmt.Sprintf("docker run: remove container %s: %v", id, err))
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
// event JSON to stdin. Container stdout/stderr are forwarded to the Relay
// process output (see output.go) line by line, live while the container runs —
// a raw transport, unaffected by Relay's LOG_LEVEL and not inferred into log
// severity levels. The container exit status is the result: 0 is success,
// non-zero is failure. A
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
	log *slog.Logger,
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
		// The rest of the hardening (non-root user set in the image, all
		// capabilities dropped, memory/CPU/pids limits, read-only rootfs, and a
		// bounded /tmp tmpfs) comes from the shared hardenedHostConfig. /tmp is
		// the one writable path handlers get: Python's tempfile and Node's
		// temp-file helpers default to it, so a bounded tmpfs keeps normal
		// library behavior working without giving the container a writable
		// rootfs. The tmpfs is size-bounded and mounted nosuid/noexec so it
		// cannot be used to escalate or execute dropped binaries.
		HostConfig: hardenedHostConfig(true),
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

	// Demultiplex the non-TTY attach stream (stdout/stderr are multiplexed)
	// into two line forwarders, which emit each completed line to the
	// function-output sink live while the container runs. Reading runs
	// concurrently so a chatty container cannot deadlock on a full socket
	// while we write stdin. The forwarders are written from this single reader
	// goroutine (no concurrent writes per forwarder); the sink itself is
	// mutex-guarded against other in-flight invocations.
	stdoutFwd := newStreamForwarder("stdout", name, handler, meta)
	stderrFwd := newStreamForwarder("stderr", name, handler, meta)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_, _ = stdcopy.StdCopy(stdoutFwd, stderrFwd, attach.Reader)
		// Flush any trailing partial lines so every byte the container wrote
		// is delivered before runContainer returns (the same guarantee the old
		// buffered dump gave, minus the buffering).
		stdoutFwd.flush()
		stderrFwd.flush()
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
