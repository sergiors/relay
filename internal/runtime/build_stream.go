package runtime

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"

	"relay/internal/observability/metrics"
)

// buildOutputRetention bounds the whole retained Docker build diagnostic: the
// Docker error message and the retained stream text together. A build can stream
// unbounded output and the daemon's error can itself be arbitrarily large, so
// both are capped; the remainder is drained and discarded. This is a diagnostic
// bound, not a behavior knob, so it has no user-facing configuration.
const buildOutputRetention = 1 << 20 // 1 MiB

// buildOutputTruncatedMarker marks a retained diagnostic whose excess was
// discarded, so a genuinely short failure is distinguishable from a truncated
// one.
const buildOutputTruncatedMarker = "[build output truncated]"

// runImageBuild is the shared ImageBuild tail: the Dockerfile is already written
// by the caller; this streams the context tar, builds, and drains the response.
// labels are the managed-image labels stamped onto the resulting image (see
// buildImageOptions); nil means no labels.
//
// The context tar is STREAMED through an io.Pipe rather than buffered in memory,
// so an arbitrarily large context never has to be materialized in the worker
// heap. The producer goroutine is joined on every path (success, daemon error,
// cancellation, and early consumer close), so a failed or cancelled build cannot
// leak it or block on a full pipe.
func runImageBuild(ctx context.Context, cli *client.Client, name, ctxDir, image string, labels map[string]string, reg *metrics.Registry) error {
	pr, pw := io.Pipe()
	producerDone := make(chan error, 1)
	go func() {
		err := writeContextTar(ctx, ctxDir, pw)
		// nil yields io.EOF for a clean end; otherwise the reader observes the
		// producer's error.
		_ = pw.CloseWithError(err)
		producerDone <- err
	}()

	resp, buildErr := cli.ImageBuild(ctx, pr, buildImageOptions(image, labels))
	if buildErr != nil {
		// The client produced no response (transport failure or cancellation).
		// It may have abandoned the context body, so unblock the producer before
		// joining it; Close is idempotent, so the join cannot hang.
		_ = pr.Close()
		producerErr := <-producerDone
		// The transport failure is the build failure. A producer error caused by
		// the abandoned stream is not a second fault to report.
		if producerErr != nil && !errors.Is(producerErr, io.ErrClosedPipe) && !errors.Is(producerErr, context.Canceled) {
			return fmt.Errorf("app %q: docker build: %w", name, errors.Join(buildErr, producerErr))
		}
		return fmt.Errorf("app %q: docker build: %w", name, buildErr)
	}

	// Drain the response BEFORE joining the producer. The transport writes the
	// request body on its own goroutine while this goroutine reads the response,
	// so the daemon can flush output and finish reading the context; waiting for
	// the producer first could deadlock against a server blocked writing a
	// response nobody is reading. Draining also consumes the full stream after a
	// build failure.
	out, truncated, drainErr := drainBuildResponse(resp.Body)
	_ = resp.Body.Close()
	// If the response ended without the transport consuming the whole context
	// (an early abort or a daemon that stopped reading), unblock the producer so
	// the join cannot hang. Closing an already-closed pipe is a no-op.
	_ = pr.Close()
	producerErr := <-producerDone

	// A producer error that is only the pipe closing under it, or the build
	// context being cancelled, is not a tar fault: the drained response is the
	// real outcome. Any OTHER producer error (a filesystem failure) means the tar
	// Relay sent was incomplete, which must never pass as a successful build.
	if producerErr != nil && !errors.Is(producerErr, io.ErrClosedPipe) && !errors.Is(producerErr, context.Canceled) && !errors.Is(producerErr, context.DeadlineExceeded) {
		return fmt.Errorf("app %q: tar build context: %w", name, producerErr)
	}
	if truncated {
		reg.Inc(metrics.MetricBuildOutputTruncated)
	}
	// The build API returns 200 even when the build fails; failure is signalled
	// by an "error" JSON message (drainErr) or by the build context being
	// cancelled. Output is a bounded diagnostic, discarded by the caller on
	// success.
	if drainErr != nil {
		return fmt.Errorf("app %q: docker build: %w\n%s", name, drainErr, strings.TrimSpace(out))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("app %q: docker build: %w", name, ctxErr)
	}
	return nil
}

// drainBuildResponse reads the JSON message stream returned by ImageBuild,
// retaining a BOUNDED diagnostic and failing on the first message that carries
// an error.
//
// The whole response is always consumed, even after the retention cap is reached
// and after the first Docker error, so the daemon's body is never abandoned
// mid-stream (which can stall the connection). The first error is remembered;
// draining continues. The retained diagnostic as a whole (the first Docker error
// message plus retained stream text) is bounded by buildOutputRetention: an
// oversized error message is itself capped (copied, never left backed by the
// original decoded string), and the stream budget is reduced by the error message
// it must accompany. Whatever was cut is marked with
// buildOutputTruncatedMarker, so an operator can tell a short failure from a
// truncated one. On a successful stream the (possibly bounded) output is returned
// with a nil error; the caller discards it.
//
// A malformed message ends parsing: JSON cannot be resynchronized, so the
// remaining messages are not interpreted. After the first error the malformed
// message is ignored (the first error already explains the failure); before any
// error the decode error is returned. Either way the decoder's buffered
// remainder and the underlying reader are drained to io.Discard first, so the
// response body is still fully consumed rather than abandoned.
//
// It also reports whether bytes were discarded (truncation), so the caller can
// record the bounded-diagnostic anomaly.
func drainBuildResponse(r io.Reader) (string, bool, error) {
	var out strings.Builder
	truncated := false
	var firstErr error
	var decodeErr error
	// errMsg is the bounded first Docker error message. It reserves part of the
	// retention budget so the combined diagnostic stays bounded.
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
// the failure to report, and draining is best-effort cleanup of a response that
// will be closed anyway.
func drainDecoderRemainder(dec *json.Decoder, r io.Reader) {
	_, _ = io.Copy(io.Discard, dec.Buffered())
	_, _ = io.Copy(io.Discard, r)
}

// finishBuildOutput renders the retained build text, appending the truncation
// marker when bytes were discarded and a separating newline when the retained
// text does not already end with one. errMsg is the retained first Docker error
// message, whose bytes reserve part of the retention budget: the returned stream
// text is trimmed to the remaining budget, so the combined diagnostic never
// exceeds buildOutputRetention before the fixed marker. It returns the rendered
// text and whether it truncated, which covers the case where the budget only
// shrank when a later error message was decoded. It never returns more than the
// stream budget bytes of build output before the marker.
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
