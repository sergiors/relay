// HTTP exposition of the registry.
//
// This file wires the Prometheus exporter to net/http: a Handler that serves
// the registry in the Prometheus text exposition format on GET /metrics and
// GET /, plus a small server helper that runs a dedicated http.Server with
// graceful shutdown and retry-bind so a temporarily occupied port heals instead
// of crashing the process.
package metrics

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultAddr is the default listen address for the metrics server. 9090 is the
// conventional port in the Prometheus ecosystem; it avoids colliding with Redis
// (6379) and any future admin port.
const DefaultAddr = ":9090"

// prometheusContentType is the Content-Type served by promhttp for the
// Prometheus text exposition format (version 0.0.4). The header also carries
// `; charset=utf-8`, but tests assert on this stable prefix.
const prometheusContentType = "text/plain; version=0.0.4"

// Handler returns an http.Handler that serves the registry's Prometheus
// exposition on GET/HEAD /metrics (and /, which is harmless). Other paths return
// 404; other methods return 405. The exposition itself is rendered by promhttp.
// A nil receiver returns a valid handler that serves an empty 200 body, so a
// scrape of a metrics-disabled process never errors.
func (r *Registry) Handler() http.Handler {
	if r == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	}
	expose := promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{}).ServeHTTP
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" && req.URL.Path != "/metrics" {
			http.NotFound(w, req)
			return
		}
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// promhttp does not suppress the body on HEAD, so wrap the writer to
		// discard it while preserving headers and status.
		if req.Method == http.MethodHead {
			w = &headWriter{ResponseWriter: w}
		}
		expose(w, req)
	})
}

// headWriter discards the response body while letting headers and status
// through, so a HEAD scrape has headers but no body.
type headWriter struct {
	http.ResponseWriter
}

func (h *headWriter) Write(b []byte) (int, error) { return len(b), nil }

// Serve runs a dedicated http.Server on addr serving only the registry's
// metrics handler. It blocks until ctx is cancelled, then shuts the server down
// gracefully with a bounded timeout and returns the server's error after
// shutdown. A nil receiver still serves an empty 200 body.
//
// If the initial bind fails (for example, the port is temporarily occupied), the
// error is logged via logf and the bind is retried every 10s until ctx is done,
// so a port race at container start heals rather than crashing the process.
// Per-connection errors are handled by net/http itself and never crash the
// server.
func (r *Registry) ServeHTTP(ctx context.Context, addr string, logf func(string, ...any)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// Retry-bind: log and keep trying until ctx is done.
		if logf != nil {
			logf("metrics: listen %s failed: %v; retrying every 10s", addr, err)
		}
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				ln, err = net.Listen("tcp", addr)
				if err == nil {
					break
				}
				if logf != nil {
					logf("metrics: listen %s failed: %v; retrying", addr, err)
				}
			}
			if ln != nil {
				break
			}
		}
	}
	return r.Serve(ctx, ln, logf)
}

// Serve runs a dedicated http.Server on the given listener serving only the
// registry's metrics handler. It blocks until ctx is cancelled, then shuts the
// server down gracefully with a bounded timeout and returns the server's error
// after shutdown. A nil receiver still serves an empty 200 body.
func (r *Registry) Serve(ctx context.Context, ln net.Listener, logf func(string, ...any)) error {
	srv := &http.Server{
		Handler:           r.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		// Wait for Serve to return; it returns http.ErrServerClosed on shutdown.
		<-errCh
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}
