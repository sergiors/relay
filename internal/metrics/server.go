// HTTP exposition of the registry.
//
// This file wires the Prometheus exporter to net/http. Server owns the
// http.ServeMux — routing only `GET /metrics` to the supplied exposition
// handler (Registry.Handler, which lives in metrics.go) — plus the dedicated
// http.Server lifecycle with synchronous fail-fast binding and bounded graceful
// shutdown. Unlike the old retry-bind behavior, a bind conflict (e.g. :9090
// already taken) surfaces immediately so the worker process fails startup
// instead of silently healing a port race.
//
// Routing: ONLY `GET /metrics` is registered; a request to any other path is a
// 404 (the old `/` alias that also served metrics is deliberately gone), and a
// request to /metrics with any method other than GET or HEAD is a 405 with an
// Allow header. In Go's ServeMux a "GET" method pattern matches HEAD too, so
// HEAD /metrics reaches the handler — promhttp writes a body on HEAD without
// suppression, which harmless scrapers ignore (documented in the NewServer
// comment below).
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Server owns the dedicated http.Server that exposes the registry on /metrics.
// It holds the listen address (which comes from application config, never a
// package-level default), the log sink for bind-failure reporting, and the
// http.Server/listener it has started.
//
// Lifecycle: Start binds synchronously and returns nil once serving, or the
// bind error immediately (fail-fast — a taken port is a config conflict, not a
// transient race to heal). Stop performs a bounded graceful shutdown using the
// caller-supplied context as the bound. Start may be called once; a second call
// returns an error even after a prior Stop.
type Server struct {
	handler http.Handler
	addr    string
	logger  *slog.Logger

	srv      *http.Server
	serveErr chan error

	started      atomic.Bool
	shutdownOnce sync.Once
}

// NewServer builds a Server exposing handler on addr. handler must be
// non-nil: it is registered directly with the mux, and http.ServeMux.Handle
// panics on a nil handler, so a nil argument fails fast at construction.
// logger receives bind-failure messages and may be nil to drop them.
//
// The Server builds its own http.ServeMux registering ONLY `GET /metrics`
// (method pattern). The old `/` alias that also served metrics is gone: root
// now returns 404. Other methods return 405 and other paths return 404 (both
// handled by the mux automatically). Note that Go's ServeMux accepts HEAD for
// a "GET" pattern, and promhttp writes a body on HEAD without suppression —
// HEAD /metrics therefore returns 200 with an exposition body, which harmless
// scrapers ignore.
func NewServer(addr string, handler http.Handler, logger *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)

	return &Server{addr: addr, handler: mux, logger: logger}
}

// Start binds s.addr synchronously and, on success, spawns the serving
// goroutine in the background, returning nil. It does NOT block once serving.
//
// Binding is synchronous so address conflicts surface immediately rather than
// being retried: the worker calls Start during startup and treats a non-nil
// return as fatal, so a taken metrics port fails startup fast instead of
// healing invisibly. If the bind fails, the error is logged via logger (when
// non-nil) and returned; Start may then be retried with a correct address.
//
// Start may be called once. A second call (whether or not a previous Stop has
// run) returns an error stating the server was already started. A nil receiver
// returns an error.
func (s *Server) Start() error {
	if s == nil {
		return errors.New("metrics: nil Server")
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("metrics server already started")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.started.Store(false)
		if s.logger != nil {
			s.logger.Error("Metrics: listen failed", "addr", s.addr, "error", err)
		}
		return fmt.Errorf("metrics: listen %s: %w", s.addr, err)
	}
	s.srv = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.serveErr = make(chan error, 1)
	go func() {
		s.serveErr <- s.srv.Serve(ln)
	}()

	// Per-connection errors are handled by net/http itself and never returned
	// here; Serve's return value is observed by Stop.
	return nil
}

// Stop performs a bounded graceful shutdown bounded by ctx: it calls
// srv.Shutdown(ctx) (which lets in-flight requests drain up to ctx's deadline),
// waits for the serving goroutine to exit, and returns the shutdown result —
// nil on a clean shutdown. It is idempotent: repeated calls are no-ops returning
// the first call's result, and calling Stop on a server that was never started
// (or on a nil receiver) returns nil.
func (s *Server) Stop(ctx context.Context) error {
	if s == nil || !s.started.Load() {
		return nil
	}
	var err error
	s.shutdownOnce.Do(func() {
		err = s.srv.Shutdown(ctx)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		// Shutdown causes Serve to return http.ErrServerClosed; wait for the
		// serving goroutine before returning so a caller can be sure no handler
		// is still running.
		<-s.serveErr
	})
	return err
}
