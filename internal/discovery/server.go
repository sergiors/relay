package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/state"
)

const (
	// defaultQueryTimeout bounds one registry read. It stays comfortably below
	// the documented 5s Traefik pollTimeout so JSON projection, serialization,
	// and response writing retain deadline margin. A slow database must fail the
	// request rather than hold a provider connection indefinitely.
	defaultQueryTimeout = 4 * time.Second
	// readHeaderTimeout bounds request headers. The handler has no request body.
	readHeaderTimeout = 5 * time.Second
	// readTimeout bounds the whole request read; writeTimeout bounds the
	// response. Both keep a stuck client from occupying the dedicated server.
	readTimeout  = 10 * time.Second
	writeTimeout = 10 * time.Second
	// idleTimeout bounds an idle keep-alive connection.
	idleTimeout = 30 * time.Second
)

// EndpointReader is the consumer-owned persistence seam. *state.State
// implements it; tests can substitute a lightweight fake. Discovery only reads:
// it never publishes, mutates, or drives convergence.
type EndpointReader interface {
	ReadEligibleServiceRegistryEndpoints(
		ctx context.Context,
		filter state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error)
}

// Server owns the dedicated discovery http.Server. Start binds synchronously and
// fails fast on a taken or unparseable address, matching the metrics and webhook
// servers. Stop performs a bounded graceful shutdown.
type Server struct {
	addr         string
	reader       EndpointReader
	logger       *slog.Logger
	queryTimeout time.Duration
	conflicts    *conflictTracker

	handler  http.Handler
	srv      *http.Server
	serveErr chan error

	started      atomic.Bool
	shutdownOnce sync.Once
	stopErr      error
}

// NewServer builds a discovery server on addr. An empty addr means the feature
// is disabled and returns nil, mirroring the optional webhook server. reader may
// be nil (the handler then reports 503); logger may be nil (bind and query
// diagnostics are dropped, but behavior is unchanged).
//
// Routing: ONLY /discovery/traefik is registered. The handler accepts GET and
// rejects every other method (including HEAD, which ServeMux would otherwise
// match to a GET pattern) with 405 and Allow: GET. Other paths receive 404 from
// net/http's ServeMux.
func NewServer(addr string, reader EndpointReader, logger *slog.Logger) *Server {
	if addr == "" {
		return nil
	}
	s := &Server{
		addr:         addr,
		reader:       reader,
		logger:       logger,
		queryTimeout: defaultQueryTimeout,
		conflicts:    newConflictTracker(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/discovery/traefik", s.handleTraefik)
	s.handler = mux
	return s
}

// Start binds synchronously and spawns the serving goroutine. A bind failure is
// returned immediately so the worker can fail startup fast. Start may be called
// once; a second call returns an error even after Stop.
func (s *Server) Start() error {
	if s == nil {
		return errors.New("discovery: nil Server")
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("discovery server already started")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.started.Store(false)
		if s.logger != nil {
			s.logger.Error("Discovery: listen failed", "addr", s.addr, "error", err)
		}
		return fmt.Errorf("discovery: listen %s: %w", s.addr, err)
	}
	s.srv = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	s.serveErr = make(chan error, 1)
	go func() {
		s.serveErr <- s.srv.Serve(ln)
	}()
	return nil
}

// Stop gracefully drains the server within ctx. It is idempotent and returns
// nil on a server that was never started (or a nil receiver).
func (s *Server) Stop(ctx context.Context) error {
	if s == nil || !s.started.Load() {
		return nil
	}
	s.shutdownOnce.Do(func() {
		err := s.srv.Shutdown(ctx)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		<-s.serveErr
		s.stopErr = err
	})
	return s.stopErr
}

func (s *Server) handleTraefik(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.reader == nil {
		writeUnavailable(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
	defer cancel()

	endpoints, err := s.reader.ReadEligibleServiceRegistryEndpoints(
		ctx, state.ServiceRegistryFilter{})
	if err != nil {
		// The response never includes the internal error: it may name the
		// database or connection details. The provider keeps its last successful
		// configuration on a 503.
		if s.logger != nil {
			s.logger.Error("Discovery: read service registry failed", "error", err)
		}
		writeUnavailable(w)
		return
	}

	config, conflicts := buildTraefikConfig(projectRoutableEndpoints(endpoints))
	// Observe only after a successful read: a failed query must not clear or
	// re-emit the active conflict set.
	s.conflicts.observe(conflicts, s.logger)

	payload, err := json.Marshal(config)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("Discovery: encode response failed", "error", err)
		}
		writeUnavailable(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("discovery temporarily unavailable\n"))
}
