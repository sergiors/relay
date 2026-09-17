// Package webhook owns provider-specific webhook delivery endpoints (currently
// GitHub at POST /github) that trigger the existing manual git sync workflow
// automatically. It is a deliberately separate package from git, so the
// guardrail that pins "the git package keeps no background timers/loggers"
// stays intact: the webhook server and its coalescing sync scheduler live here,
// isolated from the git transport core.
//
// The webhook feature does NOT reimplement synchronization: a valid, matching
// push merely schedules a sync through the coalescing scheduler (see github.go's
// SyncScheduler). Scheduling never runs git transport logic inside the HTTP
// handler; it hands control to the scheduler's background goroutine, which calls
// into internal/git's existing Sync entry, preserving the one-shot, no-timer
// contract of the git package.
//
// Architecture (why a separate HTTP server): the metrics server owns the
// Prometheus HTTP endpoint and must remain the only thing bound to METRICS_ADDR.
// The webhook is triggered by external provider deliveries and has different
// lifecycle/security concerns (HMAC verification, a dedicated port), so it gets
// its own http.Server and address (GIT_WEBHOOK_ADDR), mirroring the metrics
// server's fail-fast synchronous bind and bounded graceful shutdown.
//
// Provider seam: server.go is provider-agnostic — it owns lifecycle, route
// registration, dispatch to per-provider handlers, and shared HTTP plumbing —
// while provider semantics (authentication headers, signature schemes, event
// names, payload shapes, repository/ref matching) live in per-provider files
// (github.go today, gitlab.go later). Adding a provider is implementing the
// Provider interface in a new file and registering it inside NewServer; Server's
// routing/lifecycle itself never changes. NewServer owns the whole assembly —
// secret checks, git source config, the coalescing scheduler, and every
// provider handler — and the worker orchestrates the resulting Server behind one
// Start/Stop pair (mirroring metrics.Server), so it never knows how the server is
// built.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/function"
	"relay/internal/git"
	"relay/internal/secrets"
)

// maxBodyBytes caps a webhook delivery body at 10 MiB. Provider payloads are
// small, but a body cap bounds memory use and rejects (as a 400) anything
// oversized. http.MaxBytesReader enforces it while the request is being read.
// It is the shared delivery-body cap every provider applies.
const maxBodyBytes = 10 << 20

// Provider is the seam between the provider-agnostic Server and one
// provider-specific webhook implementation. Implementations own everything
// provider-specific — authentication headers, signature schemes, event
// names, payload shapes, and repository/ref matching — and the server owns
// only HTTP plumbing. Embedding http.Handler keeps the boundary at exactly
// what net/http needs for dispatch: ServeHTTP.
type Provider interface {
	http.Handler
	// Name is the provider's URL path segment: a provider named "github"
	// is registered at POST /github. It must be a single non-empty path
	// segment with no "/" — newServer and NewServer panic otherwise, mirroring
	// ServeMux's own panic-on-programmer-error contract.
	Name() string
}

// Server owns the dedicated webhook http.Server AND the coalescing sync
// scheduler its providers trigger, behind exactly one Start/Stop pair (mirroring
// metrics.Server). It dispatches to one or more provider handlers (currently
// POST /github). Separate from the Prometheus metrics server per the
// architecture requirement.
type Server struct {
	addr    string
	handler http.Handler
	logger  *slog.Logger

	// scheduler is the coalescing sync trigger assembled by NewServer (nil when
	// Server is built by newServer for pure routing/lifecycle tests). Stop drains
	// it after the HTTP server. It is nil-safe: SyncScheduler.Stop tolerates a
	// nil receiver and Stop also guards s.scheduler != nil.
	scheduler    *SyncScheduler
	srv          *http.Server
	serveErr     chan error
	started      atomic.Bool
	shutdownOnce sync.Once
}

// NewServer assembles the whole webhook subsystem in a single constructor
// (mirroring metrics.NewServer's one-constructor shape): it loads the git source
// config, decides enablement, builds the coalescing sync scheduler, constructs
// the provider handlers (GitHub today; a future GitLab provider is registered
// here too), and wires the HTTP server — all inside this package, so the worker
// only orchestrates (construct/start/stop) like it does metrics.
//
// It returns nil (disabled) — the documented disabled contract — when there is
// no git source configured, the config is unreadable, or a webhook secret
// reference is configured but no secret resolver is — logging a Warn for each
// disable reason so the operator knows why nothing is bound. An empty webhook
// secret reference (the common case) does NOT disable the server: it is
// enabled-by-default with signature verification disabled, so unsigned GitHub
// deliveries (a webhook configured without a secret sends no signature header)
// are accepted. A nil *Server means the subsystem is disabled; callers must
// nil-check before Start, mirroring how the worker nil-checks. addr is the
// listen address ("" never reaches the worker path: the worker gates on
// cfg.GitWebhookAddr before calling).
//
// logger receives disable-Warns, bind-failure, and lifecycle messages; it is
// injected (DI) — this package never constructs its own logger. Adding a
// provider means implementing Provider in a new file and registering its
// constructed instance inside NewServer's assembly; the worker orchestration
// never changes.
func NewServer(addr string, logger *slog.Logger, cfg Config) *Server {
	configPath := defaultStr(cfg.ConfigPath, git.ConfigPath)

	gitCfg, err := git.LoadConfig(configPath)
	if err != nil {
		if errors.Is(err, git.ErrConfigNotFound()) {
			// No git source configured: there is nothing to sync, so do not bind
			// the endpoint. An operator running `relay git set` later must
			// restart the worker to enable webhook delivery again.
			logger.Warn("Git webhook: no git source configured; webhook disabled")
			return nil
		}
		logger.Warn(fmt.Sprintf("Git webhook: read git config: %v (continuing without webhook)", err))
		return nil
	}
	if gitCfg.WebhookSecretRef != "" && cfg.Secrets == nil {
		// A configured webhook secret reference cannot be resolved without a
		// resolver, so the handler would 500 every delivery. Disable the server
		// (defensive) and say so. An EMPTY secret reference needs no resolver
		// (unsigned deliveries are accepted), so it does not reach this branch.
		logger.Warn("Git webhook: no secret resolver configured; webhook disabled")
		return nil
	}

	// Build the scheduler's sync options, defaulting each directory to the
	// production convention unless the caller overrode it.
	opts := git.NewSyncOptions()
	opts.ConfigPath = configPath
	opts.CheckoutDir = defaultStr(cfg.CheckoutDir, git.CheckoutDir)
	opts.FunctionsDir = defaultStr(cfg.FunctionsDir, function.Dir)
	opts.SSHDir = defaultStr(cfg.SSHDir, git.SSHDir)
	opts.Log = logger
	scheduler := NewSyncScheduler(opts, logger)

	provider := NewGitHubProvider(
		logger,
		gitCfg.WebhookSecretRef,
		cfg.Secrets,
		configPath,
		opts.CheckoutDir,
		opts.FunctionsDir,
		opts.SSHDir,
		scheduler,
	)
	srv := newServer(addr, logger, provider)
	srv.scheduler = scheduler
	return srv
}

// newServer builds a Server registering each provider at POST /<provider.Name()>
// without any assembly (no git config, no scheduler — the scheduler stays nil).
// It is the routing/mux half that NewServer delegates to, and the seam tests use
// to exercise dispatch and lifecycle with stub providers.
//
// Each provider is dispatched through the Provider seam: the server owns only
// HTTP plumbing and lifecycle, while every provider-specific behavior lives in
// the provider's file. A nil provider is skipped rather than registered
// (defensive — NewServer never passes nil because construction is gated). A
// provider whose Name() is empty or contains "/" panics with a clear message,
// mirroring ServeMux's own panic-on-programmer-error contract for a malformed
// pattern. Zero providers is valid: the mux then has no routes, so every request
// 404s.
//
// logger receives bind-failure and start/stop lifecycle messages and may be nil
// to drop them. It is injected (DI): this package never constructs its own
// logger. The webhook secret is never logged here.
func newServer(addr string, logger *slog.Logger, providers ...Provider) *Server {
	mux := http.NewServeMux()
	for _, p := range providers {
		if p == nil {
			// Nil provider: skip registration rather than register a route that
			// would 500 or panic. Defensive only; NewServer never passes nil
			// because construction is gated on a configured address.
			continue
		}
		name := p.Name()
		if name == "" || strings.Contains(name, "/") {
			panic(fmt.Sprintf("webhook: invalid provider name %q (must be a single non-empty path segment without /)", name))
		}
		mux.Handle("POST /"+name, p)
	}
	return &Server{addr: addr, handler: mux, logger: logger}
}

// readBody reads the request body through a hard size cap (maxBodyBytes) before
// any provider-specific validation, so no provider parses an unbounded or
// oversized body. It is the shared first step for every provider. On error it
// logs a warning via the nil-safe shared logger helper and writes 400 "bad
// request", returning (nil, false) so the caller returns early.
func readBody(logger *slog.Logger, w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		logf(logger, r.Context(), slog.LevelWarn, "webhook: read request body: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

// logf routes a structured log line through the injected logger at the given
// level using the request context. Nil logger is tolerated (logs dropped). It
// is shared provider plumbing: providers pass their injected logger. The
// webhook secret value and signature header are never passed as attrs.
func logf(logger *slog.Logger, ctx context.Context, lvl slog.Level, msg string, args ...any) {
	if logger == nil {
		return
	}
	switch lvl {
	case slog.LevelDebug:
		logger.DebugContext(ctx, msg, args...)
	case slog.LevelInfo:
		logger.InfoContext(ctx, msg, args...)
	case slog.LevelWarn:
		logger.WarnContext(ctx, msg, args...)
	default:
		logger.ErrorContext(ctx, msg, args...)
	}
}

// Start binds s.addr synchronously and, on success, spawns the serving
// goroutine in the background, returning nil. It does NOT block once serving.
//
// Binding is synchronous so address conflicts surface immediately rather than
// being retried: the worker calls Start during startup and treats a non-nil
// return as fatal, so a taken webhook port fails startup fast (matching the
// metrics server). If the bind fails, the error is logged via logger (when
// non-nil) and returned; Start may then be retried with a correct address.
//
// Start may be called once. A second call (whether or not a previous Stop has
// run) returns an error stating the server was already started. A nil receiver
// returns an error.
func (s *Server) Start() error {
	if s == nil {
		return errors.New("webhook: nil Server")
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("webhook server already started")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.started.Store(false)
		if s.logger != nil {
			s.logger.Error(fmt.Sprintf("Webhook: listen %s failed: %v", s.addr, err))
		}
		return fmt.Errorf("webhook: listen %s: %w", s.addr, err)
	}
	s.srv = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
		// The handler completes quickly — it only schedules a background sync
		// (it never runs the sync itself) — so generous read/write timeouts are
		// safe and bound slow or stalled provider deliveries.
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	s.serveErr = make(chan error, 1)
	// Per-connection errors are handled by net/http itself and never returned
	// here; Serve's return value is observed by Stop.
	go func() {
		s.serveErr <- s.srv.Serve(ln)
	}()
	if s.logger != nil {
		s.logger.Info("Webhook server started")
	}
	return nil
}

// Stop performs a bounded graceful shutdown bounded by ctx, in the order that
// matters: the HTTP server FIRST (so no new deliveries can arrive, using
// srv.Shutdown(ctx) to let in-flight requests drain up to ctx's deadline), then
// the coalescing sync scheduler (which drains the in-flight sync, also bounded
// by ctx). The scheduler stop runs inside the same shutdownOnce closure after
// the serving goroutine exits, so it runs exactly once.
//
// If the server shutdown itself errored, that (first) error is returned — first
// error wins — while a scheduler stop error is still logged at Warn. It is
// idempotent: repeated calls are no-ops returning the first call's result, and
// calling Stop on a server that was never started (or on a nil receiver)
// returns nil. A successful Stop logs via logger (when non-nil).
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

		// The HTTP server is fully drained; now stop the coalescing sync
		// scheduler the providers trigger — it cancels its internal context so
		// an in-flight sync aborts at its next transport step, bounded by ctx.
		if s.scheduler != nil {
			if serr := s.scheduler.Stop(ctx); serr != nil && s.logger != nil {
				s.logger.Warn(fmt.Sprintf("Git webhook scheduler: graceful shutdown: %v", serr))
			}
		}
	})
	if err == nil && s.logger != nil {
		s.logger.Info("Webhook server stopped")
	}
	return err
}

// Config carries the shared dependencies and high-level configuration the
// webhook subsystem needs. Directories default to the production application
// conventions (git.ConfigPath, git.CheckoutDir, function.Dir, git.SSHDir) when
// zero-valued so callers pass only what differs (tests).
type Config struct {
	// Secrets resolves the webhook secret reference(s). It is required only
	// when a webhook secret reference is configured in the git source (so HMAC
	// verification can resolve it); with an empty secret reference deliveries
	// are accepted unsigned and no resolver is needed. It is a
	// secrets.Provider, which never exposes a secret's value in an error.
	Secrets secrets.Provider
	// Optional directory overrides (zero value = production default):
	// git.ConfigPath, git.CheckoutDir, function.Dir, git.SSHDir.
	ConfigPath, CheckoutDir, FunctionsDir, SSHDir string
}

// defaultStr returns v when non-empty, else def. It is the tiny helper the
// assembly uses to fall back each directory override to its production default.
func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
