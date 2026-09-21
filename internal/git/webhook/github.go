package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/go-git/go-git/v5/plumbing/transport"

	"relay/internal/git"
	"relay/internal/secrets"
)

// signatureHexRe matches the hex digest portion of a GitHub X-Hub-Signature-256
// header: a 256-bit SHA-256 digest formatted as 64 lowercase or uppercase hex
// digits. It is enforced BEFORE any HMAC work so a malformed header is rejected
// fast without any signature comparison.
var signatureHexRe = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// pushPayload is the subset of a GitHub `push` delivery the handler inspects:
// the pushed ref, the resulting commit hash, whether the push deleted the ref,
// and the repository metadata used to match against the configured source. The
// repository provides multiple URL identities (ssh/clone/git/html) all pointing
// at the same host+path; matching accepts any of them so the configured SSH URL
// lines up with how GitHub reports the repository.
type pushPayload struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Deleted    bool   `json:"deleted"`
	Repository struct {
		FullName string `json:"full_name"`
		Name     string `json:"name"`
		Owner    struct {
			Login string `json:"login"`
		} `json:"owner"`
		HTMLURL  string `json:"html_url"`
		CloneURL string `json:"clone_url"`
		SSHURL   string `json:"ssh_url"`
		GitURL   string `json:"git_url"`
	} `json:"repository"`
}

// GitHubProvider validates GitHub webhook deliveries (when the git source is
// configured with a webhook secret reference) and triggers the existing git
// sync for matching pushes. It implements Provider, so all provider-specific
// behavior for GitHub lives here: it owns no HTTP server and no git transport —
// routing is handled by the provider-agnostic Server, and actually running a
// sync is delegated to the injected SyncTrigger (the coalescing scheduler). It
// is the only component that reads the webhook secret (via secrets.Provider) —
// never logs it — and it resolves the configured source per request (via
// git.LoadConfig) so a `relay git set` takes effect without a worker restart.
//
// An empty secretRef (no webhook secret configured for the source) disables
// signature verification: deliveries are accepted unauthenticated, matching a
// GitHub webhook configured without a secret (which sends no signature header).
// A non-empty secretRef requires a secrets.Provider to resolve it and HMAC
// verification of every delivery.
//
// The name says "Provider", not "Handler", because this type is a component
// behind the Provider seam (server.go) rather than a bare HTTP handler: it is
// assembled inside the webhook subsystem, handed to a Server, and dispatched as
// one Provider among possibly several. Its ServeHTTP remains and is intentional
// — it IS the Provider dispatch entry the mux invokes (Provider embeds
// http.Handler). The rename is purely one of nomenclature; there is no behavior
// or interface change.
type GitHubProvider struct {
	logger    *slog.Logger
	secretRef string           // webhook secret name in Relay's store; empty = signature verification disabled
	secrets   secrets.Provider // resolves secretRef; may be nil only in tests
	sync      SyncTrigger      // the coalescing sync scheduler

	configPath string // persisted git source config, read per request
}

// SyncTrigger schedules a coalescing git sync. Done returns a channel closed
// when a scheduled sync attempt finishes (success or failure), so tests can
// await convergence; the production scheduler never blocks the handler.
type SyncTrigger interface {
	Trigger() error
	Done() <-chan struct{}
}

// NewGitHubProvider builds a GitHubProvider. secretRef is the name of the
// webhook secret in Relay's store — empty
// means signature verification is disabled and deliveries are accepted
// unauthenticated (a GitHub webhook configured without a secret sends no
// signature header); secrets resolves secretRef to the value (when non-empty,
// a nil provider = "not configured" → every request 500s, which NewServer
// prevents). configPath is the persisted git source config used to match
// repository/ref per request; syncer is the coalescing trigger (or a test fake).
func NewGitHubProvider(
	logger *slog.Logger,
	secretRef string,
	secrets secrets.Provider,
	configPath string,
	syncer SyncTrigger,
) *GitHubProvider {
	return &GitHubProvider{
		logger:     logger,
		secretRef:  secretRef,
		secrets:    secrets,
		sync:       syncer,
		configPath: configPath,
	}
}

// ServeHTTP authenticates (when a webhook secret is configured) and filters a
// webhook delivery, then — for a matching push — schedules a sync and returns
// 202 Accepted. It implements the http.Handler half of the Provider seam: the
// server dispatches GitHub deliveries here. The order is deliberate:
// authentication runs against the raw body BEFORE the payload is parsed or any
// source lookup happens. A malformed or unsigned delivery (when a secret IS
// configured) is rejected before any non-trivial processing, so the handler
// never does JSON parsing, config reads, or sync scheduling unless the delivery
// is authentic and relevant.
//
// When no webhook secret is configured (empty secretRef), signature
// verification is skipped entirely: deliveries are accepted unauthenticated,
// matching a webhook configured without a secret. Only the event filter then
// gates whether a payload is acted on.
//
// The webhook secret value is NEVER logged: only its reference name and the
// resolution error's name (which the Provider guarantees). The signature header
// value is likewise never logged.
func (h *GitHubProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Read the body with a hard size cap, before anything else.
	body, ok := readBody(h.logger, w, r)
	if !ok {
		return
	}
	// 2/3. Authenticate the delivery ONLY when a webhook secret is configured.
	// An empty secretRef means no secret is configured for the source: skip
	// signature verification entirely (a GitHub webhook set up without a secret
	// sends no signature header, so unsigned deliveries are valid and must be
	// accepted). Log this at Debug — per-request — never as a per-request Warn.
	if h.secretRef == "" {
		logAt(h.logger, r.Context(), slog.LevelDebug,
			"webhook: no webhook secret configured; accepting delivery without signature verification")
	} else if h.secrets == nil {
		// A non-empty secretRef with no resolver cannot be verified. Defensive
		// only — NewServer prevents this combination — but keep the 500 so a
		// misassembled provider never silently accepts an unauthenticated push.
		logAt(h.logger, r.Context(), slog.LevelError, "webhook secret not configured")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	} else {
		secret, err := h.secrets.Resolve(r.Context(), h.secretRef)
		if err != nil {
			// The error never carries the value (Provider guarantee); it may carry
			// the name, which is safe to log.
			logAt(h.logger, r.Context(), slog.LevelError, "webhook: resolve secret",
				"secret_ref", h.secretRef, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		// Verify X-Hub-Signature-256 (constant-time HMAC), never logging the
		// header value.
		if !verifySignature(secret, r.Header.Get("X-Hub-Signature-256"), body) {
			logAt(h.logger, r.Context(), slog.LevelWarn, "webhook signature verification failed")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	// 4. Event filter: only `push` deliveries can trigger a sync. Unsupported
	// events (ping, etc) and a missing event header are acknowledged (200) and
	// ignored. Event names are not secret, so they are logged at Debug.
	ev := r.Header.Get("X-GitHub-Event")
	if ev != "push" {
		logAt(h.logger, r.Context(), slog.LevelDebug, "webhook: ignoring event", "event", ev)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ignored")
		return
	}
	// 5. Parse the payload.
	var payload pushPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		logAt(h.logger, r.Context(), slog.LevelWarn, "webhook: malformed push payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// A deleted push removes a ref; nothing to sync (there is no new tree).
	if payload.Deleted {
		logAt(h.logger, r.Context(), slog.LevelDebug, "webhook: push deleted; nothing to sync", "repo", h.repoName(&payload))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ignored")
		return
	}
	// 6. Resolve the configured source per request (so `relay git set` takes
	// effect without a restart) and match the repository identity.
	cfg, cfgErr := git.LoadConfig(h.configPath)
	if cfgErr != nil || !matchRepository(cfg.Repository, &payload) {
		logAt(h.logger, r.Context(), slog.LevelWarn, "webhook: repository mismatch", "repo", h.repoName(&payload))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ignored")
		return
	}
	// 7. Match the pushed ref against the configured ref.
	if !git.MatchPushedRef(payload.Ref, cfg.Ref) {
		logAt(h.logger, r.Context(), slog.LevelDebug, "webhook: pushed ref does not match configured ref",
			"repo", h.repoName(&payload), "ref", payload.Ref, "configured_ref", cfg.Ref)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ignored")
		return
	}
	// 8. Schedule the coalescing sync. This never blocks the handler; the
	// scheduler goroutine owns the actual git sync. A scheduler failure (only a
	// nil receiver) is an internal error.
	if h.sync == nil {
		logAt(h.logger, r.Context(), slog.LevelError, "webhook: sync trigger not configured")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := h.sync.Trigger(); err != nil {
		logAt(h.logger, r.Context(), slog.LevelError, "webhook: schedule sync", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	logAt(h.logger, r.Context(), slog.LevelInfo, "GitHub push accepted",
		"repo", h.repoName(&payload), "ref", payload.Ref)
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, "accepted")
}

// Name implements Provider: GitHub deliveries are served at POST /github.
func (h *GitHubProvider) Name() string {
	return "github"
}

// repoName renders a safe human-readable label for the payload's repository,
// preferring full_name (owner/repo), then the bare name, then "<unknown>". It is
// used in logs for an unrecognized/non-matching delivery.
func (h *GitHubProvider) repoName(p *pushPayload) string {
	if p == nil {
		return "<unknown>"
	}
	if p.Repository.FullName != "" {
		return p.Repository.FullName
	}
	if p.Repository.Name != "" {
		return p.Repository.Name
	}
	return "<unknown>"
}

// verifySignature reports whether sigHeader (the raw X-Hub-Signature-256 value)
// is a valid, constant-time HMAC-SHA256 of body under secret. It REQUIRES the
// header to be present and well-formed ("sha256=" + 64 hex); a malformed or
// missing header returns false without attempting a comparison. The comparison
// itself uses crypto/hmac.Equal so timing does not reveal how close a forged
// signature is to the real one.
func verifySignature(secret, sigHeader string, body []byte) bool {
	if !strings.HasPrefix(sigHeader, "sha256=") {
		return false
	}
	hexPart := strings.TrimPrefix(sigHeader, "sha256=")
	if !signatureHexRe.MatchString(hexPart) {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sigHeader), []byte(expected))
}

// endpointIdentity decomposes a repository URL into its (host, normalized path)
// identity for repository matching. It uses go-git's transport.NewEndpoint (the
// same parser the git package uses), which handles both scp-like SSH URLs
// ("git@host:org/repo.git") and https URLs, so the configured SSH URL and
// GitHub's reported clone/html URLs all normalize to the same host+path. ok is
// false when the URL does not parse. The path is normalized (case-folded first,
// then leading slashes and any ".git" suffix removed, so the suffix strip is
// case-insensitive) such that "acme/backend.git", "acme/backend.GIT", and
// "acme/backend" all compare equal.
func endpointIdentity(raw string) (host, path string, ok bool) {
	ep, err := transport.NewEndpoint(raw)
	if err != nil {
		return "", "", false
	}
	if ep.Host == "" {
		return "", "", false
	}
	p := strings.ToLower(strings.Trim(ep.Path, "/"))
	p = strings.TrimSuffix(p, ".git")
	return strings.ToLower(ep.Host), p, true
}

// matchRepository reports whether the payload's repository matches the
// configured repository URL. A configured SSH URL and GitHub's reported
// ssh_url/clone_url/git_url/html_url all describe the same host+path, so any of
// them matching the configured identity is a match. A delivery with NO usable
// repository URL is a non-match (payload with no repo info is treated as
// non-matching), matching the "200 ignored" behavior.
func matchRepository(configured string, payload *pushPayload) bool {
	cfgHost, cfgPath, ok := endpointIdentity(configured)
	if !ok {
		return false
	}
	if payload == nil {
		return false
	}
	for _, u := range []string{
		payload.Repository.SSHURL,
		payload.Repository.CloneURL,
		payload.Repository.GitURL,
		payload.Repository.HTMLURL,
	} {
		h, p, uok := endpointIdentity(u)
		if uok && h == cfgHost && p == cfgPath {
			return true
		}
	}
	return false
}

// SyncScheduler coalesces webhook pushes into at-most-one concurrent git sync
// with a bounded pending queue of exactly one (the "next" slot): the running
// sync is exclusive; a push arriving during a run sets pending=true so exactly
// one more sync runs afterward with the latest remote state (final state
// converges to the latest commit; no unbounded queue; bounded goroutines). No
// wake channel is needed: since the run loop re-checks pending under the mutex
// after each sync and continues on its own when it is set, a push arriving
// mid-run is picked up automatically without any external signal.
//
// It implements SyncTrigger. The HTTP handler is NEVER blocked: Trigger only
// takes a mutex and spawns (or coalesces into) the scheduler goroutine, which
// owns the actual git.Sync call. The worker constructs it (exported, so the
// worker can stop it) and hands it to the handler.
type SyncScheduler struct {
	opts   git.SyncOptions // built once from the dirs the handler/worker used
	logger *slog.Logger
	syncFn func(ctx context.Context, opts git.SyncOptions) error // seam, defaults to git.Sync
	ctx    context.Context
	cancel context.CancelFunc

	stopped bool

	mu      sync.Mutex
	running bool
	pending bool
	doneCh  chan struct{} // closed when the scheduler goes idle
}

// NewSyncScheduler builds a coalescing sync scheduler that runs git.Sync with
// the given opts (dirs pre-populated by the worker). It owns a cancellable
// context so Stop can abort an in-flight sync at its next transport step.
func NewSyncScheduler(opts git.SyncOptions, logger *slog.Logger) *SyncScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &SyncScheduler{
		opts:   opts,
		logger: logger,
		syncFn: git.Sync,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Trigger schedules (or coalesces into) a git sync. It never blocks: it writes
// pending/slot state under the mutex and, when idle, spawns the run goroutine.
// While running, an arriving push either coalesces into the pending slot
// (returning nil) or is dropped as already-pending (also nil) — never queuing
// unboundedly. The run loop re-checks pending under the mutex after each sync
// and continues on its own when it is set, so a push that sets pending during a
// run needs no additional wake signal. After Stop, Trigger is a no-op returning
// nil. It returns a non-nil error only for a nil receiver (defensive) — there is
// no normal failure path.
func (s *SyncScheduler) Trigger() error {
	if s == nil {
		return errors.New("webhook: nil sync scheduler")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.ctx.Err() != nil {
		// After Stop nothing runs; the server is stopped first by the worker so
		// no new triggers should arrive, but never block the handler regardless.
		return nil
	}
	if s.running {
		if s.pending {
			return nil // already coalesced into the next slot
		}
		s.pending = true
		return nil
	}
	// Idle: start a fresh run with a fresh done channel.
	s.running = true
	s.doneCh = make(chan struct{})
	go s.run()
	return nil
}

// Done returns a channel closed when the scheduler is idle (no running and no
// pending sync). While a run is in progress, it returns that run's done channel
// so a caller (typically a test) can await convergence to the latest pushed
// state. When idle it returns an already-closed channel.
func (s *SyncScheduler) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return s.doneCh
}

// run is the coalescing loop. It executes syncs back-to-back: after each
// attempt, if a pending slot was set (by a Trigger during the run), it clears
// it and runs once more with the latest remote state; when no push arrived it
// marks the scheduler idle and closes doneCh, which is exactly the convergence
// signal. It never holds the mutex during the sync itself. Cancellation is
// checked at the loop top so Stop's cancel() makes it exit promptly.
func (s *SyncScheduler) run() {
	for {
		if s.ctx.Err() != nil {
			s.closeIdle()
			return
		}
		s.logger.Info("Git sync triggered")
		err := s.syncFn(s.ctx, s.opts)
		if err != nil {
			s.logger.Error("Webhook: Git sync failed", "error", err)
		} else {
			s.logger.Info("Git sync completed")
		}
		s.mu.Lock()
		if !s.pending {
			s.running = false
			close(s.doneCh)
			s.mu.Unlock()
			return
		}
		s.pending = false
		s.mu.Unlock()
	}
}

// closeIdle marks the scheduler idle and closes its done channel. Used on the
// cancellation path so a Stop returns promptly even mid-pending.
func (s *SyncScheduler) closeIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		s.running = false
		close(s.doneCh)
	}
}

// Stop cancels the scheduler's internal context (so an in-flight git sync
// aborts at its next transport step) and waits, bounded by ctx, for the run
// goroutine to observe the cancellation and exit. After Stop, Trigger becomes a
// no-op (checked via stopped/ctx). It is idempotent and nil-safe.
func (s *SyncScheduler) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.cancel()
	}
	s.mu.Unlock()
	select {
	case <-s.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
