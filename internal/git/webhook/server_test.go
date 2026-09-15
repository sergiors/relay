package webhook

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	git "relay/internal/git"
	"relay/internal/secrets"
)

// testHandler is a trivial GitHubProvider that responds to routing/lifecycle
// tests without any auth logic (a nil secret provider makes requests 500, but
// here we only care that POST /github reaches SOMETHING).
func testHandler() *GitHubProvider {
	return &GitHubProvider{}
}

// testStubProviderName is the route segment registered for a stubProvider.
const testStubProviderName = "github"

// stubProvider is a minimal Provider harness for dispatch tests: it serves the
// given status and fixed body at POST /<Name()>, proving the provider seam
// without exercising any real provider semantics.
type stubProvider struct {
	name string
	code int
	body string
}

func (p *stubProvider) Name() string { return p.name }

func (p *stubProvider) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(p.code)
	_, _ = io.WriteString(w, p.body)
}

// TestMuxRoutesGitHub only registers POST /github: a GET on /github is a 405,
// and a POST to any other path is a 404.
func TestMuxRoutesGitHub(t *testing.T) {
	srv := newServer("127.0.0.1:0", nil, testHandler())
	mux := srv.handler.(*http.ServeMux)

	// GET /github -> 405 (method pattern only allows POST).
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/github", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /github = %d, want 405", rec.Code)
	}

	// POST /other -> 404.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST /other = %d, want 404", rec.Code)
	}

	// POST /github reaches the handler.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/github", nil))
	if rec.Code == http.StatusNotFound || rec.Code == http.StatusMethodNotAllowed {
		t.Fatalf("POST /github = %d, want routed to handler", rec.Code)
	}
}

// TestMultiProviderDispatch proves the Provider seam dispatches each provider to
// its own POST /<Name> route, with unrelated paths/methods rejected by the mux.
// A provider named "github" plus a second stubProvider named "gitlab" shows how
// a future gitlab.go plugs in without changing Server at all.
func TestMultiProviderDispatch(t *testing.T) {
	srv := newServer(
		"127.0.0.1:0",
		nil,
		&stubProvider{name: "github", code: http.StatusAccepted, body: "github-ok"},
		&stubProvider{name: "gitlab", code: http.StatusCreated, body: "gitlab-ok"},
	)
	mux := srv.handler.(*http.ServeMux)

	// POST /github reaches the github provider.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/github", nil))
	if rec.Code != http.StatusAccepted || rec.Body.String() != "github-ok" {
		t.Fatalf("POST /github = %d %q, want 202 github-ok", rec.Code, rec.Body.String())
	}

	// POST /gitlab reaches the gitlab provider.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gitlab", nil))
	if rec.Code != http.StatusCreated || rec.Body.String() != "gitlab-ok" {
		t.Fatalf("POST /gitlab = %d %q, want 201 gitlab-ok", rec.Code, rec.Body.String())
	}

	// POST /other -> 404.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST /other = %d, want 404", rec.Code)
	}

	// GET /github -> 405 (only POST is registered).
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/github", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /github = %d, want 405", rec.Code)
	}
}

// TestNewServerRoutesSkipsNilProvider verifies a nil provider is skipped rather
// than registered, so no route exists and POST /github 404s (zero providers is
// also valid and behaves identically).
func TestNewServerRoutesSkipsNilProvider(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/github", nil)
	srv := newServer("", nil, nil) // a single nil provider, plus the nil logger
	srv.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("nil-provider route status = %d, want 404 (no route registered)", rec.Code)
	}
}

// TestNewServerRejectsInvalidProviderName verifies newServer panics on a
// provider whose Name() is empty or contains "/", mirroring ServeMux's own
// panic-on-programmer-error contract. A valid name (no "/") registers fine.
func TestNewServerRejectsInvalidProviderName(t *testing.T) {
	for _, name := range []string{"", "a/b"} {
		t.Run("name="+name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("newServer did not panic for name %q", name)
				}
			}()
			newServer("", nil, &stubProvider{name: name, code: http.StatusOK, body: "x"})
		})
	}

	// A valid single-segment name registers without panicking.
	srv := newServer("", nil, &stubProvider{name: "valid", code: http.StatusOK, body: "x"})
	srv.handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/valid", nil))
}

// TestServerLifecycleStartServeStop verifies Start binds synchronously, serves
// the handler in the background, and Stop drains it gracefully.
func TestServerLifecycleStartServeStop(t *testing.T) {
	// Free-port probe-listen pattern (mirrors the metrics server tests).
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	h := testHandler()
	srv := newServer(addr, slog.New(slog.NewTextHandler(io.Discard, nil)), h)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Real POST over HTTP.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Post("http://"+addr+"/github", "application/json", strings.NewReader(`{}`))
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never became reachable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Stop twice is idempotent.
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// TestServerStartTwiceFails verifies Start may be called exactly once.
func TestServerStartTwiceFails(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv := newServer(addr, nil, testHandler())
	if err := srv.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := srv.Start(); err == nil {
		t.Fatal("second Start returned nil, want error")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := srv.Start(); err == nil {
		t.Fatal("Start after Stop returned nil, want error")
	}
}

// TestServerStartFailsFastOnBadAddr verifies Start returns the bind error
// immediately for an unparseable address.
func TestServerStartFailsFastOnBadAddr(t *testing.T) {
	err := newServer("crap", slog.New(slog.NewTextHandler(io.Discard, nil)), testHandler()).Start()
	if err == nil {
		t.Fatal("Start on bad addr returned nil, want error")
	}
	if !strings.Contains(err.Error(), "webhook:") {
		t.Fatalf("bind error missing webhook: prefix: %v", err)
	}
}

// TestServerStopNeverStartedAndNil verifies Stop is safe on a never-started
// server and a nil receiver; the never-started server (built via newServer) has
// a nil scheduler, proving Stop works with a nil scheduler.
func TestServerStopNeverStartedAndNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := newServer("", nil, testHandler()).Stop(ctx); err != nil {
		t.Fatalf("Stop on never-started server: %v", err)
	}
	var nilSrv *Server
	if err := nilSrv.Stop(ctx); err != nil {
		t.Fatalf("Stop on nil server: %v", err)
	}
	if err := nilSrv.Start(); err == nil {
		t.Fatal("Start on nil server returned nil, want error")
	}
}

// testLogger returns a logger and its capturing buffer.
func testLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// freeAddr probe-listens for a free port and returns the address after closing
// the listener (mirrors the metrics server test pattern).
func freeAddr(t *testing.T) string {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()
	return addr
}

// subsystemConfig builds a Config with temp dirs and a real LocalProvider holding
// the GitHub webhook secret under "gh_secret". The git source is persisted for
// acme/backend at ref main with webhook secret reference "gh_secret".
func subsystemConfig(t *testing.T) Config {
	t.Helper()
	secretsDir := t.TempDir()
	store, err := secrets.NewLocal(secretsDir)
	if err != nil {
		t.Fatalf("new local: %v", err)
	}
	if err := store.Set(context.Background(), "gh_secret", "test-secret"); err != nil {
		t.Fatalf("store set: %v", err)
	}
	prov, err := secrets.NewLocalProvider(secretsDir)
	if err != nil {
		t.Fatalf("provider: %v", err)
	}

	gitDir := t.TempDir()
	cfgPath := filepath.Join(gitDir, "source.json")
	if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", "main", "", "gh_secret"); err != nil {
		t.Fatalf("set source: %v", err)
	}
	return Config{
		Secrets:      prov,
		ConfigPath:   cfgPath,
		CheckoutDir:  filepath.Join(gitDir, "checkout"),
		FunctionsDir: filepath.Join(t.TempDir(), "functions"),
		SSHDir:       filepath.Join(gitDir, "ssh"),
	}
}

// subsystemConfigNoSecret builds a Config like subsystemConfig but with an EMPTY
// webhook secret reference in the persisted git source (unsigned deliveries). No
// secret is stored/needed since signature verification is disabled; a resolver
// is still supplied so NewServer's enabled path is exercised.
func subsystemConfigNoSecret(t *testing.T) Config {
	t.Helper()
	gitDir := t.TempDir()
	cfgPath := filepath.Join(gitDir, "source.json")
	if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	return Config{
		Secrets:      mustProvider(t),
		ConfigPath:   cfgPath,
		CheckoutDir:  filepath.Join(gitDir, "checkout"),
		FunctionsDir: filepath.Join(t.TempDir(), "functions"),
		SSHDir:       filepath.Join(gitDir, "ssh"),
	}
}

// TestNewDisabledWithoutGitSource verifies NewServer returns nil (Warn logged)
// when the git source config does not exist at the configured ConfigPath
// override.
func TestNewDisabledWithoutGitSource(t *testing.T) {
	logger, buf := 	testLogger()
	// Pass a temp ConfigPath pointing at a nonexistent file.
	cfg := Config{
		Secrets:    mustProvider(t),
		ConfigPath: filepath.Join(t.TempDir(), "does-not-exist", "source.json"),
	}
	if s := NewServer("127.0.0.1:0", logger, cfg); s != nil {
		t.Fatal("NewServer returned non-nil without a git source; want nil (disabled)")
	}
	if !strings.Contains(buf.String(), "no git source configured; webhook disabled") {
		t.Fatalf("missing disable Warn:\n%s", buf.String())
	}
}

// mustProvider returns a real LocalProvider over a temp dir (used where only the
// presence of a resolver is required before the disable path is reached).
func mustProvider(t *testing.T) secrets.Provider {
	t.Helper()
	p, err := secrets.NewLocalProvider(t.TempDir())
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	return p
}

// TestNewDisabledWithoutWebhookSecret verifies NewServer with a configured git
// source but EMPTY webhook secret is now ENABLED (non-nil): unsigned deliveries
// are accepted, so an empty ref never disables the server. The log must NOT
// contain the old "no webhook secret configured" disable Warn.
func TestNewDisabledWithoutWebhookSecret(t *testing.T) {
	logger, buf := testLogger()
	cfgPath := filepath.Join(t.TempDir(), "source.json")
	if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	cfg := Config{Secrets: mustProvider(t), ConfigPath: cfgPath}
	if s := NewServer("127.0.0.1:0", logger, cfg); s == nil {
		t.Fatal("NewServer returned nil with an empty webhook secret; want non-nil (enabled, unsigned deliveries)")
	}
	if strings.Contains(buf.String(), "no webhook secret configured") {
		t.Fatalf("log contained the stale disable Warn:\n%s", buf.String())
	}
}

// TestServerAssemblesAndServesUnsignedGitHub exercises the full assembled server
// over real HTTP with an EMPTY webhook secret: NewServer on a free port, Start,
// an UNSIGNED push for acme/backend refs/heads/main (no X-Hub-Signature-256
// header), 202, then Stop. It mirrors TestServerAssemblesAndServesGitHub but
// without signature verification.
func TestServerAssemblesAndServesUnsignedGitHub(t *testing.T) {
	logger, _ := testLogger()
	s := NewServer(freeAddr(t), logger, subsystemConfigNoSecret(t))
	if s == nil {
		t.Fatal("NewServer returned nil for an enabled server")
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	// Unsigned POST over HTTP: no X-Hub-Signature-256 header.
	req, err := http.NewRequest(http.MethodPost, "http://"+s.addr+"/github", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("unsigned POST: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", resp.StatusCode, respBody)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestNewDisabledWithoutSecretResolver verifies NewServer returns nil
// (defensive) when no secret resolver is configured.
func TestNewDisabledWithoutSecretResolver(t *testing.T) {
	logger, buf := 	testLogger()
	cfgPath := filepath.Join(t.TempDir(), "source.json")
	if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", "main", "", "gh_secret"); err != nil {
		t.Fatalf("set source: %v", err)
	}
	cfg := Config{ConfigPath: cfgPath} // Secrets is nil
	if s := NewServer("127.0.0.1:0", logger, cfg); s != nil {
		t.Fatal("NewServer returned non-nil without a secret resolver; want nil (disabled)")
	}
	if !strings.Contains(buf.String(), "no secret resolver configured; webhook disabled") {
		t.Fatalf("missing disable Warn:\n%s", buf.String())
	}
}

// TestServerAssemblesAndServesGitHub exercises the full assembled server over
// real HTTP: NewServer on a free port, Start, a valid signed push for
// acme/backend refs/heads/main, 202, then Stop; a second Stop is a no-op and
// Start after Stop errors. The capturing log must never contain the secret
// value.
func TestServerAssemblesAndServesGitHub(t *testing.T) {
	logger, logBuf := 	testLogger()
	s := NewServer(freeAddr(t), logger, subsystemConfig(t))
	if s == nil {
		t.Fatal("NewServer returned nil for an enabled server")
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	// Real signed POST over HTTP; the signature is computed over the exact same
	// body bytes that are sent.
	buf := bytes.NewReader(body)
	req, err := http.NewRequest(http.MethodPost, "http://"+s.addr+"/github", buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Hub-Signature-256", testSig("test-secret", body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("signed POST: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", resp.StatusCode, respBody)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Second Stop is a no-op.
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	// Start after Stop errors (Server semantics).
	if err := s.Start(); err == nil {
		t.Fatal("Start after Stop returned nil, want error")
	}
	// The secret value must never appear in the log.
	if strings.Contains(logBuf.String(), "test-secret") {
		t.Fatalf("log leaked secret value:\n%s", logBuf.String())
	}
}

// TestServerStopNilSafe verifies Stop is a no-op on a nil *Server and on a
// never-started server built via newServer (which has a nil scheduler).
func TestServerStopNilSafe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var nilSrv *Server
	if err := nilSrv.Stop(ctx); err != nil {
		t.Fatalf("Stop on nil Server: %v", err)
	}
	if err := nilSrv.Start(); err == nil {
		t.Fatal("Start on nil Server returned nil, want error")
	}

	// A never-started server built via newServer has a nil scheduler; Stop must
	// still return nil without touching it.
	s := newServer("", nil, testHandler())
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop on never-started newServer: %v", err)
	}
}
