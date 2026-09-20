package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	git "relay/internal/git"
	"relay/internal/secrets"
)

// testSig computes a GitHub X-Hub-Signature-256 value for body under secret.
func testSig(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// pushPayloadBytes marshals a push-delivery payload with the given repository
// identity and ref. The returned bytes are what the signature MUST be computed
// over.
func pushPayloadBytes(sshURL, ref string) []byte {
	p := pushPayload{
		Ref:     ref,
		After:   "0123456789abcdef0123456789abcdef01234567",
		Deleted: false,
		Repository: struct {
			FullName string `json:"full_name"`
			Name     string `json:"name"`
			Owner    struct {
				Login string `json:"login"`
			} `json:"owner"`
			HTMLURL  string `json:"html_url"`
			CloneURL string `json:"clone_url"`
			SSHURL   string `json:"ssh_url"`
			GitURL   string `json:"git_url"`
		}{FullName: "acme/backend", SSHURL: sshURL},
	}
	b, _ := json.Marshal(&p)
	return b
}

// fakeTrigger is a test SyncTrigger counting Trigger calls and exposing a Done
// channel closed on demand, so handler tests assert "sync was (or was not)
// scheduled" with no real git transport.
type fakeTrigger struct {
	mu    sync.Mutex
	calls int
	done  chan struct{}
}

func newFakeTrigger() *fakeTrigger { return &fakeTrigger{done: make(chan struct{})} }

func (f *fakeTrigger) Trigger() error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil
}

func (f *fakeTrigger) Done() <-chan struct{} { return f.done }
func (f *fakeTrigger) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// setupHandler builds a GitHubProvider wired to a real LocalProvider over a temp
// secrets dir (secret value "test-secret" under name "gh_secret"), a fake
// trigger, and a persisted git config for repository git@github.com:acme/backend.git
// at the given configured ref. When cfgRef is empty, no git config is written
// (the "no source configured" case). It returns the provider, the trigger, and a
// capturing log buffer.
func setupHandler(t *testing.T, cfgRef string) (*GitHubProvider, *fakeTrigger, *bytes.Buffer) {
	t.Helper()
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

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
	if cfgRef != "" {
		if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", cfgRef, "", "gh_secret"); err != nil {
			t.Fatalf("set source: %v", err)
		}
	}

	tr := newFakeTrigger()
	h := NewGitHubProvider(logger, "gh_secret", prov, cfgPath, tr)
	return h, tr, logBuf
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func pushReq(body []byte, sig, event string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/github", bytes.NewReader(body))
	if sig != "" {
		req.Header.Set("X-Hub-Signature-256", sig)
	}
	if event != "" {
		req.Header.Set("X-GitHub-Event", event)
	}
	return req
}

func TestMatchRepository(t *testing.T) {
	mk := func(ssh, clone, html string) *pushPayload {
		p := &pushPayload{}
		p.Repository.SSHURL = ssh
		p.Repository.CloneURL = clone
		p.Repository.HTMLURL = html
		return p
	}
	const configured = "git@github.com:acme/backend.git"
	cases := []struct {
		name string
		cfg  string
		pl   *pushPayload
		want bool
	}{
		{"ssh matches", configured, mk("git@github.com:acme/backend.git", "", ""), true},
		{"clone matches https", configured, mk("", "https://github.com/acme/backend.git", ""), true},
		{"html matches", configured, mk("", "", "https://github.com/acme/backend"), true},
		{"case-insensitive path", configured, mk("", "https://github.com/ACME/Backend.git", ""), true},
		{"ssh:// vs scp-like", "ssh://git@github.com/acme/backend.git", mk("git@github.com:acme/backend.git", "", ""), true},
		{"different repo", configured, mk("git@github.com:other/repo.git", "", ""), false},
		{"different host", configured, mk("git@gitlab.com:acme/backend.git", "", ""), false},
		{"no repo info", configured, mk("", "", ""), false},
		{"nil payload", configured, nil, false},
		{"empty configured", "", mk("git@github.com:acme/backend.git", "", ""), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchRepository(c.cfg, c.pl); got != c.want {
				t.Fatalf("matchRepository(%q) = %v, want %v", c.cfg, got, c.want)
			}
		})
	}
}

func TestVerifySignature(t *testing.T) {
	secret := "test-secret"
	body := []byte("payload")
	good := testSig(secret, body)
	for _, tc := range []struct {
		name, sig string
		want      bool
	}{
		{"valid", good, true},
		{"missing", "", false},
		{"wrong secret", testSig("other", body), false},
		{"no prefix", "0123456789abcdef0123456789abcdef01234567", false},
		{"short hex", "sha256=abc123", false},
		{"non-hex", "sha256=zzzz" + strings.Repeat("9", 60), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := verifySignature(secret, tc.sig, body); got != tc.want {
				t.Fatalf("verifySignature = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestValidPushAccepted(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 1 {
		t.Fatalf("trigger calls = %d, want 1", tr.count())
	}
}

// TestPushedFullSHAAccepted, TestPushedFullSHAMismatchIgnored,
// TestPushedTagAccepted, TestPushedAbbreviatedSHAAccepted, and
// TestPushedAbbreviatedSHAMismatchIgnored cover the handler-level pushed-ref
// matching against a hash- or tag-configured ref, exercising the centralized
// git.MatchPushedRef through the full webhook path.
func TestPushedFullSHAAccepted(t *testing.T) {
	h, tr, _ := setupHandler(t, "abcdef0123456789abcdef0123456789abcdef01")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "abcdef0123456789abcdef0123456789abcdef01")
	// Configured SHA is lowercase; GitHub reports lowercased SHAs too.
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 1 {
		t.Fatalf("trigger calls = %d, want 1", tr.count())
	}
}

func TestPushedFullSHAMismatchIgnored(t *testing.T) {
	h, tr, _ := setupHandler(t, "abcdef0123456789abcdef0123456789abcdef01")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "0123456789abcdef0123456789abcdef01234567")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestPushedTagAccepted(t *testing.T) {
	h, tr, _ := setupHandler(t, "v1")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/tags/v1")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 1 {
		t.Fatalf("trigger calls = %d, want 1", tr.count())
	}
}

func TestPushedAbbreviatedSHAAccepted(t *testing.T) {
	// Configured abbreviated prefix uppercase, pushed lowercase.
	h, tr, _ := setupHandler(t, "DEADBEEF")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "deadbeef")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 1 {
		t.Fatalf("trigger calls = %d, want 1", tr.count())
	}
}

func TestPushedAbbreviatedSHAMismatchIgnored(t *testing.T) {
	h, tr, _ := setupHandler(t, "DEADBEEF")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "deadbeee")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestMissingSignatureRejected(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, "", "push"))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestMalformedSignatureRejected(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	for _, sig := range []string{"sha256=zzz", "sha256=abcd", "noprefix"} {
		rec := serve(h, pushReq(body, sig, "push"))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("sig %q status = %d, want 401", sig, rec.Code)
		}
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestInvalidSignatureWrongSecret(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, testSig("wrong-secret", body), "push"))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestMalformedPayloadRejected(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := []byte(`{"ref": `)
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestUnsupportedEventIgnored(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	// ping event
	rec := serve(h, pushReq(body, testSig("test-secret", body), "ping"))
	if rec.Code != http.StatusOK {
		t.Fatalf("ping status = %d, want 200", rec.Code)
	}
	// missing event header
	rec = serve(h, pushReq(body, testSig("test-secret", body), ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("missing event header status = %d, want 200", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestNonMatchingRefIgnored(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/develop")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (ignored)", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestDeletedPushIgnored(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := []byte(`{"ref":"refs/heads/main","deleted":true}`)
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (ignored)", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

func TestRepositoryMismatchIgnored(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:other/repo.git", "refs/heads/main")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (ignored)", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

// setupNilSecretProvider builds a provider with NO secrets provider but a
// NON-EMPTY secret ref ("gh_secret"), so every request 500s: a configured ref
// cannot be resolved without a resolver. (The empty-ref + nil-secrets
// combination — unsigned-accepting, guarded by a 500 only as defense-in-depth —
// is the setupUnsignedProvider helper below.) It returns the provider, a fake
// trigger, and a capturing log buffer.
func setupNilSecretProvider(t *testing.T) (*GitHubProvider, *fakeTrigger, *bytes.Buffer) {
	t.Helper()
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	gitDir := t.TempDir()
	cfgPath := filepath.Join(gitDir, "source.json")
	if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", "main", "", "gh_secret"); err != nil {
		t.Fatalf("set source: %v", err)
	}
	tr := newFakeTrigger()
	h := NewGitHubProvider(logger, "gh_secret", nil, cfgPath, tr)
	return h, tr, logBuf
}

func TestSecretNotConfigured500(t *testing.T) {
	h, tr, _ := setupNilSecretProvider(t)
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

// setupUnsignedProvider builds a provider with an EMPTY secret ref (and nil
// secrets), the unsigned-delivery case: no signature verification runs, so
// deliveries are accepted unauthenticated. It persists a git source for
// git@github.com:acme/backend.git at the given ref. It returns the provider, a
// fake trigger, and a capturing log buffer.
func setupUnsignedProvider(t *testing.T, cfgRef string) (*GitHubProvider, *fakeTrigger, *bytes.Buffer) {
	t.Helper()
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	gitDir := t.TempDir()
	cfgPath := filepath.Join(gitDir, "source.json")
	if err := git.SetSource(cfgPath, "git@github.com:acme/backend.git", cfgRef, "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	tr := newFakeTrigger()
	h := NewGitHubProvider(logger, "", nil, cfgPath, tr)
	return h, tr, logBuf
}

// TestUnsignedPushAcceptedWithEmptySecret proves an empty secret ref accepts an
// unsigned matching push: no X-Hub-Signature-256 header, no secret provider —
// the delivery is treated as authentic and a matching push triggers a sync (202).
func TestUnsignedPushAcceptedWithEmptySecret(t *testing.T) {
	h, tr, _ := setupUnsignedProvider(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, "", "push"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 1 {
		t.Fatalf("trigger calls = %d, want 1", tr.count())
	}
}

// TestUnsignedNonPushEventIgnoredWithEmptySecret proves an empty secret ref
// still runs the event filter: a non-push (e.g. ping) delivery is acknowledged
// (200) and ignored, with no sync triggered.
func TestUnsignedNonPushEventIgnoredWithEmptySecret(t *testing.T) {
	h, tr, logBuf := setupUnsignedProvider(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, "", "ping"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
	// The unsigned-acceptance path logs at Debug, never a per-request Warn.
	if strings.Contains(logBuf.String(), "level=WARN") {
		t.Fatalf("unsigned delivery produced a Warn:\n%s", logBuf.String())
	}
}

func TestSecretNeverLeaksInLogsOrResponse(t *testing.T) {
	h, _, logBuf := setupHandler(t, "main")
	body := pushPayloadBytes("git@github.com:acme/backend.git", "refs/heads/main")
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	// The real secret value and the signature header value must never appear.
	if strings.Contains(logBuf.String(), "test-secret") {
		t.Fatalf("log leaked secret value:\n%s", logBuf.String())
	}
	if strings.Contains(rec.Body.String(), "test-secret") {
		t.Fatalf("response leaked secret value:\n%s", rec.Body.String())
	}
	// The signature value must not appear in logs either.
	sig := testSig("test-secret", body)
	if strings.Contains(logBuf.String(), sig) {
		t.Fatalf("log leaked signature header value:\n%s", logBuf.String())
	}
}

func TestBodyOverLimit400(t *testing.T) {
	h, tr, _ := setupHandler(t, "main")
	// > maxBodyBytes (10 MiB) of payload.
	body := bytes.Repeat([]byte("x"), maxBodyBytes+1)
	rec := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if tr.count() != 0 {
		t.Fatalf("trigger calls = %d, want 0", tr.count())
	}
}

// --- Concurrency / coalescing tests using the REAL scheduler with an
// injectable syncFn seam. ---

// makeBareRepo builds a work repo + bare remote (Copied from the git package's
// fixture pattern; the git test helpers are not exported, so this test keeps its
// own small copy). It returns the bare clone path.
func makeBareRepo(t *testing.T) string {
	t.Helper()
	work := filepath.Join(t.TempDir(), "work")
	bare := filepath.Join(t.TempDir(), "remote.git")
	r, err := gogit.PlainInit(work, false)
	if err != nil {
		t.Fatalf("init work: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(work, "fn"), 0o755); err != nil {
		t.Fatalf("mkdir fn: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "fn", "template.yaml"), []byte("runtime: node24\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if _, err := wt.Add("fn/template.yaml"); err != nil {
		t.Fatalf("add: %v", err)
	}
	h, err := wt.Commit("initial", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Now()}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := r.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName("refs/heads/main"), h)); err != nil {
		t.Fatalf("set ref: %v", err)
	}
	if _, err := gogit.PlainInit(bare, true); err != nil {
		t.Fatalf("init bare: %v", err)
	}
	b, err := gogit.PlainOpen(bare)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	orig, err := b.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{work}})
	if err != nil {
		orig, err = b.Remote("origin")
		if err != nil {
			t.Fatalf("get origin: %v", err)
		}
	}
	if err := orig.Fetch(&gogit.FetchOptions{RefSpecs: []config.RefSpec{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}}); err != nil {
		t.Fatalf("seed bare: %v", err)
	}
	return bare
}

// makeScheduler builds a real SyncScheduler whose syncFn is injectable,
// counting concurrent entries. It returns the scheduler and a release func that
// unblocks the injected sync.
func makeScheduler(t *testing.T, syncFn func(context.Context, git.SyncOptions) error) (*SyncScheduler, *atomic.Int32) {
	t.Helper()
	var concurrent atomic.Int32
	opts := git.NewSyncOptions()
	opts.ConfigPath = filepath.Join(t.TempDir(), "source.json")
	opts.CheckoutDir = filepath.Join(t.TempDir(), "checkout")
	opts.FunctionsDir = filepath.Join(t.TempDir(), "functions")
	opts.CloneURL = filepath.Join(t.TempDir(), "remote.git")
	s := NewSyncScheduler(opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.syncFn = syncFn
	return s, &concurrent
}

// blockingSync returns a syncFn that blocks on start; the caller closes start to
// release it. It tracks concurrent entries.
func blockingSync(t *testing.T, start chan struct{}, concurrent *atomic.Int32, entered chan struct{}) func(context.Context, git.SyncOptions) error {
	t.Helper()
	return func(_ context.Context, _ git.SyncOptions) error {
		c := concurrent.Add(1)
		defer concurrent.Add(-1)
		if c > 1 {
			t.Errorf("sync concurrency = %d > 1", c)
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		<-start
		return nil
	}
}

// TestSchedulerCoalescesConcurrentPushes proves at-most-one sync runs at a
// time: with a blocking sync, many rapid Triggers during a run coalesce into the
// single pending slot and never run concurrently. It also proves the final
// scheduler run converges (Done closes once idle).
func TestSchedulerCoalescesConcurrentPushes(t *testing.T) {
	start := make(chan struct{})
	release := func() { start <- struct{}{} } // rendezvous: each sync drains one
	var concurrent atomic.Int32
	entered := make(chan struct{}, 1)
	s, _ := makeScheduler(t, blockingSync(t, start, &concurrent, entered))

	if err := s.Trigger(); err != nil {
		t.Fatalf("first Trigger: %v", err)
	}
	// Wait until the run has entered the injected sync.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sync never entered")
	}
	// Many triggers while running: none run yet, all coalesce into at most the
	// single pending slot.
	for i := 0; i < 10; i++ {
		if err := s.Trigger(); err != nil {
			t.Fatalf("Trigger %d: %v", i, err)
		}
	}
	release() // let the running sync finish -> pending coalesced sync starts
	// Wait for the coalesced sync to enter, then release it too.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("coalesced sync never entered")
	}
	release()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-s.Done():
	case <-ctx.Done():
		t.Fatal("scheduler never went idle")
	}
	<-s.Done()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if c := concurrent.Load(); c > 1 {
		t.Fatalf("observed concurrency %d, want <= 1", c)
	}
}

// TestSchedulerConvergesToLatest proves the scheduler's coalescing run
// converges to the remote's latest state and materializes it: a Trigger drives a
// real git.Sync (the scheduler's default syncFn) against a local bare repo, and
// when the scheduler returns to idle the persisted config is marked Synced —
// i.e. the pushed source reached /functions.
func TestSchedulerConvergesToLatest(t *testing.T) {
	bare := makeBareRepo(t)
	s, err := schedForBare(t, bare)
	if err != nil {
		t.Fatalf("sched: %v", err)
	}
	if err := s.Trigger(); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-s.Done():
	case <-ctx.Done():
		t.Fatal("scheduler never went idle")
	}
	cfg, err := git.LoadConfig(s.opts.ConfigPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Synced {
		t.Fatalf("config not marked synced after scheduler run: %+v", cfg)
	}
}

func schedForBare(t *testing.T, bare string) (*SyncScheduler, error) {
	t.Helper()
	opts := git.NewSyncOptions()
	opts.ConfigPath = filepath.Join(t.TempDir(), "source.json")
	opts.CheckoutDir = filepath.Join(t.TempDir(), "checkout")
	opts.FunctionsDir = filepath.Join(t.TempDir(), "functions")
	opts.CloneURL = bare
	// Persist a config so Sync (which reads it) has a source.
	if err := git.SetSource(opts.ConfigPath, "git@github.com:acme/backend.git", "main", "", ""); err != nil {
		return nil, err
	}
	s := NewSyncScheduler(opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return s, nil
}
