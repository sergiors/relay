package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Distinctive Infisical test fixtures. Every sentinel is chosen so an error or
// log line that echoes credential material, an access token, or a resolved
// value can be caught by the redaction assertions in these tests.
const (
	infisicalTestProject      = "proj-relay-tests"
	infisicalTestEnvironment  = "staging"
	infisicalTestSecretPath   = "/relay/secrets"
	infisicalTestClientID     = "universal-auth-client-id"
	infisicalTestClientSecret = "universal-auth-client-secret"
	infisicalTestToken        = "access-token-sentinel"
	infisicalTestValue        = "resolved-secret-value-sentinel"

	infisicalLoginPath    = "/api/v1/auth/universal-auth/login"
	infisicalSecretPrefix = "/api/v4/secrets/"
	// infisicalListPath is the collection endpoint `List` calls; it must not
	// collide with the per-name read path, which always carries a trailing
	// slash plus the secret name.
	infisicalListPath = "/api/v4/secrets"
)

// recordedLogin is one Universal Auth login request as the test server saw it.
type recordedLogin struct {
	method      string
	path        string
	contentType string
	clientID    string
	// clientSecret is captured for redaction assertions only: the provider
	// must send it in the body, and no failure may echo it back.
	clientSecret string
}

// recordedSecretRead is one secret read request as the test server saw it.
type recordedSecretRead struct {
	method string
	path   string
	auth   string
	query  url.Values
	// clientDone mirrors the request context: it is closed when the waiting
	// client disconnects or its context ends, so gated handlers can return
	// and let the test server shut down.
	clientDone <-chan struct{}
}

// recordedList is one secret-list request as the test server saw it.
type recordedList struct {
	method string
	path   string
	auth   string
	query  url.Values
}

// infisicalRequests records every request the provider sent, so tests can
// assert on the exact request shape and on how many network calls happened.
type infisicalRequests struct {
	mu          sync.Mutex
	logins      []recordedLogin
	secretReads []recordedSecretRead
	lists       []recordedList
}

func (r *infisicalRequests) addLogin(l recordedLogin) {
	r.mu.Lock()
	r.logins = append(r.logins, l)
	r.mu.Unlock()
}

func (r *infisicalRequests) addList(l recordedList) {
	r.mu.Lock()
	r.lists = append(r.lists, l)
	r.mu.Unlock()
}

func (r *infisicalRequests) addSecretRead(s recordedSecretRead) {
	r.mu.Lock()
	r.secretReads = append(r.secretReads, s)
	r.mu.Unlock()
}

func (r *infisicalRequests) counts() (logins, reads int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.logins), len(r.secretReads)
}

func (r *infisicalRequests) loginCalls() []recordedLogin {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedLogin(nil), r.logins...)
}

func (r *infisicalRequests) secretCalls() []recordedSecretRead {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedSecretRead(nil), r.secretReads...)
}

func (r *infisicalRequests) listCalls() []recordedList {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedList(nil), r.lists...)
}

// fakeInfisical is a TLS test double for the two Infisical endpoints the
// provider calls. A nil handler serves a healthy default, so each test only
// spells out the behavior it is exercising.
type fakeInfisical struct {
	*httptest.Server
	requests *infisicalRequests
}

func (f *fakeInfisical) client() *http.Client { return f.Server.Client() }

func newFakeInfisical(
	t *testing.T,
	onLogin func(w http.ResponseWriter, r recordedLogin),
	onRead func(w http.ResponseWriter, r recordedSecretRead),
	onList ...func(w http.ResponseWriter, r recordedList),
) *fakeInfisical {
	t.Helper()
	reqs := &infisicalRequests{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == infisicalLoginPath:
			raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			var creds struct {
				ClientID     string `json:"clientId"`
				ClientSecret string `json:"clientSecret"`
			}
			_ = json.Unmarshal(raw, &creds)
			call := recordedLogin{
				method:       r.Method,
				path:         r.URL.Path,
				contentType:  r.Header.Get("Content-Type"),
				clientID:     creds.ClientID,
				clientSecret: creds.ClientSecret,
			}
			reqs.addLogin(call)
			if onLogin != nil {
				onLogin(w, call)
				return
			}
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"accessToken": infisicalTestToken,
				"expiresIn":   900,
			})
		case r.Method == http.MethodGet && r.URL.Path == infisicalListPath:
			call := recordedList{
				method: r.Method,
				path:   r.URL.Path,
				auth:   r.Header.Get("Authorization"),
				query:  r.URL.Query(),
			}
			reqs.addList(call)
			if len(onList) > 0 && onList[0] != nil {
				onList[0](w, call)
				return
			}
			// A healthy default: an empty scope, so a test only spells out
			// the listing behavior it exercises.
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secrets": []any{},
			})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, infisicalSecretPrefix):
			call := recordedSecretRead{
				method:     r.Method,
				path:       r.URL.Path,
				auth:       r.Header.Get("Authorization"),
				query:      r.URL.Query(),
				clientDone: r.Context().Done(),
			}
			reqs.addSecretRead(call)
			if onRead != nil {
				onRead(w, call)
				return
			}
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secret": map[string]string{"secretValue": infisicalTestValue},
			})
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewUnstartedServer(handler)
	// The gated/parallel handlers can leave handshakes in flight when the
	// server closes; keep that shutdown noise out of the test output.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &fakeInfisical{Server: srv, requests: reqs}
}

func writeInfisicalJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// newInfisicalTestProvider builds a provider wired to a fake server with the
// fixture project/environment/path/credentials. Mutators override one option
// at a time (for example to shorten the cache window or script a custom
// client), mirroring how production wires InfisicalOptions from Config.
func newInfisicalTestProvider(
	t *testing.T,
	fake *fakeInfisical,
	mutate ...func(*InfisicalOptions),
) *InfisicalProvider {
	t.Helper()
	opts := InfisicalOptions{
		SiteURL:      fake.URL,
		ProjectID:    infisicalTestProject,
		Environment:  infisicalTestEnvironment,
		SecretPath:   infisicalTestSecretPath,
		ClientID:     infisicalTestClientID,
		ClientSecret: infisicalTestClientSecret,
		HTTPClient:   fake.client(),
	}
	for _, m := range mutate {
		m(&opts)
	}
	p, err := NewInfisicalProvider(opts)
	if err != nil {
		t.Fatalf("NewInfisicalProvider: %v", err)
	}
	return p
}

// testClock is a manually advanced clock so TTL expiry is exercised without
// sleeping. Install it with p.now = clock.Now before the first Resolve.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// mustResolve resolves name or fails the test with the returned error.
func mustResolve(t *testing.T, p *InfisicalProvider, name string) string {
	t.Helper()
	got, err := p.Resolve(context.Background(), name)
	if err != nil {
		t.Fatalf("Resolve(%q): %v", name, err)
	}
	return got
}

// assertNoSecretMaterial fails the test if text echoes any of the credential
// or value sentinels the fixtures use.
func assertNoSecretMaterial(t *testing.T, text string, forbidden ...string) {
	t.Helper()
	for _, s := range forbidden {
		if strings.Contains(text, s) {
			t.Fatalf("error leaked secret material %q: %s", s, text)
		}
	}
}

// TestInfisicalResolveSendsUniversalAuthAndConfiguredParameters pins the full
// request contract of a successful resolve: Universal Auth login with the
// configured credentials, then one secret read carrying the issued token and
// the configured project, environment, and path for the requested name — and
// the resolved value returned verbatim.
func TestInfisicalResolveSendsUniversalAuthAndConfiguredParameters(t *testing.T) {
	fake := newFakeInfisical(t, nil, nil)
	p := newInfisicalTestProvider(t, fake)

	got := mustResolve(t, p, "database-url")
	if got != infisicalTestValue {
		t.Fatalf("resolved = %q, want %q", got, infisicalTestValue)
	}

	logins := fake.requests.loginCalls()
	if len(logins) != 1 {
		t.Fatalf("logins = %d, want 1", len(logins))
	}
	login := logins[0]
	if login.method != http.MethodPost || login.path != infisicalLoginPath {
		t.Fatalf("login request = %s %s, want POST %s",
			login.method, login.path, infisicalLoginPath)
	}
	if !strings.Contains(login.contentType, "application/json") {
		t.Fatalf("login Content-Type = %q, want application/json", login.contentType)
	}
	if login.clientID != infisicalTestClientID ||
		login.clientSecret != infisicalTestClientSecret {
		t.Fatalf("login credentials = (%q, %q), want the configured pair",
			login.clientID, login.clientSecret)
	}

	reads := fake.requests.secretCalls()
	if len(reads) != 1 {
		t.Fatalf("secret reads = %d, want 1", len(reads))
	}
	read := reads[0]
	if read.method != http.MethodGet {
		t.Fatalf("secret read method = %s, want GET", read.method)
	}
	if want := infisicalSecretPrefix + "database-url"; read.path != want {
		t.Fatalf("secret read path = %q, want %q", read.path, want)
	}
	if read.auth != "Bearer "+infisicalTestToken {
		t.Fatalf("Authorization = %q, want the issued bearer token", read.auth)
	}

	wantQuery := url.Values{
		"projectId":              {infisicalTestProject},
		"environment":            {infisicalTestEnvironment},
		"secretPath":             {infisicalTestSecretPath},
		"type":                   {"shared"},
		"viewSecretValue":        {"true"},
		"expandSecretReferences": {"true"},
		"includeImports":         {"true"},
	}
	for key, want := range wantQuery {
		if got := read.query.Get(key); got != want[0] {
			t.Errorf("query %s = %q, want %q", key, got, want[0])
		}
	}
	if len(read.query) != len(wantQuery) {
		t.Errorf("query keys = %v, want exactly %v", read.query, wantQuery)
	}
}

// TestNewInfisicalProviderValidatesOptions pins the construction-time
// configuration contract: every required setting is checked, the site URL must
// be a credential-free absolute https URL whose raw input is never echoed
// (an operator may have put credentials there), and the secret path must be
// absolute.
func TestNewInfisicalProviderValidatesOptions(t *testing.T) {
	base := func() InfisicalOptions {
		return InfisicalOptions{
			SiteURL:      "https://vault.internal.example",
			ProjectID:    infisicalTestProject,
			Environment:  infisicalTestEnvironment,
			SecretPath:   infisicalTestSecretPath,
			ClientID:     infisicalTestClientID,
			ClientSecret: infisicalTestClientSecret,
		}
	}
	tests := []struct {
		name   string
		mutate func(*InfisicalOptions)
		want   string // substring the error must contain
		forbid string // substring the error must never contain
	}{
		{
			"missing project ID",
			func(o *InfisicalOptions) { o.ProjectID = "  " },
			"project ID", "",
		},
		{
			"missing environment",
			func(o *InfisicalOptions) { o.Environment = "" },
			"environment", "",
		},
		{
			"missing client ID",
			func(o *InfisicalOptions) { o.ClientID = " " },
			"client ID", "",
		},
		{
			"missing client secret",
			func(o *InfisicalOptions) { o.ClientSecret = "  " },
			"client secret", "",
		},
		{
			"plain http site URL",
			func(o *InfisicalOptions) { o.SiteURL = "http://vault.internal.example" },
			"absolute https://", "",
		},
		{
			"site URL without host",
			func(o *InfisicalOptions) { o.SiteURL = "https://" },
			"absolute https://", "",
		},
		{
			"site URL without scheme",
			func(o *InfisicalOptions) { o.SiteURL = "vault.internal.example" },
			"absolute https://", "",
		},
		{
			"site URL with credentials is not echoed",
			func(o *InfisicalOptions) {
				o.SiteURL = "https://operator:hunter2@vault.internal.example"
			},
			"absolute https://", "hunter2",
		},
		{
			"site URL query is not echoed",
			func(o *InfisicalOptions) { o.SiteURL = "https://vault.internal.example/?token=abc" },
			"must not contain a query or fragment", "token=abc",
		},
		{
			"malformed site URL is not echoed",
			func(o *InfisicalOptions) { o.SiteURL = "https://exa mple.com" },
			"absolute https://", "exa mple",
		},
		{
			"relative secret path",
			func(o *InfisicalOptions) { o.SecretPath = "team/app" },
			"must begin with '/'", "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := base()
			tt.mutate(&opts)
			_, err := NewInfisicalProvider(opts)
			if err == nil {
				t.Fatal("NewInfisicalProvider = nil error, want a configuration error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.want)
			}
			if tt.forbid != "" {
				assertNoSecretMaterial(t, err.Error(), tt.forbid)
			}
		})
	}
}

// TestNewInfisicalProviderDefaults pins the documented defaults and the
// caller-owned HTTP client contract: an empty site URL and secret path fall
// back to the documented values, the cache TTL falls back to the default, and
// an injected client keeps its own timeout while the provider enforces its
// request timeout and redirect policy on a private copy.
func TestNewInfisicalProviderDefaults(t *testing.T) {
	p, err := NewInfisicalProvider(InfisicalOptions{
		ProjectID:    infisicalTestProject,
		Environment:  infisicalTestEnvironment,
		ClientID:     infisicalTestClientID,
		ClientSecret: infisicalTestClientSecret,
	})
	if err != nil {
		t.Fatalf("NewInfisicalProvider: %v", err)
	}
	if p.baseURL.String() != DefaultInfisicalSiteURL {
		t.Errorf("base URL = %q, want %q", p.baseURL, DefaultInfisicalSiteURL)
	}
	if p.secretPath != DefaultInfisicalSecretPath {
		t.Errorf("secret path = %q, want %q", p.secretPath, DefaultInfisicalSecretPath)
	}
	if p.cacheTTL != DefaultInfisicalCacheTTL {
		t.Errorf("cache TTL = %v, want %v", p.cacheTTL, DefaultInfisicalCacheTTL)
	}
	if p.client.Timeout != DefaultInfisicalRequestTimeout {
		t.Errorf("client timeout = %v, want %v",
			p.client.Timeout, DefaultInfisicalRequestTimeout)
	}

	caller := &http.Client{Timeout: 3 * time.Second}
	withClient, err := NewInfisicalProvider(InfisicalOptions{
		SiteURL:      "https://vault.internal.example/",
		ProjectID:    infisicalTestProject,
		Environment:  infisicalTestEnvironment,
		ClientID:     infisicalTestClientID,
		ClientSecret: infisicalTestClientSecret,
		HTTPClient:   caller,
	})
	if err != nil {
		t.Fatalf("NewInfisicalProvider: %v", err)
	}
	if withClient.baseURL.String() != "https://vault.internal.example" {
		t.Errorf("base URL = %q, want the trailing slash trimmed", withClient.baseURL)
	}
	if withClient.client == caller {
		t.Error("provider kept the caller-owned client; want a private copy")
	}
	if withClient.client.Timeout != 3*time.Second {
		t.Errorf("timeout = %v, want the caller's 3s preserved", withClient.client.Timeout)
	}
	if caller.Timeout != 3*time.Second || caller.CheckRedirect != nil {
		t.Errorf("caller's client was mutated: Timeout=%v CheckRedirect set=%t",
			caller.Timeout, caller.CheckRedirect != nil)
	}
}

// TestInfisicalResolveRejectsInvalidNameWithoutNetwork pins that the shared
// name validator runs before any network traffic, so a traversal-shaped name
// can never become a request path segment.
func TestInfisicalResolveRejectsInvalidNameWithoutNetwork(t *testing.T) {
	fake := newFakeInfisical(t, nil, nil)
	p := newInfisicalTestProvider(t, fake)

	for _, name := range []string{"", "../etc", "/abs", "a/b", "UPPER", "trail."} {
		if _, err := p.Resolve(context.Background(), name); err == nil {
			t.Errorf("Resolve(%q) = nil, want error", name)
		}
	}
	if logins, reads := fake.requests.counts(); logins != 0 || reads != 0 {
		t.Fatalf("requests = (%d logins, %d reads), want none before validation", logins, reads)
	}
}

// TestInfisicalResolveClassifiesSecretStatusCodes pins the typed-error mapping
// for the secret read: 404 is ErrNotFound (also os.ErrNotExist) so missing
// secrets keep the local provider's semantics, 403 is ErrPermission, and
// anything else is ErrUnavailable. Response bodies are never echoed.
func TestInfisicalResolveClassifiesSecretStatusCodes(t *testing.T) {
	const bodySentinel = "response-body-sentinel"
	tests := []struct {
		name          string
		status        int
		body          string
		wantErr       error
		wantNotExists bool
	}{
		{
			"404 maps to ErrNotFound", http.StatusNotFound,
			`{"message":"` + bodySentinel + `"}`,
			ErrNotFound, true,
		},
		{
			"403 maps to ErrPermission", http.StatusForbidden,
			`{"message":"` + bodySentinel + `"}`,
			ErrPermission, false,
		},
		{
			"429 maps to ErrUnavailable", http.StatusTooManyRequests,
			bodySentinel, ErrUnavailable, false,
		},
		{
			"500 maps to ErrUnavailable", http.StatusInternalServerError,
			bodySentinel, ErrUnavailable, false,
		},
		{
			"502 maps to ErrUnavailable", http.StatusBadGateway,
			bodySentinel, ErrUnavailable, false,
		},
		{
			"200 without a secret maps to ErrUnavailable", http.StatusOK,
			`{"secret":null}`, ErrUnavailable, false,
		},
		{
			"200 with malformed JSON maps to ErrUnavailable", http.StatusOK,
			`not-json{{`, ErrUnavailable, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			})
			p := newInfisicalTestProvider(t, fake)

			got, err := p.Resolve(context.Background(), "missing-secret")
			if err == nil {
				t.Fatalf("Resolve = (%q, nil), want an error", got)
			}
			if got != "" {
				t.Fatalf("resolved value = %q, want empty on failure", got)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want it to wrap %v", err, tt.wantErr)
			}
			if tt.wantNotExists && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("error = %v, want it to wrap os.ErrNotExist", err)
			}
			if !strings.Contains(err.Error(), "missing-secret") {
				t.Fatalf("error = %v, want it to name the secret", err)
			}
			assertNoSecretMaterial(t, err.Error(), bodySentinel, infisicalTestToken)
		})
	}
}

// TestInfisicalResolveClassifiesLoginFailures pins the authentication
// classification: rejected Universal Auth credentials are ErrAuth, server or
// transport failures are ErrUnavailable, and a malformed login reply that
// carries no usable token is ErrAuth — all surfaced before any secret read.
func TestInfisicalResolveClassifiesLoginFailures(t *testing.T) {
	const bodySentinel = "login-body-sentinel"
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr error
	}{
		{"login 401 is ErrAuth", http.StatusUnauthorized, bodySentinel, ErrAuth},
		{"login 400 is ErrAuth", http.StatusBadRequest, bodySentinel, ErrAuth},
		{"login 403 is ErrAuth", http.StatusForbidden, bodySentinel, ErrAuth},
		{"login 422 is ErrAuth", http.StatusUnprocessableEntity, bodySentinel, ErrAuth},
		{
			"login 500 is ErrUnavailable",
			http.StatusInternalServerError, bodySentinel, ErrUnavailable,
		},
		{
			"login 503 is ErrUnavailable",
			http.StatusServiceUnavailable, bodySentinel, ErrUnavailable,
		},
		{"login without a token is ErrAuth", http.StatusOK, `{"expiresIn":900}`, ErrAuth},
		{
			"login with zero lifetime is ErrAuth",
			http.StatusOK, `{"accessToken":"x","expiresIn":0}`, ErrAuth,
		},
		{
			"login with malformed JSON is ErrUnavailable",
			http.StatusOK, `not-json{{`, ErrUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeInfisical(t, func(w http.ResponseWriter, _ recordedLogin) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}, nil)
			p := newInfisicalTestProvider(t, fake)

			got, err := p.Resolve(context.Background(), "api-token")
			if err == nil {
				t.Fatalf("Resolve = (%q, nil), want an error", got)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want it to wrap %v", err, tt.wantErr)
			}
			assertNoSecretMaterial(t, err.Error(),
				bodySentinel, infisicalTestClientID, infisicalTestClientSecret)
			if logins, reads := fake.requests.counts(); logins != 1 || reads != 0 {
				t.Fatalf("requests = (%d logins, %d reads), want (1, 0)", logins, reads)
			}
		})
	}
}

// TestInfisicalResolveReauthenticatesAfterRejectedToken pins that a token
// rejected between the token check and the secret read is invalidated and
// renewed exactly once, transparently to the caller.
func TestInfisicalResolveReauthenticatesAfterRejectedToken(t *testing.T) {
	var mu sync.Mutex
	logins := 0
	fake := newFakeInfisical(t,
		func(w http.ResponseWriter, _ recordedLogin) {
			mu.Lock()
			logins++
			n := logins
			mu.Unlock()
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"accessToken": fmt.Sprintf("token-%d", n),
				"expiresIn":   900,
			})
		},
		func(w http.ResponseWriter, r recordedSecretRead) {
			if r.auth == "Bearer token-1" {
				writeInfisicalJSON(w, http.StatusUnauthorized,
					map[string]any{"message": "access token expired"})
				return
			}
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secret": map[string]string{"secretValue": "post-reauth-value"},
			})
		},
	)
	p := newInfisicalTestProvider(t, fake)

	got := mustResolve(t, p, "api-token")
	if got != "post-reauth-value" {
		t.Fatalf("resolved = %q, want the post-reauth value", got)
	}
	if logins, reads := fake.requests.counts(); logins != 2 || reads != 2 {
		t.Fatalf("requests = (%d logins, %d reads), want (2, 2): one re-auth", logins, reads)
	}
}

// TestInfisicalResolveStopsAfterSecondTokenRejection pins that the
// re-authentication retry is bounded: a server that keeps rejecting the fresh
// token fails with ErrAuth after exactly one re-login (no retry loop), the
// failure is never cached, and a later resolve recovers once the server
// accepts the credentials again.
func TestInfisicalResolveStopsAfterSecondTokenRejection(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
		if reject.Load() {
			writeInfisicalJSON(w, http.StatusUnauthorized,
				map[string]any{"message": "rejected"})
			return
		}
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]string{"secretValue": "healed-value"},
		})
	})
	p := newInfisicalTestProvider(t, fake)

	_, err := p.Resolve(context.Background(), "api-token")
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("error = %v, want ErrAuth", err)
	}
	assertNoSecretMaterial(t, err.Error(), infisicalTestToken, infisicalTestClientSecret)
	if logins, reads := fake.requests.counts(); logins != 2 || reads != 2 {
		t.Fatalf("requests = (%d logins, %d reads), want (2, 2): one bounded re-auth",
			logins, reads)
	}

	reject.Store(false)
	if got := mustResolve(t, p, "api-token"); got != "healed-value" {
		t.Fatalf("recovered value = %q, want healed-value (failure must not be cached)", got)
	}
	if logins, reads := fake.requests.counts(); logins != 3 || reads != 3 {
		t.Fatalf("requests after recovery = (%d, %d), want (3, 3)", logins, reads)
	}
}

// TestInfisicalResolveHonorsContextCancellation pins the cancellation
// contract at three boundaries: an already-canceled context performs no
// request at all, a caller waiting on another caller's in-flight fetch
// returns its own context error instead of hanging, and a deadline expiring
// mid-read surfaces as the context error.
func TestInfisicalResolveHonorsContextCancellation(t *testing.T) {
	// gatedRead builds a fake whose secret endpoint blocks until released (or
	// until its own request context dies), signaling when the first read
	// arrives. Release is registered as cleanup so the server can always shut
	// down, even when a subtest fails first.
	gatedRead := func(t *testing.T) (chan struct{}, func(), *fakeInfisical) {
		t.Helper()
		entered := make(chan struct{}, 1)
		gate := make(chan struct{})
		unlock := sync.OnceFunc(func() { close(gate) })
		fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, r recordedSecretRead) {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-r.clientDone:
				return
			}
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secret": map[string]string{"secretValue": "gated-value"},
			})
		})
		t.Cleanup(unlock)
		return entered, unlock, fake
	}

	t.Run("canceled context performs no request", func(t *testing.T) {
		fake := newFakeInfisical(t, nil, nil)
		p := newInfisicalTestProvider(t, fake)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := p.Resolve(ctx, "api-token")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if logins, reads := fake.requests.counts(); logins != 0 || reads != 0 {
			t.Fatalf("requests = (%d, %d), want none", logins, reads)
		}
	})

	t.Run("waiting caller returns its own context error", func(t *testing.T) {
		entered, release, fake := gatedRead(t)
		p := newInfisicalTestProvider(t, fake)

		type result struct {
			value string
			err   error
		}
		leaderDone := make(chan result, 1)
		go func() {
			v, err := p.Resolve(context.Background(), "gated-secret")
			leaderDone <- result{v, err}
		}()
		<-entered

		waiterCtx, cancelWaiter := context.WithCancel(context.Background())
		waiterDone := make(chan result, 1)
		go func() {
			v, err := p.Resolve(waiterCtx, "gated-secret")
			waiterDone <- result{v, err}
		}()
		cancelWaiter()

		select {
		case res := <-waiterDone:
			if !errors.Is(res.err, context.Canceled) {
				t.Fatalf("waiter error = %v, want context.Canceled", res.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("waiter did not return after its context was canceled")
		}

		release()
		select {
		case res := <-leaderDone:
			if res.err != nil || res.value != "gated-value" {
				t.Fatalf("leader = (%q, %v), want the gated value", res.value, res.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("leader did not finish after the gate opened")
		}
	})

	t.Run("deadline during the secret read surfaces the context error", func(t *testing.T) {
		_, _, fake := gatedRead(t)
		p := newInfisicalTestProvider(t, fake)

		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := p.Resolve(ctx, "gated-secret")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded", err)
		}
	})
}

// TestInfisicalResolveSurfacesRequestTimeoutAsUnavailable pins that a hung
// endpoint bounded by the HTTP client's own timeout fails as ErrUnavailable
// rather than stalling the caller indefinitely.
func TestInfisicalResolveSurfacesRequestTimeoutAsUnavailable(t *testing.T) {
	gate := make(chan struct{})
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, r recordedSecretRead) {
		select {
		case <-gate:
		case <-r.clientDone:
			return
		}
	})
	t.Cleanup(sync.OnceFunc(func() { close(gate) }))

	client := &http.Client{
		Transport: fake.client().Transport,
		Timeout:   100 * time.Millisecond,
	}
	p := newInfisicalTestProvider(t, fake, func(o *InfisicalOptions) {
		o.HTTPClient = client
	})

	got, err := p.Resolve(context.Background(), "slow-secret")
	if err == nil {
		t.Fatalf("Resolve = (%q, nil), want a timeout error", got)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "slow-secret") {
		t.Fatalf("error = %v, want it to name the secret", err)
	}
}

// TestInfisicalResolveCachesUntilTTLThenRefreshes pins the cache contract:
// a resolved value is reused for the whole TTL window (no extra login, no
// extra read), an expired entry always refetches, and the rotated upstream
// value replaces the cached one — while the still-live access token is reused.
func TestInfisicalResolveCachesUntilTTLThenRefreshes(t *testing.T) {
	var mu sync.Mutex
	reads := 0
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
		mu.Lock()
		reads++
		n := reads
		mu.Unlock()
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]string{"secretValue": fmt.Sprintf("rotated-value-%d", n)},
		})
	})
	p := newInfisicalTestProvider(t, fake)
	clock := newTestClock()
	p.now = clock.Now

	if got := mustResolve(t, p, "api-token"); got != "rotated-value-1" {
		t.Fatalf("first resolve = %q, want rotated-value-1", got)
	}
	if got := mustResolve(t, p, "api-token"); got != "rotated-value-1" {
		t.Fatalf("cached resolve = %q, want the cached value", got)
	}
	if logins, reads := fake.requests.counts(); logins != 1 || reads != 1 {
		t.Fatalf("requests inside TTL = (%d, %d), want (1, 1): cache hit", logins, reads)
	}

	clock.Advance(DefaultInfisicalCacheTTL - time.Second)
	if got := mustResolve(t, p, "api-token"); got != "rotated-value-1" {
		t.Fatalf("resolve at TTL-1s = %q, want the cached value", got)
	}
	if logins, reads := fake.requests.counts(); logins != 1 || reads != 1 {
		t.Fatalf("requests at TTL-1s = (%d, %d), want (1, 1)", logins, reads)
	}

	clock.Advance(2 * time.Second)
	if got := mustResolve(t, p, "api-token"); got != "rotated-value-2" {
		t.Fatalf("resolve past TTL = %q, want the refreshed rotated value", got)
	}
	if logins, reads := fake.requests.counts(); logins != 1 || reads != 2 {
		t.Fatalf("requests past TTL = (%d, %d), want (1, 2): read refreshed, token reused",
			logins, reads)
	}
}

// TestInfisicalResolveFailedRefreshDoesNotServeStale pins that an expired
// cache entry is refreshed rather than served: if the refresh fails, the
// caller gets the typed error (never the stale value, and the error never
// quotes it), and the failure is not cached so a later resolve retries.
func TestInfisicalResolveFailedRefreshDoesNotServeStale(t *testing.T) {
	// modes: 0 healthy, 1 failing upstream, 2 healthy with a new value.
	var mode atomic.Int32
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
		switch mode.Load() {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "upstream down")
		case 2:
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secret": map[string]string{"secretValue": "fresh-value-after-recovery"},
			})
		default:
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secret": map[string]string{"secretValue": "stale-cache-value"},
			})
		}
	})
	p := newInfisicalTestProvider(t, fake)
	clock := newTestClock()
	p.now = clock.Now

	if got := mustResolve(t, p, "api-token"); got != "stale-cache-value" {
		t.Fatalf("first resolve = %q, want stale-cache-value", got)
	}

	mode.Store(1)
	clock.Advance(DefaultInfisicalCacheTTL + time.Second)
	got, err := p.Resolve(context.Background(), "api-token")
	if err == nil {
		t.Fatalf("stale refresh = (%q, nil), want an error", got)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
	if got != "" {
		t.Fatalf("value = %q, want empty: the stale value must not be served", got)
	}
	assertNoSecretMaterial(t, err.Error(), "stale-cache-value", "upstream down")

	mode.Store(2)
	if got := mustResolve(t, p, "api-token"); got != "fresh-value-after-recovery" {
		t.Fatalf("value after recovery = %q, want the fresh value", got)
	}
}

// TestInfisicalResolveCoalescesConcurrentMisses pins that an event burst
// resolving the same name concurrently produces exactly one login and one
// secret read: every caller shares the single in-flight fetch and observes
// the same value.
func TestInfisicalResolveCoalescesConcurrentMisses(t *testing.T) {
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	unlock := sync.OnceFunc(func() { close(gate) })
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, r recordedSecretRead) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-r.clientDone:
			return
		}
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]string{"secretValue": "burst-value"},
		})
	})
	t.Cleanup(unlock)
	p := newInfisicalTestProvider(t, fake)

	const callers = 8
	start := make(chan struct{})
	results := make(chan string, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			<-start
			v, err := p.Resolve(context.Background(), "burst-secret")
			results <- v
			errs <- err
		}()
	}
	close(start)
	<-entered
	// Give the remaining callers time to attach to the in-flight fetch; the
	// assertions hold either way, but this makes the coalescing path likely.
	time.Sleep(20 * time.Millisecond)
	unlock()

	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
		if v := <-results; v != "burst-value" {
			t.Fatalf("caller %d value = %q, want burst-value", i, v)
		}
	}
	if logins, reads := fake.requests.counts(); logins != 1 || reads != 1 {
		t.Fatalf("requests = (%d logins, %d reads), want (1, 1)", logins, reads)
	}
}

// TestInfisicalConcurrentRefreshAfterExpiryLogsInOnce pins race safety of the
// shared token and cache after both expire: concurrent fetches for distinct
// names perform exactly one new login (the token mutex serializes it), one
// refresh per name, and return each name's own value.
func TestInfisicalConcurrentRefreshAfterExpiryLogsInOnce(t *testing.T) {
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, r recordedSecretRead) {
		name := strings.TrimPrefix(r.path, infisicalSecretPrefix)
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]string{"secretValue": "value-" + name},
		})
	})
	p := newInfisicalTestProvider(t, fake)
	clock := newTestClock()
	p.now = clock.Now

	if got := mustResolve(t, p, "alpha"); got != "value-alpha" {
		t.Fatalf("first resolve = %q, want value-alpha", got)
	}

	// Past both the 5m cache TTL and the token's refreshed-at instant, so
	// every name must fetch while sharing a single fresh login.
	clock.Advance(15 * time.Minute)

	names := []string{"alpha", "beta", "gamma", "delta"}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, len(names)*2)
	for _, name := range names {
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				v, err := p.Resolve(context.Background(), name)
				if err == nil && v != "value-"+name {
					errs <- fmt.Errorf("Resolve(%q) = %q, want value-%s", name, v, name)
				}
				if err != nil {
					errs <- fmt.Errorf("Resolve(%q): %w", name, err)
				}
			}()
		}
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if logins, reads := fake.requests.counts(); logins != 2 || reads != 5 {
		t.Fatalf("requests = (%d logins, %d reads), want (2, 5): one initial plus one refresh",
			logins, reads)
	}
}

// TestInfisicalErrorsNeverLeakSecretMaterial pins the redaction contract of
// every failure path: neither the Universal Auth credentials, nor an issued
// access token, nor a previously resolved value may appear in an error —
// even when the server's response bodies echo them.
func TestInfisicalErrorsNeverLeakSecretMaterial(t *testing.T) {
	t.Run("login failure hides credentials", func(t *testing.T) {
		fake := newFakeInfisical(t, func(w http.ResponseWriter, _ recordedLogin) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w,
				`{"message":"rejected `+infisicalTestClientID+` `+infisicalTestClientSecret+`"}`)
		}, nil)
		p := newInfisicalTestProvider(t, fake)

		_, err := p.Resolve(context.Background(), "api-token")
		if err == nil {
			t.Fatal("Resolve = nil error, want ErrAuth")
		}
		if !errors.Is(err, ErrAuth) {
			t.Fatalf("error = %v, want ErrAuth", err)
		}
		assertNoSecretMaterial(t, err.Error(),
			infisicalTestClientID, infisicalTestClientSecret)
	})

	t.Run("secret failures hide credentials, tokens, and values", func(t *testing.T) {
		// modes: 0 healthy (a value is resolved and cached), 1 echo-everything
		// error responses, 2 persistent 401 re-auth loop.
		var mode atomic.Int32
		fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, r recordedSecretRead) {
			switch mode.Load() {
			case 1:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, fmt.Sprintf(
					`{"message":"denied for %s with %s using %s"}`,
					r.auth, infisicalTestClientSecret, infisicalTestValue,
				))
			case 2:
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, "rejected "+r.auth)
			default:
				writeInfisicalJSON(w, http.StatusOK, map[string]any{
					"secret": map[string]string{"secretValue": infisicalTestValue},
				})
			}
		})
		p := newInfisicalTestProvider(t, fake)

		if got := mustResolve(t, p, "api-token"); got != infisicalTestValue {
			t.Fatalf("resolved = %q, want the fixture value", got)
		}

		forbidden := []string{
			infisicalTestClientID,
			infisicalTestClientSecret,
			infisicalTestToken,
			"Bearer",
			infisicalTestValue,
		}

		mode.Store(1)
		for _, name := range []string{"broken-one", "broken-two"} {
			_, err := p.Resolve(context.Background(), name)
			if err == nil {
				t.Fatalf("Resolve(%q) = nil, want an error", name)
			}
			assertNoSecretMaterial(t, err.Error(), forbidden...)
		}

		mode.Store(2)
		_, err := p.Resolve(context.Background(), "rejected-secret")
		if !errors.Is(err, ErrAuth) {
			t.Fatalf("error = %v, want ErrAuth", err)
		}
		assertNoSecretMaterial(t, err.Error(), forbidden...)
	})
}

// TestInfisicalResolveRejectsHiddenValueWithoutCaching pins that
// secretValueHidden=true is a withheld value, never an empty success: the
// resolve fails with ErrPermission, the withheld bytes reach neither the
// caller nor the error, nothing is cached for the TTL, and a later visible
// response for the same name succeeds with its own value.
func TestInfisicalResolveRejectsHiddenValueWithoutCaching(t *testing.T) {
	// modes: 0 hides the value, 1 reveals it.
	var mode atomic.Int32
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
		if mode.Load() == 0 {
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secret": map[string]any{
					"secretValue":       "must-not-be-used",
					"secretValueHidden": true,
				},
			})
			return
		}
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]any{
				"secretValue":       "visible-value",
				"secretValueHidden": false,
			},
		})
	})
	p := newInfisicalTestProvider(t, fake)

	got, err := p.Resolve(context.Background(), "api-token")
	if err == nil {
		t.Fatalf("Resolve = (%q, nil), want an error for a hidden value", got)
	}
	if got != "" {
		t.Fatalf("resolved = %q, want empty: a hidden value is never a success", got)
	}
	if !errors.Is(err, ErrPermission) {
		t.Fatalf("error = %v, want ErrPermission", err)
	}
	if !strings.Contains(err.Error(), "did not reveal") {
		t.Fatalf("error = %v, want it to say the value was not revealed", err)
	}
	assertNoSecretMaterial(t, err.Error(), "must-not-be-used", infisicalTestToken)

	mode.Store(1)
	if next := mustResolve(t, p, "api-token"); next != "visible-value" {
		t.Fatalf("next resolve = %q, want visible-value (the hidden miss must not be cached)", next)
	}
	if logins, reads := fake.requests.counts(); logins != 1 || reads != 2 {
		t.Fatalf("requests = (%d logins, %d reads), want (1, 2)", logins, reads)
	}
}

// TestInfisicalResolveCachesVisibleEmptyValue pins that an explicitly visible
// empty secret stays a valid success: it resolves to "", and the empty value
// is cached like any other, so the next resolve is a hit rather than a
// refetch or an error.
func TestInfisicalResolveCachesVisibleEmptyValue(t *testing.T) {
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]any{
				"secretValue":       "",
				"secretValueHidden": false,
			},
		})
	})
	p := newInfisicalTestProvider(t, fake)

	got, err := p.Resolve(context.Background(), "empty-secret")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "" {
		t.Fatalf("resolved = %q, want the empty value", got)
	}
	got, err = p.Resolve(context.Background(), "empty-secret")
	if err != nil {
		t.Fatalf("second Resolve: %v", err)
	}
	if got != "" {
		t.Fatalf("cached resolve = %q, want the empty value", got)
	}
	if logins, reads := fake.requests.counts(); logins != 1 || reads != 1 {
		t.Fatalf("requests = (%d logins, %d reads), want (1, 1): empty value is a cache hit", logins, reads)
	}
}

// TestInfisicalResolveNotFoundDiagnosticNamesInfisical pins that a remote 404
// is reported as an Infisical miss (so it is never mistaken for a missing
// local secret file) while still unwrapping to os.ErrNotExist for callers
// that rely on the ErrNotFound alias.
func TestInfisicalResolveNotFoundDiagnosticNamesInfisical(t *testing.T) {
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"message":"`+infisicalTestValue+`"}`)
	})
	p := newInfisicalTestProvider(t, fake)

	got, err := p.Resolve(context.Background(), "absent-secret")
	if err == nil {
		t.Fatalf("Resolve = (%q, nil), want an error", got)
	}
	if got != "" {
		t.Fatalf("resolved = %q, want empty on a 404", got)
	}
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want it to unwrap to ErrNotFound/os.ErrNotExist", err)
	}
	if !strings.Contains(err.Error(), "Infisical") || !strings.Contains(err.Error(), "404") {
		t.Fatalf("error = %v, want it to name Infisical and the HTTP status", err)
	}
	if !strings.Contains(err.Error(), "absent-secret") {
		t.Fatalf("error = %v, want it to name the secret", err)
	}
	if strings.Contains(err.Error(), "file does not exist") {
		t.Fatalf("error borrowed the local-file diagnostic: %v", err)
	}
	assertNoSecretMaterial(t, err.Error(), infisicalTestValue, infisicalTestToken)
}

// TestInfisicalResolveWaiterRetriesWhenLeaderContextEnds pins the coalescing
// retry fix: when the leader's own context ends mid-fetch, the in-flight
// waiter with a live context retries once (as the new leader) and succeeds,
// instead of inheriting an unrelated cancellation — while the burst still
// produces exactly one login and one read per attempt, not a duplicate storm.
func TestInfisicalResolveWaiterRetriesWhenLeaderContextEnds(t *testing.T) {
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	unlock := sync.OnceFunc(func() { close(gate) })
	fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, r recordedSecretRead) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-r.clientDone:
			return
		}
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secret": map[string]string{"secretValue": "waiter-retry-value"},
		})
	})
	t.Cleanup(unlock)
	p := newInfisicalTestProvider(t, fake)

	type result struct {
		value string
		err   error
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan result, 1)
	go func() {
		v, err := p.Resolve(leaderCtx, "gated-secret")
		leaderDone <- result{v, err}
	}()
	// The leader now owns the single in-flight fetch for this name.
	<-entered

	waiterDone := make(chan result, 1)
	go func() {
		v, err := p.Resolve(context.Background(), "gated-secret")
		waiterDone <- result{v, err}
	}()

	cancelLeader()
	select {
	case res := <-leaderDone:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("leader error = %v, want context.Canceled", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not return after its context was canceled")
	}

	unlock()
	select {
	case res := <-waiterDone:
		if res.err != nil {
			t.Fatalf("waiter error = %v, want the retried fetch to succeed", res.err)
		}
		if res.value != "waiter-retry-value" {
			t.Fatalf("waiter value = %q, want waiter-retry-value", res.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not succeed after the leader's context ended")
	}

	if logins, reads := fake.requests.counts(); logins != 1 || reads != 2 {
		t.Fatalf("requests = (%d logins, %d reads), want (1, 2): one retry, no storm", logins, reads)
	}
}

// redirectTarget is a decoy endpoint the fake Infisical server redirects to;
// receiving anything at all would mean credentials left the configured origin.
type redirectTarget struct {
	*httptest.Server
	mu         sync.Mutex
	hits       int
	authHeader string
}

func (rt *redirectTarget) record(r *http.Request) {
	rt.mu.Lock()
	rt.hits++
	rt.authHeader = r.Header.Get("Authorization")
	rt.mu.Unlock()
}

func (rt *redirectTarget) received() (int, string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.hits, rt.authHeader
}

func newRedirectTarget(t *testing.T, useTLS bool) *redirectTarget {
	t.Helper()
	rt := &redirectTarget{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rt.record(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"secret":{"secretValue":"target-value"}}`)
	})
	if useTLS {
		rt.Server = httptest.NewTLSServer(handler)
	} else {
		rt.Server = httptest.NewServer(handler)
	}
	t.Cleanup(rt.Server.Close)
	return rt
}

// TestInfisicalResolveRefusesUnsafeRedirects pins that the redirect policy
// keeps the access token on the configured HTTPS origin: a redirect to a
// plain-http URL or to a different HTTPS host is refused before any request
// reaches the target, the resolve fails as ErrUnavailable, and the token
// appears neither at the target nor in the error.
func TestInfisicalResolveRefusesUnsafeRedirects(t *testing.T) {
	tests := []struct {
		name   string
		useTLS bool
	}{
		{"http scheme target", false},
		{"different https host target", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := newRedirectTarget(t, tt.useTLS)
			fake := newFakeInfisical(t, nil, func(w http.ResponseWriter, _ recordedSecretRead) {
				w.Header().Set("Location", target.URL+"/api/v4/secrets/api-token")
				w.WriteHeader(http.StatusFound)
			})
			p := newInfisicalTestProvider(t, fake)

			got, err := p.Resolve(context.Background(), "api-token")
			if err == nil {
				t.Fatalf("Resolve = (%q, nil), want the redirect to be refused", got)
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("error = %v, want ErrUnavailable", err)
			}
			if !strings.Contains(err.Error(), "refusing") {
				t.Fatalf("error = %v, want it to report the refusal", err)
			}
			assertNoSecretMaterial(t, err.Error(), infisicalTestToken)

			hits, authHeader := target.received()
			if hits != 0 {
				t.Fatalf("target received %d requests (Authorization=%q), want 0", hits, authHeader)
			}
		})
	}
}
