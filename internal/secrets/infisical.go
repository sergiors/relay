package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Infisical provider defaults. The cache window bounds how long a successfully
// resolved value is reused; an expired entry is always refreshed rather than
// serving a stale value. The request timeout bounds each HTTP exchange on top of
// the caller's context, so a hung Infisical endpoint cannot stall an invocation
// indefinitely.
const (
	DefaultInfisicalSiteURL        = "https://app.infisical.com"
	DefaultInfisicalSecretPath     = "/"
	DefaultInfisicalCacheTTL       = 5 * time.Minute
	DefaultInfisicalRequestTimeout = 10 * time.Second

	infisicalMaxResponseBytes     = 1 << 20
	infisicalMaxListResponseBytes = 16 << 20
)

// Typed provider failures. ErrNotFound aliases os.ErrNotExist so callers that
// already distinguish a missing local secret keep working unchanged.
var (
	// ErrNotFound reports that the requested secret does not exist.
	ErrNotFound = os.ErrNotExist
	// ErrPermission reports that the provider identity cannot read the secret.
	ErrPermission = errors.New("secret permission denied")
	// ErrAuth reports that the provider could not authenticate.
	ErrAuth = errors.New("secret provider authentication failed")
	// ErrUnavailable reports a transport, server, or malformed-response
	// failure. It never implies the secret exists or is absent.
	ErrUnavailable = errors.New("secret provider unavailable")
)

// errInfisicalUnauthorized is an internal signal used to re-authenticate once
// after an access token expires between the token check and the secret read.
var errInfisicalUnauthorized = errors.New("infisical: unauthorized")

// infisicalNotFoundError reports a missing remote secret without claiming a
// local file is absent. It unwraps to ErrNotFound, so callers that use
// errors.Is(err, os.ErrNotExist) keep working unchanged.
type infisicalNotFoundError struct {
	name string
}

func (e *infisicalNotFoundError) Error() string {
	return fmt.Sprintf("secret %q not found in Infisical (HTTP 404)", e.name)
}

func (e *infisicalNotFoundError) Unwrap() error {
	return ErrNotFound
}

// InfisicalOptions configures an InfisicalProvider. SiteURL and SecretPath
// default when empty. ProjectID, Environment, ClientID, and ClientSecret are
// required. CacheTTL defaults to DefaultInfisicalCacheTTL and is primarily
// useful to shorten the window in tests.
//
// HTTPClient is optional and exists so tests can trust an httptest TLS server.
// Production callers leave it nil. Any supplied client is expected to keep
// standard TLS certificate verification; the provider never disables it.
type InfisicalOptions struct {
	SiteURL      string
	ProjectID    string
	Environment  string
	SecretPath   string
	ClientID     string
	ClientSecret string
	CacheTTL     time.Duration
	HTTPClient   *http.Client
}

// InfisicalProvider resolves secret names from an Infisical project using
// Universal Auth. One instance is intended to live for the whole worker and be
// shared by the runner, service reconciliation, and the Git webhook. It keeps
// its access token and a TTL cache per instance, and coalesces concurrent
// requests for the same name so an event burst cannot fan out into duplicate
// Infisical calls.
type InfisicalProvider struct {
	baseURL      *url.URL
	projectID    string
	environment  string
	secretPath   string
	clientID     string
	clientSecret string
	cacheTTL     time.Duration
	client       *http.Client
	now          func() time.Time

	tokenMu        sync.Mutex
	token          string
	tokenExpiresAt time.Time

	cacheMu sync.Mutex
	cache   map[string]*infisicalCacheEntry
}

// infisicalCacheEntry holds either a cached value or, while a fetch is in
// flight, the shared call its waiters observe.
type infisicalCacheEntry struct {
	value     string
	expiresAt time.Time
	call      *infisicalCall
}

// infisicalCall is one in-flight fetch. done is closed after value, err, and
// retry are written, so waiters can read them without further synchronization.
// retry marks a fetch that failed only because the leader's own context ended;
// waiters with a live context then attempt the fetch themselves instead of
// inheriting an unrelated cancellation.
type infisicalCall struct {
	done  chan struct{}
	value string
	err   error
	retry bool
}

// NewInfisicalProvider validates its options and returns a provider. It does
// not perform a network login: credential or availability failures surface as
// typed Resolve errors, exactly like a missing local secret surfaces per
// invocation. Configuration errors (unsafe URL, missing settings) fail here so
// worker startup can report them.
func NewInfisicalProvider(opts InfisicalOptions) (*InfisicalProvider, error) {
	siteURL := strings.TrimRight(strings.TrimSpace(opts.SiteURL), "/")
	if siteURL == "" {
		siteURL = DefaultInfisicalSiteURL
	}
	baseURL, err := parseInfisicalSiteURL(siteURL)
	if err != nil {
		return nil, err
	}

	projectID := strings.TrimSpace(opts.ProjectID)
	if projectID == "" {
		return nil, errors.New("secrets: Infisical project ID is required")
	}
	environment := strings.TrimSpace(opts.Environment)
	if environment == "" {
		return nil, errors.New("secrets: Infisical environment is required")
	}
	clientID := strings.TrimSpace(opts.ClientID)
	if clientID == "" {
		return nil, errors.New("secrets: Infisical client ID is required")
	}
	clientSecret := opts.ClientSecret
	if strings.TrimSpace(clientSecret) == "" {
		return nil, errors.New("secrets: Infisical client secret is required")
	}

	secretPath := strings.TrimSpace(opts.SecretPath)
	if secretPath == "" {
		secretPath = DefaultInfisicalSecretPath
	}
	if !strings.HasPrefix(secretPath, "/") {
		return nil, errors.New("secrets: Infisical secret path must begin with '/'")
	}

	cacheTTL := opts.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = DefaultInfisicalCacheTTL
	}

	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{}
	} else {
		// Copy so enforcing the timeout and redirect policy never mutates a
		// caller-owned client (tests inject one to trust a TLS test server).
		copied := *client
		client = &copied
	}
	if client.Timeout == 0 {
		client.Timeout = DefaultInfisicalRequestTimeout
	}
	client.CheckRedirect = infisicalRedirectPolicy

	return &InfisicalProvider{
		baseURL:      baseURL,
		projectID:    projectID,
		environment:  environment,
		secretPath:   secretPath,
		clientID:     clientID,
		clientSecret: clientSecret,
		cacheTTL:     cacheTTL,
		client:       client,
		now:          time.Now,
		cache:        make(map[string]*infisicalCacheEntry),
	}, nil
}

// parseInfisicalSiteURL requires an absolute https URL without embedded
// credentials. The raw input is never echoed because url.Parse includes it in
// its error, and an operator may accidentally have put credentials there.
func parseInfisicalSiteURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Host == "" || u.User != nil {
		return nil, errors.New(
			"secrets: Infisical site URL must be an absolute https:// URL without credentials",
		)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New(
			"secrets: Infisical site URL must not contain a query or fragment",
		)
	}
	return u, nil
}

// infisicalRedirectPolicy keeps every hop on HTTPS and on the same host, so an
// access token is never forwarded in cleartext or to another origin.
func infisicalRedirectPolicy(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if !strings.EqualFold(req.URL.Scheme, "https") || req.URL.User != nil {
		return errors.New("refusing non-HTTPS redirect")
	}
	if req.URL.Host != via[0].URL.Host {
		return errors.New("refusing redirect to another host")
	}
	return nil
}

// Resolve implements Provider. A cache hit is returned only while its TTL is
// still open; an expired entry is refreshed, and a failed refresh returns the
// error rather than the stale value. Concurrent misses for the same name share
// one in-flight fetch.
func (p *InfisicalProvider) Resolve(ctx context.Context, name string) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}

	for {
		p.cacheMu.Lock()
		entry := p.cache[name]
		if entry == nil {
			call := &infisicalCall{done: make(chan struct{})}
			p.cache[name] = &infisicalCacheEntry{call: call}
			p.cacheMu.Unlock()
			return p.load(ctx, name, call)
		}
		if entry.call != nil {
			call := entry.call
			p.cacheMu.Unlock()
			select {
			case <-call.done:
				if err := ctx.Err(); err != nil {
					return "", err
				}
				if call.retry {
					continue
				}
				return call.value, call.err
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		if p.now().Before(entry.expiresAt) {
			value := entry.value
			p.cacheMu.Unlock()
			return value, nil
		}
		delete(p.cache, name)
		p.cacheMu.Unlock()
	}
}

// List implements Lister. It reads the configured project, environment, and
// secret path in one request: the v4 list endpoint has no pagination
// parameters and returns every matching secret. Values are explicitly not
// requested, and the per-instance value cache is never consulted or populated
// by listing.
func (p *InfisicalProvider) List(ctx context.Context) ([]string, error) {
	token, err := p.accessToken(ctx)
	if err != nil {
		return nil, err
	}

	names, err := p.fetchSecretNames(ctx, token)
	if errors.Is(err, errInfisicalUnauthorized) {
		p.invalidateToken(token)
		token, err = p.accessToken(ctx)
		if err != nil {
			return nil, err
		}
		names, err = p.fetchSecretNames(ctx, token)
		if errors.Is(err, errInfisicalUnauthorized) {
			p.invalidateToken(token)
			return nil, fmt.Errorf(
				"%w: Infisical rejected the access token while listing secrets",
				ErrAuth,
			)
		}
	}
	return names, err
}

// infisicalListSecret is the only part of the list response Relay reads. It
// deliberately omits secretValue: listing requests names only.
type infisicalListSecret struct {
	SecretKey string `json:"secretKey"`
}

// fetchSecretNames reads the names in the configured scope. recursive=false
// keeps the listing to the exact configured path; includeImports=true also
// returns secrets imported into that path, which Resolve can resolve there.
// Personal overrides are excluded, matching Resolve's shared-secret lookup.
func (p *InfisicalProvider) fetchSecretNames(ctx context.Context, token string) ([]string, error) {
	query := url.Values{}
	query.Set("projectId", p.projectID)
	query.Set("environment", p.environment)
	query.Set("secretPath", p.secretPath)
	query.Set("recursive", "false")
	query.Set("includeImports", "true")
	query.Set("includePersonalOverrides", "false")
	query.Set("viewSecretValue", "false")
	query.Set("expandSecretReferences", "false")

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		p.endpoint("/api/v4/secrets", query),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w: build request", ErrUnavailable)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := p.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("list secrets: %w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return nil, errInfisicalUnauthorized
	case http.StatusForbidden:
		return nil, fmt.Errorf("list secrets: %w (HTTP 403)", ErrPermission)
	case http.StatusNotFound:
		return nil, fmt.Errorf(
			"list secrets: %w: Infisical project, environment, or secret path not found (HTTP 404)",
			ErrNotFound,
		)
	default:
		return nil, fmt.Errorf(
			"list secrets: %w (HTTP %d)", ErrUnavailable, resp.StatusCode,
		)
	}

	var body struct {
		Secrets *[]infisicalListSecret `json:"secrets"`
		Imports []struct {
			Secrets []infisicalListSecret `json:"secrets"`
		} `json:"imports"`
	}
	if err := decodeInfisicalJSONLimit(
		resp.Body, &body, infisicalMaxListResponseBytes,
	); err != nil {
		return nil, fmt.Errorf(
			"list secrets: %w: decode response: %v", ErrUnavailable, err,
		)
	}
	if body.Secrets == nil {
		return nil, fmt.Errorf(
			"list secrets: %w: response omitted the secrets list", ErrUnavailable,
		)
	}

	seen := make(map[string]struct{}, len(*body.Secrets))
	names := make([]string, 0, len(*body.Secrets))
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, secret := range *body.Secrets {
		add(secret.SecretKey)
	}
	for _, imported := range body.Imports {
		for _, secret := range imported.Secrets {
			add(secret.SecretKey)
		}
	}
	sort.Strings(names)
	return names, nil
}

// load is the single leader for one name. It runs the network fetch and then
// either installs a fresh cache entry or removes the failed placeholder before
// waking waiters. A fetch that stopped only because the leader's context ended
// is not cached and is marked for waiters to retry with their own context.
func (p *InfisicalProvider) load(
	ctx context.Context, name string, call *infisicalCall,
) (string, error) {
	value, err := p.fetch(ctx, name)
	retry := err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err())

	p.cacheMu.Lock()
	if current := p.cache[name]; current != nil && current.call == call {
		switch {
		case retry:
			delete(p.cache, name)
		case err == nil:
			p.cache[name] = &infisicalCacheEntry{
				value:     value,
				expiresAt: p.now().Add(p.cacheTTL),
			}
		default:
			delete(p.cache, name)
		}
	}
	call.value = value
	call.err = err
	call.retry = retry
	close(call.done)
	p.cacheMu.Unlock()

	return value, err
}

// fetch obtains a token and reads the secret, re-authenticating once if the
// token was rejected. A second rejection is reported as ErrAuth.
func (p *InfisicalProvider) fetch(ctx context.Context, name string) (string, error) {
	token, err := p.accessToken(ctx)
	if err != nil {
		return "", err
	}

	value, err := p.fetchSecret(ctx, token, name)
	if errors.Is(err, errInfisicalUnauthorized) {
		p.invalidateToken(token)
		token, err = p.accessToken(ctx)
		if err != nil {
			return "", err
		}
		value, err = p.fetchSecret(ctx, token, name)
		if errors.Is(err, errInfisicalUnauthorized) {
			p.invalidateToken(token)
			return "", fmt.Errorf(
				"secret %q: %w: Infisical rejected the access token",
				name, ErrAuth,
			)
		}
	}
	return value, err
}

// accessToken returns a live access token, logging in at most once for all
// concurrent callers that observe an expired or missing token.
func (p *InfisicalProvider) accessToken(ctx context.Context) (string, error) {
	p.tokenMu.Lock()
	defer p.tokenMu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p.token != "" && p.now().Before(p.tokenExpiresAt) {
		return p.token, nil
	}
	p.token = ""
	p.tokenExpiresAt = time.Time{}

	token, expiresAt, err := p.login(ctx)
	if err != nil {
		return "", err
	}
	p.token = token
	p.tokenExpiresAt = expiresAt
	return token, nil
}

// invalidateToken clears the current token only when it is still the one that
// was rejected, so a concurrent successful re-authentication is not discarded.
func (p *InfisicalProvider) invalidateToken(rejected string) {
	p.tokenMu.Lock()
	if p.token == rejected {
		p.token = ""
		p.tokenExpiresAt = time.Time{}
	}
	p.tokenMu.Unlock()
}

type infisicalLoginResponse struct {
	AccessToken string  `json:"accessToken"`
	ExpiresIn   float64 `json:"expiresIn"`
}

// login performs Universal Auth. The credentials are sent only in the request
// body; neither they nor the returned token appear in any error or log line.
func (p *InfisicalProvider) login(ctx context.Context) (string, time.Time, error) {
	payload, err := json.Marshal(struct {
		ClientID     string `json:"clientId"`
		ClientSecret string `json:"clientSecret"`
	}{
		ClientID:     p.clientID,
		ClientSecret: p.clientSecret,
	})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%w: encode login request", ErrUnavailable)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.endpoint("/api/v1/auth/universal-auth/login", nil),
		bytes.NewReader(payload),
	)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%w: build login request", ErrUnavailable)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", time.Time{}, ctxErr
		}
		return "", time.Time{}, fmt.Errorf("%w: authenticate: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if infisicalAuthStatus(resp.StatusCode) {
			return "", time.Time{}, fmt.Errorf(
				"%w: Infisical rejected the Universal Auth credentials (HTTP %d)",
				ErrAuth, resp.StatusCode,
			)
		}
		return "", time.Time{}, fmt.Errorf(
			"%w: authentication endpoint returned HTTP %d",
			ErrUnavailable, resp.StatusCode,
		)
	}

	var body infisicalLoginResponse
	if err := decodeInfisicalJSON(resp.Body, &body); err != nil {
		return "", time.Time{}, fmt.Errorf(
			"%w: decode authentication response: %v", ErrUnavailable, err,
		)
	}
	if body.AccessToken == "" || body.ExpiresIn <= 0 {
		return "", time.Time{}, fmt.Errorf(
			"%w: authentication response did not contain a usable token", ErrAuth,
		)
	}

	expiresIn := time.Duration(body.ExpiresIn * float64(time.Second))
	if expiresIn <= 0 {
		return "", time.Time{}, fmt.Errorf(
			"%w: authentication response contained an invalid token lifetime", ErrAuth,
		)
	}
	return body.AccessToken, infisicalTokenRefreshAt(p.now(), expiresIn), nil
}

// infisicalTokenRefreshAt returns the instant after which the token must be
// renewed. A token is refreshed ahead of expiry to avoid a request racing the
// server's clock; short-lived tokens refresh proportionally earlier.
func infisicalTokenRefreshAt(now time.Time, expiresIn time.Duration) time.Time {
	skew := expiresIn / 10
	if skew > 30*time.Second {
		skew = 30 * time.Second
	}
	return now.Add(expiresIn - skew)
}

// fetchSecret reads one secret by name from the configured project,
// environment, and path. It maps each HTTP status onto the package's typed
// errors and never includes a response body in an error.
func (p *InfisicalProvider) fetchSecret(ctx context.Context, token, name string) (string, error) {
	query := url.Values{}
	query.Set("projectId", p.projectID)
	query.Set("environment", p.environment)
	query.Set("secretPath", p.secretPath)
	query.Set("type", "shared")
	query.Set("viewSecretValue", "true")
	query.Set("expandSecretReferences", "true")
	query.Set("includeImports", "true")

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		p.endpoint("/api/v4/secrets/"+name, query),
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("resolve secret %q: %w: build request", name, ErrUnavailable)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := p.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", fmt.Errorf("resolve secret %q: %w: %v", name, ErrUnavailable, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return "", errInfisicalUnauthorized
	case http.StatusForbidden:
		return "", fmt.Errorf("secret %q: %w (HTTP 403)", name, ErrPermission)
	case http.StatusNotFound:
		return "", &infisicalNotFoundError{name: name}
	default:
		return "", fmt.Errorf(
			"resolve secret %q: %w (HTTP %d)", name, ErrUnavailable, resp.StatusCode,
		)
	}

	var body struct {
		Secret *struct {
			SecretValue       string `json:"secretValue"`
			SecretValueHidden bool   `json:"secretValueHidden"`
		} `json:"secret"`
	}
	if err := decodeInfisicalJSON(resp.Body, &body); err != nil {
		return "", fmt.Errorf(
			"resolve secret %q: %w: decode response: %v", name, ErrUnavailable, err,
		)
	}
	if body.Secret == nil {
		return "", fmt.Errorf(
			"resolve secret %q: %w: response omitted the secret", name, ErrUnavailable,
		)
	}
	// A hidden value is withheld rather than absent; reporting it as an empty
	// string would cache a wrong value for the TTL and let a handler run with
	// a credential it never received. A visible empty value stays valid, since
	// the local provider also permits empty secrets.
	if body.Secret.SecretValueHidden {
		return "", fmt.Errorf(
			"secret %q: %w: Infisical did not reveal the secret value",
			name, ErrPermission,
		)
	}
	return body.Secret.SecretValue, nil
}

// endpoint builds an absolute API URL from the configured site base. The secret
// name is already constrained by ValidateName to a safe single path segment.
func (p *InfisicalProvider) endpoint(path string, query url.Values) string {
	u := *p.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + path
	u.RawPath = ""
	u.RawQuery = query.Encode()
	return u.String()
}

// decodeInfisicalJSON bounds response parsing so a misbehaving endpoint cannot
// force an unbounded read.
func decodeInfisicalJSON(r io.Reader, dst any) error {
	return decodeInfisicalJSONLimit(r, dst, infisicalMaxResponseBytes)
}

func decodeInfisicalJSONLimit(r io.Reader, dst any, maxBytes int64) error {
	limited := &io.LimitedReader{R: r, N: maxBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	if limited.N == 0 {
		return errors.New("response exceeds size limit")
	}
	return nil
}

// infisicalAuthStatus reports whether a login status indicates rejected
// credentials rather than a network or server-side failure.
func infisicalAuthStatus(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusUnprocessableEntity:
		return true
	default:
		return false
	}
}

// ensure InfisicalProvider satisfies Provider and Lister at compile time.
var (
	_ Provider = (*InfisicalProvider)(nil)
	_ Lister   = (*InfisicalProvider)(nil)
)
