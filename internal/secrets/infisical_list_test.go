package secrets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// listResponse scripts a list-endpoint reply with an explicit status and raw
// body, so failure shapes (malformed JSON, omitted fields) are exercised
// exactly as an operator would see them from a misbehaving server.
func listResponse(status int, body string) func(w http.ResponseWriter, _ recordedList) {
	return func(w http.ResponseWriter, _ recordedList) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// TestInfisicalListSendsConfiguredScopeAndNamesOnly pins the full request
// contract of a successful listing: one login, then one GET on the collection
// endpoint carrying the configured project, environment, and secret path with
// values explicitly not requested — and only sorted names come back, never a
// secret value, even when the server sends one anyway. A later resolve still
// performs its own value read, so listing never feeds the value cache.
func TestInfisicalListSendsConfiguredScopeAndNamesOnly(t *testing.T) {
	const valueSentinel = "listed-value-sentinel"
	tests := []struct {
		name        string
		mutate      func(*InfisicalOptions)
		wantProject string
		wantEnv     string
		wantPath    string
	}{
		{
			"configured scope",
			nil,
			infisicalTestProject, infisicalTestEnvironment, infisicalTestSecretPath,
		},
		{
			"provider options pass through",
			func(o *InfisicalOptions) {
				o.ProjectID = "proj-other"
				o.Environment = "prod"
				o.SecretPath = "/team/app"
			},
			"proj-other", "prod", "/team/app",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeInfisical(t, nil, nil,
				func(w http.ResponseWriter, _ recordedList) {
					writeInfisicalJSON(w, http.StatusOK, map[string]any{
						"secrets": []map[string]any{
							{"secretKey": "zeta", "secretValue": valueSentinel},
							{"secretKey": "alpha", "secretValue": valueSentinel},
						},
					})
				})
			var mutate []func(*InfisicalOptions)
			if tt.mutate != nil {
				mutate = append(mutate, tt.mutate)
			}
			p := newInfisicalTestProvider(t, fake, mutate...)

			names, err := p.List(context.Background())
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if got := strings.Join(names, ","); got != "alpha,zeta" {
				t.Fatalf("names = %q, want sorted alpha,zeta", got)
			}
			for _, n := range names {
				if strings.Contains(n, valueSentinel) {
					t.Fatalf("listing returned a secret value as a name: %q", n)
				}
			}

			lists := fake.requests.listCalls()
			if len(lists) != 1 {
				t.Fatalf("list requests = %d, want 1", len(lists))
			}
			got := lists[0]
			if got.method != http.MethodGet || got.path != infisicalListPath {
				t.Fatalf("list request = %s %s, want GET %s",
					got.method, got.path, infisicalListPath)
			}
			if got.auth != "Bearer "+infisicalTestToken {
				t.Fatalf("Authorization = %q, want the issued bearer token", got.auth)
			}
			wantQuery := url.Values{
				"projectId":                {tt.wantProject},
				"environment":              {tt.wantEnv},
				"secretPath":               {tt.wantPath},
				"recursive":                {"false"},
				"includeImports":           {"true"},
				"includePersonalOverrides": {"false"},
				"viewSecretValue":          {"false"},
				"expandSecretReferences":   {"false"},
			}
			for key, want := range wantQuery {
				if v := got.query.Get(key); v != want[0] {
					t.Errorf("query %s = %q, want %q", key, v, want[0])
				}
			}
			if len(got.query) != len(wantQuery) {
				t.Errorf("query keys = %v, want exactly %v", got.query, wantQuery)
			}

			logins, reads := fake.requests.counts()
			if logins != 1 || reads != 0 {
				t.Fatalf("requests = (%d logins, %d reads), want (1, 0): names only",
					logins, reads)
			}

			if v := mustResolve(t, p, "alpha"); v != infisicalTestValue {
				t.Fatalf("resolve after list = %q, want the fixture value", v)
			}
			if _, readsAfter := fake.requests.counts(); readsAfter != 1 {
				t.Fatalf("reads after resolve = %d, want 1: listing must not "+
					"populate the value cache", readsAfter)
			}
		})
	}
}

// TestInfisicalListMergesImportsDeduplicatesAndSorts pins the result shape:
// names imported into the configured path are included alongside local ones,
// duplicates across both sources collapse to one entry, empty keys are
// dropped, and repeated listings of the same server state return the same
// sorted order.
func TestInfisicalListMergesImportsDeduplicatesAndSorts(t *testing.T) {
	fake := newFakeInfisical(t, nil, nil, func(w http.ResponseWriter, _ recordedList) {
		writeInfisicalJSON(w, http.StatusOK, map[string]any{
			"secrets": []map[string]any{
				{"secretKey": "zeta"},
				{"secretKey": ""},
				{"secretKey": "alpha"},
				{"secretKey": "mid"},
			},
			"imports": []map[string]any{
				{"secrets": []map[string]any{
					{"secretKey": "beta"},
					{"secretKey": "alpha"},
				}},
				{"secrets": []map[string]any{}},
				{"secrets": []map[string]any{
					{"secretKey": "zeta"},
					{"secretKey": "0day-imported"},
				}},
			},
		})
	})
	p := newInfisicalTestProvider(t, fake)

	want := "0day-imported,alpha,beta,mid,zeta"
	for i := 1; i <= 2; i++ {
		names, err := p.List(context.Background())
		if err != nil {
			t.Fatalf("List #%d: %v", i, err)
		}
		if got := strings.Join(names, ","); got != want {
			t.Fatalf("List #%d names = %q, want %q", i, got, want)
		}
	}

	logins, reads := fake.requests.counts()
	if logins != 1 || reads != 0 {
		t.Fatalf("requests = (%d logins, %d reads), want (1, 0)", logins, reads)
	}
	if lists := fake.requests.listCalls(); len(lists) != 2 {
		t.Fatalf("list requests = %d, want 2 (one per List call)", len(lists))
	}
}

// TestInfisicalListEmptyScopeReturnsEmptyListNotError pins that a scope with
// no secrets is a successful empty listing, not an error: `secret ls` can
// render the header-only table instead of failing an operator command.
func TestInfisicalListEmptyScopeReturnsEmptyListNotError(t *testing.T) {
	fake := newFakeInfisical(t, nil, nil, listResponse(http.StatusOK,
		`{"secrets":[],"imports":[]}`))
	p := newInfisicalTestProvider(t, fake)

	names, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List over an empty scope: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("names = %v, want an empty list", names)
	}
}

// TestInfisicalListClassifiesFailuresWithoutLeakingMaterial pins the typed
// error mapping of the listing path: rejected credentials and server/transport
// failures surface as ErrAuth/ErrUnavailable, 403 is ErrPermission, 404 is
// ErrNotFound naming the configured scope, and malformed or incomplete
// responses are ErrUnavailable. Every failure carries no names, performs no
// value read, and never echoes response bodies, tokens, or credentials.
func TestInfisicalListClassifiesFailuresWithoutLeakingMaterial(t *testing.T) {
	const bodySentinel = "list-body-sentinel"
	loginFailure := func(status int) func(w http.ResponseWriter, _ recordedLogin) {
		return func(w http.ResponseWriter, _ recordedLogin) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"message":"`+bodySentinel+`"}`)
		}
	}
	tests := []struct {
		name          string
		onLogin       func(w http.ResponseWriter, r recordedLogin)
		onList        func(w http.ResponseWriter, r recordedList)
		wantErr       error
		wantContains  string
		wantListCalls int
	}{
		{
			"rejected login is ErrAuth",
			loginFailure(http.StatusUnauthorized), nil, ErrAuth, "", 0,
		},
		{
			"login endpoint failure is ErrUnavailable",
			loginFailure(http.StatusInternalServerError), nil, ErrUnavailable, "", 0,
		},
		{
			"403 is ErrPermission",
			nil, listResponse(http.StatusForbidden,
				`{"message":"`+bodySentinel+`"}`),
			ErrPermission, "", 1,
		},
		{
			"404 is ErrNotFound naming the scope",
			nil, listResponse(http.StatusNotFound,
				`{"message":"`+bodySentinel+`"}`),
			ErrNotFound, "project, environment, or secret path", 1,
		},
		{
			"429 is ErrUnavailable",
			nil, listResponse(http.StatusTooManyRequests, bodySentinel),
			ErrUnavailable, "", 1,
		},
		{
			"500 is ErrUnavailable",
			nil, listResponse(http.StatusInternalServerError, bodySentinel),
			ErrUnavailable, "", 1,
		},
		{
			"malformed body is ErrUnavailable",
			nil, listResponse(http.StatusOK, "not-json{{"),
			ErrUnavailable, "", 1,
		},
		{
			"omitted secrets list is ErrUnavailable",
			nil, listResponse(http.StatusOK, `{"imports":[]}`),
			ErrUnavailable, "omitted the secrets list", 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeInfisical(t, tt.onLogin, nil, tt.onList)
			p := newInfisicalTestProvider(t, fake)

			names, err := p.List(context.Background())
			if err == nil {
				t.Fatalf("List = (%v, nil), want an error", names)
			}
			if len(names) != 0 {
				t.Fatalf("names = %v, want none on failure", names)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want it to wrap %v", err, tt.wantErr)
			}
			if tt.wantContains != "" && !strings.Contains(err.Error(), tt.wantContains) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantContains)
			}
			assertNoSecretMaterial(t, err.Error(), bodySentinel,
				infisicalTestToken, infisicalTestClientID, infisicalTestClientSecret)

			logins, reads := fake.requests.counts()
			if reads != 0 {
				t.Fatalf("value reads = %d, want 0: listing never reads values", reads)
			}
			if logins != 1 {
				t.Fatalf("logins = %d, want 1", logins)
			}
			if lists := fake.requests.listCalls(); len(lists) != tt.wantListCalls {
				t.Fatalf("list requests = %d, want %d", len(lists), tt.wantListCalls)
			}
		})
	}
}

// TestInfisicalListReauthenticatesOnceAfterRejectedToken pins that a token
// rejected between login and the listing is invalidated and renewed exactly
// once, transparently to the caller: the second attempt succeeds with the
// fresh token and no value read ever happens.
func TestInfisicalListReauthenticatesOnceAfterRejectedToken(t *testing.T) {
	var loginCount atomic.Int32
	fake := newFakeInfisical(t,
		func(w http.ResponseWriter, _ recordedLogin) {
			n := loginCount.Add(1)
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"accessToken": fmt.Sprintf("token-%d", n),
				"expiresIn":   900,
			})
		},
		nil,
		func(w http.ResponseWriter, r recordedList) {
			if r.auth == "Bearer token-1" {
				writeInfisicalJSON(w, http.StatusUnauthorized,
					map[string]any{"message": "access token expired"})
				return
			}
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secrets": []map[string]any{{"secretKey": "alpha"}},
			})
		})
	p := newInfisicalTestProvider(t, fake)

	names, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := strings.Join(names, ","); got != "alpha" {
		t.Fatalf("names = %q, want alpha", got)
	}
	if logins, reads := fake.requests.counts(); logins != 2 || reads != 0 {
		t.Fatalf("requests = (%d logins, %d reads), want (2, 0): one re-auth",
			logins, reads)
	}
	lists := fake.requests.listCalls()
	if len(lists) != 2 || lists[1].auth != "Bearer token-2" {
		t.Fatalf("list attempts = %+v, want a second call with the fresh token", lists)
	}
}

// TestInfisicalListStopsAfterSecondTokenRejection pins that the listing's
// re-authentication is bounded: a server that keeps rejecting fresh tokens
// fails with ErrAuth after exactly one re-login, the failure is never cached
// (a later listing recovers once the server accepts the credentials), and no
// token or credential material appears in any error.
func TestInfisicalListStopsAfterSecondTokenRejection(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	fake := newFakeInfisical(t, nil, nil,
		func(w http.ResponseWriter, r recordedList) {
			if reject.Load() {
				writeInfisicalJSON(w, http.StatusUnauthorized,
					map[string]any{"message": "rejected " + r.auth})
				return
			}
			writeInfisicalJSON(w, http.StatusOK, map[string]any{
				"secrets": []map[string]any{{"secretKey": "alpha"}},
			})
		})
	p := newInfisicalTestProvider(t, fake)

	_, err := p.List(context.Background())
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("error = %v, want ErrAuth", err)
	}
	if !strings.Contains(err.Error(), "listing secrets") {
		t.Fatalf("error = %v, want it to name the listing operation", err)
	}
	assertNoSecretMaterial(t, err.Error(),
		infisicalTestClientSecret, "Bearer", "token-1")
	if logins, reads := fake.requests.counts(); logins != 2 || reads != 0 {
		t.Fatalf("requests = (%d logins, %d reads), want (2, 0): one bounded re-auth",
			logins, reads)
	}
	if lists := fake.requests.listCalls(); len(lists) != 2 {
		t.Fatalf("list requests = %d, want 2", len(lists))
	}

	reject.Store(false)
	names, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("recovered List: %v", err)
	}
	if got := strings.Join(names, ","); got != "alpha" {
		t.Fatalf("recovered names = %q, want alpha (failure must not be cached)", got)
	}
	if logins, reads := fake.requests.counts(); logins != 3 || reads != 0 {
		t.Fatalf("requests after recovery = (%d logins, %d reads), want (3, 0)",
			logins, reads)
	}
}

// TestInfisicalListHonorsCanceledContextWithoutRequests pins that listing with
// an already-canceled context surfaces the context error before any network
// traffic: no login and no list request happen.
func TestInfisicalListHonorsCanceledContextWithoutRequests(t *testing.T) {
	fake := newFakeInfisical(t, nil, nil)
	p := newInfisicalTestProvider(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := p.List(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if logins, reads := fake.requests.counts(); logins != 0 || reads != 0 {
		t.Fatalf("requests = (%d logins, %d reads), want none", logins, reads)
	}
	if lists := fake.requests.listCalls(); len(lists) != 0 {
		t.Fatalf("list requests = %d, want none", len(lists))
	}
}

// TestInfisicalListHandlesLargeUnpaginatedResponse pins the larger bounded
// response allowance for the v4 list endpoint, which does not support
// pagination. A valid scope with many names must not be mistaken for an
// unavailable provider merely because its response exceeds the secret-read
// limit.
func TestInfisicalListHandlesLargeUnpaginatedResponse(t *testing.T) {
	const secretCount = 20_000
	var response strings.Builder
	response.WriteString(`{"secrets":[`)
	for i := 0; i < secretCount; i++ {
		if i > 0 {
			response.WriteByte(',')
		}
		fmt.Fprintf(
			&response,
			`{"secretKey":"%s-%05d"}`,
			strings.Repeat("a", 40), i,
		)
	}
	response.WriteString(`],"imports":[]}`)
	if response.Len() <= infisicalMaxResponseBytes {
		t.Fatalf("fixture size = %d, want greater than %d",
			response.Len(), infisicalMaxResponseBytes)
	}

	fake := newFakeInfisical(t, nil, nil,
		listResponse(http.StatusOK, response.String()))
	p := newInfisicalTestProvider(t, fake)

	names, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List of large scope: %v", err)
	}
	if len(names) != secretCount {
		t.Fatalf("names = %d, want %d", len(names), secretCount)
	}
}

// TestDecodeInfisicalJSONLimitRejectsOversizedTrailingData pins the decoder's
// strict byte bound even when the first JSON value is complete. This prevents
// a large list response from hiding excess data after the parsed object.
func TestDecodeInfisicalJSONLimitRejectsOversizedTrailingData(t *testing.T) {
	var body struct {
		Value string `json:"value"`
	}
	raw := `{"value":"ok"}` + strings.Repeat(" ", 64)

	err := decodeInfisicalJSONLimit(strings.NewReader(raw), &body, 16)
	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("decode error = %v, want a size-limit error", err)
	}
	if body.Value != "ok" {
		t.Fatalf("decoded value = %q, want the first JSON value", body.Value)
	}
}
