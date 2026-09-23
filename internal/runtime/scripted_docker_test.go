package runtime

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/moby/moby/client"
)

// scriptedDocker is an in-memory Docker Engine API used by unit tests that read
// daemon state but need no real daemon (image listing, container listing). It
// routes requests by method + path substring to a canned JSON body so the
// production client call path (URL building, response decoding) is exercised
// without httptest or a socket. Unmatched requests fail the test rather than
// silently returning an empty list.
type scriptedDocker struct {
	t      *testing.T
	routes []dockerRoute
}

type dockerRoute struct {
	method string // http.MethodGet / http.MethodHead; empty matches any
	path   string // path substring; empty matches any
	body   string
	status int // 0 means 200
	// onMatch, when set, runs once when the route matches. It lets a test count
	// or record calls (e.g. how many pulls were attempted) through the real
	// client call path.
	onMatch func()
}

func (s *scriptedDocker) RoundTrip(r *http.Request) (*http.Response, error) {
	for _, rt := range s.routes {
		if rt.method != "" && rt.method != r.Method {
			continue
		}
		if rt.path != "" && !strings.Contains(r.URL.Path, rt.path) {
			continue
		}
		if rt.onMatch != nil {
			rt.onMatch()
		}
		status := rt.status
		if status == 0 {
			status = http.StatusOK
		}
		body := rt.body
		if body == "" {
			body = "{}"
		}
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Request:    r,
		}, nil
	}
	// Fail loudly: an unscripted daemon call means the test's routing is wrong.
	s.t.Errorf("scriptedDocker: unhandled request %s %s", r.Method, r.URL.Path)
	return &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader(`{"message":"not found"}`)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    r,
	}, nil
}

// newScriptedDockerClient returns a client wired to the scripted routes. The
// ping route every Relay client issues first is always provided.
func newScriptedDockerClient(t *testing.T, routes ...dockerRoute) *client.Client {
	t.Helper()
	s := &scriptedDocker{t: t, routes: append([]dockerRoute{
		{method: http.MethodHead, path: "/_ping", body: ""},
		{method: http.MethodGet, path: "/_ping", body: "OK"},
	}, routes...)}
	cli, err := client.New(client.WithHTTPClient(&http.Client{Transport: s}))
	if err != nil {
		t.Fatalf("scripted docker client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// imageListJSON renders a minimal /images/json response for the given
// repo tag sets, mirroring the fields relayTags reads.
func imageListJSON(tags ...string) string {
	var items []string
	for _, tag := range tags {
		items = append(items, fmt.Sprintf(`{"RepoTags":[%q],"Id":"sha256:%s"}`, tag, strings.Repeat("0", 4)))
	}
	return "[" + strings.Join(items, ",") + "]"
}
