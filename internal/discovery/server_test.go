package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/state"
	"relay/internal/testutil"
)

const (
	conflictMsg = "Discovery: duplicate route conflict"
	resolvedMsg = "Discovery: duplicate route conflict resolved"
	readFailMsg = "Discovery: read service registry failed"
	traefikPath = "/discovery/traefik"
)

// readerFunc adapts a function to the EndpointReader seam so tests can script
// registry reads without a state fixture.
type readerFunc func(context.Context, state.ServiceRegistryFilter) (
	[]state.ServiceRegistryEndpoint, error)

func (f readerFunc) ReadEligibleServiceRegistryEndpoints(
	ctx context.Context, filter state.ServiceRegistryFilter,
) ([]state.ServiceRegistryEndpoint, error) {
	return f(ctx, filter)
}

// The worker hands its *state.State handle directly to NewServer; pin that the
// persistence seam is satisfied by the real implementation at compile time.
var _ EndpointReader = (*state.State)(nil)

// registryEndpoint builds a state registry row carrying both the routing facts
// and the persistence-only details that must never reach a response body.
func registryEndpoint(
	app, service, host, path, addr string, hostPort int,
) state.ServiceRegistryEndpoint {
	return state.ServiceRegistryEndpoint{
		App: app, Service: service, Host: host, Path: path,
		AdvertiseAddr: addr, HostPort: hostPort,
	}
}

// logRecord captures one structured log emission for assertions.
type logRecord struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

// recordingLogHandler is a concurrency-safe slog handler that keeps every
// record in memory.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []logRecord
}

func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, logRecord{
		level: r.Level, msg: r.Message, attrs: attrs,
	})
	return nil
}

func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingLogHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingLogHandler) snapshot() []logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]logRecord, len(h.records))
	copy(out, h.records)
	return out
}

// matching returns the recorded messages equal to msg, optionally filtered by
// their "state" attribute.
func (h *recordingLogHandler) matching(msg, state string) []logRecord {
	var out []logRecord
	for _, record := range h.snapshot() {
		if record.msg != msg {
			continue
		}
		if state != "" && record.attrs["state"] != state {
			continue
		}
		out = append(out, record)
	}
	return out
}

// newTestServer builds a server bound to a throwaway address; handler-level
// tests never start it.
func newTestServer(t *testing.T, reader EndpointReader, logger *slog.Logger) *Server {
	t.Helper()
	s := NewServer("127.0.0.1:0", reader, logger)
	if s == nil {
		t.Fatal("NewServer with a non-empty address returned nil")
	}
	return s
}

// pollTraefik issues one GET through the assembled handler.
func pollTraefik(t *testing.T, s *Server) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, traefikPath, nil)
	s.handler.ServeHTTP(rec, req)
	return rec
}

// emptyReader answers every read with an empty registry.
func emptyReader() EndpointReader {
	return readerFunc(func(
		context.Context, state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		return nil, nil
	})
}

// An empty DISCOVERY_ADDR disables the feature entirely: no server at all.
func TestNewServerDisabledWithoutAddr(t *testing.T) {
	if s := NewServer("", emptyReader(), testutil.DiscardLogger()); s != nil {
		t.Fatalf("NewServer with empty addr = %+v, want nil (feature disabled)", s)
	}
	if NewServer("127.0.0.1:0", nil, nil) == nil {
		t.Fatal("NewServer with an address returned nil; a configured address must enable the server")
	}
}

// Only GET is served: HEAD must not be silently answered as GET (Go's GET
// ServeMux pattern would match it), other methods are 405 with Allow: GET, and
// unknown paths are 404 — all before any registry read happens.
func TestTraefikEndpointMethodRouting(t *testing.T) {
	t.Run("GET serves the configuration", func(t *testing.T) {
		s := newTestServer(t, emptyReader(), testutil.DiscardLogger())
		rec := pollTraefik(t, s)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", traefikPath, rec.Code)
		}
		if got := rec.Body.String(); got != `{"http":{}}` {
			t.Fatalf("GET body = %q, want an empty configuration", got)
		}
	})

	t.Run("other methods get 405 with Allow GET", func(t *testing.T) {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			s := newTestServer(t, readerFunc(func(
				context.Context, state.ServiceRegistryFilter,
			) ([]state.ServiceRegistryEndpoint, error) {
				t.Errorf("%s must be rejected before any registry read", method)
				return nil, nil
			}), testutil.DiscardLogger())
			rec := httptest.NewRecorder()
			s.handler.ServeHTTP(rec, httptest.NewRequest(method, traefikPath, nil))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d, want 405", method, traefikPath, rec.Code)
			}
			if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
				t.Fatalf("%s Allow = %q, want to include GET", method, allow)
			}
		}
	})

	t.Run("unknown paths get 404", func(t *testing.T) {
		s := newTestServer(t, emptyReader(), testutil.DiscardLogger())
		for _, path := range []string{
			"/", "/discovery", "/discovery/traefik/", "/discovery/traefik/extra",
			"/other",
		} {
			rec := httptest.NewRecorder()
			s.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("GET %s = %d, want 404", path, rec.Code)
			}
		}
	})
}

// HEAD /discovery/traefik must return 405, not 200: Go's GET method pattern in
// ServeMux also matches HEAD, so the handler owns the method guard. Allow: GET
// is asserted whenever the handler sets it, and the guard runs before the
// registry is read.
func TestTraefikEndpointHeadReturns405(t *testing.T) {
	s := newTestServer(t, readerFunc(func(
		context.Context, state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		t.Error("registry read triggered by HEAD; the method guard must reject it first")
		return nil, nil
	}), testutil.DiscardLogger())

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, traefikPath, nil))

	if rec.Code == http.StatusOK {
		t.Fatal("HEAD /discovery/traefik = 200; ServeMux's GET pattern matches " +
			"HEAD, but only GET may be served")
	}
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("HEAD /discovery/traefik = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "" &&
		!strings.Contains(allow, http.MethodGet) {
		t.Fatalf("Allow = %q, want to include GET", allow)
	}
}

// A registry read succeeds: the response is the projected/transformed JSON
// configuration, never the persistence details of the rows it came from.
func TestTraefikEndpointServesRegistryEndpointsAsJSON(t *testing.T) {
	endpoint := registryEndpoint(
		"alpha", "web", "api.example.com", "/v2", "192.0.2.10", 8080)
	endpoint.NodeID = "node-persist"
	endpoint.Replica = 2
	endpoint.HostIP = "10.9.9.9"
	endpoint.InternalPort = 3000
	endpoint.ContainerID = "ctr-persist"
	endpoint.Generation = "gen-persist"

	s := newTestServer(t, readerFunc(func(
		context.Context, state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		return []state.ServiceRegistryEndpoint{endpoint}, nil
	}), testutil.DiscardLogger())

	rec := pollTraefik(t, s)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}

	var cfg traefikConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("response is not a Traefik configuration: %v", err)
	}
	if len(cfg.HTTP.Routers) != 1 {
		t.Fatalf("routers = %v, want exactly one", cfg.HTTP.Routers)
	}
	for _, router := range cfg.HTTP.Routers {
		if router.Rule != "Host(`api.example.com`) && PathPrefix(`/v2`)" {
			t.Fatalf("rule = %q", router.Rule)
		}
		service, ok := cfg.HTTP.Services[router.Service]
		if !ok {
			t.Fatalf("router service %q missing from services", router.Service)
		}
		want := []traefikServer{{URL: "http://192.0.2.10:8080"}}
		if !slices.Equal(service.LoadBalancer.Servers, want) {
			t.Fatalf("servers = %+v, want %+v", service.LoadBalancer.Servers, want)
		}
	}

	body := rec.Body.String()
	for _, leaked := range []string{"persist", "10.9.9.9", "node-"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("response leaks registry persistence detail %q: %s", leaked, body)
		}
	}
}

// An empty registry is a valid, servable configuration — not an error.
func TestTraefikEndpointEmptyRegistry(t *testing.T) {
	s := newTestServer(t, emptyReader(), testutil.DiscardLogger())
	rec := pollTraefik(t, s)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET on an empty registry = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"http":{}}` {
		t.Fatalf("body = %q, want an empty configuration", got)
	}
}

// A registry read error is 503 so the provider keeps its last good
// configuration: the internal error text never reaches the response, and the
// failure is logged once.
func TestTraefikEndpointReadErrorReturns503WithoutLeakingDetails(t *testing.T) {
	logs := &recordingLogHandler{}
	s := newTestServer(t, readerFunc(func(
		context.Context, state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		return nil, errors.New("postgres: connection refused for db.internal:5432")
	}), slog.New(logs))

	rec := pollTraefik(t, s)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET after a read error = %d, want 503", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/plain; charset=utf-8", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
	wantBody := "discovery temporarily unavailable\n"
	if got := rec.Body.String(); got != wantBody {
		t.Fatalf("body = %q, want %q", got, wantBody)
	}
	for _, leaked := range []string{"postgres", "db.internal"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("body leaks the internal error detail %q", leaked)
		}
	}
	if got := len(logs.matching(readFailMsg, "")); got != 1 {
		t.Fatalf("%s records = %d, want exactly 1", readFailMsg, got)
	}
	if got := len(logs.matching(conflictMsg, "")); got != 0 {
		t.Fatalf("conflict records = %d after a failed read, want 0", got)
	}
}

// A server built without a reader reports 503 instead of panicking, including
// under a nil logger.
func TestTraefikEndpointNilReaderUnavailable(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil, nil)
	rec := pollTraefik(t, s)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET with a nil reader = %d, want 503", rec.Code)
	}
	if got := rec.Body.String(); got != "discovery temporarily unavailable\n" {
		t.Fatalf("body = %q, want the unavailable response", got)
	}
}

// The registry read is bounded by the query timeout: a slow database fails the
// request with 503 instead of holding the connection, and the reader observes a
// real deadline.
func TestTraefikQueryTimeoutBoundsSlowRead(t *testing.T) {
	const queryTimeout = 20 * time.Millisecond
	deadlineCh := make(chan time.Time, 1)
	s := newTestServer(t, readerFunc(func(
		ctx context.Context, _ state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		if d, ok := ctx.Deadline(); ok {
			deadlineCh <- d
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}), testutil.DiscardLogger())
	s.queryTimeout = queryTimeout

	start := time.Now()
	rec := pollTraefik(t, s)
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET against a stalled read = %d, want 503", rec.Code)
	}
	select {
	case d := <-deadlineCh:
		if until := time.Until(d); until > queryTimeout+time.Second {
			t.Fatalf("read deadline is %v away, want about the %v query timeout",
				until, queryTimeout)
		}
	default:
		t.Fatal("the registry read observed no deadline; the query timeout must bound it")
	}
	if elapsed < 10*time.Millisecond {
		t.Fatalf("handler returned in %v, want it to wait for the query timeout", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("handler took %v, want a bounded query timeout", elapsed)
	}
}

// A client that goes away cancels the in-flight registry read: the handler
// returns promptly instead of waiting out the query timeout.
func TestTraefikRequestCancellationStopsRead(t *testing.T) {
	started := make(chan struct{})
	s := newTestServer(t, readerFunc(func(
		ctx context.Context, _ state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), testutil.DiscardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, traefikPath, nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		s.handler.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("registry read never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after the request context was cancelled")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET after client cancellation = %d, want 503", rec.Code)
	}
}

// The dedicated server bounds every phase of a stuck client (header read, full
// read, write, and idle keep-alive) so one wedged connection cannot occupy it.
func TestServerBoundsIOTimeouts(t *testing.T) {
	s := newTestServer(t, emptyReader(), testutil.DiscardLogger())
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s.srv == nil {
		t.Fatal("server is nil after Start")
	}
	if s.srv.ReadHeaderTimeout <= 0 || s.srv.ReadTimeout <= 0 ||
		s.srv.WriteTimeout <= 0 || s.srv.IdleTimeout <= 0 {
		t.Fatalf("IO timeouts = header %v read %v write %v idle %v, want all bounded",
			s.srv.ReadHeaderTimeout, s.srv.ReadTimeout, s.srv.WriteTimeout,
			s.srv.IdleTimeout)
	}
}

// Given concurrent provider polls over a conflicting registry snapshot, when
// they race through the handler, then every poll sees the identical
// configuration and the conflict is logged exactly once (deduplicated by the
// shared conflict state).
func TestConcurrentPollingIsConsistentAndLogsConflictOnce(t *testing.T) {
	logs := &recordingLogHandler{}
	conflicting := []state.ServiceRegistryEndpoint{
		registryEndpoint("alpha", "web", "api.test", "", "192.0.2.10", 8080),
		registryEndpoint("beta", "web", "api.test", "", "192.0.2.11", 8081),
	}
	s := newTestServer(t, readerFunc(func(
		context.Context, state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		return conflicting, nil
	}), slog.New(logs))

	const goroutines, pollsEach = 16, 8
	codes := make([]int, goroutines*pollsEach)
	bodies := make([]string, len(codes))
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for p := 0; p < pollsEach; p++ {
				i := g*pollsEach + p
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, traefikPath, nil)
				s.handler.ServeHTTP(rec, req)
				codes[i] = rec.Code
				bodies[i] = rec.Body.String()
			}
		}(g)
	}
	wg.Wait()

	for i := range codes {
		if codes[i] != http.StatusOK {
			t.Fatalf("poll %d = %d, want 200", i, codes[i])
		}
		if bodies[i] != bodies[0] {
			t.Fatalf("poll %d body differs from the first poll:\n%s\nvs\n%s",
				i, bodies[i], bodies[0])
		}
	}
	if got := len(logs.matching(conflictMsg, "new")); got != 1 {
		t.Fatalf("%s state=new records = %d across %d concurrent polls, want exactly 1",
			conflictMsg, got, len(codes))
	}
	if got := len(logs.matching(conflictMsg, "changed")); got != 0 {
		t.Fatalf("%s state=changed records = %d, want 0", conflictMsg, got)
	}
}

// The conflict log is a state machine over successive polls: a conflict logs
// once when new, once when its identities change, once when it resolves, and
// never re-emits while unchanged — and a failed read neither clears the active
// set nor re-logs it.
func TestConflictStateLogLifecycle(t *testing.T) {
	logs := &recordingLogHandler{}
	pair := []state.ServiceRegistryEndpoint{
		registryEndpoint("alpha", "web", "api.test", "", "192.0.2.10", 8080),
		registryEndpoint("beta", "web", "api.test", "", "192.0.2.11", 8081),
	}
	changed := append(slices.Clone(pair), registryEndpoint(
		"alpha", "api", "api.test", "", "192.0.2.12", 8082))
	clean := []state.ServiceRegistryEndpoint{
		registryEndpoint("alpha", "web", "api.test", "/v2", "192.0.2.10", 8080),
	}

	var endpoints []state.ServiceRegistryEndpoint
	var readErr error
	s := newTestServer(t, readerFunc(func(
		context.Context, state.ServiceRegistryFilter,
	) ([]state.ServiceRegistryEndpoint, error) {
		if readErr != nil {
			return nil, readErr
		}
		return endpoints, nil
	}), slog.New(logs))

	poll := func(want int) *httptest.ResponseRecorder {
		t.Helper()
		rec := pollTraefik(t, s)
		if rec.Code != want {
			t.Fatalf("poll = %d, want %d", rec.Code, want)
		}
		return rec
	}
	expect := func(msg, state string, want int) {
		t.Helper()
		if got := len(logs.matching(msg, state)); got != want {
			t.Fatalf("%s state=%q records = %d, want %d", msg, state, got, want)
		}
	}

	// A clean registry logs nothing.
	endpoints = clean
	poll(http.StatusOK)
	expect(conflictMsg, "", 0)
	expect(resolvedMsg, "", 0)

	// The conflict appears: one "new" record carrying the full context.
	endpoints = pair
	poll(http.StatusOK)
	expect(conflictMsg, "new", 1)
	expect(conflictMsg, "changed", 0)
	record := logs.matching(conflictMsg, "new")[0]
	if record.level != slog.LevelError {
		t.Fatalf("new conflict level = %v, want Error", record.level)
	}
	if got := record.attrs["host"]; got != "api.test" {
		t.Fatalf("conflict host = %v, want api.test", got)
	}
	if got := record.attrs["path"]; got != "/" {
		t.Fatalf("conflict path = %v, want / (host-only rendered as root)", got)
	}
	services, ok := record.attrs["services"].([]string)
	if !ok || !slices.Equal(services, []string{"alpha/web", "beta/web"}) {
		t.Fatalf("conflict services = %v, want [alpha/web beta/web] sorted",
			record.attrs["services"])
	}
	for _, attr := range []string{"reason", "action"} {
		if _, ok := record.attrs[attr].(string); !ok {
			t.Fatalf("conflict record is missing the %q attribute: %v",
				attr, record.attrs)
		}
	}

	// The identical conflict on the next poll is not re-logged.
	poll(http.StatusOK)
	expect(conflictMsg, "new", 1)
	expect(conflictMsg, "changed", 0)

	// The conflict's identities changed: one "changed" record, no new record.
	endpoints = changed
	poll(http.StatusOK)
	expect(conflictMsg, "new", 1)
	expect(conflictMsg, "changed", 1)
	changedRecord := logs.matching(conflictMsg, "changed")[0]
	if changedRecord.level != slog.LevelError {
		t.Fatalf("changed conflict level = %v, want Error", changedRecord.level)
	}
	changedServices, ok := changedRecord.attrs["services"].([]string)
	if !ok || !slices.Equal(changedServices,
		[]string{"alpha/api", "alpha/web", "beta/web"}) {
		t.Fatalf("changed conflict services = %v, want [alpha/api alpha/web beta/web]",
			changedRecord.attrs["services"])
	}

	// A failed read logs its own error and touches the conflict state in
	// neither direction.
	readErr = errors.New("database unreachable")
	poll(http.StatusServiceUnavailable)
	expect(readFailMsg, "", 1)
	expect(conflictMsg, "new", 1)
	expect(conflictMsg, "changed", 1)
	expect(resolvedMsg, "", 0)

	// The active conflict survived the failed read: the same snapshot does not
	// re-emit it as new or changed.
	readErr = nil
	poll(http.StatusOK)
	expect(conflictMsg, "new", 1)
	expect(conflictMsg, "changed", 1)

	// The conflict resolves: exactly one recovery record with the previous
	// members, and only once.
	endpoints = clean
	poll(http.StatusOK)
	expect(resolvedMsg, "", 1)
	expect(conflictMsg, "", 2)
	resolved := logs.matching(resolvedMsg, "")[0]
	if resolved.level != slog.LevelInfo {
		t.Fatalf("resolved level = %v, want Info", resolved.level)
	}
	resolvedServices, ok := resolved.attrs["services"].([]string)
	if !ok || !slices.Equal(resolvedServices,
		[]string{"alpha/api", "alpha/web", "beta/web"}) {
		t.Fatalf("resolved services = %v, want the previous conflict members",
			resolved.attrs["services"])
	}
	poll(http.StatusOK)
	expect(resolvedMsg, "", 1)
	expect(conflictMsg, "", 2)
}

// Start binds synchronously and serves real requests; Stop drains the server
// and is idempotent.
func TestServerLifecycleStartServeStop(t *testing.T) {
	addr := testutil.FreeAddr(t)
	s := NewServer(addr, emptyReader(), testutil.DiscardLogger())
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + traefikPath)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				t.Fatalf("read response: %v", readErr)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET = %d, want 200", resp.StatusCode)
			}
			if string(body) != `{"http":{}}` {
				t.Fatalf("body = %q, want an empty configuration", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never became reachable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// Start fails fast — on a taken address and on an unparseable one — without a
// logger, and reports the discovery-prefixed bind error to the caller.
func TestServerStartFailsFastOnBadAddress(t *testing.T) {
	t.Run("taken address", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve address: %v", err)
		}
		defer ln.Close()

		s := NewServer(ln.Addr().String(), nil, nil)
		if err := s.Start(); err == nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = s.Stop(stopCtx)
			t.Fatal("Start on a taken address returned nil, want a bind error")
		} else if !strings.Contains(err.Error(), "discovery: listen") {
			t.Fatalf("bind error = %v, want the discovery: listen prefix", err)
		}
	})

	t.Run("unparseable address", func(t *testing.T) {
		err := NewServer("crap", nil, nil).Start()
		if err == nil {
			t.Fatal("Start on an unparseable address returned nil, want a bind error")
		}
		if !strings.Contains(err.Error(), "discovery: listen") {
			t.Fatalf("bind error = %v, want the discovery: listen prefix", err)
		}
	})
}

// Start may run exactly once — a second call fails even after Stop — so a
// worker cannot silently double-bind the configured address.
func TestServerStartsExactlyOnce(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil, testutil.DiscardLogger())
	if err := s.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := s.Start(); err == nil {
		t.Fatal("second Start returned nil, want an already-started error")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Start(); err == nil {
		t.Fatal("Start after Stop returned nil, want an already-started error")
	}
}

// Stop is safe on a never-started server and on a nil receiver, and Start on a
// nil receiver reports an error instead of panicking.
func TestServerStopWithoutStartAndNilReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := NewServer("127.0.0.1:0", nil, nil).Stop(ctx); err != nil {
		t.Fatalf("Stop on a never-started server: %v", err)
	}
	var nilServer *Server
	if err := nilServer.Stop(ctx); err != nil {
		t.Fatalf("Stop on a nil server: %v", err)
	}
	if err := nilServer.Start(); err == nil {
		t.Fatal("Start on a nil server returned nil, want an error")
	}
}
