package metrics

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const handlerContentType = "text/plain; version=0.0.4"

// TestMuxServesMetrics verifies GET /metrics through the Server's mux returns
// the registry exposition with the expected body and Content-Type.
func TestMuxServesMetrics(t *testing.T) {
	r := New()
	r.Inc("events_received_total")
	r.SetGauge("pending_entries", 2)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	NewServer("127.0.0.1:0", r.Handler(), nil).handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, handlerContentType) {
		t.Fatalf("Content-Type = %q, want prefix %q", ct, handlerContentType)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "events_received_total 1") {
		t.Fatalf("body missing counter:\n%s", body)
	}
	if !strings.Contains(body, "pending_entries 2") {
		t.Fatalf("body missing gauge:\n%s", body)
	}
}

// TestMuxRootNotFound verifies the root path no longer serves metrics: the mux
// registers only /metrics, so "/" now 404s (the old "/" alias is gone).
func TestMuxRootNotFound(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)
	rec := httptest.NewRecorder()
	NewServer("127.0.0.1:0", r.Handler(), nil).handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// TestMuxNotFound verifies a request to any path other than /metrics returns
// 404 from the mux.
func TestMuxNotFound(t *testing.T) {
	r := New()
	rec := httptest.NewRecorder()
	NewServer("127.0.0.1:0", r.Handler(), nil).handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// TestMuxMethodNotAllowed verifies a non-GET/HEAD method on /metrics returns
// 405 with an Allow header from the "GET /metrics" method pattern.
func TestMuxMethodNotAllowed(t *testing.T) {
	r := New()
	rec := httptest.NewRecorder()
	NewServer("127.0.0.1:0", r.Handler(), nil).handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
		t.Fatalf("Allow = %q, want to include GET", allow)
	}
}

// TestMuxHead verifies HEAD /metrics is accepted (Go's "GET" method pattern
// matches HEAD too) and served directly by the registered handler: status 200
// with the exposition Content-Type. The direct mux.Handle registration does
// not suppress the HEAD body (promhttp writes it; net/http does not filter
// HEAD bodies), so this test pins that known behavior rather than asserting a
// body-less response.
func TestMuxHead(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)
	rec := httptest.NewRecorder()
	NewServer("127.0.0.1:0", r.Handler(), nil).handler.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, handlerContentType) {
		t.Fatalf("Content-Type = %q, want prefix %q", ct, handlerContentType)
	}
}

// TestMuxNilHandlerPanics pins that a nil handler is rejected at construction:
// http.ServeMux.Handle panics on a nil handler, so NewServer(nil) fails fast
// rather than deferring the failure to request time. (There is no fallback
// handler — a nil handler is a programming error.)
func TestMuxNilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewServer with a nil handler did not panic; want a fail-fast registration panic")
		}
	}()
	NewServer("127.0.0.1:0", nil, nil)
}

// TestServerStartServesInBackground verifies Start binds synchronously and then
// serves the registry in the background (returning nil immediately), so a
// scrape succeeds without requiring Stop.
func TestServerStartServesInBackground(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv := NewServer(addr, r.Handler(), nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The server is serving in the background; scrape-retry until reachable.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !strings.Contains(string(body), "pending_entries 1") {
				t.Fatalf("scrape body missing metric:\n%s", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never became scrapeable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestServerStartFailsFastOnBadAddr verifies Start returns the bind error
// IMMEDIATELY for an unparseable address, without retrying. This is the
// deliberate replacement for the old retry-bind: a metrics port conflict is a
// fatal config error, not a transient race.
func TestServerStartFailsFastOnBadAddr(t *testing.T) {
	err := NewServer("crap", New().Handler(), log.New(io.Discard, "", 0)).Start()
	if err == nil {
		t.Fatal("Start on bad addr returned nil, want error")
	}
}

// TestServerStopIdempotentAndNeverStarted verifies Stop is safe on a
// never-started server (returns nil) and remains so on repeated calls.
func TestServerStopIdempotentAndNeverStarted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := NewServer("", New().Handler(), nil).Stop(ctx); err != nil {
		t.Fatalf("Stop on never-started server: %v", err)
	}

	srv := NewServer("", New().Handler(), nil)
	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("Stop on never-started server: %v", err)
	}
	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	var nilSrv *Server
	if err := nilSrv.Stop(ctx); err != nil {
		t.Fatalf("Stop on nil server: %v", err)
	}
}

// TestServerStartTwiceFails verifies a second Start (even after a prior Stop)
// returns an error — Start may be called exactly once.
func TestServerStartTwiceFails(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv := NewServer(addr, New().Handler(), nil)
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

// TestServerStartBadAddrThenGoodAddr verifies a failed Start (bad addr) can be
// followed by a successful Start on a corrected address.
func TestServerStartBadAddrThenGoodAddr(t *testing.T) {
	srv := NewServer("crap", New().Handler(), log.New(io.Discard, "", 0))
	if err := srv.Start(); err == nil {
		t.Fatal("Start on bad addr returned nil, want error")
	}

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv.addr = addr
	if err := srv.Start(); err != nil {
		t.Fatalf("Start on corrected addr: %v", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConcurrentIncrementAndRender(t *testing.T) {
	r := New()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					r.Inc("events_received_total")
					r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", "a"}, {"handler", "x"}})
					r.ObserveDurationLabels("handler_duration_seconds", []Label{{"function", "a"}, {"handler", "x"}}, time.Millisecond)
					r.SetGauge("pending_entries", 1)
				}
			}
		}()
	}

	// Reader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.Snapshot()
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
