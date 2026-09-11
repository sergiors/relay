package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const handlerContentType = "text/plain; version=0.0.4"

func TestHandlerServesMetrics(t *testing.T) {
	r := New()
	r.Inc("events_received_total")
	r.SetGauge("pending_entries", 2)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Handler().ServeHTTP(rec, req)

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

func TestHandlerRootServesMetrics(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "pending_entries 1") {
		t.Fatalf("root body missing metric:\n%s", rec.Body.String())
	}
}

func TestHandlerNotFound(t *testing.T) {
	r := New()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	r := New()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHandlerHead(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD body length = %d, want 0", rec.Body.Len())
	}
}

func TestHandlerNilRegistryServesEmpty(t *testing.T) {
	var r *Registry
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("nil registry body = %q, want empty", rec.Body.String())
	}
}

func TestServeShutsDownOnCancel(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx, ln, nil) }()

	// The server starts asynchronously; scrape-retry instead of a fixed sleep.
	for {
		resp, err := http.Get("http://" + ln.Addr().String() + "/metrics")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !strings.Contains(string(body), "pending_entries 1") {
				t.Fatalf("scrape body missing metric:\n%s", body)
			}
			break
		}
		select {
		case <-done:
			t.Fatalf("Serve returned before becoming scrapeable: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

func TestServeHTTPAddrConvenience(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1)

	// Find a free port by listening and closing first.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.ServeHTTP(ctx, addr, nil) }()

	// The server starts asynchronously; scrape-retry instead of a fixed sleep.
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
		select {
		case <-done:
			t.Fatalf("ServeHTTP returned before becoming scrapeable: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeHTTP returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not return after cancel")
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
