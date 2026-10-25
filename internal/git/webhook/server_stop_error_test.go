package webhook

// Regression coverage for Server.Stop's shutdown-failure path: the first Stop
// result — including a shutdown error — must be persisted and observed by every
// later (and concurrent) caller, instead of a repeated caller seeing the zero
// value. The blocking provider seam parks one delivery inside the handler so
// srv.Shutdown deterministically cannot drain before the test's Stop context
// expires.
//
// These tests build the server via newServer (routing/lifecycle half), so the
// sync scheduler stays nil and Stop's HTTP shutdown error policy is exercised
// exactly as assembled for plain routing. The scheduler stop ordering/error
// policy (HTTP first, scheduler drained after, Warn-only scheduler errors,
// first error wins) remains covered unchanged by the existing assembly and
// scheduler tests.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// blockingProvider parks an in-flight delivery inside the provider handler: it
// signals entry through a channel and waits for release before responding. It
// implements the existing Provider seam; no new server interface is needed.
type blockingProvider struct {
	name      string
	entered   chan struct{}
	releaseCh chan struct{}
}

func (p *blockingProvider) Name() string { return p.name }

func (p *blockingProvider) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	// Buffered: signaling entry never blocks the handler, even if no one is
	// waiting on it anymore.
	p.entered <- struct{}{}
	<-p.releaseCh
	w.WriteHeader(http.StatusOK)
}

// parkDeliveryInProvider starts a Server on addr whose github provider parks
// one in-flight POST /github delivery: the provider signals entry through a
// channel, waits for release, and only then responds. It returns once the
// delivery is parked inside the provider. t.Cleanup releases the provider,
// joins the delivery goroutine, and stops the server — every wait bounded so a
// failing assertion cannot hang the test.
func parkDeliveryInProvider(t *testing.T, addr string) *Server {
	t.Helper()

	entered := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	srv := newServer(addr, testutil.DiscardLogger(), &blockingProvider{
		name: "github", entered: entered, releaseCh: releaseCh,
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deliveryDone := make(chan error, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Post(
			"http://"+addr+"/github", "application/json", strings.NewReader(`{}`))
		if err == nil {
			resp.Body.Close()
		}
		deliveryDone <- err
	}()

	// Given: an in-flight delivery is parked inside the provider (bounded: a
	// delivery that never arrives fails the test instead of hanging it).
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		releaseOnce.Do(func() { close(releaseCh) })
		t.Fatal("in-flight delivery never reached the provider handler")
	}

	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseCh) })
		select {
		case err := <-deliveryDone:
			if err != nil {
				t.Errorf("in-flight delivery did not complete: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("delivery goroutine did not finish within 5s after release")
		}
		cleanCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Stop(cleanCtx)
	})
	return srv
}

// TestServerStopShutdownContextErrorPersistsAcrossCalls verifies Stop surfaces
// a shutdown that cannot drain and that the FIRST result persists: with an
// in-flight delivery parked in the provider, a Stop bounded by an
// already-canceled context returns context.Canceled; a later Stop from an
// independent clean context observes the same error, never a fresh nil success.
func TestServerStopShutdownContextErrorPersistsAcrossCalls(t *testing.T) {
	srv := parkDeliveryInProvider(t, testutil.FreeAddr(t))

	// When: the first Stop is bounded by an already-canceled context while the
	// delivery is parked in the provider.
	failCtx, cancel := context.WithCancel(context.Background())
	cancel()
	first := srv.Stop(failCtx)

	// Then: the shutdown failure bounded by that context is returned.
	if !errors.Is(first, context.Canceled) {
		t.Fatalf("first Stop = %v, want context.Canceled", first)
	}

	// And: a repeated Stop observes the persisted first result.
	second := srv.Stop(context.Background())
	if !errors.Is(second, context.Canceled) {
		t.Fatalf("second Stop = %v, want the same context.Canceled", second)
	}
	if first != second {
		t.Fatalf("repeated Stop returned a different error: first %v, later %v", first, second)
	}
}

// TestServerStopConcurrentCallersObserveSameShutdownError verifies concurrent
// Stop callers with canceled contexts all observe the same failed result: the
// shutdown outcome is published once, so no caller sees the zero value or a
// different error than the others. Synchronization is by channels; the join is
// bounded so a stalled Stop fails the test rather than hanging it.
func TestServerStopConcurrentCallersObserveSameShutdownError(t *testing.T) {
	srv := parkDeliveryInProvider(t, testutil.FreeAddr(t))

	const callers = 5
	failCtx, cancel := context.WithCancel(context.Background())
	cancel()

	results := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- srv.Stop(failCtx)
		}()
	}

	allStopped := make(chan struct{})
	go func() {
		wg.Wait()
		close(allStopped)
	}()
	select {
	case <-allStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Stop calls did not all finish within 10s")
	}
	close(results)

	var seen []error
	for err := range results {
		seen = append(seen, err)
	}
	if len(seen) != callers {
		t.Fatalf("collected %d Stop results, want %d", len(seen), callers)
	}
	for _, err := range seen {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop = %v, want context.Canceled", err)
		}
		if err != seen[0] {
			t.Fatalf("Stop results differ across callers: first %v, later %v", seen[0], err)
		}
	}
}
