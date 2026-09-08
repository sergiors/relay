package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"
)

// healthAddr is the fixed listen address for the /health endpoint. It is an
// application convention for container orchestrators, not configuration.
const healthAddr = ":80"

// healthServer serves a single GET /health endpoint used only as a container
// healthcheck for orchestrators. It reports 200 while the consumer is healthy
// and 503 otherwise. It is not a public API and exposes no other endpoints.
type healthServer struct {
	server *http.Server
}

// newHealthServer builds an HTTP server on the fixed health port that reports
// the consumer's health. healthy is a read-only probe (the Consumer's
// Healthy()).
func newHealthServer(healthy func() bool) *healthServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if healthy() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	return &healthServer{
		server: &http.Server{Addr: healthAddr, Handler: mux},
	}
}

// start runs the server in a goroutine and shuts it down (with a short timeout)
// when ctx is cancelled. It returns immediately.
func (h *healthServer) start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := h.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server: %v", err)
		}
	}()
}
