package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	tests := []struct {
		name       string
		healthy    bool
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{"healthy ok", true, http.MethodGet, "/health", http.StatusOK, "ok"},
		{"unhealthy 503", false, http.MethodGet, "/health", http.StatusServiceUnavailable, ""},
		{"wrong path 404", true, http.MethodGet, "/other", http.StatusNotFound, ""},
		{"wrong method 405", true, http.MethodPost, "/health", http.StatusMethodNotAllowed, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := newHealthServer(func() bool { return tt.healthy })
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			hs.server.Handler.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}
