package config

import (
	"strings"
	"testing"
	"time"
)

// TestStreamRetention covers the REDIS_STREAM_RETENTION parsing contract:
// unset/empty disables retention (0, nil); a valid duration parses; an invalid
// duration and a zero/negative value are configuration errors.
func TestStreamRetention(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"unset disables", "", 0, false},
		{"valid 6h", "6h", 6 * time.Hour, false},
		{"valid 30m", "30m", 30 * time.Minute, false},
		{"invalid duration", "bogus", 0, true},
		{"zero rejected", "0", 0, true},
		{"negative rejected", "-5m", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("REDIS_STREAM_RETENTION", tt.value)
			got, err := StreamRetention()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("StreamRetention(%q) = nil error, want error", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("StreamRetention(%q) error: %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("StreamRetention(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// TestStreamRetentionErrorMentionsVariable pins that the error text names the
// offending variable so an operator can find the misconfiguration.
func TestStreamRetentionErrorMentionsVariable(t *testing.T) {
	t.Setenv("REDIS_STREAM_RETENTION", "bogus")
	_, err := StreamRetention()
	if err == nil {
		t.Fatal("StreamRetention() = nil error, want error")
	}
	if !strings.Contains(err.Error(), "REDIS_STREAM_RETENTION") {
		t.Fatalf("error does not mention REDIS_STREAM_RETENTION: %q", err.Error())
	}
}
