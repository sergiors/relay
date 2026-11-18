package git

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The worker's source-driven participation branch selects local /apps mode ONLY
// for errors.Is(err, ErrConfigNotFound()) — an absent source file. A source
// file that exists but is corrupt or holds invalid values must classify as a
// different error so startup fails closed ("deployment: invalid local git
// source") instead of silently activating a divergent local tree. These tests
// pin that classification at its single source of truth.
func TestLoadConfigClassifiesAbsentCorruptAndInvalidSources(t *testing.T) {
	tests := []struct {
		name string
		// write returns the config file content to plant (nil = leave absent).
		write        func(t *testing.T, path string)
		wantNotFound bool
	}{
		{
			name:         "absent source is the only local-mode selector",
			write:        nil,
			wantNotFound: true,
		},
		{
			name: "corrupt JSON is fail-closed",
			write: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
					t.Fatalf("write corrupt config: %v", err)
				}
			},
		},
		{
			name: "an empty source file is fail-closed",
			write: func(t *testing.T, path string) {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatalf("write empty config: %v", err)
				}
			},
		},
		{
			name: "valid JSON with a non-SSH repository is fail-closed",
			write: func(t *testing.T, path string) {
				writeConfigFile(t, path,
					`{"repository":"https://github.com/acme/r.git","ref":"main"}`)
			},
		},
		{
			name: "valid JSON with a malformed repository is fail-closed",
			write: func(t *testing.T, path string) {
				writeConfigFile(t, path,
					`{"repository":"git@:acme/r.git","ref":"main"}`)
			},
		},
		{
			name: "valid JSON with an unsafe ref is fail-closed",
			write: func(t *testing.T, path string) {
				writeConfigFile(t, path,
					`{"repository":"git@github.com:acme/r.git","ref":"bad ref"}`)
			},
		},
		{
			name: "valid JSON with an unsafe monorepo path is fail-closed",
			write: func(t *testing.T, path string) {
				writeConfigFile(t, path,
					`{"repository":"git@github.com:acme/r.git","ref":"main","path":"../escape"}`)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.json")
			if tc.write != nil {
				tc.write(t, path)
			}

			cfg, err := LoadConfig(path)
			isNotFound := errors.Is(err, ErrConfigNotFound())
			if tc.wantNotFound {
				if !isNotFound {
					t.Fatalf("absent source LoadConfig err = %v, want the ErrConfigNotFound sentinel", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("LoadConfig = %+v, want an error for a present-but-broken source", cfg)
			}
			if isNotFound {
				t.Fatalf("LoadConfig err = %v, must NOT match ErrConfigNotFound: that "+
					"sentence is the worker's local-/apps selector, and only an absent "+
					"source may select it", err)
			}
			if strings.Contains(err.Error(), "no git source configured") {
				t.Fatalf("LoadConfig err = %v, must not read as 'not configured'", err)
			}
		})
	}
}

// writeConfigFile plants raw config content directly, bypassing the validating
// writer — an operator's hand-edited or corrupted source file.
func writeConfigFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
