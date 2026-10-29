package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestParseNodeIDValid pins that an operator-supplied NODE_ID is trimmed and
// used verbatim.
func TestParseNodeIDValid(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"ops-1", "ops-1"},
		{"  node.a_1  ", "node.a_1"},
		{"ABC.def-123_", "ABC.def-123_"},
	} {
		got, err := ParseNodeID(tc.in)
		if err != nil {
			t.Fatalf("ParseNodeID(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseNodeID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestParseNodeIDInvalid pins that NODE_ID is REQUIRED and validated: an
// empty/whitespace value and a malformed value fail rather than being silently
// coerced or replaced with a generated identity.
func TestParseNodeIDInvalid(t *testing.T) {
	for _, value := range []string{
		"",
		"   ",
		"has space",
		"a/b",
		"café",
		"a\tb",
		strings.Repeat("a", maxNodeIDLen+1),
	} {
		got, err := ParseNodeID(value)
		if err == nil {
			t.Fatalf("ParseNodeID(%q) = %q, want error", value, got)
		}
		if !strings.Contains(err.Error(), "NODE_ID") {
			t.Fatalf("ParseNodeID(%q) error = %q, want it to mention NODE_ID", value, err)
		}
	}
}

// TestLoadNodeIdentity pins that Load resolves the REQUIRED stable NodeID
// verbatim from NODE_ID and always generates a fresh InstanceID distinct from
// the NodeID, so a stale process incarnation can be fenced out.
func TestLoadNodeIdentity(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NODE_ID", "ops-1")

	cfg := mustLoad(t)
	if cfg.NodeID != "ops-1" {
		t.Fatalf("NodeID = %q, want the pinned NODE_ID", cfg.NodeID)
	}
	if cfg.InstanceID == "" || !strings.HasPrefix(cfg.InstanceID, instanceIDPrefix) {
		t.Fatalf("InstanceID = %q, want a fresh %q token", cfg.InstanceID, instanceIDPrefix)
	}
	if cfg.InstanceID == cfg.NodeID {
		t.Fatal("InstanceID and NodeID must differ")
	}
	// A second Load (a new process) keeps the same NodeID but gets a fresh
	// InstanceID.
	second := mustLoad(t)
	if second.NodeID != cfg.NodeID {
		t.Fatalf("NodeID changed across loads: %q -> %q", cfg.NodeID, second.NodeID)
	}
	if second.InstanceID == cfg.InstanceID {
		t.Fatalf("InstanceID reused across loads: %q", cfg.InstanceID)
	}
}

// TestLoadRequiresNodeID pins that NODE_ID is REQUIRED with no hostname or
// generated fallback: an unset or empty value fails Load with an error naming
// the variable, so a worker never starts with an invented identity.
func TestLoadRequiresNodeID(t *testing.T) {
	for _, unset := range []bool{true, false} {
		name := "empty"
		if unset {
			name = "unset"
		}
		t.Run(name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("NODE_ID", "")
			if unset {
				os.Unsetenv("NODE_ID")
			}

			logger, buf := testLogger()
			_, err := Load(logger)
			if err == nil {
				t.Fatal("Load succeeded without NODE_ID; want an error")
			}
			if !strings.Contains(err.Error(), "NODE_ID") {
				t.Fatalf("error %q does not name NODE_ID", err)
			}
			if buf.Len() != 0 {
				t.Fatalf("Load logged %q for a returned config error; want no fatal log", buf.String())
			}
		})
	}
}

// TestLoadStatsRetention pins STATS_RETENTION resolution: unset defaults to 7d,
// an explicit duration is honored, and empty/zero/negative/malformed disable
// compaction (0) with a log naming the variable rather than failing startup.
func TestLoadStatsRetention(t *testing.T) {
	t.Run("unset default", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("STATS_RETENTION", "")
		os.Unsetenv("STATS_RETENTION")
		if got := mustLoad(t).StatsRetention; got != DefaultStatsRetention {
			t.Fatalf("StatsRetention = %v, want %v", got, DefaultStatsRetention)
		}
	})
	t.Run("explicit", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("STATS_RETENTION", "720h")
		if got := mustLoad(t).StatsRetention; got != 720*time.Hour {
			t.Fatalf("StatsRetention = %v, want 720h", got)
		}
	})
	for _, value := range []string{"", "0", "-1h", "bogus"} {
		t.Run("disabled="+value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("STATS_RETENTION", value)
			logger, buf := testLogger()
			cfg, err := Load(logger)
			if err != nil {
				t.Fatalf("Load(%q) error: %v", value, err)
			}
			if cfg.StatsRetention != 0 {
				t.Fatalf("StatsRetention = %v, want 0 (disabled) for %q", cfg.StatsRetention, value)
			}
			// A malformed value logs and names the variable; an explicit
			// empty/zero disable is silent.
			if value == "bogus" && !strings.Contains(buf.String(), "STATS_RETENTION") {
				t.Fatalf("Load(%q) log does not mention STATS_RETENTION: %q", value, buf.String())
			}
		})
	}
}
