package git

import (
	"os"
	"path/filepath"
	"testing"
)

// cfgAt writes (via the package writeConfig) a config to a temp path and
// returns the path.
func configPath(t *testing.T) string {
	return filepath.Join(t.TempDir(), "source.json")
}

func TestConfigCreateAndUpdate(t *testing.T) {
	p := configPath(t)
	c := Config{Repository: "git@github.com:a/r.git", Ref: "main"}
	if err := writeConfig(c, p); err != nil {
		t.Fatalf("writeConfig: %v", err)
	}
	got, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got.Repository != c.Repository || got.Ref != c.Ref || got.Synced {
		t.Fatalf("loaded = %+v, want repository/ref and Synced=false", got)
	}
	// Update (upsert): change ref and path, set a sync timestamp.
	c2 := Config{Repository: "git@github.com:a/r.git", Ref: "dev", Path: "pkg/fn", Synced: true, LastSyncedAt: "t"}
	if err := writeConfig(c2, p); err != nil {
		t.Fatalf("writeConfig update: %v", err)
	}
	got2, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got2.Ref != "dev" || got2.Path != "pkg/fn" || !got2.Synced || got2.LastSyncedAt != "t" {
		t.Fatalf("updated = %+v, want dev/pkg/fn/synced", got2)
	}
}

func TestConfigDefaultRefApplied(t *testing.T) {
	// A config written without a ref still loads as DefaultRef (defensive for
	// old/hand-edited files).
	p := configPath(t)
	raw := `{"repository":"git@github.com:a/r.git","synced":false}`
	if err := os.WriteFile(p, []byte(raw), 0o600); err != nil {
		t.Fatalf("write raw: %v", err)
	}
	got, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got.Ref != DefaultRef {
		t.Fatalf("Ref = %q, want default %q", got.Ref, DefaultRef)
	}
}

func TestValidateRef(t *testing.T) {
	for _, ref := range []string{"main", "feature/x", "v1.2.3", "0123456789abcdef0123456789abcdef01234567"} {
		if err := validateRef(ref); err != nil {
			t.Errorf("validateRef(%q): %v, want nil", ref, err)
		}
	}
	for _, ref := range []string{"", "with space", "tab\there", "-leading"} {
		if err := validateRef(ref); err == nil {
			t.Errorf("validateRef(%q): nil, want error", ref)
		}
	}
}

func TestConfigPathTraversalRejected(t *testing.T) {
	p := configPath(t)
	for _, path := range []string{"../x", "/abs", "a/../../b", "..", ".", `back\slash`} {
		c := Config{Repository: "git@github.com:a/r.git", Ref: "main", Path: path}
		if err := writeConfig(c, p); err == nil {
			t.Errorf("writeConfig with path %q: nil, want error", path)
		}
	}
	// No config should have been written: every attempted write above rejected
	// the traversal before persisting anything.
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("config persisted despite an invalid path")
	}
}
