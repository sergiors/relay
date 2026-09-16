package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *LocalStore {
	t.Helper()
	s, err := NewLocal(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s
}

// Resolve of a missing secret returns a clear error wrapping os.ErrNotExist
// with the name, and never any value.
func TestLocalResolveMissing(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Resolve(context.Background(), "nope")
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want os.ErrNotExist", err)
	}
	if !strings.Contains(err.Error(), `"nope"`) {
		t.Fatalf("error should name the secret, got: %v", err)
	}
}

// Traversal and path-escape names are rejected before any filesystem access.
func TestLocalResolveTraversalRejected(t *testing.T) {
	s := newTestStore(t)
	for _, name := range []string{"../foo", "/abs", "a/b", ".", "..", "a\\b", "UPPER", "trail.", ""} {
		if _, err := s.Resolve(context.Background(), name); err == nil {
			t.Errorf("Resolve(%q) = nil, want error", name)
		}
	}
}

// Set writes a file with mode 0600 and creates the directory with mode 0700.
func TestLocalSetAtomicAndPerms(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "db", "s3cr3t"); err != nil {
		t.Fatalf("set: %v", err)
	}
	info, err := os.Stat(filepath.Join(s.Dir(), "db"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %o, want 0600", info.Mode().Perm())
	}
	dirInfo, err := os.Stat(s.Dir())
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode = %o, want 0700", dirInfo.Mode().Perm())
	}
}

// Set overwrites atomically and leaves no temp files behind.
func TestLocalSetOverwritesAtomically(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "tok", "v1"); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if err := s.Set(context.Background(), "tok", "v2"); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	got, err := s.Resolve(context.Background(), "tok")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "v2" {
		t.Fatalf("resolved = %q, want v2", got)
	}
	// No temp files may remain.
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("dir entries = %d, want 1 (no temp files left)", len(entries))
	}
}

// List returns sorted names only, skipping subdirectories and never reading
// contents.
func TestLocalListNamesOnly(t *testing.T) {
	s := newTestStore(t)
	for _, n := range []string{"zeta", "alpha", "mid"} {
		if err := s.Set(context.Background(), n, "v"); err != nil {
			t.Fatalf("set %s: %v", n, err)
		}
	}
	// A subdirectory must be skipped.
	if err := os.MkdirAll(filepath.Join(s.Dir(), "subdir"), 0o700); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	names, err := s.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := strings.Join(names, ","); got != "alpha,mid,zeta" {
		t.Fatalf("list = %q, want alpha,mid,zeta", got)
	}
}

// Resolve returns the stored bytes exactly, including internal newlines and a
// trailing newline (the CLI strips a single trailing newline before writing;
// Resolve does not trim).
func TestLocalPreservesValue(t *testing.T) {
	s := newTestStore(t)
	val := "line1\nline2\n"
	if err := s.Set(context.Background(), "multi", val); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Resolve(context.Background(), "multi")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != val {
		t.Fatalf("resolved = %q, want stored bytes %q verbatim", got, val)
	}
}

// Delete removes a secret; deleting a missing one returns os.ErrNotExist.
func TestLocalDelete(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "gone", "v"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Delete(context.Background(), "gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Resolve(context.Background(), "gone"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolve after delete = %v, want os.ErrNotExist", err)
	}
	if err := s.Delete(context.Background(), "gone"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete missing = %v, want os.ErrNotExist", err)
	}
}

// Stat reports existence without reading contents.
func TestLocalStat(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "here", "v"); err != nil {
		t.Fatalf("set: %v", err)
	}
	ok, err := s.Stat(context.Background(), "here")
	if err != nil || !ok {
		t.Fatalf("stat existing = %v, %v; want true, nil", ok, err)
	}
	ok, err = s.Stat(context.Background(), "absent")
	if err != nil || ok {
		t.Fatalf("stat missing = %v, %v; want false, nil", ok, err)
	}
}

// List over a missing directory yields an empty list, not an error.
func TestLocalListMissingDir(t *testing.T) {
	s := newTestStore(t)
	names, err := s.List(context.Background())
	if err != nil {
		t.Fatalf("list missing dir: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("list = %v, want empty", names)
	}
}

// pemValue is a canonical PEM-shaped private-key fixture (header/footer,
// multiple internal newlines, a blank line, and an indented value with double
// spaces). It is deliberately inert — not a real RSA key — so printing it in a
// test failure is harmless. It has no trailing newline. Every multiline test in
// this package uses exactly this fixture so a single corruption at any hop is
// caught.
const pemValue = "-----BEGIN PRIVATE KEY-----\nMIIB\nline2\n\nindented:  value\n-----END PRIVATE KEY-----"

// TestLocalStoreMultilinePEMRoundTrip pins that LocalStore.Set writes a
// multiline (PEM-shaped) value byte-for-byte to disk with no normalization or
// JSON-encoding at rest, and LocalStore.Resolve returns those stored bytes
// verbatim — proving no hop transforms the bytes, even the raw file on disk.
func TestLocalStoreMultilinePEMRoundTrip(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "rsa", pemValue); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Resolve(context.Background(), "rsa")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != pemValue {
		t.Fatalf("resolved length = %d, want %d (byte-for-byte)", len(got), len(pemValue))
	}
	// The raw file on disk must equal the fixture exactly: no trimming, no
	// trailing newline added, no JSON/base64 encoding.
	raw, err := os.ReadFile(filepath.Join(s.Dir(), "rsa"))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(raw) != pemValue {
		t.Fatalf("on-disk length = %d, want %d (raw bytes verbatim)", len(raw), len(pemValue))
	}
	info, err := os.Stat(filepath.Join(s.Dir(), "rsa"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %o, want 0600", info.Mode().Perm())
	}
}

// TestLocalStoreMultilinePEMRoundTripTrailingNewline pins that Resolve returns a
// stored trailing newline verbatim (Resolve never trims; only the CLI strips one
// trailing newline before writing).
func TestLocalStoreMultilinePEMRoundTripTrailingNewline(t *testing.T) {
	s := newTestStore(t)
	val := "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n"
	if err := s.Set(context.Background(), "rsa", val); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Resolve(context.Background(), "rsa")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != val {
		t.Fatalf("resolved length = %d, want %d (trailing newline preserved)", len(got), len(val))
	}
}

// TestLocalStoreMultilineInternalBlanksAndTrailingSpaces pins that an internal
// blank line and internal trailing spaces are preserved exactly.
func TestLocalStoreMultilineInternalBlanksAndTrailingSpaces(t *testing.T) {
	s := newTestStore(t)
	val := "line   \n\nnext"
	if err := s.Set(context.Background(), "ws", val); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Resolve(context.Background(), "ws")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != val {
		t.Fatalf("resolved length = %d, want %d (internal blanks/trailing spaces preserved)", len(got), len(val))
	}
}

// TestLocalProviderMultilineResolve pins that LocalProvider.Resolve returns the
// multiline fixture verbatim through the Provider interface — the exact seam the
// runner uses to inject secret values into execution environments.
func TestLocalProviderMultilineResolve(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "rsa", pemValue); err != nil {
		t.Fatalf("set: %v", err)
	}
	p, err := NewLocalProvider(s.Dir())
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	got, err := p.Resolve(context.Background(), "rsa")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != pemValue {
		t.Fatalf("resolved length = %d, want %d (byte-for-byte)", len(got), len(pemValue))
	}
}
