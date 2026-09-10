package secrets

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

// secretNamePattern restricts the characters a secret name may contain. It is
// deliberately the same shape as a function name (see internal/function) so a
// secret reference is always a safe, single path component: it can never
// contain a path separator, an absolute path, or "..", which is what lets the
// store resolve it to a file under the secrets directory without any escaping
// risk. The rule is duplicated in internal/function (which is a leaf package and
// cannot import this one); the two are pinned equivalent by a cross-check test
// in this package.
var secretNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// maxSecretNameLen caps secret-name length, matching the function-name cap so a
// secret reference can always be a valid function name too.
const maxSecretNameLen = 63

// ValidateName reports whether name is a legal secret name, returning nil when
// it is and a descriptive error otherwise. It rejects empty names, names over
// maxSecretNameLen characters, names containing path separators or "..", and
// names ending in '.'. The value is never included in the error.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("invalid secret name: must not be empty")
	}
	if len(name) > maxSecretNameLen {
		return fmt.Errorf("invalid secret name: must be at most %d characters", maxSecretNameLen)
	}
	if !secretNamePattern.MatchString(name) {
		return fmt.Errorf("invalid secret name %q: must match [a-z0-9][a-z0-9._-]*", name)
	}
	if name[len(name)-1] == '.' {
		return fmt.Errorf("invalid secret name %q: must not end with '.'", name)
	}
	return nil
}

// LocalStore reads and writes secrets as files under a fixed directory: one
// file per secret, named by the secret reference. It is the CLI's write path
// and the LocalProvider's read path. The directory is created lazily on Set
// (never on construction or Resolve), so a store over a missing directory is
// valid until a write happens.
type LocalStore struct {
	dir string
}

// NewLocal returns a store rooted at dir. It does not create the directory;
// that happens lazily on Set. dir is fixed for the lifetime of the store.
func NewLocal(dir string) (*LocalStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("secrets: empty directory")
	}
	return &LocalStore{dir: dir}, nil
}

// Dir returns the store's root directory.
func (s *LocalStore) Dir() string { return s.dir }

// path resolves a validated secret name to its file path. It is only called
// after ValidateName, so the name is a single safe path component and the join
// cannot escape the directory.
func (s *LocalStore) path(name string) string {
	return filepath.Join(s.dir, name)
}

// Resolve returns the value for the named secret, reading the file at
// dir/name. The stored bytes are authoritative: they are returned exactly as
// stored, with no trimming. (The CLI strips a single trailing newline from
// terminal input before writing; a file placed by other means is used
// verbatim.) A missing secret returns an error wrapping os.ErrNotExist with the
// name; the value is never part of any error.
func (s *LocalStore) Resolve(ctx context.Context, name string) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}
	data, err := os.ReadFile(s.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("secret %q not found: %w", name, os.ErrNotExist)
		}
		return "", fmt.Errorf("secret %q: read: %w", name, err)
	}
	return string(data), nil
}

// Set writes (or overwrites) a secret atomically. The write is atomic on the
// same filesystem: the value is written to a temp file in the same directory,
// fsynced, then renamed over the target, so a reader never observes a partial
// value. The directory is created with 0700 and the file with 0600, so only the
// owning user can read or list secrets. On any error the temp file is removed.
func (s *LocalStore) Set(ctx context.Context, name, value string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return fmt.Errorf("secret %q: create dir: %w", name, err)
	}
	tmp, err := os.CreateTemp(s.dir, ".secret-*")
	if err != nil {
		return fmt.Errorf("secret %q: create temp: %w", name, err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup on every path. After a successful rename the temp
	// path no longer exists, so this deferred remove is a harmless no-op.
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secret %q: chmod temp: %w", name, err)
	}
	if _, err := tmp.WriteString(value); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secret %q: write temp: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secret %q: sync temp: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("secret %q: close temp: %w", name, err)
	}
	if err := os.Rename(tmpName, s.path(name)); err != nil {
		return fmt.Errorf("secret %q: rename: %w", name, err)
	}
	return nil
}

// Delete removes a secret. A missing secret returns an error wrapping
// os.ErrNotExist with the name.
func (s *LocalStore) Delete(ctx context.Context, name string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	err := os.Remove(s.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("secret %q not found: %w", name, os.ErrNotExist)
		}
		return fmt.Errorf("secret %q: remove: %w", name, err)
	}
	return nil
}

// List returns the sorted names of the secrets in the store. Subdirectories are
// skipped (never read), and file contents are never read. A missing directory
// yields an empty list.
func (s *LocalStore) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("secrets: list dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// Stat reports whether a secret exists. It is used by the CLI for rm/set
// messaging.
func (s *LocalStore) Stat(ctx context.Context, name string) (bool, error) {
	if err := ValidateName(name); err != nil {
		return false, err
	}
	_, err := os.Stat(s.path(name))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("secret %q: stat: %w", name, err)
}

// LocalProvider is a Provider backed by a LocalStore. It is the production
// provider the worker wires at startup; it is infallible to construct (the
// directory is created lazily on Set, never on Resolve), and a missing secret
// surfaces as a per-invocation Resolve error rather than a startup failure.
type LocalProvider struct {
	store *LocalStore
}

// NewLocalProvider returns a Provider that resolves secrets from dir.
func NewLocalProvider(dir string) (*LocalProvider, error) {
	store, err := NewLocal(dir)
	if err != nil {
		return nil, err
	}
	return &LocalProvider{store: store}, nil
}

// Resolve implements Provider.
func (p *LocalProvider) Resolve(ctx context.Context, name string) (string, error) {
	return p.store.Resolve(ctx, name)
}

// ensure LocalProvider satisfies Provider at compile time.
var _ Provider = (*LocalProvider)(nil)
