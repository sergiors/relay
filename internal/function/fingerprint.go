package function

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// Fingerprint returns a deterministic SHA-256 over the complete contents of the
// function directory: every file's slash-separated relative path plus its bytes,
// sorted by path. Renames (path change), adds, removes, and content edits all
// change the digest. File permissions are intentionally excluded: mode changes
// are rare and do not alter the image inputs (the build copies dirs wholesale).
// An unreadable file is surfaced as an error so the reconciler can retain the
// previous version rather than guessing.
func Fingerprint(dir string) (string, error) {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk %q: %w", dir, err)
	}
	sort.Strings(paths)

	h := sha256.New()
	for _, rel := range paths {
		io.WriteString(h, rel)
		h.Write([]byte{0})
		f, err := os.Open(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			return "", fmt.Errorf("read %q: %w", rel, err)
		}
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return "", fmt.Errorf("read %q: %w", rel, err)
		}
		if err := f.Close(); err != nil {
			return "", fmt.Errorf("close %q: %w", rel, err)
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
