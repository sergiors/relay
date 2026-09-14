package git

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"relay/internal/function"
)

// discoverFunctions returns the sorted set of valid function names under srcDir:
// each DIRECT subdirectory that has a template.yaml. Subdirectories without a
// template.yaml, files, and directories failing function.ValidName are ignored —
// the same discovery rules as internal/function/loader.go, so the set we
// materialize is exactly what the reconciler would load.
func discoverFunctions(srcDir string) ([]string, error) {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		// A missing source dir means the validated monorepo path (or the whole
		// checkout) is absent from the checked-out ref. That is a hard sync
		// failure: silently producing an empty /functions would look like a
		// successful "there are no functions yet" sync and could erase valid
		// materialized functions through the deterministic-removal rule.
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("git: source dir %q does not exist in the checkout", srcDir)
		}
		return nil, fmt.Errorf("git: read source dir %q: %w", srcDir, err)
	}
	// A successfully read but empty directory is a valid, deliberate "no
	// functions here" source, NOT an error: materialize still runs the
	// deterministic-removal pass so stale dirs are cleared. We return the empty
	// slice (non-nil not required) and the caller distinguishes it from an error
	// purely by the nil err.
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if function.ValidName(name) != nil {
			continue
		}
		if _, err := os.Stat(filepath.Join(srcDir, name, "template.yaml")); err != nil {
			continue // no template.yaml, or unreadable: not a function
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// materialize rewrites dstDir (the /functions root) so that it contains exactly
// the function directories discovered under srcDir, recursively and
// deterministically:
//
//   - Each function in the discovered set is copied/refreshed from srcDir into
//     dstDir. The copy is made "atomic-ish" per function: the tree is copied
//     into a temp directory beside dstDir, then the existing target is removed
//     and the temp renamed over it. A reader (the reconciler's watcher) sees at
//     worst the target absent for an instant — which the reconciler tolerates —
//     and never a half-copied function.
//   - Any pre-existing DIRECTORY in dstDir whose name is not in the new set is
//     removed, so /functions reflects exactly the configured source (the
//     deterministic-replace rule). Only directories are removed, never files,
//     and nothing outside dstDir is ever touched.
//
// REMOVED are treated by removal of the directory. The returned values are the
// names materialized (sorted) and removed (sorted) for the sync summary.
//
// Ownership note: once a git source is configured and synced, /functions is
// managed by git. Operator-placed function directories are subject to the same
// deterministic rule: a sync removes any directory not in the source. This is
// documented in README.md "Git" and in Sync's doc comment.
func materialize(srcDir, dstDir string) (materialized, removed []string, err error) {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("git: create functions dir %q: %w", dstDir, err)
	}

	newSet, err := discoverFunctions(srcDir)
	if err != nil {
		return nil, nil, err
	}
	want := make(map[string]bool, len(newSet))
	for _, n := range newSet {
		want[n] = true
	}

	// Copy/refresh each wanted function atomically-ish: temp dir beside dst,
	// remove old target, rename temp into place.
	for _, name := range newSet {
		if err := copyFunctionDir(filepath.Join(srcDir, name), dstDir, name); err != nil {
			return nil, nil, err
		}
		materialized = append(materialized, name)
	}

	// Remove directories in dstDir that are not in the new set. Only DIRECTORY
	// entries are considered; files and unreadable entries are left alone.
	//
	// Note on the variable name: dstEntries is deliberately distinct from the
	// newSet/entries naming above. Both reads must succeed independently and a
	// ReadDir error here must NOT be conflated with the discovery result that
	// drives the copy loop — an empty dstDir (nil dstEntries) is normal (first
	// sync, or /functions already empty) and is NOT the same as a read failure.
	dstEntries, err := os.ReadDir(dstDir)
	if err != nil {
		return nil, nil, fmt.Errorf("git: list functions dir %q: %w", dstDir, err)
	}
	for _, e := range dstEntries {
		if !e.IsDir() {
			continue
		}
		if want[e.Name()] {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dstDir, e.Name())); err != nil {
			return nil, nil, fmt.Errorf("git: remove stale function dir %q: %w", e.Name(), err)
		}
		removed = append(removed, e.Name())
	}
	sort.Strings(removed)
	return materialized, removed, nil
}

// copyFunctionDir copies the whole tree at src into dst/name atomically-ish: the
// tree is first copied to a temp directory created beside dst, then the existing
// dst/name is removed and the temp renamed into place, so the reconciler never
// observes a partially-written function directory. The source's own ".git" is
// never copied (functions do not contain one, and skipping it defensively avoids
// dragging a nested checkout into /functions). File modes are preserved.
func copyFunctionDir(src, dst, name string) error {
	tmp, err := os.MkdirTemp(dst, ".sync-*")
	if err != nil {
		return fmt.Errorf("git: create temp dir: %w", err)
	}
	tmpPath := tmp
	defer func() {
		// Clean up the temp dir unless it was renamed away.
		if tmpPath != "" {
			_ = os.RemoveAll(tmpPath)
		}
	}()

	if err := copyTree(src, tmpPath); err != nil {
		return err
	}

	target := filepath.Join(dst, name)
	// A brief absence is acceptable: the reconciler tolerates a missing
	// template (ErrNotReady) and retried loads.
	if _, err := os.Stat(target); err == nil {
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("git: remove existing function dir %q: %w", name, err)
		}
	}
	if err := os.Rename(tmpPath, target); err != nil {
		return fmt.Errorf("git: rename function dir %q: %w", name, err)
	}
	tmpPath = "" // renamed away; skip deferred cleanup
	return nil
}

// copyTree recursively copies the directory tree at src into dst (which must
// already exist), preserving file permissions and skipping any ".git" directory
// (functions never contain one; skipping it defensively keeps a nested repo out
// of /functions). Symlinks are dereferenced and copied as regular files/dirs by
// walking the target, mirroring a naive cp -r.
func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if info.IsDir() {
			if info.Name() == ".git" {
				return filepath.SkipDir
			}
			return os.MkdirAll(filepath.Join(dst, rel), info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, filepath.Join(dst, rel), info.Mode().Perm())
	})
}

// copyFile copies a single regular file, preserving its mode.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("git: open source file %q: %w", src, err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("git: create file %q: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("git: copy file %q: %w", dst, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("git: close file %q: %w", dst, err)
	}
	return nil
}
