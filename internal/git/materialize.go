package git

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"relay/internal/function"
	"relay/internal/source"
)

// discoverFunctions returns the sorted set of valid function names under the
// selected source: each DIRECT subdirectory that has a template.yaml and is not
// excluded by the source-selection policy. Subdirectories without a
// template.yaml, files, and directories failing function.ValidName are ignored —
// the same discovery rules as internal/function/loader.go, so the set we
// materialize is exactly what the reconciler would load. An ignored directory is
// skipped just as the loader would (git never descends into it), so a function
// directory excluded by .gitignore is not materialized.
func discoverFunctions(sel *source.Selection) ([]string, error) {
	entries, err := os.ReadDir(sel.Dir())
	if err != nil {
		// A missing source dir means the validated monorepo path (or the whole
		// checkout) is absent from the checked-out ref. That is a hard sync
		// failure: silently producing an empty /functions would look like a
		// successful "there are no functions yet" sync and could erase valid
		// materialized functions through the deterministic-removal rule.
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("git: source dir %q does not exist in the checkout", sel.Dir())
		}
		return nil, fmt.Errorf("git: read source dir %q: %w", sel.Dir(), err)
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
		if !sel.IncludesPath(filepath.Join(sel.Dir(), name, "template.yaml"), false) {
			continue // excluded by the source-selection policy
		}
		if _, err := os.Stat(filepath.Join(sel.Dir(), name, "template.yaml")); err != nil {
			continue // no template.yaml, or unreadable: not a function
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// materialize rewrites dstDir (the /functions root) so that it contains exactly
// the function directories discovered under sel, recursively and
// deterministically:
//
//   - Each function in the discovered set is copied/refreshed from sel into
//     dstDir. The copy is made "atomic-ish" per function: the tree is copied
//     into a temp directory beside dstDir, then the existing target is removed
//     and the temp renamed over it. A reader (the reconciler's watcher) sees at
//     worst the target absent for an instant — which the reconciler tolerates —
//     and never a half-copied function.
//   - Files excluded by the source-selection policy (the .gitignore rules
//     anchored at the checkout root) are NOT copied. The applicable .gitignore
//     files themselves ARE copied: they are the selection policy, so they must
//     travel with the function for the build context and fingerprint to keep
//     applying the same rules against /functions.
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
func materialize(sel *source.Selection, dstDir string) (materialized, removed []string, err error) {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("git: create functions dir %q: %w", dstDir, err)
	}

	newSet, err := discoverFunctions(sel)
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
		fnSel, err := sel.Sub(filepath.Join(sel.Dir(), name))
		if err != nil {
			return nil, nil, err
		}
		if err := copyFunctionDir(fnSel, dstDir, name); err != nil {
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

// copyFunctionDir copies the whole tree at sel into dst/name atomically-ish: the
// tree is first copied to a temp directory created beside dst, then the existing
// dst/name is removed and the temp renamed into place, so the reconciler never
// observes a partially-written function directory. Files the source-selection
// policy excludes are not copied; the applicable .gitignore files are (they are
// the policy and must travel with the function). File modes are preserved.
func copyFunctionDir(sel *source.Selection, dst, name string) error {
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

	if err := copyTree(sel, tmpPath); err != nil {
		return err
	}
	if err := preserveAncestorIgnoreFiles(sel, tmpPath); err != nil {
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

// preserveAncestorIgnoreFiles carries checkout-level policy into the
// materialized function. Without this, a subsequent runtime fingerprint would
// forget an ancestor .gitignore when the ancestor rule happened not to change
// the selected files.
func preserveAncestorIgnoreFiles(sel *source.Selection, dst string) error {
	local := filepath.Join(sel.Dir(), source.IgnoreFile)
	var contents []byte
	for _, path := range sel.ApplicableIgnoreFiles() {
		if path == local || strings.HasPrefix(path, sel.Dir()+string(filepath.Separator)) {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("git: read ancestor ignore file %q: %w", path, err)
		}
		contents = append(contents, b...)
		if len(b) > 0 && b[len(b)-1] != '\n' {
			contents = append(contents, '\n')
		}
	}
	if len(contents) == 0 {
		return nil
	}
	localBytes, err := os.ReadFile(local)
	if err == nil {
		contents = append(contents, localBytes...)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("git: read local ignore file %q: %w", local, err)
	}
	return os.WriteFile(filepath.Join(dst, source.IgnoreFile), contents, 0o644)
}

// copyTree recursively copies the selected source tree at sel into dst (which
// must already exist), preserving file permissions. Excluded files (the
// .gitignore rules) and ".git" are skipped by the selection itself. Symlinks are
// dereferenced and copied as regular files/dirs by walking the target, mirroring
// a naive cp -r.
func copyTree(sel *source.Selection, dst string) error {
	return sel.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sel.Dir(), path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		if d.IsDir() {
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
