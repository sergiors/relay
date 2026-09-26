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

	"relay/internal/source"
)

// Fingerprint returns a deterministic SHA-256 over the complete contents of the
// function's SELECTED source: every included file's slash-separated relative path
// plus its bytes, sorted by path. template.yaml is hashed VERBATIM, so any
// template edit is visible to the reconciler as a content change.
//
// Selection is the shared source policy (internal/source): the function's own
// .gitignore rules decide which files under dir are source. A file the rules
// ignore is not source, so its bytes never enter the digest — editing or removing
// it cannot force a rebuild. The applicable .gitignore files themselves ARE source
// (the policy is an input to selection), so their content is hashed: editing a
// rule changes the digest, which is exactly the change-detection guarantee a
// rule-only edit needs (a rule edit can alter the source set without any included
// file changing).
//
// Renames (path change), adds, removes, and content edits to included files all
// change the digest. File permissions are intentionally excluded: mode changes
// are rare and do not alter the image inputs (the build copies dirs wholesale).
// An unreadable included file — or an unreadable .gitignore — is surfaced as an
// error so the reconciler can retain the previous version rather than guessing.
func Fingerprint(dir string) (string, error) {
	selection, err := source.ForDir(dir)
	if err != nil {
		return "", fmt.Errorf("select %q: %w", dir, err)
	}
	return FingerprintSelection(selection)
}

// FingerprintFunction returns the content fingerprint that gates a function's
// reconciliation, choosing the narrowest input set that can actually change its
// behavior:
//
//   - a template that needs a runtime (events, schedules, an explicit runtime,
//     or an entrypoint service) builds and runs its own image, so the full
//     selected source is fingerprinted (see Fingerprint);
//   - a template that needs no runtime (its only services use external `image`
//     sources) never builds an image from source, so only template.yaml — the
//     sole file whose content affects the desired services, routing, env, and
//     secrets — is fingerprinted. Hashing the whole tree would read files that
//     cannot influence anything and would make an irrelevant source edit look
//     like a desired-state change.
//
// A nil template falls back to the full source fingerprint, so a caller that has
// not parsed one keeps the conservative behavior.
//
// It is the single fingerprint entry point the reconciler, worker, and state
// layers share, so every layer agrees on what "changed" means for a function.
func FingerprintFunction(dir string, tmpl *Template) (string, error) {
	if tmpl != nil && !tmpl.NeedsRuntime() {
		return fingerprintTemplate(dir)
	}
	return Fingerprint(dir)
}

// fingerprintTemplate hashes template.yaml alone, using the same per-entry
// framing FingerprintSelection uses (path, NUL, content, NUL) but with a
// distinct domain prefix, so a template-only fingerprint is never accidentally
// equal to a full-source fingerprint of a single-file tree. template.yaml is read
// directly rather than through the source selection: the loader parses it
// regardless of .gitignore rules, and for a no-runtime template it is the only
// input that matters.
func fingerprintTemplate(dir string) (string, error) {
	const name = "template.yaml"
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "", fmt.Errorf("read %q: %w", filepath.Join(dir, name), err)
	}
	h := sha256.New()
	io.WriteString(h, "template-only")
	h.Write([]byte{0})
	io.WriteString(h, name)
	h.Write([]byte{0})
	h.Write(raw)
	h.Write([]byte{0})
	return hex.EncodeToString(h.Sum(nil)), nil
}

// FingerprintSelection fingerprints an already-resolved source selection. It is
// the seam the manager uses when it has already selected a function's source (so
// the policy is not re-derived), and it keeps the hashing logic in one place.
func FingerprintSelection(selection *source.Selection) (string, error) {
	// Collect the included files as (hash path, filesystem path) pairs. The hash
	// path is root-relative and slash-separated so the serialization is canonical
	// and independent of how the walk produced absolute paths.
	type entry struct{ rel, path string }
	var entries []entry
	err := selection.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := selection.Rel(path)
		if err != nil {
			return err
		}
		entries = append(entries, entry{rel: rel, path: path})
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk %q: %w", selection.Dir(), err)
	}
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		seen[e.rel] = true
	}
	// A subtree walk cannot visit policy files above the selected directory,
	// but those files still affect the selected source and must be versioned.
	for _, path := range selection.ApplicableIgnoreFiles() {
		rel, err := selection.Rel(path)
		if err != nil {
			continue
		}
		if seen[rel] {
			continue
		}
		entries = append(entries, entry{rel: rel, path: path})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].rel < entries[j].rel })

	h := sha256.New()
	for _, e := range entries {
		io.WriteString(h, e.rel)
		h.Write([]byte{0})
		f, err := os.Open(e.path)
		if err != nil {
			return "", fmt.Errorf("read %q: %w", e.rel, err)
		}
		raw, err := io.ReadAll(f)
		if err != nil {
			_ = f.Close()
			return "", fmt.Errorf("read %q: %w", e.rel, err)
		}
		if err := f.Close(); err != nil {
			return "", fmt.Errorf("close %q: %w", e.rel, err)
		}
		h.Write(raw)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
