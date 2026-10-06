package app

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"relay/internal/source"
)

// Fingerprint returns a deterministic SHA-256 over the complete contents of the
// app's SELECTED source: every included file's slash-separated relative path
// plus its bytes, sorted by path. template.yaml is hashed with its top-level
// `resources` mapping removed, so a resource-only edit is deliberately invisible
// to the digest (a resource change is applied to containers, not baked into the
// image; see stripTemplateResources).
//
// Selection is the shared source policy (internal/source): the app's own
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
	_, fp, err := SelectAndFingerprint(dir)
	return fp, err
}

// SelectAndFingerprint resolves dir's source-selection policy and computes the
// full-source fingerprint in one call, returning BOTH the resolved Selection and
// the digest. It exists so a caller that must hash an app AND stage its build
// context (the runtime Manager) resolves the selection once and shares it: the
// policy is not re-derived between the hash and the build, so a concurrent
// .gitignore edit cannot make the tag and the staged bytes disagree.
//
// On error the Selection is nil; callers that only need the digest call
// Fingerprint, which discards it.
func SelectAndFingerprint(dir string) (*source.Selection, string, error) {
	selection, err := source.ForDir(dir)
	if err != nil {
		return nil, "", fmt.Errorf("select %q: %w", dir, err)
	}
	fp, err := FingerprintSelection(selection)
	if err != nil {
		return nil, "", err
	}
	return selection, fp, nil
}

// FingerprintApp returns the content fingerprint that gates an app's
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
// layers share, so every layer agrees on what "changed" means for an app.
func FingerprintApp(dir string, tmpl *Template) (string, error) {
	_, fp, err := SelectAndFingerprintApp(dir, tmpl)
	return fp, err
}

// SelectAndFingerprintApp is FingerprintApp plus the resolved source
// selection when one is needed. It applies the same narrow-input rule as
// FingerprintApp: a no-runtime template is hashed over template.yaml alone
// and returns a nil Selection (no source is ever staged into an image); a
// runtime-backed (or nil) template resolves the full selection once and returns
// it alongside the digest, so a caller that will also build the app's image
// (the runtime Manager) shares one policy for both the tag and the staged files.
func SelectAndFingerprintApp(dir string, tmpl *Template) (*source.Selection, string, error) {
	if tmpl != nil && !tmpl.NeedsRuntime() {
		fp, err := fingerprintTemplate(dir)
		if err != nil {
			return nil, "", err
		}
		return nil, fp, nil
	}
	return SelectAndFingerprint(dir)
}

// fingerprintTemplate hashes template.yaml alone, using the same per-entry
// framing FingerprintSelection uses (path, NUL, content, NUL) but with a
// distinct domain prefix, so a template-only fingerprint is never accidentally
// equal to a full-source fingerprint of a single-file tree. template.yaml is read
// directly rather than through the source selection: the loader parses it
// regardless of .gitignore rules, and for a no-runtime template it is the only
// input that matters.
//
// The top-level `resources` mapping is stripped before hashing (see
// stripTemplateResources), so a resource-only edit never changes the digest. The
// strip re-serializes the YAML node tree, which means the FIRST fingerprint
// computed after this behavior was introduced may differ from one computed by an
// older Relay (a one-time rebuild per app on upgrade); subsequent
// fingerprints are stable and resource edits are invisible.
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
	// Strip the top-level `resources` mapping before hashing: resource limits are
	// per-container runtime configuration, not image inputs, so a resource-only
	// edit must not change the fingerprint (and must not trigger a rebuild).
	h.Write(stripTemplateResources(raw))
	h.Write([]byte{0})
	return hex.EncodeToString(h.Sum(nil)), nil
}

// FingerprintSelection fingerprints an already-resolved source selection. It is
// the seam the manager uses when it has already selected an app's source (so
// the policy is not re-derived), and it keeps the hashing logic in one place. It
// streams one file at a time (it never holds the whole tree in memory), so the
// reconciler's periodic audit stays cheap; a caller that must ALSO stage the
// same bytes uses CaptureSourceSnapshot instead.
func FingerprintSelection(selection *source.Selection) (string, error) {
	entries, err := collectSelectionEntries(selection)
	if err != nil {
		return "", err
	}
	templateRel := templateRelFor(selection)

	h := sha256.New()
	for _, e := range entries {
		if e.isDir {
			continue
		}
		raw, rerr := os.ReadFile(e.path)
		if rerr != nil {
			return "", fmt.Errorf("read %q: %w", e.rel, rerr)
		}
		writeFingerprintEntry(h, e.rel, templateRel, raw)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// snapshotEntry is one entry collected from a selection: its canonical
// root-relative path and filesystem path, its selected-directory-relative stage
// path, its mode, and whether it is a directory. It is the intermediate the
// streaming fingerprinter and the capturing snapshot share, so both see exactly
// the same selection and ordering.
type snapshotEntry struct {
	rel      string
	path     string
	stageRel string
	mode     fs.FileMode
	isDir    bool
}

// collectSelectionEntries walks the resolved selection once and returns every
// selected entry in canonical root-relative order. It is the single definition of
// "which files are source and in what order" shared by FingerprintSelection and
// CaptureSourceSnapshot, so the digest and the staged context can never select
// differently. Applicable ignore files ABOVE the selected directory are included
// (they affect selection) with an empty stageRel.
func collectSelectionEntries(selection *source.Selection) ([]snapshotEntry, error) {
	if selection == nil {
		return nil, fmt.Errorf("collect source entries: nil selection")
	}
	dir := selection.Dir()

	entries := make([]snapshotEntry, 0, 32)
	seen := make(map[string]bool, 32)
	err := selection.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := selection.Rel(path)
		if rerr != nil {
			return rerr
		}
		if rel == "." || seen[rel] {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		seen[rel] = true
		entries = append(entries, snapshotEntry{
			rel: rel, path: path, stageRel: stagedRel(dir, path),
			mode: info.Mode(), isDir: d.IsDir(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %q: %w", dir, err)
	}

	// A subtree walk cannot visit policy files ABOVE the selected directory, but
	// those files still affect the selected source and must be versioned. They are
	// fingerprinted but never staged: they live outside the image.
	for _, path := range selection.ApplicableIgnoreFiles() {
		rel, rerr := selection.Rel(path)
		if rerr != nil || rel == "." || seen[rel] {
			continue
		}
		seen[rel] = true
		entries = append(entries, snapshotEntry{
			rel: rel, path: path, mode: 0o644,
		})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].rel < entries[j].rel })
	return entries, nil
}

// templateRelFor returns the root-relative path of the selected directory's own
// template.yaml, or "" when the selection has none. Only that file has its
// top-level `resources` mapping stripped before hashing; a nested template.yaml
// is ordinary source.
func templateRelFor(selection *source.Selection) string {
	rel, err := selection.Rel(filepath.Join(selection.Dir(), "template.yaml"))
	if err != nil {
		return ""
	}
	return rel
}

// writeFingerprintEntry frames one entry's bytes into the digest using the
// canonical (path, NUL, content, NUL) serialization, stripping resources from the
// selected directory's own template.yaml. It is the ONE place that framing is
// defined, so the streaming fingerprinter and the snapshot fingerprinter can
// never drift.
func writeFingerprintEntry(h hash.Hash, rel, templateRel string, raw []byte) {
	io.WriteString(h, rel)
	h.Write([]byte{0})
	if rel != "" && rel == templateRel {
		raw = stripTemplateResources(raw)
	}
	h.Write(raw)
	h.Write([]byte{0})
}

// snapshotRootPattern is the os.MkdirTemp pattern for a captured source
// snapshot's private root. The root IS the app image build context: the runtime
// writes its generated Dockerfile and plan files into it and tars it, so the
// source is never copied a second time. The "relay-build-" prefix matches the
// transient build directory the runtime used to create (and that its cleanup
// tests observe); Discard removes it on every path.
const snapshotRootPattern = "relay-build-*"

// SnapshotEntry is one captured entry of an app's selected source. It carries
// only metadata: the captured bytes live on disk under the owning
// SourceSnapshot's private root (see Root), so a whole selected tree is never
// held in memory.
type SnapshotEntry struct {
	// Rel is the entry's ROOT-relative slash path: the canonical path the
	// fingerprint frames entries with (so the serialization is independent of how
	// the walk produced absolute paths and independent of the selected subtree).
	Rel string
	// StageRel is the entry's SELECTED-DIRECTORY-relative slash path: the path
	// the build context stages it at. It is empty when the entry is a policy file
	// above the selected directory (an ancestor .gitignore), which participates in
	// the fingerprint but is not part of the image.
	StageRel string
	// Mode is the entry's file mode at capture time.
	Mode fs.FileMode
	// IsDir reports whether the entry is a directory.
	IsDir bool
	// Regular reports whether the entry is a regular file. Only regular files are
	// staged into the build context (matching the historical stager); a
	// non-regular entry still participates in the fingerprint when it is selected.
	Regular bool
}

// SourceSnapshot is an immutable, single-read capture of an app's selected
// source. The content fingerprint and the runtime image build context BOTH derive
// from exactly this one read, so the tag can never describe one set of files
// while the image bakes another (the fingerprint-then-stage TOCTOU the previous
// selection-only handoff left open).
//
// The capture is DISK-BACKED: CaptureSourceSnapshot copies each selected regular
// file into the snapshot's private root (minus template.yaml, which is Relay
// configuration and never staged) while streaming it through the fingerprint
// hash, so a captured tree does not have to be held in memory. The root is the
// app image build context; the runtime writes its generated files there and tars
// it directly.
//
// A snapshot is owned by ONE preparation. CaptureSourceSnapshot reads every
// selected file once; the caller threads the same value through the fingerprint
// and the build-context staging and then releases it with Discard (a deferred
// call covers success, error, and cancellation).
type SourceSnapshot struct {
	// root is the private directory holding the staged selected source. It is the
	// build context and is removed by Discard (never by a caller).
	root    string
	entries []SnapshotEntry
	// fingerprint is the digest computed while capturing, so Fingerprint is a
	// pure accessor and is stable even after Discard.
	fingerprint string
}

// CaptureSourceSnapshot walks the resolved selection once and captures every
// selected entry's exact bytes and mode into a private on-disk root. It applies
// exactly the same selection and ordering as FingerprintSelection (both use
// collectSelectionEntries), and it hashes the bytes it stages, so the two can
// never disagree.
//
// Selection policy is preserved exactly: ignored files and .git are never
// captured; an applicable ancestor .gitignore is fingerprinted but not staged;
// template.yaml is fingerprinted but excluded from the build context by base name
// anywhere in the tree (with its subtree when it is a directory); a non-regular
// entry is fingerprinted but never staged.
//
// Any read error is surfaced and the private root is removed: a caller must never
// build an image from a tree it could not fully capture, because that would bake
// a partial (and so misidentified) source set.
func CaptureSourceSnapshot(selection *source.Selection) (*SourceSnapshot, error) {
	entries, err := collectSelectionEntries(selection)
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", snapshotRootPattern)
	if err != nil {
		return nil, fmt.Errorf("create source snapshot: %w", err)
	}
	snapshot, err := captureSnapshot(selection, entries, root)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	return snapshot, nil
}

// captureSnapshot fills root with the selected, staged source while streaming
// every entry through the fingerprint hash. It is the single place the staging
// policy and the hashing policy meet, so the digest and the staged context can
// never select or exclude differently.
func captureSnapshot(selection *source.Selection, entries []snapshotEntry, root string) (*SourceSnapshot, error) {
	templateRel := templateRelFor(selection)
	h := sha256.New()
	captured := make([]SnapshotEntry, 0, len(entries))
	// excludedDirs holds the stage-prefixes of directories literally named
	// template.yaml whose whole subtree is excluded from the context. Sorted entry
	// order guarantees a directory is seen before its children.
	var excludedDirs []string

	for _, e := range entries {
		entry := SnapshotEntry{Rel: e.rel, StageRel: e.stageRel, Mode: e.mode, IsDir: e.isDir, Regular: !e.isDir}
		if !e.isDir {
			entry.Regular = e.mode.IsRegular()
		}
		captured = append(captured, entry)

		if e.isDir {
			if e.stageRel != "" && filepath.Base(e.stageRel) == "template.yaml" {
				excludedDirs = append(excludedDirs, e.stageRel+"/")
				continue
			}
			if e.stageRel == "" || underStageDir(e.stageRel, excludedDirs) {
				continue
			}
			if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(e.stageRel)), e.mode.Perm()); err != nil {
				return nil, fmt.Errorf("stage %q: %w", e.rel, err)
			}
			continue
		}

		stageRel := ""
		if e.stageRel != "" && e.mode.IsRegular() &&
			filepath.Base(e.stageRel) != "template.yaml" &&
			!underStageDir(e.stageRel, excludedDirs) {
			stageRel = e.stageRel
		}
		if err := captureEntryBytes(h, e.path, e.rel, templateRel, root, stageRel, e.mode); err != nil {
			return nil, err
		}
	}

	return &SourceSnapshot{
		root:        root,
		entries:     captured,
		fingerprint: hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// captureEntryBytes frames one entry into the fingerprint exactly as
// writeFingerprintEntry does while staging its bytes into root when stageRel is
// non-empty. The template's own top-level `resources` stripping requires the
// whole document, so that one file (never staged) is read into memory; every
// other entry streams straight from source to hash and destination.
func captureEntryBytes(h hash.Hash, path, rel, templateRel, root, stageRel string, mode fs.FileMode) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read %q: %w", rel, err)
	}
	defer f.Close()

	io.WriteString(h, rel)
	h.Write([]byte{0})

	switch {
	case rel != "" && rel == templateRel:
		raw, rerr := io.ReadAll(f)
		if rerr != nil {
			return fmt.Errorf("read %q: %w", rel, rerr)
		}
		h.Write(stripTemplateResources(raw))
	case stageRel == "":
		if _, err := io.Copy(h, f); err != nil {
			return fmt.Errorf("read %q: %w", rel, err)
		}
	default:
		target := filepath.Join(root, filepath.FromSlash(stageRel))
		if dir := filepath.Dir(target); dir != root {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("stage %q: %w", rel, err)
			}
		}
		dst, cerr := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode.Perm())
		if cerr != nil {
			return fmt.Errorf("stage %q: %w", rel, cerr)
		}
		if _, err := io.Copy(io.MultiWriter(h, dst), f); err != nil {
			_ = dst.Close()
			return fmt.Errorf("read %q: %w", rel, err)
		}
		if err := dst.Close(); err != nil {
			return fmt.Errorf("stage %q: %w", rel, err)
		}
	}

	h.Write([]byte{0})
	return nil
}

// Fingerprint returns the content digest over the snapshot's captured files,
// using the same per-entry framing, canonical ordering, and template-resource
// stripping as FingerprintSelection. It was computed while capturing, so it is
// stable across on-disk mutation and even after Discard.
//
// The digest is always a non-empty 64-hex SHA-256 for a successfully captured
// snapshot (even an empty selected tree hashes the empty input), so a caller can
// treat "" as "no identity" without ambiguity.
func (s *SourceSnapshot) Fingerprint() string {
	if s == nil {
		return ""
	}
	return s.fingerprint
}

// Entries returns the captured entry metadata in canonical root-relative order.
// The returned slice must not be mutated; entries with an empty StageRel are
// policy files outside the selected tree and are not part of the build context.
// A directory literally named template.yaml is present as metadata but its
// subtree is not staged.
func (s *SourceSnapshot) Entries() []SnapshotEntry {
	if s == nil {
		return nil
	}
	return s.entries
}

// Root returns the private directory holding the snapshot's staged selected
// source: the exact regular files (minus template.yaml) the fingerprint was
// derived from. The runtime uses it directly as the app image build context, so
// the source is never copied a second time. It is "" after Discard, and the
// caller must never remove it: Discard owns its lifetime.
func (s *SourceSnapshot) Root() string {
	if s == nil {
		return ""
	}
	return s.root
}

// Discard releases the snapshot's staged source by removing its private root and
// dropping the entry metadata. It is the explicit cleanup for one preparation
// and is owned by the capturing caller, so capturing code must defer it: a
// preparation that succeeds, fails, or is cancelled must not retain (or leak) the
// captured source. It is nil-safe and idempotent.
func (s *SourceSnapshot) Discard() {
	if s == nil {
		return
	}
	if s.root != "" {
		_ = os.RemoveAll(s.root)
		s.root = ""
	}
	s.entries = nil
}

// underStageDir reports whether the slash path rel lies inside one of the
// excluded directory prefixes.
func underStageDir(rel string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(rel, p) {
			return true
		}
	}
	return false
}

// stagedRel returns path's path relative to the selected directory dir as a slash
// path, or "" when it lies outside dir (an ancestor policy file).
func stagedRel(dir, path string) string {
	rel, err := filepath.Rel(dir, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(rel)
}
