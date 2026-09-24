package source

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// IgnoreFile is the per-directory file Relay reads source-selection rules from.
// Only .gitignore is honored: Relay deliberately ignores no other exclude source
// (.git/info/exclude, core.excludesFile, or the operator's global gitignore) so
// selection is self-contained and reproducible from the tree alone.
const IgnoreFile = ".gitignore"

// ErrNotExist reports that the selected source directory does not exist. Callers
// that must distinguish "the configured source is absent" (a hard sync failure)
// from other selection errors can test it with errors.Is.
var ErrNotExist = errors.New("source: directory does not exist")

// Selection is a resolved source-selection policy: the directory being selected
// (dir), the scope its ignore rules are anchored to (root), and the compiled
// rules that decide which paths under dir are part of the source.
//
// A Selection is read-only after construction. It never writes to the
// filesystem, so the git materializer, the build-context stager, and the
// fingerprint can share one policy.
type Selection struct {
	// root is the directory ignore patterns are anchored to. Rules are read from
	// root down to dir (inclusive) plus every non-ignored descendant of dir, so a
	// monorepo's root-level .gitignore applies to a selected subtree exactly as it
	// would to git. For a self-contained directory (ForDir) root == dir.
	root string
	// dir is the selected source directory: the tree walked by WalkDir.
	dir string
	// selRel is dir relative to root, as a slash path ("" at the root). It is the
	// path that is always included: the selected directory is explicitly chosen,
	// so an ancestor rule matching it does not suppress the selection itself.
	selRel string
	// matcher decides whether a root-relative path is ignored. A Selection with no
	// rules still has a matcher (an empty one).
	matcher gitignore.Matcher
	// ignoreFiles is the set of applicable .gitignore files, keyed by root-relative
	// slash path. These are always reported by IgnoreFiles and always included by
	// Includes/WalkDir even when a pattern would otherwise ignore them: the
	// selection policy itself must never be silently dropped from a copy or a
	// fingerprint, or editing it would not be observable.
	ignoreFiles map[string]bool
	// descendantOnly contains root-relative directories matched by `dir/**`.
	// Git keeps such a directory traversable while ignoring its contents.
	descendantOnly map[string]bool
}

// ForDir builds a Selection for a self-contained source directory: the ignore
// rules anchored at dir apply to dir and its descendants. It is the
// fingerprint/build-context constructor, where the tree is the source root and no
// checkout above it is known.
func ForDir(dir string) (*Selection, error) {
	return New(dir, dir)
}

// New builds a Selection for dir, reading .gitignore rules from root down to dir
// (inclusive) and then through dir's non-ignored descendants. An empty root means
// dir is its own root. dir must reside inside root.
//
// The selected directory is treated as explicitly chosen, so an ancestor ignore
// rule matching dir itself does not suppress it (the operator configured this
// path on purpose); rules still govern everything beneath it.
//
// A missing dir returns an error wrapping ErrNotExist.
func New(root, dir string) (*Selection, error) {
	if root == "" {
		root = dir
	}
	root = filepath.Clean(root)
	dir = filepath.Clean(dir)

	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %q", ErrNotExist, dir)
		}
		return nil, fmt.Errorf("source: stat %q: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("source: %q is not a directory", dir)
	}

	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return nil, fmt.Errorf("source: relate %q to %q: %w", dir, root, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("source: dir %q is outside root %q", dir, root)
	}

	selection := &Selection{root: root, dir: dir, ignoreFiles: map[string]bool{}, descendantOnly: map[string]bool{}}
	if rel != "." {
		selection.selRel = filepath.ToSlash(rel)
	}
	patterns, err := selection.gather()
	if err != nil {
		return nil, err
	}
	selection.matcher = gitignore.NewMatcher(patterns)
	return selection, nil
}

// Root returns the anchor directory the selection's ignore rules are relative to.
func (s *Selection) Root() string { return s.root }

// Dir returns the selected source directory.
func (s *Selection) Dir() string { return s.dir }

// Sub returns a Selection over a subdirectory of the selected tree. It shares the
// parent's root and compiled rules, so root-relative matching (and therefore
// ancestor rules) still applies; only the walked subtree changes. It is how the
// materializer selects one function directory without re-reading rules.
func (s *Selection) Sub(dir string) (*Selection, error) {
	dir = filepath.Clean(dir)
	rel, err := filepath.Rel(s.dir, dir)
	if err != nil {
		return nil, fmt.Errorf("source: relate %q to %q: %w", dir, s.dir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("source: dir %q is outside %q", dir, s.dir)
	}
	sub := &Selection{root: s.root, dir: dir, matcher: s.matcher, ignoreFiles: s.ignoreFiles, descendantOnly: s.descendantOnly}
	if rootRel, rerr := filepath.Rel(s.root, dir); rerr == nil && rootRel != "." {
		sub.selRel = filepath.ToSlash(rootRel)
	}
	return sub, nil
}

// Rel returns path as a root-relative slash path. It is the argument form
// Includes expects.
func (s *Selection) Rel(path string) (string, error) {
	rel, err := filepath.Rel(s.root, path)
	if err != nil {
		return "", fmt.Errorf("source: relate %q to %q: %w", path, s.root, err)
	}
	return filepath.ToSlash(rel), nil
}

// Includes reports whether the root-relative slash path rel is part of the
// source. The selected directory itself is always included. Applicable
// .gitignore files and ".git" are special-cased: ignore files are always source,
// ".git" never is.
func (s *Selection) Includes(rel string, isDir bool) bool {
	rel = strings.TrimSuffix(filepath.ToSlash(rel), "/")
	if rel == "." || rel == "" || rel == s.selRel {
		return true
	}
	if s.ignoreFiles[rel] {
		return true
	}
	if isDir && s.descendantOnly[rel] {
		return true
	}
	parts := strings.Split(rel, "/")
	if slices.Contains(parts, ".git") {
		return false
	}
	// Git does not mark a directory ignored for a pattern such as dir/**;
	// keeping the directory traversable is necessary to discover the ignored
	// children. go-git models that distinction with a non-directory match.
	return !s.matcher.Match(parts, isDir)
}

// IncludesPath is Includes for a filesystem path. A path outside the root is not
// included.
func (s *Selection) IncludesPath(path string, isDir bool) bool {
	rel, err := s.Rel(path)
	if err != nil {
		return false
	}
	return s.Includes(rel, isDir)
}

// IgnoreFiles returns the absolute paths of the applicable .gitignore files, in
// sorted order. They are the files whose content is part of the selection policy;
// callers fold them into a fingerprint or preserve them in a copy. For a Sub
// selection it returns the files gathered for the whole parent source, so callers
// that want a subtree's rules should walk (WalkDir visits exactly the subtree's
// applicable ignore files).
func (s *Selection) IgnoreFiles() []string {
	out := make([]string, 0, len(s.ignoreFiles))
	for rel := range s.ignoreFiles {
		out = append(out, filepath.Join(s.root, filepath.FromSlash(rel)))
	}
	sort.Strings(out)
	return out
}

// ApplicableIgnoreFiles returns the ignore files that can affect this selected
// directory: files on its ancestor chain and files below it. This is narrower
// than IgnoreFiles for a Sub selection, whose parent selection may also know
// about sibling policies.
func (s *Selection) ApplicableIgnoreFiles() []string {
	var out []string
	for rel := range s.ignoreFiles {
		path := filepath.Join(s.root, filepath.FromSlash(rel))
		dir := filepath.Dir(path)
		if path == filepath.Join(s.dir, IgnoreFile) ||
			strings.HasPrefix(s.dir+string(filepath.Separator), dir+string(filepath.Separator)) ||
			strings.HasPrefix(path, s.dir+string(filepath.Separator)) {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

// WalkDir walks the selected source tree, calling fn for every included entry
// (the selected root, included directories, included files, and applicable
// .gitignore files). Ignored entries and ".git" are never visited: an ignored
// directory is skipped with fs.SkipDir just as git refuses to descend into it, so
// a nested .gitignore can never re-include a path whose parent was ignored.
//
// Paths passed to fn are as filepath.WalkDir produces them (absolute when dir is
// absolute); fn's error handling mirrors filepath.WalkDir.
func (s *Selection) WalkDir(fn fs.WalkDirFunc) error {
	return filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == ".git" {
			return fs.SkipDir
		}
		if path != s.dir {
			rel, rerr := s.Rel(path)
			if rerr != nil {
				return rerr
			}
			if !s.Includes(rel, d.IsDir()) {
				if d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
		}
		return fn(path, d, nil)
	})
}

// gather reads the applicable .gitignore files and compiles their patterns in
// increasing priority order (root first, deeper last), matching git: the deepest
// matching rule wins because the matcher evaluates patterns from last to first.
func (s *Selection) gather() ([]gitignore.Pattern, error) {
	var ps []gitignore.Pattern

	// Ancestors from root down to dir, inclusive. dir is the explicitly selected
	// root, so an ignore rule matching it does not stop these reads.
	acc := s.root
	ps, err := s.readIgnore(acc, ps)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(s.root, s.dir)
	if err != nil {
		return nil, fmt.Errorf("source: relate %q to %q: %w", s.dir, s.root, err)
	}
	if rel != "." {
		for part := range strings.SplitSeq(filepath.ToSlash(rel), "/") {
			acc = filepath.Join(acc, part)
			ps, err = s.readIgnore(acc, ps)
			if err != nil {
				return nil, err
			}
		}
	}

	return s.gatherChildren(s.dir, ps)
}

// gatherChildren recursively reads .gitignore files below dir, refusing to
// descend into directories the rules accumulated so far ignore. The matcher used
// to gate descent is built from the patterns known on entry, exactly like git: a
// .gitignore file inside an ignored directory is never read, and a sibling's
// rules cannot affect whether another sibling is descended into.
func (s *Selection) gatherChildren(dir string, ps []gitignore.Pattern) ([]gitignore.Pattern, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("source: read dir %q: %w", dir, err)
	}
	matcher := gitignore.NewMatcher(ps)
	for _, e := range entries {
		if !e.IsDir() || e.Name() == ".git" {
			continue
		}
		child := filepath.Join(dir, e.Name())
		childRel, rerr := filepath.Rel(s.root, child)
		if rerr != nil {
			return nil, fmt.Errorf("source: relate %q to %q: %w", child, s.root, rerr)
		}
		if matcher.Match(strings.Split(filepath.ToSlash(childRel), "/"), true) {
			continue
		}
		ps, err = s.readIgnore(child, ps)
		if err != nil {
			return nil, err
		}
		ps, err = s.gatherChildren(child, ps)
		if err != nil {
			return nil, err
		}
	}
	return ps, nil
}

// readIgnore appends the patterns in dir/.gitignore (if present) to ps. It
// records the file as applicable only when it exists, so IgnoreFiles never names
// a file that is not part of the source. A missing file is not an error: its
// absence is simply the absence of rules at that level.
func (s *Selection) readIgnore(dir string, ps []gitignore.Pattern) ([]gitignore.Pattern, error) {
	ignorePath := filepath.Join(dir, IgnoreFile)
	file, err := os.Open(ignorePath)
	if err != nil {
		if os.IsNotExist(err) {
			return ps, nil
		}
		return nil, fmt.Errorf("source: read %q: %w", ignorePath, err)
	}
	defer file.Close()

	rel, err := filepath.Rel(s.root, ignorePath)
	if err != nil {
		return nil, fmt.Errorf("source: relate %q to %q: %w", ignorePath, s.root, err)
	}
	s.ignoreFiles[filepath.ToSlash(rel)] = true

	domain := domainOf(s.root, dir)
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text()

		// Comments and blank lines carry no rule. An escaped leading '#' is a
		// literal pattern and must reach go-git's parser unchanged.
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		if p, ok := strings.CutSuffix(line, "/**"); ok {
			p = strings.TrimPrefix(p, "/")
			base := path.Join(strings.Join(domain, "/"), p)
			s.descendantOnly[strings.TrimPrefix(base, "./")] = true
		}

		ps = append(ps, gitignore.ParsePattern(line, domain))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("source: read %q: %w", ignorePath, err)
	}
	return ps, nil
}

// domainOf returns the slash path of dir relative to root as pattern-domain
// components (nil at the root). go-git anchors a pattern to the directory of its
// .gitignore, so a rule in a nested ignore file only matches paths beneath that
// directory.
func domainOf(root, dir string) []string {
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == "." {
		return nil
	}
	return strings.Split(filepath.ToSlash(rel), "/")
}
