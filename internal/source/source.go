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

// AppTemplateFile is the mandatory app definition file at a selection root. It
// mirrors app.TemplateFile and must stay equal to it; source cannot import app
// because app imports source. App discovery and the loader read it regardless of
// ignore rules, so it is always part of the selected source.
const AppTemplateFile = "template.yaml"

// ErrNotExist reports that the selected source directory does not exist. Callers
// that must distinguish "the configured source is absent" (a hard sync failure)
// from other selection errors can test it with errors.Is.
var ErrNotExist = errors.New("source: directory does not exist")

// Selection is a resolved source-selection policy for one app directory: the
// directory being selected (dir) and the compiled rules that decide which paths
// under dir are part of the source.
//
// The app directory is the selection root. Ignore files above it are never read:
// only the app's own .gitignore and those in its non-ignored descendants apply.
//
// A Selection is read-only after construction. It never writes to the
// filesystem, so the git materializer, the build-context stager, and the
// fingerprint can share one policy.
type Selection struct {
	// dir is the selected source directory: the tree walked by WalkDir and the
	// anchor for every ignore rule.
	dir string
	// matcher decides whether a directory-relative path is ignored. A Selection
	// with no rules still has a matcher (an empty one).
	matcher gitignore.Matcher
	// ignoreFiles is the set of applicable .gitignore files, keyed by
	// directory-relative slash path. These are always reported by IgnoreFiles
	// and always included by Includes/WalkDir even when a pattern would otherwise
	// ignore them: the selection policy itself must never be silently dropped
	// from a copy or a fingerprint, or editing it would not be observable.
	ignoreFiles map[string]bool
	// descendantOnly contains directory-relative directories matched by `dir/**`.
	// Git keeps such a directory traversable while ignoring its contents.
	descendantOnly map[string]bool
}

// ForDir builds a Selection rooted at dir: dir's own .gitignore and every
// .gitignore in its non-ignored descendants apply. Ignore files above dir are
// deliberately ignored, so an app directory's source policy is self-contained.
// A missing dir returns an error wrapping ErrNotExist.
func ForDir(dir string) (*Selection, error) {
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

	selection := &Selection{
		dir:            dir,
		ignoreFiles:    map[string]bool{},
		descendantOnly: map[string]bool{},
	}
	patterns, err := selection.gather()
	if err != nil {
		return nil, err
	}
	selection.matcher = gitignore.NewMatcher(patterns)
	return selection, nil
}

// Dir returns the selected source directory.
func (s *Selection) Dir() string { return s.dir }

// Rel returns path as a slash path relative to the selected directory. It is the
// argument form Includes expects.
func (s *Selection) Rel(path string) (string, error) {
	rel, err := filepath.Rel(s.dir, path)
	if err != nil {
		return "", fmt.Errorf("source: relate %q to %q: %w", path, s.dir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("source: path %q is outside selected directory %q", path, s.dir)
	}
	return filepath.ToSlash(rel), nil
}

// Includes reports whether the directory-relative slash path rel is part of the
// source. The selected directory itself is always included, as is its mandatory
// template and every applicable .gitignore file. ".git" is never source.
func (s *Selection) Includes(rel string, isDir bool) bool {
	rel = strings.TrimSuffix(filepath.ToSlash(rel), "/")
	if rel == "." || rel == "" || rel == AppTemplateFile {
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

// IncludesPath is Includes for a filesystem path. A path outside the selected
// directory is not included.
func (s *Selection) IncludesPath(path string, isDir bool) bool {
	rel, err := s.Rel(path)
	if err != nil {
		return false
	}
	return s.Includes(rel, isDir)
}

// IgnoreFiles returns the absolute paths of the selected directory's applicable
// .gitignore files, in sorted order. They are the files whose content is part of
// the selection policy; callers fold them into a fingerprint or preserve them in
// a copy.
func (s *Selection) IgnoreFiles() []string {
	out := make([]string, 0, len(s.ignoreFiles))
	for rel := range s.ignoreFiles {
		out = append(out, filepath.Join(s.dir, filepath.FromSlash(rel)))
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

// gather reads the .gitignore files at dir and in its non-ignored descendants,
// compiling their patterns in increasing priority order (shallower first, deeper
// last), matching git: the deepest matching rule wins because the matcher
// evaluates patterns from last to first.
func (s *Selection) gather() ([]gitignore.Pattern, error) {
	ps, err := s.readIgnore(s.dir, nil)
	if err != nil {
		return nil, err
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
		childRel, rerr := s.Rel(child)
		if rerr != nil {
			return nil, rerr
		}
		if matcher.Match(strings.Split(childRel, "/"), true) {
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

	rel, err := s.Rel(ignorePath)
	if err != nil {
		return nil, err
	}
	s.ignoreFiles[rel] = true

	domain := domainOf(s.dir, dir)
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
