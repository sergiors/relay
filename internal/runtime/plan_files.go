package runtime

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"relay/internal/runtime/plan"
)

// planFileDockerfileName is the context-relative path of the Dockerfile the
// builder renders after staging the plan files. A plan file can never own it:
// the generated Dockerfile is written to exactly this path, so a plan file here
// would be silently replaced and its generated identity lost.
const planFileDockerfileName = "Dockerfile"

// validatePlanFiles validates the WHOLE plan file list before anything is
// written and returns each file's context-relative slash path. Validating up
// front means a malformed later entry cannot leave a partially staged context,
// and the duplicate and reserved-name rules see the complete list.
func validatePlanFiles(files []plan.File) ([]string, error) {
	rels := make([]string, len(files))
	seen := make(map[string]struct{}, len(files))
	for i, f := range files {
		rel, err := planFileContextPath(f.Path)
		if err != nil {
			return nil, fmt.Errorf("plan file %d: %w", i, err)
		}
		if _, dup := seen[rel]; dup {
			return nil, fmt.Errorf("plan file %d: duplicate path %q", i, f.Path)
		}
		seen[rel] = struct{}{}
		rels[i] = rel
	}
	for i, rel := range rels {
		for parent := path.Dir(rel); parent != "."; parent = path.Dir(parent) {
			if _, ok := seen[parent]; ok {
				return nil, fmt.Errorf("plan file %d: path %q conflicts with parent file %q", i, files[i].Path, parent)
			}
		}
	}
	return rels, nil
}

// planFileContextPath validates one plan.File.Path and returns the equivalent
// path inside the build context (no leading separator). Plan paths name image
// destinations, so they are POSIX paths: the same string must mean the same
// place to the Dockerfile COPY and to the on-disk staging code regardless of
// the host. A path is REJECTED rather than repaired, so both consumers always
// derive the same relative path from the same accepted input.
func planFileContextPath(p string) (string, error) {
	switch {
	case p == "":
		return "", fmt.Errorf("path is empty")
	case !strings.HasPrefix(p, "/"):
		return "", fmt.Errorf("path %q must be absolute", p)
	case p == "/":
		return "", fmt.Errorf("path %q must not be the image root", p)
	case strings.IndexByte(p, '\\') >= 0:
		// A backslash is not a POSIX separator. In a Dockerfile it is the escape
		// and line-continuation byte, so it can also change how COPY parses the
		// token.
		return "", fmt.Errorf("path %q must not contain a backslash", p)
	}
	if err := validatePlanFilePathCharacters(p); err != nil {
		return "", fmt.Errorf("path %q: %w", p, err)
	}
	if path.Clean(p) != p {
		return "", fmt.Errorf(
			"path %q is not canonical: repeated, \".\", \"..\", or trailing separators",
			p,
		)
	}

	rel := strings.TrimPrefix(p, "/")
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf(
				"path %q must not contain an empty, \".\", or \"..\" segment", p,
			)
		}
	}
	if strings.HasPrefix(rel, "--") {
		// The relative path is rendered as the first COPY token, where a
		// double-dash prefix is parsed as an instruction flag (for example
		// --chmod) rather than as a source.
		return "", fmt.Errorf("path %q would render as a Dockerfile flag", p)
	}
	if rel == planFileDockerfileName {
		return "", fmt.Errorf("path %q collides with the generated Dockerfile", p)
	}
	if rel == ".dockerignore" || rel == "Dockerfile.dockerignore" {
		return "", fmt.Errorf("path %q collides with a reserved build-context control file", p)
	}
	return rel, nil
}

// validatePlanFilePathCharacters rejects bytes that have meaning to the
// Dockerfile parser inside an unquoted COPY source:
//
//   - whitespace ends or separates arguments and can terminate the line;
//   - control bytes (including NUL) are not valid path data;
//   - '\' escapes, and quotes delimit, COPY arguments;
//   - '$' starts build-arg/environment substitution;
//   - '*', '?' and '[' begin COPY's filepath.Match pattern syntax, so a path
//     containing them could name a different set of files than it appears to.
//
// The explicitly allowed ASCII punctuation has none of those meanings: '/' is
// the separator; '_', '-', '.', '+', '@', '%', '=', ':', ',', '~' and '#' are
// literal in both the tokenizer and COPY's pattern matching. '#' starts a
// Dockerfile comment only when it is the first non-space byte of a line, and a
// rendered COPY line starts with "COPY". Other punctuation is rejected rather
// than reasoned about case by case. Printable non-ASCII runes are allowed
// because the Dockerfile tokenizer passes their UTF-8 bytes through unchanged;
// the whole path must nevertheless be valid UTF-8.
func validatePlanFilePathCharacters(p string) error {
	if !utf8.ValidString(p) {
		// Invalid UTF-8 would otherwise decode as RuneError and be classified as
		// printable; the Dockerfile is text, so a path must be valid text too.
		return fmt.Errorf("must be valid UTF-8")
	}
	for _, r := range p {
		if r <= unicode.MaxASCII {
			if planFilePathASCIIByte(byte(r)) {
				continue
			}
			switch r {
			case ' ', '\t', '\n', '\r', '\v', '\f':
				return fmt.Errorf("must not contain whitespace")
			case 0x7f:
				return fmt.Errorf("must not contain control characters")
			}
			if r < 0x20 {
				return fmt.Errorf("must not contain control characters")
			}
			return fmt.Errorf("%q is unsafe in an unquoted COPY source", r)
		}
		if !unicode.IsPrint(r) || unicode.IsSpace(r) {
			return fmt.Errorf("must contain only printable, non-space characters")
		}
	}
	return nil
}

// planFilePathASCIIByte reports whether c is literal inside an unquoted COPY
// source (see validatePlanFilePathCharacters).
func planFilePathASCIIByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '/', c == '_', c == '-', c == '.', c == '+', c == '@',
		c == '%', c == '=', c == ':', c == ',', c == '~', c == '#':
		return true
	}
	return false
}

// writePlanFiles writes the generated plan files (bootstrap, injected
// package.json) into the staged build context. The caller's app directory is
// never modified: generated files live only in the transient context.
//
// The whole list is validated before the first byte is written, and every write
// is confined to ctxDir component by component. The snapshot root is an
// exclusive, private (mode 0700) directory owned by this preparation; it is
// checked to be a real directory, and the snapshot stages only regular files
// and directories (never symlinks from the source tree), so the app cannot
// introduce a link inside the context. The containment checks below are defense
// in depth against malformed plan data and a tampered context, not a boundary
// against a hostile process. They are a sequence of Lstat/Mkdir/open calls, not
// an atomic openat-style confinement: an actor that can already write inside
// this private root could still replace a component between the check and the
// write. Plan data is trusted (embedded engine constants).
func writePlanFiles(ctxDir, name string, files []plan.File) error {
	rels, err := validatePlanFiles(files)
	if err != nil {
		return fmt.Errorf("app %q: %w", name, err)
	}
	if !filepath.IsAbs(ctxDir) {
		return fmt.Errorf("app %q: build context %q must be absolute", name, ctxDir)
	}
	root := filepath.Clean(ctxDir)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("app %q: inspect build context %s: %w", name, root, err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("app %q: build context %s is a symlink", name, root)
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("app %q: build context %s is not a directory", name, root)
	}
	for i, f := range files {
		if err := writePlanFile(root, rels[i], f); err != nil {
			return fmt.Errorf("app %q: %w", name, err)
		}
	}
	return nil
}

// writePlanFile writes one validated plan file under root. A generated file
// intentionally replaces a staged source file at the same context path, which
// is why an existing regular file is overwritten (never a symlink or any other
// non-regular target).
func writePlanFile(root, rel string, f plan.File) error {
	target, err := planFileTarget(root, rel)
	if err != nil {
		return fmt.Errorf("write %s: %w", f.Path, err)
	}

	info, err := os.Lstat(target)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("write %s: refusing to replace symlink %s", f.Path, target)
		}
		if info.IsDir() {
			return fmt.Errorf("write %s: %s is a directory", f.Path, target)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("write %s: %s is not a regular file", f.Path, target)
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("write %s: inspect %s: %w", f.Path, target, err)
	}

	mode := f.Mode
	if mode == 0 {
		mode = 0o644
	}
	if err := os.WriteFile(target, f.Content, mode); err != nil {
		return fmt.Errorf("write %s: %w", f.Path, err)
	}
	return nil
}

// planFileTarget resolves a safe relative path under root one component at
// a time, creating missing directories, and rejects a symlink at any existing
// parent or at the final target. The component walk confines every existing
// path to root; validating the relative path before any filesystem operation
// and the final lexical containment check keep that true even if a future
// caller bypasses planFileContextPath.
func planFileTarget(root, rel string) (string, error) {
	if rel == "" || filepath.IsAbs(rel) || filepath.VolumeName(rel) != "" || strings.ContainsRune(rel, '\\') {
		return "", fmt.Errorf("path %q escapes or is not a safe relative path", rel)
	}
	segments := strings.Split(rel, "/")
	for _, seg := range segments {
		if seg == ".." {
			return "", fmt.Errorf("path %q escapes the build context", rel)
		}
		if seg == "" || seg == "." {
			return "", fmt.Errorf("path %q is not a safe relative path", rel)
		}
	}
	dir := root
	for _, seg := range segments[:len(segments)-1] {
		dir = filepath.Join(dir, seg)
		if err := ensurePlanDir(dir); err != nil {
			return "", err
		}
	}
	target := filepath.Join(dir, segments[len(segments)-1])
	inside, err := filepath.Rel(root, target)
	if err != nil {
		return "", fmt.Errorf("path %q: %w", rel, err)
	}
	if filepath.ToSlash(inside) != rel {
		return "", fmt.Errorf("path %q escapes the build context", rel)
	}
	return target, nil
}

// ensurePlanDir creates one missing context directory and rejects any existing
// path that is not a real directory. A directory created concurrently
// (os.Mkdir returning ErrExist) is inspected again rather than trusted, so a
// symlink created in that window is still rejected.
func ensurePlanDir(dir string) error {
	info, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		info, err = os.Lstat(dir)
	}
	if err != nil {
		return fmt.Errorf("inspect %s: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s is a symlink; refusing to write through it", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	return nil
}
