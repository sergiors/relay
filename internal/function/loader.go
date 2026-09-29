package function

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

// Dir is the fixed application-convention root the loader reads functions from.
const Dir = "/functions"

// Function is a loaded function: its name (from the directory name), directory
// path (used later for image building), and parsed template.
type Function struct {
	Name     string
	Dir      string
	Template *Template
}

// Loader discovers functions from a directory: each direct subdirectory is one
// function and must contain a template.yaml.
type Loader struct {
	dir string
	log *slog.Logger
}

func NewLoader(dir string, logger *slog.Logger) *Loader {
	return &Loader{dir: dir, log: logger}
}

// ErrNotReady reports that a function directory exists but has no template yet;
// it should be retried on later changes rather than treated as a removal.
var ErrNotReady = errors.New("template.yaml not present")

// ErrInvalidPath reports a path that can never be a function directory: an
// illegal function name, a path that is not a DIRECT child of the functions
// root, a symlink, or a non-directory. It is deliberately distinct from
// ErrNotReady so a caller can tell "not yet copied" (retain and retry) from
// "never loadable" (retain any previously-loaded version and do not replace it).
var ErrInvalidPath = errors.New("not a valid function directory")

// resolveFunctionDir resolves name to its function directory under root, applying
// the discovery policy shared by startup discovery (Loader.Load) and live/
// periodic reload (LoadSingle): the name must be a legal, single-element
// function name; the directory must be a real DIRECT child of root; and a
// SYMLINK is never accepted (os.Lstat, not Stat, so an outside target the link
// points at can never be read, fingerprinted, or built).
//
// A missing directory wraps fs.ErrNotExist (so a caller can treat it as a
// removal); an illegal name, a non-child path, a symlink, or a non-directory is
// ErrInvalidPath; any other stat failure is returned as-is so a flaky read is
// never mistaken for a removal.
func resolveFunctionDir(root, name string) (string, error) {
	if err := ValidName(name); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidPath, err)
	}
	// ValidName already excludes separators and "."/".."; the base check is a
	// belt-and-braces single-element assertion before the lexical join below.
	if name != filepath.Base(name) {
		return "", fmt.Errorf("%w: %q is not a single path element", ErrInvalidPath, name)
	}
	dir := filepath.Join(root, name)
	// The join is lexical: confirm the result is a direct child of root so a
	// crafted name can never address an ancestor or a nested path.
	if filepath.Dir(dir) != filepath.Clean(root) {
		return "", fmt.Errorf("%w: %q is not a direct child of the functions root", ErrInvalidPath, name)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("function %q: %w", name, fs.ErrNotExist)
		}
		return "", fmt.Errorf("function %q: stat: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: %q is a symlink", ErrInvalidPath, name)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%w: %q is not a directory", ErrInvalidPath, name)
	}
	return dir, nil
}

// LoadSingle loads exactly one function from root by name, enforcing the same
// discovery policy as Loader.Load: a legal single-element name and a real, direct
// child directory of root that is not a symlink. It surfaces the cases
// distinctly so the reconciler can decide how to act: a missing directory wraps
// fs.ErrNotExist (a removal), an invalid name/symlink/non-directory is
// ErrInvalidPath (retain, never replace), a missing template.yaml is ErrNotReady
// (the directory may be mid-copy), an invalid template returns a parse error, and
// unreadable templates surface as an error too.
func LoadSingle(root, name string) (Function, error) {
	dir, err := resolveFunctionDir(root, name)
	if err != nil {
		return Function{}, err
	}
	templatePath := filepath.Join(dir, "template.yaml")
	data, err := os.ReadFile(templatePath)
	if err != nil {
		if os.IsNotExist(err) {
			return Function{}, ErrNotReady
		}
		return Function{}, fmt.Errorf("function %q: read template: %w", name, err)
	}
	tmpl, err := ParseTemplate(data)
	if err != nil {
		return Function{}, fmt.Errorf("function %q: %w", name, err)
	}
	return Function{Name: name, Dir: dir, Template: tmpl}, nil
}

// Load returns the successfully loaded functions. Directories without a
// template.yaml are silently skipped; invalid names, symlinked or non-directory
// entries, and invalid templates are logged and skipped without aborting the
// load. Each entry is loaded through LoadSingle, so startup discovery and live
// reload apply exactly the same path policy.
func (l *Loader) Load() ([]Function, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("read functions dir %q: %w", l.dir, err)
	}

	var functions []Function
	for _, entry := range entries {
		if !entry.IsDir() {
			// A file, or a symlink (ReadDir reports the link's own type, never
			// its target): never a function directory.
			continue
		}
		name := entry.Name()
		fn, err := LoadSingle(l.dir, name)
		if err != nil {
			if errors.Is(err, ErrNotReady) || errors.Is(err, fs.ErrNotExist) {
				// No template.yaml (or the directory vanished between the read
				// and the load): not a function; silently skip, matching the
				// historical "no template -> skip" behavior.
				continue
			}
			// An invalid name, symlink, non-directory, or invalid template:
			// logged and skipped rather than aborting the whole load.
			l.log.Warn("Function: invalid; skipping", "function", name, "error", err)
			continue
		}
		functions = append(functions, fn)
	}

	return functions, nil
}
