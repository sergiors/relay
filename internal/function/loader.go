package function

import (
	"errors"
	"fmt"
	"log"
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
	log *log.Logger
}

func NewLoader(dir string, logger *log.Logger) *Loader {
	return &Loader{dir: dir, log: logger}
}

// LoadSingle loads exactly one function from a directory, returning the parsed
// Function. It surfaces three cases distinctly so the reconciler can decide how
// to act: a missing template.yaml is reported as ErrNotReady (the directory may
// be mid-copy), an invalid template returns a parse error, and unreadable
// templates surface as an error too.
func LoadSingle(dir, name string) (Function, error) {
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

// ErrNotReady reports that a function directory exists but has no template yet;
// it should be retried on later changes rather than treated as a removal.
var ErrNotReady = errors.New("template.yaml not present")

// Load returns the successfully loaded functions. Directories without a
// template.yaml are silently skipped; invalid templates are logged and skipped
// without aborting the load.
func (l *Loader) Load() ([]Function, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("read functions dir %q: %w", l.dir, err)
	}

	var functions []Function
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		dir := filepath.Join(l.dir, name)

		// Validate the directory name before anything else: an invalid name can
		// never be a valid function, and skipping it (like a broken template) is
		// better than crashing on it. A stray non-function directory then simply
		// logs and is ignored.
		if err := ValidName(name); err != nil {
			l.log.Printf("function %q: invalid name: %v; skipping", name, err)
			continue
		}

		templatePath := filepath.Join(dir, "template.yaml")

		data, err := os.ReadFile(templatePath)
		if err != nil {
			if os.IsNotExist(err) {
				// No template.yaml -> silently skip (unlike template errors, which log).
				continue
			}
			l.log.Printf("function %q: read template: %v", name, err)
			continue
		}

		tmpl, err := ParseTemplate(data)
		if err != nil {
			l.log.Printf("function %q: invalid template: %v", name, err)
			continue
		}

		functions = append(functions, Function{Name: name, Dir: dir, Template: tmpl})
	}

	return functions, nil
}
