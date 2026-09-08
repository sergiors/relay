package function

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// Dir is the fixed application-convention root the loader reads functions from.
const Dir = "/app/functions"

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
