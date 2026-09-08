// Package python is the Python runtime engine. It holds the language-specific
// preparation knowledge for Python functions: the embedded bootstrap, the
// requirements.txt dependency handling, and the container entrypoint. It does
// NOT run docker build or generate Dockerfiles; it produces a generic
// runtime.BuildPlan that the Docker builder renders.
package python

import (
	_ "embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"relay/internal/runtime/plan"
)

//go:embed bootstrap.py
var bootstrap []byte

// Bootstrap exposes the embedded bootstrap so tests can verify its content.
var Bootstrap = bootstrap

type Engine struct{}

var entrypoint = []string{"python", "/relay/bootstrap.py"}

const workDir = "/app"
const dependenciesFile = "requirements.txt"
const installCommand = "pip install --no-cache-dir -r requirements.txt"

// Plan returns the generic build plan for a Python function. It only inspects
// fnDir (for requirements.txt) and never writes into it.
func (Engine) Plan(spec plan.Spec, fnDir string) (plan.BuildPlan, error) {
	files := []plan.File{{
		Path:    "/relay/bootstrap.py",
		Content: bootstrap,
		Mode:    fs.FileMode(0o644),
	}}

	var install []string
	if _, err := os.Stat(filepath.Join(fnDir, dependenciesFile)); err == nil {
		install = append(install, installCommand)
	} else if !os.IsNotExist(err) {
		return plan.BuildPlan{}, fmt.Errorf("stat %s: %w", dependenciesFile, err)
	}

	return plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    workDir,
		Files:      files,
		Install:    install,
		Entrypoint: entrypoint,
	}, nil
}
