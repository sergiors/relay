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

// The runtime user is a fixed numeric identity (10001:10001) shared by every
// Relay function so the container never runs as root and the identity is stable
// across images and rebuilds; the numeric id is what the kernel enforces, so the
// user/group name is cosmetic. python:3.14-slim (Debian) has no pre-created user,
// so the Dockerfile creates one at build time via UserSetup.
const (
	userID   = "10001:10001"
	userName = "app"
	// Create the non-root runtime user and make application files readable
	// under the same ownership used by the container process.
	userSetup = "groupadd -g 10001 app && useradd -u 10001 -g 10001 -m -d /home/app -s /usr/sbin/nologin app && chown -R 10001:10001 /app /relay"
	// Disable Python bytecode writes because /app runs on a read-only rootfs.
	noBytecodeEnv = "PYTHONDONTWRITEBYTECODE=1"
)

// Plan returns the generic build plan for a Python function. It only inspects
// fnDir (for requirements.txt) and never writes into it.
func (Engine) Plan(spec plan.Spec, fnDir string) (plan.BuildPlan, error) {
	files := []plan.File{{
		Path:    "/relay/bootstrap.py",
		Content: bootstrap,
		Mode:    fs.FileMode(0o644),
	}}

	// A requirements.txt declares the function's dependency layer: a reusable
	// image that installs the requirements into /app (the same directory the
	// function image uses as its WORKDIR, so a FROM of the dependency image
	// inherits the installed packages in place). Without requirements.txt there
	// are no dependencies, so no Deps and no dependency image.
	var deps plan.Deps
	if _, err := os.Stat(filepath.Join(fnDir, dependenciesFile)); err == nil {
		deps = plan.Deps{
			Files:   []string{dependenciesFile},
			Install: installCommand,
			Dir:     workDir,
		}
	} else if !os.IsNotExist(err) {
		return plan.BuildPlan{}, fmt.Errorf("stat %s: %w", dependenciesFile, err)
	}

	return plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    workDir,
		Files:      files,
		Deps:       deps,
		UserSetup:  userSetup,
		User:       userID,
		Env:        []string{noBytecodeEnv},
		Entrypoint: entrypoint,
	}, nil
}
