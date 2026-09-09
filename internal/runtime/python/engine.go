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
	// userSetup runs as root at build time. /app is on a read-only rootfs at
	// runtime, but the user must own it so Python can write __pycache__ bytecode
	// next to imported sources (see the PYTHONDONTWRITEBYTECODE env below, which
	// disables that write so the read-only rootfs is never hit). /relay holds the
	// bootstrap, which the user only needs to read. chown uses the numeric id so
	// it is independent of the user/group name.
	userSetup = "groupadd -g 10001 app && useradd -u 10001 -g 10001 -m -d /home/app -s /usr/sbin/nologin app && chown -R 10001:10001 /app /relay"
	// PYTHONDONTWRITEBYTECODE stops Python from writing __pycache__ bytecode
	// next to imported sources. /app is on a read-only rootfs, so without this
	// the first import of a user module would fail trying to create a bytecode
	// cache directory. It is a runtime env, not a build env.
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
		UserSetup:  userSetup,
		User:       userID,
		Env:        []string{noBytecodeEnv},
		Entrypoint: entrypoint,
	}, nil
}
