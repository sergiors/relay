// Package python is the Python runtime engine. It holds the language-specific
// preparation knowledge for Python functions: the embedded bootstrap, the
// dependency handling (requirements.txt and native uv projects), and the
// container entrypoint. It does NOT run docker build or generate Dockerfiles; it
// produces a generic runtime.BuildPlan that the Docker builder renders.
//
// Dependencies are installed with the uv binary (copied into the runtime image
// from the pinned official uv image; see the runtime registry) rather than with
// pip, so both the classic requirements.txt path and a native uv project use the
// same fast, locked installer and land in the system site-packages the runtime
// entrypoint already uses.
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

// entrypoint runs the bootstrap unbuffered (-u): user print() output must
// keep streaming live through the attach pipe while the reused container
// stays running, instead of block-buffering into the pipe for its lifetime.
var entrypoint = []string{"python", "-u", "/relay/bootstrap.py"}

const (
	workDir = "/app"
	// requirementsFile is the classic pip-style manifest, still fully supported.
	requirementsFile = "requirements.txt"
	// pyprojectFile and uvLockFile are the native uv project pair. A committed
	// uv.lock selects the native path deterministically and is never resolved
	// fresh (see plan): both files must be present together.
	pyprojectFile = "pyproject.toml"
	uvLockFile    = "uv.lock"
	// exportedRequirements is the transient path the native workflow exports
	// the locked requirements to before installing them. Relay owns the name;
	// the install command removes it again.
	exportedRequirements = "/tmp/uv-requirements.txt"
)

// install commands (run inside the dependency image, after uv has been copied
// in). Both install into the SYSTEM site-packages (--system) so the existing
// `python -u /relay/bootstrap.py` entrypoint sees the packages; no project-local
// .venv is created, keeping runtime execution and the dependency image's
// FROM-inheritance unchanged.
const (
	// requirementsInstall installs a plain requirements.txt with uv's pip
	// interface. --no-cache keeps the layer small.
	requirementsInstall = "uv pip install --system --no-cache -r " + requirementsFile

	// nativeInstall exports the locked dependency set from the native uv project
	// and installs it, without re-resolving: `--locked` asserts the committed
	// uv.lock is up to date and fails the build if it is not, `--no-dev` drops
	// development groups, and `--no-emit-project` excludes the project itself
	// (Relay only installs the project's dependencies into the base layer; the
	// function's own source is copied by the function image). The export goes to
	// a file (not a pipe) and is chained with && so an export failure aborts the
	// build instead of silently installing an empty set.
	nativeInstall = "uv export --locked --no-dev --no-emit-project -o " + exportedRequirements +
		" && uv pip install --system --no-cache -r " + exportedRequirements +
		" && rm -f " + exportedRequirements
)

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
// fnDir (for the dependency manifests) and never writes into it.
//
// Dependency detection is deterministic:
//
//   - uv.lock + pyproject.toml: the native uv path, installed from the committed
//     lock via `uv export --locked` + `uv pip install` (no re-resolution).
//     requirements.txt is ignored in this case: a committed lock is the
//     strongest, most specific statement of the dependency set.
//   - uv.lock without pyproject.toml: an error — a lock alone cannot be
//     validated or exported.
//   - requirements.txt (with or without an unrelated pyproject.toml): the
//     classic path, installed with `uv pip install -r`.
//   - pyproject.toml without uv.lock: an error — Relay never resolves a fresh
//     set, so a native project whose lock is missing cannot be installed
//     deterministically. Commit the lock (or use requirements.txt).
//   - none: no dependency layer.
func (Engine) Plan(spec plan.Spec, fnDir string) (plan.BuildPlan, error) {
	files := []plan.File{{
		Path:    "/relay/bootstrap.py",
		Content: bootstrap,
		Mode:    fs.FileMode(0o644),
	}}

	deps, err := dependencyPlan(fnDir)
	if err != nil {
		return plan.BuildPlan{}, err
	}

	return plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    workDir,
		Files:      files,
		Deps:       deps,
		ToolCopies: spec.ToolCopies,
		UserSetup:  userSetup,
		User:       userID,
		Env:        []string{noBytecodeEnv},
		Entrypoint: entrypoint,
	}, nil
}

// dependencyPlan resolves fnDir's dependency manifests into a Deps value (zero
// when the function declares none) or a detection error. It is split out so the
// detection rules above are readable in one place and unit-testable without a
// plan.Spec.
func dependencyPlan(fnDir string) (plan.Deps, error) {
	hasLock, err := fileExists(fnDir, uvLockFile)
	if err != nil {
		return plan.Deps{}, err
	}
	hasPyproject, err := fileExists(fnDir, pyprojectFile)
	if err != nil {
		return plan.Deps{}, err
	}
	hasRequirements, err := fileExists(fnDir, requirementsFile)
	if err != nil {
		return plan.Deps{}, err
	}

	switch {
	case hasLock && hasPyproject:
		// A native uv project: both files are listed so a change to either the
		// declared dependencies or the resolved lock re-fingerprints the layer.
		return plan.Deps{
			Files:   []string{pyprojectFile, uvLockFile},
			Install: nativeInstall,
			Dir:     workDir,
		}, nil
	case hasLock:
		return plan.Deps{}, fmt.Errorf(
			"native uv project is incomplete: %s is present without %s; commit both files together",
			uvLockFile, pyprojectFile,
		)
	case hasRequirements:
		// Requirements-only remains valid, including alongside an unrelated
		// pyproject.toml (e.g. tool configuration), which is left to the
		// function's source.
		return plan.Deps{
			Files:   []string{requirementsFile},
			Install: requirementsInstall,
			Dir:     workDir,
		}, nil
	case hasPyproject:
		return plan.Deps{}, fmt.Errorf(
			"native uv project is incomplete: %s is present without %s; Relay installs only from a committed lock (run `uv lock`), or use %s",
			pyprojectFile, uvLockFile, requirementsFile,
		)
	default:
		return plan.Deps{}, nil
	}
}

// fileExists reports whether name exists in fnDir. A stat error other than
// not-exist (e.g. a permission problem) is surfaced rather than mistaken for
// absence, so dependency detection never silently degrades.
func fileExists(fnDir, name string) (bool, error) {
	_, err := os.Stat(filepath.Join(fnDir, name))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("stat %s: %w", name, err)
}
