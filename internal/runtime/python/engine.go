// Package python is the Python runtime engine. It holds the language-specific
// preparation knowledge for Python apps: the embedded bootstrap, the
// dependency handling (requirements.txt and native uv projects), and the
// container entrypoint. It does NOT run docker build or generate Dockerfiles; it
// produces a generic runtime.BuildPlan that the Docker builder renders.
//
// Dependencies are installed with the uv binary (copied into the runtime image
// from the pinned official uv image; see the runtime registry) rather than with
// pip, so both the classic requirements.txt path and a native uv project use the
// same fast, locked installer and land in the system site-packages the runtime
// entrypoint already uses.
//
// The optional, worker-global PYTHON_ZUVLOOP setting is also build knowledge:
// for an app image that serves event or schedule handlers the engine injects a
// bootstrap variant and a managed install of the pinned zuvloop package, so the
// warm invocation process selects that loop once at startup. It is never applied
// to a service-only image, and services keep their own command and loop.
package python

import (
	"bytes"
	_ "embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"relay/internal/runtime/plan"
)

//go:embed bootstrap.py
var bootstrap []byte

// The embedded bootstrap is the standard-asyncio variant. When PYTHON_ZUVLOOP
// applies to an image, bootstrapFor replaces the exact build-time marker line
// with an import of the pinned package and an enabled flag; the bootstrap's
// main() then selects the loop once at process startup. A marker that is
// missing or ambiguous is an error rather than a silently unmodified bootstrap.
const (
	zuvloopBootstrapMarker  = "\nZUVLOOP_ENABLED = False\n"
	zuvloopBootstrapEnabled = "\nimport zuvloop\n\nZUVLOOP_ENABLED = True\n"
)

// Bootstrap exposes the embedded standard-asyncio bootstrap so tests can verify
// its content and run it as a live process.
var Bootstrap = bootstrap

// bootstrapFor returns the bootstrap content for the resolved loop mode. The
// disabled variant is the embedded file byte-for-byte; the enabled variant
// imports zuvloop at module load (so a missing package fails process startup)
// and calls asyncio.run with zuvloop's loop factory. It never falls back.
func bootstrapFor(zuvloop bool) ([]byte, error) {
	if !zuvloop {
		return bootstrap, nil
	}
	if bytes.Count(bootstrap, []byte(zuvloopBootstrapMarker)) != 1 {
		return nil, fmt.Errorf("python bootstrap: expected exactly one %q marker", zuvloopBootstrapMarker)
	}
	return bytes.Replace(bootstrap, []byte(zuvloopBootstrapMarker), []byte(zuvloopBootstrapEnabled), 1), nil
}

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

// otelAPIFloor is the managed OpenTelemetry API installed into every Python
// runtime image (see otelInstall). It is a floor RANGE rather than an exact pin
// on purpose: unlike Node's standalone @opentelemetry/api, the Python API
// version is coupled to the user's opentelemetry-sdk version, so an exact pin
// would silently replace a matching API and leave the SDK paired with a
// mismatched one. The floor is the first release that imports without
// `pkg_resources` (gone from python:3.14-slim), which the bootstrap's
// context/propagate imports need; the <2 cap refuses a future major Relay has
// not validated. When a user SDK already installed a satisfying API, uv keeps
// it and Relay never downgrades it.
const otelAPIFloor = "opentelemetry-api>=1.16,<2"

// zuvloopVersion is the exact pinned release installed when PYTHON_ZUVLOOP
// applies to a Python app image. zuvloop requires Python 3.14+, which is the
// managed python3.14 runtime, and publishes prebuilt Linux amd64/arm64 wheels.
// The pin is exact (no range) so an image cannot change loop behavior when a
// new release appears.
const zuvloopVersion = "0.0.17"

// install commands (run after uv has been copied into the image being built).
// Dependency installs land in the SYSTEM site-packages (--system) so the
// existing `python -u /relay/bootstrap.py` entrypoint sees the packages; no
// project-local .venv is created, keeping runtime execution and the dependency
// image's FROM-inheritance unchanged.
const (
	// requirementsInstall installs a plain requirements.txt with uv's pip
	// interface. --no-cache keeps the layer small.
	requirementsInstall = "uv pip install --system --no-cache -r " + requirementsFile

	// otelInstall installs the managed OpenTelemetry API into the app
	// image's system site-packages (the same environment the bootstrap and any
	// user-installed SDK resolve from). The spec is single-quoted because the
	// version range's < and > are shell redirection operators. It runs in the
	// app image (not the shared dependency layer) so it never enters the
	// content-addressed dependency fingerprint; being a build Install command,
	// it is covered by the relay.bootstrap label, so images pick it up on the
	// next reconcile.
	otelInstall = "uv pip install --system --no-cache '" + otelAPIFloor + "'"

	// zuvloopPackage is the exact package spec installed for PYTHON_ZUVLOOP,
	// and zuvloopInstall is its managed uv install. Like otelInstall it runs in
	// the app image (never the shared dependency layer), so the dependency
	// image identity is unchanged whether the loop is enabled or not, and only
	// an image that actually serves event/schedule handlers pays for the
	// install. The spec is single-quoted like otelInstall (the pin is exact,
	// but the quoting keeps the managed-install form consistent).
	zuvloopPackage = "zuvloop==" + zuvloopVersion
	zuvloopInstall = "uv pip install --system --no-cache '" + zuvloopPackage + "'"

	// nativeInstall exports the locked dependency set from the native uv project
	// and installs it, without re-resolving: `--locked` asserts the committed
	// uv.lock is up to date and fails the build if it is not, `--no-dev` drops
	// development groups, and `--no-emit-project` excludes the project itself
	// (Relay only installs the project's dependencies into the base layer; the
	// app's own source is copied by the app image). The export goes to
	// a file (not a pipe) and is chained with && so an export failure aborts the
	// build instead of silently installing an empty set.
	nativeInstall = "uv export --locked --no-dev --no-emit-project -o " + exportedRequirements +
		" && uv pip install --system --no-cache -r " + exportedRequirements +
		" && rm -f " + exportedRequirements
)

// The runtime user is a fixed numeric identity (10001:10001) shared by every
// Relay app so the container never runs as root and the identity is stable
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

// Plan returns the generic build plan for a Python app. It only inspects
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
//
// handlers is the app's handler MODULE parts, collected from its event rules and
// cron schedules. The Node engine uses it to transpile TypeScript at build time.
// The Python engine uses only its EMPTINESS: a non-empty list means this image
// will serve at least one event or schedule invocation, so PYTHON_ZUVLOOP may
// apply. A service-only app therefore keeps the standard asyncio bootstrap and
// installs nothing extra even when the worker-global setting is enabled.
func (Engine) Plan(spec plan.Spec, fnDir string, handlers []string) (plan.BuildPlan, error) {
	// The loop mode is a build input, not a runtime environment probe: enabling
	// it changes the injected bootstrap and adds the managed install, both of
	// which participate in the relay.bootstrap label. A service-only app has no
	// invocation handlers, so it stays byte-identical to the disabled mode.
	zuvloop := spec.PythonZuvloop && len(handlers) > 0
	bootstrapContent, err := bootstrapFor(zuvloop)
	if err != nil {
		return plan.BuildPlan{}, err
	}
	files := []plan.File{{
		Path:    "/relay/bootstrap.py",
		Content: bootstrapContent,
		Mode:    fs.FileMode(0o644),
	}}

	deps, err := dependencyPlan(fnDir)
	if err != nil {
		return plan.BuildPlan{}, err
	}

	// The managed OpenTelemetry API is installed in the APP image (not the
	// shared dependency layer): it must be present in every runtime even when
	// the app declares no dependencies, and keeping it out of Deps leaves
	// the content-addressed dependency fingerprint untouched. It runs after the
	// dependency image's FROM has already provided the user's packages, so a
	// user SDK that pinned its own matching API is not disturbed (see
	// otelAPIFloor). uv is available: the no-deps app image copies it as a
	// tool, and a deps app image inherits it from the dependency base.
	install := []string{otelInstall}
	if zuvloop {
		// The pinned zuvloop package joins the same app-image managed-install
		// path. Keeping it out of Deps leaves the dependency fingerprint and
		// the shared dependency layer unchanged; a service-only image and a
		// disabled worker install nothing extra.
		install = append(install, zuvloopInstall)
	}
	return plan.BuildPlan{
		BaseImage:    spec.BaseImage,
		WorkDir:      workDir,
		Files:        files,
		Deps:         deps,
		Install:      install,
		RuntimeTools: spec.RuntimeTools,
		UserSetup:    userSetup,
		User:         userID,
		Env:          []string{noBytecodeEnv},
		Entrypoint:   entrypoint,
	}, nil
}

// dependencyPlan resolves fnDir's dependency manifests into a Deps value (zero
// when the app declares none) or a detection error. It is split out so the
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
		// app's source.
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
