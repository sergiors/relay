// Package node is the Node.js runtime engine. It holds the language-specific
// preparation knowledge for Node.js (ESM) functions: the embedded bootstrap,
// the dependency handling (package-lock.json / package.json), and the container
// entrypoint. It does NOT run docker build or generate Dockerfiles; it produces
// a generic runtime.BuildPlan that the Docker builder renders.
package node

import (
	_ "embed"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"

	"relay/internal/runtime/plan"
)

//go:embed bootstrap.mjs
var bootstrap []byte

// Bootstrap exposes the embedded bootstrap so tests can verify its content.
var Bootstrap = bootstrap

type Engine struct{}

var entrypoint = []string{"node", "/relay/bootstrap.mjs"}

const workDir = "/app"
const pkgFile = "package.json"
const lockFile = "package-lock.json"

// The runtime user is a fixed numeric identity (10001:10001) shared by every
// Relay function so the container never runs as root and the identity is stable
// across images and rebuilds; the numeric id is what the kernel enforces, so the
// user/group name is cosmetic. node:24-alpine (BusyBox) has no pre-created user,
// so the Dockerfile creates one at build time via UserSetup. Node needs only read
// access to /app (ESM modules are read, not compiled to a cache by default), so
// /app is chowned to the user for symmetry with the Python engine; the read-only
// rootfs is never written at runtime.
const (
	userID   = "10001:10001"
	userName = "app"
	// userSetup runs as root at build time. /relay holds the bootstrap, which the
	// user only needs to read. chown uses the numeric id so it is independent of
	// the user/group name.
	userSetup = "addgroup -g 10001 app && adduser -D -u 10001 -G app -h /home/app -s /sbin/nologin app && chown -R 10001:10001 /app /relay"
)

// esmPackageJSON is a minimal package.json injected when the function has none,
// so that .js files are interpreted as ESM.
var esmPackageJSON = func() []byte {
	b, err := json.Marshal(map[string]string{"type": "module"})
	if err != nil {
		panic(err) // static construction; cannot fail
	}
	return b
}()

// Plan returns the generic build plan for a Node.js function. It only inspects
// fnDir (for package-lock.json and package.json) and never writes into it; any
// injected file is returned for the builder to write.
func (Engine) Plan(spec plan.Spec, fnDir string) (plan.BuildPlan, error) {
	files := []plan.File{{
		Path:    "/relay/bootstrap.mjs",
		Content: bootstrap,
		Mode:    fs.FileMode(0o644),
	}}

	// A package.json / package-lock.json declares the function's dependencies:
	// a reusable layer that installs them into /app (the function WORKDIR).
	var deps plan.Deps

	lockErr := stat(fnDir, lockFile)
	pkgErr := stat(fnDir, pkgFile)

	switch {
	case lockErr == nil && pkgErr == nil:
		// A lockfile pins the exact tree, so npm ci reproduces it: deterministic,
		// correct, and the fastest install. Both files are listed so a lock change
		// (a different pinned tree) re-fingerprints the layer even when the
		// manifest is unchanged.
		deps = plan.Deps{Files: []string{pkgFile, lockFile}, Install: "npm ci --omit=dev", Dir: workDir}
	case pkgErr == nil:
		// No lock: npm install resolves from the manifest.
		deps = plan.Deps{Files: []string{pkgFile}, Install: "npm install --omit=dev", Dir: workDir}
	case lockErr == nil:
		// A lock without a manifest is unusual (npm ci needs both) but preserve
		// the previous engine's intent to ci-install from the lock. Only the
		// present file is listed so fingerprinting and the dep build never read a
		// missing manifest.
		//
		// Intentional surface: npm ci fails without a package.json, so this
		// branch's build fails loudly — the same failure mode as the previous
		// single-stage behavior. Operators get an explicit error rather than
		// silent misbehavior. No ESM package.json is injected here because the
		// Deps path replaces the inject.
		deps = plan.Deps{Files: []string{lockFile}, Install: "npm ci --omit=dev", Dir: workDir}
	default:
		// No package.json or lock: inject a minimal ESM package.json so .js files
		// are treated as ESM. There are no dependencies, so no Deps and no
		// dependency image.
		files = append(files, plan.File{
			Path:    filepath.Join(workDir, pkgFile),
			Content: esmPackageJSON,
			Mode:    fs.FileMode(0o644),
		})
	}

	return plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    workDir,
		Files:      files,
		Deps:       deps,
		UserSetup:  userSetup,
		User:       userID,
		Entrypoint: entrypoint,
	}, nil
}

func stat(fnDir, name string) error {
	_, err := os.Stat(filepath.Join(fnDir, name))
	return err
}
