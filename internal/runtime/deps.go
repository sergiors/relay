package runtime

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"relay/internal/runtime/plan"
)

// dependencySnapshot is an immutable snapshot of a dependency layer's manifest
// inputs: every declared manifest file's relative name and its exact staged copy
// under a private root, read exactly once. It is the single source for BOTH the
// dependency fingerprint and the staged dependency build context, so the
// resulting hash/tag always corresponds to the bytes actually baked into the
// dependency image even if a manifest is edited concurrently.
//
// The manifests live on disk under root (which IS the dependency build context),
// not in memory, so a large manifest such as a pnpm-lock.yaml is never held
// in the worker heap. The fingerprint is recomputed deterministically from the
// staged bytes (see dependencyFingerprintFrom), so it remains valid even after
// the root is removed.
type dependencySnapshot struct {
	root  string
	files []dependencyManifest
}

// dependencyManifest is one staged manifest: its relative name (as declared by
// the engine). Its content bytes live at root/<name>.
type dependencyManifest struct {
	name string
}

// depBuildRootPattern is the os.MkdirTemp pattern for a dependency snapshot's
// private root (the shared relay-dep-* build context). The "relay-dep-build-"
// prefix matches the transient dependency build directory the runtime used to
// create (and that its cleanup tests observe); release removes it.
const depBuildRootPattern = "relay-dep-build-*"

// snapshotDependency reads the manifest files declared by deps from fnDir into
// an immutable disk-backed snapshot sorted by name, so the serialization is
// canonical and a later edit to the on-disk manifest cannot make the fingerprint
// and the staged bytes disagree. The snapshot's private root is the dependency
// build context; the caller must release it with snap.release() once the
// fingerprint and the dependency image build are done.
//
// A missing/unreadable manifest is an error: the engine only declares Deps when
// the manifests exist, so an absent one means a race / mid-reconcile state and
// must not be silently hashed as empty (that would poison the shared layer
// cache). On any error the private root is removed, so a failed capture leaks
// nothing.
func snapshotDependency(fnDir string, deps plan.Deps) (dependencySnapshot, error) {
	if len(deps.Files) == 0 {
		return dependencySnapshot{}, fmt.Errorf("fingerprint deps: no manifest files")
	}

	files := append([]string(nil), deps.Files...)
	sort.Strings(files)

	root, err := os.MkdirTemp("", depBuildRootPattern)
	if err != nil {
		return dependencySnapshot{}, fmt.Errorf("dependency snapshot: create root: %w", err)
	}

	snap := dependencySnapshot{root: root, files: make([]dependencyManifest, 0, len(files))}
	for _, name := range files {
		src, oerr := os.Open(filepath.Join(fnDir, filepath.FromSlash(name)))
		if oerr != nil {
			snap.release()
			return dependencySnapshot{}, fmt.Errorf("fingerprint deps: read manifest %q: %w", name, oerr)
		}
		target := filepath.Join(root, filepath.FromSlash(name))
		if dir := filepath.Dir(target); dir != root {
			if merr := os.MkdirAll(dir, 0o755); merr != nil {
				_ = src.Close()
				snap.release()
				return dependencySnapshot{}, fmt.Errorf("fingerprint deps: stage manifest %q: %w", name, merr)
			}
		}
		dst, cerr := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if cerr != nil {
			_ = src.Close()
			snap.release()
			return dependencySnapshot{}, fmt.Errorf("fingerprint deps: stage manifest %q: %w", name, cerr)
		}
		_, copyErr := io.Copy(dst, src)
		closeDstErr := dst.Close()
		closeSrcErr := src.Close()
		if copyErr != nil {
			snap.release()
			return dependencySnapshot{}, fmt.Errorf("fingerprint deps: read manifest %q: %w", name, copyErr)
		}
		if closeDstErr != nil {
			snap.release()
			return dependencySnapshot{}, fmt.Errorf("fingerprint deps: stage manifest %q: %w", name, closeDstErr)
		}
		if closeSrcErr != nil {
			snap.release()
			return dependencySnapshot{}, fmt.Errorf("fingerprint deps: read manifest %q: %w", name, closeSrcErr)
		}

		snap.files = append(snap.files, dependencyManifest{name: name})
	}
	return snap, nil
}

// release removes the snapshot's private root if it owns one. It is nil-safe and
// idempotent. A snapshot constructed directly by tests (no root) releases
// nothing.
func (s *dependencySnapshot) release() {
	if s == nil || s.root == "" {
		return
	}
	_ = os.RemoveAll(s.root)
	s.root = ""
}

// DependencyFingerprint computes the content address for a dependency layer:
// a deterministic SHA-256 over every input that shapes the installed payload.
// The same inputs always yield the same 64-hex digest, so a dependency image
// tagged `relay-dep-<first16>` is a content-addressed cache shared across every
// app (and every version of that app) that declares identical deps.
//
// Inputs (the requirement for what makes one layer distinct from another):
//
//   - the runtime version (spec.Name) AND the base image (spec.BaseImage).
//     BaseImage matters separately because it pins the runtime interpreter and
//     the OS userland the native wheels / node modules are built for — two
//     specs could share a display name but differ in base, and vice versa.
//   - the machine architecture (GOARCH + GOOS) of the BUILD HOST. pip install /
//     pnpm install produce arch-specific compiled artifacts (native wheels, node
//     native modules), and Relay builds on the same daemon it executes on, so
//     the build host's architecture is the correct key (see #5). It is injected
//     as a parameter so tests can simulate other architectures without relying
//     on the host.
//   - the relative name AND content bytes of each manifest file, in sorted
//     order. A rename is a content change (the filename is hashed), and the
//     sorted order keeps the serialization canonical regardless of the order
//     the engine listed the files.
//   - the Install command. A changed install procedure (dependency set, flags,
//     install method) means a different installed payload even with identical
//     manifest bytes.
//   - the runtime's external tools (spec.RuntimeTools): the install runs the
//     tool (e.g. uv), so its pinned version is an input to the installed
//     payload even when the manifests are unchanged.
//
// Field boundaries are length-prefixed so two distinct concatenations (e.g.
// "ab"+"c" vs "a"+"bc") can never collide under the flat hash.
func DependencyFingerprint(
	arch,
	platform string,
	spec plan.Spec,
	fnDir string,
	deps plan.Deps,
) (string, error) {
	snap, err := snapshotDependency(fnDir, deps)
	if err != nil {
		return "", err
	}
	defer snap.release()
	return dependencyFingerprintFrom(arch, platform, spec, deps, snap)
}

// dependencyFingerprintFrom hashes an already-captured manifest snapshot. It is
// the single fingerprint implementation, shared by DependencyFingerprint and
// the Manager's prepare path, so the fingerprint and the bytes staged into the
// dependency image come from the exact same read. Because the snapshot is
// disk-backed, each manifest's bytes are STREAMED into the hash (with the same
// 8-byte length prefix as hashFieldBytes would write), so an arbitrarily large
// manifest is never materialized in memory.
func dependencyFingerprintFrom(
	arch,
	platform string,
	spec plan.Spec,
	deps plan.Deps,
	snap dependencySnapshot,
) (string, error) {
	h := sha256.New()

	// Runtime identity.
	hashField(h, spec.Name)
	hashField(h, spec.BaseImage)

	// Build-host architecture.
	hashField(h, arch)
	hashField(h, platform)

	// External build tools the install depends on (e.g. the pinned uv binary or
	// the pinned pnpm JS CLI distribution). Hashed in plan order: RuntimeTools is
	// a fixed registry-declared list, and the field boundaries keep distinct tool
	// sets from colliding.
	for _, tool := range spec.RuntimeTools {
		hashField(h, tool.From)
		hashField(h, tool.Source)
		hashField(h, tool.Destination)
	}

	// Install procedure.
	hashField(h, deps.Install)
	hashField(h, deps.Dir)

	// Each manifest file: its relative name then its bytes, so a rename (name
	// change) or an edit (bytes change) both alter the digest. The snapshot is
	// already name-sorted by snapshotDependency.
	for _, f := range snap.files {
		hashField(h, f.name)
		if err := hashManifestBytes(h, snap.root, f.name); err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashManifestBytes streams the staged manifest's bytes into the digest, framed
// exactly as hashFieldBytes frames a byte slice (8-byte big-endian length then
// content), so the disk-backed fingerprint is byte-for-byte identical to the
// historical in-memory one. A read failure is surfaced: a manifest that vanished
// from the snapshot root is an anomaly, never hashed as empty.
func hashManifestBytes(h io.Writer, root, name string) error {
	f, err := os.Open(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		return fmt.Errorf("dependency snapshot: read manifest %q: %w", name, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("dependency snapshot: stat manifest %q: %w", name, err)
	}
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(info.Size()))
	if _, err := h.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("dependency snapshot: hash manifest %q: %w", name, err)
	}
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("dependency snapshot: hash manifest %q: %w", name, err)
	}
	return nil
}

// hashField writes a length-prefixed string field into the digest.
func hashField(h io.Writer, value string) {
	hashFieldBytes(h, []byte(value))
}

// hashFieldBytes writes a length-prefixed bytes field into the digest. The
// positional order of the fields plus the length prefix disambiguate them, so
// two distinct concatenations can never collide under the flat hash.
func hashFieldBytes(h io.Writer, value []byte) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(value)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(value)
}

// Known limitation: DependencyFingerprint keys on the base image TAG
// (spec.BaseImage, e.g. "python:3.14-slim") rather than its immutable digest.
// If the daemon later pulls a newer image for that tag, an existing relay-dep-*
// image built from the older base is still reused (same tag -> same fingerprint
// -> same cached layer). This matches the pre-existing exposure of the
// app-image reuse path (app fingerprints never included the base image
// either). Operators wanting a refresh must force it (e.g. remove the
// relay-dep-* images); a future digest-pinning feature is the proper fix.
//
// depImageRef maps a dependency fingerprint to the docker reference for that
// exact dependency layer. Dependency images are content-addressed by
// fingerprint with NO app name: the same manifest set + runtime + arch +
// install command is one shared image across every app that needs it, and
// a changed requirements.txt naturally yields a NEW tag (old layers are never
// mutated). The "relay-dep-" prefix keeps dependency images in their own
// namespace, distinct from "relay-app-" so app-image lifecycle ops never
// touch them.
func depImageRef(fingerprint string) string {
	if len(fingerprint) > tagPrefixLen {
		fingerprint = fingerprint[:tagPrefixLen]
	}
	return "relay-dep-" + fingerprint
}

// depRepoPrefix is the repository-name prefix for dependency images, kept
// separate from relayRepoPrefix ("relay-app-") on purpose: dependency layers are
// shared, content-addressed bases, so they are deliberately INVISIBLE to
// AppImageTags / RemoveImagesExcept — removing an app version must
// never remove a dependency image other apps may still depend on.
const depRepoPrefix = "relay-dep-"

// isDepRepo reports whether repo is a dependency-image repository name.
func isDepRepo(repo string) bool {
	return strings.HasPrefix(repo, depRepoPrefix)
}
