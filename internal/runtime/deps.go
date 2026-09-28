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

// dependencySnapshot is an immutable, in-memory snapshot of a dependency
// layer's manifest inputs: every declared manifest file's relative name and its
// exact bytes, read exactly once. It is the single source for BOTH the
// dependency fingerprint and the staged dependency build context, so the
// resulting hash/tag always corresponds to the bytes actually baked into the
// dependency image even if a manifest is edited concurrently.
type dependencySnapshot struct {
	files []dependencyManifest
}

// dependencyManifest is one staged manifest: its relative name (as declared by
// the engine) and its content bytes.
type dependencyManifest struct {
	name    string
	content []byte
}

// snapshotDependency reads the manifest files declared by deps from fnDir into
// an immutable snapshot sorted by name, so the serialization is canonical and a
// later edit to the on-disk manifest cannot make the fingerprint and the staged
// bytes disagree. A missing/unreadable manifest is an error: the engine only
// declares Deps when the manifests exist, so an absent one means a race /
// mid-reconcile state and must not be silently hashed as empty (that would
// poison the shared layer cache).
func snapshotDependency(fnDir string, deps plan.Deps) (dependencySnapshot, error) {
	if len(deps.Files) == 0 {
		return dependencySnapshot{}, fmt.Errorf("fingerprint deps: no manifest files")
	}

	files := append([]string(nil), deps.Files...)
	sort.Strings(files)
	snap := dependencySnapshot{files: make([]dependencyManifest, 0, len(files))}
	for _, name := range files {
		content, err := os.ReadFile(filepath.Join(fnDir, filepath.FromSlash(name)))
		if err != nil {
			return dependencySnapshot{}, fmt.Errorf("fingerprint deps: read manifest %q: %w", name, err)
		}

		snap.files = append(snap.files, dependencyManifest{name: name, content: content})
	}
	return snap, nil
}

// DependencyFingerprint computes the content address for a dependency layer:
// a deterministic SHA-256 over every input that shapes the installed payload.
// The same inputs always yield the same 64-hex digest, so a dependency image
// tagged `relay-dep-<first16>` is a content-addressed cache shared across every
// function (and every version of that function) that declares identical deps.
//
// Inputs (the requirement for what makes one layer distinct from another):
//
//   - the runtime version (spec.Name) AND the base image (spec.BaseImage).
//     BaseImage matters separately because it pins the runtime interpreter and
//     the OS userland the native wheels / node modules are built for — two
//     specs could share a display name but differ in base, and vice versa.
//   - the machine architecture (GOARCH + GOOS) of the BUILD HOST. pip install /
//     npm install produce arch-specific compiled artifacts (native wheels, node
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
//   - the runtime's external tool copies (spec.ToolCopies): the install runs the
//     copied tool (e.g. uv), so its pinned version is an input to the installed
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
	return dependencyFingerprintFrom(arch, platform, spec, deps, snap), nil
}

// dependencyFingerprintFrom hashes an already-captured manifest snapshot. It is
// the single fingerprint implementation, shared by DependencyFingerprint and
// the Manager's prepare path, so the fingerprint and the bytes staged into the
// dependency image come from the exact same read.
func dependencyFingerprintFrom(
	arch,
	platform string,
	spec plan.Spec,
	deps plan.Deps,
	snap dependencySnapshot,
) string {
	h := sha256.New()

	// Runtime identity.
	hashField(h, spec.Name)
	hashField(h, spec.BaseImage)

	// Build-host architecture.
	hashField(h, arch)
	hashField(h, platform)

	// External build tools the install depends on (e.g. the pinned uv binary).
	// Hashed in plan order: ToolCopies is a fixed registry-declared list, and
	// the field boundaries keep distinct copy sets from colliding.
	for _, tc := range spec.ToolCopies {
		hashField(h, tc.From)
		hashField(h, tc.Source)
		hashField(h, tc.Dest)
	}

	// Install procedure.
	hashField(h, deps.Install)
	hashField(h, deps.Dir)

	// Each manifest file: its relative name then its bytes, so a rename (name
	// change) or an edit (bytes change) both alter the digest. The snapshot is
	// already name-sorted by snapshotDependency.
	for _, f := range snap.files {
		hashField(h, f.name)
		hashFieldBytes(h, f.content)
	}

	return hex.EncodeToString(h.Sum(nil))
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
// function-image reuse path (function fingerprints never included the base image
// either). Operators wanting a refresh must force it (e.g. remove the
// relay-dep-* images); a future digest-pinning feature is the proper fix.
//
// depImageRef maps a dependency fingerprint to the docker reference for that
// exact dependency layer. Dependency images are content-addressed by
// fingerprint with NO function name: the same manifest set + runtime + arch +
// install command is one shared image across every function that needs it, and
// a changed requirements.txt naturally yields a NEW tag (old layers are never
// mutated). The "relay-dep-" prefix keeps dependency images in their own
// namespace, distinct from "relay-fn-" so function-image lifecycle ops never
// touch them.
func depImageRef(fingerprint string) string {
	if len(fingerprint) > tagPrefixLen {
		fingerprint = fingerprint[:tagPrefixLen]
	}
	return "relay-dep-" + fingerprint
}

// depRepoPrefix is the repository-name prefix for dependency images, kept
// separate from relayRepoPrefix ("relay-fn-") on purpose: dependency layers are
// shared, content-addressed bases, so they are deliberately INVISIBLE to
// FunctionImageTags / RemoveImagesExcept — removing a function version must
// never remove a dependency image other functions may still depend on.
const depRepoPrefix = "relay-dep-"

// isDepRepo reports whether repo is a dependency-image repository name.
func isDepRepo(repo string) bool {
	return strings.HasPrefix(repo, depRepoPrefix)
}
