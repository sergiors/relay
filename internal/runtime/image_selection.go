package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// RuntimeImageSelection is one startup runtime base-image pin supplied to
// NewManager (see WithRuntimeImageSelections). RuntimeID is the template
// runtime identifier, SourceImage must equal that runtime's configured
// Spec.BaseImage exactly, and Digest is the immutable Docker multi-platform
// index digest in the form "sha256:<64 lowercase hex>". Generation and
// SelectedAt are carried for diagnostics only; the runtime identity is the
// configured source reference plus the selected digest.
//
// The state package persists these rows without importing this package; the
// worker converts its read model into this type at startup. There is no live
// refresh: the Manager takes one immutable snapshot at construction.
type RuntimeImageSelection struct {
	RuntimeID   string
	SourceImage string
	Digest      string
	Generation  int64
	SelectedAt  string
}

// imageSelection is the validated, immutable per-runtime value a Manager keeps
// for the process lifetime. A zero value means the runtime is unpinned.
type imageSelection struct {
	sourceImage string
	digest      string
}

// pinned reports whether the selection pins an immutable base-image identity.
func (s imageSelection) pinned() bool {
	return s.digest != ""
}

// baseImage returns the Docker base reference a pinned plan must build FROM.
// The digest is the multi-platform index digest exactly as selected; Relay never
// resolves a registry or child manifest digest.
func (s imageSelection) baseImage() string {
	if !s.pinned() {
		return s.sourceImage
	}
	return s.sourceImage + "@" + s.digest
}

// WithRuntimeImageSelections supplies the startup snapshot of runtime base-image
// pins read from the shared state database. A pinned entry must name a known
// runtime, carry that runtime's EXACT configured base image, and use a valid
// "sha256:<64 lowercase hex>" multi-platform index digest; NewManager rejects any
// malformed or mismatched entry for a known runtime. Entries for runtime IDs this
// binary does not know are ignored (a newer Relay sharing the database may have
// written them), because no app here can select that runtime.
//
// The snapshot is copied and immutable after construction. A running Manager
// does not poll or refresh selections; changing a pin requires a worker restart.
// Relay does not automatically pin unselected runtimes.
func WithRuntimeImageSelections(selections []RuntimeImageSelection) ManagerOption {
	return func(o *managerOptions) {
		o.imageSelections = append([]RuntimeImageSelection(nil), selections...)
	}
}

// resolveImageSelections validates the startup pin snapshot against the runtime
// registry and returns the pinned subset plus the unknown runtime IDs it ignored.
// A known runtime with a mismatched source image or malformed digest is an error:
// Relay must never silently fall back to an unpinned image for a selection it
// was explicitly given.
func resolveImageSelections(selections []RuntimeImageSelection) (map[string]imageSelection, []string, error) {
	pins := make(map[string]imageSelection, len(selections))
	var unknown []string
	for _, selection := range selections {
		if selection.RuntimeID == "" {
			return nil, nil, fmt.Errorf("runtime image selection: runtime ID is empty")
		}
		spec, err := lookup(selection.RuntimeID)
		if err != nil {
			unknown = append(unknown, selection.RuntimeID)
			continue
		}
		if selection.SourceImage != spec.BaseImage {
			return nil, nil, fmt.Errorf(
				"runtime %q: selected source image %q does not match configured base image %q",
				selection.RuntimeID, selection.SourceImage, spec.BaseImage)
		}
		if !validImageDigest(selection.Digest) {
			return nil, nil, fmt.Errorf(
				"runtime %q: selected digest %q must be sha256:<64 lowercase hex>",
				selection.RuntimeID, selection.Digest)
		}
		if _, exists := pins[selection.RuntimeID]; exists {
			return nil, nil, fmt.Errorf("runtime %q: duplicate image selection", selection.RuntimeID)
		}
		pins[selection.RuntimeID] = imageSelection{
			sourceImage: selection.SourceImage,
			digest:      selection.Digest,
		}
	}
	return pins, unknown, nil
}

// validImageDigest reports whether digest is a Docker multi-platform index
// digest in the persisted form: "sha256:" followed by 64 lowercase hex chars.
func validImageDigest(digest string) bool {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) || len(digest) != len(prefix)+64 {
		return false
	}
	for _, c := range digest[len(prefix):] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// imageSelectionFor returns the validated immutable pin for runtimeID, or the
// zero (unpinned) value when none was supplied.
func (m *Manager) imageSelectionFor(runtimeID string) imageSelection {
	if m == nil {
		return imageSelection{}
	}
	return m.imageSelections[runtimeID]
}

// imageIdentityWithSelection folds a pinned runtime identity into an app image's
// derived identity. An unpinned selection returns base unchanged, preserving
// every historical tag byte-for-byte. For a pinned runtime the digest, the
// configured source reference, and the dependency reference (when present) are
// hashed in, so baked source, dependency-backed, HOT_RELOAD (whose rendered
// Dockerfile already carries FROM ref@digest), and zuvloop mode all get a
// distinct image for a different pin and can never retag one reference for two
// pins. Prepared.Fingerprint remains the source-only identity.
func imageIdentityWithSelection(base string, selection imageSelection, dependency string) string {
	if !selection.pinned() {
		return base
	}
	h := sha256.New()
	hashField(h, "relay-image-selection")
	hashField(h, base)
	hashField(h, selection.sourceImage)
	hashField(h, selection.digest)
	hashField(h, dependency)
	return hex.EncodeToString(h.Sum(nil))
}

// foldDependencySelection folds a pinned runtime identity into an already
// computed dependency fingerprint. It is used only when a test injects the
// Manager's dependency-fingerprint seam (which has no selection parameter); the
// production path hashes the same fields directly inside
// dependencyFingerprintFrom. An unpinned selection returns base unchanged.
func foldDependencySelection(base string, selection imageSelection) string {
	if !selection.pinned() {
		return base
	}
	h := sha256.New()
	hashField(h, "relay-dependency-image-selection")
	hashField(h, base)
	hashField(h, selection.sourceImage)
	hashField(h, selection.digest)
	return hex.EncodeToString(h.Sum(nil))
}
