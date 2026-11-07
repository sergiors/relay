package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrRuntimeImageSelectionStale reports that the authoritative runtime image
// selection changed after a preparation captured the selection it built from.
// A preparation that returns this error must not be published: its image was
// built for a pin that is no longer current. The caller discards the prepared
// generation (releasing its image lease), retains the active version, and
// retries against the refreshed snapshot.
var ErrRuntimeImageSelectionStale = errors.New("runtime image selection changed during preparation")

// RuntimeImageSelection is one runtime base-image pin snapshot entry. RuntimeID
// is the template runtime identifier, SourceImage must equal that runtime's
// configured Spec.BaseImage exactly, and Digest is the immutable Docker
// multi-platform index digest in the form "sha256:<64 lowercase hex>".
// Generation and SelectedAt are carried for diagnostics only; the runtime
// identity is the configured source reference plus the selected digest.
//
// The state package persists these rows without importing this package; the
// worker adapts its read model to this type. The Manager keeps a validated
// snapshot that each reconciliation pass may refresh from the shared database.
type RuntimeImageSelection struct {
	RuntimeID   string
	SourceImage string
	Digest      string
	Generation  int64
	SelectedAt  string
}

// RuntimeImageSelectionStore is the shared, authoritative runtime base-image
// selection view a Manager refreshes from and fences publication against. The
// production implementation is backed by the shared PostgreSQL state database
// (see internal/state); the worker adapts it here so runtime never imports
// state.
//
// ReadRuntimeImageSelections returns the complete current snapshot. It is called
// once per reconciliation pass; a read error leaves the Manager's last-known-good
// snapshot untouched.
//
// WithRuntimeImageSelectionLock runs fn while holding the cross-process
// per-runtime lock shared by every selection write, passing the CURRENT persisted
// row (nil when the runtime is unpinned). Publication validates the row and
// replaces the registry entry inside fn, so a concurrent CAS write cannot slip
// between the authoritative check and publication. The lock is held only for
// fn's duration, never across an image build.
//
// If fn returns nil, the implementation MUST return nil: a failure to release the
// lock afterwards cannot invalidate a check-and-publication that already
// succeeded, and reporting it would make the caller tear down a live registry
// entry. Errors before or from fn propagate unchanged.
type RuntimeImageSelectionStore interface {
	ReadRuntimeImageSelections(ctx context.Context) ([]RuntimeImageSelection, error)
	WithRuntimeImageSelectionLock(
		ctx context.Context,
		runtimeID string,
		fn func(context.Context, *RuntimeImageSelection) error,
	) error
}

// imageSelection is one validated per-runtime value in the Manager's current
// snapshot. Individual values are immutable; the snapshot map is replaced as a
// whole by periodic refresh. A zero value means the runtime is unpinned.
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

// identity returns the stable, persisted selection identity a preparation
// captures and publication later compares with the authoritative row: the exact
// configured source reference plus the selected digest. An unpinned runtime has
// the empty identity, which is distinct from every pinned identity and from the
// source fingerprint. Generation is deliberately excluded: rewriting the same
// digest under a new generation is not a new image identity and must not rebuild.
func (s imageSelection) identity() string {
	if !s.pinned() {
		return ""
	}
	return s.baseImage()
}

// selectionIdentityFromSelection converts an authoritative persisted row (nil =
// absent/unpinned) to the same stable identity a preparation captures. It does
// not validate the row against the runtime registry; a row that does not match
// what the preparation captured simply yields a different identity and fences
// the publication.
func selectionIdentityFromSelection(selection *RuntimeImageSelection) string {
	if selection == nil || selection.Digest == "" {
		return ""
	}
	return selection.SourceImage + "@" + selection.Digest
}

// WithRuntimeImageSelections supplies the initial snapshot of runtime base-image
// pins read from the shared state database. A pinned entry must name a known
// runtime, carry that runtime's EXACT configured base image, and use a valid
// "sha256:<64 lowercase hex>" multi-platform index digest; NewManager rejects any
// malformed or mismatched entry for a known runtime. Entries for runtime IDs this
// binary does not know are ignored (a newer Relay sharing the database may have
// written them), because no app here can select that runtime.
//
// When a RuntimeImageSelectionStore is also supplied (the worker does), the
// Manager refreshes this snapshot from the shared database once per periodic
// reconciliation pass; a refresh validation failure or read error leaves this
// last-known-good snapshot in place. Without a store the snapshot stays immutable.
// Relay does not automatically pin unselected runtimes.
func WithRuntimeImageSelections(selections []RuntimeImageSelection) ManagerOption {
	return func(o *managerOptions) {
		o.imageSelections = append([]RuntimeImageSelection(nil), selections...)
	}
}

// WithRuntimeImageSelectionStore supplies the shared authoritative selection
// source. The worker wires the state-backed adapter. With a store the Manager can
// refresh its snapshot from the database and fence publication against the
// current row; without one (direct Manager construction, tests, embedders) the
// startup snapshot remains immutable and publication is not cross-process fenced.
func WithRuntimeImageSelectionStore(store RuntimeImageSelectionStore) ManagerOption {
	return func(o *managerOptions) { o.selectionStore = store }
}

// resolveImageSelections validates a complete pin snapshot against the runtime
// registry and returns the pinned subset plus the unknown runtime IDs it ignored.
// A known runtime with a mismatched source image or malformed digest is an error:
// Relay must never silently fall back to an unpinned image for a selection it
// was explicitly given. Refresh validates the whole snapshot BEFORE replacing the
// last-known-good view, so one bad row cannot clear or partially apply the rest.
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

// RuntimeImageSelectionIdentity returns the effective identity of runtimeID in
// the Manager's current validated snapshot: the configured source reference plus
// selected digest for a pinned runtime, or the empty string when unpinned. The
// reconciler compares it with the identity recorded for each app's published
// generation, so a pin change is detected on every app pass even if a snapshot
// refresh already consumed the change notification. It never resolves a registry
// or child digest.
func (m *Manager) RuntimeImageSelectionIdentity(runtimeID string) string {
	return m.imageSelectionFor(runtimeID).identity()
}

// imageSelectionFor returns the validated pin for runtimeID from the Manager's
// current snapshot, or the zero (unpinned) value when none is selected. It is
// captured once at the start of a preparation, so every input derived from that
// preparation observes one consistent selection.
func (m *Manager) imageSelectionFor(runtimeID string) imageSelection {
	if m == nil {
		return imageSelection{}
	}
	m.selectionMu.RLock()
	defer m.selectionMu.RUnlock()
	return m.imageSelections[runtimeID]
}

// RefreshRuntimeImageSelections reads the complete authoritative selection
// snapshot and atomically replaces the Manager's validated view. It returns the
// runtime IDs whose effective selection identity changed (including a runtime
// that became pinned or unpinned), sorted for deterministic dispatch.
//
// A read error or a validation error for a known runtime leaves the
// last-known-good snapshot completely untouched: a malformed row must never
// silently unpin a runtime or fall back to a floating tag. Unknown runtime IDs
// are ignored, exactly as at construction. Without a store the Manager has no
// live source and reports no changes.
func (m *Manager) RefreshRuntimeImageSelections(ctx context.Context) ([]string, error) {
	if m == nil || m.selectionStore == nil {
		return nil, nil
	}
	selections, err := m.selectionStore.ReadRuntimeImageSelections(ctx)
	if err != nil {
		return nil, fmt.Errorf("read runtime image selections: %w", err)
	}
	pins, unknown, err := resolveImageSelections(selections)
	if err != nil {
		return nil, fmt.Errorf("runtime image selection: %w", err)
	}
	for _, runtimeID := range unknown {
		m.Logger().Debug("Runtime: ignoring image selection for an unknown runtime",
			"runtime", runtimeID)
	}

	m.selectionMu.Lock()
	changed := changedImageSelections(m.imageSelections, pins)
	m.imageSelections = pins
	m.selectionMu.Unlock()
	return changed, nil
}

// changedImageSelections returns the sorted runtime IDs whose selection identity
// differs between old and next. A runtime absent from both maps is unchanged; an
// entry removed from next is a change to unpinned. Generation-only rewrites of
// the same source reference and digest are NOT changes: the image identity is
// the ref@digest pair.
func changedImageSelections(old, next map[string]imageSelection) []string {
	var changed []string
	for runtimeID, selection := range next {
		if old[runtimeID] != selection {
			changed = append(changed, runtimeID)
		}
	}
	for runtimeID := range old {
		if _, ok := next[runtimeID]; !ok {
			changed = append(changed, runtimeID)
		}
	}
	sort.Strings(changed)
	return changed
}

// PublishPrepared validates the preparation's captured selection against the
// authoritative row and runs publish in the same per-runtime critical section.
// It is the runtime Manager's publication gate: the caller passes the registry
// mutation that makes the prepared generation live. Live reconciliation passes
// a replacement; worker startup passes the INITIAL registry add, so a selection
// cannot change between startup preparation and the app first becoming visible.
//
// The authoritative row is checked under the same cross-process lock every
// selection CAS write takes, so a pin cannot change between the check and the
// swap. A mismatch, including a row that appeared while an unpinned image was
// built or disappeared while a pinned image was built, returns
// ErrRuntimeImageSelectionStale and publish is never called. A no-runtime app or
// a Manager without a store (direct construction/tests) publishes directly.
// Warm-container state is activated only AFTER publication succeeds, so a
// rejected generation never alters the active app's warm pool, source mount, or
// resources.
func (m *Manager) PublishPrepared(
	ctx context.Context,
	prepared *Prepared,
	publish func() error,
) error {
	if publish == nil {
		return fmt.Errorf("runtime: publish callback is nil")
	}
	if m == nil || prepared == nil || prepared.Runtime == "" || m.selectionStore == nil {
		if err := publish(); err != nil {
			return err
		}
		m.activatePrepared(prepared)
		return nil
	}
	err := m.selectionStore.WithRuntimeImageSelectionLock(ctx, prepared.Runtime,
		func(ctx context.Context, current *RuntimeImageSelection) error {
			if selectionIdentityFromSelection(current) != prepared.ImageSelection {
				return fmt.Errorf(
					"%w: runtime %q built selection %q, authoritative %q",
					ErrRuntimeImageSelectionStale,
					prepared.Runtime,
					prepared.ImageSelection,
					selectionIdentityFromSelection(current),
				)
			}
			return publish()
		})
	if err != nil {
		return err
	}
	m.activatePrepared(prepared)
	return nil
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
