package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// relayRepoPrefix is the repository name prefix shared by every Relay-owned
// function image (also the prefix ImageRef emits). It is the single namespace
// guard for all image lifecycle operations: nothing outside this prefix is ever
// touched, so a stray non-Relay image on the daemon can never be removed or
// considered a Relay version.
const relayRepoPrefix = "relay-fn-"

// tagPrefixLen is the number of hex fingerprint characters used as the docker
// tag. The full fingerprint is a 64-hex SHA-256 over the function directory;
// that remains authoritative everywhere it is persisted (SQLite, Prepared). The
// tag prefix is an opaque, collision-safe short handle: 16 hex chars = 64 bits,
// and a birthday collision at the function-version counts Relay deals with (a
// handful per function, tens of functions) is astronomically unlikely. Git's
// default short-hash length is 7–12 chars (28–48 bits); 16 chars is comfortably
// beyond that while staying well inside docker's tag length limit combined with
// a validated name (name ≤ 63 chars, "relay-fn-" prefix, ":<16hex>" suffix → ≤
// ~89 chars < 128). A full-fingerprint tag would add nothing but length: the
// fingerprint still uniquely determines the tag, so equality on the tag is
// equality on the source.
const tagPrefixLen = 16

// ImageRef maps a validated function name and its content fingerprint to the
// docker image reference for that exact source version. No sanitizing is needed
// for the name: function names are validated at load time (internal/function) to
// be [a-z0-9][a-z0-9._-]* and not end in '.', so they are already legal docker
// repository names. The reference always carries the short fingerprint tag, so
// every distinct source version of a function is a distinct docker image and can
// be built, reused, and retired independently without ever clobbering a sibling
// version. The "relay-fn-" prefix namespaces all of Relay's images so they never
// collide with unrelated images on the same daemon.
//
// Defensive on short input: fingerprints are always 64 chars in practice, but a
// caller (or a truncated persisted value) passing fewer hex chars must not panic;
// the tag is simply the first min(len, tagPrefixLen) chars.
func ImageRef(name, fingerprint string) string {
	if len(fingerprint) > tagPrefixLen {
		fingerprint = fingerprint[:tagPrefixLen]
	}
	return "relay-fn-" + name + ":" + fingerprint
}

// repoForName returns the repository name (without tag) that a function's images
// all share, e.g. "relay-fn-user-events".
func repoForName(name string) string {
	return relayRepoPrefix + name
}

// nameFromRepo strips the relay-fn- prefix back to the function name, returning
// ("", false) when repo is not Relay-owned. Every Relay version image's repo is
// exactly "relay-fn-<name>" (validated names never contain ':' or '/'), so the
// remainder is unambiguously the function name.
func nameFromRepo(repo string) (string, bool) {
	if !strings.HasPrefix(repo, relayRepoPrefix) {
		return "", false
	}
	name := strings.TrimPrefix(repo, relayRepoPrefix)
	if name == "" {
		return "", false
	}
	return name, true
}

// functionNameFromImage returns the function name encoded in a Relay-owned
// image reference ("relay-fn-<name>:<tag>"), reporting false for a reference
// without a tag or outside the Relay namespace. It is the scoping guard that
// ensures a re-activation can only un-retire the activating function's OWN
// image, never a foreign function's retired reference.
func functionNameFromImage(image string) (string, bool) {
	repo, _, ok := strings.Cut(image, ":")
	if !ok {
		return "", false
	}
	return nameFromRepo(repo)
}

// relayTags lists every local image RepoTag carrying the Relay namespace prefix,
// returning name -> set of full tags. It lists all images and filters client-side
// rather than using server-side filters: single client implementation reused by
// every cleanup path, no dependency on a specific Engine API filter version. It
// is lifecycle-owned (see beginRemovalOperation): once shutdown begins the
// listing is refused, and a listing in flight is joined before the client closes.
func (m *Manager) relayTags(ctx context.Context) (map[string]map[string]struct{}, error) {
	opCtx, finish, err := m.beginRemovalOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	list, err := m.cli.ImageList(opCtx, client.ImageListOptions{})
	if err != nil {
		return nil, m.leaseCoord().shutdownErr(ctx, fmt.Errorf("list images: %w", err))
	}
	byName := map[string]map[string]struct{}{}
	for _, img := range list.Items {
		for _, tag := range img.RepoTags {
			repo, _, ok := strings.Cut(tag, ":")
			if !ok {
				continue
			}
			name, ok := nameFromRepo(repo)
			if !ok {
				continue
			}
			if byName[name] == nil {
				byName[name] = map[string]struct{}{}
			}
			byName[name][tag] = struct{}{}
		}
	}
	return byName, nil
}

// listImagesForRemoval lists all local images through the lifecycle-owned
// removal gate, so a dependency-GC pass that fires after manager shutdown is
// refused (ErrManagerShuttingDown) and a listing in flight is joined before the
// Docker client closes.
func (m *Manager) listImagesForRemoval(ctx context.Context) (client.ImageListResult, error) {
	opCtx, finish, err := m.beginRemovalOperation(ctx)
	if err != nil {
		return client.ImageListResult{}, err
	}
	defer finish()
	list, err := m.cli.ImageList(opCtx, client.ImageListOptions{})
	if err != nil {
		return client.ImageListResult{}, m.leaseCoord().shutdownErr(ctx, fmt.Errorf("list images: %w", err))
	}
	return list, nil
}

// IsRelayImage reports whether image is within one of Relay's own image
// namespaces ("relay-fn-" or "relay-dep-"). It is the guard that keeps the
// image-lease coordinator scoped to images Relay owns: an external service
// image must NEVER be leased, retired, or garbage-collected by Relay.
func IsRelayImage(image string) bool {
	repo, _, ok := strings.Cut(image, ":")
	if !ok {
		repo = image
	}
	return strings.HasPrefix(repo, relayRepoPrefix) || isDepRepo(repo)
}

// imageExists reports whether a local image carrying the exact reference ref is
// present. It is conservative: an error (daemon hiccup, broken daemon) is
// reported as "not present" so the caller rebuilds rather than trusting a
// stale cache. Fingerprint equality is the identity — the reference already
// encodes the full fingerprint's first 16 hex chars — so no content comparison
// is performed here.
func (m *Manager) imageExists(ctx context.Context, ref string) bool {
	if _, err := m.cli.ImageInspect(ctx, ref); err != nil {
		// Any inspect failure means we cannot confirm the image is usable;
		// rebuild. A not-found is the common case (first build).
		return false
	}
	return true
}

// resolvedImage is the immutable content identity Relay resolves for a managed
// function image before leasing or creating an execution container. It pairs the
// mutable reference (used for labels, spans, and image retirement, which is
// scoped to references) with the immutable content identity (the Docker image ID
// plus the Relay fingerprint metadata label). Warm container generations key on
// the identity, so a tag whose content changes — the same reference resolving to
// a new image ID — rotates its containers instead of reusing stale ones.
//
// It is deliberately label-agnostic: the Relay fingerprint is OPTIONAL metadata,
// so an image without Relay labels (e.g. an external service image) simply
// contributes no fingerprint and is still identified by its content ID.
type resolvedImage struct {
	// ref is the mutable image reference (e.g. "relay-fn-fn:abc"). It scopes
	// image retirement (InvalidateImage/RemoveImage) and stays the relay.image
	// label and the span attribute; it is NOT the generation identity.
	ref string
	// id is the Docker image ID (content digest) resolved by inspecting ref.
	// Empty when resolution is unavailable (a Docker-less direct/test caller or
	// a daemon hiccup); the identity then degrades to the reference.
	id string
	// fingerprint is the Relay fingerprint metadata (relay.fingerprint) carried
	// by a managed function image, or the prepared content fingerprint when the
	// label is absent. It is never required.
	fingerprint string
}

// identity returns the immutable generation-key half of the image identity. Two
// resolutions with the same reference but different content (a moved/retagged
// image) produce different identities, which is what makes a generation rotate;
// two resolutions of identical content produce the same identity, which is what
// lets a warm container be reused.
func (r resolvedImage) identity() string {
	return imageIdentityKey(r.ref, r.id, r.fingerprint)
}

// createImage returns the reference to hand to Docker's container create. When
// the content ID is known it is used directly, so the container is created from
// exactly the bytes that were resolved and the inspect-then-create TOCTOU window
// is closed; otherwise the mutable reference is used.
func (r resolvedImage) createImage() string {
	if r.id != "" {
		return r.id
	}
	return r.ref
}

// imageIdentityKey builds the immutable content-identity key for a resolved
// managed image. It embeds the reference first so identity retirement and
// re-activation stay scoped to a function's own tag, then the Docker image ID
// and the Relay fingerprint when known. A fallback to the reference keeps a
// Docker-less caller (direct tests) and a transient inspect failure pooling under
// the historical reference identity rather than rotating spuriously. An empty
// reference (a no-runtime function) yields an empty identity, matching the
// historical "no image" key.
func imageIdentityKey(ref, id, fingerprint string) string {
	if ref == "" {
		return ""
	}
	switch {
	case id != "":
		return ref + "\x00" + id + "\x00" + fingerprint
	case fingerprint != "":
		return ref + "\x00" + fingerprint
	default:
		return ref
	}
}

// imageInspectContent inspects ref and returns its immutable content ID and the
// Relay fingerprint metadata label when present. present is false (with a nil
// error) when no local image carries the reference; a non-not-found inspect
// failure is returned so the caller never decides on unknown daemon state. Relay
// labels are OPTIONAL: an image without them still yields its content ID, so an
// external service image is never required to carry Relay metadata.
func (m *Manager) imageInspectContent(ctx context.Context, ref string) (id, fingerprint string, present bool, err error) {
	insp, err := m.cli.ImageInspect(ctx, ref)
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	id = insp.ID
	if insp.Config != nil {
		fingerprint = insp.Config.Labels[labelFingerprint]
	}
	return id, fingerprint, true, nil
}

// resolveImageIdentity resolves ref to its immutable content identity. It uses
// the injected seam when set (tests), the real Docker inspect otherwise, and
// degrades to the reference-only identity when the image cannot be resolved (a
// Docker-less direct/test caller, or a daemon hiccup) so an invocation is never
// failed merely because identity resolution was unavailable. The function's
// prepared fingerprint is the fallback metadata when the image carries no
// relay.fingerprint label.
func (m *Manager) resolveImageIdentity(ctx context.Context, ref, fingerprint string) resolvedImage {
	img := resolvedImage{ref: ref, fingerprint: fingerprint}
	if ref == "" {
		return img
	}
	if m.resolveImageIdentityFn != nil {
		resolved, err := m.resolveImageIdentityFn(ctx, ref, fingerprint)
		if err != nil {
			return img
		}
		resolved.ref = ref
		if resolved.fingerprint == "" {
			resolved.fingerprint = fingerprint
		}
		return resolved
	}
	if m.cli == nil {
		return img
	}
	id, labelFingerprint, present, err := m.imageInspectContent(ctx, ref)
	if err != nil {
		m.Logger().Debug("Runtime: image identity inspect failed; using reference identity",
			"image", ref, "error", err)
		return img
	}
	if !present {
		return img
	}
	img.id = id
	if labelFingerprint != "" {
		img.fingerprint = labelFingerprint
	}
	return img
}

// ErrImageInUse is returned (wrapped) by RemoveImage when the image is still
// referenced by a Relay-owned container. It marks a normal transitional state —
// a service container on the old image that the reconcile has not yet replaced —
// so callers can treat it as a skip (retry/defer) rather than a genuine cleanup
// failure. It must never be caused by forced removal: an image referenced by a
// Relay-owned container is removable only after that container is gone.
var ErrImageInUse = errors.New("image still referenced by a relay-owned container")

// ImageReferencedByManagedContainer reports whether any Relay-owned container
// (event, schedule, or service) still references the given image via its
// relay.image label. It is strict: only containers whose labels classify them as
// Relay-owned AND whose relay.image exactly matches are counted; non-Relay
// containers are never inspected or touched. An error listing containers is
// propagated to the caller so a conservative failure can refuse removal rather
// than delete on unknown state.
//
// It is lifecycle-owned: once manager shutdown has begun it refuses with
// ErrManagerShuttingDown instead of listing containers against a closing client,
// and a listing already in flight is joined before the client closes.
func (m *Manager) ImageReferencedByManagedContainer(ctx context.Context, image string) (bool, error) {
	opCtx, finish, err := m.beginRemovalOperation(ctx)
	if err != nil {
		return false, err
	}
	defer finish()
	referenced, err := m.imageReferencedByManagedContainer(opCtx, image)
	if err != nil {
		return false, m.leaseCoord().shutdownErr(ctx, err)
	}
	return referenced, nil
}

// imageReferencedByManagedContainer is the un-gated container-reference guard,
// used by removal paths that already hold a lifecycle-owned removal operation
// (retireOwned → removeImageLocked) so the guard does not double-register.
func (m *Manager) imageReferencedByManagedContainer(ctx context.Context, image string) (bool, error) {
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return false, fmt.Errorf("list containers for %s: %w", image, err)
	}
	for _, c := range list.Items {
		if isManagedContainer(c.Labels) && c.Labels[labelImage] == image {
			return true, nil
		}
	}
	return false, nil
}

// RemoveImage removes a single image reference through the single production
// ownership authority. It commits the image to retirement (rejecting NEW
// independent leases while work admitted before this point drains), waits on ctx
// for every admitted lease to drain, then consults
// ImageReferencedByManagedContainer (the cross-process / warm-container defense)
// and performs a FORCE-FREE ImageRemove.
//
// It is the caller-visible authority-aware removal: a direct Prepare caller
// that never published its handle holds an admitted lease and must release it
// (Prepared.ReleaseLease) for the image to become removable; a published image
// is held by its registry publication lease and becomes removable only after
// the entry is superseded. When ctx is done before the drain completes the image
// stays committed to removal and the returned error is ctx.Err(), so the caller
// retries later rather than removing early.
//
// On any non-removal outcome (a container reference, a daemon error) the
// retirement gate is cleared so the image stays usable and a later natural
// cleanup pass retries. On success the image's coordination state is dropped.
// A duplicate caller that finds removal already in progress (another goroutine
// owns the retirement) gets a wrapped ErrImageRetiring and must defer; it never
// races a second ImageRemove. Removing an image that is already gone is a
// success (the daemon reports not-found). This is the only path that removes a
// named Relay image; it never removes anything outside an exact reference the
// caller computed. The ImageRemove options stay FORCE-FREE.
//
// It is lifecycle-owned: once the manager has begun shutting down it returns
// ErrManagerShuttingDown without issuing any Docker call, and a removal already
// in flight is cancelled and joined before the Docker client closes.
func (m *Manager) RemoveImage(ctx context.Context, image string) error {
	if _, ok := ctx.Deadline(); !ok {
		// Defense: never block forever on a drain when the caller passed an
		// unbounded context. Callers that need a longer bound pass a deadline.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, retireBound)
		defer cancel()
	}
	_, err := m.retireOwned(ctx, image, true)
	return err
}

// RemoveImageNow is the NON-BLOCKING form of RemoveImage: it commits the image
// to retirement and, when every admitted lease is already drained, removes it.
// When any admitted lease is still held it clears the gate and returns a
// wrapped ErrImageRetiring immediately, never blocking. The startup sweep and
// dependency GC use it so a leaked/legitimately-held lease can never stall a
// best-effort cleanup pass; the runner's retirement path uses the blocking
// RetireImageLease so an in-flight execution's image is removed as soon as it
// drains.
func (m *Manager) RemoveImageNow(ctx context.Context, image string) error {
	_, err := m.retireOwned(ctx, image, false)
	return err
}

// retireOwned commits image to retirement and, when it owns removal, performs
// it. When wait is true it blocks on ctx for every admitted lease to drain;
// when false a still-held lease is reported as ErrImageRetiring immediately.
// removed reports whether this call actually issued a successful removal.
//
// The whole operation is lifecycle-owned: its context is derived from both the
// caller's ctx and the manager's shutdown signals (see Manager.removalContext),
// and it is registered with the coordinator's beginRemoval, so manager shutdown
// refuses new removals and joins this one before the Docker client closes.
func (m *Manager) retireOwned(ctx context.Context, image string, wait bool) (removed bool, err error) {
	if image == "" {
		return false, nil
	}
	rmCtx, stop := m.removalContext(ctx)
	defer stop()
	coord := m.leaseCoord()
	drained, owner, err := coord.beginRemoval(image)
	if err != nil {
		// The manager is shutting down: no new Docker removal may begin. A
		// caller whose own context is already done sees its own error (the
		// cancellation, not a synthesized shutdown sentinel), preserving caller
		// cancellation semantics.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, err
	}
	if !owner {
		// Another caller already owns this image's removal: defer without
		// racing a second ImageRemove.
		return false, fmt.Errorf("remove image %s: %w", image, ErrImageRetiring)
	}
	// Every owned removal concludes exactly once (success or failure), so the
	// coordinator's join registration is always released and the retirement gate
	// is always cleared on a non-removal outcome.
	concluded := false
	defer func() {
		if !concluded {
			coord.finishRemoval(image, false)
		}
	}()
	// Drop warm execution containers first so the container-reference guard
	// clears without waiting for an in-flight invocation to release.
	if m.containers != nil {
		m.containers.invalidateImage(image)
	}
	if wait {
		select {
		case <-drained:
		case <-rmCtx.Done():
			// The bound expired or the manager began shutting down with admitted
			// work still draining. Clear the gate (the image stays reusable and a
			// later cleanup pass re-owns the removal) rather than leaving it
			// permanently committed. A shutdown abort is reported distinctly so
			// the runner defers instead of retrying against a closed client.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			return false, fmt.Errorf("remove image %s: %w", image, ErrManagerShuttingDown)
		}
	} else {
		select {
		case <-drained:
		default:
			// Admitted work still holds the image: clear the gate (the image
			// stays usable) and report the retryable transitional state.
			return false, fmt.Errorf("remove image %s: %w", image, ErrImageRetiring)
		}
	}
	// The drain may close concurrently with shutdown: re-check the removal
	// context so a removal that won the drain race still aborts instead of
	// issuing Docker calls against a closing client.
	if err := rmCtx.Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, fmt.Errorf("remove image %s: %w", image, ErrManagerShuttingDown)
	}
	if err := m.removeImageLocked(rmCtx, image); err != nil {
		return false, coord.shutdownErr(ctx, err)
	}
	coord.finishRemoval(image, true)
	concluded = true
	return true, nil
}

// removeImageLocked performs the container-reference guard and the actual
// FORCE-FREE ImageRemove for an image whose retirement gate is already held and
// whose removal operation is already registered with the coordinator. It uses
// the un-gated guard so the operation is registered exactly once.
func (m *Manager) removeImageLocked(ctx context.Context, image string) error {
	referenced, err := m.imageReferencedByManagedContainer(ctx, image)
	if err != nil {
		return fmt.Errorf("remove image %s: %w", image, err)
	}
	if referenced {
		return fmt.Errorf("remove image %s: %w", image, ErrImageInUse)
	}
	_, err = m.cli.ImageRemove(ctx, image, client.ImageRemoveOptions{})
	if errors.Is(err, cerrdefs.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove image %s: %w", image, err)
	}
	return nil
}

// FunctionImageTags lists every full image reference (repo:tag) that belongs to
// the named function's repository ("relay-fn-<name>"). It is the per-function
// listing the runner uses to retire each superseded version independently with
// in-flight safety.
func (m *Manager) FunctionImageTags(ctx context.Context, name string) ([]string, error) {
	repo := repoForName(name)
	byName, err := m.relayTags(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for tag := range byName[name] {
		if r, _, ok := strings.Cut(tag, ":"); ok && r == repo {
			out = append(out, tag)
		}
	}
	return out, nil
}

// RemoveImagesExcept removes every Relay-owned image NOT in the keep set. keep
// maps full image references (exactly what ImageRef produces) to true. It is the
// conservative startup sweep: after functions are loaded and prepared, the keep
// set holds (a) each current function's expected ImageRef for its fingerprint
// and (b) the last-active image recorded in state (so a recovery mid-swap never
// removes the version that may still serve). Everything Relay-owned outside keep
// — superseded versions of existing functions and versions of functions removed
// while the worker was down — is retired. The sweep is double-defensive: the
// keep-set covers the known-referenced images up front, and RemoveImage's
// container-reference guard covers the transitional case where a container still
// references an image not in the keep-set (a leftover that this boot has not
// yet replaced). It never touches non-Relay images. A failure removing one image
// is logged and does not abort the sweep; an in-use skip (ErrImageInUse) is the
// normal transitional state and is neither counted nor surfaced.
func (m *Manager) RemoveImagesExcept(ctx context.Context, keep map[string]bool) (int, error) {
	byName, err := m.relayTags(ctx)
	if err != nil {
		return 0, err
	}
	removed := 0
	var firstErr error
	for _, tags := range byName {
		for tag := range tags {
			if keep[tag] {
				continue
			}
			if err := m.RemoveImageNow(ctx, tag); err != nil {
				if errors.Is(err, ErrImageRetiring) || errors.Is(err, ErrImageInUse) || errors.Is(err, ErrManagerShuttingDown) {
					// An admitted lease (a build/execution/publication still in
					// flight), a container still references this image, or the
					// manager began shutting down. This is expected during the
					// sweep; the owning function's reconcile retires it on a
					// later pass. Log at debug and leave it.
					m.log.Debug("Image cleanup: image still in use; skipping", "image", tag)
					continue
				}
				if firstErr == nil {
					firstErr = err
				}
				m.log.Warn("Image cleanup: skip failed", "image", tag, "error", err)
				continue
			}
			removed++
		}
	}
	if firstErr != nil {
		return removed, firstErr
	}
	return removed, nil
}
