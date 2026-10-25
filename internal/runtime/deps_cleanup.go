package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
)

// CleanupUnusedDependencies removes managed dependency images that no managed
// app image references anymore. It is the lifecycle-driven tail of Relay's
// dependency-image ownership. It runs at three natural lifecycle points, and
// only those: once at worker startup (after the boot sweep removed superseded
// app images), once after every successful app-image removal in the runner, and
// once after a FAILED app image build in Prepare (after that preparation
// releases BOTH its local dependency lease and its cross-process shared use
// protection, so a layer the failed preparation uniquely consumed does not
// linger unreferenced). It is never periodic, never on a ticker, and never
// forced.
//
// Ownership is derived purely from the managed-image labels (see labels.go):
//   - a managed APP image is an image whose relay.type == ImageTypeApp;
//     its relay.dependency label names the exact dependency image it was built
//     FROM. The union of those values is the referenced set.
//   - a managed DEPENDENCY image is an image whose relay.type == ImageTypeDependency
//     that also carries a RepoTag with the relay-dep- prefix. Each such image is
//     a removal candidate.
//
// Repository names are never the sole classification signal. An image with NO
// relay.type label is unmanaged and ignored entirely, whether its name looks
// like relay-dep-* or relay-app-*: a legacy image built before this label model
// existed has no label, so it is left alone forever (inert garbage; backward
// compat for pre-label builds is explicitly out of scope). The app-image
// cleanup paths (relayTags / AppImageTags / RemoveImagesExcept) apply the
// same strict, label-derived rule, so an unlabeled relay-app-* image is likewise
// never retired. A dependency image that carries the dependency label but NO
// tagged RepoTag (a dangling image a failed build can leave behind) is also
// skipped: GC deliberately keeps its scope tight to tagged, labeled dependency
// images, so it can always remove by reference.
//
// A candidate is removed only when it is absent from the label-derived
// referenced set AND this process holds the cross-process EXCLUSIVE dependency
// protection for it. The referenced set is authoritative: it is derived from
// the managed app images' relay.dependency labels, so a layer a completed app
// image still inherits is never a candidate.
//
// The Docker daemon is NOT a parent-reference guard. Force-free ImageRemove can
// untag a dependency image even while a completed child app image still
// references its layers (observed on Docker 29.9.2), so a successful removal is
// never evidence that the layer was orphaned. Removal therefore depends on two
// coordination layers, both required:
//
//   - the per-Manager image lease coordinator (in-process uses: an admitted
//     Prepare consuming the layer, a retirement already draining), and
//   - the cross-process DependencyImageCoordinator (shared use leases held by
//     any manager/process; the exclusive GC lease held here).
//
// The exclusive lease is acquired PER CANDIDATE and non-blockingly; the image
// set is freshly listed and repartitioned WHILE the lease is held, and only
// then is the layer removed. That revalidation confirms BOTH that the target is
// still an eligible tagged managed dependency candidate (another pass may have
// removed or untagged it while this one waited for the lease) and that no
// managed app image references it; a candidate that no longer qualifies is
// skipped without a DELETE and is never counted as removed. Contended candidates
// are skipped; any coordination error is fail-closed, so the pass removes
// nothing further.
// Removal stays FORCE-FREE. A Manager with no configured coordinator removes
// nothing and reports ErrDependencyImageCoordination.
func (m *Manager) CleanupUnusedDependencies(ctx context.Context) (int, error) {
	if m.depCoord == nil {
		return 0, fmt.Errorf("cleanup unused dependencies: %w",
			ErrDependencyImageCoordination)
	}
	list, err := m.listImagesForRemoval(ctx)
	if err != nil {
		return 0, fmt.Errorf("cleanup unused dependencies: list images: %w", err)
	}
	candidates, referenced := partitionManagedImages(list.Items)

	// Sweep every candidate; keep those referenced, remove the rest. The initial
	// partition only selects candidates cheaply: the authoritative decision is
	// re-made under the exclusive protection below, because another process may
	// have completed a build from (or of) this layer since this listing.
	removed := 0
	var firstErr error
	for _, dep := range candidates {
		if referenced[dep] {
			m.log.Debug("Dependency image still referenced; keeping", "dep_image", dep)
			continue
		}
		gcLease, acquired, err := m.tryAcquireDependencyImageGC(ctx, dep)
		if err != nil {
			// Fail closed: without the exclusive view no removal is safe, and the
			// error may mean coordination is unavailable rather than merely
			// contended. Stop the pass; the next natural lifecycle point retries.
			return removed, fmt.Errorf("cleanup unused dependencies: %s: %w", dep, err)
		}
		if !acquired {
			// A use lease is held (this manager, another manager, or another
			// process) or a GC pass already owns this layer. Skip it; never wait
			// inside a bounded GC pass.
			m.log.Debug("Dependency image still in use; keeping", "dep_image", dep)
			continue
		}
		view, rerr := m.revalidateDependencyImage(ctx, dep)
		if rerr != nil {
			_ = gcLease.Release()
			return removed, fmt.Errorf(
				"cleanup unused dependencies: revalidate %s: %w", dep, rerr,
			)
		}
		if !view.candidate {
			// Another GC pass removed or untagged the layer while this pass waited
			// for the exclusive lease. There is nothing left to remove, and a second
			// ImageRemove would only report a no-op removal (ErrNotFound) as work.
			_ = gcLease.Release()
			m.log.Debug("Dependency image no longer a removal candidate; skipping",
				"dep_image", dep)
			continue
		}
		if view.referenced {
			// An app image referencing this layer appeared while the exclusive
			// lease was being acquired (a concurrent build completed and released
			// its shared use). The label-derived referenced set wins.
			_ = gcLease.Release()
			m.log.Debug("Dependency image became referenced; keeping", "dep_image", dep)
			continue
		}
		// Re-prove the exclusive lease immediately before the Docker removal: a
		// session dropped while the images were being listed would leave the
		// decision unprotected, so the pass fails closed instead of removing.
		if err := gcLease.Verify(ctx); err != nil {
			_ = gcLease.Release()
			return removed, fmt.Errorf("cleanup unused dependencies: %w: %s: %w",
				ErrDependencyImageCoordination, dep, err)
		}
		err = m.removeUnreferencedDependencyImage(ctx, dep)
		if releaseErr := gcLease.Release(); releaseErr != nil {
			// The removal already concluded; a failed release can leave the layer
			// pinned on a live session only until this handle closes, so it must
			// never turn a successful removal into a failure.
			m.log.Debug("Dependency image removal protection release failed",
				"dep_image", dep, "error", releaseErr)
		}
		if err != nil {
			// An admitted local lease (an active Prepare consuming this layer via
			// FROM) or an in-progress manager shutdown is the normal transitional
			// state, not a failure: skip at debug and retry at the next natural
			// lifecycle point.
			if errors.Is(err, ErrImageRetiring) || errors.Is(err, ErrManagerShuttingDown) {
				m.log.Debug("Dependency image still in use; keeping", "dep_image", dep)
				continue
			}
			// A genuine removal failure: keep the layer and retry at the next
			// natural lifecycle point. Only the first genuine error is surfaced, so
			// one transient failure never aborts the whole sweep.
			if firstErr == nil {
				firstErr = err
			}
			m.log.Debug("Dependency image removal failed; keeping",
				"dep_image", dep, "error", err)
			continue
		}
		removed++
		m.log.Debug("Removed unused dependency image", "dep_image", dep)
	}
	if firstErr != nil {
		return removed, firstErr
	}
	return removed, nil
}

// dependencyImageRevalidation is the fresh, exclusive-lease view of one
// dependency-image candidate. Both facts are needed for a removal decision, and
// they are distinct: a layer can remain unreferenced yet stop being removable
// (another pass removed or untagged it), or become referenced yet stay a
// candidate.
type dependencyImageRevalidation struct {
	// candidate reports whether dep is STILL an eligible removal candidate in the
	// daemon's current listing: a tagged, strictly managed dependency image (see
	// partitionManagedImages). A gone, untagged, or unmanaged layer reports false.
	candidate bool
	// referenced reports whether a managed app image still names dep as its
	// dependency.
	referenced bool
}

// revalidateDependencyImage re-reads the daemon's image set while the caller
// holds the cross-process exclusive dependency protection and reports BOTH
// facts a removal decision needs: the target is still an eligible candidate,
// and no managed app image references it. Checking only the referenced set is
// not enough: another GC pass may have removed or untagged the layer while this
// pass waited for the lease, and issuing another ImageRemove would then count a
// no-op removal. Any listing error fails closed.
func (m *Manager) revalidateDependencyImage(
	ctx context.Context,
	dep string,
) (dependencyImageRevalidation, error) {
	list, err := m.listImagesForRemoval(ctx)
	if err != nil {
		return dependencyImageRevalidation{}, err
	}
	candidates, referenced := partitionManagedImages(list.Items)
	view := dependencyImageRevalidation{referenced: referenced[dep]}
	for _, candidate := range candidates {
		if candidate == dep {
			view.candidate = true
			break
		}
	}
	return view, nil
}

// removeUnreferencedDependencyImage is the FORCE-FREE removal of one already
// decision-unreferenced dependency image. It commits the image to retirement
// through the SAME lease coordinator that gates app images and, when every
// admitted lease has drained, performs the ImageRemove.
//
// An active Prepare holds a dependency lease through its FROM consumption, so
// this call must not remove the layer between the probe and the build. It
// therefore does NOT block waiting for that build: if any admitted lease is
// still held it clears the gate and returns a wrapped ErrImageRetiring so the
// caller keeps the layer and retries at the next natural lifecycle point. This
// keeps the GC pass bounded (it runs inside a fixed reconcile budget) while
// still making the TOCTOU window unremovable.
//
// It deliberately does NOT go through Manager.RemoveImage: that path guards
// against containers via ImageReferencedByManagedContainer, which is irrelevant
// for a dependency image (no container ever references a dependency image) and
// would add a container listing round trip per removal. The label-based
// ownership decision (plus the caller's cross-process exclusive lease) is the
// guard; the per-Manager lease gate is the in-process TOCTOU guard. The daemon
// is NOT a parent-reference guard: force-free ImageRemove can untag a
// dependency even while a completed child app image still references its layers.
// This was observed on Docker 29.9.2, so removal must not rely on daemon refusal
// to prove the layer is unreferenced. Removal stays FORCE-FREE. An already-gone
// image is a success (ErrNotFound→nil), matching RemoveImage. A concurrent
// retirement of the same dependency reports a wrapped ErrImageRetiring so the
// caller keeps it and retries later.
func (m *Manager) removeUnreferencedDependencyImage(ctx context.Context, dep string) error {
	rmCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return fmt.Errorf("remove dependency image %s: %w", dep, err)
	}
	defer release()
	coord := m.leaseCoord()
	drained, owner, err := coord.beginRemoval(dep)
	if err != nil {
		return fmt.Errorf("remove dependency image %s: %w", dep, err)
	}
	if !owner {
		return fmt.Errorf("remove dependency image %s: %w", dep, ErrImageRetiring)
	}
	concluded := false
	defer func() {
		if !concluded {
			coord.finishRemoval(dep, false)
		}
	}()
	select {
	case <-drained:
	default:
		// An admitted lease still holds the layer (an active Prepare consuming it
		// via FROM). Clear the gate so the layer stays usable and retry later.
		// The removal is non-blocking by design: the dependency GC pass runs
		// inside a fixed reconcile budget.
		return fmt.Errorf("remove dependency image %s: %w", dep, ErrImageRetiring)
	}
	// The drain and shutdown may race: re-check the removal context so a
	// dependency removal that won the drain still aborts rather than racing a
	// closing client.
	if rmCtx.Err() != nil {
		return fmt.Errorf("remove dependency image %s: %w", dep, ErrManagerShuttingDown)
	}
	_, err = m.cli.ImageRemove(rmCtx, dep, client.ImageRemoveOptions{})
	if err != nil && !errors.Is(err, cerrdefs.ErrNotFound) {
		return fmt.Errorf("remove dependency image %s: %w", dep, coord.shutdownErr(ctx, err))
	}
	coord.finishRemoval(dep, true)
	concluded = true
	return nil
}

// partitionManagedImages classifies a daemon's images into dependency-image
// removal candidates and the set of dependency references that managed app
// images still hold. It is a pure function (no I/O) so it can be unit-tested
// against synthetic image summaries. See CleanupUnusedDependencies for the
// ownership semantics; this app implements the label-driven classification
// only.
//
// DANGLING images (empty RepoTags) are classified as neither: they are
// unreachable build residue. This matters because racing same-content builds
// or a tag-removal that left the underlying image ID behind WITH its labels
// would otherwise let a dangling labeled app image hold its dependency hostage
// forever: its tags were removed, its lifecycle ended (nothing Relay
// manages can start it — the reconciler, the state DB, and containers all
// reference images by tag), so GC derives references from TAGGED app images
// only. Do NOT lean on a daemon refusal to protect such a layer: force-free
// removal can untag a dependency that a completed child image still references.
// The exclusive cross-process lease and this tagged-reference set — not the
// daemon — are the authority.
func partitionManagedImages(items []image.Summary) (candidates []string, referenced map[string]bool) {
	referenced = make(map[string]bool)
	candidateSeen := make(map[string]bool)
	for _, img := range items {
		if len(img.RepoTags) == 0 {
			// Dangling (untagged) image: build residue, classified as neither a
			// reference holder nor a candidate. See the doc comment above.
			continue
		}
		switch img.Labels[labelType] {
		case ImageTypeApp:
			// A tagged managed app image holds exactly one dependency
			// reference (its parent). Collect it.
			if dependency := img.Labels[labelDependency]; dependency != "" {
				referenced[dependency] = true
			}
		case ImageTypeDependency:
			// A managed dependency image's full tagged references name the
			// layers GC may prune. Only tagged references count (an untagged,
			// dangling dependency image is skipped) and only those carrying the
			// relay-dep- prefix. A dependency reference is "relay-dep-<fp>" with
			// NO tag, which Docker normalizes in the daemon's RepoTags to a
			// ":latest" suffix; normalize every tag to its repo so candidates
			// compare equal to the label value the way RemoveImage accepts them
			// ("relay-dep-<fp>" removes the latest reference). Repo dedupe
			// prevents the same reference appearing once per daemon tag.
			for _, tag := range img.RepoTags {
				repo, _, _ := strings.Cut(tag, ":")
				if isDepRepo(repo) && !candidateSeen[repo] {
					candidateSeen[repo] = true
					candidates = append(candidates, repo)
				}
			}
		}
	}
	return candidates, referenced
}
