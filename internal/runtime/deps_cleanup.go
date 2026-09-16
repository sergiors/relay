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
// function image references anymore. It is the lifecycle-driven tail of Relay's
// dependency-image ownership: every successfully retired function image is
// re-run here so a dependency layer whose last referencing function version has
// been removed is pruned. It runs once at worker startup (after the boot sweep
// removed superseded function images) and once after every successful
// function-image removal in the runner — never periodically, never on a ticker,
// and never forced.
//
// Ownership is derived purely from the managed-image labels (see labels.go):
//   - a managed FUNCTION image is an image whose relay.type == ImageTypeFunction;
//     its relay.dependency label names the exact dependency image it was built
//     FROM. The union of those values is the referenced set.
//   - a managed DEPENDENCY image is an image whose relay.type == ImageTypeDependency
//     that also carries a RepoTag with the relay-dep- prefix. Each such image is
//     a removal candidate.
//
// Repository names are never the sole classification signal. An image with NO
// relay.type label is unmanaged and ignored entirely, whether its name looks
// like relay-dep-* or relay-fn-*: a legacy dependency image built before this
// label model existed has no label, so it is left alone forever (inert garbage;
// backward compat for pre-label builds is explicitly out of scope), and the
// legacy function-image cleanup paths already handle unlabeled relay-fn-* images
// by name. A dependency image that carries the dependency label but NO tagged
// RepoTag (a dangling image a failed build can leave behind) is also skipped:
// GC deliberately keeps its scope tight to tagged, labeled dependency images,
// so it can always remove by reference.
//
// A candidate is removed only when it is absent from the referenced set. Before
// removing, the daemon itself is the second line of defense: a dependency image
// that is still a parent of ANY built function image (including a function
// image that predates labels and thus does not appear in the referenced set)
// is refused by the daemon when no Force is given, so a removal error here is
// treated conservatively as "still referenced; keeping" and the image is left
// for the next natural pass — never force-removed. A genuine listing error is
// propagated (do not assume orphaned).
func (m *Manager) CleanupUnusedDependencies(ctx context.Context) (int, error) {
	list, err := m.cli.ImageList(ctx, client.ImageListOptions{})
	if err != nil {
		return 0, fmt.Errorf("cleanup unused dependencies: list images: %w", err)
	}
	candidates, referenced := partitionManagedImages(list.Items)

	// Sweep every candidate; keep those referenced, remove the rest. Removal is
	// FORCE-FREE and best-effort: the label check above is the ownership
	// decision, and the daemon's refusal to remove an image that is still a
	// parent is the safety net for any function image (labeled or not) that
	// still inherits this dependency's layers. Errors are conservative — never
	// assume orphaned — so a removal that fails for any reason is skipped and
	// retried at the next natural lifecycle point. The first genuine error is
	// propagated (matching RemoveImagesExcept style) without aborting the sweep.
	removed := 0
	var firstErr error
	for _, dep := range candidates {
		if referenced[dep] {
			m.log.Debug("Dependency image still referenced; keeping", "dep_image", dep)
			continue
		}
		if err := m.removeUnreferencedDependencyImage(ctx, dep); err != nil {
			// The daemon refused (a function image — labeled or legacy — still
			// inherits this dependency's layers, or another worker is mid-build
			// FROM it). Treat it as still referenced and keep it; the next
			// natural lifecycle point retries. Only the first genuine error is
			// surfaced, so one transient failure never aborts the whole sweep.
			if firstErr == nil {
				firstErr = err
			}
			m.log.Debug(fmt.Sprintf("Dependency image still referenced; keeping %s: %v", dep, err))
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

// removeUnreferencedDependencyImage is the FORCE-FREE removal of one already
// decision-unreferenced dependency image. It deliberately does NOT go through
// Manager.RemoveImage: that path guards against containers via
// ImageReferencedByManagedContainer, which is irrelevant for a dependency image
// (no container ever references a dependency image) and would add a container
// listing round trip per removal. The label-based ownership decision above is
// the guard; the daemon refuses a force-free removal of an image that is still
// a parent of any other image. An already-gone image is a success
// (ErrNotFound→nil), matching RemoveImage.
func (m *Manager) removeUnreferencedDependencyImage(ctx context.Context, dep string) error {
	_, err := m.cli.ImageRemove(ctx, dep, client.ImageRemoveOptions{})
	if errors.Is(err, cerrdefs.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove dependency image %s: %w", dep, err)
	}
	return nil
}

// partitionManagedImages classifies a daemon's images into dependency-image
// removal candidates and the set of dependency references that managed function
// images still hold. It is a pure function (no I/O) so it can be unit-tested
// against synthetic image summaries. See CleanupUnusedDependencies for the
// ownership semantics; this function implements the label-driven classification
// only.
func partitionManagedImages(items []image.Summary) (candidates []string, referenced map[string]bool) {
	referenced = make(map[string]bool)
	candidateSeen := make(map[string]bool)
	for _, img := range items {
		switch img.Labels[labelType] {
		case ImageTypeFunction:
			// A managed function image holds exactly one dependency reference
			// (its parent). Collect it.
			if d := img.Labels[labelDependency]; d != "" {
				referenced[d] = true
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
