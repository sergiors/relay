package runtime

import (
	"context"
	"fmt"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"relay/internal/app"
)

// serviceImagePullInterval bounds how often Relay performs a REMOTE pull check
// for an external service image. A successful check is recorded in memory (per
// worker, per independent source) and suppresses further remote checks for this
// window; a failed check does not advance the clock, so it is retried at the
// next reconcile and a transient registry outage recovers promptly. Changing a
// service's configured source (under the same name or not) is a new
// independent source with no recorded check, so it is checked immediately. The
// window is deliberately not configurable and never persisted: it is a runtime
// freshness policy, not state.
const serviceImagePullInterval = time.Hour

// ServiceImage is a resolved service source ready to start a container from.
//
// Ref is the image reference passed to the container create call. ID is the
// local image content ID (relay.image_id), used to detect that a moved tag now
// points at different bytes so the running container is replaced; it is empty
// when the reference itself already encodes the content (Relay-built images,
// whose tag embeds the source fingerprint) or when inspection is unavailable.
//
// Entry is the per-container entrypoint override. A nil Entry means the
// container preserves the image's own ENTRYPOINT/CMD — true for the `image`
// source. Only the runtime-managed `entrypoint` source overrides it.
type ServiceImage struct {
	Ref   string
	ID    string
	Entry []string
}

// ResolveServiceImage resolves the desired image for one service from its
// configured source. It is the single place that knows how a service's source
// becomes a runnable image, so the service reconciler stays source-agnostic and
// the two sources share one container lifecycle.
//
//	appImage is the app's own prepared runtime image, used only by an
//	`entrypoint` service (which overrides that image's invocation bootstrap).
//
// Resolution is read-only except for the one lifecycle action a source may
// need: pulling an `image` source's reference. A pull failure is returned as an
// error WITHOUT resolving, so the caller can preserve healthy containers.
func (m *Manager) ResolveServiceImage(
	ctx context.Context,
	fnName string,
	tmpl *app.Template,
	svc app.Service,
	appImage string,
) (ServiceImage, error) {
	switch svc.Source() {
	case app.ServiceSourceImage:
		return m.resolveExternalServiceImage(ctx, fnName, svc.SourceRef())
	default:
		entry, err := ServiceEntry(tmpl.Runtime, svc.Entrypoint)
		if err != nil {
			return ServiceImage{}, err
		}
		return ServiceImage{Ref: appImage, Entry: entry}, nil
	}
}

// resolveExternalServiceImage resolves an `image` source: inspect the local
// image and, when the freshness policy allows, pull it from its registry.
//
// The remote check runs at most once per serviceImagePullInterval per
// independent source (app + image reference), measured from the last
// SUCCESSFUL check; a failed check does not advance that instant (it is retried
// at the next reconcile). A source with no recorded check — a first sighting, a
// changed reference, or a worker restart (the map is in-memory only) — is
// checked immediately. A pull failure is always surfaced so the caller preserves
// whatever containers it already has; a missing local image with no successful
// pull cannot start a replica at all.
//
// It reuses the shared imageInspectContent helper, so the external path never
// requires Relay labels: an image whose inspect response carries none still
// yields its local content ID (relay.image_id), which is what the reconciler uses
// to detect a moved tag.
func (m *Manager) resolveExternalServiceImage(ctx context.Context, fnName, sourceRef string) (ServiceImage, error) {
	id, _, present, err := m.imageInspectContent(ctx, sourceRef)
	if err != nil {
		// Any non-not-found inspect failure is inconclusive: surface it rather
		// than guessing, so a broken daemon never leads to a spurious pull or a
		// container decision on unknown state.
		return ServiceImage{}, fmt.Errorf("inspect image %q: %w", sourceRef, err)
	}

	// A missing local image is pulled immediately regardless of the freshness
	// window: there is no healthy container to preserve, so waiting would only
	// leave the service unable to start. For a present image the window governs,
	// so a healthy service is never re-pulled more than once per interval.
	if !present || m.pullDue(fnName, sourceRef) {
		if err := m.pullImage(ctx, sourceRef); err != nil {
			return ServiceImage{}, fmt.Errorf("pull image %q: %w", sourceRef, err)
		}
		m.recordPullCheck(fnName, sourceRef, m.clock())
		// Re-inspect after a successful pull: a moved tag may now point at
		// different bytes, and the container must be replaced when it does.
		id, _, present, err = m.imageInspectContent(ctx, sourceRef)
		if err != nil {
			return ServiceImage{}, fmt.Errorf("inspect image %q after pull: %w", sourceRef, err)
		}
		if !present {
			return ServiceImage{}, fmt.Errorf("inspect image %q after pull: %w", sourceRef, cerrdefs.ErrNotFound)
		}
	}
	return ServiceImage{Ref: sourceRef, ID: id}, nil
}

// pullDue reports whether a remote pull check is due for fnName's service image
// source reference: true when no successful check is recorded (first sighting,
// changed reference, or worker restart) or when at least one interval has
// elapsed since the last successful one. The check record is updated only on
// success, so a failed pull keeps this true and retries next pass.
func (m *Manager) pullDue(fnName, sourceRef string) bool {
	m.pullMu.Lock()
	defer m.pullMu.Unlock()
	last, ok := m.pullChecks[pullCheckKey(fnName, sourceRef)]
	if !ok {
		return true
	}
	return m.clock().Sub(last) >= serviceImagePullInterval
}

// recordPullCheck stamps a successful pull check at t.
func (m *Manager) recordPullCheck(fnName, sourceRef string, t time.Time) {
	m.pullMu.Lock()
	defer m.pullMu.Unlock()
	if m.pullChecks == nil {
		m.pullChecks = map[string]time.Time{}
	}
	m.pullChecks[pullCheckKey(fnName, sourceRef)] = t
}

// forgetServicePullChecks drops every pull-check record for an app. It is
// called when an app is removed so a later re-added app starts fresh
// (immediate remote check) and the map does not grow without bound across
// removals. It is idempotent and safe with no records.
func (m *Manager) forgetServicePullChecks(fnName string) {
	m.pullMu.Lock()
	defer m.pullMu.Unlock()
	prefix := fnName + "\x00"
	for k := range m.pullChecks {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(m.pullChecks, k)
		}
	}
}

// pullCheckKey is the per-independent-source pull-check key: the app and
// the service's source reference, so two apps never share a freshness
// window, a changed source reference is a new independent source, and the two
// parts cannot collide (the NUL separator is not valid in either an app name
// or an image reference). The key is deliberately the SOURCE reference, not the
// service name: freshness is a property of the image bytes, shared by two
// distinct services that reference the same image.
func pullCheckKey(fnName, sourceRef string) string {
	return fnName + "\x00" + sourceRef
}

// pullImage pulls exactly one image reference (`All` is false: only the tag or
// digest the service names is fetched, never every tag in the repository). The
// response stream is drained to completion so the pull's errors surface.
func (m *Manager) pullImage(ctx context.Context, ref string) error {
	resp, err := m.cli.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	return resp.Wait(ctx)
}
