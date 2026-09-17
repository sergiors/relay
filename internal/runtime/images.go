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

// relayTags lists every local image RepoTag carrying the Relay namespace prefix,
// returning name -> set of full tags. It lists all images and filters client-side
// rather than using server-side filters: single client implementation reused by
// every cleanup path, no dependency on a specific Engine API filter version.
func (m *Manager) relayTags(ctx context.Context) (map[string]map[string]struct{}, error) {
	list, err := m.cli.ImageList(ctx, client.ImageListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list images: %w", err)
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
func (m *Manager) ImageReferencedByManagedContainer(ctx context.Context, image string) (bool, error) {
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

// RemoveImage removes a single image reference. Removing an image that is
// already gone is a success (the daemon reports not-found): log at debug and
// return nil. This is the only path that removes a named Relay image; it never
// removes anything outside an exact reference the caller computed.
//
// Defensive guard: before calling docker, it consults
// ImageReferencedByManagedContainer and refuses (returning a wrapped ErrImageInUse)
// while any Relay-owned container still references the image. This is the single
// enforcement point for "never remove an image a Relay-owned container still
// depends on", guarding every caller (runner async removal, the startup sweep,
// RetireServiceImages). The ImageRemove options stay FORCE-FREE: never Force, an
// image referenced by a Relay-owned container must be removable only after that
// container is gone.
func (m *Manager) RemoveImage(ctx context.Context, image string) error {
	referenced, err := m.ImageReferencedByManagedContainer(ctx, image)
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
			if err := m.RemoveImage(ctx, tag); err != nil {
				if errors.Is(err, ErrImageInUse) {
					// A container still references this image (a leftover not yet
					// replaced this boot). This is expected during the sweep; the
					// owning function's reconcile replaces the container first and
					// only then retires the image. Log at debug and leave it for a
					// later pass.
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
