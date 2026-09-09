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

// RemoveImage removes a single image reference. Removing an image that is
// already gone is a success (the daemon reports not-found): log at debug and
// return nil. This is the only path that removes a named Relay image; it never
// removes anything outside an exact reference the caller computed.
func (m *Manager) RemoveImage(ctx context.Context, image string) error {
	_, err := m.cli.ImageRemove(ctx, image, client.ImageRemoveOptions{})
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
// while the worker was down — is retired. It never touches non-Relay images, and
// a failure removing one image is logged and does not abort the sweep.
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
				if firstErr == nil {
					firstErr = err
				}
				m.log.Printf("image cleanup: skip %s: %v", tag, err)
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
