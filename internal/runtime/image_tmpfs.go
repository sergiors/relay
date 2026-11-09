package runtime

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/client"
)

// serviceTmpfsImageInspectTimeout bounds the ImageSave stream used to prove a
// service tmpfs target is safe. The image archive can be large, so the bound is
// deliberately generous; a daemon that cannot deliver it in time fails the
// create closed rather than letting the check block indefinitely.
const serviceTmpfsImageInspectTimeout = 5 * time.Minute

// maxImageManifestBytes bounds the in-memory image manifest read from a Docker
// image archive. Real manifests are a few KiB; anything larger is malformed or
// hostile.
const maxImageManifestBytes = 1 << 20

// maxImageArchiveLayers, maxImageLayerRecords, and
// maxImageArchiveRelevantRecords bound the state retained while replaying an
// untrusted image archive. Real images have far fewer layers and relevant path
// entries; a higher count fails the inspection closed.
const (
	maxImageArchiveLayers          = 4096
	maxImageLayerRecords           = 4096
	maxImageArchiveRelevantRecords = 65536
)

// errImageLayerTooManyRecords fails closed when a layer repeats relevant paths
// beyond the inspection bound. Real layers have only a handful.
var errImageLayerTooManyRecords = fmt.Errorf(
	"image layer exceeds %d tmpfs-relevant entries", maxImageLayerRecords)

var errImageArchiveTooManyRecords = fmt.Errorf(
	"image archive exceeds %d tmpfs-relevant entries",
	maxImageArchiveRelevantRecords)

// layerRecordLimitReached reports whether one layer's retained records have hit
// the inspection bound.
func layerRecordLimitReached(scan *imageLayerScan) bool {
	return len(scan.entries)+len(scan.removals) >= maxImageLayerRecords
}

// imageFSEntryKind is the only filesystem information Relay needs about the
// components leading to a service tmpfs target.
type imageFSEntryKind uint8

const (
	imageFSDir imageFSEntryKind = iota + 1
	imageFSSymlink
	imageFSOther
)

// imageLayerEntry is one relevant path set by a layer.
type imageLayerEntry struct {
	path string
	kind imageFSEntryKind
}

// imageLayerRemoval is an OCI whiteout that removes lower-layer paths.
type imageLayerRemoval struct {
	path   string
	opaque bool
}

// imageLayerScan records the relevant mutations of one layer, before the
// archive's manifest (which may appear after the layers) establishes layer
// order.
type imageLayerScan struct {
	entries  []imageLayerEntry
	removals []imageLayerRemoval
	err      error
}

// imageSaveManifestEntry is the subset of Docker's manifest.json Relay needs.
type imageSaveManifestEntry struct {
	Config string   `json:"Config"`
	Layers []string `json:"Layers"`
}

// validateImageTmpfsTargets proves, using the exact immutable image content ID
// and BEFORE the service ContainerCreate, that every configured tmpfs target
// and each parent component is not a symlink (and every existing component is a
// directory). Docker follows symlinks in the container rootfs when applying
// HostConfig.Tmpfs; without this check an image containing
// `/var/cache -> /etc` could turn a configured tmpfs at `/var/cache` into a
// writable mount over `/etc`, defeating ReadonlyRootfs.
//
// The image filesystem is inspected from its Docker save archive. No image code
// is executed: Relay only reads layer tar headers and rejects any symlink or
// non-directory component on the path. Missing components are allowed (the
// daemon may create the mountpoint), but every component that exists must be a
// directory, so no existing path redirection can be followed. An archive that
// cannot be read or decoded fails closed.
func validateImageTmpfsTargets(
	ctx context.Context,
	cli *client.Client,
	imageID string,
	destinations []string,
) error {
	if len(destinations) == 0 {
		return nil
	}
	if strings.TrimSpace(imageID) == "" {
		return errors.New("tmpfs validation: immutable image ID is missing")
	}
	relevant := tmpfsRelevantPaths(destinations)

	inspectCtx, cancel := context.WithTimeout(
		ctx, serviceTmpfsImageInspectTimeout)
	defer cancel()
	saved, err := cli.ImageSave(inspectCtx, []string{imageID})
	if err != nil {
		return fmt.Errorf("inspect image filesystem %q: %w", imageID, err)
	}
	defer saved.Close()
	state, err := readImageFilesystem(inspectCtx, saved, relevant)
	if err != nil {
		return fmt.Errorf("inspect image filesystem %q: %w", imageID, err)
	}
	for _, destination := range destinations {
		for _, component := range tmpfsPathAndAncestors(destination) {
			kind, ok := state[component]
			if !ok {
				// Missing components are safe: they cannot redirect anything,
				// and the daemon creates the mountpoint beneath checked parents.
				continue
			}
			display := "/" + component
			switch kind {
			case imageFSSymlink:
				return fmt.Errorf(
					"tmpfs destination %q is redirected through symlink %q "+
						"in image %q",
					destination, display, imageID)
			case imageFSDir:
			default:
				return fmt.Errorf(
					"tmpfs destination %q has non-directory component %q "+
						"in image %q",
					destination, display, imageID)
			}
		}
	}
	return nil
}

// tmpfsImageValidationKey identifies one reusable image-filesystem validation.
// Both fields are required: the same image validated for one destination set
// says nothing about another set, and the same set against different image
// content says nothing about that content. targets is a NUL-joined, sorted copy
// of the destinations; a NUL cannot occur in a container path, so components
// cannot run together across the join.
type tmpfsImageValidationKey struct {
	imageID string
	targets string
}

// tmpfsImageValidation is one in-flight or completed validation. done is closed
// after err is set. A successful validation stays in the cache; a failed one is
// removed so the next request re-inspects. Waiters already holding a pointer to
// a removed entry still observe its result.
type tmpfsImageValidation struct {
	done chan struct{}
	err  error
}

// imageTmpfsValidationCache coalesces concurrent validations of the same
// (immutable image ID, destination set) and reuses successful outcomes for the
// life of the Manager. It is deliberately in-memory only: an image ID is
// content-addressed, so an entry can never describe different bytes, and a
// worker restart simply validates again once.
type imageTmpfsValidationCache struct {
	mu      sync.Mutex
	entries map[tmpfsImageValidationKey]*tmpfsImageValidation
}

// validate runs fn unless a successful validation for key is already known, and
// coalesces concurrent callers onto one invocation. fn runs on the leader's
// context; a waiter honors its own cancellation. A failure is reported to every
// caller coalesced onto it and is then forgotten, never reused.
func (c *imageTmpfsValidationCache) validate(
	ctx context.Context,
	key tmpfsImageValidationKey,
	fn func(context.Context) error,
) error {
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		c.mu.Unlock()
		select {
		case <-entry.done:
			return entry.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	entry := &tmpfsImageValidation{done: make(chan struct{})}
	if c.entries == nil {
		c.entries = make(map[tmpfsImageValidationKey]*tmpfsImageValidation)
	}
	c.entries[key] = entry
	c.mu.Unlock()

	err := fn(ctx)

	c.mu.Lock()
	entry.err = err
	if err != nil {
		// A failure (including a cancelled inspection) must never be reused:
		// the next request has to retry against the daemon.
		delete(c.entries, key)
	}
	close(entry.done)
	c.mu.Unlock()
	return err
}

// validateImageTmpfsTargetsCached is the Manager-scoped form of
// validateImageTmpfsTargets used by service creates. Successful validations are
// reused per immutable image content ID and exact destination set, and
// concurrent requests for the same pair coalesce onto one archive read, so
// repeated replicas do not each stream the same image. Failures are never
// reused, and a missing immutable ID still fails closed before any cache
// lookup.
func (m *Manager) validateImageTmpfsTargetsCached(
	ctx context.Context,
	imageID string,
	destinations []string,
) error {
	if len(destinations) == 0 {
		return nil
	}
	if strings.TrimSpace(imageID) == "" {
		return errors.New("tmpfs validation: immutable image ID is missing")
	}
	key := tmpfsImageValidationKey{
		imageID: imageID,
		targets: tmpfsTargetsKey(destinations),
	}
	return m.tmpfsChecks.validate(
		ctx, key, func(validateCtx context.Context) error {
			return validateImageTmpfsTargets(
				validateCtx, m.cli, imageID, destinations)
		})
}

// tmpfsTargetsKey renders the exact destination set as a cache-key component.
// Sorting removes declaration-order sensitivity; NUL joining keeps ["/a",
// "/b"] distinct from ["/a/b"]. It does not deduplicate: an invalid duplicate
// is rejected before this point, and preserving it means the key never hides a
// caller's input.
func tmpfsTargetsKey(destinations []string) string {
	sorted := append([]string(nil), destinations...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

// tmpfsRelevantPaths returns the container path components (without leading
// slash) whose final image filesystem type can affect a configured tmpfs
// target: every target and each of its ancestors.
func tmpfsRelevantPaths(destinations []string) map[string]struct{} {
	relevant := make(map[string]struct{}, len(destinations)*4)
	for _, destination := range destinations {
		for _, component := range tmpfsPathAndAncestors(destination) {
			relevant[component] = struct{}{}
		}
	}
	return relevant
}

// tmpfsPathAndAncestors returns the slash-separated components of a canonical
// absolute container path, from the first component through the target itself.
// The root component is omitted because it is always a directory.
func tmpfsPathAndAncestors(containerPath string) []string {
	trimmed := strings.TrimPrefix(path.Clean(containerPath), "/")
	if trimmed == "" || trimmed == "." {
		return nil
	}
	parts := strings.Split(trimmed, "/")
	out := make([]string, 0, len(parts))
	current := ""
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if current == "" {
			current = part
		} else {
			current += "/" + part
		}
		out = append(out, current)
	}
	return out
}

// readImageFilesystem reads a Docker save archive and returns the FINAL type of
// every relevant path. Docker's save format is either a legacy Docker archive
// (`<id>/layer.tar`) or, with the containerd image store, an OCI layout
// (`blobs/sha256/...`); both carry a Docker-style manifest.json listing layer
// order. Layers may be uncompressed or gzip-compressed.
//
// The manifest can appear after the layers, so per-layer relevant mutations are
// collected by outer archive name and replayed in manifest order. Only relevant
// paths are retained, and archive file contents are never written or executed.
func readImageFilesystem(
	ctx context.Context,
	archive io.Reader,
	relevant map[string]struct{},
) (map[string]imageFSEntryKind, error) {
	tr := tar.NewReader(archive)
	scans := make(map[string]*imageLayerScan)
	archiveRecords := 0
	var manifestJSON []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read image archive: %w", err)
		}
		name, err := cleanArchiveEntryName(hdr.Name)
		if err != nil {
			return nil, err
		}
		if name == "manifest.json" {
			data, err := readBoundedManifest(tr)
			if err != nil {
				return nil, err
			}
			manifestJSON = data
			continue
		}
		if !isImageLayerCandidate(name) {
			continue
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}
		if _, seen := scans[name]; !seen &&
			len(scans) >= maxImageArchiveLayers {
			return nil, fmt.Errorf(
				"image archive exceeds %d layers",
				maxImageArchiveLayers)
		}
		scan := &imageLayerScan{}
		scan.err = scanImageLayer(ctx, tr, relevant, scan, &archiveRecords)
		if errors.Is(scan.err, errImageArchiveTooManyRecords) {
			return nil, scan.err
		}
		scans[name] = scan
	}
	if len(manifestJSON) == 0 {
		return nil, errors.New("image archive has no manifest.json")
	}
	var manifest []imageSaveManifestEntry
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		return nil, fmt.Errorf("decode image manifest: %w", err)
	}
	if len(manifest) != 1 {
		return nil, fmt.Errorf(
			"image archive describes %d images, want exactly one",
			len(manifest))
	}
	if len(manifest[0].Layers) == 0 {
		return nil, errors.New("image archive declares no layers")
	}

	state := make(map[string]imageFSEntryKind)
	for _, layerName := range manifest[0].Layers {
		name, err := cleanArchiveEntryName(layerName)
		if err != nil {
			return nil, err
		}
		scan, ok := scans[name]
		if !ok {
			return nil, fmt.Errorf("image archive is missing layer %q", name)
		}
		if scan.err != nil {
			return nil, fmt.Errorf("inspect image layer %q: %w", name, scan.err)
		}
		applyImageLayer(state, scan)
	}
	return state, nil
}

// readBoundedManifest reads manifest.json without allowing an unbounded
// in-memory allocation.
func readBoundedManifest(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxImageManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read image manifest: %w", err)
	}
	if len(data) > maxImageManifestBytes {
		return nil, fmt.Errorf(
			"image manifest exceeds %d bytes", maxImageManifestBytes)
	}
	return data, nil
}

// cleanArchiveEntryName normalizes an archive entry name and rejects absolute
// paths and any parent component. Image archives are untrusted input; a `..`
// component is rejected rather than normalized so parser and daemon semantics
// can never disagree about which filesystem path an entry names.
func cleanArchiveEntryName(name string) (string, error) {
	trimmed := strings.TrimPrefix(name, "./")
	if path.IsAbs(trimmed) ||
		strings.Contains("/"+trimmed+"/", "/../") {
		return "", fmt.Errorf("image archive contains unsafe path %q", name)
	}
	clean := path.Clean(trimmed)
	if clean == "." || clean == "" {
		return "", nil
	}
	return clean, nil
}

// isImageLayerCandidate identifies outer archive entries that can carry a layer
// tarball. OCI layouts keep every blob under blobs/, including the (non-layer)
// config blob; a scan error there is ignored because the manifest does not list
// it as a layer. Legacy Docker archives name each layer .../layer.tar.
func isImageLayerCandidate(name string) bool {
	return strings.HasPrefix(name, "blobs/") ||
		name == "layer.tar" || strings.HasSuffix(name, "/layer.tar")
}

// scanImageLayer collects relevant entries and whiteouts from one layer tar.
// File contents are skipped by tar.Reader; only headers matter.
func scanImageLayer(
	ctx context.Context,
	content io.Reader,
	relevant map[string]struct{},
	scan *imageLayerScan,
	archiveRecords *int,
) error {
	br := bufio.NewReaderSize(content, 32*1024)
	magic, err := br.Peek(2)
	if err != nil {
		return fmt.Errorf("read layer header: %w", err)
	}
	var layer io.Reader = br
	if magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return fmt.Errorf("open gzip layer: %w", err)
		}
		defer gz.Close()
		layer = gz
	}
	tr := tar.NewReader(layer)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name, err := cleanArchiveEntryName(hdr.Name)
		if err != nil {
			return err
		}
		if name == "" {
			continue
		}
		dir, base := path.Split(name)
		if strings.HasPrefix(base, ".wh.") {
			if base == ".wh..wh..opq" {
				opaqueDir := strings.TrimSuffix(dir, "/")
				if _, ok := relevant[opaqueDir]; ok {
					if layerRecordLimitReached(scan) {
						return errImageLayerTooManyRecords
					}
					if *archiveRecords >= maxImageArchiveRelevantRecords {
						return errImageArchiveTooManyRecords
					}
					scan.removals = append(scan.removals,
						imageLayerRemoval{path: opaqueDir, opaque: true})
					(*archiveRecords)++
				}
				continue
			}
			removed := path.Join(
				dir, strings.TrimPrefix(base, ".wh."))
			if _, ok := relevant[removed]; ok {
				if layerRecordLimitReached(scan) {
					return errImageLayerTooManyRecords
				}
				if *archiveRecords >= maxImageArchiveRelevantRecords {
					return errImageArchiveTooManyRecords
				}
				scan.removals = append(scan.removals,
					imageLayerRemoval{path: removed})
				(*archiveRecords)++
			}
			continue
		}
		if _, ok := relevant[name]; !ok {
			continue
		}
		if layerRecordLimitReached(scan) {
			return errImageLayerTooManyRecords
		}
		if *archiveRecords >= maxImageArchiveRelevantRecords {
			return errImageArchiveTooManyRecords
		}
		scan.entries = append(scan.entries,
			imageLayerEntry{path: name, kind: imageEntryKind(hdr)})
		(*archiveRecords)++
	}
}

// imageEntryKind classifies a tar header for the only distinction Relay needs:
// a directory, a symlink, or anything else (which cannot be a mountpoint
// ancestor).
func imageEntryKind(hdr *tar.Header) imageFSEntryKind {
	switch hdr.Typeflag {
	case tar.TypeDir:
		return imageFSDir
	case tar.TypeSymlink:
		return imageFSSymlink
	default:
		return imageFSOther
	}
}

// applyImageLayer replays one layer's relevant whiteouts and entries. Whiteouts
// apply to lower layers only, so they are processed before the layer's own
// entries; a layer may delete a path and provide its replacement.
func applyImageLayer(state map[string]imageFSEntryKind, scan *imageLayerScan) {
	for _, removal := range scan.removals {
		prefix := removal.path + "/"
		for current := range state {
			if current == removal.path {
				// An opaque whiteout keeps the directory itself; a named
				// whiteout removes the path and everything beneath it.
				if !removal.opaque {
					delete(state, current)
				}
				continue
			}
			if strings.HasPrefix(current, prefix) {
				delete(state, current)
			}
		}
	}
	for _, entry := range scan.entries {
		state[entry.path] = entry.kind
	}
}
