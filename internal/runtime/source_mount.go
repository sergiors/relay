package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
)

// SourceMount describes a read-only bind mount of an app's live source tree into
// its execution and entrypoint-service containers, used when SOURCE_MOUNT is
// enabled and the runtime's dependency layout permits it (see
// plan.Spec.MountableSource). The source is mounted from the app's directory on
// the Docker daemon's host filesystem, so the daemon — not the Relay process —
// resolves HostPath. Native-host Relay passes fn.Dir, which is correct when the
// daemon sees the same /apps tree; a containerized Relay whose /apps is a host
// bind mount (the bundled Compose layout) instead passes the inspected mount's
// daemon-visible Source plus the relative path, so the daemon resolves the same
// live tree (see daemonSourcePath).
//
// Identity is the source content fingerprint the mount corresponds to. It is
// NOT part of the Docker mount itself; it is the generation key the reconciler
// stamps on service containers (relay.source) and the execution warm pool uses
// (via Prepared.Fingerprint) so a source edit recycles its containers even
// though the (source-independent) image tag does not change.
type SourceMount struct {
	// HostPath is the app directory as the Docker daemon sees it. Native-host
	// Relay passes fn.Dir (e.g. /apps/foo). A containerized Relay whose app tree
	// is a host bind mount passes the inspected mount's host Source plus the
	// relative path (see daemonSourcePath).
	HostPath string
	// Target is the in-container mount point: the runtime's source root. It is
	// the engine's workdir for a runtime whose dependencies live outside it
	// (Python, /app); for a runtime that keeps dependencies or generated files
	// under the workdir it is a DISTINCT subdirectory (Node, /app/src) so the
	// mount never hides /app/node_modules and the persisted esbuild.
	Target string
	// WorkDir, when non-empty, is the in-container working directory the
	// container runs with, so process.cwd() and relative filesystem operations
	// see the app's source root. It is empty when the source root is the image
	// workdir (Python, and every unmounted container), preserving the historical
	// working directory exactly.
	WorkDir string
	// Masks are in-container paths inside Target masked with an empty, read-only
	// filesystem at create time, so a whole-directory bind cannot shadow a path
	// the image owns (Node masks Target/node_modules so host modules never
	// shadow the dependency image's /app/node_modules).
	Masks []string
	// Identity is the source fingerprint the mount corresponds to.
	Identity string
}

// bindMounts converts a SourceMount into the Docker mount list applied at
// container create. It returns nil for a nil/zero mount, so every unmounted
// container is byte-for-byte the pre-SOURCE_MOUNT behavior. The bind is always
// read-only: Relay never lets a container write back into /apps.
//
// Each mask becomes an additional empty, read-only tmpfs at its in-container
// path. The mask is nested inside the read-only source bind (Docker applies the
// shorter bind target first), so an empty directory is visible where a host
// directory of the same name would otherwise shadow the image's own path. The
// tmpfs is ReadOnly (not merely mode-restricted), so it adds no writable surface
// even to root.
func bindMounts(sm *SourceMount) []mount.Mount {
	if sm == nil || sm.HostPath == "" || sm.Target == "" {
		return nil
	}
	mounts := []mount.Mount{{
		Type:     mount.TypeBind,
		Source:   sm.HostPath,
		Target:   sm.Target,
		ReadOnly: true,
	}}
	for _, mask := range sm.Masks {
		if mask == "" {
			continue
		}
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeTmpfs,
			Target:   mask,
			ReadOnly: true,
			TmpfsOptions: &mount.TmpfsOptions{
				Mode: 0o555,
			},
		})
	}
	return mounts
}

// sourceMountImageFingerprint derives the app image identity when the source is
// NOT baked into the image (SOURCE_MOUNT). It hashes exactly the non-source
// build inputs — the runtime name, the dependency reference the image is built
// FROM, and the effective Dockerfile rendered with the source COPY omitted — so
// a dependency, runtime/tool, bootstrap, user, or entrypoint change produces a
// new tag, while a source-only change does not (the live source is mounted and
// the reconciler advances the warm/service generation instead).
func sourceMountImageFingerprint(runtimeName, dependency, dockerfile string) string {
	h := sha256.New()
	for _, field := range []string{"relay-source-mount", runtimeName, dependency, dockerfile} {
		io.WriteString(h, field)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// setSourceMount records the live source mount for an app. It is published by a
// successful Prepare and read at container create (execution) and by
// ResolveServiceImage (entrypoint services). It is safe for concurrent use.
func (m *Manager) setSourceMount(name string, sm SourceMount) {
	if name == "" {
		return
	}
	m.mountMu.Lock()
	defer m.mountMu.Unlock()
	if m.mounts == nil {
		m.mounts = map[string]SourceMount{}
	}
	m.mounts[name] = sm
}

// clearSourceMount drops an app's recorded source mount, so an app that stops
// being mountable (a runtime change, or SOURCE_MOUNT disabled across a restart)
// never has a stale mount applied.
func (m *Manager) clearSourceMount(name string) {
	if name == "" {
		return
	}
	m.mountMu.Lock()
	defer m.mountMu.Unlock()
	delete(m.mounts, name)
}

// sourceMountFor returns an app's recorded source mount, or false when the app
// is not being source-mounted.
func (m *Manager) sourceMountFor(name string) (SourceMount, bool) {
	m.mountMu.RLock()
	defer m.mountMu.RUnlock()
	sm, ok := m.mounts[name]
	return sm, ok
}

// executionSourceMount returns the SOURCE_MOUNT container configuration for an
// app's execution containers: the working directory (empty preserves the image
// workdir) and the bind/mask mounts. Both are nil/empty when the app has no
// recorded source mount, which is the exact pre-SOURCE_MOUNT create shape.
func (m *Manager) executionSourceMount(fnName string) (string, []mount.Mount) {
	sm, ok := m.sourceMountFor(fnName)
	if !ok {
		return "", nil
	}
	return sm.WorkDir, bindMounts(&sm)
}

// containerMounts caches the one-time resolution of Relay's OWN container's
// mounts. A running container's mounts never change, so a successful resolution
// is cached for the Manager's lifetime; a failure is deliberately NOT cached so
// a transient daemon error can be retried by a later call.
type containerMounts struct {
	mu       sync.Mutex
	mounts   []container.MountPoint
	resolved bool
}

// daemonSourcePath returns the path to hand the Docker daemon to bind the app
// directory appDir. When Relay itself is a container on the same daemon and
// appDir lies under one of Relay's own bind mounts (the bundled Compose mounts
// the host's app tree at /apps), it returns that mount's daemon-visible Source
// joined with the relative path. Otherwise — native-host Relay, an unrelated
// container, a custom hostname, or any resolution failure — it returns appDir
// unchanged, preserving the historical behavior. Docker's own source existence
// validation remains the final authority in every case.
func (m *Manager) daemonSourcePath(ctx context.Context, appDir string) string {
	mounts := m.daemonMounts(ctx)
	if len(mounts) == 0 {
		return appDir
	}
	return mapToDaemonPath(appDir, mounts)
}

// daemonMounts inspects Relay's own container (identified by the worker
// hostname) and returns its mounts, or nil when Relay is not a container, is not
// reliably identifiable, or the daemon cannot be consulted. It is conservative:
// any uncertainty yields nil so the caller keeps fn.Dir. A successful resolution
// is cached; a failure is not.
func (m *Manager) daemonMounts(ctx context.Context) []container.MountPoint {
	if m == nil || m.cli == nil || m.hostname == "" {
		return nil
	}
	detect := m.containerDetect
	if detect == nil {
		detect = func() bool { return isContainerIDLike(m.hostname) }
	}
	if !detect() {
		return nil
	}
	m.selfMounts.mu.Lock()
	defer m.selfMounts.mu.Unlock()
	if m.selfMounts.resolved {
		return m.selfMounts.mounts
	}
	insp, err := m.cli.ContainerInspect(ctx, m.hostname, client.ContainerInspectOptions{})
	if err != nil {
		// Not a resolvable self container (e.g. the hostname is not a container
		// ID/name) or a transient daemon failure. Fall back WITHOUT caching so a
		// later call can still map.
		m.Logger().Debug("Runtime: cannot inspect Relay's own container; using the app dir as the bind source",
			"error", err)
		return nil
	}
	if !isSelfContainer(insp.Container, m.hostname) {
		m.Logger().Debug("Runtime: inspected container does not match this Relay; using the app dir as the bind source")
		return nil
	}
	m.selfMounts.mounts = insp.Container.Mounts
	m.selfMounts.resolved = true
	// Log only the count: the individual source paths can be high-cardinality
	// and must never be logged.
	m.Logger().Debug("Runtime: resolved Relay's own container mounts for SOURCE_MOUNT",
		"mounts", len(m.selfMounts.mounts))
	return m.selfMounts.mounts
}

// isContainerIDLike reports whether s is the default Docker container hostname:
// the short (12-character) or full hex container ID Docker assigns when no
// explicit `hostname:` is configured. It is the reliable signal that Relay's OS
// hostname (config.ConsumerName) is its own container ID, so inspecting by it
// identifies THIS container. A normal hostname, or a custom `hostname:`, is not
// ID-like and leaves the mapping off (falling back to fn.Dir).
func isContainerIDLike(s string) bool {
	if len(s) < 12 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// isSelfContainer reports whether an inspected container is the Relay container
// this process runs in. Relay's identity is its hostname (config.ConsumerName,
// i.e. os.Hostname): the inspected container must declare that same hostname,
// and — because Docker resolves a name or ID prefix — when the hostname is the
// default container-ID form it must also prefix the inspected container's full
// ID. Together these reject an unrelated container that merely happens to be
// resolvable by the hostname string.
func isSelfContainer(c container.InspectResponse, hostname string) bool {
	if hostname == "" || c.Config == nil || c.Config.Hostname != hostname {
		return false
	}
	if isContainerIDLike(hostname) {
		return strings.HasPrefix(c.ID, hostname)
	}
	return true
}

// mapToDaemonPath maps appDir (a path in Relay's own filesystem) to the path the
// Docker daemon sees, using the mounts inspected from Relay's own container.
// Only bind mounts with a non-empty Source are considered; the most-specific
// (longest) matching Destination wins, so a nested bind is never shadowed by its
// parent. appDir is returned unchanged when no mount covers it. Paths are
// compared with slash semantics because container paths are always
// slash-separated regardless of the OS Relay is built for.
func mapToDaemonPath(appDir string, mounts []container.MountPoint) string {
	clean := path.Clean(appDir)
	bestDest := ""
	bestPath := ""
	for _, mp := range mounts {
		if mp.Type != mount.TypeBind || mp.Source == "" || mp.Destination == "" {
			continue
		}
		dest := path.Clean(mp.Destination)
		rel, ok := relativeTo(clean, dest)
		if !ok {
			continue
		}
		if len(dest) > len(bestDest) {
			bestDest = dest
			bestPath = path.Join(mp.Source, rel)
		}
	}
	if bestDest == "" {
		return appDir
	}
	return bestPath
}

// relativeTo reports whether p is dir or lies under dir, returning the
// slash-relative path from dir to p ("." when p == dir). Comparison is by path
// boundary, so /apps2 is never treated as under /apps.
func relativeTo(p, dir string) (string, bool) {
	switch {
	case p == dir:
		return ".", true
	case dir == "/":
		return strings.TrimPrefix(p, "/"), true
	case strings.HasPrefix(p, dir+"/"):
		return p[len(dir)+1:], true
	default:
		return "", false
	}
}
