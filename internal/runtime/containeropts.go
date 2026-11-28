package runtime

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"relay/internal/app"
)

// ptr returns a pointer to v. It is a tiny helper for the pointer-typed fields
// in the container HostConfig (e.g. PidsLimit) so the hardening values read as
// literals rather than requiring a local variable.
func ptr[T any](v T) *T { return &v }

// resourceHostConfig maps an app's EFFECTIVE resource limits onto Docker's
// HostConfig resource fields. It is the single conversion point for every Relay
// container kind (event/schedule/manual invocation containers and both service
// source kinds), so the Docker field mapping (bytes → Memory, cores → NanoCPUs,
// count → PidsLimit) lives in exactly one place. The limits are per container.
//
// A zero/partial ResourceLimits is normalized to the package defaults via
// OrDefault before mapping, so a hand-built value can never produce a Docker
// HostConfig with a zero limit (which would mean "unlimited" to the daemon).
func resourceHostConfig(limits app.ResourceLimits) container.Resources {
	limits = limits.OrDefault()
	return container.Resources{
		Memory:    limits.MemoryBytes,
		NanoCPUs:  limits.NanoCPUs,
		PidsLimit: ptr(limits.PidsLimit),
	}
}

// noNewPrivileges is the Docker security option that prevents a process from
// gaining privileges through setuid/setgid binaries or file capabilities. It is
// part of the shared baseline; no other SecurityOpt is set, so Docker's default
// seccomp profile remains active.
const noNewPrivileges = "no-new-privileges:true"

// lowPortCapability is the single capability Relay may add to a service
// container, and only when the service declares a privileged port. Docker's
// default unprivileged-port floor is usually 0, but a daemon can configure a
// higher floor; granting exactly NET_BIND_SERVICE keeps low-port services
// working without reopening any other capability.
const lowPortCapability = "NET_BIND_SERVICE"

// hardenedHostConfig returns the shared security/resource baseline HostConfig
// every Relay container is created with: all capabilities dropped, the resolved
// memory/CPU/pids limits, a read-only rootfs, a disk-backed anonymous volume at
// /tmp, and no-new-privileges while leaving Docker's default seccomp profile in
// place. The security baseline is internal and not configuration; the resource
// limits are per-app configuration mapped by resourceHostConfig. Networking is
// left enabled (outbound access is a legitimate app need) and PID/IPC/network
// namespaces stay private because the corresponding HostConfig modes are unset
// (the zero value); Relay never selects host or container namespace sharing.
// autoRemove is true for one-shot invocation containers (the daemon removes them
// and their anonymous volumes the moment they exit) and false for persistent
// service containers (the reconciler owns their removal). A persistent service
// may add configured anonymous volumes on top of the baseline /tmp (see
// applyServiceEphemeral); execution containers never do.
func hardenedHostConfig(autoRemove bool, limits app.ResourceLimits) *container.HostConfig {
	return &container.HostConfig{
		AutoRemove:     autoRemove,
		Resources:      resourceHostConfig(limits),
		CapDrop:        []string{"ALL"},
		SecurityOpt:    []string{noNewPrivileges},
		ReadonlyRootfs: true,
		Mounts:         []mount.Mount{ephemeralVolumeMount(tmpVolumeTarget)},
	}
}

// hardenedServiceHostConfig is the persistent-service variant of the shared
// baseline. It differs only in AutoRemove (false: the reconciler owns removal)
// and, for a service that declares a privileged TCP port, a single narrowly
// scoped NET_BIND_SERVICE capability addition on top of CapDrop ALL. No other
// capability is ever granted.
func hardenedServiceHostConfig(limits app.ResourceLimits, port int) *container.HostConfig {
	hc := hardenedHostConfig(false, limits)
	if port > 0 && port < 1024 {
		hc.CapAdd = []string{lowPortCapability}
	}
	return hc
}

// tmpVolumeTarget is Relay's always-present writable path. It is a Docker
// anonymous volume (disk-backed, initialized from the image) rather than a
// tmpfs, so /tmp does not consume the container's memory limit and anonymous
// volume removal can reclaim it with the container.
const tmpVolumeTarget = "/tmp"

// ephemeralVolumeMount builds the only writable mount shape Relay emits: an
// anonymous Docker volume at an absolute container path. No Source, driver, or
// VolumeOptions are set, so the daemon creates a fresh anonymous volume for each
// container and copies the image directory's mode and ownership into it. That
// copy is what makes a pre-existing image directory (for example /tmp with mode
// 1777) writable by the image's non-root user.
func ephemeralVolumeMount(target string) mount.Mount {
	return mount.Mount{Type: mount.TypeVolume, Target: target}
}

// applyServiceEphemeral validates a service's configured additional ephemeral
// destinations and appends one anonymous volume mount per destination to
// hc.Mounts. It is the only path that adds service ephemeral volumes; invocation
// containers never call it, so they keep their /tmp-only baseline. Validation
// uses the same app.ValidateServiceEphemeral policy as template parsing, so a
// direct ServiceSpec caller cannot bypass the path rules.
func applyServiceEphemeral(
	hc *container.HostConfig,
	destinations []string,
) error {
	if len(destinations) == 0 {
		return nil
	}
	if err := app.ValidateServiceEphemeral(destinations); err != nil {
		return fmt.Errorf("additional ephemeral volume: %w", err)
	}
	for _, destination := range destinations {
		hc.Mounts = append(hc.Mounts, ephemeralVolumeMount(destination))
	}
	return nil
}

// validateContainerHostConfig rejects a container configuration that departs
// from Relay's hardening contract BEFORE Docker ContainerCreate. The shared
// builders already produce a safe HostConfig; this check is the enforcement
// point for every create path, so a future caller cannot accidentally introduce
// a privileged container, host namespace sharing, host networking, writable
// rootfs, an over-broad capability, an unexpected security option, a device, an
// unsafe mount, a non-anonymous or unsafe volume, or a mount layout that would
// shadow the HOT_RELOAD source bind or its masks.
//
// mounts are the mount.Mount entries a caller intends to apply (the shared
// source-mount read-only bind plus its read-only tmpfs masks). The HostConfig's
// own Mounts are validated too, so neither list can bypass the check.
//
// declaredPort is the service's configured port (0 for one-shot invocation
// containers). NET_BIND_SERVICE is accepted only when a service declares a
// privileged port, so an invocation container can never gain a capability.
//
// sourceRoot is the app's TRUSTED live-source host directory, resolved by Relay
// when it recorded the app's SourceMount ("" when the workload has no source
// mount). A read-only bind is accepted only when it maps that provenance or a
// path beneath it, so an arbitrary host directory cannot be mounted. Basing
// the check on the recorded source root, rather than a fixed /apps prefix,
// keeps the bundled Docker-named-volume mapping working.
func validateContainerHostConfig(
	hc *container.HostConfig,
	mounts []mount.Mount,
	declaredPort int,
	sourceRoot string,
) error {
	if hc == nil {
		return errors.New("nil host config")
	}
	if hc.Privileged {
		return errors.New("privileged mode is not allowed")
	}
	if hc.PidMode != "" && hc.PidMode != "private" {
		return fmt.Errorf("PID namespace mode %q is not allowed", hc.PidMode)
	}
	if hc.IpcMode != "" && hc.IpcMode != "private" {
		return fmt.Errorf("IPC namespace mode %q is not allowed", hc.IpcMode)
	}
	switch hc.NetworkMode {
	case "", "default", "bridge":
	default:
		return fmt.Errorf("network mode %q is not allowed", hc.NetworkMode)
	}
	if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
		return fmt.Errorf(
			"dropping ALL capabilities is required (got %v)", hc.CapDrop)
	}
	if len(hc.CapAdd) > 0 {
		if len(hc.CapAdd) != 1 || hc.CapAdd[0] != lowPortCapability {
			return fmt.Errorf("capabilities %v may not be added", hc.CapAdd)
		}
		if declaredPort < 1 || declaredPort >= 1024 {
			return fmt.Errorf(
				"capability %q requires a service with a declared port below 1024 (got %d)",
				lowPortCapability, declaredPort)
		}
	}
	if len(hc.Binds) > 0 {
		return fmt.Errorf("host bind configuration is not allowed (%v)", hc.Binds)
	}
	if len(hc.VolumesFrom) > 0 {
		return fmt.Errorf("volumes-from is not allowed (%v)", hc.VolumesFrom)
	}
	if len(hc.Devices) > 0 || len(hc.DeviceRequests) > 0 {
		return errors.New("device access is not allowed")
	}
	if !hc.ReadonlyRootfs {
		return errors.New("read-only root filesystem is required")
	}
	if !hasSecurityOpt(hc.SecurityOpt, noNewPrivileges) {
		return fmt.Errorf("security option %q is required", noNewPrivileges)
	}
	for _, opt := range hc.SecurityOpt {
		if opt != noNewPrivileges {
			return fmt.Errorf("security option %q may not override the default profile", opt)
		}
	}
	allMounts := append(append([]mount.Mount{}, hc.Mounts...), mounts...)
	if err := validateHostMounts(hc.Tmpfs, allMounts); err != nil {
		return err
	}
	return validateWorkloadMounts(allMounts, sourceRoot)
}

// validateHostMounts validates the HostConfig's final mount set against Relay's
// contract before ContainerCreate. HostConfig.Tmpfs must be empty: Relay's only
// tmpfs mounts are the internal read-only HOT_RELOAD masks, which arrive as
// mount.Mount entries so their ReadOnly flag is enforced. The always-present
// /tmp anonymous volume must be present exactly once, and no volume destination
// may overlap (equal or ancestor/descendant) any other mount target — in
// particular the HOT_RELOAD source bind and its read-only mask tmpfs. This is
// the runtime enforcement point that a direct ServiceSpec caller cannot bypass.
func validateHostMounts(tmpfs map[string]string, mounts []mount.Mount) error {
	if len(tmpfs) != 0 {
		return fmt.Errorf(
			"HostConfig.Tmpfs is not allowed (%v); writable paths must be anonymous volumes",
			tmpfs)
	}
	tmpVolumes := 0
	for i, m := range mounts {
		if m.Type != mount.TypeVolume {
			continue
		}
		if m.Target == tmpVolumeTarget {
			tmpVolumes++
		}
		for j, other := range mounts {
			if i == j || other.Target == "" {
				continue
			}
			if pathsOverlap(m.Target, other.Target) {
				return fmt.Errorf(
					"volume %q conflicts with mounted path %q",
					m.Target, other.Target)
			}
		}
	}
	if tmpVolumes != 1 {
		return fmt.Errorf(
			"the /tmp anonymous volume must be configured exactly once (got %d)",
			tmpVolumes)
	}
	return nil
}

// pathsOverlap reports whether two canonical container paths are equal or one
// is an ancestor of the other.
func pathsOverlap(a, b string) bool {
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// validateImageDeclaredVolumes enforces Relay's writable-path contract for an
// image's Docker VOLUME destinations, BEFORE Docker ContainerCreate. An image
// VOLUME is materialized by the daemon as an implicit anonymous writable mount
// even when HostConfig.ReadonlyRootfs is true, so it would otherwise bypass the
// read-only filesystem. Both production workload create paths (execution
// containers and persistent services) call this with the exact image they are
// about to create from.
//
// coveredVolumes is the set of container targets Relay explicitly mounts:
// Relay's always-present /tmp volume plus the service's validated `ephemeral`
// destinations. A declared volume is permitted ONLY when exactly one of those
// explicit mounts covers it, in which case the daemon mounts Relay's anonymous
// volume at that path instead of creating its own implicit one. Relay never
// auto-adds a mount for an image VOLUME: a destination that is not explicitly
// configured remains an error. Execution containers pass only /tmp, so every
// other declared volume is still rejected there.
//
// Fail closed: an image that cannot be inspected, has no Config, or has no
// immutable ID is an error, not a pass. A nonempty Config.Volumes with any
// uncovered destination is an error naming those destinations. Success returns
// the inspected image ID so the caller can create from exactly the content it
// validated, closing a mutable-reference inspect/create race. The error is
// wrapped by the caller with its own workload prefix.
func validateImageDeclaredVolumes(
	ctx context.Context,
	cli *client.Client,
	image string,
	coveredVolumes ...string,
) (string, error) {
	insp, err := cli.ImageInspect(ctx, image)
	if err != nil {
		return "", fmt.Errorf("inspect image %q: %w", image, err)
	}
	if insp.ID == "" {
		return "", fmt.Errorf("inspect image %q: immutable image ID is missing", image)
	}
	if insp.Config == nil {
		return "", fmt.Errorf("inspect image %q: image config is missing", image)
	}
	if len(insp.Config.Volumes) == 0 {
		return insp.ID, nil
	}
	covered := make(map[string]bool, len(coveredVolumes))
	for _, destination := range coveredVolumes {
		covered[destination] = true
	}
	var uncovered []string
	for destination := range insp.Config.Volumes {
		if !covered[destination] {
			uncovered = append(uncovered, destination)
		}
	}
	if len(uncovered) == 0 {
		return insp.ID, nil
	}
	slices.Sort(uncovered)
	return "", fmt.Errorf(
		"image %q declares writable volumes %v not explicitly mounted by Relay "+
			"(the /tmp volume or a configured service ephemeral destination), "+
			"which would bypass the read-only root filesystem",
		image, uncovered)
}

// validateRecursiveReadOnlyMountSupport prevents HOT_RELOAD from relying on a
// read-only bind whose nested mounts may remain writable. Docker's recursive
// read-only option is available from API 1.44; earlier API versions default
// read-only binds to non-recursive behavior and silently ignore the force flag.
func validateRecursiveReadOnlyMountSupport(cli *client.Client, mounts []mount.Mount) error {
	for _, m := range mounts {
		if m.Type != mount.TypeBind || !m.ReadOnly || m.BindOptions == nil || !m.BindOptions.ReadOnlyForceRecursive {
			continue
		}
		version := strings.TrimPrefix(cli.ClientVersion(), "v")
		majorMinor := strings.SplitN(version, ".", 3)
		if len(majorMinor) < 2 {
			return fmt.Errorf("recursive read-only bind mounts require Docker API 1.44 or newer (got %q)", version)
		}
		major, majorErr := strconv.Atoi(majorMinor[0])
		minor, minorErr := strconv.Atoi(majorMinor[1])
		if majorErr != nil || minorErr != nil || major < 1 || (major == 1 && minor < 44) {
			return fmt.Errorf("recursive read-only bind mounts require Docker API 1.44 or newer (got %q)", version)
		}
		return nil
	}
	return nil
}

// hasSecurityOpt reports whether opt is present in opts.
func hasSecurityOpt(opts []string, want string) bool {
	for _, opt := range opts {
		if opt == want {
			return true
		}
	}
	return false
}

// validateWorkloadMounts rejects any mount that is not one of the three safe
// shapes Relay emits: a read-only bind of app source, a read-only tmpfs mask, or
// an anonymous writable volume at Relay's /tmp or a validated service ephemeral
// destination. A writable bind, a bind outside the app's trusted source root, a
// Docker/container-runtime socket, a named volume, a volume driver/option, or any
// other mount type is refused before create. The check is deliberately explicit
// rather than relying on the absence of mount fields in the template schema: the
// runtime is the last boundary before the daemon.
func validateWorkloadMounts(mounts []mount.Mount, sourceRoot string) error {
	for _, m := range mounts {
		switch m.Type {
		case mount.TypeBind:
			if !m.ReadOnly {
				return fmt.Errorf("bind mount %q at %q must be read-only", m.Source, m.Target)
			}
			if m.BindOptions == nil || !m.BindOptions.ReadOnlyForceRecursive {
				return fmt.Errorf("bind mount %q at %q must be recursively read-only", m.Source, m.Target)
			}
			if m.Source == "" {
				return fmt.Errorf("bind mount at %q has no source", m.Target)
			}
			if isContainerRuntimeSocket(m.Source) {
				return fmt.Errorf("mounting container runtime socket %q is not allowed", m.Source)
			}
			if !isTrustedSourcePath(m.Source, sourceRoot) {
				return fmt.Errorf(
					"bind mount source %q is not the app's trusted source root",
					m.Source)
			}
		case mount.TypeTmpfs:
			if !m.ReadOnly {
				return fmt.Errorf("tmpfs mount at %q must be read-only", m.Target)
			}
		case mount.TypeVolume:
			if m.Source != "" {
				return fmt.Errorf("named volume %q at %q is not allowed", m.Source, m.Target)
			}
			if m.ReadOnly {
				return fmt.Errorf("volume at %q must be writable", m.Target)
			}
			if m.VolumeOptions != nil {
				return fmt.Errorf("volume options at %q are not allowed", m.Target)
			}
			if m.Target != tmpVolumeTarget {
				if err := app.ValidateServiceEphemeralDestination(m.Target); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("mount type %q at %q is not allowed", m.Type, m.Target)
		}
	}
	return nil
}

// isContainerRuntimeSocket reports whether p names the Docker/container runtime
// socket. Workload containers must never receive control-plane access through a
// bind mount.
func isContainerRuntimeSocket(p string) bool {
	base := strings.ToLower(path.Base(strings.ReplaceAll(p, `\`, "/")))
	return base == "docker.sock" || base == "containerd.sock" || base == "podman.sock"
}

// isTrustedSourcePath reports whether a bind source is the app's recorded live
// source root or lies beneath it. sourceRoot is the HostPath Relay itself
// resolved for the SourceMount — the same path it hands the daemon — so a
// legitimate daemon-visible source under Docker named-volume storage is
// accepted without treating every read-only host path as safe. A missing root,
// or the filesystem root, matches nothing, so a workload can never mount an
// arbitrary sensitive directory such as /etc.
func isTrustedSourcePath(source, sourceRoot string) bool {
	if source == "" || sourceRoot == "" {
		return false
	}
	root := cleanHostPath(sourceRoot)
	if root == "/" {
		return false
	}
	source = cleanHostPath(source)
	return source == root || strings.HasPrefix(source, root+"/")
}

// cleanHostPath normalizes a host path to slash form for provenance comparison.
// Container-host paths are compared with slash semantics, and a Windows-style
// separator is folded so the comparison cannot be bypassed by spelling.
func cleanHostPath(p string) string {
	return path.Clean(strings.ReplaceAll(p, `\`, "/"))
}

// benignRemovalErr reports whether a container-removal error means the container
// will not outlive the call, which is the only contract a best-effort removal
// has. Two daemon responses are benign:
//
//   - not-found (cerrdefs.ErrNotFound): the daemon already removed the container
//     (AutoRemove finished, or a previous remove succeeded). Nothing left to do.
//   - conflict (cerrdefs.ErrConflict, HTTP 409): the daemon is removing the
//     container right now (AutoRemove in flight). The container still exists in
//     the daemon's map until the removal completes, so a DELETE issued in that
//     window races the daemon's own removal and answers 409 rather than 404; the
//     container will still be gone.
//
// A nil error (no error) is trivially benign. Any other error (permission
// denied, internal, ...) is a genuine failure and is NOT benign. Matching is
// typed (errors.Is against the errdefs sentinels), never on the error string.
func benignRemovalErr(err error) bool {
	return err == nil || errors.Is(err, cerrdefs.ErrNotFound) || errors.Is(err, cerrdefs.ErrConflict)
}

// removeContainer removes a container, treating an already-removed container and
// a removal already in progress as success. With AutoRemove the daemon removes
// the container as soon as it exits, so Relay's best-effort removal routinely
// races the daemon: it either finds the container already gone (not-found) or
// already being removed (conflict/409). Both mean "the container will not outlive
// this call", which is the only contract a best-effort remove has, so neither is
// logged as noise. Only a real (non-benign) removal failure is surfaced so the
// caller can log it.
func removeContainer(cli *client.Client, id string) error {
	return removeContainerContext(context.Background(), cli, id)
}

// removeContainerContext is the context-aware removal: the bound is min(ctx, the
// containerOpTimeout cap). It preserves removeContainer's benign-error semantics
// exactly (not-found/conflict are success). RemoveVolumes is always set so every
// anonymous volume Relay created for /tmp or a service ephemeral destination is
// reclaimed on every explicit delete path; AutoRemove already does the same for
// one-shot containers when their process exits.
func removeContainerContext(ctx context.Context, cli *client.Client, id string) error {
	rmCtx, cancel := context.WithTimeout(ctx, containerOpTimeout)
	defer cancel()
	_, err := cli.ContainerRemove(rmCtx, id, client.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	})
	if benignRemovalErr(err) {
		return nil
	}
	return err
}
