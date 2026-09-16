package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
	"relay/internal/runtime/python"
)

// arch and platform are the build-host architecture keys for the dependency
// fingerprint. They default to the running process's GOARCH/GOOS (the daemon
// this process builds on is the daemon it executes on, so the build host is the
// correct key). Making them variables lets tests inject synthetic architectures.
var (
	arch     = runtime.GOARCH
	platform = runtime.GOOS
)

// engineFor returns the engine that prepares a spec. A single engine serves
// every version registered for its language.
func engineFor(spec plan.Spec) (interface {
	Plan(plan.Spec, string) (plan.BuildPlan, error)
}, error) {
	switch spec.Engine {
	case plan.EnginePython:
		return python.Engine{}, nil
	case plan.EngineNode:
		return node.Engine{}, nil
	default:
		return nil, fmt.Errorf("runtime %q: no engine for %q", spec.Name, spec.Engine)
	}
}

// Manager prepares function images and executes handler invocations. It owns a
// single Docker Engine client, reused for every build and invocation.
type Manager struct {
	log *slog.Logger
	cli *client.Client
	// metrics is an optional observability registry. A nil registry disables
	// all metric recording; every call is a no-op.
	metrics *metrics.Registry
	// hostname identifies this worker for container ownership. It is the same
	// value as the Redis consumer identity (config.ConsumerName), so Relay's
	// container-label hostname and its stream consumer identity are one and the
	// same. The startup orphan sweep uses it to distinguish this worker's
	// stalled containers from those of every other worker sharing the daemon.
	hostname string
}

// NewManager connects to the Docker daemon so failures surface at startup
// rather than per event. The client is configured from the environment
// (DOCKER_HOST / DOCKER_TLS_VERIFY / DOCKER_CERT_PATH) and negotiates the API
// version automatically. m is an optional observability registry; a nil registry
// disables metric recording (every call is a no-op). hostname is this worker's
// hostname-scoped container ownership identity (e.g. config.ConsumerName()); it
// is stamped as the relay.hostname label on every execution container and gates
// the startup orphan sweep.
func NewManager(logger *slog.Logger, m *metrics.Registry, hostname string) (*Manager, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to Docker daemon: %w", err)
	}
	if _, err := cli.Ping(context.Background(), client.PingOptions{}); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("cannot connect to Docker daemon: %w", err)
	}
	return &Manager{log: logger, cli: cli, metrics: m, hostname: hostname}, nil
}

// Close releases the Docker Engine client. It is safe to call once during
// shutdown.
func (m *Manager) Close() error {
	return m.cli.Close()
}

// Prepared is a function whose image has been built.
type Prepared struct {
	Name        string
	Image       string
	Fingerprint string
	// Env are the runtime environment variables the function's engine requires
	// (e.g. PYTHONDONTWRITEBYTECODE for Python). They are applied to every
	// execution container for this function, after the base RELAY_HANDLER var.
	Env []string
	// Dependency is the full "relay-dep-*" reference this function image was
	// built FROM, or "" when the function declares no dependency layer. It is
	// the function image's parent, so a caller (the runner) knows which
	// dependency image this function version pulls its payload from — the input
	// to dependency garbage collection. It is populated on BOTH the build and
	// reuse paths.
	Dependency string
}

// Prepare builds exactly ONE image for the function's current content (never per
// handler or event), then returns a handle for executing invocations against it.
//
// The image reference is derived from a content fingerprint computed here, so
// the same source always maps to the same fingerprinted image. If that image is
// already present locally (an earlier build or previous boot produced it), the
// build is skipped and the existing image reused — restart-without-changes is
// cheap. A fingerprint error fails Prepare: the reconciler already computes the
// fingerprint before calling Prepare and retains the previous version on error,
// and for startup a fingerprint failure marks the function unavailable, which is
// consistent with the existing build-failure handling.
func (m *Manager) Prepare(ctx context.Context, fn function.Function) (*Prepared, error) {
	fp, err := function.Fingerprint(fn.Dir)
	if err != nil {
		return nil, fmt.Errorf("function %q: fingerprint: %w", fn.Name, err)
	}

	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		return nil, fmt.Errorf("function %q: %w", fn.Name, err)
	}

	eng, err := engineFor(spec)
	if err != nil {
		return nil, fmt.Errorf("function %q: %w", fn.Name, err)
	}

	p, err := eng.Plan(spec, fn.Dir)
	if err != nil {
		return nil, fmt.Errorf("function %q: plan: %w", fn.Name, err)
	}

	image := ImageRef(fn.Name, fp)

	// Prepare the dependency label reference for the return value on both paths.
	// On the build path it is the dependency image built FROM; on the reuse path
	// it is computed WITHOUT building (the dependency image obviously exists, or
	// the existing function image — which inherits its layers — would never have
	// built). Computing the dependency fingerprint needs the same fnDir reads the
	// function fingerprint above already performed, so it stays cheap.
	funcPrepared := &Prepared{Name: fn.Name, Image: image, Fingerprint: fp, Env: p.Env}
	if !p.Deps.IsZero() {
		// Split out the pure fingerprint computation so the reuse path below can
		// name the function image's dependency without touching the daemon.
		depFp, err := DependencyFingerprint(arch, platform, spec, fn.Dir, p.Deps)
		if err != nil {
			return nil, fmt.Errorf("function %q: %w", fn.Name, fmt.Errorf("dependency fingerprint: %w", err))
		}
		funcPrepared.Dependency = depImageRef(depFp)
	}

	// Reuse an existing local image when present. The fingerprinted reference is
	// the identity: an image carrying this exact tag was necessarily built from
	// identical source (the tag embeds the fingerprint prefix), so no content
	// comparison is needed.
	if m.imageExists(ctx, image) {
		m.log.Debug(
			"Function: image exists; reusing",
			"function", fn.Name,
			"image", image,
		)
		return funcPrepared, nil
	}

	// When the function declares a dependency layer, ensure the dependency image
	// exists first and build the function image FROM it. The dependency image is
	// content-addressed (no function name): it is shared across every function
	// and every version with identical (runtime + arch + manifest + install), so
	// a changed requirements.txt yields a NEW tag and an unchanged one reuses the
	// existing layer with no rebuild (even when the function's source changed).
	depRef := funcPrepared.Dependency
	if !p.Deps.IsZero() {
		depRef, err = m.ensureDependencyImage(ctx, fn, spec, p.Deps, depRef)
		if err != nil {
			return nil, fmt.Errorf("function %q: %w", fn.Name, err)
		}
		// The function image inherits every layer of the dependency image, so
		// its Dockerfile's FROM is the dependency reference rather than the raw
		// base image. The engine moved the install into Deps, so the function
		// image carries no install RUN of its own — only UserSetup/User/Env/
		// Entrypoint on top of the dependency layer.
		p.BaseImage = depRef
	}

	start := time.Now()
	if err := buildImage(ctx, m.cli, fn.Name, fn, p, image, functionImageLabels(fn.Name, fp, depRef)); err != nil {
		d := time.Since(start)
		m.metrics.ObserveDurationLabels("function_build_seconds", []metrics.Label{
			{Name: "function", Value: fn.Name},
		}, d)
		m.metrics.IncLabels("build_failures_total", []metrics.Label{
			{Name: "function", Value: fn.Name},
		})
		// Function names are validated to [a-z0-9][a-z0-9._-]* (bounded by
		// function count), so using them as labels is low-cardinality.
		m.log.Error("Function: build failed",
			"function", fn.Name,
			"duration", d,
			"result", "failed",
		)
		return nil, err
	}
	d := time.Since(start)
	m.metrics.ObserveDurationLabels("function_build_seconds",
		[]metrics.Label{{Name: "function", Value: fn.Name}}, d)
	m.log.Info("Function: built",
		"function", fn.Name,
		"duration", d,
		"result", "success",
	)
	return &Prepared{Name: fn.Name, Image: image, Fingerprint: fp, Env: p.Env, Dependency: funcPrepared.Dependency}, nil
}

// ensureDependencyImage builds the dependency image for the function's
// dependency manifest set, returning the dependency image reference. It is a
// no-op (returns the existing reference) when the dependency image is already
// present locally — the content address makes existence the correctness test,
// since the tag embeds the fingerprint over every relevant input. If the
// dependency build fails, Prepare fails: there is no fallback to the old
// single-stage build, because the function image's Dockerfile inherits its
// dependency layers via FROM and cannot be built without them.
//
// depRef is the reference the caller already derived for the dependency
// fingerprint (so a build failure is attributable, and the caller has it in hand
// even for the reuse case). It must be the reference for depFingerprint; the
// two travel together to keep "which dependency was this built for" exact.
func (m *Manager) ensureDependencyImage(ctx context.Context, fn function.Function, spec plan.Spec, deps plan.Deps, depRef string) (string, error) {
	// Derive the dependency fingerprint so the built image is stamped with the
	// exact content address it encodes (see dependencyImageLabels). The
	// fingerprint has already been computed by Prepare's split above, but
	// recomputing here keeps this method self-contained and cheap (the same
	// manifest reads); the reference passed in is what identifies the image.
	fp, err := DependencyFingerprint(arch, platform, spec, fn.Dir, deps)
	if err != nil {
		return "", fmt.Errorf("dependency fingerprint: %w", err)
	}
	if depRef == "" {
		depRef = depImageRef(fp)
	}
	if m.imageExists(ctx, depRef) {
		m.log.Debug("Dependency image exists; reusing", "dep_image", depRef)
		return depRef, nil
	}

	start := time.Now()
	if err := buildDependencyImage(ctx, m.cli, spec, fn.Dir, deps, depRef, fp); err != nil {
		d := time.Since(start)
		// Dependency-image build failures count as function build failures so the
		// existing failure metric/label surface stays the single observability
		// contract for "this function could not be prepared".
		m.metrics.IncLabels("build_failures_total", []metrics.Label{
			{Name: "function", Value: fn.Name},
		})
		m.log.Error("Function: dependency build failed",
			"function", fn.Name,
			"duration", d,
			"dep_image", depRef,
			"result", "failed",
		)
		return "", err
	}
	d := time.Since(start)
	m.metrics.ObserveDurationLabels("function_build_seconds",
		[]metrics.Label{{Name: "function", Value: fn.Name}}, d)
	m.log.Info("Function: dependency layer built",
		"function", fn.Name,
		"duration", d,
		"dep_image", depRef,
		"result", "success",
	)
	return depRef, nil
}

// Execute runs the container for one invocation of the given handler with the
// event JSON on stdin. The context must carry the per-invocation timeout; a
// timeout kills the invocation and is treated as a failure. The invocation's
// diagnostic RunMeta is read from ctx (see WithRunMeta); when absent the labels
// are empty, which is harmless (labels are diagnostic-only).
//
// extraEnv are additional environment variables applied to the container after
// the function's plan env (and after the base RELAY_HANDLER var). They carry the
// template's literal env values and the resolved secret values for this single
// invocation. They are resolved per execution by the runner and never stored on
// Prepared, so rotating a secret value never requires a rebuild. Later entries
// win on duplicate names (container env semantics), so a template env var may
// intentionally override a runtime default like PYTHONDONTWRITEBYTECODE.
func (m *Manager) Execute(
	ctx context.Context,
	prepared *Prepared,
	handler string,
	eventJSON []byte,
	extraEnv []string,
) error {
	meta := RunMetaFrom(ctx)
	if meta.Hostname == "" {
		// Fall back to the manager's worker identity so a direct caller that
		// did not inject RunMeta still stamps the container's owner (and so an
		// orphaned container from this worker is still attributable at sweep
		// time). The runner always injects the full meta; this is the safety
		// net for direct/integration callers.
		meta.Hostname = m.hostname
	}
	return runContainer(
		ctx,
		m.cli,
		m.log,
		prepared.Name,
		prepared.Image,
		prepared.Env,
		extraEnv,
		handler,
		eventJSON,
		meta,
	)
}
