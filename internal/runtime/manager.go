package runtime

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/logging"
	"relay/internal/metrics"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
	"relay/internal/runtime/python"
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
	log *log.Logger
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
func NewManager(logger *log.Logger, m *metrics.Registry, hostname string) (*Manager, error) {
	if logger == nil {
		logger = log.Default()
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

	// Reuse an existing local image when present. The fingerprinted reference is
	// the identity: an image carrying this exact tag was necessarily built from
	// identical source (the tag embeds the fingerprint prefix), so no content
	// comparison is needed.
	if m.imageExists(ctx, image) {
		m.log.Printf("function %q: image %s exists; reusing", fn.Name, image)
		return &Prepared{Name: fn.Name, Image: image, Fingerprint: fp, Env: p.Env}, nil
	}

	start := time.Now()
	if err := buildImage(ctx, m.cli, fn.Name, fn, p, image); err != nil {
		d := time.Since(start)
		m.metrics.ObserveDurationLabels("function_build_seconds", []metrics.Label{
			{Name: "function", Value: fn.Name},
		}, d)
		m.metrics.IncLabels("build_failures_total", []metrics.Label{
			{Name: "function", Value: fn.Name},
		})
		// Function names are validated to [a-z0-9][a-z0-9._-]* (bounded by
		// function count), so using them as labels is low-cardinality.
		m.log.Printf("function %q: build failed%s", fn.Name,
			logging.Fields("function", fn.Name, "duration", d, "result", "failed"))
		return nil, err
	}
	d := time.Since(start)
	m.metrics.ObserveDurationLabels("function_build_seconds",
		[]metrics.Label{{Name: "function", Value: fn.Name}}, d)
	m.log.Printf("function %q: built%s",
		fn.Name, logging.Fields("function", fn.Name, "duration", d, "result", "success"))
	return &Prepared{Name: fn.Name, Image: image, Fingerprint: fp, Env: p.Env}, nil
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
		m.log.Printf,
		prepared.Name,
		prepared.Image,
		prepared.Env,
		extraEnv,
		handler,
		eventJSON,
		meta,
	)
}
