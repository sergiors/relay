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
}

// NewManager connects to the Docker daemon so failures surface at startup
// rather than per event. The client is configured from the environment
// (DOCKER_HOST / DOCKER_TLS_VERIFY / DOCKER_CERT_PATH) and negotiates the API
// version automatically. m is an optional observability registry; a nil registry
// disables metric recording (every call is a no-op).
func NewManager(logger *log.Logger, m *metrics.Registry) (*Manager, error) {
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
	return &Manager{log: logger, cli: cli, metrics: m}, nil
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
		return &Prepared{Name: fn.Name, Image: image, Fingerprint: fp}, nil
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
	return &Prepared{Name: fn.Name, Image: image, Fingerprint: fp}, nil
}

// Execute runs the container for one invocation of the given handler with the
// event JSON on stdin. The context must carry the per-invocation timeout; a
// timeout kills the invocation and is treated as a failure.
func (m *Manager) Execute(
	ctx context.Context,
	prepared *Prepared,
	handler string,
	eventJSON []byte,
) error {
	return runContainer(
		ctx,
		m.cli,
		m.log.Printf,
		prepared.Name,
		prepared.Image,
		handler,
		eventJSON,
	)
}
