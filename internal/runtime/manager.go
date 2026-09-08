package runtime

import (
	"context"
	"fmt"
	"log"

	"github.com/moby/moby/client"

	"relay/internal/function"
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
}

// NewManager connects to the Docker daemon so failures surface at startup
// rather than per event. The client is configured from the environment
// (DOCKER_HOST / DOCKER_TLS_VERIFY / DOCKER_CERT_PATH) and negotiates the API
// version automatically.
func NewManager(logger *log.Logger) (*Manager, error) {
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
	return &Manager{log: logger, cli: cli}, nil
}

// Close releases the Docker Engine client. It is safe to call once during
// shutdown.
func (m *Manager) Close() error {
	return m.cli.Close()
}

// Prepared is a function whose image has been built.
type Prepared struct {
	Name  string
	Image string
}

// Prepare builds exactly ONE image for the function (never per handler or
// event), then returns a handle for executing invocations against it.
func (m *Manager) Prepare(ctx context.Context, fn function.Function) (*Prepared, error) {
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

	image := imageRef(fn.Name)
	if err := buildImage(ctx, m.cli, fn.Name, fn, p, image); err != nil {
		return nil, err
	}
	return &Prepared{Name: fn.Name, Image: image}, nil
}

// Execute runs the container for one invocation of the given handler with the
// event JSON on stdin. The context must carry the per-invocation timeout; a
// timeout kills the invocation and is treated as a failure.
func (m *Manager) Execute(ctx context.Context, prepared *Prepared, handler string, eventJSON []byte) error {
	return runContainer(ctx, m.cli, m.log.Printf, prepared.Name, prepared.Image, handler, eventJSON)
}
