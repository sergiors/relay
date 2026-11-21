package reconciler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"relay/internal/runtime"
)

const (
	// serviceHealthObserverInterval is the bounded observation cadence while at
	// least one candidate is registered. It is deliberately short so a readiness
	// transition prompts reconcile promptly; the observer itself performs no
	// desired-state work.
	serviceHealthObserverInterval = time.Second
	// serviceHealthObserverInspectTimeout bounds one observation round. It is
	// independent of the reconcile budget and never delays worker shutdown.
	serviceHealthObserverInspectTimeout = 2 * time.Second
	// serviceHealthObserverRecoverTimeout bounds the startup reconstruction of
	// candidate registrations from existing Docker state.
	serviceHealthObserverRecoverTimeout = 5 * time.Second
)

// ServiceHealthDocker is the Docker-specific seam the health observer uses. It is
// deliberately narrower than the service reconciler's Docker interface: the
// observer inspects only explicitly registered pending candidates.
type ServiceHealthDocker interface {
	ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error)
	ObserveServiceHealth(
		ctx context.Context, ids []string,
	) (map[string]runtime.ServiceHealthObservation, error)
}

// ServiceHealthObserver polls Docker health for registered pending candidates and
// calls the coordinator's Notify on a relevant state/health transition. It is a
// hint source only: it never starts, stops, commits, or authorizes containers;
// reconcile revalidates every observation. It tracks only candidates reported by
// the coordinator plus the uncommitted candidates reconstructed at startup, so it
// never polls healthy active replicas.
type ServiceHealthObserver struct {
	docker  ServiceHealthDocker
	pending func() []runtime.ServiceHealthCandidate
	// tracked reports the apps the coordinator has accepted service work for
	// since Start. It is what lets a startup-recovered registration be retired
	// once the coordinator's candidate list became authoritative for its app,
	// without dropping recovery for an app that has not been reconciled yet.
	tracked func() map[string]bool
	recover func(context.Context) ([]runtime.ServiceHealthCandidate, error)
	notify  func(string)
	log     *slog.Logger
	now     func() time.Time

	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	startOne sync.Once
	stopOne  sync.Once

	mu        sync.Mutex
	recovered map[string]string
	last      map[string]runtime.ServiceHealthObservation
	loaded    bool
}

// ServiceHealthObserverOption configures an optional observer dependency.
type ServiceHealthObserverOption func(*ServiceHealthObserver)

// WithTrackedServiceApps wires the coordinator scope that retires
// startup-recovered candidate registrations once an app's service state has been
// reconciled (see ServiceCoordinator.TrackedServiceApps). Omitting it keeps the
// conservative pre-existing behavior: a recovered registration is dropped only
// when the coordinator reports candidates for its app, on a definitive Missing
// observation, or when its container turns healthy.
func WithTrackedServiceApps(tracked func() map[string]bool) ServiceHealthObserverOption {
	return func(o *ServiceHealthObserver) { o.tracked = tracked }
}

// NewServiceHealthObserver builds the observer. pending must return the
// coordinator's current pending candidates; recover must reconstruct uncommitted
// candidates from Docker state at startup. notify is the coordinator's Notify.
func NewServiceHealthObserver(
	docker ServiceHealthDocker,
	pending func() []runtime.ServiceHealthCandidate,
	recover func(context.Context) ([]runtime.ServiceHealthCandidate, error),
	notify func(string),
	log *slog.Logger,
	opts ...ServiceHealthObserverOption,
) *ServiceHealthObserver {
	o := &ServiceHealthObserver{
		docker:    docker,
		pending:   pending,
		recover:   recover,
		notify:    notify,
		log:       log,
		now:       time.Now,
		recovered: map[string]string{},
		last:      map[string]runtime.ServiceHealthObservation{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return o
}

// Start roots the observer in the worker lifecycle. Cancelling lifecycle stops
// the loop; Join waits for it. Start is idempotent.
func (o *ServiceHealthObserver) Start(lifecycle context.Context) {
	if o == nil {
		return
	}
	o.startOne.Do(func() {
		o.ctx, o.cancel = context.WithCancel(lifecycle)
		o.done = make(chan struct{})
		go o.loop()
	})
}

// Join cancels the observer and waits for its loop to exit. It is safe when Start
// was never called and on repeated calls.
func (o *ServiceHealthObserver) Join(ctx context.Context) error {
	if o == nil {
		return nil
	}
	o.stopOne.Do(func() {
		if o.cancel != nil {
			o.cancel()
		}
	})
	if o.done == nil {
		return nil
	}
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *ServiceHealthObserver) loop() {
	defer close(o.done)
	ticker := time.NewTicker(serviceHealthObserverInterval)
	defer ticker.Stop()
	for {
		o.observeOnce()
		select {
		case <-ticker.C:
		case <-o.ctx.Done():
			return
		}
	}
}

func (o *ServiceHealthObserver) observeOnce() {
	if o.ctx.Err() != nil {
		return
	}
	o.loadRecoveredOnce()
	candidates := o.snapshotCandidates()
	if len(candidates) == 0 {
		return
	}
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	ctx, cancel := context.WithTimeout(o.ctx, serviceHealthObserverInspectTimeout)
	observations, err := o.docker.ObserveServiceHealth(ctx, ids)
	cancel()
	if err != nil && o.log != nil {
		o.log.Debug("Service: health observation failed", "error", err)
	}
	if len(observations) == 0 {
		// A transient round failure must not drop registrations.
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, app := range candidates {
		observation, ok := observations[id]
		if !ok {
			// No result for this ID means a transient inspect failure, not a
			// confirmed removal. Retain the registration and retry next tick.
			continue
		}
		previous, seen := o.last[id]
		o.last[id] = observation
		if observation.Missing || observation.Health == "healthy" {
			delete(o.recovered, id)
		}
		relevant := false
		switch {
		case observation.Missing:
			relevant = !seen || !previous.Missing
		case !seen:
			relevant = observation.State != "running" ||
				observation.Health == "unhealthy" ||
				observation.Health == "healthy"
		default:
			relevant = observation != previous
		}
		if relevant && o.notify != nil {
			o.notify(app)
		}
	}
}

// snapshotCandidates merges the coordinator's authoritative pending candidate
// list with startup-recovered uncommitted candidates. Once the coordinator has
// accepted service work for an app, its list is authoritative for that app:
// recovered entries it does not report are stale (the candidate was committed,
// superseded, or the app was removed) and are dropped, so the observer stops
// polling containers that are no longer candidates. Before that first accepted
// request the recovered registration is retained, which is what keeps startup
// recovery — and a missed readiness notification — working.
func (o *ServiceHealthObserver) snapshotCandidates() map[string]string {
	out := map[string]string{}
	byApp := map[string]map[string]bool{}
	if o.pending != nil {
		for _, candidate := range o.pending() {
			if candidate.ContainerID == "" || candidate.App == "" {
				continue
			}
			out[candidate.ContainerID] = candidate.App
			if byApp[candidate.App] == nil {
				byApp[candidate.App] = map[string]bool{}
			}
			byApp[candidate.App][candidate.ContainerID] = true
		}
	}
	tracked := map[string]bool{}
	if o.tracked != nil {
		tracked = o.tracked()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, app := range o.recovered {
		if pending, ok := byApp[app]; ok {
			// The coordinator reports candidates for this app. Its list is
			// authoritative, so a recovered ID it omits is not pending.
			if !pending[id] {
				delete(o.recovered, id)
				delete(o.last, id)
			}
			continue
		}
		if tracked[app] {
			// The coordinator has concluded (or is converging) this app's
			// service work but reports no candidate with this ID: it was
			// committed, replaced, or removed. Stop observing it.
			delete(o.recovered, id)
			delete(o.last, id)
			continue
		}
		out[id] = app
	}
	// Drop observation records that are no longer candidates so a replacement
	// container observed later starts with a clean transition baseline.
	for id := range o.last {
		if _, ok := out[id]; !ok {
			delete(o.last, id)
		}
	}
	return out
}

func (o *ServiceHealthObserver) loadRecoveredOnce() {
	o.mu.Lock()
	loaded := o.loaded
	o.mu.Unlock()
	if loaded || o.recover == nil {
		return
	}
	ctx, cancel := context.WithTimeout(o.ctx, serviceHealthObserverRecoverTimeout)
	candidates, err := o.recover(ctx)
	cancel()
	if err != nil {
		if o.log != nil {
			o.log.Debug("Service: health candidate recovery failed", "error", err)
		}
		return
	}
	o.mu.Lock()
	if !o.loaded {
		for _, candidate := range candidates {
			if candidate.ContainerID == "" || candidate.App == "" {
				continue
			}
			o.recovered[candidate.ContainerID] = candidate.App
		}
		o.loaded = true
	}
	o.mu.Unlock()
}
