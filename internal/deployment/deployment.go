// Package deployment implements Relay's source-driven coordinated
// deployment mode.
//
// Participation is source-driven and independent of the execution backend: a
// configured Git source activates the coordinator automatically, with no
// mode flag. A shared PostgreSQL
// row names the exact immutable revision every node must materialize, and each
// node independently fetches, checks out, and materializes that revision through
// the existing internal/git source and materialization code. Discovery,
// building, activation, services, schedules, warm containers, and event matching
// remain the existing reconciler/runtime behavior.
//
// There is no leader, executor, or global barrier. Publishing commits a monotonic
// deployment generation; after that commit every node converges on its own,
// including if the publisher process dies immediately. A node that cannot fetch
// or authenticate reports its own failure and keeps serving its previously
// active generation; local SSH keys, known_hosts, and other private credentials
// are never shared.
//
// Local drift of /apps is detected by the app reconciler's periodic pass, which
// compares each app against the materialized authorization and reports every
// mismatch back through Coordinator.ReportDrift. A coordinator-side check runs
// independently of the reconciler's serial preparation pump, so a long build
// cannot delay drift detection; it hashes only the apps the reconciler has not
// verified in the current scan window (since the previous check), so the same
// tree is not hashed twice per cadence and no verification is trusted by two
// checks.
//
// Git updates /apps per app before any authorization exists for the new
// generation. The materialization is therefore committed atomically: the node's
// applied generation, the complete per-app authorization, and the materialized
// phase are written in one short, incarnation-fenced transaction. A failed or
// fenced commit writes nothing, so the previous generation keeps serving, and a
// later failure report never rolls a committed applied generation back. Because
// /apps can be left partially synchronized by a failed attempt, the worker's
// startup validator refuses to consume when a published revision could not be
// materialized and the local tree can no longer be proven to be exactly the
// previous authorized generation. Without such proof the node fails closed
// instead of ACKing previously matched deliveries unmatched.
//
// Rollback is a NEW generation carrying an older commit. A manual publication may
// pin that revision; while pinned, automatic webhook pushes are ignored so a
// later or out-of-order branch delivery cannot silently undo the rollback.
// Unpinning resumes normal branch tracking without materializing anything new.
package deployment

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"time"

	"relay/internal/app"
	"relay/internal/git"
	"relay/internal/state"
)

// DefaultPollInterval is the fallback cadence for re-reading the shared desired
// revision when no notification arrives. PostgreSQL NOTIFY hints normally
// deliver a committed change immediately (see the worker's listener
// supervisor); this bounded read is what keeps convergence correct when a
// notification is missed, a listener is reconnecting, or the database is
// reached through a transaction-pooling proxy that cannot preserve a LISTEN
// session.
const DefaultPollInterval = 60 * time.Second

// DefaultRetryInterval is the short retry cadence used only while retryable
// work is pending: a failed materialization attempt or a failed desired-state
// read. It keeps a transient failure from waiting for the full fallback poll.
// It is never a general reconciliation cadence.
const DefaultRetryInterval = 5 * time.Second

// DefaultDriftInterval is the periodic local-drift check. Immediate drift
// detection comes from the reconciler's filesystem watch; this is the bounded
// backstop that re-materializes the authorized revision if an edit was missed.
// It runs on the coordinator loop, independent of the reconciler's serial
// preparation pump, and hashes only the apps the reconciler has not verified
// since the previous check, under the current authorization.
const DefaultDriftInterval = 30 * time.Second

// materializeTimeout bounds one fetch/checkout/materialization attempt. The
// attempt holds no database transaction.
const materializeTimeout = 15 * time.Minute

// ErrDesiredGenerationChanged reports that the shared desired revision moved on
// while a startup preparation was in flight. The prepared generation must not be
// published; the previous active generation keeps serving and the newer revision
// converges normally.
var ErrDesiredGenerationChanged = errors.New("coordinated deployment desired generation changed")

// Config assembles one node's coordinator. State, NodeID, and InstanceID come
// from the worker; the directories are the same local Git/source conventions
// used by `relay git`.
type Config struct {
	State      *state.State
	NodeID     string
	InstanceID string

	GitConfigPath  string
	GitCheckoutDir string
	GitSSHDir      string
	AppsDir        string

	Logger *slog.Logger
	// LoadApps overrides the materialized-tree app discovery/load used by
	// authorize and the drift check. It is a narrow seam for exercising a
	// root/global loader failure without a filesystem race. Nil uses
	// app.NewLoader(AppsDir, Logger).
	LoadApps func(dir string) ([]app.App, []app.LoadIssue, error)
	// PollInterval, RetryInterval, and DriftInterval override the package
	// defaults when positive.
	PollInterval  time.Duration
	RetryInterval time.Duration
	DriftInterval time.Duration
}

// Snapshot is the generation this node has materialized and authorized in
// /apps. Fingerprints is the authoritative app-name -> content fingerprint set
// computed from that materialized tree; Removed names were part of the previous
// authorized generation but are absent from this one. Authoritative is false
// when no materialized generation is known yet, in which case the reconciler
// must not authorize cluster-mode local reconciliation.
type Snapshot struct {
	Generation         int64
	Commit             string
	Fingerprints       map[string]string
	TemplateIdentities map[string]string
	// Invalid holds app names that were materialized but whose fingerprint or
	// load failed, with a diagnostic. Invalid template bytes are never authorized
	// to activate; a fingerprint-only failure is retried while the materialized
	// template identity still matches.
	Invalid map[string]string
	// InvalidTemplates holds the materialized template identity for an invalid
	// app when one is known, so the live reconciler can distinguish the desired
	// invalid bytes from a locally introduced correction/edit.
	InvalidTemplates map[string]string
	// Removed names were part of the previous authorized generation but are
	// absent from this one.
	Removed       []string
	Authoritative bool
	// Error reports a non-fatal startup/reconciliation materialization failure.
	// The previous snapshot remains authoritative while it is set.
	Error error
}

// Coordinator observes the shared desired revision, materializes it locally,
// and records per-node materialization plus per-app activation progress. All
// progress writes are fenced by the PostgreSQL-allocated incarnation and the
// deployment generation, so a stale process or stale generation cannot overwrite
// newer progress.
type Coordinator struct {
	cfg   Config
	poll  time.Duration
	retry time.Duration
	drift time.Duration

	mu      sync.Mutex
	applied Snapshot
	// driftSince is the boundary of the current independent drift window: it is
	// the start time of the previous check. A reconciler verification is trusted
	// by exactly one check (the one whose window contains it), so a check that
	// runs shortly after another cannot re-trust the same verification and defer
	// detection.
	driftSince time.Time
	// verifiedSince is the reconciler's authorization-scoped verification ledger.
	// It is runtime wiring installed by SetVerificationLedger, not a tunable
	// option; nil performs the full check.
	verifiedSince func(since time.Time) map[string]struct{}
	// incarnation and appliedIncarnation are the PostgreSQL fences for this
	// node's observed generation and applied generation respectively. App
	// progress uses appliedIncarnation.
	incarnation        int64
	appliedIncarnation int64
	ctx                context.Context
	// kick is the coalescing wake signal (capacity one). Wake and Kick both
	// send to it; a signal that arrives while the loop is syncing stays queued
	// and is handled after the active pass returns.
	kick chan struct{}
	// forceWake records that the pending wake must rematerialize an unchanged
	// generation. It is consumed together with the wake signal, so a drift
	// request cannot be downgraded by a coalesced database hint.
	forceWake bool
	// retryGeneration and retryCommit identify a generation whose attempt
	// failed after the applied state already matched it (for example a restart
	// that could not re-fetch an unchanged revision). Without them the
	// unchanged-generation fast path would suppress the retry.
	retryGeneration int64
	retryCommit     string
	// stateRetry records a failed desired-state read that must be retried on
	// the short retry cadence instead of waiting for the fallback poll.
	stateRetry bool
	// appliedUncertain records that a materialization commit returned an outcome
	// the coordinator could not resolve from persisted state. The next attempt
	// re-reads the persisted applied authorization before it uses the in-memory
	// view as the previous generation or reports a failure, so a commit that
	// landed is never rolled back to a stale generation, commit, or same-generation
	// authorization set.
	appliedUncertain bool
	// onMaterialized publishes a newly applied authorization to the reconciler.
	onMaterialized func(Snapshot)
}

// New builds a coordinator. It performs no I/O.
func New(cfg Config) *Coordinator {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = DefaultRetryInterval
	}
	if cfg.DriftInterval <= 0 {
		cfg.DriftInterval = DefaultDriftInterval
	}
	if cfg.LoadApps == nil {
		cfg.LoadApps = func(dir string) ([]app.App, []app.LoadIssue, error) {
			return app.NewLoader(dir, cfg.Logger).LoadWithDiagnostics()
		}
	}
	return &Coordinator{
		cfg:        cfg,
		poll:       cfg.PollInterval,
		retry:      cfg.RetryInterval,
		drift:      cfg.DriftInterval,
		driftSince: time.Now(),
		kick:       make(chan struct{}, 1),
	}
}

// SetVerificationLedger installs the reconciler's verification ledger, which
// reports the app names whose content the reconciler has verified since a given
// time under its current authorization. The independent drift check then hashes
// only the authorized apps the reconciler has not verified in the current scan
// window, so a long preparation holding the reconciler's single pump cannot
// delay drift detection while the steady state performs no duplicate full-tree
// hash. It is runtime wiring, not a Config option; it must be called before
// Start, and a nil ledger keeps the full check.
func (c *Coordinator) SetVerificationLedger(verified func(since time.Time) map[string]struct{}) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.verifiedSince = verified
	c.mu.Unlock()
}

// ObserveStartup reads the shared desired revision and, when one exists, fetches
// and materializes its exact commit before the worker loads /apps. A failed
// attempt is reported in the snapshot Error and leaves the previously
// materialized generation authoritative, so the node keeps serving the old
// generation and retries in the background. Git may already have changed parts of
// /apps by then; the worker's startup validator decides whether that previous
// authorization can still be proven intact locally before consumption is
// allowed.
//
// A state read failure is returned: a coordinator-active node cannot converge
// without the shared row, and silently treating it as "nothing published" would
// be unsafe.
func (c *Coordinator) ObserveStartup(ctx context.Context) (Snapshot, error) {
	if c == nil {
		return Snapshot{}, nil
	}
	if c.cfg.State == nil {
		return Snapshot{}, errors.New("deployment: state is not configured")
	}
	previous, err := c.loadApplied(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	c.setApplied(previous)
	// The per-app progress rows for the previous applied generation become the
	// authorization fallback if this startup's fetch fails.
	if c.cfg.NodeID != "" {
		if node, found, err := c.cfg.State.ReadDeploymentNode(ctx, c.cfg.NodeID); err == nil && found {
			c.setAppliedIncarnation(node.AppliedIncarnation)
			if node.Generation > 0 {
				// Take ownership of this node's progress with a fresh incarnation
				// even before observing a desired revision. Without this, a restart
				// with no published revision could not report activation progress for
				// the generation it keeps serving.
				acquired, aerr := c.cfg.State.AcquireDeploymentNode(
					ctx, c.cfg.NodeID, c.cfg.InstanceID, node.Generation,
					state.DeploymentNodeProgress{
						NodeID:            c.cfg.NodeID,
						Generation:        node.Generation,
						AppliedGeneration: node.AppliedGeneration,
						Instance:          c.cfg.InstanceID,
						Phase:             node.Phase,
						Commit:            node.Commit,
						AppliedCommit:     node.AppliedCommit,
					})
				if aerr == nil {
					c.incarnation = acquired.Incarnation
					c.setAppliedIncarnation(acquired.Incarnation)
				}
			}
		}
	}
	desired, found, err := c.cfg.State.ReadDeploymentDesired(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if !found {
		if previous.Error == nil && c.cfg.Logger != nil {
			c.cfg.Logger.Warn(
				"Deployment: no coordinated revision published; serving the last materialized apps",
			)
		}
		return previous, nil
	}
	snapshot, err := c.materialize(ctx, previous, desired)
	if err != nil {
		c.mu.Lock()
		c.retryGeneration = desired.Generation
		c.retryCommit = desired.Commit
		c.mu.Unlock()
		if c.cfg.Logger != nil {
			c.cfg.Logger.WarnContext(
				ctx, "Deployment: startup materialization failed; retaining previous generation",
				"generation", desired.Generation, "commit", desired.Commit, "error", err,
			)
		}
		previous.Error = err
		return previous, nil
	}
	return snapshot, nil
}

// Start runs the convergence loop until ctx is cancelled and returns a channel
// closed after the loop exits. onMaterialized receives each newly applied
// authorization (so the reconciler's cluster gate advances with it); enqueue is
// called with app names that need a normal reconciler pass after a successful
// materialization. Both callbacks are nil-safe. The first pass is immediate so a
// revision published while startup was in progress converges promptly.
//
// Besides the 60s desired poll and the conditional 5s retry, the loop runs the
// independent drift check on the drift interval. That check does not share the
// reconciler's serial preparation pump, so a long build cannot delay it, and it
// hashes only apps the reconciler has not verified since the previous check
// (see SetVerificationLedger), so no verification is trusted twice.
func (c *Coordinator) Start(
	ctx context.Context,
	onMaterialized func(Snapshot),
	enqueue func(string),
) <-chan struct{} {
	done := make(chan struct{})
	if c == nil || c.cfg.State == nil {
		close(done)
		return done
	}
	c.mu.Lock()
	c.ctx = ctx
	c.onMaterialized = onMaterialized
	c.mu.Unlock()
	if enqueue == nil {
		enqueue = func(string) {}
	}
	go func() {
		defer close(done)
		poll := time.NewTicker(c.poll)
		defer poll.Stop()
		retryTick := time.NewTicker(c.retry)
		defer retryTick.Stop()
		driftTick := time.NewTicker(c.drift)
		defer driftTick.Stop()
		// Startup-invalid apps were deliberately excluded from startup
		// authorization, and materialization happened before watches existed, so no
		// initial fsnotify event will drive them through the reconciler. Enqueue
		// exactly the invalid names from the already-applied snapshot now; the
		// initial syncOnce below may return early when the desired generation
		// already equals the applied generation and would otherwise skip enqueueing
		// them. Valid startup apps remain untouched.
		c.mu.Lock()
		startupInvalid := make([]string, 0, len(c.applied.Invalid))
		for name := range c.applied.Invalid {
			startupInvalid = append(startupInvalid, name)
		}
		c.mu.Unlock()
		if ctx.Err() == nil {
			for _, name := range startupInvalid {
				enqueue(name)
			}
		}
		c.syncOnce(ctx, enqueue, false)
		for {
			select {
			case <-ctx.Done():
				return
			case <-poll.C:
				c.syncOnce(ctx, enqueue, false)
			case <-retryTick.C:
				// Only retryable work wakes the short timer: a materialization
				// failure or a failed desired-state read. An idle coordinator
				// performs no extra reads here.
				if c.retryPending() {
					c.syncOnce(ctx, enqueue, false)
				}
			case <-c.kick:
				c.mu.Lock()
				force := c.forceWake
				c.forceWake = false
				c.mu.Unlock()
				c.syncOnce(ctx, enqueue, force)
			case <-driftTick.C:
				c.checkDrift(ctx)
			}
		}
	}()
	return done
}

// Wake requests a normal, non-forcing convergence attempt. It is the database
// hint path: the authoritative desired row is always re-read, and an unchanged
// generation is not rematerialized. It never blocks and is safe on a nil
// receiver.
func (c *Coordinator) Wake() {
	c.requestSync(false)
}

// Kick requests an immediate forced convergence attempt that rematerializes the
// current applied commit even when the shared generation is unchanged. It is
// reserved for local-drift repair (ReportDrift and the periodic drift check); a
// database hint must use Wake instead. It never blocks.
func (c *Coordinator) Kick() {
	c.requestSync(true)
}

// requestSync coalesces immediate convergence requests into one pending wake
// without losing a request that lands during an active sync: the buffered
// signal is one-shot, while every caller sets its force intent under the mutex
// before the loop consumes it. A wake that cannot enqueue because a signal is
// already pending is therefore still represented by that pending pass.
func (c *Coordinator) requestSync(force bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if force {
		c.forceWake = true
	}
	c.mu.Unlock()
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// ReportApp records per-app activation progress for the node's currently applied
// generation. It is the reconciler's progress callback. A stale coordinator (no
// applied generation or no incarnation yet) is a no-op. An empty-fingerprint
// Drifted update is also ignored while the applied snapshot marks the app
// desired-invalid, preserving its Failed row for restart recovery.
func (c *Coordinator) ReportApp(name, phase, errText string) {
	if c == nil || c.cfg.State == nil || name == "" {
		return
	}
	c.mu.Lock()
	generation := c.applied.Generation
	incarnation := c.appliedIncarnation
	commit := c.applied.Commit
	fingerprint := c.applied.Fingerprints[name]
	_, desiredInvalid := c.applied.Invalid[name]
	templateIdentity := c.applied.TemplateIdentities[name]
	if templateIdentity == "" {
		// Desired-invalid apps keep their materialized identity in InvalidTemplates
		// rather than TemplateIdentities. Preserve it on failed/drift diagnostics so
		// restart recovery does not see an empty identity and misclassify the app as
		// local drift during a fetch outage.
		templateIdentity = c.applied.InvalidTemplates[name]
	}
	ctx := c.ctx
	c.mu.Unlock()
	if generation <= 0 || incarnation <= 0 {
		return
	}

	if phase == state.DeploymentAppDrifted && desiredInvalid && fingerprint == "" {
		return
	}
	if fingerprint == "" {
		switch phase {
		case state.DeploymentAppActive,
			state.DeploymentAppActivating,
			state.DeploymentAppDegraded:
			// These phases describe authorized content. Without a materialized
			// fingerprint the write would erase a persisted fingerprint or record
			// an empty expected identity that startup must not authorize. Failed
			// diagnostics and permitted drift reports are still recorded.
			return
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.cfg.State.RecordDeploymentAppProgress(
		writeCtx, c.cfg.NodeID, name, generation, incarnation,
		state.DeploymentAppProgress{
			NodeID:           c.cfg.NodeID,
			App:              name,
			Generation:       generation,
			Instance:         c.cfg.InstanceID,
			Phase:            phase,
			Fingerprint:      fingerprint,
			TemplateIdentity: templateIdentity,
			Commit:           commit,
			Error:            errText,
		},
	)
	if err != nil && c.cfg.Logger != nil {
		c.cfg.Logger.DebugContext(
			writeCtx, "Deployment: record app progress failed", "app",
			name, "phase", phase, "error", err,
		)
	}
}

// ReportDrift records that local /apps content no longer matches the authorized
// generation and requests an immediate re-materialization. The previous active
// generation keeps serving until the restore succeeds. It is the reconciler's
// per-app drift seam; the coordinator's own periodic check (checkDrift) is the
// independent backstop for a reconcile that a build backlog has not reached yet.
//
// A name the applied snapshot already classifies as desired-invalid only requests
// the restore. Such an app has no authorized fingerprint, so a Drifted progress
// row would carry an empty fingerprint, which loadApplied discards on restart; the
// existing Failed row is the persisted authority for its desired-invalid state and
// must not be replaced by a transient diagnostic. Valid and local-only drift keeps
// recording Drifted progress.
func (c *Coordinator) ReportDrift(name, reason string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	_, invalid := c.applied.Invalid[name]
	c.mu.Unlock()
	if !invalid {
		c.ReportApp(name, state.DeploymentAppDrifted, reason)
	}
	c.Kick()
}

// syncOnce performs one background convergence attempt. force rematerializes the
// current applied commit even when the shared generation is unchanged (the drift
// restore path).
func (c *Coordinator) syncOnce(ctx context.Context, enqueue func(string), force bool) {
	desired, found, err := c.cfg.State.ReadDeploymentDesired(ctx)
	if err != nil {
		// A transient read failure is retryable work, not a permanent condition:
		// the short retry timer keeps it from waiting for the fallback poll.
		c.mu.Lock()
		c.stateRetry = true
		c.mu.Unlock()
		if c.cfg.Logger != nil {
			c.cfg.Logger.WarnContext(
				ctx, "Deployment: read desired revision failed", "error", err,
			)
		}
		return
	}
	c.mu.Lock()
	c.stateRetry = false
	applied := c.applied
	c.mu.Unlock()

	if !found {
		if !force || !applied.Authoritative || applied.Commit == "" {
			return
		}
		local, cfgErr := git.LoadConfig(c.cfg.GitConfigPath)
		if cfgErr != nil {
			return
		}
		desired = state.DeploymentDesired{
			Generation: applied.Generation,
			Commit:     applied.Commit,
			Repository: local.Repository,
			Ref:        local.Ref,
			Path:       local.Path,
		}
	}
	if desired.Generation < applied.Generation {
		return
	}
	c.mu.Lock()
	needsRetry := c.retryGeneration == desired.Generation &&
		strings.EqualFold(c.retryCommit, desired.Commit)
	c.mu.Unlock()
	if !force && !needsRetry &&
		desired.Generation == applied.Generation && strings.EqualFold(desired.Commit, applied.Commit) {
		return
	}
	next, err := c.materialize(ctx, applied, desired)
	if err != nil {
		c.mu.Lock()
		c.retryGeneration = desired.Generation
		c.retryCommit = desired.Commit
		c.mu.Unlock()
		if c.cfg.Logger != nil {
			c.cfg.Logger.WarnContext(
				ctx, "Deployment: materialization failed; retaining active generation",
				"generation", desired.Generation, "commit", desired.Commit, "error", err,
			)
		}
		return
	}
	c.mu.Lock()
	c.retryGeneration = 0
	c.retryCommit = ""
	apply := c.onMaterialized
	c.mu.Unlock()
	if apply != nil {
		apply(next)
	}
	c.enqueueSnapshot(enqueue, next)
}

// retryPending reports whether a previous attempt left retryable work: a failed
// desired-state read or a failed materialization. The short retry timer only
// runs a pass while this is true.
func (c *Coordinator) retryPending() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stateRetry || c.retryGeneration != 0
}

func (c *Coordinator) enqueueSnapshot(enqueue func(string), snapshot Snapshot) {
	for name := range snapshot.Fingerprints {
		enqueue(name)
	}
	for name := range snapshot.Invalid {
		enqueue(name)
	}
	for _, name := range snapshot.Removed {
		enqueue(name)
	}
}

// materialize runs one complete attempt for desired. Git materialization and
// authorization happen outside any transaction; the resulting complete
// authorization is then persisted atomically with the node's materialized
// progress. Only that commit advances the in-memory applied snapshot. A retry
// whose earlier commit outcome was ambiguous first confirms the persisted
// authorization: when it already covers the requested revision and the local tree
// still matches, it is published without another fetch that could keep failing.
func (c *Coordinator) materialize(
	ctx context.Context,
	previous Snapshot,
	desired state.DeploymentDesired,
) (Snapshot, error) {
	if c.cfg.State == nil {
		return Snapshot{}, errors.New("deployment: state is not configured")
	}
	commit, err := state.NormalizeDeploymentCommit(desired.Commit)
	if err != nil {
		return Snapshot{}, err
	}
	desired.Commit = commit
	resolved, err := c.resolveUncertainApplied(ctx, &previous)
	if err != nil {
		return Snapshot{}, err
	}
	if resolved && previous.Authoritative &&
		previous.Generation == desired.Generation &&
		strings.EqualFold(previous.Commit, desired.Commit) &&
		c.authorizationIntact(ctx, previous) {
		// The ambiguous commit is confirmed to have persisted this exact
		// generation, and the local tree still matches its authorization. Publishing
		// it is the safe success path: no Git work is needed, and the reconciler and
		// its enqueue set must advance with the confirmed authorization instead of
		// waiting for a later fetch that may keep failing.
		c.setApplied(previous)
		if err := c.cfg.State.PruneDeploymentAppProgress(
			ctx, c.cfg.NodeID, previous.Generation); err != nil && c.cfg.Logger != nil {
			c.cfg.Logger.Warn("Deployment: prune old app progress failed", "error", err)
		}

		if c.cfg.Logger != nil {
			c.cfg.Logger.InfoContext(ctx,
				"Deployment: confirmed materialized revision after an ambiguous commit",
				"generation", previous.Generation,
				"commit", previous.Commit,
				"apps", len(previous.Fingerprints),
				"removed", len(previous.Removed))
		}

		return previous, nil
	}
	observed := state.DeploymentNodeProgress{
		NodeID:            c.cfg.NodeID,
		Generation:        desired.Generation,
		AppliedGeneration: previous.Generation,
		Instance:          c.cfg.InstanceID,
		Phase:             state.DeploymentNodeMaterializing,
		Commit:            desired.Commit,
		AppliedCommit:     previous.Commit,
	}
	acquired, err := c.cfg.State.AcquireDeploymentNode(
		ctx, c.cfg.NodeID, c.cfg.InstanceID, desired.Generation, observed,
	)
	if err != nil {
		if errors.Is(err, state.ErrDeploymentNodeSuperseded) {
			return Snapshot{}, err
		}
		return Snapshot{}, fmt.Errorf("deployment: acquire node progress: %w", err)
	}
	c.mu.Lock()
	c.incarnation = acquired.Incarnation
	// Adopt the new process incarnation for APP progress immediately. The
	// applied generation stays the previous one until materialization succeeds,
	// so reconciler reports for the still-serving generation remain valid and
	// attributable to this process, while a stale incarnation is fenced out.
	c.appliedIncarnation = acquired.Incarnation
	c.mu.Unlock()

	fail := func(cause error) (Snapshot, error) {
		// The failed attempt must not destroy the previous generation's
		// authorization, and a later failure report must never roll back an
		// applied generation a concurrent (or ambiguously completed) commit
		// already persisted. RecordDeploymentNodeFailure therefore updates only
		// the observed phase/commit/error.
		progressCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		landed, werr := c.cfg.State.RecordDeploymentNodeFailure(
			progressCtx, c.cfg.NodeID, desired.Generation,
			acquired.Incarnation, desired.Commit, cause.Error(),
		)
		if werr != nil && c.cfg.Logger != nil {
			c.cfg.Logger.Warn("Deployment: record node failure failed", "error", werr)
		} else if !landed && c.cfg.Logger != nil {
			c.cfg.Logger.Debug("Deployment: node failure report was fenced out")
		}

		c.setAppliedIncarnation(acquired.Incarnation)

		return Snapshot{}, cause
	}

	local, err := git.LoadConfig(c.cfg.GitConfigPath)
	if err != nil {
		return fail(fmt.Errorf("deployment: load local git source: %w", err))
	}
	if mismatch := deploymentSourceMismatch(desired, local); mismatch != "" {
		return fail(fmt.Errorf(
			"deployment: local git source does not match the shared revision: %s", mismatch,
		))
	}

	attemptCtx, cancel := context.WithTimeout(ctx, materializeTimeout)
	defer cancel()
	opts := git.NewSyncOptions()
	opts.ConfigPath = c.cfg.GitConfigPath
	opts.CheckoutDir = c.cfg.GitCheckoutDir
	opts.AppsDir = c.cfg.AppsDir
	opts.SSHDir = c.cfg.GitSSHDir
	opts.Log = c.cfg.Logger
	if err := git.SyncCommitFromConfig(attemptCtx, opts, local, desired.Commit); err != nil {
		return fail(fmt.Errorf("deployment: materialize commit %s: %w", desired.Commit, err))
	}

	fingerprints, templateIdentities, invalidTemplates, appErrors, authErr := c.authorize(attemptCtx)
	if authErr != nil {
		// A root/global loader failure is NOT a valid empty app set. Git may have
		// updated /apps but discovery could not read it; keep the previous applied
		// generation authorized and retry the desired generation instead of
		// recording materialized/pruning prior progress.
		return fail(authErr)
	}
	previousNames := make(map[string]string, len(previous.Fingerprints)+len(previous.Invalid))
	for name := range previous.Fingerprints {
		previousNames[name] = previous.Fingerprints[name]
	}
	// A previously invalid app is also part of the previous desired set: if it is
	// absent now it is a genuine removal, not an invisible stale row.
	for name := range previous.Invalid {
		previousNames[name] = ""
	}
	removed := missingApps(previousNames, fingerprints, appErrors)

	var confirmed *Snapshot
	committed, commitErr := c.cfg.State.CommitDeploymentMaterialization(ctx,
		state.DeploymentMaterialization{
			NodeID:      c.cfg.NodeID,
			Generation:  desired.Generation,
			Instance:    c.cfg.InstanceID,
			Incarnation: acquired.Incarnation,
			Commit:      desired.Commit,
			Apps: materializationApps(
				fingerprints, templateIdentities,
				invalidTemplates, appErrors, removed,
			),
		})
	if commitErr != nil {
		// The commit may have reached PostgreSQL before the client observed the
		// failure. Resolve the outcome from persisted state instead of reporting a
		// failure that would write the previous applied generation back.
		landed, resolveErr := c.resolveMaterializationCommit(ctx, desired, acquired.Incarnation)
		if resolveErr != nil {
			// Unresolved: leave the applied authorization untouched and let the
			// retry re-read it before doing anything else.
			c.setAppliedUncertain(true)
			return Snapshot{}, fmt.Errorf(
				"deployment: commit materialization (outcome unknown): %w", commitErr)
		}
		if !landed {
			return fail(fmt.Errorf("deployment: commit materialization: %w", commitErr))
		}
		// The commit landed, but this process never saw its acknowledgement.
		// Adopt the authorization persisted by that commit rather than assuming the
		// in-memory set was the one committed.
		persisted, loadErr := c.loadApplied(ctx)
		if loadErr != nil || !persisted.Authoritative ||
			persisted.Generation != desired.Generation {
			c.setAppliedUncertain(true)
			return Snapshot{}, fmt.Errorf(
				"deployment: materialization commit cannot be confirmed: %w", commitErr)
		}
		committed = true
		confirmed = &persisted
	}
	if !committed {
		// The generation/incarnation fence no longer matches this process, so
		// nothing was written and no failure progress may be recorded.
		return Snapshot{}, fmt.Errorf(
			"deployment: materialization of generation %d was superseded", desired.Generation)
	}
	c.setAppliedUncertain(false)
	if err := c.cfg.State.PruneDeploymentAppProgress(
		ctx, c.cfg.NodeID, desired.Generation); err != nil && c.cfg.Logger != nil {
		c.cfg.Logger.Warn("Deployment: prune old app progress failed", "error", err)
	}

	invalid := make(map[string]string, len(appErrors))
	for name, message := range appErrors {
		if name != "" {
			invalid[name] = message
		}
	}
	snapshot := Snapshot{
		Generation:         desired.Generation,
		Commit:             desired.Commit,
		Fingerprints:       fingerprints,
		TemplateIdentities: templateIdentities,
		Invalid:            invalid,
		InvalidTemplates:   invalidTemplates,
		Removed:            removed,
		Authoritative:      true,
	}
	if confirmed != nil {
		snapshot = *confirmed
	}
	c.setApplied(snapshot)
	c.setAppliedIncarnation(acquired.Incarnation)
	if c.cfg.Logger != nil {
		c.cfg.Logger.InfoContext(ctx, "Deployment: revision materialized",
			"generation", desired.Generation,
			"commit", desired.Commit,
			"apps", len(snapshot.Fingerprints),
			"removed", len(snapshot.Removed))
	}
	return snapshot, nil
}

// resolveUncertainApplied re-reads the persisted applied authorization after a
// materialization commit whose outcome could not be resolved, updating previous
// in place. Persisted applied progress is written only by a complete atomic
// commit, so the re-read state is the only safe previous authorization to reuse —
// including a same-generation replacement that repaired the generation's row set.
// It reports whether the persisted authorization was re-read.
func (c *Coordinator) resolveUncertainApplied(
	ctx context.Context,
	previous *Snapshot,
) (bool, error) {
	c.mu.Lock()
	uncertain := c.appliedUncertain
	c.mu.Unlock()
	if !uncertain {
		return false, nil
	}
	persisted, err := c.loadApplied(ctx)
	if err != nil {
		return false, fmt.Errorf(
			"deployment: resolve uncertain materialization commit: %w", err)
	}
	// The persisted applied generation is authoritative even when it EQUALS the
	// in-memory generation: a landed same-generation re-materialization replaces
	// that generation's complete authorization, while the in-memory snapshot may
	// still hold the older set (for example a legacy partial authorization the
	// atomic replacement repaired). Keeping the stale set would let activation
	// reporting and the next removal computation use an authorization a restart
	// would never reconstruct.
	if persisted.Authoritative && persisted.Generation >= previous.Generation {
		*previous = persisted
		c.setApplied(persisted)
	}
	c.setAppliedUncertain(false)
	return true, nil
}

// authorizationIntact reports whether the current /apps tree still matches a
// persisted authorization exactly: every authorized app (valid or
// materialized-invalid) is present with its recorded fingerprint/template
// identity, and no extra app candidate exists. It is the narrow proof required
// before an ambiguous-commit retry may publish a confirmed authorization without
// re-materializing; anything unprovable falls back to the normal Git path.
func (c *Coordinator) authorizationIntact(ctx context.Context, snapshot Snapshot) bool {
	fingerprints, templateIdentities, invalidTemplates, appErrors, err := c.authorize(ctx)
	if err != nil {
		return false
	}
	current := make(map[string]struct{}, len(fingerprints)+len(appErrors))
	for name, fingerprint := range fingerprints {
		expected, authorized := snapshot.Fingerprints[name]
		if !authorized || expected != fingerprint {
			return false
		}
		if expectedIdentity := snapshot.TemplateIdentities[name]; expectedIdentity != "" &&
			templateIdentities[name] != expectedIdentity {
			return false
		}
		current[name] = struct{}{}
	}
	for name := range appErrors {
		if name == "" {
			continue
		}
		if _, invalid := snapshot.Invalid[name]; !invalid {
			return false
		}
		if expectedIdentity := snapshot.InvalidTemplates[name]; expectedIdentity != "" &&
			invalidTemplates[name] != expectedIdentity {
			return false
		}
		current[name] = struct{}{}
	}
	if len(current) != len(snapshot.Fingerprints)+len(snapshot.Invalid) {
		return false
	}
	for name := range snapshot.Fingerprints {
		if _, ok := current[name]; !ok {
			return false
		}
	}
	for name := range snapshot.Invalid {
		if _, ok := current[name]; !ok {
			return false
		}
	}
	return true
}

// resolveMaterializationCommit reads the node's persisted progress after a commit
// whose outcome was unknown. The authorization and the applied generation are
// written in one transaction, so a matching applied generation, commit, and
// incarnation prove that the complete row set landed. A read error leaves the
// outcome unresolved for the retry instead of assuming either result.
func (c *Coordinator) resolveMaterializationCommit(
	ctx context.Context,
	desired state.DeploymentDesired,
	incarnation int64,
) (bool, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	node, found, err := c.cfg.State.ReadDeploymentNode(readCtx, c.cfg.NodeID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	return node.Generation == desired.Generation &&
		node.Incarnation == incarnation &&
		node.AppliedGeneration == desired.Generation &&
		node.Phase == state.DeploymentNodeMaterialized &&
		strings.EqualFold(node.AppliedCommit, desired.Commit), nil
}

// materializationApps assembles the COMPLETE authorization persisted atomically
// with the materialized node progress: valid apps as pending with their
// materialized fingerprint and template identity, materialized-but-invalid apps
// as failed with their diagnostic, and genuinely removed apps as removals.
func materializationApps(
	fingerprints, templateIdentities, invalidTemplates, appErrors map[string]string,
	removed []string,
) []state.DeploymentAppAuthorization {
	apps := make([]state.DeploymentAppAuthorization, 0,
		len(fingerprints)+len(appErrors)+len(removed))
	for name, fingerprint := range fingerprints {
		apps = append(apps, state.DeploymentAppAuthorization{
			App:              name,
			Phase:            state.DeploymentAppPending,
			Fingerprint:      fingerprint,
			TemplateIdentity: templateIdentities[name],
		})
	}
	for name, message := range appErrors {
		if name == "" {
			continue
		}
		apps = append(apps, state.DeploymentAppAuthorization{
			App:              name,
			Phase:            state.DeploymentAppFailed,
			TemplateIdentity: invalidTemplates[name],
			Error:            message,
		})
	}
	for _, name := range removed {
		apps = append(apps, state.DeploymentAppAuthorization{
			App:   name,
			Phase: state.DeploymentAppRemoved,
		})
	}
	return apps
}

func deploymentSourceMismatch(desired state.DeploymentDesired, local git.Config) string {
	switch {
	case !strings.EqualFold(strings.TrimSpace(desired.Repository), strings.TrimSpace(local.Repository)):
		return fmt.Sprintf(
			"repository is %q locally, %q in the shared revision", local.Repository, desired.Repository,
		)
	case strings.TrimSpace(desired.Ref) != strings.TrimSpace(local.Ref):
		return fmt.Sprintf(
			"ref is %q locally, %q in the shared revision", local.Ref, desired.Ref,
		)
	case strings.TrimSpace(desired.Path) != strings.TrimSpace(local.Path):
		return fmt.Sprintf(
			"path is %q locally, %q in the shared revision", local.Path, desired.Path,
		)
	}
	return ""
}

// authorize computes the app-name -> fingerprint set for the materialized /apps
// tree. Invalid app candidates are reported per app and excluded from the
// authorized set; they are never activated.
func (c *Coordinator) authorize(
	ctx context.Context,
) (fingerprints, templateIdentities, invalidTemplates, appErrors map[string]string, err error) {
	fingerprints = map[string]string{}
	templateIdentities = map[string]string{}
	invalidTemplates = map[string]string{}
	appErrors = map[string]string{}
	apps, issues, err := c.cfg.LoadApps(c.cfg.AppsDir)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"deployment: load materialized apps from %s: %w", c.cfg.AppsDir, err)
	}
	for _, issue := range issues {
		appErrors[issue.Name] = issue.Err.Error()
		if issue.Identity != "" {
			invalidTemplates[issue.Name] = issue.Identity
		}
	}
	for _, fn := range apps {
		if err := ctx.Err(); err != nil {
			appErrors[fn.Name] = err.Error()
			continue
		}
		templateSource, err := app.ReadTemplateSource(c.cfg.AppsDir, fn.Name)
		if err != nil {
			appErrors[fn.Name] = err.Error()
			continue
		}
		_, fingerprint, err := app.SelectAndFingerprintApp(fn.Dir, fn.Template)
		if err != nil {
			appErrors[fn.Name] = err.Error()
			if templateSource.Identity != "" {
				invalidTemplates[fn.Name] = templateSource.Identity
			}
			continue
		}
		fingerprints[fn.Name] = fingerprint
		templateIdentities[fn.Name] = templateSource.Identity
	}
	return fingerprints, templateIdentities, invalidTemplates, appErrors, nil
}

func missingApps(previous, current, invalid map[string]string) []string {
	var removed []string
	for name := range previous {
		if _, ok := current[name]; ok {
			continue
		}
		// An app present in the new revision but invalid is a DESIRED invalid app,
		// not a removal. Its previous active generation may still be retained.
		if _, ok := invalid[name]; ok {
			continue
		}
		removed = append(removed, name)
	}
	return removed
}

// loadApplied reconstructs the last materialized authorization from persisted
// node/app progress. It deliberately uses the node's APPLIED generation, not its
// observed target, so a failed newer target never erases the previous
// generation's authorization.
func (c *Coordinator) loadApplied(ctx context.Context) (Snapshot, error) {
	node, found, err := c.cfg.State.ReadDeploymentNode(ctx, c.cfg.NodeID)
	if err != nil {
		return Snapshot{}, err
	}
	if !found || node.AppliedGeneration <= 0 {
		return Snapshot{}, nil
	}
	rows, err := c.cfg.State.ReadDeploymentAppProgress(ctx, c.cfg.NodeID, node.AppliedGeneration)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		Generation:         node.AppliedGeneration,
		Commit:             node.AppliedCommit,
		Fingerprints:       map[string]string{},
		TemplateIdentities: map[string]string{},
		Invalid:            map[string]string{},
		InvalidTemplates:   map[string]string{},
	}
	// applied_generation advances only through the atomic materialization commit,
	// which writes the complete authorization in the same transaction, so any
	// applied generation reconstructs an authoritative set — including a
	// legitimately EMPTY deployment, which stores no app rows.
	snapshot.Authoritative = true
	for _, row := range rows {
		switch row.Phase {
		case state.DeploymentAppRemoved:
			snapshot.Removed = append(snapshot.Removed, row.App)
		case state.DeploymentAppFailed:
			if row.Fingerprint != "" {
				// A valid materialized app whose build/service preparation failed.
				// Keep it authorized so startup retries it and installs the normal
				// unavailable/matched placeholder instead of ACKing events unmatched.
				snapshot.Fingerprints[row.App] = row.Fingerprint
				snapshot.TemplateIdentities[row.App] = row.TemplateIdentity
				continue
			}
			// Materialized-but-unloadable app: never authorized to activate, but
			// its startup issue is genuine (not local drift) and is retained.
			snapshot.Invalid[row.App] = row.Error
			if row.TemplateIdentity != "" {
				snapshot.InvalidTemplates[row.App] = row.TemplateIdentity
			}
		default:
			if row.Fingerprint == "" {
				// An authorized app always records the materialized fingerprint.
				// Treating an empty one (for example a drifted local-only app) as
				// authorization would make the reconciler gate compare local
				// content against "" and accept anything after a restart.
				continue
			}
			snapshot.Fingerprints[row.App] = row.Fingerprint
			snapshot.TemplateIdentities[row.App] = row.TemplateIdentity
		}
	}
	return snapshot, nil
}

// checkDrift independently verifies local /apps against the authorized
// generation. It runs on the coordinator loop, so it is serialized with
// materialization and never observes a tree mid-sync.
//
// The reconciler's periodic pass is the primary verifier: it re-reads every app
// and records each successful content verification under the authorization epoch
// it was made against (VerifiedSince). This backstop audits the whole authorized
// set on every drift tick — valid apps, desired-invalid apps, their materialized
// template identities, and every local entry that is not authorized — and hashes
// a valid authorized app only when the reconciler has not verified it in the
// current authorization since this backstop's previous check. The scan boundary
// is therefore explicit: each check covers the window since the previous check,
// so one verification can never be trusted by two checks and a long build in the
// reconciler's pump cannot defer detection indefinitely. Every authorization
// update advances the reconciler's epoch and invalidates earlier verifications,
// so a rematerialization is never masked by pre-update timestamps.
//
// Every app is still verified at least once per window: by the reconciler if it
// covers the app, otherwise by this check. A nil ledger performs the full check
// (tests and embedders).
//
// This path must stay independent of the reconciler's pump: do not move it onto
// the reconcile queue, where a build backlog can starve it. The coordinator loop
// itself is only serialized with its own materialization, which is the repair.
func (c *Coordinator) checkDrift(ctx context.Context) {
	c.mu.Lock()
	applied := c.applied
	since := c.driftSince
	verifiedSince := c.verifiedSince
	c.mu.Unlock()
	// Snapshot the ledger BEFORE advancing the window boundary. A verification
	// that lands while the snapshot is being taken is then either included here
	// (and hashed again by nobody, because the new boundary is at or after it) or
	// missed here and hashed conservatively now; it can never be trusted by two
	// checks.
	var verified map[string]struct{}
	if verifiedSince != nil {
		verified = verifiedSince(since)
	}
	c.mu.Lock()
	c.driftSince = time.Now()
	c.mu.Unlock()
	if !applied.Authoritative {
		return
	}
	apps, issues, err := c.cfg.LoadApps(c.cfg.AppsDir)
	if err != nil {
		// A transiently unreadable /apps is not evidence of drift; do not churn a
		// rematerialization attempt from the drift backstop. The convergence loop
		// still retries the desired generation with its own failure handling.
		if c.cfg.Logger != nil {
			c.cfg.Logger.WarnContext(ctx, "Deployment: drift check could not read /apps", "error", err)
		}
		return
	}
	present := make(map[string]bool, len(apps)+len(issues))
	for _, issue := range issues {
		present[issue.Name] = true
		if _, authorized := applied.Fingerprints[issue.Name]; authorized {
			// An authorized app that no longer loads at all is drift.
			c.Kick()
			return
		}
		if _, invalid := applied.Invalid[issue.Name]; !invalid {
			// A local entry that is neither authorized nor desired-invalid is
			// unauthorized content, whether or not the loader could read it. This
			// is the invalid counterpart of a local-only app.
			c.Kick()
			return
		}
		// Desired-invalid: a deterministic local identity is unverifiable drift
		// when the materialized revision never recorded one for this app — there is
		// nothing to compare against, and the reconciler's parse-error path cannot
		// distinguish a locally changed invalid template from the materialized one
		// without an expected identity. Request re-authorization of the exact shared
		// SHA instead of trusting the bytes. When both identities are known, only
		// changed bytes are drift. An empty local identity is an operational
		// failure (permissions, I/O) that says nothing about the bytes, so it stays
		// with the reconciler's retry.
		expected := applied.InvalidTemplates[issue.Name]
		if issue.Identity != "" && (expected == "" || issue.Identity != expected) {
			c.Kick()
			return
		}
	}
	for _, fn := range apps {
		if ctx.Err() != nil {
			return
		}
		present[fn.Name] = true
		if expected, authorized := applied.Fingerprints[fn.Name]; authorized {
			// The content fingerprint deliberately excludes runtime-only service
			// configuration (resources, tmpfs, healthcheck) so those edits do not
			// rebuild an image. The raw template identity is the authorization
			// identity for ALL template bytes, including healthcheck, so compare it
			// before the cached-verification shortcut; otherwise a healthcheck-only
			// local edit could be invisible to this drift backstop until the next
			// reconciler pass.
			if expectedTemplate := applied.TemplateIdentities[fn.Name]; expectedTemplate != "" {
				source, sourceErr := app.ReadTemplateSource(c.cfg.AppsDir, fn.Name)
				if sourceErr != nil {
					// Only a deterministic read outcome (the authorized app or
					// its template is gone, or its path is no longer a valid app
					// directory) is evidence of drift. A transient read failure —
					// permissions, I/O, a read racing a replacement — says nothing
					// about the bytes, exactly like the LoadApps failure above, so
					// it is left to the reconciler's retry instead of churning a
					// rematerialization. The raw template identity is still
					// compared whenever the bytes WERE read, which is what keeps a
					// healthcheck-only local edit visible here.
					if !templateSourceDrift(sourceErr) {
						if c.cfg.Logger != nil {
							c.cfg.Logger.WarnContext(ctx,
								"Deployment: drift check could not read an app template",
								"app", fn.Name, "error", sourceErr)
						}
						continue
					}
					c.Kick()
					return
				}
				if source.Identity != expectedTemplate {
					c.Kick()
					return
				}
			}
			if _, done := verified[fn.Name]; done {
				// The reconciler verified this app's content under the current
				// authorization since the previous check; it either matched or
				// reported drift. Hashing it again would duplicate that work.
				continue
			}
			_, fingerprint, err := app.SelectAndFingerprintApp(fn.Dir, fn.Template)
			if err != nil || fingerprint != expected {
				// An authorized app whose content can no longer be read or differs
				// from the authorization is drift.
				c.Kick()
				return
			}
			continue
		}
		if _, invalid := applied.Invalid[fn.Name]; invalid {
			if _, done := verified[fn.Name]; done {
				// The reconciler read this app under the current authorization since
				// the previous check and handled it: a changed template identity is
				// reported as drift, and a non-activatable app requests
				// re-authorization. Re-checking would duplicate that work.
				continue
			}
			// The local template identity is the only decision input. A matching
			// identity is the materialized content (a restore would be a no-op), so
			// the remaining fingerprint retry belongs to the reconciler. A changed
			// identity is drift, and an identity that cannot be compared because the
			// materialization recorded none is unverifiable drift: either way the
			// exact shared revision must be restored and re-authorized. The source
			// content hash would not change that decision.
			source, err := app.ReadTemplateSource(c.cfg.AppsDir, fn.Name)
			if err != nil || source.Identity == "" {
				// An operational failure says nothing about the bytes; it stays with
				// the reconciler's retry.
				continue
			}
			if expected := applied.InvalidTemplates[fn.Name]; expected == "" || source.Identity != expected {
				c.Kick()
				return
			}
			continue
		}
		// A name present outside the authorized generation is drift regardless of
		// whether its source can be hashed: the authorized tree must be restored.
		c.Kick()
		return
	}
	for name := range applied.Fingerprints {
		if !present[name] {
			c.Kick()
			return
		}
	}
	for name := range applied.Invalid {
		if !present[name] {
			c.Kick()
			return
		}
	}
}

// templateSourceDrift reports whether a ReadTemplateSource error is deterministic
// evidence that an authorized app no longer matches its materialized generation:
// the app directory or its template is gone, or the path is no longer a valid
// app directory. Any other failure (permissions, I/O, a read that raced a
// replacement) is transient and says nothing about the bytes, so the drift
// backstop leaves it to the reconciler's retry instead of requesting a
// rematerialization — the same classification already applied to a failed
// LoadApps read.
func templateSourceDrift(err error) bool {
	return errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, app.ErrNotReady) ||
		errors.Is(err, app.ErrInvalidPath)
}

func (c *Coordinator) setApplied(snapshot Snapshot) {
	c.mu.Lock()
	c.applied = snapshot
	c.mu.Unlock()
}

func (c *Coordinator) setAppliedIncarnation(incarnation int64) {
	c.mu.Lock()
	c.appliedIncarnation = incarnation
	c.mu.Unlock()
}

func (c *Coordinator) setAppliedUncertain(uncertain bool) {
	c.mu.Lock()
	c.appliedUncertain = uncertain
	c.mu.Unlock()
}
