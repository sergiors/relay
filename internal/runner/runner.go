package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/app"
	eventmatch "relay/internal/event"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/secrets"
	"relay/internal/stream"
)

// DefaultMaxConcurrency is the runner's default global cap on concurrently
// executing invocations in a single worker (MAX_CONCURRENCY). A zero or
// negative value passed to SetMaxConcurrency falls back to this. The runner
// deliberately owns this constant (it cannot import config; worker wires the
// cfg value).
const DefaultMaxConcurrency = 8

// slotWaitTimeout bounds how long Handle waits for a free concurrency slot
// (global or per-app) before giving up. It is deliberately well below the
// stream layer's default MinPendingIdle reclaim threshold (1m): if slots never
// free within the wait, Handle returns ErrInvocationNotEligible and the message
// stays pending, so reclaim replays it later — and locally buffered events never
// sit long enough to defeat the reclaim pacing (see the README's
// "Concurrency and backpressure" note).
const slotWaitTimeout = 30 * time.Second

// Manual-invocation sentinel errors. They let the worker socket map a manual
// invocation failure onto a stable wire code without inspecting error strings
// (see internal/worker/socket.go). They are returned (wrapped) by
// Runner.InvokeApp.
var (
	// ErrAppNotFound reports a manual invocation for an app that is
	// absent from the current registry (never loaded, or already removed).
	ErrAppNotFound = errors.New("app not found")
	// ErrAppUnavailable reports a manual invocation for an app that is
	// registered but not runnable (its image could not be built at
	// startup/reconcile, so it has no Prepared handle).
	ErrAppUnavailable = errors.New("app unavailable")
	// ErrHandlerNotFound reports a DLQ replay whose handler is no longer present
	// in the app's CURRENT template (an intentional configuration change).
	// The worker socket maps it onto a stable wire code and the CLI keeps the
	// DLQ entry.
	ErrHandlerNotFound = errors.New("handler not found")
)

// invocationOutcome classifies what one delivery round did for a single matched
// invocation. Handle's rule loop returns one of these per invocation so it can
// aggregate the per-invocation outcomes AFTER the whole loop (see Handle's doc
// comment), rather than fail-fast on the first failure. outcomeRetryable and
// outcomeExhausted are produced by recordFailure; the skip outcomes are produced
// by the TryStart/slot paths.
type invocationOutcome int

const (
	// outcomeExecuted means the handler ran and returned success (the invocation
	// was marked complete).
	outcomeExecuted invocationOutcome = iota
	// outcomeTerminalSkip means the invocation was skipped because it is already
	// terminal: complete or exhausted (never eligible again). Exhausted skips are
	// distinguishable from complete skips by the existing marker's attempt
	// count (>0).
	outcomeTerminalSkip
	// outcomePendingSkip means the invocation was skipped because it is protected
	// (running or waiting out a retry backoff) or its concurrency slot timed out:
	// it is unresolved, so the message must stay pending (never ACKed).
	outcomePendingSkip
	// outcomeRetryable means the invocation failed a retryable attempt: a retry
	// backoff was scheduled and the message must stay pending for a later retry.
	outcomeRetryable
	// outcomeExhausted means the invocation failed its last attempt: it was
	// marked exhausted (terminal) and, if every other matched invocation is also
	// terminal, the message routes to the DLQ.
	outcomeExhausted
)

// Executor is the subset of the runtime Manager that invocations need. It is a
// small interface so Handle and PreparedApp construction can be exercised
// in tests without a Docker daemon; the concrete *runtime.Manager satisfies it.
type Executor interface {
	Execute(
		ctx context.Context,
		prepared *runtime.Prepared,
		handler string,
		eventJSON []byte,
		extraEnv []string,
	) error
}

// Registry holds the current set of prepared apps behind a lock so swaps
// are atomic: Handle takes one snapshot per call and keeps it for the whole
// invocation, so an in-flight execution never sees a half-replaced set. It is
// exported so the reconciler can swap apps live from its own package.
//
// Alongside the app slice the registry owns an immutable candidate index
// (eventmatch.RuleIndex) per published app, rebuilt in the SAME locked
// mutation as the slice. The two are published together, so a snapshot can never
// observe an app set from one generation and an index from another. The
// index is a false-positive-only prefilter: it only narrows which rules are
// exact-tested, and the exact matcher remains the authority on every candidate.
type Registry struct {
	mu  sync.RWMutex
	fns []*PreparedApp
	// ruleIdx maps each published app to its immutable candidate index,
	// built from that app's CURRENT template events. Both available and
	// unavailable entries are indexed: an unavailable app must still be
	// classified as matching (it is never DLQ'd for unavailability alone). A
	// hand-built entry with a nil template has no entry and falls back to a full
	// exact scan. The map is replaced wholesale on every mutation (never mutated
	// in place), so a snapshot's shared reference stays immutable.
	ruleIdx map[*PreparedApp]*eventmatch.RuleIndex
	// pending holds, per app name, the desired template of a valid generation
	// that is being prepared but is not yet runnable: a brand-new app's first
	// build, or an available active generation being rebuilt to a new one. A
	// pending entry is immutable and non-runnable (it is built with
	// NewUnavailable: no image, no executor, no publication lease). It exists
	// ONLY to keep event matching live during preparation: a delivery matching a
	// pending rule is matched-but-unavailable and stays pending instead of being
	// acknowledged as unmatched. Pending entries never execute, claim a
	// TryStart, exhaust, ACK, or DLQ. The slice is replaced/rebuilt under the
	// registry lock together with pendingIdx, so a snapshot can never observe a
	// pending set from one generation and an index from another.
	pending []*PreparedApp
	// pendingIdx maps each pending entry to its immutable candidate index, built
	// from that entry's desired events in the SAME locked mutation as pending.
	pendingIdx map[*PreparedApp]*eventmatch.RuleIndex
}

// buildRuleIndex derives the per-app candidate index from the given prepared
// set. It is called while the registry lock is held, immediately after the app
// slice is updated, so the index and the slice share one generation. The same
// builder serves both the active set and the pending desired set (each rebuilt
// under the lock in the SAME mutation as its slice), so a snapshot can never
// observe a slice from one generation and an index from another. Entries whose
// template is nil (only reachable from a hand-built value, never the loader) are
// omitted; matching falls back to a full exact scan for them.
func buildRuleIndex(fns []*PreparedApp) map[*PreparedApp]*eventmatch.RuleIndex {
	if len(fns) == 0 {
		return nil
	}
	out := make(map[*PreparedApp]*eventmatch.RuleIndex, len(fns))
	for _, pf := range fns {
		if pf == nil || pf.fn.Template == nil {
			continue
		}
		out[pf] = eventmatch.NewRuleIndex(pf.fn.Template.Events)
	}
	return out
}

// pendingIndexEntry finds the pending desired entry for name in the slice,
// returning its index or -1. Callers hold the registry lock.
func pendingIndexEntry(pending []*PreparedApp, name string) int {
	for i, pf := range pending {
		if pf != nil && pf.fn.Name == name {
			return i
		}
	}
	return -1
}

func (r *Registry) snapshot() []*PreparedApp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Copy the slice header only; the underlying elements are immutable once
	// published, so in-flight Handles keep using the snapshot even if Swap runs.
	return append([]*PreparedApp(nil), r.fns...)
}

// pinnedSnapshot is a snapshot of the registry whose published images are
// individually pinned by a shared publication lease acquired WHILE the registry
// lock protected the current entry. The pins are held until release is called,
// so an image published at snapshot time cannot be removed out from under the
// snapshot even if the entry is superseded immediately after. The pins are
// SHARES of each entry's publication lease (see PreparedApp.lease), so
// admitting them is always allowed — even while the image is retiring — because
// the work was admitted before retirement.
type pinnedSnapshot struct {
	fns  []*PreparedApp
	pins map[*PreparedApp]*runtime.ImageLease
	// ruleIdx is the candidate index generation published WITH fns, captured
	// under the same read lock. It is shared immutably, so one Handle consumes
	// exactly one index generation for the whole delivery.
	ruleIdx map[*PreparedApp]*eventmatch.RuleIndex
	// pending is the pending-desired generation published WITH fns. Its entries
	// are non-runnable (NewUnavailable) placeholders for valid generations being
	// prepared but not yet active. They never execute; they only contribute
	// matched-but-unavailable invocations so a delivery during preparation stays
	// pending instead of being acknowledged as unmatched.
	pending []*PreparedApp
	// pendingIdx is the candidate index generation for pending, captured under
	// the same read lock so pending and its index are coherent.
	pendingIdx map[*PreparedApp]*eventmatch.RuleIndex
}

// rulesFor returns the immutable candidate index bound to pf in this snapshot's
// generation, or nil when pf has no index (a nil-template entry), in which case
// the caller must fall back to a full exact scan.
func (s *pinnedSnapshot) rulesFor(pf *PreparedApp) *eventmatch.RuleIndex {
	if s == nil || s.ruleIdx == nil {
		return nil
	}
	return s.ruleIdx[pf]
}

// pendingRulesFor returns the immutable candidate index bound to pf in this
// snapshot's pending generation, or nil when pf has no pending index (a
// nil-template entry), in which case the caller must fall back to a full exact
// scan.
func (s *pinnedSnapshot) pendingRulesFor(pf *PreparedApp) *eventmatch.RuleIndex {
	if s == nil || s.pendingIdx == nil {
		return nil
	}
	return s.pendingIdx[pf]
}

// matchingRules returns the rules of pf matching event under THIS snapshot's
// index generation. When pf has a published candidate index the indexed matcher
// is used (anchor prefilter plus exact verification, identical to a full scan);
// otherwise — a nil template, only reachable from a hand-built value — it falls
// back to a full exact scan. Both paths are bound to the snapshot's generation,
// so every match decision in one Handle call comes from one index generation.
func (s *pinnedSnapshot) matchingRules(pf *PreparedApp, event map[string]any) []app.EventRule {
	if ix := s.rulesFor(pf); ix != nil {
		return ix.MatchingEventRules(event)
	}
	if pf == nil || pf.fn.Template == nil {
		return nil
	}
	return eventmatch.MatchingEventRules(pf.fn.Template.Events, event)
}

// matchingPendingRules is matchingRules for a pending desired entry, bound to
// the snapshot's pending index generation.
func (s *pinnedSnapshot) matchingPendingRules(pf *PreparedApp, event map[string]any) []app.EventRule {
	if ix := s.pendingRulesFor(pf); ix != nil {
		return ix.MatchingEventRules(event)
	}
	if pf == nil || pf.fn.Template == nil {
		return nil
	}
	return eventmatch.MatchingEventRules(pf.fn.Template.Events, event)
}

// pinFor returns the shared publication lease pinning pf's image for this
// snapshot, or nil (an unavailable app, a no-runtime app, or a
// hand-built test value).
func (s *pinnedSnapshot) pinFor(pf *PreparedApp) *runtime.ImageLease {
	if s == nil {
		return nil
	}
	return s.pins[pf]
}

// release drops every pin acquired for the snapshot. It is idempotent-safe per
// lease (ImageLease.Release is idempotent) and must be called exactly once when
// the snapshot's work is done.
func (s *pinnedSnapshot) release() {
	if s == nil {
		return
	}
	for _, l := range s.pins {
		l.Release()
	}
}

// snapshotPinned takes a consistent snapshot AND shares each published entry's
// image publication lease while the read lock still protects the entry, so no
// concurrent Replace can release a lease between the copy and the share. The
// caller must call the returned snapshot's release when its execution work is
// done.
func (r *Registry) snapshotPinned() *pinnedSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := &pinnedSnapshot{
		fns:        append([]*PreparedApp(nil), r.fns...),
		pins:       make(map[*PreparedApp]*runtime.ImageLease, len(r.fns)),
		ruleIdx:    r.ruleIdx,
		pending:    append([]*PreparedApp(nil), r.pending...),
		pendingIdx: r.pendingIdx,
	}
	for _, pf := range out.fns {
		if lease := pf.sharePublication(); lease != nil {
			out.pins[pf] = lease
		}
	}
	return out
}

// getByNamePinned returns the prepared app for name PLUS a shared pin of
// its published image, acquired while the registry lock protects the entry. The
// caller must release the returned lease (nil-safe) when done. It is the
// execution-path lookup (schedule/manual) so those paths hold the image pin
// through matching, slot waits, and handler completion.
func (r *Registry) getByNamePinned(name string) (*PreparedApp, *runtime.ImageLease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pf := range r.fns {
		if pf.fn.Name == name {
			return pf, pf.sharePublication()
		}
	}
	return nil, nil
}

// Set replaces the entire registry contents in one atomic step. Apps are
// kept sorted by name so iteration order (and Names) is deterministic. The
// publication lease of every superseded entry is released AFTER the swap, once
// the new set is visible, so a snapshot taken before the swap still holds the
// old entry's pin and a snapshot taken after holds the new entry's.
func (r *Registry) Set(fns []*PreparedApp) {
	r.mu.Lock()
	old := r.fns
	r.fns = append([]*PreparedApp(nil), fns...)
	sortFn(r.fns)
	// Rebuild the candidate index in the same locked step as the slice, so a
	// snapshot either sees both from the old generation or both from the new one.
	r.ruleIdx = buildRuleIndex(r.fns)
	// A wholesale replacement is the initial-population seam (construction and
	// tests), where no reconcile is preparing a generation: reset any pending
	// desired entries so a snapshot can never observe a stale pending set beside
	// a freshly Set active set.
	r.pending = nil
	r.pendingIdx = nil
	r.mu.Unlock()
	releaseSuperseded(old, fns)
}

// Replace swaps the entry for name, adding it if absent. A nil pf removes the
// entry (used when an app directory disappears). The slice stays name-sorted.
// The superseded entry's publication lease is released AFTER the swap, so any
// snapshot that pinned it before the swap keeps the image admitted until that
// snapshot's work drains.
//
// Replace also ATOMICALLY clears any pending desired entry for name: publishing
// the runnable generation (success) and removing the app both supersede the
// in-preparation desired view, so a reader never sees a successful new active
// snapshot beside its now-obsolete pending rules.
func (r *Registry) Replace(name string, pf *PreparedApp) {
	r.mu.Lock()
	var superseded *PreparedApp
	replaced := false
	for i, cur := range r.fns {
		if cur.fn.Name == name {
			superseded = cur
			if pf == nil {
				r.fns = append(r.fns[:i], r.fns[i+1:]...)
			} else {
				r.fns[i] = pf
			}
			sortFn(r.fns)
			replaced = true
			break
		}
	}
	if !replaced && pf != nil {
		r.fns = append(r.fns, pf)
		sortFn(r.fns)
	}
	// Rebuild the candidate index in the same locked step as the slice. A nil pf
	// (removal) drops the entry from the index; an invalid/absent template is
	// omitted and falls back to a full exact scan. Rebuilding wholesale (rather
	// than mutating the previous map) keeps every already-published snapshot's
	// shared index immutable.
	r.ruleIdx = buildRuleIndex(r.fns)
	// Clear the pending desired entry in the SAME locked step (the removal above
	// handles a pending-only name that had no active entry).
	if i := pendingIndexEntry(r.pending, name); i >= 0 {
		r.pending = append(r.pending[:i], r.pending[i+1:]...)
		r.pendingIdx = buildRuleIndex(r.pending)
	}
	r.mu.Unlock()
	// An app replacement supersedes only the old entry; an add/remove
	// supersedes only the removed entry.
	if superseded != nil && superseded != pf {
		superseded.ReleasePublication()
	}
}

// SetPending installs (or replaces) the pending desired entry for name: a valid
// generation's event rules that is being prepared but is not yet runnable. The
// entry is a non-runnable NewUnavailable placeholder that carries the desired
// rules for MATCHING only — no image, executor, or publication lease — so a
// delivery matching a pending rule is matched-but-unavailable and stays pending
// instead of being acknowledged as unmatched. Pending entries never execute,
// claim a TryStart, exhaust, ACK, or DLQ. The pending slice and its candidate
// index are rebuilt in the SAME locked step, so no snapshot can observe a
// partial pending generation.
func (r *Registry) SetPending(name string, fn app.App) {
	pf := NewUnavailable(fn)
	r.mu.Lock()
	if i := pendingIndexEntry(r.pending, name); i >= 0 {
		r.pending[i] = pf
	} else {
		r.pending = append(r.pending, pf)
		sortFn(r.pending)
	}
	r.pendingIdx = buildRuleIndex(r.pending)
	r.mu.Unlock()
}

// ClearPending drops the pending desired entry for name, if any. It is used when
// a reconcile decides no image transition is needed (unchanged, resource-only),
// when the desired definition is invalid, and on removal — so a stale desired
// rule never keeps gating events.
func (r *Registry) ClearPending(name string) {
	r.mu.Lock()
	if i := pendingIndexEntry(r.pending, name); i >= 0 {
		r.pending = append(r.pending[:i], r.pending[i+1:]...)
		r.pendingIdx = buildRuleIndex(r.pending)
	}
	r.mu.Unlock()
}

// PendingNames returns a snapshot of the names that currently have a pending
// desired entry (a valid generation being prepared but not yet runnable). It is
// the removal-sweep companion to Names: a pending-only name is not in Names, so
// the reconciler must enumerate it too to detect a vanished directory.
func (r *Registry) PendingNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.pending))
	for _, pf := range r.pending {
		out = append(out, pf.fn.Name)
	}
	return out
}

// HasPending reports whether name currently has a pending desired entry.
func (r *Registry) HasPending(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return pendingIndexEntry(r.pending, name) >= 0
}

// releaseSuperseded releases the publication leases of the old entries that are
// not present in the new set. An entry kept by pointer (unchanged) is not
// released; a replaced entry is released by Replace directly.
func releaseSuperseded(old, next []*PreparedApp) {
	if len(old) == 0 {
		return
	}
	keep := make(map[*PreparedApp]bool, len(next))
	for _, pf := range next {
		keep[pf] = true
	}
	for _, pf := range old {
		if !keep[pf] {
			pf.ReleasePublication()
		}
	}
}

// sortFn orders the registry by app name so Names() and iteration are
// deterministic regardless of the order apps were discovered or swapped in.
func sortFn(fns []*PreparedApp) {
	sort.Slice(fns, func(i, j int) bool { return fns[i].fn.Name < fns[j].fn.Name })
}

// GetByName returns the prepared app for the given name, or nil if absent,
// without disturbing the running snapshot.
func (r *Registry) GetByName(name string) *PreparedApp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pf := range r.fns {
		if pf.fn.Name == name {
			return pf
		}
	}
	return nil
}

// Names returns a snapshot of the currently registered app names, so the
// reconciler can enumerate what is active without pinning the whole slice.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.fns))
	for _, pf := range r.fns {
		out = append(out, pf.fn.Name)
	}
	return out
}

// Orchestrates the flow: for each decoded event it evaluates all loaded
// apps and, for every matching rule, executes the corresponding handler in
// a container. It contains no Redis, matcher, or docker details; execution is
// delegated to the runtime executor. The app set is an atomic snapshot so
// it can be reconciled (swapped) live without disrupting in-flight invocations.
type Runner struct {
	reg     *Registry
	log     *slog.Logger
	metrics *metrics.Registry
	// refs tracks which relay images are currently executing and which have been
	// retired but cannot be removed yet. It is what lets the runner retire
	// superseded app versions without interrupting an in-flight execution
	// (see ImageInUse / RetireImage).
	refs *imageRefCounter
	// cleaner resolves to the optional image lifecycle capability of the
	// executor, resolved once and reused. A nil cleaner (fake executors in tests)
	// makes every retirement a no-op.
	cleanerOnce sync.Once
	cleaner     ImageCleaner
	// invalidator resolves to the executor's ContainerInvalidator capability
	// the same way cleaner above is resolved (one pass over the registry).
	invalidatorOnce sync.Once
	invalidator     ContainerInvalidator
	// hostname is this worker's container-ownership identity, stamped as the
	// relay.hostname label on every execution container via RunMeta. It is the
	// same value as the Redis consumer identity. It must be set (via
	// SetHostname) before Consume begins; when unset, an empty hostname label is
	// emitted, which is diagnostic-only and harmless.
	hostname string
	// maxHandlerTimeout caps every rule's handler timeout (0 = uncapped), stored
	// as nanoseconds in an atomic.Int64. It is defense in depth: template
	// validation enforces the cap at load, and this runtime cap guarantees a
	// misconfigured or hot-swapped template can never run a handler longer than
	// the stream layer's MaxRuleTimeout. The capped value is also what TryStart
	// persists as the invocation's running deadline, so the persisted deadline
	// matches the local timer by construction.
	maxHandlerTimeout atomic.Int64
	// secrets resolves secret references to values immediately before each
	// execution. It is nil when no provider is configured (a template that
	// references secrets then fails the invocation with a clear error). It is
	// set via SetSecretProvider; the worker wires the production local provider.
	secrets secrets.Provider
	// maxConcurrency is the worker-global cap on concurrently executing
	// invocations (0 = uncapped, which SetMaxConcurrency normalizes to
	// DefaultMaxConcurrency). It is stored as an atomic so a SetMaxConcurrency
	// call (worker wires it right after construction) and concurrent Handle
	// calls read a consistent value.
	maxConcurrency atomic.Int64
	// globalSem is the global concurrency semaphore, sized to the (normalized)
	// max concurrency. It is stored as an atomic pointer so a SetMaxConcurrency
	// call (worker wires it right after construction) is race-free against
	// concurrent Handle calls reading it: readers get either the old or the new
	// semaphore, both of which are internally consistent.
	globalSem atomic.Pointer[semaphore]
	// fnSems is a mutex-protected map of per-app semaphores, keyed by
	// app name and created on demand. An app's semaphore is RESIZED by
	// replacing its pointer when the app's resolved template concurrency
	// changes (see concurrencySems), so a hot-swapped concurrency takes effect
	// without a restart. In-flight acquisitions release to the semaphore pointer
	// they captured, so replacing the map entry never strands a held slot.
	fnSemsMu sync.Mutex
	fnSems   map[string]*semaphore
	// inFlight is the current number of invocations executing concurrently in
	// this worker. It is kept in sync with the global semaphore slots and feeds
	// the in_flight_invocations gauge (set on acquire/release).
	inFlight atomic.Int64
	// slotWait is how long an invocation waits for a free concurrency slot
	// before giving up (leaving the message pending and reclaiming it later). It
	// defaults to slotWaitTimeout and is overridable by tests (package-internal
	// tests set r.slotWait directly to keep the slot-timeout tests fast).
	slotWait time.Duration
	// imageCleanupRetryDelays is the bounded backoff between retries of an image
	// removal that was skipped because a relay-owned container still references
	// it. It defaults to the production ~60s horizon (2+4+8+16+30s across 5
	// attempts) before the image is deferred to a later natural cleanup pass.
	// It is a field (not a package var) so package-internal tests can shrink it
	// directly to milliseconds without mutating shared state. It is read-only
	// after New/NewWithMetrics: tests must set it before use and never mutate it
	// while the runner is running.
	imageCleanupRetryDelays []time.Duration
	// imageCleanupAttemptDone, when non-nil, is called at the end of every async
	// image-cleanup attempt (the goroutine retryImageCleanupAttempt starts),
	// whether the attempt removed the image or deferred it. It is the test-only
	// synchronization seam: production leaves it nil, and tests install it to
	// wait for an attempt's terminal classification deterministically instead of
	// sleeping. Like imageCleanupRetryDelays it is read-only after
	// New/NewWithMetrics: tests must set it before the retirement that starts the
	// attempt. It runs on the cleanup goroutine and must not block.
	imageCleanupAttemptDone func(image string)
}

// defaultImageCleanupRetryDelays is the production backoff schedule for image
// removal retries: a ~60s horizon across 5 attempts before deferring to a later
// natural cleanup pass.
var defaultImageCleanupRetryDelays = []time.Duration{
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
}

// ImageCleaner is the subset of the runtime Manager that image retirement
// needs. It is a small interface so the runner can retire superseded app
// images without depending on the runtime package concretely; test fakes that
// do not implement it simply yield a nil cleaner (no retirement).
// ContainerInvalidator is the optional capability of the runtime executor
// that image retirement needs on top of ImageCleaner: invalidate pooled
// execution containers for a retired image so the image's
// container-reference guard clears promptly. Implementations must never
// block on an in-flight invocation: Manager.InvalidateImage discards idle
// containers immediately and retires busy ones (discarded on release) without
// waiting for them.
type ContainerInvalidator interface {
	// InvalidateImage discards any cached execution container running the
	// given image.
	InvalidateImage(image string)
}

type ImageCleaner interface {
	// RemoveImage removes a single relay-owned image, treating an already-gone
	// image as success.
	RemoveImage(ctx context.Context, image string) error
	// ImageReferencedByManagedContainer reports whether any relay-owned container
	// (event, schedule, or service) still references the image via its
	// relay.image label. It lets the runner confirm no relay-owned container still
	// references an image before removing it, and to decide (conservatively) how
	// to interpret a RemoveImage failure.
	ImageReferencedByManagedContainer(ctx context.Context, image string) (bool, error)
	// AppImageTags lists every local image tag (full references) belonging
	// to an app's repository, so the runner can retire each version with
	// in-flight safety.
	AppImageTags(ctx context.Context, name string) ([]string, error)
	// CleanupUnusedDependencies removes managed dependency images no managed
	// app image references anymore. Lifecycle-driven: call it after a
	// managed app image was successfully removed, so a dependency layer
	// whose last referencing app version just disappeared is pruned. It is
	// best-effort and must not affect the outcome of the removal that preceded
	// it.
	CleanupUnusedDependencies(ctx context.Context) (int, error)
}

// Pairs a loaded app with its prepared image and the executor used to run
// invocations. An app whose image could not be built is marked unavailable
// and skipped during execution.
type PreparedApp struct {
	fn        app.App
	prepared  *runtime.Prepared
	executor  Executor
	available bool
	// lease is the publication lease for this app's image: the admitted
	// reference transferred from Prepared when the app is published into
	// the registry. It is owned by the registry entry and released when the
	// entry is superseded or removed (after the swap), so a published image
	// stays admitted for as long as it is published and can never be removed
	// while the registry still serves it. Nil for unavailable apps,
	// hand-built test values, and no-runtime apps.
	lease *runtime.ImageLease
}

func (p *PreparedApp) Name() string {
	return p.fn.Name
}

func (p *PreparedApp) App() app.App {
	return p.fn
}

// Prepared returns the built image handle, or nil for an unavailable app.
func (p *PreparedApp) Prepared() *runtime.Prepared {
	return p.prepared
}

// ReleasePublication drops this app's publication lease, if any. It is
// idempotent and nil-safe. The registry calls it when the entry is superseded;
// a caller that builds a PreparedApp but never publishes it must call it
// to avoid stranding the image.
func (p *PreparedApp) ReleasePublication() {
	if p == nil || p.lease == nil {
		return
	}
	p.lease.Release()
}

// sharePublication returns a SHARED lease of this entry's publication lease, so
// a registry snapshot can pin the published image for the duration of its work
// even if the entry is superseded concurrently. It is nil when the entry has no
// publication lease (unavailable/no-runtime/test values).
func (p *PreparedApp) sharePublication() *runtime.ImageLease {
	if p == nil || p.lease == nil {
		return nil
	}
	return p.lease.Share()
}

// SharePublication is the exported form of sharePublication: it admits a
// shared reference to this app's published image, held until the caller
// releases it. It is used by the worker's startup service enqueue, which
// publishes an app's initial desired service state with the app's own
// publication lease shared so a concurrent retirement cannot remove the image
// while the service pass converges. A nil result means no Relay-owned image
// (unavailable/no-runtime app, or a hand-built test value).
func (p *PreparedApp) SharePublication() *runtime.ImageLease {
	return p.sharePublication()
}

func NewPrepared(
	fn app.App,
	prepared *runtime.Prepared,
	executor Executor,
) *PreparedApp {
	pf := &PreparedApp{
		fn:        fn,
		prepared:  prepared,
		executor:  executor,
		available: true,
	}
	// Transfer the prepared handle's admitted image lease into the publication:
	// ownership moves from the build to the registry entry, which releases it
	// when the entry is superseded. This closes the Prepare→Registry gap — the
	// image stays admitted across the swap. TakeLease clears the handle's own
	// reference so a later Prepared.ReleaseLease cannot double-release.
	if prepared != nil {
		pf.lease = prepared.TakeLease()
	}
	return pf
}

// NewUnavailable wraps an app whose image could not be built so the runner
// can skip it without losing the app's identity.
func NewUnavailable(fn app.App) *PreparedApp {
	return &PreparedApp{fn: fn, available: false}
}

// New creates a Runner over the given prepared apps. Each invocation is
// bounded by the matching rule's own timeout. Metrics are nil (disabled).
func New(prepared []*PreparedApp, logger *slog.Logger) *Runner {
	return NewWithMetrics(prepared, logger, nil)
}

// NewWithMetrics is like New but wires an optional metrics registry. A nil
// registry is safe: every metric call is a no-op.
func NewWithMetrics(prepared []*PreparedApp, logger *slog.Logger, registry *metrics.Registry) *Runner {
	r := &Runner{
		reg:                     &Registry{},
		log:                     logger,
		metrics:                 registry,
		refs:                    newImageRefCounter(),
		fnSems:                  map[string]*semaphore{},
		slotWait:                slotWaitTimeout,
		imageCleanupRetryDelays: defaultImageCleanupRetryDelays,
	}
	// maxConcurrency defaults to DefaultMaxConcurrency so an uncalled
	// SetMaxConcurrency (a runner constructed directly, as in tests) still has a
	// bounded global concurrency. SetMaxConcurrency overwrites it.
	r.maxConcurrency.Store(DefaultMaxConcurrency)
	r.globalSem.Store(newSemaphore(DefaultMaxConcurrency))
	// When a retired image's last in-flight execution releases it, run the async
	// removal automatically. r is fully built before any goroutine can run, and
	// imageRemovedIdle is nil-safe on a nil cleaner.
	r.refs.setOnIdle(r.imageRemovedIdle)
	r.reg.Set(prepared)
	return r
}

// imageRemovedIdle is the onIdle hook: a retired image just became idle, so
// remove it off the event path.
func (r *Runner) imageRemovedIdle(image string) {
	r.removeImageAsync(image)
}

// Registry exposes the runner's mutable snapshot set so the reconciler can swap
// apps live without round-tripping through New.
func (r *Runner) Registry() *Registry { return r.reg }

// SetHostname sets this worker's container-ownership hostname, stamped as the
// relay.hostname label on every execution container. It must be called before
// Consume begins processing; it takes effect on the next Handle, so setting it
// right after construction (as internal/worker does) labels every invocation.
// The constructor guarantees a non-nil Runner.
func (r *Runner) SetHostname(hostname string) {
	r.hostname = hostname
}

// SetMaxHandlerTimeout caps every rule's handler timeout to at most timeout. A value
// of 0 (the default) leaves rule timeouts uncapped. It takes effect on the next
// Handle. It is defense in depth: template validation enforces the cap at load,
// and this runtime cap guarantees a misconfigured or hot-swapped template can
// never run a handler longer than the stream layer's MaxRuleTimeout. The capped
// value is also what TryStart persists as the invocation's running deadline, so
// the persisted deadline matches the local timer by construction.
func (r *Runner) SetMaxHandlerTimeout(timeout time.Duration) {
	r.maxHandlerTimeout.Store(int64(timeout))
}

// SetSecretProvider wires the provider that resolves secret references to
// values at execution time. It takes effect on the next Handle. A nil provider
// means no secrets are available: a template that references a secret then fails
// the invocation with a clear error. The worker wires the production local
// provider after construction.
func (r *Runner) SetSecretProvider(provider secrets.Provider) {
	r.secrets = provider
}

// SetMaxConcurrency sets the worker-global cap on concurrently executing
// invocations. A value of 0 or negative (the zero value) falls back to
// DefaultMaxConcurrency (8); a value of 0 must not mean "unbounded". It takes
// effect on the next Handle. It is wired by the worker right next to
// SetHostname/SetSecretProvider/SetMaxHandlerTimeout. The global semaphore is
// (re)built on the next acquisition, so a call after construction resizes it.
func (r *Runner) SetMaxConcurrency(n int) {
	if n < 1 {
		n = DefaultMaxConcurrency
	}
	r.maxConcurrency.Store(int64(n))
	r.globalSem.Store(newSemaphore(n))
}

// resolver returns the runner's resolved image cleaner, or nil when the executor
// does not implement retirement (tests, unavailable-only runners). It is resolved
// once and cached; resolution scanning the registry is cheap and safe.
func (r *Runner) resolver() ImageCleaner {
	r.cleanerOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if cleaner, ok := pf.executor.(ImageCleaner); ok {
				r.cleaner = cleaner
				return
			}
		}
	})
	return r.cleaner
}

// invalidatorResolver returns the runner's resolved container invalidator, or
// nil when the executor does not implement retirement (tests, unavailable-only
// runners). Resolution scans the registry snapshot once and caches.
func (r *Runner) invalidatorResolver() ContainerInvalidator {
	r.invalidatorOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if invalidator, ok := pf.executor.(ContainerInvalidator); ok {
				r.invalidator = invalidator
				return
			}
		}
	})
	return r.invalidator
}

// ImageInUse reports whether any execution is currently holding a reference to
// the given image (in-flight Handle). It is the guard the reconciler consults
// before removing a superseded image, and Release uses it to know when a retired
// image becomes removable.
func (r *Runner) ImageInUse(image string) bool {
	return r.refs.inUse(image)
}

// RetireImage retires the given image reference so it can be removed once it is
// no longer in use. When nothing holds it now, it is removed immediately off the
// event path; otherwise it is marked pending and removed once the last in-flight
// execution releases it (see imageRefCounter.release → imageRemovedIdle). A nil
// cleaner (no ImageCleaner executor) makes this a no-op, which is correct for
// test fakes and unavailable-only runners.
func (r *Runner) RetireImage(image string) {
	if image == "" {
		return
	}
	if !r.refs.recordRetired(image) {
		// Already retired (first retirement owns removal); nothing to do.
		return
	}
	// Invalidate pooled execution containers running this image BEFORE any
	// removal attempt: the discard clears the image's relay-owned-container
	// reference promptly, so ErrImageInUse / the reference guard does not wait
	// for a retry pass. InvalidateImage is strictly non-blocking manager-side
	// (idle containers are discarded now, busy ones retired until release), so
	// retirement never stalls on an in-flight invocation.
	if inv := r.invalidatorResolver(); inv != nil {
		inv.InvalidateImage(image)
	}
	if !r.ImageInUse(image) {
		// Idle right now: remove immediately instead of waiting for a release
		// that will only ever fire if a new execution picks this image up.
		r.removeImageAsync(image)
	}
}

// RemoveAppImages retires every local version of an app's images so
// that, once idle, each is removed. It is the app-removal path: the
// reconciler calls it when an app directory vanishes, and all of its version
// images become garbage. A nil cleaner makes this a no-op.
func (r *Runner) RemoveAppImages(name string) {
	cleaner := r.resolver()
	if cleaner == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	tags, err := cleaner.AppImageTags(ctx, name)
	if err != nil {
		// Manager shutdown is a terminal deferral for this worker: the images are
		// left for the next boot's natural cleanup pass. It is not a genuine
		// failure, so it is a debug deferral, not a Warn.
		if errors.Is(err, runtime.ErrManagerShuttingDown) {
			r.log.Debug("Image cleanup: manager shutting down; deferring app image listing", "app", name)
			return
		}
		r.log.Warn("Image cleanup: list app versions failed", "app", name, "error", err)
		return
	}
	for _, tag := range tags {
		r.RetireImage(tag)
	}
}

// removeImageAsync removes a retired image off the event path so a docker round
// trip can never add latency (or failure) to Handle. Removal is gated by two
// independent guards: the in-flight refcount (an execution may have (re)claimed
// the image after it was retired) and the relay-owned container reference check
// (a persistent service container may still reference it). When either guard
// holds, removal is skipped and retried with a bounded backoff
// (r.imageCleanupRetryDelays); when the attempts exhaust, the image is deferred
// to a later natural cleanup pass rather than force-removed. A nil cleaner is a
// no-op.
func (r *Runner) removeImageAsync(image string) {
	cleaner := r.resolver()
	if cleaner == nil {
		return
	}
	// Snapshot the retry backoff once so a goroutine that outlives a test
	// (which may set r.imageCleanupRetryDelays) never races a later field write.
	// attempt is the retry counter shared across a single guard-skip chain; a
	// later retire of the same image that reaches removal does not cancel it —
	// the guards make duplicate attempts benign, so a bounded per-image cleanup
	// is safe and keeps goroutine count bounded.
	delays := append([]time.Duration(nil), r.imageCleanupRetryDelays...)
	attempt := 0
	r.retryImageCleanupAttempt(image, cleaner, delays, &attempt)
}

// retryImageCleanupAttempt performs one removal attempt off the event path. It
// guards the removal with the in-flight refcount and the relay-owned container
// reference check; a guard-skip schedules a bounded retry (sharing the attempt
// counter) using the snapshot backoff, and exhausting the retries defers the
// image to a later natural cleanup pass.
func (r *Runner) retryImageCleanupAttempt(image string, cleaner ImageCleaner, delays []time.Duration, attempt *int) {
	go func() {
		// The attempt reached a terminal classification (removed, deferred, or
		// rescheduled) when this goroutine returns. Signal it to the test-only
		// seam so a test can assert the decision without sleeping.
		if done := r.imageCleanupAttemptDone; done != nil {
			defer done(image)
		}
		// A retirement that is superseded by a new execution must not remove an
		// image a container is about to start; skip removal if it became in-use.
		// The in-flight execution's release path re-owns the removal when it
		// goes idle, so we do not burn a retry attempt here.
		if r.ImageInUse(image) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		referenced, err := cleaner.ImageReferencedByManagedContainer(ctx, image)
		if err != nil {
			// Manager shutdown is a terminal deferral for this worker: the image
			// is left for the next boot's natural cleanup pass, and retrying
			// would only hammer a closing Docker client. It is NOT a genuine
			// failure, so it is a debug deferral, not a Warn, and it never
			// reschedules.
			if errors.Is(err, runtime.ErrManagerShuttingDown) {
				r.log.Debug("Image cleanup: manager shutting down; deferring to next boot", "image", image)
				return
			}
			// Never remove on unknown state: treat the reference check failure as
			// conservatively referenced and retry.
			r.skipAndRetryImageCleanup(image, delays, "reference check failed", attempt)
			return
		}
		if referenced {
			r.skipAndRetryImageCleanup(image, delays, "relay-owned container references it", attempt)
			return
		}

		if err := cleaner.RemoveImage(ctx, image); err != nil {
			// Manager shutdown mid-attempt: terminal deferral, never a second
			// removal against a closing client, never rescheduled.
			if errors.Is(err, runtime.ErrManagerShuttingDown) {
				r.log.Debug("Image cleanup: manager shutting down; deferring to next boot", "image", image)
				return
			}
			// A removal that fails while a container references the image is a
			// guard-skip (the daemon refused, or the reference appeared mid-call),
			// not a genuine failure. Re-consult the reference to classify the
			// error: referenced -> debug skip + retry; otherwise the genuine Warn.
			if again, aerr := cleaner.ImageReferencedByManagedContainer(ctx, image); aerr == nil && again {
				r.skipAndRetryImageCleanup(image, delays, "relay-owned container references it", attempt)
				return
			} else if errors.Is(aerr, runtime.ErrManagerShuttingDown) {
				r.log.Debug("Image cleanup: manager shutting down; deferring to next boot", "image", image)
				return
			}
			// A retirement still draining an admitted lease (a registry snapshot
			// or a service pass) or a drain bound that expired is a retryable
			// transitional state, not a failure: defer and retry rather than
			// warning. The runtime's lease coordinator keeps the image committed
			// to removal, so the next attempt resumes the drain.
			if errors.Is(err, runtime.ErrImageRetiring) || errors.Is(err, context.DeadlineExceeded) {
				r.skipAndRetryImageCleanup(image, delays, "image retirement still draining", attempt)
				return
			}
			r.log.Warn("Image cleanup: remove retired failed", "image", image, "error", err)
			return
		}

		// The app image was successfully removed. This is the lifecycle
		// moment a dependency layer may become orphaned: the removed app
		// image was the only reference to its dependency, so run dependency GC
		// now to prune any layer no managed app image references anymore.
		// It is best-effort: a failure here (a genuine daemon error) is worth a
		// Warn — it retries on the next natural pass after the next app-image
		// removal or at the next startup — and must NOT affect the outcome of the
		// removal that already succeeded. The app image's own removal (and
		// this GC) both proceed off the event path in this same goroutine, and the
		// reconciler pump is serial with this retire hook, so this cannot race a
		// build that FROM the dependency.
		if _, err := cleaner.CleanupUnusedDependencies(ctx); err != nil {
			// Manager shutdown is a terminal deferral for this worker (the next
			// boot's startup GC prunes the layer), not a failure.
			if errors.Is(err, runtime.ErrManagerShuttingDown) {
				r.log.Debug("Image cleanup: manager shutting down; deferring dependency GC", "image", image)
				return
			}
			r.log.Warn("Dependency image cleanup failed", "error", err)
		}
	}()
}

// skipAndRetryImageCleanup logs a debug-level skip and schedules the next
// removal attempt with bounded backoff (delays is the per-chain snapshot), or —
// on the final attempt — logs a single Info deferring the image to a later natural
// cleanup pass (boot sweep, the next rebuild's retire, or app-removal
// retirement). It is context-free and off the event path.
func (r *Runner) skipAndRetryImageCleanup(image string, delays []time.Duration, reason string, attempt *int) {
	r.log.Debug("Image cleanup: image still in use; skipping", "image", image, "reason", reason)
	*attempt++
	if *attempt > len(delays) {
		r.log.Info("Image cleanup: image still in use after retries; deferring to a later cleanup pass", "image", image)
		return
	}
	delay := delays[*attempt-1]
	time.AfterFunc(delay, func() {
		r.retryImageCleanupAttempt(image, r.resolver(), delays, attempt)
	})
}

// toImage returns the image a prepared app executes, or "" when it is
// unavailable/nil so refcount and retirement stay nil-safe for fake and
// unavailable paths.
func toImage(pf *PreparedApp) string {
	if pf == nil || pf.prepared == nil {
		return ""
	}
	return pf.prepared.Image
}

// executeWithRefs runs one rule's handler while holding a reference to the
// app's image for the duration of the invocation, so a concurrent
// RetireImage cannot remove the image an in-flight execution still needs
// (at-least-once safety). The release is deferred so it runs even if the
// executor panics; the helper is called per rule so the defer scope is
// per-invocation rather than accumulating across a long rule loop. extraEnv are
// the per-invocation env vars (template env values + resolved secrets).
//
// lease is the snapshot's admitted publication lease pinning the image; it is
// attached to the execution context so Manager.Execute executes under that
// admitted authority rather than acquiring a fresh (possibly rejected) lease. A
// nil lease (fake executors, no-runtime apps, test values) leaves the
// context unchanged and lets Execute acquire its own.
func (r *Runner) executeWithRefs(
	pf *PreparedApp,
	invokeCtx context.Context,
	handler string,
	eventJSON []byte,
	extraEnv []string,
	lease *runtime.ImageLease,
) error {
	image := toImage(pf)
	r.refs.acquire(image)
	defer r.refs.release(image)
	if lease != nil {
		invokeCtx = runtime.WithSnapshotLease(invokeCtx, lease)
	}
	return pf.executor.Execute(invokeCtx, pf.prepared, handler, eventJSON, extraEnv)
}

// semaphore is a channel-based counting semaphore that bounds how many
// invocations may execute concurrently (globally or per app). acquireReserve
// blocks up to slotWaitTimeout for a free slot, returning false on timeout (the
// invocation is left pending and reclaimed later). A per-app semaphore is
// REPLACED (not mutated) when the app's resolved concurrency changes, so an
// in-flight acquisition still releases to the semaphore pointer it captured
// while new acquisitions use the resized one; the global semaphore is rebuilt by
// SetMaxConcurrency.
//
// The per-app semaphore is the ONLY per-app invocation limiter: the
// runtime's warm container pool is sized from the same effective concurrency
// (template concurrency clipped to MAX_CONCURRENCY), so the semaphore always
// admits no more concurrent Execute calls than the pool has containers, and the
// pool never blocks in the runner path. The global semaphore is the broader cap
// shared across apps.
type semaphore struct {
	slots chan struct{}
	// capacity is the limit the channel was created with. It is immutable and
	// lets concurrencySems detect a reconciled concurrency change by comparing
	// the desired value against the installed semaphore, so the map entry is
	// replaced only on a real change (never resized per call).
	capacity int
}

func newSemaphore(n int) *semaphore {
	return &semaphore{slots: make(chan struct{}, n), capacity: n}
}

// acquire acquires one slot, blocking up to wait until a slot frees, ctx is
// done, or wait elapses. On success it returns (true, waited false/true) and the
// caller MUST release (via release) exactly once. On timeout/cancel it returns
// (false, ...) and no slot is held. waited reports whether the call had to
// block at all (a slot was not immediately available): it feeds the
// concurrency_waits_total counter.
func (s *semaphore) acquire(ctx context.Context, wait time.Duration) (got bool, waited bool) {
	select {
	case s.slots <- struct{}{}:
		return true, false
	default:
		// Not immediately free: fall through to the blocking wait.
	}
	waited = true
	select {
	case s.slots <- struct{}{}:
		return true, true
	case <-time.After(wait):
		return false, true
	case <-ctx.Done():
		return false, true
	}
}

func (s *semaphore) release() {
	<-s.slots
}

// concurrencySems returns the global and per-app semaphores for the given
// app. The app's semaphore is created on demand and RESIZED in place
// (by replacing the map entry with a freshly sized semaphore) whenever the
// app's resolved concurrency differs from the installed one. Replacement
// rather than channel mutation is deliberate: an in-flight acquisition holds
// the OLD pointer and releases to it, so shrinking can never block a release on
// a full new channel nor lose a slot, and no held slot is ever stranded. The
// global semaphore is non-nil on the runner (normalized on construction).
//
// The resized-to concurrency is resolved from the CURRENT registry entry, not
// only the caller's snapshot value (see currentConcurrency): an in-flight Handle
// holding an older snapshot cannot resize the semaphore backwards after a newer
// snapshot already applied a larger bound. The resolved value is additionally
// clipped to the worker-global MAX_CONCURRENCY (see effectiveConcurrency), so a
// app asking for more than the global cap never gets a per-app
// semaphore larger than the global one — and the runtime's warm-pool bound,
// also clipped to the cap, agrees with it.
func (r *Runner) concurrencySems(fnName string, fnConcurrency int) (global *semaphore, fn *semaphore) {
	// Global semaphore: normalized on construction / SetMaxConcurrency; it is
	// always non-nil in practice. Guard nil defensively (a zero-valued Runner
	// in tests would read nil).
	global = r.globalSem.Load()
	if global == nil {
		global = newSemaphore(DefaultMaxConcurrency)
	}
	fnConcurrency = r.effectiveConcurrency(fnName, fnConcurrency)
	r.fnSemsMu.Lock()
	defer r.fnSemsMu.Unlock()
	if s, ok := r.fnSems[fnName]; ok {
		if s.capacity == fnConcurrency {
			return global, s
		}
		// Resolved concurrency changed: install a freshly sized semaphore so new
		// acquisitions use the new bound. In-flight holds keep the old pointer
		// and release to it (see the semaphore doc).
		s = newSemaphore(fnConcurrency)
		r.fnSems[fnName] = s
		return global, s
	}
	s := newSemaphore(fnConcurrency)
	r.fnSems[fnName] = s
	return global, s
}

// currentConcurrency returns fnName's current resolved per-app concurrency
// from the registry, falling back to fallback when the app is absent or has
// no parsed template. It makes a semaphore resize authoritative to the latest
// published template rather than a caller's possibly-stale registry snapshot.
func (r *Runner) currentConcurrency(fnName string, fallback int) int {
	if r.reg == nil {
		return fallback
	}
	if pf := r.reg.GetByName(fnName); pf != nil && pf.fn.Template != nil {
		return pf.fn.Template.Concurrency
	}
	return fallback
}

// effectiveConcurrency returns the per-app semaphore capacity for fnName:
// its current resolved template concurrency clipped to the worker-global
// MAX_CONCURRENCY. Clipping matters when the template asks for more than the
// global cap (e.g. concurrency 15 with MAX_CONCURRENCY=8): the per-app
// semaphore is then sized to the cap, matching the runtime's effective warm-pool
// bound, so the pool and the semaphore never disagree. A zero/negative template
// value falls back to app.DefaultConcurrency; a zero maxConcurrency (a
// zero-valued Runner in tests) falls back to DefaultMaxConcurrency, never
// "uncapped". The global value is read fresh, so a SetMaxConcurrency call is
// reflected on the next acquisition (which resizes the app's semaphore).
func (r *Runner) effectiveConcurrency(fnName string, fallback int) int {
	n := r.currentConcurrency(fnName, fallback)
	if n < 1 {
		n = app.DefaultConcurrency
	}
	limit := int(r.maxConcurrency.Load())
	if limit < 1 {
		limit = DefaultMaxConcurrency
	}
	if n > limit {
		return limit
	}
	return n
}

// RemoveAppSemaphore drops an app's per-app semaphore. It is
// called when the app is removed from the registry, so a later recreation
// of the same name starts from a fresh semaphore rather than inheriting a stale
// bound (and so a removed app's map entry does not linger). In-flight
// acquisitions still release to the pointer they captured. The constructor
// guarantees a non-nil Runner.
func (r *Runner) RemoveAppSemaphore(fnName string) {
	r.fnSemsMu.Lock()
	delete(r.fnSems, fnName)
	r.fnSemsMu.Unlock()
}

// reserveSlots acquires both the global and per-app slots for one
// invocation, counting a concurrency_waits_total whenever either slot was not
// immediately free (the acquisition had to block). It returns a release func on
// success (call it after the invocation, releasing both slots and the in-flight
// gauge) and whether the acquisition had to block at all (for a debug log); on
// timeout it returns (nil, ...) and does NOT hold any slot. It must be called
// BEFORE TryStart so a blocked invocation is never counted as an attempt and does
// not persist state.
func (r *Runner) reserveSlots(ctx context.Context, fnName string, fnConcurrency int) (release func(), waited bool) {
	global, fn := r.concurrencySems(fnName, fnConcurrency)

	// Acquire the global slot first (the broader bound). A blocked acquire
	// counts a wait, whether it eventually succeeds or not.
	got, w := global.acquire(ctx, r.slotWait)
	if w {
		r.metrics.Inc(metrics.MetricConcurrencyWaits)
	}
	waited = waited || w
	if !got {
		return nil, waited
	}

	// Then the per-app slot. A blocked acquire also counts a wait. If the
	// per-app slot never frees, release the global slot so it does not
	// leak to another app's wait.
	got, w = fn.acquire(ctx, r.slotWait)
	if w {
		r.metrics.Inc(metrics.MetricConcurrencyWaits)
	}
	waited = waited || w
	if !got {
		global.release()
		return nil, waited
	}

	// Both slots held and the invocation is about to execute: publish the
	// in-flight gauge for it.
	r.inFlight.Add(1)
	r.metrics.SetGauge(metrics.MetricInFlightInvocations, float64(r.inFlight.Load()))

	released := false
	release = func() {
		if released {
			return
		}
		released = true
		fn.release()
		global.release()
		// Publish the in-flight gauge AFTER dropping below the cap.
		r.inFlight.Add(-1)
		r.metrics.SetGauge(metrics.MetricInFlightInvocations, float64(r.inFlight.Load()))
	}
	return release, waited
}

// runInvocation runs one rule's handler while holding a reference to the
// app's image for the duration of the invocation, converting an
// executor/runtime panic into a failed-attempt error instead of letting it
// escape Handle and kill the worker. This is the single panic boundary for
// message processing: panics inside executor/runtime code are a misbehaving
// execution (isolated, retried via the normal pending/reclaim flow), while
// panics elsewhere (startup, reconciler, Redis client) stay fatal and visible.
//
// The invocation context's cancel is deferred here so it always runs, even when
// the executor panics: without this, a panic that unwinds past the call site
// would skip the explicit cancel() and leak the context's timer until it fired
// on its own. executeWithRefs's own defers (image refcount release) run during
// panic unwinding BEFORE the recover here, so the image reference is never
// leaked; the returned panic value is logged by the caller.
func (r *Runner) runInvocation(
	pf *PreparedApp,
	invokeCtx context.Context,
	cancel context.CancelFunc,
	handler string,
	eventJSON []byte,
	extraEnv []string,
	lease *runtime.ImageLease,
	trace invocationTrace,
) (panicked bool, panicValue any, err error) {
	// The end-to-end invocation span, shared by the event-rule, schedule, and
	// manual paths (all funnel through here). It is a child of the delivery
	// (or replay/manual operation) context, and its span context is propagated
	// into the executor so the runtime's runtime.execute/acquire/invoke children
	// nest beneath it. RunMeta and the timeout already stamped on invokeCtx are
	// inherited.
	//
	// Retry lineage: a new attempt links to the PREVIOUS attempt's stored span
	// reference (when one was persisted) and then records its own span context
	// so a later retry, even after a worker restart, can link back to it. The
	// link is not a parent: every attempt is a distinct function.invoke span
	// (usually sharing the original upstream trace through the delivery context)
	// while the link records the causal retry chain across attempts. Recording
	// only ever persists the compact traceparent/tracestate lineage, never
	// baggage, and a state-free caller (manual/replay) records nothing.
	var runtimeName string
	if pf != nil && pf.fn.Template != nil {
		runtimeName = pf.fn.Template.Runtime
	}
	var fnName string
	if pf != nil {
		fnName = pf.fn.Name
	}
	spanCtx, span := startInvocationSpan(invokeCtx, fnName, handler, runtimeName, trace.spanOpts()...)
	trace.record(span.SpanContext())
	defer func() {
		if panicked {
			finishInvocationSpan(span, fmt.Errorf("executor panic: %v", panicValue))
			return
		}
		finishInvocationSpan(span, err)
	}()
	defer cancel()
	defer func() {
		if pv := recover(); pv != nil {
			panicked = true
			panicValue = pv
			err = fmt.Errorf("executor panic: %v", pv)
		}
	}()
	return false, nil, r.executeWithRefs(pf, spanCtx, handler, eventJSON, extraEnv, lease)
}

// Handle evaluates the event against all loaded apps and executes every
// matching rule's handler. It returns nil only when every invocation succeeded
// (or nothing matched); otherwise it returns an error so the stream layer does
// not acknowledge the message.
//
// At-least-once semantics: Handle returns an error after partial successes, so
// a retried message re-runs the handlers that already succeeded. The event
// classification counters (events_received/matched/unmatched_total) are NOT
// delivery-attempt counters: each logical event is classified exactly once
// across redeliveries/retries via an atomic claim in the message's
// invocation-state hash (see stream.InvocationState.ClaimClassification). A
// handler failure stays in the matched class. Handle does NOT claim
// exactly-once execution: redeliveries re-run whatever is not yet recorded as
// complete, and the handlers must stay idempotent.
//
// Aggregate, per-invocation semantics: a single Redis message can match multiple
// "<app>/<handler>" invocations, and each is tracked independently in the
// per-message invocation-state hash. Every eligible matching invocation gets its
// own attempt on each delivery regardless of the others' outcomes: a failure in one
// handler does NOT prevent later matching handlers from running. The rule loop is
// strictly sequential (never parallel), and after iterating every matching rule
// Handle aggregates the per-invocation outcomes into the message-level return
// contract below.
//
// Invocation state: when the stream layer injects an InvocationState into ctx
// (see stream.WithInvocationState), Handle skips any matching invocation whose
// "<app>/<handler>" ID is already recorded as completed on a previous
// delivery, is protected by an active attempt deadline or a retry backoff (a
// running or next_attempt_at marker whose persisted deadline has not yet passed
// — this or another replica may be executing it, or it is waiting out its
// backoff), or is exhausted (terminal). Skipped invocations are not executions:
// they do not touch the handler_* or function_* metrics.
// app_events_matched_total still counts the app as engaged (it
// matched), which is attribution, not execution counting. When no invocation
// state is present (direct Handle callers/tests, or invocation tracking
// disabled) Handle behaves exactly as before: it runs every matching handler
// and returns the first failure's plain error immediately, with no invocation
// wrapping or DLQ attribution.
//
// Pending desired generations: the registry can hold, beside the active set, a
// PENDING desired template for a valid generation that is being prepared but is
// not yet runnable (see Registry.SetPending). Pending rules are MATCHED — they
// engage their app and keep the message pending as unavailable — but they never
// execute, claim a TryStart, exhaust, ACK, or DLQ. This closes the event-loss
// window during preparation: a delivery that matches ONLY a pending rule (a
// brand-new app before its first image, or a new rule not yet in the active
// generation) is matched-but-unavailable rather than unmatched, so it is never
// ACKed away. A pending rule whose invocation is ALSO matched by the active
// generation is deduped by "<app>/<handler>": the active generation executes it
// and it is not separately held. When the new generation is installed (or the
// app is removed/invalidated) the pending entry is cleared atomically with the
// registry mutation.
//
// Return contract (with invocation state, aggregated after the full rule loop):
//   - a plain (retryable) error when any matched invocation had a retryable
//     failure this delivery — regardless of other invocations' outcomes — so
//     the message stays pending and is retried. The first such error is returned.
//   - a wrapped runner.ErrAppUnavailable when at least one MATCHED
//     invocation belongs to a configured app whose image is not currently
//     built and that invocation has not already completed. Such an event is
//     MATCHED, never unmatched, but the invocation cannot run this delivery: the
//     message stays pending (not ACKed, and not DLQ'd solely for
//     unavailability). No handler attempt is claimed and no handler counter is
//     touched, because no handler ran. A later delivery — once the app is
//     rebuilt and available again — completes the outstanding invocation.
//   - a wrapped stream.ErrInvocationExhausted when every invocation in the
//     matched set is terminal (complete or exhausted), at least one of them is
//     exhausted, and no retryable failure or unavailable match occurred this
//     delivery. The whole message is terminal, so the stream layer routes it to
//     the DLQ. This also fires on a redelivery where the exhausting invocation
//     was already marked exhausted by an earlier delivery: the message may still
//     be pending because its DLQ write (or the post-DLQ XACK) failed, so it must
//     be re-routed rather than returned nil and ACKed without a DLQ entry.
//   - a wrapped stream.ErrInvocationNotEligible when no retryable failure
//     occurred and the message is not all-terminal, but at least one matched
//     invocation was skipped because it is protected (running or waiting out a
//     retry backoff) or its concurrency slot timed out. This fires even when
//     other invocations executed successfully in this same call: a protected
//     or slot-timeout invocation is unresolved, so the message must stay pending
//     and NOT be ACKed (this is what fixes the cross-replica ACK hazard — a
//     replica that reclaims a message whose invocation is still in flight on
//     another replica must not ACK it).
//   - nil when every matched invocation is complete (or nothing matched), so the
//     stream layer ACKs the message.
//
// Panic boundary: each invocation's execution runs inside runInvocation, which
// recovers an executor/runtime panic and converts it into a normal failed
// attempt (see runInvocation). A panicking execution is therefore isolated and
// retried via the same failure/backoff/exhaustion machinery as any other
// failure — it never escapes Handle to kill the worker. The image refcount is
// still released (executeWithRefs's defer runs during unwinding) and the
// invocation's context cancel is deferred so no timer leaks. Panics elsewhere
// (startup, reconciler, Redis client) are not recovered here and stay fatal.
func (r *Runner) Handle(ctx context.Context, msgID string, event map[string]any) error {
	// Best-effort delivery attempt, defaulting to 1 when the stream did not set
	// it (e.g. when the runner is driven directly in tests). It is the delivery
	// attempt used for logging; without invocation state the handler attempt is
	// unknown, so the delivery attempt is logged instead.
	deliveryAttempt := stream.DeliveryAttemptFrom(ctx)
	// Best-effort invocation state, absent when the stream did not inject it
	// (direct Handle callers/tests, or invocation tracking disabled).
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Trackers for the message-level return contract, aggregated only after the
	// whole rule loop has run (see the Handle doc comment). skippedPending records
	// whether any matched invocation was skipped because it is protected (running
	// or waiting out a retry backoff) or its concurrency slot timed out — an
	// unresolved invocation that must keep the message pending even when other
	// invocations succeeded this call. firstErr holds the first plain retryable
	// failure; exhaustedInvocations collects EVERY exhausted matched invocation —
	// the typed metadata of one that exhausted this delivery, or one synthesized
	// from a terminal-skip of an invocation already marked exhausted on a previous
	// delivery (so a redelivery after a failed DLQ write re-routes instead of
	// ACKing). The aggregate returns them all on a *stream.HandlerExhaustedError,
	// so each exhausted app/handler gets its own DLQ entry with its own
	// attempt count. anyExhausted records whether any matched invocation is
	// exhausted (this delivery or a previous one). These are only meaningful when
	// hasState is true.
	skippedPending := false
	var firstErr error
	// claimErr records the first ambiguous-claim failure (a TryStart store
	// error) seen this delivery. It is kept separate from firstErr because a
	// claim failure is NOT a failed attempt (no handler ran, no retry/exhaustion
	// accounting); it must stay pending, and it is surfaced (wrapping
	// ErrInvocationNotEligible) so the cause is distinguishable from an ordinary
	// protected skip.
	var claimErr error
	var exhaustedInvocations []stream.ExhaustedInvocation
	anyExhausted := false
	// executed tracks whether any invocation actually executed, for the
	// no-state (direct caller/test) path, which preserves the old fail-fast
	// tail exactly. With invocation state it is unused (the aggregate uses the
	// per-invocation outcomes instead).
	executed := false

	// Take one consistent snapshot for the whole call so a concurrent registry
	// swap mid-execution cannot reorder or drop apps under us, and PIN each
	// published app's image while the registry lock still protects its
	// entry. The pins are held until Handle returns, so an image published at
	// snapshot time cannot be removed out from under this delivery even if the
	// entry is superseded concurrently; each matching execution carries its
	// app's pin into Manager.Execute. The pins are shares of the published
	// lease, so a retirement in progress never blocks an already-admitted
	// delivery.
	snap := r.reg.snapshotPinned()
	defer snap.release()
	snapshot := snap.fns

	// Pre-pass: collect every matched invocation ID so an exhausted attempt can
	// decide whether the whole message is terminal (all matched invocations
	// complete or exhausted), and collect the engaged app names. The
	// registry snapshot holds one entry per app, so an app with several
	// matching rules is appended once here (deduped by construction). This must
	// be complete before any execution, because a rule that exhausts early must
	// still see the full set of matched invocations (including ones that sort
	// later). Matching is pure; a panic here is a programming error that escapes
	// and no counter has been touched yet.
	//
	// Matching includes configured apps whose image is not currently built
	// (available == false): an event matching ONLY such an app is MATCHED,
	// not unmatched, so it is classified matched and the app is counted as
	// engaged. Skipping unavailable apps here (the previous behavior) both
	// mis-classified those events as unmatched and let a mixed message ACK
	// without running the unavailable app's share. unavailableMatched
	// records the invocations that matched but cannot run this delivery; an
	// unresolved one keeps the message pending rather than ACKed or DLQ'd for
	// unavailability alone (see the aggregate below). A nil Template (only
	// reachable from a hand-built unavailable entry, never from the loader)
	// cannot match and is skipped.
	var matched []string
	var matchedFns []string
	var unavailableMatched []string
	for _, pf := range snapshot {
		if pf.fn.Template == nil {
			continue
		}
		rules := snap.matchingRules(pf, event)
		if len(rules) == 0 {
			continue
		}
		matchedFns = append(matchedFns, pf.fn.Name)
		for _, rule := range rules {
			invocation := pf.fn.Name + "/" + rule.Handler
			matched = append(matched, invocation)
			if !pf.available {
				unavailableMatched = append(unavailableMatched, invocation)
			}
		}
	}

	// Pending desired matches: a valid generation being PREPARED but not yet
	// runnable contributes its desired invocations as matched-but-unavailable,
	// so a delivery arriving during preparation stays pending instead of being
	// acknowledged as unmatched (the new-app / new-rule event-loss gap this
	// closes). Pending entries are non-runnable placeholders (no image, no
	// executor): they are matched here for CLASSIFICATION and holding only and
	// are deliberately absent from the execution loop below. Dedupe by
	// invocation identity against the active matches: a rule that is active in
	// v1 and also present in pending v2 is executed by the active path and must
	// not be separately held. A pending-only invocation still counts as matched
	// and engages its app, but its non-terminal (unresolved) state keeps the
	// message pending — it never executes, claims a TryStart, exhausts, ACKs, or
	// DLQs.
	matchedSet := make(map[string]struct{}, len(matched))
	for _, inv := range matched {
		matchedSet[inv] = struct{}{}
	}
	matchedFnSet := make(map[string]struct{}, len(matchedFns))
	for _, fnName := range matchedFns {
		matchedFnSet[fnName] = struct{}{}
	}
	for _, pf := range snap.pending {
		if pf.fn.Template == nil {
			continue
		}
		rules := snap.matchingPendingRules(pf, event)
		if len(rules) == 0 {
			continue
		}
		engaged := false
		for _, rule := range rules {
			invocation := pf.fn.Name + "/" + rule.Handler
			if _, dup := matchedSet[invocation]; dup {
				// Already matched (and executable) through the active
				// generation: do not separately hold the same invocation.
				continue
			}
			matchedSet[invocation] = struct{}{}
			matched = append(matched, invocation)
			unavailableMatched = append(unavailableMatched, invocation)
			engaged = true
		}
		if engaged {
			if _, dup := matchedFnSet[pf.fn.Name]; !dup {
				matchedFnSet[pf.fn.Name] = struct{}{}
				matchedFns = append(matchedFns, pf.fn.Name)
			}
		}
	}

	// An unavailable matched invocation is UNRESOLVED unless it already COMPLETED
	// for this message: a complete marker means the invocation's work here is
	// settled (it ran before this worker lost/never had the app), so it does
	// not hold the message pending. Anything else — never ran, or exhausted
	// awaiting a DLQ re-route whose attempt count only the available path can read
	// back — is unresolved, and the message must stay pending: it must neither be
	// ACKed (which could drop an outstanding execution or an unpersisted DLQ
	// entry) nor be dead-lettered solely for unavailability. The exhausted case is
	// deliberately included: this worker cannot attribute the exhausted attempt
	// count from an unavailable entry, and ACKing would race another replica's
	// DLQ write, so the safe choice is to hold pending until the app is
	// available again and the normal path surfaces the exhaustion with correct
	// metadata. With no invocation state there is no marker to consult, so any
	// unavailable match is treated as unresolved (the fail-safe direction). This
	// is read-only: no attempt is claimed and no counter is touched for an
	// unavailable app.
	var unresolvedUnavailable []string
	if len(unavailableMatched) > 0 {
		if hasState {
			for _, invocation := range unavailableMatched {
				if !invState.IsComplete(invocation) {
					unresolvedUnavailable = append(unresolvedUnavailable, invocation)
				}
			}
		} else {
			unresolvedUnavailable = unavailableMatched
		}
	}

	// Classify the logical event exactly once (received == matched +
	// unmatched), regardless of delivery attempt. With invocation state the
	// stream-injected handle claims the classification atomically in the
	// message's invocation-state hash: only the first delivery across
	// redeliveries, reclaims, and replicas wins, so retries never double-count.
	// A claim error counts nothing (classification is a partition, so a missed
	// count is preferable to a double count). Without invocation state (direct
	// Handle callers/tests) there is no dedup channel, so each call is treated
	// as a distinct logical event.
	//
	// A handler failure does NOT move the event out of the matched class: the
	// classification is decided from matching alone, before any execution.
	// Unmatched events are acknowledged and never retried.
	classify := true
	if hasState {
		claimed, err := invState.ClaimClassification()
		if err != nil {
			classify = false
		} else {
			classify = claimed
		}
	}
	if classify {
		r.metrics.Inc(metrics.MetricEventsReceived)
		if len(matchedFns) > 0 {
			r.metrics.Inc(metrics.MetricEventsMatched)
			// An app is "engaged" by an event when at least one of its
			// rules matches, regardless of whether the execution later fails:
			// an event matching two apps counts once per app here,
			// while MetricEventsMatched counts it once globally.
			for _, fnName := range matchedFns {
				r.metrics.IncLabels(metrics.MetricAppEventsMatched,
					[]metrics.Label{{Name: "app", Value: fnName}})
			}
		} else {
			r.metrics.Inc(metrics.MetricEventsUnmatched)
		}
	}

	for _, pf := range snapshot {
		if !pf.available {
			continue
		}
		rules := snap.matchingRules(pf, event)
		for _, rule := range rules {
			// The invocation identity is stable across restarts and config
			// reloads as long as the rule still exists: the app name and the
			// rule handler string. Renaming either invalidates old invocation
			// state — old entries simply never match, and the msg-level set of
			// required invocations is recomputed each delivery from current
			// templates, so a rule removed from the template no longer gates the
			// ACK.
			invocation := pf.fn.Name + "/" + rule.Handler
			if hasState && invState.IsComplete(invocation) {
				r.log.Debug("App handler: already succeeded for event; skipping",
					"app", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"delivery_attempt", deliveryAttempt,
				)
				continue
			}
			// Cap the rule timeout at the configured maximum (defense in depth;
			// template validation enforces the cap at load). This guarantees a
			// handler can never run longer than the stream layer's MaxRuleTimeout,
			// and the same capped value is what TryStart persists as the running
			// deadline, so the persisted deadline matches the local timer by
			// construction.
			timeout := rule.Timeout
			if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
				timeout = cap
			}
			// executeRule runs this single rule's invocation while holding the
			// worker-global and per-app concurrency slots (whose defer scope
			// is THIS call, so a multi-rule message never accumulates slots across
			// rules). It classifies this invocation's outcome for this delivery and
			// returns it plus the plain error (for a failed attempt) so the outer
			// loop can aggregate AFTER iterating every matching rule — a failure in
			// one handler never prevents later handlers from running.
			executeRule := func() (invocationOutcome, error) {
				// The per-invocation handler attempt/claim. With invocation
				// state it comes from TryStart (Redis-backed, incremented per
				// actual execution, with a fresh opaque token). Without
				// invocation state there is no persisted handler attempt, so it
				// stays the zero claim (explicitly not attributed): the delivery
				// count is NOT reused as a handler attempt count, because
				// deliveries count redeliveries, not executions. The no-state path
				// never calls recordFailure, so it never fabricates an
				// exhaustion/DLQ attribution either.
				var claim stream.InvocationClaim
				// Reserve the worker-global and per-app concurrency slots
				// BEFORE TryStart, so a blocked invocation is never counted as an
				// attempt and does not persist state. If no slot frees within
				// slotWait (well below MinPendingIdle, so a locally buffered event
				// never defeats reclaim), the invocation is treated as not
				// eligible: it stays pending and is replayed by a later reclaim,
				// with no retry accounting and no DLQ. The slots are released via
				// defer as soon as THIS invocation finishes.
				releaseSlots, waited := r.reserveSlots(ctx, pf.fn.Name, pf.fn.Template.Concurrency)
				if releaseSlots == nil {
					// No slot freed in time: skip this invocation without marking
					// it failed, leave the message pending (reclaim replays it
					// later).
					skippedPending = true
					r.log.Debug("App handler: concurrency slot wait timed out; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				if waited {
					r.log.Debug("App handler: waiting for concurrency slot",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
				}
				defer releaseSlots()
				// Claim the invocation for this execution before running it.
				// TryStart atomically persists an absolute running deadline
				// (now + timeout), the attempt, and a fresh claim token, and
				// returns started=false when the invocation is already complete
				// (handled above), exhausted, or protected by an active attempt
				// deadline or a retry backoff — this or another replica may be
				// executing it, or it is waiting out its backoff, so we must not
				// run it concurrently. The IsComplete check above is the fast path
				// that avoids a script call on completed invocations; TryStart's
				// own read also covers "ok" and the same case, so the two are
				// consistent.
				if hasState {
					started, startClaim, wait, startErr := invState.TryStart(invocation, timeout)
					if startErr != nil {
						// The claim outcome is unknown (Redis/transport or
						// token-generation error): do NOT run the handler (an
						// ambiguous claim could race a replica that won the same
						// claim) and leave the message pending so a later delivery
						// retries the claim. No handler attempt was confirmed, so
						// the claim stays zero and no retry/exhaustion accounting
						// happens here. The returned error wraps
						// ErrInvocationNotEligible (the stream's pending/no-ACK
						// contract) plus the distinct ErrInvocationClaimUnconfirmed
						// sentinel so the aggregate can surface a claim failure
						// separately from an ordinary protected skip.
						skippedPending = true
						r.log.Warn("App handler: claim failed (outcome unknown); leaving pending without executing",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"error", startErr,
						)
						return outcomePendingSkip, fmt.Errorf("%w: %v: %w: %w",
							stream.ErrInvocationNotEligible, invocation,
							stream.ErrInvocationClaimUnconfirmed, startErr)
					}
					if !started {
						// The slot is released by the deferred releaseSlots before
						// the next rule acquires.
						if wait > 0 {
							// Protected by an active running deadline or a retry
							// backoff. The message must stay pending (the protected
							// invocation may still complete or fail on its own), so
							// this is a "not eligible" skip, not a completion.
							skippedPending = true
							r.log.Debug("App handler: not eligible for event (running or waiting for retry); leaving pending",
								"app", pf.fn.Name,
								"handler", rule.Handler,
								"message_id", msgID,
								"handler_attempt", startClaim.Attempt,
								"next_attempt_in", wait,
							)
							return outcomePendingSkip, nil
						}
						// Terminal (complete or exhausted): never execute again. An exhausted
						// invocation must still drive the message-level DLQ decision, so a redelivery
						// after a failed DLQ write or XACK routes to the DLQ again instead of being
						// acknowledged as complete.
						if startClaim.Attempt > 0 {
							anyExhausted = true
							exhaustedInvocations = append(exhaustedInvocations, stream.ExhaustedInvocation{
								App:      pf.fn.Name,
								Handler:  rule.Handler,
								Attempts: startClaim.Attempt,
							})
						}
						r.log.Debug("App handler: terminal for event; skipping",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", startClaim.Attempt,
						)
						return outcomeTerminalSkip, nil
					}
					claim = startClaim
				}
				handlerAttempt := claim.Attempt
				// The invocation attempt has actually begun: the TryStart above
				// (or the absence of invocation state, for direct callers)
				// claimed it and the slots are held. This is the
				// last_execution_at attribution point — every claimed attempt
				// counts, retries included (they are real executions), while
				// the skip branches returned above never reach it.
				r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampExecution, time.Now().Unix())
				r.log.Debug("App rule: matched event",
					handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt)...,
				)
				eventJSON, err := json.Marshal(event)
				if err != nil {
					// The invocation was already claimed (deferred-released) but
					// will not execute: this is a failed attempt, so treat it as
					// such — schedule a retry (or exhaust), then CONTINUE to the
					// next matching rule so each independent invocation gets its own
					// failed attempt.
					if hasState {
						return r.recordFailure(invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("app %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
				}
				// Resolve the template's env values and secret references into the
				// per-invocation extra env, immediately before container creation.
				// Secret values are resolved per execution (never cached on
				// Prepared, never in the fingerprint), so rotating a secret value
				// never requires a rebuild. A resolution failure is a failed
				// attempt (the invocation was already claimed by TryStart), so it
				// flows through the same retry/exhaustion machinery as an
				// execution failure and, like marshal failures, does not
				// short-circuit the remaining rules. The error names the secret
				// REFERENCE only — never any value.
				extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
				if err != nil {
					if hasState {
						return r.recordFailure(invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("app %q handler %q: %w", pf.fn.Name, rule.Handler, err)
				}
				invokeCtx, cancel := context.WithTimeout(ctx, timeout)
				// Stamp the invocation's diagnostic metadata into the context so
				// the executor can attach it as container labels. This keeps the
				// Executor interface (and every test fake) unchanged.
				eventID, eventName := eventFields(event)
				invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
					Type:      runtime.ContainerTypeEvent,
					App:       pf.fn.Name,
					Handler:   rule.Handler,
					MessageID: msgID,
					EventID:   eventID,
					EventName: eventName,
					Hostname:  r.hostname,
					Image:     toImage(pf),
				})
				start := time.Now()
				panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv, snap.pinFor(pf), invocationTrace{state: invState, invocation: invocation, attempt: handlerAttempt})
				elapsed := time.Since(start)
				if panicked {
					// A panicking execution is a misbehaving handler, not a
					// healthy failure: log the panic value and the full stack so
					// the bug is visible and attributable, then funnel it through
					// the SAME failure branch below (metrics + recordFailure) so
					// retry and exhaustion accounting stay per-invocation.
					r.log.Error("App handler: PANICKED for event",
						append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
							"panic_value", fmt.Sprintf("%v", panicValue),
							"stack", string(debug.Stack()),
						)...,
					)
				}
				if err != nil {
					r.metrics.IncLabels(metrics.MetricHandlerInvocations,
						[]metrics.Label{
							{Name: "outcome", Value: "failure"},
							{Name: "app", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						})
					// Unlabeled total for the SQLite snapshot; the labeled counter
					// above stays for Prometheus.
					r.metrics.Inc(metrics.MetricHandlerFailure)
					// Per-app failure attribution (per rule execution). A
					// failed attempt that will retry still counts as a failure
					// (and as an execution above); only a DLQ-routed exhaustion
					// additionally sets last_dlq_at (see recordFailure).
					r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
						[]metrics.Label{{Name: "app", Value: pf.fn.Name}})
					r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampFailure, time.Now().Unix())
					r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
						[]metrics.Label{
							{Name: "app", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						}, elapsed)
					r.log.Warn("App handler: execution failed for event",
						append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
							"duration", elapsed,
							"reason", err,
						)...,
					)
					// Record the failure and decide retry vs exhaustion. This is
					// the per-invocation retry driver: a retryable failure
					// schedules a backoff and counts function_retries_total; an
					// exhausted attempt marks the invocation terminal and counts
					// function_dlq_total. The message-level DLQ decision (whether
					// EVERY matched invocation is terminal) is left to Handle's
					// end-of-loop aggregate, so an invocation may exhaust while
					// others still run.
					if hasState {
						return r.recordFailure(invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					// No invocation state (direct callers/tests): every failure
					// counts as a retry driver, but there is no Redis-backed
					// attempt count to decide exhaustion — so no DLQ attribution
					// here either. The DLQ metric is only meaningful with
					// invocation state, where exhaustion is actually persisted
					// and observable.
					r.metrics.IncLabels(metrics.MetricFunctionRetries,
						[]metrics.Label{{Name: "app", Value: pf.fn.Name}})
					return outcomeRetryable, err
				}
				// Record the invocation as completed so a redelivery skips it.
				// This happens BEFORE the success metrics so a crash between the
				// side effect and MarkComplete re-runs the handler (at-least-once;
				// the handler must remain idempotent). The completion is CASed on
				// this attempt's claim; a refusal (the marker was re-claimed by a
				// newer token, or is already terminal exhausted) means this
				// delivery does not own the invocation's resolution, so treat it
				// as unresolved: leave the message pending (never ACK a
				// superseded claim's outcome) and let the newer claim or terminal
				// state drive the decision on a later delivery.
				if hasState {
					if !invState.MarkComplete(invocation, claim) {
						skippedPending = true
						r.log.Warn("App handler: completion superseded; leaving pending",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", handlerAttempt,
						)
						return outcomePendingSkip, nil
					}
				}
				r.recordHandlerSuccess(pf.fn.Name, rule.Handler, elapsed)
				r.log.Info("App handler: executed for event",
					append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
						"duration", elapsed,
					)...,
				)
				return outcomeExecuted, nil
			}
			outcome, err := executeRule()
			if !hasState {
				// No invocation state (direct callers/tests): preserve the old
				// fail-fast behavior exactly — run every matching handler and
				// return the first failure's plain error immediately; the tail
				// handles the nothing-executed/not-eligible case. No invocation
				// wrapping or DLQ attribution when matched.
				if outcome == outcomeRetryable {
					return err
				}
				if outcome == outcomeExecuted {
					executed = true
				}
				continue
			}
			// Aggregate the per-invocation outcome for the message-level contract,
			// decided only after the whole loop (see Handle's doc comment). Keep
			// iterating regardless: each matching invocation gets its own attempt.
			switch outcome {
			case outcomePendingSkip:
				// Every pending skip (protected invocation, slot timeout, claim
				// failure, or a superseded stale transition) leaves the message
				// unresolved: it must never be ACKed. A claim failure is
				// additionally remembered via its distinguishable error so the
				// aggregate can surface its cause.
				skippedPending = true
				if err != nil && errors.Is(err, stream.ErrInvocationClaimUnconfirmed) && claimErr == nil {
					claimErr = err
				}
			case outcomeRetryable:
				if firstErr == nil {
					firstErr = err
				}
			case outcomeExhausted:
				anyExhausted = true
				// recordFailure returns a *stream.HandlerExhaustedError carrying
				// exactly one exhausted invocation; collect it so the aggregate
				// reports every exhausted app/handler with its own attempt
				// count. A non-typed error is ignored here (it cannot happen in
				// production) and the terminal-skip metadata still drives the
				// aggregate.
				var typed *stream.HandlerExhaustedError
				if errors.As(err, &typed) {
					exhaustedInvocations = append(exhaustedInvocations, typed.Invocations...)
				}
			}
		}
	}
	// Aggregate: decide the single message-level error from the per-invocation
	// outcomes collected across the whole rule loop.
	if hasState {
		// 0. An ambiguous claim failure (a TryStart store error) is surfaced
		//    first: the claim outcome is unknown, so the message stays pending
		//    with no handler execution and no retry accounting, and the distinct
		//    sentinel keeps the cause visible even when another invocation also
		//    failed this delivery.
		if claimErr != nil {
			return claimErr
		}
		// 1. Any retryable failure this delivery → the message stays pending
		//    (retryable): return the first such failure, even if other
		//    invocations succeeded or are otherwise still running.
		if firstErr != nil {
			return firstErr
		}
		// 2. A matched-but-unavailable invocation is unresolved (it matched and
		//    has not already completed) → the message stays pending and is NOT
		//    ACKed, and it is NOT dead-lettered solely for unavailability. No
		//    handler attempt was claimed and no handler counter was touched for
		//    it (no handler ran). Returning the retryable ErrAppUnavailable
		//    makes the stream leave the message pending (an ordinary retryable
		//    error), exactly like the schedule path's unavailable handling, so a
		//    later delivery — after the app is rebuilt and available again —
		//    completes the outstanding work. This is checked BEFORE the DLQ
		//    decision so an exhausted sibling can never dead-letter a message
		//    whose unavailable invocation is still outstanding.
		if len(unresolvedUnavailable) > 0 {
			return unavailableMatchError(unresolvedUnavailable)
		}
		// 3. Every matched invocation is terminal (complete or exhausted) AND at
		//    least one exhausted → the message is terminal; route it to the DLQ.
		//    allMatchedTerminal fails open to false on a read error, keeping the
		//    message pending rather than DLQ'ing it. The aggregate error carries
		//    EVERY exhausted invocation's exact app/handler/attempt metadata
		//    and wraps stream.ErrInvocationExhausted, so the stream layer writes
		//    one correctly-attributed DLQ entry per exhausted invocation.
		if anyExhausted && allMatchedTerminal(invState, matched) {
			return &stream.HandlerExhaustedError{Invocations: dedupeExhausted(exhaustedInvocations)}
		}
		// 4. Any matched invocation was protected- or slot-timeout-skipped
		//    (unresolved) → the message stays pending with NO retry accounting.
		//    This fires even when other invocations executed successfully this
		//    call: an unresolved invocation must not be ACKed away. A claim
		//    failure was already surfaced at step 0.
		if skippedPending {
			return stream.ErrInvocationNotEligible
		}
		// 5. Every matched invocation is complete (or nothing matched) → ACK.
		return nil
	}
	// No invocation state: preserve the old fail-fast tail — nil when something
	// executed successfully (or nothing matched); ErrInvocationNotEligible when
	// nothing executed and at least one invocation was protected- or
	// slot-timeout-skipped (so the stream leaves the message pending). A
	// matched-but-unavailable invocation is likewise unresolved: it is reported
	// (never silently treated as unmatched/complete) so the caller leaves the
	// message pending. With no state there is no terminal marker to consult, so
	// any unavailable match counts as unresolved.
	if len(unresolvedUnavailable) > 0 {
		return unavailableMatchError(unresolvedUnavailable)
	}
	if executed {
		return nil
	}
	if skippedPending {
		return stream.ErrInvocationNotEligible
	}
	return nil
}

// unavailableMatchError reports the matched-but-unavailable invocations of a
// message as a retryable error wrapping ErrAppUnavailable. An unavailable
// app's event is MATCHED (not unmatched) and must keep the message pending
// without being dead-lettered for unavailability alone, so the stream layer
// treats this exactly like any other retryable failure. The invocation IDs are
// included so the log line names the outstanding app/handler.
func unavailableMatchError(invocations []string) error {
	return fmt.Errorf("%w: matched but unavailable: %s", ErrAppUnavailable, strings.Join(invocations, ", "))
}

// obsoleteOccurrence is the terminal error returned when a schedule occurrence
// names an app or schedule that is no longer in the configuration. The
// stream layer recognizes ErrInvocationObsolete and ACKs the message: an
// obsolete occurrence is never retried or dead-lettered.
func obsoleteOccurrence(fnName, scheduleName string) error {
	return fmt.Errorf(
		"%w: schedule app %q schedule %q no longer in configuration",
		stream.ErrInvocationObsolete, fnName, scheduleName,
	)
}

// InvokeHandler executes a single schedule-occurrence invocation routed through
// the stream. Its admission boundary is deliberate and documented here:
//
// BEFORE ADMISSION (no descriptor pinned for the message): the occurrence
// resolves the CURRENT template by its STABLE schedule NAME on every delivery, so
// a handler/timeout/retries change under the same name takes effect and the
// name's current handler is what runs. Because an occurrence may block for a long
// time waiting for a concurrency slot, this config snapshot is refreshed from the
// registry AFTER the slot is admitted and immediately before the atomic first
// claim, so a reload that completed during the wait is observed. A missing NAME
// means the schedule was removed while the occurrence was pending; if nothing was
// ever pinned the occurrence is OBSOLETE, and — with invocation state — the
// stream ACKs it (never retried, never dead-lettered).
//
// AT ADMISSION (the first successful claim): the resolved descriptor — schedule
// name, handler, the CAPPED timeout, and the retry budget — is pinned ATOMICALLY
// with the claim (stream.InvocationState.TryStartScheduled), so a concurrent
// replica with a different template, or a reload racing the post-slot refresh,
// can never diverge: exactly one descriptor wins and every other delivery adopts
// it. Redis is therefore the global first-writer: a reload concurrent with that
// atomic first claim is treated as a competing proposal, and whichever descriptor
// the script persists is authoritative.
//
// AFTER ADMISSION (a descriptor is pinned): the descriptor is the single source
// of truth. The pinned handler, timeout, and retries are used EVEN IF the current
// schedule was renamed, retimed, or REMOVED, so an admitted invocation completes
// its retry/DLQ lifecycle instead of being cancelled by a config change. The
// schedule name remains the occurrence/dedup identity, but a pinned message no
// longer consults the current template for execution.
//
// The handler timeout is capped at the configured maximum exactly like Handle
// caps rule timeouts, and the CAPPED value is passed BOTH to admission (persisted
// as the running deadline) and to context.WithTimeout so the persisted deadline
// matches the local kill timer. The capped value is what the descriptor pins, so
// it stays stable across attempts.
//
// It reuses the exact event execution path: registry snapshot lookup, the global
// + per-app concurrency slots, per-invocation secret resolution, the panic
// boundary, and the same handler metrics. Invocation/DLQ attribution stays
// handler-based ("<app>/<handler>"); the descriptor adds schedule provenance
// only.
//
// When scheduleName is empty (the state-free DLQ-replay path, which knows only a
// recorded app/handler), resolution falls back to the first schedule entry
// matching the handler, then to that exact event rule (never event matching), and
// the no-state path executes a single attempt exactly as before.
//
// InvokeHandler takes only the app/schedule/handler strings; it does not carry
// the occurrence's scheduled instant, so it performs no semantic validation.
// The production stream entry point is InvokeOccurrence, which additionally
// validates the occurrence's scheduled_at against the current schedule before
// admission.
func (r *Runner) InvokeHandler(
	ctx context.Context, msgID, fnName, scheduleName, handler string, payload []byte,
) error {
	return r.invokeHandler(ctx, msgID, fnName, scheduleName, handler, payload, nil)
}

// InvokeOccurrence executes one schedule occurrence routed through the stream.
// It is InvokeHandler's production entry point: the decoded Occurrence (app,
// stable schedule name, envelope handler, scheduled instant, and derived
// identity) is passed so that, BEFORE admission, the occurrence is semantically
// validated against the CURRENT template — the named app must be configured and
// available, the stable schedule name must exist, and scheduled_at must be a
// real firing of that schedule's current cron/timezone (see validateOccurrence).
//
// The envelope handler is NEVER the execution target: only the app/schedule
// name select the invocation, and the current template's handler is what runs.
// The envelope handler is carried for provenance/logging only, so a handler
// change under the same name remains a valid live change.
//
// Validation applies only to a not-yet-admitted occurrence. Once a descriptor
// is pinned (first successful admission), it is authoritative and the
// occurrence completes its lifecycle even if the schedule was since changed or
// removed (see InvokeHandler). The state-free path (no invocation state) keeps
// its legacy single-attempt behavior and does not validate.
func (r *Runner) InvokeOccurrence(
	ctx context.Context, msgID string, occ schedule.Occurrence, payload []byte,
) error {
	return r.invokeHandler(ctx, msgID, occ.App, occ.Schedule, occ.Handler, payload,
		func(tmpl *app.Template) error { return validateOccurrence(tmpl, occ) })
}

// invokeHandler is the shared implementation of InvokeHandler and
// InvokeOccurrence. validate, when non-nil, is the occurrence's pre-admission
// semantic validation, called with the CURRENT template once the app and
// schedule name have resolved and the app is available. It runs only for an
// unadmitted occurrence (no pinned descriptor) whose schedule name still exists;
// a missing name is left to the existing obsolete path, and an admitted
// occurrence is authoritative and never re-validated.
func (r *Runner) invokeHandler(
	ctx context.Context, msgID, fnName, scheduleName, handler string, payload []byte,
	validate func(tmpl *app.Template) error,
) error {
	// Invocation state (when present) distinguishes the production stream path
	// from direct callers/tests: obsolete-removal and descriptor pinning only
	// apply on the production path. See the availability checks below.
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Find the app in the current registry. GetByName returns nil only when
	// the app is ABSENT (removed) — a present but unavailable app
	// returns a non-nil entry whose Prepared() is nil. These two cases must be
	// handled differently:
	//   - absent (removed): an intentional configuration change, so an occurrence
	//     for it is OBSOLETE and terminal (ACKed, never retried/DLQ'd) — but only
	//     on the production state-carrying path; direct callers/tests keep the
	//     legacy plain error. This holds even for an already-admitted occurrence:
	//     with no app there is no image or executor to run, so it cannot
	//     complete. (The schedule NAME, not the app, is what admission pins;
	//     a still-present app whose schedule was removed is NOT obsolete once
	//     admitted — see the descriptor path below.)
	//   - present but unavailable: temporary (build failed at startup/reconcile),
	//     retryable exactly as today.
	//
	// The lookup PINS the app's published image (a share of its publication
	// lease, acquired under the registry lock), held until this invocation
	// returns, so a concurrent retirement cannot remove the image between this
	// lookup and the handler's execution. An UNADMITTED schedule occurrence
	// re-reads the current entry after the slot wait below and swaps in that
	// entry's pin (see the refresh there); the closure releases whichever lease
	// `pin` holds at return, so the stale lookup's lease is not leaked.
	pf, pin := r.reg.getByNamePinned(fnName)
	defer func() { pin.Release() }()
	if pf == nil {
		if hasState {
			r.log.Warn("Schedule: occurrence obsolete; app removed; acknowledging",
				"app", fnName,
				"schedule", scheduleName,
			)
			return obsoleteOccurrence(fnName, scheduleName)
		}
		r.log.Warn("Schedule: app is not available", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}
	if pf.Prepared() == nil {
		// The app exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the app
		// is still configured).
		r.log.Warn("Schedule: app is temporarily unavailable", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}

	// Reserve the worker-global and per-app concurrency slots BEFORE the
	// admission claim so a blocked invocation is never counted as an attempt and
	// does not persist state (same ordering as Handle's event path). A slot
	// timeout means the invocation is unresolved: with invocation state it
	// returns a "not eligible" skip (the stream leaves the message pending with no
	// retry accounting); without state it preserves the legacy plain error. The
	// reservation is made for both the state-free and state-carrying paths, so a
	// held app slot also bounds a direct/no-state caller.
	releaseSlots, _ := r.reserveSlots(ctx, fnName, pf.fn.Template.Concurrency)
	if releaseSlots == nil {
		r.log.Warn("Schedule: concurrency slot wait timed out", "app", fnName, "handler", handler)
		if hasState {
			return stream.ErrInvocationNotEligible
		}
		return fmt.Errorf("schedule invocation: concurrency slot wait timed out")
	}
	defer releaseSlots()

	// The state-free path (direct callers/tests, DLQ replay) preserves the legacy
	// single-attempt behavior exactly: resolve the current schedule, cap the
	// timeout, and execute once. No descriptor, no broker lifecycle.
	if !hasState {
		desc := resolveScheduleDescriptor(pf.fn.Template, scheduleName, handler)
		if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && desc.Timeout > cap {
			desc.Timeout = cap
		}
		return r.invokeOnce(ctx, pf, desc.Handler, payload, desc.Timeout, pin, nil, "", stream.InvocationClaim{}, msgID)
	}

	// Read the pinned schedule descriptor ONCE (a Redis read, and the
	// authoritative source for an already-admitted occurrence). A descriptor
	// pinned by an earlier first admission is immutable while the message is
	// recoverable, so this read is stable for the rest of the delivery.
	desc, known := invState.ScheduleDescriptor()
	// admitted records whether a descriptor was already pinned at the START of
	// this delivery. `known` is later reassigned when the current template can
	// propose a descriptor, so this is the stable "is this occurrence
	// already-admitted?" signal the pre-admission validation keys on.
	admitted := known

	// An UNADMITTED schedule occurrence (scheduleName != "" and no pinned
	// descriptor) resolves its template by the stable schedule NAME, and it may
	// have waited arbitrarily long for a concurrency slot while a reload
	// hot-swapped or removed the app. The lookup above is stale for that
	// occurrence: refresh it from the registry under the lock and pin the NEWEST
	// entry, dropping the stale pin, so the descriptor proposed for the atomic
	// first admission comes from the CURRENT config. A reload racing AFTER this
	// fresh snapshot is a concurrent proposal: the atomic descriptor write in
	// TryStartScheduled remains the global first-winner boundary. A pinned
	// descriptor is authoritative and is NEVER re-resolved (its admitted schedule
	// may already be gone), but the app's availability is still rechecked.
	if scheduleName != "" && !known {
		// getByNamePinned returns a FRESH share of the current entry's
		// publication lease (always a distinct lease object, or nil when the
		// entry has none), so release the stale lookup's pin and adopt the fresh
		// one unconditionally — even when fresh is nil, so an app removed
		// while the occurrence waited drops the stale pin with it.
		fresh, freshPin := r.reg.getByNamePinned(fnName)
		pin.Release()
		pf, pin = fresh, freshPin
	}
	if pf == nil {
		r.log.Warn("Schedule: occurrence obsolete; app removed; acknowledging",
			"app", fnName,
			"schedule", scheduleName,
		)
		return obsoleteOccurrence(fnName, scheduleName)
	}
	if pf.Prepared() == nil {
		// The app exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the
		// app is still configured).
		r.log.Warn("Schedule: app is temporarily unavailable", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}

	// Resolve the descriptor that owns this message. A pinned descriptor (read
	// above) is authoritative and survives a later template change or the
	// schedule's removal; otherwise resolve the CURRENT template by schedule NAME
	// (the production path) or by handler (the state-carrying fallback).
	if !known {
		desc = resolveScheduleDescriptor(pf.fn.Template, scheduleName, handler)
		// The production path carries a schedule NAME. When the app is
		// present but that NAME is gone, the occurrence may have been admitted by
		// ANOTHER replica whose template still had it, so we must NOT pre-empt the
		// atomic admission: pass no descriptor (known=false) and let the script
		// hand back the pinned one to adopt, or report it obsolete.
		if scheduleName != "" && !scheduleNameExists(pf.fn.Template, scheduleName) {
			known = false
		} else {
			// A resolved name, or the state-carrying handler fallback: a descriptor
			// is known (a fallback with no matching entry keeps the defaults).
			known = true
		}
	}
	// Pre-admission semantic validation (schedule-occurrence entry point only):
	// the app is present and available, so validate the occurrence against the
	// CURRENT template — the stable name must resolve to a schedule and
	// scheduled_at must be a real firing of that schedule's current
	// cron/timezone. A MISSING name is deliberately a no-op here: the atomic
	// admission below already decides between adopting a descriptor pinned by
	// another replica and reporting the never-admitted occurrence obsolete. A
	// pinned (already-admitted) occurrence is authoritative and never
	// re-validated, so a schedule edited or removed after admission still
	// completes under its pinned contract.
	//
	// On failure the disposition is deliberately NOT decided here: the caller
	// proposes no descriptor (known=false) and the atomic admission below still
	// observes any descriptor a concurrent replica has pinned, so an admitted
	// occurrence is adopted rather than raced into an ACK (no ACK hazard). Only
	// an admission that finds nothing pinned AND no proposal is then reported as
	// invalid, exactly where a never-admitted occurrence is reported obsolete.
	var invalidErr error
	if validate != nil && !admitted {
		if invalidErr = validate(pf.fn.Template); invalidErr != nil {
			r.log.Warn("Schedule: invalid occurrence claim; deferring to admission",
				"app", fnName,
				"schedule", scheduleName,
				"reason", invalidErr,
			)
			known = false
		}
	}

	// Cap the timeout exactly like Handle caps each rule's timeout (defense in
	// depth; template validation enforces the cap at load). When the descriptor is
	// pinned, its timeout was already capped at admission, so re-capping is a
	// no-op that keeps a hand-built/oversized pinned value safe.
	if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && desc.Timeout > cap {
		desc.Timeout = cap
	}

	// Atomically pin the descriptor (only when none is pinned) and claim the
	// invocation. A lost descriptor race is resolved inside the handle: the
	// pinned descriptor is adopted and the admission retried, so the winning
	// handler/timeout/retries is what executes and no second invocation field is
	// ever claimed.
	admission, err := invState.TryStartScheduled(desc, known, func(d stream.ScheduleDescriptor) string {
		return fnName + "/" + d.Handler
	})
	if err != nil {
		// The claim outcome is unknown (Redis/transport or token-generation
		// error): do NOT run the handler and leave the message pending so a later
		// delivery retries the claim. No handler attempt was confirmed. The error
		// wraps ErrInvocationNotEligible (the stream's pending/no-ACK contract)
		// plus ErrInvocationClaimUnconfirmed so the cause is distinguishable.
		r.log.Warn("Schedule: claim failed (outcome unknown); leaving pending without executing",
			"app", fnName,
			"handler", desc.Handler,
			"error", err,
		)
		return fmt.Errorf("%w: %v: %w: %w",
			stream.ErrInvocationNotEligible, fnName+"/"+desc.Handler,
			stream.ErrInvocationClaimUnconfirmed, err)
	}
	if admission.Obsolete {
		// Nothing was ever pinned and the caller proposed nothing. For the
		// schedule-occurrence entry point that can mean two distinct things:
		//
		//   - the schedule NAME is gone from the current template: an intentional
		//     removal, so the occurrence is OBSOLETE and the stream ACKs it; or
		//   - the name exists but the occurrence is semantically invalid (a
		//     non-firing scheduled_at, or an unparseable current cron): the claim
		//     can never succeed, so it is INVALID and the stream dead-letters it.
		//
		// invalidErr distinguishes them. Removal semantics are preserved exactly.
		if invalidErr != nil {
			r.log.Warn("Schedule: occurrence invalid; routing to DLQ",
				"app", fnName,
				"schedule", scheduleName,
				"reason", invalidErr,
			)
			return invalidErr
		}
		r.log.Warn("Schedule: occurrence obsolete; schedule no longer in template; acknowledging",
			"app", fnName,
			"schedule", scheduleName,
		)
		return obsoleteOccurrence(fnName, scheduleName)
	}

	// The descriptor that owns the message (adopted on a lost race, else our own).
	ed := admission.Descriptor
	handler = ed.Handler
	timeout := ed.Timeout
	retries := ed.Retries
	invocation := fnName + "/" + handler

	if !admission.Started {
		if admission.Wait > 0 {
			// Protected by an active running deadline or a retry backoff. The
			// message must stay pending: the protected invocation may still
			// complete or fail on its own, so this is a "not eligible" skip, never
			// an ACK.
			r.log.Debug("Schedule: invocation not eligible (running or waiting for retry); leaving pending",
				"app", fnName,
				"handler", handler,
				"handler_attempt", admission.Claim.Attempt,
				"next_attempt_in", admission.Wait,
			)
			return stream.ErrInvocationNotEligible
		}
		// Terminal. Attempt 0 is a complete (or terminal-retained) invocation, so
		// the message is ACKed. A positive attempt is an exhausted invocation: a
		// schedule has exactly ONE invocation, so the message is terminal and must
		// route to the DLQ, with the persisted exhausted attempt carried on the
		// typed error for DLQ attribution.
		if admission.Claim.Attempt == 0 {
			r.log.Debug("Schedule: invocation already succeeded; skipping (stream ACKs)",
				"app", fnName,
				"handler", handler,
			)
			return nil
		}
		r.log.Debug("Schedule: invocation terminal (exhausted); routing to DLQ",
			"app", fnName,
			"handler", handler,
			"handler_attempt", admission.Claim.Attempt,
		)
		return &stream.HandlerExhaustedError{
			Invocations: []stream.ExhaustedInvocation{{
				App:      fnName,
				Handler:  handler,
				Attempts: admission.Claim.Attempt,
			}},
		}
	}

	err = r.invokeOnce(ctx, pf, handler, payload, timeout, pin, invState, invocation, admission.Claim, msgID)
	if err != nil {
		// A stale completion (the claim was superseded or the marker is already
		// terminal) is not a handler failure: do NOT ACK a superseded claim's
		// outcome — leave the message pending so a later delivery resolves it.
		if errors.Is(err, stream.ErrInvocationNotEligible) {
			return err
		}
		// A failed attempt — resolve extra env, marshal, execution, or timeout
		// failures all land here. recordFailure decides retry vs exhaustion using
		// the ADMITTED descriptor's retry budget: a retryable failure schedules a
		// backoff and returns a plain error (the stream leaves the message pending,
		// gated by next_attempt_at); an exhausted attempt marks the invocation
		// terminal and returns a typed *stream.HandlerExhaustedError (which already
		// wraps stream.ErrInvocationExhausted and carries the exhausted handler
		// attempt). A schedule has exactly ONE invocation (this one), so the message
		// is terminal and the stream routes it to the DLQ.
		outcome, retErr := r.recordFailure(invState, invocation, admission.Claim, retries, fnName, handler, msgID, err)
		if outcome == outcomePendingSkip {
			// A stale transition (the claim was superseded, or the marker is
			// already terminal): do NOT ACK a superseded claim's outcome — leave
			// the message pending so a later delivery resolves it.
			return stream.ErrInvocationNotEligible
		}
		return retErr
	}
	return nil
}

// resolveScheduleDescriptor resolves a schedule occurrence's descriptor from the
// app's CURRENT template. When scheduleName is non-empty the schedule is
// resolved by its STABLE NAME (the production identity); a handler change under
// the same name is picked up, and a missing NAME leaves the envelope handler with
// the defaults (the caller decides whether that is obsolete). When scheduleName
// is empty (the state-free DLQ-replay path) it resolves by exact handler: first
// among the current schedules, then among event rules. This is never event
// matching — it selects by handler string alone, so no other rule can run. The
// returned descriptor's Timeout is NOT capped here; the caller caps it (and the
// capped value is what gets pinned).
func resolveScheduleDescriptor(tmpl *app.Template, scheduleName, handler string) stream.ScheduleDescriptor {
	desc := stream.ScheduleDescriptor{
		Handler: handler,
		Timeout: app.DefaultTimeout,
		Retries: app.DefaultRetries,
	}
	found := false
	if scheduleName != "" {
		for _, sch := range tmpl.Schedules {
			if sch.Name == scheduleName {
				desc.Schedule = scheduleName
				desc.Handler = sch.Handler
				desc.Timeout = sch.Timeout
				desc.Retries = sch.Retries
				found = true
				break
			}
		}
	}
	if !found {
		for _, sch := range tmpl.Schedules {
			if sch.Handler == handler {
				desc.Handler = handler
				desc.Timeout = sch.Timeout
				desc.Retries = sch.Retries
				found = true
				break
			}
		}
	}
	if !found {
		for _, rule := range tmpl.Events {
			if rule.Handler == handler {
				if rule.Timeout > 0 {
					desc.Timeout = rule.Timeout
				}
				desc.Retries = rule.Retries
				break
			}
		}
	}
	return desc
}

// scheduleNameExists reports whether the template still declares a schedule with
// the given stable name. The production path uses it only to decide whether a
// first-time (unpinned) occurrence can be admitted locally, never to cancel an
// already-admitted one.
func scheduleNameExists(tmpl *app.Template, scheduleName string) bool {
	for _, sch := range tmpl.Schedules {
		if sch.Name == scheduleName {
			return true
		}
	}
	return false
}

// scheduleByName returns the template's schedule with the given stable NAME, and
// whether it exists.
func scheduleByName(tmpl *app.Template, scheduleName string) (app.Schedule, bool) {
	for _, sch := range tmpl.Schedules {
		if sch.Name == scheduleName {
			return sch, true
		}
	}
	return app.Schedule{}, false
}

// validateOccurrence is the schedule occurrence's pre-admission semantic
// validation against the CURRENT template. It is called only once the app is
// present and available and no descriptor has been pinned (an admitted
// occurrence is authoritative and never re-validated).
//
// It enforces two invariants, using the schedule's stable NAME and the shared
// firing primitive:
//
//   - The name must resolve to a currently-configured schedule. A name that is
//     gone is NOT rejected here: returning nil lets the existing admission path
//     decide between adopting a descriptor pinned by another replica and
//     reporting the never-admitted occurrence OBSOLETE (ACKed, never retried or
//     dead-lettered). This keeps removal semantics exactly as they were.
//   - scheduled_at must be a real firing of that schedule's current cron in its
//     effective timezone (see schedule.IsFiring), so timezone/DST handling is
//     the parser's, not a re-implementation. A schedule whose own cron cannot be
//     parsed is a corrupt configuration: it is surfaced as invalid (a bounded,
//     non-retryable DLQ) rather than silently accepted, because no instant can
//     be a firing of an unparseable schedule.
//
// The envelope handler plays no part: the schedule is selected by name and its
// current handler is the execution target, so a handler change under the same
// name stays valid.
func validateOccurrence(tmpl *app.Template, occ schedule.Occurrence) error {
	sch, ok := scheduleByName(tmpl, occ.Schedule)
	if !ok {
		// Missing name: left to the admission path's obsolete decision.
		return nil
	}
	if sch.Location == nil {
		return fmt.Errorf("%w: app %q schedule %q has no timezone",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule)
	}
	firing, err := schedule.IsFiring(sch.Cron, sch.Location, occ.ScheduledAt)
	if err != nil {
		return fmt.Errorf("%w: app %q schedule %q has an unparseable cron %q: %v",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule, sch.Cron, err)
	}
	if !firing {
		return fmt.Errorf("%w: app %q schedule %q scheduled_at %s is not a firing of cron %q in %s",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule,
			occ.ScheduledAt.UTC().Format(time.RFC3339), sch.Cron, sch.Location)
	}
	return nil
}

// InvokeApp executes every event rule of the named app whose pattern
// matches event, in declaration order, and returns the number of handlers
// invoked together with a concise error describing any failures. It is the
// synchronous manual-invocation primitive behind `relay app invoke`,
// driven by the worker over its query socket against the LIVE runner/runtime
// pool.
//
// It deliberately reuses the event execution path: the current registry
// snapshot, the worker-global + per-app concurrency slots, the rule
// timeout (capped at the configured maximum exactly like Handle), per-invocation
// env/secret resolution, the image in-flight reference, the panic boundary, the
// normal runtime executor (Manager.Execute), and the handler success/failure
// execution metrics. It deliberately does NOT reuse the broker lifecycle: it
// never consults stream.InvocationState, never claims or counts the event
// classification counters (events_received/matched/unmatched, and not
// app_events_matched_total), never schedules a retry, and never
// dead-letters. A manual invocation is an operator action, not a stream
// delivery, so no broker state is written.
//
// Failure semantics mirror Handle's aggregate behavior where it is sensible: a
// failure in one matching handler does NOT prevent the later matching handlers
// from running, and the returned error is the first failure after every matching
// handler has been attempted. Matching zero rules is not an error: it returns
// (0, nil) so the caller renders "no matching handlers".
//
// An app absent from the current registry returns ErrAppNotFound; a
// registered but unrunnable app (its image could not be built) returns
// ErrAppUnavailable. Both are wrapped with the app name so the socket
// can map them onto stable wire codes.
//
// Tracing: the worker-side root operation span is opened BEFORE registry and
// match validation, so an invalid app, an unavailable app, or a
// no-match invocation is still traced (with its error recorded) without changing
// any return semantics.
func (r *Runner) InvokeApp(
	ctx context.Context, name string, event map[string]any,
) (count int, err error) {
	// A worker-side root operation span around the whole manual invocation, so
	// an operator `relay app invoke` is traced on the worker without the
	// CLI carrying any trace context. It is a NEW root regardless of the
	// caller's context; every matching rule's function.invoke runs as its child
	// (the root span's context is the base for each rule's invokeCtx). It is
	// opened before validation and finalized by the deferred finalizer on every
	// return path (success, validation error, or handler failure).
	opCtx, opSpan := startManualInvokeSpan(ctx, name)
	ctx = opCtx
	defer func() { finishOperationSpan(opSpan, err) }()

	pf, pin := r.reg.getByNamePinned(name)
	if pin != nil {
		defer pin.Release()
	}
	if pf == nil {
		return 0, fmt.Errorf("%w: %q", ErrAppNotFound, name)
	}
	if pf.Prepared() == nil {
		return 0, fmt.Errorf("%w: %q", ErrAppUnavailable, name)
	}

	// Matching is pure and in declaration order. An app with no matching
	// rule is a successful no-op.
	rules := eventmatch.MatchingEventRules(pf.fn.Template.Events, event)
	if len(rules) == 0 {
		return 0, nil
	}

	// Marshal the supplied event once for every matching rule. A marshal
	// failure is a programming error in the caller's decoded value; report it
	// rather than executing a handler with a corrupt payload.
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("app %q: marshal event: %w", name, err)
	}
	eventID, eventName := eventFields(event)

	var firstErr error
	for _, rule := range rules {
		// Cap the rule timeout at the configured maximum (defense in depth;
		// template validation enforces the cap at load), exactly like Handle.
		timeout := rule.Timeout
		if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
			timeout = cap
		}

		// Reserve the worker-global and per-app concurrency slots, exactly
		// like the event path. A slot timeout is not an execution: record the
		// first such error and keep trying the later rules (which may themselves
		// be blocked, in which case they are reported the same way).
		releaseSlots, _ := r.reserveSlots(ctx, name, pf.fn.Template.Concurrency)
		if releaseSlots == nil {
			r.log.Warn("App invoke: concurrency slot wait timed out",
				"app", name,
				"handler", rule.Handler,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("app %q handler %q: concurrency slot wait timed out", name, rule.Handler)
			}
			continue
		}

		err := func() error {
			defer releaseSlots()

			// The handler attempt is about to begin: this is the
			// last_execution_at attribution point, exactly as Handle's rule loop
			// and InvokeHandler stamp it. A slot timeout above never reaches here
			// (it is not an execution).
			r.metrics.SetAppTimestamp(name, metrics.AppTimestampExecution, time.Now().Unix())

			extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
			if err != nil {
				r.recordHandlerFailure(name, rule.Handler, 0)
				return fmt.Errorf("app %q handler %q: %w", name, rule.Handler, err)
			}

			invokeCtx, cancel := context.WithTimeout(ctx, timeout)
			// Stamp the invocation's diagnostic metadata so the execution
			// container carries its owner and identity labels. The type is the
			// event one-shot type: a manual invocation runs an event rule against
			// an operator-supplied event, so it belongs to the same execution
			// population (and is swept by hostname if the worker dies mid-call).
			// MessageID is empty: there is no stream message.
			invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
				Type:      runtime.ContainerTypeEvent,
				App:       name,
				Handler:   rule.Handler,
				EventID:   eventID,
				EventName: eventName,
				Hostname:  r.hostname,
				Image:     toImage(pf),
			})

			start := time.Now()
			panicked, panicValue, err := r.runInvocation(
				pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv,
				pin, invocationTrace{},
			)
			elapsed := time.Since(start)
			if panicked {
				r.log.Error("App invoke: handler PANICKED",
					"app", name,
					"handler", rule.Handler,
					"panic_value", fmt.Sprintf("%v", panicValue),
					"stack", string(debug.Stack()),
				)
			}
			if err != nil {
				r.recordHandlerFailure(name, rule.Handler, elapsed)
				r.log.Warn("App invoke: handler execution failed",
					"app", name,
					"handler", rule.Handler,
					"duration", elapsed,
					"reason", err,
				)
				return fmt.Errorf("app %q handler %q: %w", name, rule.Handler, err)
			}
			r.recordHandlerSuccess(name, rule.Handler, elapsed)
			r.log.Info("App invoke: handler executed",
				"app", name,
				"handler", rule.Handler,
				"duration", elapsed,
			)
			return nil
		}()
		// Count every handler whose execution was attempted, including a failed
		// attempt: the count answers "how many handlers ran", and the caller
		// only prints it when there is no error anyway.
		count++
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return count, firstErr
	}
	return count, nil
}

// ReplayDLQ re-executes the exact app/handler recorded by one DLQ entry
// against the CURRENT registry, for the `relay dlq replay` command. It is the
// DLQ counterpart of InvokeApp: an operator action against the LIVE runner,
// never a stream delivery.
//
// It validates the current configuration and then delegates to InvokeHandler:
//
//   - an app absent from the current registry returns ErrAppNotFound;
//   - a registered but unrunnable app (image build failed) returns
//     ErrAppUnavailable;
//   - a handler no longer present in the app's current template (its event
//     rules or schedules) returns ErrHandlerNotFound — the entry is retained and
//     the operator sees that the configuration changed.
//
// The delegation runs with a context carrying NO stream.InvocationState (an
// explicit opt-out, so it holds even if the caller's context came from the stream
// delivery path), so InvokeHandler takes its state-free single-attempt path:
// exactly one synchronous execution of the named handler (never event matching,
// so no other rule can run), against the app's current runtime, env/secrets
// resolution, concurrency slots, and timeout resolution. It writes NO broker
// state — no invocation-state hash, no event classification counters, and no
// retry/DLQ counters (recordFailure is only reached on the state-carrying path).
// It DOES record the normal handler success/failure execution metrics, exactly as
// a manual invocation does, because those are execution outcomes rather than
// broker state.
//
// The handler timeout follows InvokeHandler's current resolution: the matching
// schedule entry's timeout when the handler has one, otherwise the CURRENT event
// rule's timeout for that exact handler, otherwise the app default — always
// capped by the configured maximum. Resolving the event rule is by exact handler
// string, never event matching, so the replay still runs exactly one handler.
// event is the entry's original payload, replayed verbatim.
//
// Tracing: a `dlq.replay` span wraps the whole replay as a NEW ROOT worker
// operation (independent of the caller's context), carrying a span link to the
// original failed invocation when lineage is supplied (the DLQ entry's optional
// compact trace reference, forwarded by the control path — never embedded in the
// event JSON). The replayed `function.invoke` runs as its child. An empty or
// malformed lineage simply omits the link; the replay still runs. The operation
// span is opened BEFORE registry/handler validation, so a removed app or
// handler is still traced (with its error recorded) without changing any return
// semantics.
func (r *Runner) ReplayDLQ(ctx context.Context, fnName, handler string, event []byte, lineage string) (err error) {
	// The replay operation is a new root worker span; its child function.invoke
	// (started via InvokeHandler below) inherits this context. It is opened
	// before validation and records the terminal outcome through the deferred
	// finalizer on every return path.
	opCtx, opSpan := startReplaySpan(ctx, fnName, handler, lineage)
	defer func() { finishOperationSpan(opSpan, err) }()

	pf := r.reg.GetByName(fnName)
	if pf == nil {
		return fmt.Errorf("%w: %q", ErrAppNotFound, fnName)
	}
	if pf.Prepared() == nil {
		return fmt.Errorf("%w: %q", ErrAppUnavailable, fnName)
	}
	if !templateHasHandler(pf.fn.Template, handler) {
		return fmt.Errorf("%w: app %q handler %q", ErrHandlerNotFound, fnName, handler)
	}
	// Strip any inherited invocation state and run the state-free path: no
	// TryStart/complete/retry/exhaustion, and no DLQ accounting. The opt-out
	// makes the guarantee explicit even when the caller's context came from the
	// stream delivery path.
	return r.InvokeHandler(stream.WithoutInvocationState(opCtx), "", fnName, "", handler, event)
}

// templateHasHandler reports whether handler is present in the template's
// current event rules or schedules. A DLQ entry attributes an exhausted
// invocation that came from one of those, so either counts as "still
// configured". A nil template has no handlers.
func templateHasHandler(tmpl *app.Template, handler string) bool {
	if tmpl == nil {
		return false
	}
	for _, rule := range tmpl.Events {
		if rule.Handler == handler {
			return true
		}
	}
	for _, sch := range tmpl.Schedules {
		if sch.Handler == handler {
			return true
		}
	}
	return false
}

// invokeOnce is the smallest reusable single-handler execution core shared by
// InvokeHandler (with or without invocation state): it resolves the per-invocation
// template env/secrets, runs the handler inside the panic boundary and the
// caller-provided capped timeout, and emits the handler success/failure metrics.
// It assumes the caller has already reserved the concurrency slots; on success,
// when invState is non-nil it marks the invocation complete BEFORE the success
// metrics so a crash between the side effect and MarkComplete re-runs the
// handler (at-least-once; the handler must remain idempotent), mirroring
// Handle's per-rule ordering. It returns the plain execution error (nil on
// success) that the caller routes through the invocation-state lifecycle.
func (r *Runner) invokeOnce(
	ctx context.Context,
	pf *PreparedApp,
	handler string,
	payload []byte,
	timeout time.Duration,
	lease *runtime.ImageLease,
	invState stream.InvocationState,
	invocation string,
	claim stream.InvocationClaim,
	msgID string,
) error {
	// The invocation attempt has actually begun: the caller claimed it via
	// TryStart (or is a direct no-state caller) and holds the concurrency
	// slots. This is the schedule path's last_execution_at attribution point —
	// every claimed attempt counts, retries included.
	r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampExecution, time.Now().Unix())

	// Resolve the template's env values and secret references immediately before
	// container creation, mirroring Handle's rule path.
	extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
	if err != nil {
		// Counted as a handler failure with a zero duration (no execution
		// happened), mirroring how Handle attributes a resolution failure.
		r.recordHandlerFailure(pf.fn.Name, handler, 0)
		return fmt.Errorf("schedule invocation: app %q handler %q: %w", pf.fn.Name, handler, err)
	}

	r.log.Debug("Schedule: invoking handler",
		"app", pf.fn.Name,
		"handler", handler,
	)

	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	// Stamp the invocation's diagnostic metadata. MessageID is the schedule
	// occurrence's real Redis stream message ID, stamped on relay.message_id;
	// EventID/EventName are empty. Type: containerTypeSchedule is the strict
	// relay.type marker that classifies this one-shot invocation container.
	invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
		Type:      runtime.ContainerTypeSchedule,
		App:       pf.fn.Name,
		Handler:   handler,
		MessageID: msgID,
		Hostname:  r.hostname,
		Image:     toImage(pf),
	})
	start := time.Now()
	panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, handler, payload, extraEnv, lease,
		invocationTrace{state: invState, invocation: invocation, attempt: claim.Attempt})
	elapsed := time.Since(start)
	if panicked {
		r.log.Error("App handler: PANICKED for schedule",
			"app", pf.fn.Name,
			"handler", handler,
			"panic_value", fmt.Sprintf("%v", panicValue),
			"stack", string(debug.Stack()),
		)
		r.recordHandlerFailure(pf.fn.Name, handler, elapsed)
		return err
	}
	if err != nil {
		r.recordHandlerFailure(pf.fn.Name, handler, elapsed)
		r.log.Warn("App handler: execution failed for schedule",
			"app", pf.fn.Name,
			"handler", handler,
			"duration", elapsed,
			"reason", err,
		)
		return err
	}
	// Mark the invocation complete on success BEFORE the success metrics so a
	// crash between the side effect and MarkComplete re-runs the handler
	// (at-least-once; the handler must remain idempotent). The completion is
	// CASed on this attempt's claim; a refusal means the marker was re-claimed by
	// a newer claim or is already terminal exhausted, so this delivery does not
	// own the resolution: return the not-eligible sentinel so the stream leaves
	// the message pending rather than ACK a superseded claim's outcome.
	if invState != nil {
		if !invState.MarkComplete(invocation, claim) {
			return fmt.Errorf("%w: completion superseded for %v", stream.ErrInvocationNotEligible, invocation)
		}
	}
	r.recordHandlerSuccess(pf.fn.Name, handler, elapsed)
	r.log.Info("App handler: executed for schedule",
		"app", pf.fn.Name,
		"handler", handler,
		"duration", elapsed,
	)
	return nil
}

// handlerLogFields builds the common structured-log fields for a handler
// execution log line: app, handler, message_id, and the attempt
// attribution. With invocation state the attempt attribution is the
// Redis-backed handler attempt (the count that drives retry/exhaustion); without
// it there is no handler attempt to attribute, so the message delivery attempt is
// reported instead. It never presents a delivery count as a handler attempt.
func handlerLogFields(hasState bool, fnName, handler, msgID string, handlerAttempt int, deliveryAttempt int64) []any {
	fields := []any{
		"app", fnName,
		"handler", handler,
		"message_id", msgID,
	}
	if hasState {
		return append(fields, "handler_attempt", handlerAttempt)
	}
	return append(fields, "delivery_attempt", int(deliveryAttempt))
}

// recordHandlerSuccess increments the success metrics shared by Handle's
// success branch, InvokeHandler, and manual invocation: the labeled invocation
// outcome counter, the unlabeled total, per-app success attribution, the
// last_success_at timestamp, and the duration histogram. It deliberately does
// NOT touch the event classification counters or app_events_matched_total —
// Handle owns those and counts them once per logical event, so an execution
// must not be double-attributed here.
func (r *Runner) recordHandlerSuccess(fnName, handler string, duration time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "success"},
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		})
	// Unlabeled total for the SQLite snapshot; the labeled counter above stays
	// for Prometheus.
	r.metrics.Inc(metrics.MetricHandlerSuccess)
	// Per-app success attribution.
	r.metrics.IncLabels(metrics.MetricFunctionHandlerSuccess,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampSuccess, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		}, duration)
}

// recordHandlerFailure increments the failure metrics shared by Handle's
// failure branch, InvokeHandler, and manual invocation: the labeled invocation
// outcome counter, the unlabeled total, per-app failure attribution, and
// the duration histogram. A failed attempt that will retry still counts as a
// failure here (it sets last_failure_at); only the DLQ-routed exhaustion
// additionally sets last_dlq_at (see recordFailure). It deliberately does NOT
// touch the event classification counters or app_events_matched_total —
// Handle owns those and counts them once per logical event, so a failure must
// not be double-attributed here.
func (r *Runner) recordHandlerFailure(fnName, handler string, duration time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "failure"},
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		})
	r.metrics.Inc(metrics.MetricHandlerFailure)
	r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampFailure, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		}, duration)
}

// recordFailure handles a failed invocation attempt: it decides whether the
// handler attempt is retryable or exhausted, updates the invocation state and
// metrics accordingly, and returns the outcome plus the plain error Handle
// should aggregate. It is used for execution, marshal, secret-resolution, and
// timeout failures (all are failed attempts).
//
// A retryable handler attempt (attempt < 1+retries) schedules a retry backoff
// via RecordFailure, counts function_retries_total, and returns
// (outcomeRetryable, err). An exhausted handler attempt (attempt >= 1+retries)
// marks the invocation terminal via MarkExhausted, counts function_dlq_total, and
// returns (outcomeExhausted, err). Both transitions are CASed on the claim
// (attempt + token): a refusal (the claim is stale, or the marker is already
// terminal) returns (outcomePendingSkip, nil) so the message stays pending and a
// superseded claim never ACKs or dead-letters a newer claim's message. The
// message-level DLQ decision (whether EVERY matched invocation is terminal) is
// NOT made here; it is deferred to Handle's end-of-loop aggregation, so an
// invocation can exhaust while others still run without short-circuiting them.
//
// The retryable error is returned plain. The exhausted error is a
// *stream.HandlerExhaustedError carrying the exhausted handler attempt, so the
// stream layer can attribute the DLQ entry's handler_attempts from the handler
// retry state rather than the message delivery count. Its message names the
// same attempt count, keeping the DLQ `reason` consistent with
// `handler_attempts`.
func (r *Runner) recordFailure(
	invState stream.InvocationState,
	invocation string,
	claim stream.InvocationClaim,
	retries int,
	fnName, handler, msgID string,
	origErr error,
) (invocationOutcome, error) {
	handlerAttempt := claim.Attempt
	maxAttempts := 1 + retries
	if handlerAttempt >= maxAttempts {
		// Exhausted: mark the invocation terminal, CASed on this attempt's claim
		// (attempt + token). If the store refuses (the claim is stale — a newer
		// token now owns the marker — or the marker is already terminal), this
		// delivery does not own the invocation's resolution: do NOT report
		// exhaustion (which would dead-letter a message whose newer claim may
		// still resolve), leave the message pending, and let the newer claim or a
		// later terminal-skip delivery drive the DLQ decision. This is the
		// exhaustion-commit attribution point: the invocation exhausted its
		// retries and its terminal exhausted marker is now persisted, so
		// function_dlq_total and last_dlq_at are stamped HERE — not on every
		// failure, not again on the later terminal-skip redeliveries of the same
		// invocation, and not at the message-level DLQ write (which happens later
		// in the stream layer, only after all exhausted siblings resolve, and
		// only if the XADD succeeds).
		if !invState.MarkExhausted(invocation, claim) {
			r.log.Warn("App handler: exhaustion superseded; leaving pending",
				"app", fnName,
				"handler", handler,
				"message_id", msgID,
				"handler_attempt", handlerAttempt,
			)
			return outcomePendingSkip, nil
		}
		r.metrics.IncLabels(metrics.MetricFunctionDLQ,
			[]metrics.Label{{Name: "app", Value: fnName}})
		r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampDLQ, time.Now().Unix())
		r.log.Error("App handler: exhausted; invocation terminal",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
			"handler_attempts_total", maxAttempts,
		)
		return outcomeExhausted, &stream.HandlerExhaustedError{
			Invocations: []stream.ExhaustedInvocation{{
				App:      fnName,
				Handler:  handler,
				Attempts: handlerAttempt,
				Err:      origErr,
			}},
		}
	}
	// Retryable: schedule a retry backoff and count the retry. The claim
	// (attempt + token) is passed as a compare-and-set guard so a stale owner
	// (whose running deadline elapsed and whose invocation was re-claimed by a
	// newer claim) cannot overwrite the newer claim's marker. If the store
	// refuses (stale claim or terminal marker), this delivery does not own the
	// invocation: leave the message pending rather than schedule a retry on a
	// superseded claim's behalf.
	backoff := retryBackoff(handlerAttempt)
	if !invState.RecordFailure(invocation, claim, backoff) {
		r.log.Warn("App handler: retry superseded; leaving pending",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
		)
		return outcomePendingSkip, nil
	}
	r.metrics.IncLabels(metrics.MetricFunctionRetries,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.log.Warn("App handler: failed attempt; retrying later",
		"app", fnName,
		"handler", handler,
		"message_id", msgID,
		"handler_attempt", handlerAttempt,
		"handler_attempts_total", maxAttempts,
		"retry_backoff", backoff,
		"reason", origErr,
	)
	return outcomeRetryable, fmt.Errorf(
		"app %q handler %q: handler attempt %d failed: %w",
		fnName, handler, handlerAttempt, origErr,
	)
}

// dedupeExhausted returns the exhausted invocations deduplicated by
// "<app>/<handler>", preserving first-seen order. This is a defensive
// backstop: a parsed template rejects duplicate event handlers, so the same
// invocation should not appear twice in the aggregate, but hand-built
// templates and legacy aggregates may still carry duplicate metadata; the DLQ
// contract is one entry per exhausted invocation, so it is collapsed here.
func dedupeExhausted(invocations []stream.ExhaustedInvocation) []stream.ExhaustedInvocation {
	if len(invocations) < 2 {
		return invocations
	}
	seen := make(map[string]bool, len(invocations))
	out := make([]stream.ExhaustedInvocation, 0, len(invocations))
	for _, iv := range invocations {
		inv := iv.Invocation()
		if seen[inv] {
			continue
		}
		seen[inv] = true
		out = append(out, iv)
	}
	return out
}

// allMatchedTerminal reports whether every matched invocation is terminal
// (complete or exhausted). It is used to decide whether a message whose last
// failing invocation just exhausted has any runnable invocation left: if none,
// the message is terminal and must be routed to the DLQ. A read error fails
// open to false (not terminal), so the message is conservatively left pending.
func allMatchedTerminal(invState stream.InvocationState, matched []string) bool {
	for _, inv := range matched {
		if !invState.IsTerminal(inv) {
			return false
		}
	}
	return true
}

// retryBackoff returns the retry delay to apply after a failed attempt with the
// given completed attempt number. The schedule is fixed and non-configurable:
// attempt 1 → 1m, 2 → 2m, 3 → 5m, 4+ → 10m (capped). The runner owns rule
// semantics, so the schedule lives here, not in the stream store.
func retryBackoff(completedAttempts int) time.Duration {
	switch {
	case completedAttempts <= 1:
		return time.Minute
	case completedAttempts == 2:
		return 2 * time.Minute
	case completedAttempts == 3:
		return 5 * time.Minute
	default:
		return 10 * time.Minute
	}
}

// eventFields extracts the low-cardinality, label-safe event_id and event_name
// from the event for the invocation's runtime RunMeta (container labels and the
// stream output prefix). These are never used as metric labels or structured
// log fields.
func eventFields(event map[string]any) (eventID, eventName string) {
	if v, ok := event["event_id"]; ok {
		eventID = stringify(v)
	}
	if v, ok := event["event_name"]; ok {
		eventName = stringify(v)
	}
	return eventID, eventName
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// resolveExtraEnv builds the per-invocation env for a template: the literal env
// values followed by the resolved secret values, both name-ordered. Secret
// values are resolved here, immediately before execution, so they are never
// cached on Prepared or baked into the fingerprint. A template that references a
// secret but has no provider configured fails with a clear error naming the
// reference. The returned slice is the ONLY place a resolved secret value lives
// before it is handed to the executor; the executor carries it in the request
// frame's env map to the reused bootstrap process. It is never logged,
// persisted, or written into an execution container's Docker Config.Env.
func (r *Runner) resolveExtraEnv(ctx context.Context, tmpl *app.Template) ([]string, error) {
	var extra []string
	for _, ev := range tmpl.EnvList() {
		extra = append(extra, ev.Name+"="+ev.Value)
	}
	for _, sb := range tmpl.SecretList() {
		if r.secrets == nil {
			return nil, fmt.Errorf("app references secret %q but no secret provider is configured", sb.Ref)
		}
		val, err := r.secrets.Resolve(ctx, sb.Ref.String())
		if err != nil {
			// The provider's error carries the reference name only, never a value.
			return nil, fmt.Errorf("resolve secret %q: %w", sb.Ref, err)
		}
		extra = append(extra, sb.Name+"="+val)
	}
	return extra, nil
}

// cleanupTimeout bounds every docker image-removal call made off the event path
// (see removeImageAsync), so a wedged daemon cannot hold a goroutine or block
// shutdown indefinitely. It is deliberately short: retirement is best-effort
// cleanup, never on the critical path.
const cleanupTimeout = 10 * time.Second

// imageRefCounter tracks how many in-flight executions hold each image and which
// images have been retired (superseded) but can only be removed once idle.
//
// acquire/release bracket a single invocation: the count for prepared.Image is
// bumped on entry and dropped on return, so ImageInUse reports live executions
// even while a swap concurrently replaces the registry entry. RetireImage marks
// an image retired; when its count reaches zero, release triggers its
// asynchronous removal via the runner (which owns the cleaner). A retired image
// that is never in flight (count already zero at retire time) is removed
// immediately by RetireImage instead.
type imageRefCounter struct {
	mu      sync.Mutex
	uses    map[string]int64
	retired map[string]bool
	onIdle  func(image string)
}

// newImageRefCounter builds an empty counter whose onIdle hook fires removal.
func newImageRefCounter() *imageRefCounter {
	return &imageRefCounter{
		uses:    map[string]int64{},
		retired: map[string]bool{},
	}
}

func (c *imageRefCounter) setOnIdle(fn func(string)) {
	c.mu.Lock()
	c.onIdle = fn
	c.mu.Unlock()
}

func (c *imageRefCounter) acquire(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	c.uses[image]++
	// A retired image was idle when its removal fired but is being (re)claimed by
	// a new execution; drop the retirement mark so the pending removal aborts.
	// The image cannot vanish mid-invocation: retire is guarded by in-use, and
	// removing is guarded by in-use again under the same lock discipline.
	delete(c.retired, image)
	c.mu.Unlock()
}

// release drops a reference and, when a retired image's count just reached zero,
// fires onIdle so the runner can remove it. Retired-but-idle images are removed
// exactly once, and only after the last in-flight execution has finished.
func (c *imageRefCounter) release(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	left := c.uses[image] - 1
	if left <= 0 {
		delete(c.uses, image)
		left = 0
	}
	// If the image was marked retired and is now idle, ownership of its removal
	// has transferred to this release: clear the mark (so no other release fires
	// a duplicate) and report the idle transition.
	becameIdle := left == 0 && c.retired[image]
	if becameIdle {
		delete(c.retired, image)
	}
	onIdle := c.onIdle
	c.mu.Unlock()
	if becameIdle && onIdle != nil {
		onIdle(image)
	}
}

// inUse reports whether an execution currently holds the image.
func (c *imageRefCounter) inUse(image string) bool {
	if image == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uses[image] > 0
}

// recordRetired marks image as retired (superseded). It returns true when the
// image was not already retired, so the caller knows whether this retirement
// owns removal.
func (c *imageRefCounter) recordRetired(image string) bool {
	if image == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired[image] {
		return false
	}
	c.retired[image] = true
	return true
}
