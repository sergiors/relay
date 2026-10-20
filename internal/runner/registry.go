package runner

import (
	"sort"
	"sync"

	"relay/internal/app"
	eventmatch "relay/internal/event"
	"relay/internal/runtime"
)

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

// ServesImage reports whether the registry currently publishes image as an
// AVAILABLE app version, i.e. whether an app's live, runnable generation IS
// exactly that image reference.
//
// It is the explicit activation signal for image retirement, and it is what
// makes retirement survive a stale use: a snapshot taken before a registry swap
// can still execute the superseded image — that use is stale and must NOT cancel
// the intent to remove it — whereas an image the registry serves again (a
// re-prepared/reverted generation that reuses the same content-addressed tag) is
// CURRENT, so a stale retirement for it must be cancelled rather than remove the
// version the registry is serving. An unavailable entry (a failed build) has no
// image and never serves one; a pending entry carries no image by construction,
// so only published, available generations are considered.
func (r *Registry) ServesImage(image string) bool {
	if image == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pf := range r.fns {
		if pf == nil || pf.prepared == nil {
			continue
		}
		if pf.prepared.Image == image {
			return true
		}
	}
	return false
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
