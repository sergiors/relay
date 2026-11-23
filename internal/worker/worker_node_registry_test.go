package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/config"
	"relay/internal/state"
	"relay/internal/testutil"
)

// Unit tests for the worker's GLOBAL node registry lifecycle: registration
// independent of SERVICE_BIND_ADDR, heartbeat ownership of the lease,
// failure-rate-limiting, the bounded periodic prune seam, and shutdown
// join/release semantics. Deterministic fakes only — no PostgreSQL.

// fakeNodeLeaseStore is an in-memory nodeLeaseStore recording every call and
// capturing the contexts its bounded operations receive.
type fakeNodeLeaseStore struct {
	mu              sync.Mutex
	claims          int
	heartbeats      int
	releases        int
	prunes          int
	lastMetadata    state.NodeLeaseMetadata
	lastTTL         time.Duration
	claimErr        error
	heartbeatErr    error
	releaseErr      error
	pruneErr        error
	heartbeatExpiry time.Time
	pruneResult     state.NodeRegistryPruneResult

	// claimHook/heartbeatHook run at operation ENTRY, before the store lock,
	// so a test can deliver lifecycle cancellation WHILE the operation is in
	// flight (deterministically: cancellation between entry and error return).
	claimHook     func()
	heartbeatHook func()

	releaseCtxErrs     []error
	releaseCtxDeadline time.Time
	pruneCtxDeadlines  []time.Time

	claimed   chan struct{}
	claimOnce sync.Once
}

func newFakeNodeLeaseStore() *fakeNodeLeaseStore {
	return &fakeNodeLeaseStore{claimed: make(chan struct{})}
}

func (f *fakeNodeLeaseStore) ClaimNodeLease(
	_ context.Context, metadata state.NodeLeaseMetadata, ttl time.Duration,
) (state.NodeLease, error) {
	if f.claimHook != nil {
		f.claimHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	f.lastMetadata = metadata
	f.lastTTL = ttl
	if f.claimErr != nil {
		return state.NodeLease{}, f.claimErr
	}
	f.claimOnce.Do(func() { close(f.claimed) })
	return state.NodeLease{
		NodeID:        "node-1",
		Instance:      "inst-1",
		Incarnation:   int64(f.claims),
		BindAddr:      metadata.BindAddr,
		AdvertiseAddr: metadata.AdvertiseAddr,
		ExpiresAt:     time.Now().Add(ttl),
	}, nil
}

func (f *fakeNodeLeaseStore) HeartbeatNodeLease(
	_ context.Context, lease state.NodeLease,
) (state.NodeLease, error) {
	if f.heartbeatHook != nil {
		f.heartbeatHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats++
	if f.heartbeatErr != nil {
		return state.NodeLease{}, f.heartbeatErr
	}
	if !f.heartbeatExpiry.IsZero() {
		lease.ExpiresAt = f.heartbeatExpiry
	}
	return lease, nil
}

func (f *fakeNodeLeaseStore) ReleaseNodeLease(
	ctx context.Context, _ state.NodeLease,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	f.releaseCtxErrs = append(f.releaseCtxErrs, ctx.Err())
	if deadline, ok := ctx.Deadline(); ok {
		f.releaseCtxDeadline = deadline
	}
	return f.releaseErr
}

func (f *fakeNodeLeaseStore) PruneExpiredNodeRegistry(
	ctx context.Context,
) (state.NodeRegistryPruneResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prunes++
	if deadline, ok := ctx.Deadline(); ok {
		f.pruneCtxDeadlines = append(f.pruneCtxDeadlines, deadline)
	}
	if f.pruneErr != nil {
		return state.NodeRegistryPruneResult{}, f.pruneErr
	}
	return f.pruneResult, nil
}

func (f *fakeNodeLeaseStore) counts() (claims, heartbeats, releases, prunes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims, f.heartbeats, f.releases, f.prunes
}

func (f *fakeNodeLeaseStore) claimedMetadata() state.NodeLeaseMetadata {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastMetadata
}

func (f *fakeNodeLeaseStore) claimedTTL() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTTL
}

// registryLogRecord is one captured log record for transition assertions.
type registryLogRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

// recordingLogHandler captures every record at every level.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []registryLogRecord
}

func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]any{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, registryLogRecord{
		Level: r.Level, Message: r.Message, Attrs: attrs,
	})
	return nil
}

func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingLogHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingLogHandler) snapshot() []registryLogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]registryLogRecord(nil), h.records...)
}

func newRecordingLogger(t *testing.T) (*slog.Logger, *recordingLogHandler) {
	t.Helper()
	handler := &recordingLogHandler{}
	return slog.New(handler), handler
}

// newNodeRegistryFixture builds a registry whose loops are NOT started; the
// test drives heartbeatOnce/pruneOnce directly for deterministic call counts.
func newNodeRegistryFixture(
	t *testing.T, st nodeLeaseStore, logger *slog.Logger,
) *nodeRegistry {
	t.Helper()
	r := &nodeRegistry{
		st:       st,
		log:      logger,
		reporter: newRegistryFailureReporter(logger),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	t.Cleanup(r.cancel)
	return r
}

// Given no SERVICE_BIND_ADDR (or any dependency missing), when the worker
// starts the node registry, then global registration stays independent of
// bind: with state present it ALWAYS starts — even with bind unset, claiming
// empty-address metadata and registering one barrier shutdown step — while a
// nil state handle registers nothing; the advertise address falls back to the
// bind address only when unset.
func TestStartNodeRegistryIndependentOfServiceBindAddr(t *testing.T) {
	logger := testutil.DiscardLogger()

	t.Run("nil state registers nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		shutdown := &shutdownRegistry{}
		if r := startNodeRegistry(ctx, logger, config.Config{}, nil, shutdown); r != nil {
			t.Fatal("node registry started without a state handle")
		}
		if len(shutdown.steps) != 0 {
			t.Fatalf("registered steps = %d, want none without a state handle",
				len(shutdown.steps))
		}
	})

	for _, tc := range []struct {
		name     string
		cfg      config.Config
		wantBind string
		wantAdv  string
	}{
		{"bind unset registers with empty metadata",
			config.Config{}, "", ""},
		{"advertise falls back to the bind address",
			config.Config{ServiceBindAddr: "192.0.2.10"}, "192.0.2.10", "192.0.2.10"},
		{"explicit advertise address wins",
			config.Config{ServiceBindAddr: "192.0.2.10",
				ServiceAdvertiseAddr: "198.51.100.7"},
			"192.0.2.10", "198.51.100.7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			shutdown := &shutdownRegistry{}
			// A node-less state handle never acquires a lease, so the started
			// loops stay idle and shutdown only joins them.
			r := startNodeRegistry(ctx, logger, tc.cfg, &state.State{}, shutdown)
			if r == nil {
				t.Fatal("node registry did not start with a state handle present")
			}
			metadata := r.metadata
			if metadata.BindAddr != tc.wantBind ||
				metadata.AdvertiseAddr != tc.wantAdv {
				t.Fatalf("metadata = %+v, want bind %q advertise %q",
					metadata, tc.wantBind, tc.wantAdv)
			}
			if len(shutdown.steps) != 1 {
				t.Fatalf("registered steps = %d, want exactly one", len(shutdown.steps))
			}
			step := shutdown.steps[0]
			if step.name != shutdownStepNodeRegistry || !step.barrier ||
				step.timeout != shutdownServiceTimeout {
				t.Fatalf("step = %+v, want the %q barrier with the service timeout",
					step, shutdownStepNodeRegistry)
			}

			shutdownCtx, shutdownCancel := context.WithTimeout(
				context.Background(), 10*time.Second)
			defer shutdownCancel()
			if err := r.shutdown(shutdownCtx); err != nil {
				t.Fatalf("shutdown with idle loops: %v", err)
			}
		})
	}
}

// Given the registry's heartbeat tick, when no lease exists yet, then one
// bounded claim runs with the configured metadata and TTL, the heartbeat
// renews that lease, and Lease() hands out COPIES so a consumer can never
// mutate the stored lease.
func TestNodeRegistryClaimsAndHeartbeatsOnFirstTick(t *testing.T) {
	fake := newFakeNodeLeaseStore()
	renewedExpiry := time.Date(2026, 11, 23, 9, 0, 45, 0, time.UTC)
	fake.heartbeatExpiry = renewedExpiry
	r := newNodeRegistryFixture(t, fake, testutil.DiscardLogger())
	r.metadata = state.NodeLeaseMetadata{
		BindAddr: "127.0.0.1", AdvertiseAddr: "127.0.0.1:7001",
	}

	r.heartbeatOnce()

	claims, heartbeats, _, _ := fake.counts()
	if claims != 1 || heartbeats != 1 {
		t.Fatalf("calls = (%d claims, %d heartbeats), want (1, 1)", claims, heartbeats)
	}
	if got := fake.claimedMetadata(); got != r.metadata {
		t.Fatalf("claim metadata = %+v, want %+v", got, r.metadata)
	}
	if ttl := fake.claimedTTL(); ttl != nodeRegistryLeaseTTL {
		t.Fatalf("claim TTL = %v, want the registry TTL %v", ttl, nodeRegistryLeaseTTL)
	}

	lease := r.Lease()
	if lease == nil {
		t.Fatal("Lease() = nil after a successful claim and heartbeat")
	}
	if lease.Incarnation != 1 || !lease.ExpiresAt.Equal(renewedExpiry) {
		t.Fatalf("lease = %+v, want incarnation 1 with the renewed expiry", lease)
	}

	// A consumer mutating its copy must not corrupt the stored lease.
	lease.Incarnation = 99
	lease.AdvertiseAddr = "mutated"
	again := r.Lease()
	if again == nil || again.Incarnation != 1 ||
		again.AdvertiseAddr != "127.0.0.1:7001" {
		t.Fatalf("stored lease = %+v, want it isolated from consumer mutation", again)
	}

	var absent *nodeRegistry
	if absent.Lease() != nil {
		t.Fatal("a nil registry must report no lease")
	}
}

// A fenced heartbeat means the lease is no longer ours: the registry clears it
// and the next tick performs a fenced re-claim. A database failure is
// transient: the lease is retained (no re-claim) and the transition is
// reported once at Warn.
func TestNodeRegistryHeartbeatClassifiesWriteErrors(t *testing.T) {
	t.Run("fence rejection re-claims", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.heartbeatErr = fmt.Errorf("heartbeat superseded: %w",
			state.ErrNodeLeaseFence)
		r := newNodeRegistryFixture(t, fake, testutil.DiscardLogger())

		r.heartbeatOnce()
		r.heartbeatOnce()

		claims, heartbeats, _, _ := fake.counts()
		if claims != 2 || heartbeats != 2 {
			t.Fatalf("calls = (%d claims, %d heartbeats), want (2, 2): a fence "+
				"rejection must clear the lease so the next tick re-claims",
				claims, heartbeats)
		}
	})

	t.Run("database failure retains the lease", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.heartbeatErr = errors.New("connection refused")
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)

		r.heartbeatOnce()
		r.heartbeatOnce()

		claims, heartbeats, _, _ := fake.counts()
		if claims != 1 || heartbeats != 2 {
			t.Fatalf("calls = (%d claims, %d heartbeats), want (1, 2): a "+
				"transient failure must retry with the retained lease",
				claims, heartbeats)
		}
		warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn)
		if len(warns) != 1 || warns[0].Attrs["operation"] != "heartbeat" {
			t.Fatalf("Warn records = %+v, want exactly one heartbeat failure "+
				"(rate-limited to the transition)", warns)
		}
	})

	t.Run("claim failure is reported once", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.claimErr = errors.New("database down")
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)

		r.heartbeatOnce()
		r.heartbeatOnce()

		claims, heartbeats, _, _ := fake.counts()
		if claims != 2 || heartbeats != 0 {
			t.Fatalf("calls = (%d claims, %d heartbeats), want (2, 0): a failed "+
				"claim retries without heartbeating", claims, heartbeats)
		}
		warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn)
		if len(warns) != 1 || warns[0].Attrs["operation"] != "claim" {
			t.Fatalf("Warn records = %+v, want exactly one claim failure", warns)
		}
	})

	t.Run("fenced claim is expected and stays quiet", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.claimErr = fmt.Errorf("claim fenced: %w", state.ErrNodeLeaseFence)
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)

		r.heartbeatOnce()

		if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 0 {
			t.Fatalf("Warn records = %+v, want none for an expected fence", warns)
		}
	})
}

// Given lifecycle cancellation arriving WHILE a claim or heartbeat is in
// flight, when the operation then fails (as canceled work does), then NO Warn
// or failure transition is logged — cancellation is normal termination. The
// contrasts pin the remaining contract: a fence rejection stays a Debug record
// (never a Warn), and prune cancellation stays silent (covered by
// TestNodeRegistryPruneCadenceBoundsAndLogging). Genuine active-context DB
// errors still warn exactly once (TestNodeRegistryHeartbeatClassifiesWriteErrors
// "database failure retains the lease" and "claim failure is reported once").
func TestNodeRegistryCancellationIsNotAFailureTransition(t *testing.T) {
	t.Run("in-flight claim canceled mid-operation stays silent", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.claimErr = errors.New("connection refused")
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)
		// Cancellation lands between claim ENTRY and its error return.
		fake.claimHook = func() { r.cancel() }

		r.heartbeatOnce()

		claims, heartbeats, _, _ := fake.counts()
		if claims != 1 || heartbeats != 0 {
			t.Fatalf("calls = (%d claims, %d heartbeats), want (1, 0): the claim "+
				"must have been ENTERED (not skipped) for this to exercise the "+
				"in-flight path", claims, heartbeats)
		}
		if records := handler.snapshot(); len(records) != 0 {
			t.Fatalf("log records = %+v, want NONE: a canceled in-flight claim "+
				"is normal termination, not a failure", records)
		}
		if r.Lease() != nil {
			t.Fatal("no lease may be stored after a canceled claim")
		}
	})

	t.Run("in-flight heartbeat canceled mid-operation stays silent", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)
		r.heartbeatOnce() // establish a healthy lease on the live context
		if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 0 {
			t.Fatalf("setup Warn records = %+v, want none", warns)
		}
		before := len(handler.snapshot())
		fake.heartbeatErr = errors.New("connection refused")
		fake.heartbeatHook = func() { r.cancel() } // cancellation lands mid-heartbeat

		r.heartbeatOnce()

		if added := handler.snapshot()[before:]; len(added) != 0 {
			t.Fatalf("records after the canceled heartbeat = %+v, want NONE", added)
		}
		if _, heartbeats, _, _ := fake.counts(); heartbeats != 2 {
			t.Fatalf("heartbeat calls = %d, want 2 (entered, then canceled)",
				heartbeats)
		}
		if r.Lease() == nil {
			t.Fatal("a non-fence failure must retain the stored lease")
		}
	})

	t.Run("fence rejection remains a Debug record", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)
		r.heartbeatOnce() // healthy lease first
		fake.heartbeatErr = fmt.Errorf("heartbeat superseded: %w",
			state.ErrNodeLeaseFence)

		r.heartbeatOnce()

		if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 0 {
			t.Fatalf("Warn records = %+v, want none for a fence rejection", warns)
		}
		found := false
		for _, debug := range recordsAtLevel(handler.snapshot(), slog.LevelDebug) {
			if debug.Message == "Node registry: lease fenced; re-claiming" &&
				debug.Attrs["operation"] == "heartbeat" {
				found = true
			}
		}
		if !found {
			t.Fatalf("Debug records = %+v, want the heartbeat fence transition "+
				"logged at Debug", handler.snapshot())
		}
		if r.Lease() != nil {
			t.Fatal("a fence rejection must clear the stored lease")
		}
	})
}

func recordsAtLevel(
	records []registryLogRecord, level slog.Level,
) []registryLogRecord {
	var out []registryLogRecord
	for _, record := range records {
		if record.Level == level {
			out = append(out, record)
		}
	}
	return out
}

// The failure reporter logs only TRANSITIONS: Warn on the first failure of an
// operation, Debug while it keeps failing, Info exactly once on recovery, and
// a fresh Warn if the operation fails again later — per operation, not global.
func TestRegistryFailureReporterRateLimitsTransitions(t *testing.T) {
	logger, handler := newRecordingLogger(t)
	reporter := newRegistryFailureReporter(logger)

	reporter.failure("heartbeat", errors.New("db down"))
	reporter.failure("heartbeat", errors.New("db down"))
	reporter.failure("publish", errors.New("db down"))
	reporter.recovery("heartbeat")
	reporter.recovery("heartbeat") // no previous failure: nothing to recover
	reporter.failure("heartbeat", errors.New("db down again"))

	records := handler.snapshot()
	if len(records) != 5 {
		t.Fatalf("records = %d, want exactly the 5 transition records", len(records))
	}
	want := []struct {
		level     slog.Level
		message   string
		operation string
	}{
		{slog.LevelWarn, "Registry: operation failing", "heartbeat"},
		{slog.LevelDebug, "Registry: operation still failing", "heartbeat"},
		{slog.LevelWarn, "Registry: operation failing", "publish"},
		{slog.LevelInfo, "Registry: operation recovered", "heartbeat"},
		{slog.LevelWarn, "Registry: operation failing", "heartbeat"},
	}
	for i, expected := range want {
		got := records[i]
		if got.Level != expected.level || got.Message != expected.message ||
			got.Attrs["operation"] != expected.operation {
			t.Errorf("record[%d] = (%s, %q, op=%v), want (%s, %q, op=%q)",
				i, got.Level, got.Message, got.Attrs["operation"],
				expected.level, expected.message, expected.operation)
		}
	}
}

// The periodic prune seam: the slow cadence and per-invocation bound are the
// documented constants; prune does NOT run at startup (first tick only);
// pruneOnce carries a fresh bounded deadline, logs the pruned counts only when
// something was removed, rate-limits failures per operation, and stays silent
// when shutdown cancelled the bounded context.
func TestNodeRegistryPruneCadenceBoundsAndLogging(t *testing.T) {
	if nodeRegistryPruneInterval != 5*time.Minute {
		t.Fatalf("prune interval = %v, want the documented 5m cadence",
			nodeRegistryPruneInterval)
	}
	if nodeRegistryPruneTimeout != 10*time.Second {
		t.Fatalf("prune timeout = %v, want the documented 10s bound",
			nodeRegistryPruneTimeout)
	}

	t.Run("no prune at startup", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		r := newNodeRegistryFixture(t, fake, testutil.DiscardLogger())
		r.Start(context.Background())
		select {
		case <-fake.claimed:
		case <-time.After(10 * time.Second):
			t.Fatal("the first heartbeat never claimed a lease")
		}
		_, _, _, prunes := fake.counts()
		if prunes != 0 {
			t.Fatalf("prune calls = %d at startup, want 0 (cleanup starts on "+
				"the first %v tick)", prunes, nodeRegistryPruneInterval)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		select {
		case <-r.done:
		default:
			t.Fatal("done must be closed once both loops joined")
		}
	})

	t.Run("pruneOnce is deadline-bounded and logs only results", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)

		r.pruneOnce()

		fake.mu.Lock()
		var deadline time.Time
		if n := len(fake.pruneCtxDeadlines); n > 0 {
			deadline = fake.pruneCtxDeadlines[n-1]
		}
		fake.mu.Unlock()
		if deadline.IsZero() {
			t.Fatal("prune context carried no deadline")
		}
		maxDeadline := time.Now().Add(nodeRegistryPruneTimeout + 2*time.Second)
		if deadline.After(maxDeadline) {
			t.Fatalf("prune deadline %s exceeds now+%v (+slack)",
				deadline, nodeRegistryPruneTimeout)
		}
		if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 0 {
			t.Fatalf("Warn records after a clean prune = %+v, want none", warns)
		}
		for _, record := range handler.snapshot() {
			if record.Message == "Node registry: pruned expired rows" {
				t.Fatal("an empty prune must not log pruned counts")
			}
		}

		fake.mu.Lock()
		fake.pruneResult = state.NodeRegistryPruneResult{Endpoints: 3, Nodes: 1}
		fake.mu.Unlock()
		r.pruneOnce()
		logged := false
		for _, record := range handler.snapshot() {
			if record.Message == "Node registry: pruned expired rows" {
				logged = true
				if record.Attrs["endpoints"] != int64(3) ||
					record.Attrs["nodes"] != int64(1) {
					t.Fatalf("pruned record attrs = %v, want endpoints 3 nodes 1",
						record.Attrs)
				}
			}
		}
		if !logged {
			t.Fatal("a prune that removed rows must log the counts")
		}
	})

	t.Run("prune failures are rate-limited and silent at shutdown", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.pruneErr = errors.New("database down")
		logger, handler := newRecordingLogger(t)
		r := newNodeRegistryFixture(t, fake, logger)

		r.pruneOnce()
		r.pruneOnce()

		warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn)
		if len(warns) != 1 || warns[0].Attrs["operation"] != "prune" {
			t.Fatalf("Warn records = %+v, want exactly one prune failure", warns)
		}
		debugs := recordsAtLevel(handler.snapshot(), slog.LevelDebug)
		if len(debugs) != 1 || debugs[0].Attrs["operation"] != "prune" {
			t.Fatalf("Debug records = %+v, want exactly one still-failing rate "+
				"limit record", debugs)
		}

		// Shutdown cancelled the bounded cleanup: not a registry failure.
		r.cancel()
		r.pruneOnce()
		if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 1 {
			t.Fatalf("Warn records after a shutdown-cancelled prune = %d, want the "+
				"original 1 (cancellation stays silent)", len(warns))
		}
	})
}

// Given an EXPIRED step context (a barrier join already timed out), when
// shutdown runs, then it still releases the lease using a FRESH bounded
// context — the recorded release context is healthy with the release timeout
// deadline — and joins both loops before returning.
func TestNodeRegistryShutdownReleasesWithFreshContext(t *testing.T) {
	fake := newFakeNodeLeaseStore()
	r := newNodeRegistryFixture(t, fake, testutil.DiscardLogger())
	r.Start(context.Background())
	select {
	case <-fake.claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("the first heartbeat never claimed a lease")
	}

	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	// The join error is timing-dependent (the loops may already have exited);
	// the release freshness below is the contract under test.
	_ = r.shutdown(expired)

	_, _, releases, prunes := fake.counts()
	if releases != 1 {
		t.Fatalf("release calls = %d, want 1 despite the expired step context",
			releases)
	}
	fake.mu.Lock()
	releaseErrs := append([]error(nil), fake.releaseCtxErrs...)
	deadline := fake.releaseCtxDeadline
	fake.mu.Unlock()
	if len(releaseErrs) != 1 || releaseErrs[0] != nil {
		t.Fatalf("release context errors = %v, want the release to run on a "+
			"HEALTHY fresh context", releaseErrs)
	}
	maxDeadline := time.Now().Add(nodeRegistryReleaseTimeout + 2*time.Second)
	if deadline.IsZero() || deadline.After(maxDeadline) {
		t.Fatalf("release deadline = %s, want a fresh bound of now+%v (+slack)",
			deadline, nodeRegistryReleaseTimeout)
	}
	if prunes != 0 {
		t.Fatalf("prune calls = %d during shutdown, want none", prunes)
	}
	select {
	case <-r.done:
	default:
		t.Fatal("done must be closed: shutdown joins the loops before releasing")
	}
}

// A fenced release (a successor owns the rows now) is expected and never fails
// the worker's teardown; a genuine database failure IS surfaced.
func TestNodeRegistryShutdownClassifiesReleaseFailures(t *testing.T) {
	t.Run("fence rejection is not an error", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.releaseErr = fmt.Errorf("release superseded: %w",
			state.ErrNodeLeaseFence)
		r := newNodeRegistryFixture(t, fake, testutil.DiscardLogger())
		r.Start(context.Background())
		select {
		case <-fake.claimed:
		case <-time.After(10 * time.Second):
			t.Fatal("the first heartbeat never claimed a lease")
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown = %v, want nil: a fenced release leaves the "+
				"successor's rows untouched and is not a teardown failure", err)
		}
		_, _, releases, _ := fake.counts()
		if releases != 1 {
			t.Fatalf("release calls = %d, want 1 (attempted and rejected)", releases)
		}
	})

	t.Run("database failure is surfaced", func(t *testing.T) {
		fake := newFakeNodeLeaseStore()
		fake.releaseErr = errors.New("database down")
		r := newNodeRegistryFixture(t, fake, testutil.DiscardLogger())
		r.Start(context.Background())
		select {
		case <-fake.claimed:
		case <-time.After(10 * time.Second):
			t.Fatal("the first heartbeat never claimed a lease")
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := r.shutdown(shutdownCtx)
		if err == nil || !strings.Contains(err.Error(), "database down") {
			t.Fatalf("shutdown = %v, want the release failure surfaced", err)
		}
	})
}

// Given the worker's shutdown ordering, when both registry steps are placed,
// then the endpoint publisher joins strictly BEFORE the node registry
// releases the lease, and both run strictly between the service-coordinator
// join and the service cleanup — publication stops before its lease is
// released, and both finish before shared-dependency teardown.
func TestRegistryShutdownStepsJoinedBeforeTeardown(t *testing.T) {
	endpoints := serviceEndpointPublisherStep(&serviceEndpointPublisher{})
	if endpoints.name != shutdownStepServiceEndpoints || !endpoints.barrier ||
		endpoints.timeout != shutdownServiceTimeout {
		t.Fatalf("endpoint step = %+v, want the %q barrier with the service "+
			"timeout", endpoints, shutdownStepServiceEndpoints)
	}
	nodes := nodeRegistryStep(&nodeRegistry{})
	if nodes.name != shutdownStepNodeRegistry || !nodes.barrier ||
		nodes.timeout != shutdownServiceTimeout {
		t.Fatalf("node step = %+v, want the %q barrier with the service timeout",
			nodes, shutdownStepNodeRegistry)
	}

	join := indexOfStep(shutdownStepServicesJoin)
	publish := indexOfStep(shutdownStepServiceEndpoints)
	registry := indexOfStep(shutdownStepNodeRegistry)
	cleanup := indexOfStep(shutdownStepServiceCleanup)
	if join < 0 || publish < 0 || registry < 0 || cleanup < 0 {
		t.Fatalf("shutdownStepOrder = %v must contain services-join, "+
			"service-endpoints, node-registry and service-cleanup",
			shutdownStepOrder)
	}
	if !(join < publish && publish < registry && registry < cleanup) {
		t.Fatalf("shutdownStepOrder must run services-join (%d) < "+
			"service-endpoints (%d) < node-registry (%d) < service-cleanup (%d)",
			join, publish, registry, cleanup)
	}
	for _, name := range []string{
		shutdownStepServiceEndpoints, shutdownStepNodeRegistry,
	} {
		count := 0
		for _, step := range shutdownStepOrder {
			if step == name {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("step %q appears %d times in shutdownStepOrder, want exactly 1",
				name, count)
		}
	}
}

func indexOfStep(name string) int {
	for i, step := range shutdownStepOrder {
		if step == name {
			return i
		}
	}
	return -1
}
