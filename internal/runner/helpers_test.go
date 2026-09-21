package runner

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// --- loggers and polling ---

// waitFor polls cond until it holds or a generous deadline passes, so async
// removal goroutines are observed deterministically instead of by a fixed
// sleep. It is the single bounded-poll helper for this package. Unlike
// testutil.WaitFor it takes no description and uses a short 2ms tick plus a
// 5s budget, so it is kept local.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not reached before timeout")
}

// bufferLogger returns a logger that captures output into a buffer, so tests can
// assert on structured log lines (e.g. the panic log). It runs at DEBUG so
// nothing is filtered. The returned builder is NOT synchronized; callers must
// not feed a concurrently-running goroutine with it.
func bufferLogger() (*slog.Logger, *strings.Builder) {
	var b strings.Builder
	return slog.New(slog.NewTextHandler(&b, nil)), &b
}

// debugBufferLogger is like bufferLogger but at DEBUG level, so the runner's
// debug-level image-cleanup skip lines are captured too. The returned buffer is
// synchronized so concurrent async goroutines (e.g. the removal retry loop) and
// the test goroutine can share it race-free.
func debugBufferLogger() (*slog.Logger, *testutil.SyncBuffer) {
	b := &testutil.SyncBuffer{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

// --- executors ---

// countingExecutor records how many times it was invoked and can be told to fail
// on demand, optionally holding each execution briefly so concurrent Handles
// interleave. It is the default Executor for tests that only care about call
// counts and success/failure.
type countingExecutor struct {
	mu    sync.Mutex
	calls int
	fail  bool
	// hold is how long Execute sleeps before returning. It is zero for most
	// tests; a non-zero value lets concurrent registry swaps or Handles overlap.
	hold time.Duration
}

func (f *countingExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	f.mu.Lock()
	f.calls++
	fail := f.fail
	hold := f.hold
	f.mu.Unlock()
	if hold > 0 {
		select {
		case <-time.After(hold):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		return fmt.Errorf("boom")
	}
	return nil
}

func (f *countingExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *countingExecutor) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

// captureExecutor records everything a test may want to inspect about the
// invocation's context and arguments: the injected RunMeta, the handler and
// event JSON, and the per-invocation extra env. Unused fields stay zero.
type captureExecutor struct {
	mu       sync.Mutex
	meta     runtime.RunMeta
	handler  string
	payload  []byte
	extraEnv []string
}

func (e *captureExecutor) Execute(ctx context.Context, _ *runtime.Prepared, handler string, eventJSON []byte, extraEnv []string) error {
	e.mu.Lock()
	e.meta = runtime.RunMetaFrom(ctx)
	e.handler = handler
	e.payload = append([]byte(nil), eventJSON...)
	e.extraEnv = append([]string(nil), extraEnv...)
	e.mu.Unlock()
	return nil
}

func (e *captureExecutor) gotMeta() runtime.RunMeta {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.meta
}

func (e *captureExecutor) got() (string, []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handler, append([]byte(nil), e.payload...)
}

func (e *captureExecutor) gotEnv() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.extraEnv...)
}

// errRemoveBoom is the canned failure returned by blockingExecutor's cleaner
// methods.
var errRemoveBoom = &removeBoomErr{}

type removeBoomErr struct{}

func (*removeBoomErr) Error() string { return "remove boom" }

// blockingExecutor implements both Executor and ImageCleaner. It records its
// Execute calls, can hold an invocation open until release closes (so tests can
// drive retirement, release, and concurrency without Docker), and carries the
// image-lifecycle knobs the retirement tests need. Every knob is optional: a
// zero value executes immediately and treats removals as successful.
type blockingExecutor struct {
	mu    sync.Mutex
	calls int

	// release, when non-nil, makes Execute signal entered once and then block
	// until release closes or the invocation context is done.
	release     chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once

	// removed records every image successfully removed.
	removed []string
	// removeErr makes every RemoveImage fail.
	removeErr bool
	// removeErrCountdown, when > 0, makes the next that-many RemoveImage calls
	// fail while simultaneously marking the image referenced (the mid-removal
	// race the runner's re-consult classifies as a guarded skip).
	removeErrCountdown int
	// referenced reports, per image, whether a relay-owned container currently
	// references it.
	referenced map[string]bool
	// referenceCheckErr, when non-nil, makes every
	// ImageReferencedByManagedContainer call fail (unknown state).
	referenceCheckErr error
	// referenceCheckErrAfter, when > 0, lets that-many reference checks succeed
	// before referenceCheckErr takes effect. It models a reference check that
	// succeeds before a removal and fails on the post-failure re-consult.
	referenceCheckErrAfter int
	referenceChecks        int
	// gcCalls counts CleanupUnusedDependencies invocations.
	gcCalls int
	// gcErr makes CleanupUnusedDependencies fail.
	gcErr bool
	// tags is the FunctionImageTags table, keyed by function name.
	tags map[string][]string
}

// newBlockingExecutor builds a blockingExecutor whose entered channel exists up
// front, so a test's waitEntered cannot race the goroutine's Execute.
func newBlockingExecutor(release chan struct{}) *blockingExecutor {
	return &blockingExecutor{release: release, entered: make(chan struct{})}
}

func (f *blockingExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	f.mu.Lock()
	f.calls++
	release, entered := f.release, f.entered
	f.mu.Unlock()
	if release == nil {
		return nil
	}
	if entered != nil {
		f.enteredOnce.Do(func() { close(entered) })
	}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *blockingExecutor) waitEntered() {
	<-f.entered
}

func (f *blockingExecutor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *blockingExecutor) RemoveImage(_ context.Context, image string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeErr {
		return errRemoveBoom
	}
	if f.removeErrCountdown > 0 {
		f.removeErrCountdown--
		if f.referenced == nil {
			f.referenced = map[string]bool{}
		}
		f.referenced[image] = true
		return errRemoveBoom
	}
	f.removed = append(f.removed, image)
	return nil
}

func (f *blockingExecutor) ImageReferencedByManagedContainer(_ context.Context, image string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.referenceChecks++
	if f.referenceCheckErr != nil && f.referenceChecks > f.referenceCheckErrAfter {
		return false, f.referenceCheckErr
	}
	return f.referenced[image], nil
}

// setReferenced mutates whether a container references image.
func (f *blockingExecutor) setReferenced(image string, v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.referenced[image] = v
}

func (f *blockingExecutor) FunctionImageTags(_ context.Context, name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tags == nil {
		return nil, nil
	}
	return append([]string(nil), f.tags[name]...), nil
}

func (f *blockingExecutor) CleanupUnusedDependencies(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gcCalls++
	if f.gcErr {
		return 0, errRemoveBoom
	}
	return 0, nil
}

func (f *blockingExecutor) gcCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gcCalls
}

func (f *blockingExecutor) removedImages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.removed...)
}

// ctxAwareExecutor blocks until the invocation context is done and returns its
// error, so a test can observe the deadline the runner actually imposed.
type ctxAwareExecutor struct{}

func (ctxAwareExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	<-ctx.Done()
	return ctx.Err()
}

// --- prepared-function builders ---

// fnSpec captures the configurable parts of a prepared function so the builders
// below can share one construction path.
type fnSpec struct {
	name        string
	image       string
	runtime     string
	rules       []function.EventRule
	schedules   []function.Schedule
	concurrency int
	env         map[string]string
	secrets     map[string]function.SecretRef
}

// buildFn assembles a prepared function from spec. The runtime defaults to
// node24 so ServiceEntry and rule resolution behave like production.
func buildFn(spec fnSpec, exec Executor) *PreparedFunction {
	if spec.runtime == "" {
		spec.runtime = "node24"
	}
	if spec.image == "" {
		spec.image = "x"
	}
	return NewPrepared(
		function.Function{
			Name: spec.name,
			Template: &function.Template{
				Runtime:     spec.runtime,
				Concurrency: spec.concurrency,
				Events:      spec.rules,
				Schedules:   spec.schedules,
				Env:         spec.env,
				Secrets:     spec.secrets,
			},
		},
		&runtime.Prepared{Name: spec.name, Image: spec.image},
		exec,
	)
}

// alwaysMatchRule is a single any-event rule with the default retry count.
func alwaysMatchRule(timeout time.Duration) function.EventRule {
	return function.EventRule{Handler: "index.run", Pattern: function.Pattern{}, Timeout: timeout, Retries: function.DefaultRetries}
}

// newFn builds a prepared function with no rules, used by registry-only tests.
func newFn(t *testing.T, name string) *PreparedFunction {
	t.Helper()
	return buildFn(fnSpec{name: name}, &countingExecutor{hold: time.Millisecond})
}

// alwaysMatchFn builds a function whose single rule matches any event and
// carries the default retry count.
func alwaysMatchFn(t *testing.T, name string, executor Executor) *PreparedFunction {
	t.Helper()
	return buildFn(fnSpec{name: name, rules: []function.EventRule{alwaysMatchRule(time.Second)}}, executor)
}

// fp is a valid, always-matching prepared function whose executor doubles as an
// ImageCleaner so the runner's resolver finds it.
func fpClean(t *testing.T, name, image string, exec Executor) *PreparedFunction {
	t.Helper()
	return buildFn(fnSpec{name: name, image: image, rules: []function.EventRule{alwaysMatchRule(time.Second)}}, exec)
}

// fnWithTimeout builds a prepared function whose single rule matches any event
// and carries the given handler timeout and the default retry count.
func fnWithTimeout(t *testing.T, name string, timeout time.Duration, executor Executor) *PreparedFunction {
	t.Helper()
	return buildFn(fnSpec{name: name, rules: []function.EventRule{alwaysMatchRule(timeout)}}, executor)
}

// fnWithRetries builds a prepared function whose single rule matches any event
// and carries the given retry count (additional attempts after the first).
func fnWithRetries(t *testing.T, name string, retries int, executor Executor) *PreparedFunction {
	t.Helper()
	rule := alwaysMatchRule(time.Second)
	rule.Retries = retries
	return buildFn(fnSpec{name: name, rules: []function.EventRule{rule}}, executor)
}

// fnWithConcurrency builds an always-matching function with the given template
// concurrency (0 = unparsed/default in the runner).
func fnWithConcurrency(t *testing.T, name string, concurrency int, executor Executor) *PreparedFunction {
	t.Helper()
	rule := alwaysMatchRule(time.Second)
	rule.Retries = 0
	return buildFn(fnSpec{name: name, concurrency: concurrency, rules: []function.EventRule{rule}}, executor)
}

// fnWithEnv builds a prepared function whose template carries env and secrets.
func fnWithEnv(t *testing.T, name string, executor Executor, env map[string]string, secrets map[string]function.SecretRef) *PreparedFunction {
	t.Helper()
	rule := alwaysMatchRule(0)
	return buildFn(fnSpec{name: name, rules: []function.EventRule{rule}, env: env, secrets: secrets}, executor)
}

// schedFn returns a prepared function with an empty schedule-entry rule set but
// that still carries the runtime/template the InvokeHandler path reads. Its
// schedule entry carries the given timeout and a zero retry count (a schedule
// whose retries are unset, so the first failure exhausts).
func schedFn(t *testing.T, name string, executor Executor, scheduleTimeout time.Duration) *PreparedFunction {
	t.Helper()
	return schedFnRetries(t, name, executor, scheduleTimeout, 0)
}

// schedFnRetries returns a prepared function with a single schedule entry for
// handler "index.run" carrying the given timeout and retry count.
func schedFnRetries(t *testing.T, name string, executor Executor, scheduleTimeout time.Duration, retries int) *PreparedFunction {
	t.Helper()
	rule := alwaysMatchRule(scheduleTimeout)
	return buildFn(fnSpec{
		name:  name,
		rules: []function.EventRule{rule},
		schedules: []function.Schedule{{
			Handler:  "index.run",
			Cron:     "0 3 * * *",
			Location: time.UTC,
			Timeout:  scheduleTimeout,
			Retries:  retries,
		}},
	}, executor)
}

// --- fake invocation state ---

// fakeInvocationState is an in-memory InvocationState for runner tests,
// avoiding a Redis dependency. It records which invocations have completed,
// which are protected by an active running deadline or a retry backoff, and
// which are exhausted, mirroring the real stream.invocationState semantics
// (TryStart/RecordFailure/MarkComplete/MarkExhausted/IsTerminal).
type fakeInvocationState struct {
	mu        sync.Mutex
	done      map[string]bool
	marks     []string
	running   map[string]time.Time // invocation -> running deadline
	nextAt    map[string]time.Time // invocation -> next-attempt deadline
	exhausted map[string]int       // invocation -> attempts
	attempts  map[string]int       // invocation -> highest attempt started
	now       func() time.Time
	// failures records the backoff passed to RecordFailure, for tests to assert
	// the retry schedule.
	failures []time.Duration
}

func newFakeInvocationState() *fakeInvocationState {
	return &fakeInvocationState{
		done:      map[string]bool{},
		running:   map[string]time.Time{},
		nextAt:    map[string]time.Time{},
		exhausted: map[string]int{},
		attempts:  map[string]int{},
		now:       time.Now,
	}
}

// setClock overrides the fake's clock so tests can freeze or advance time.
func (p *fakeInvocationState) setClock(now func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.now = now
}

// advance moves the fake's clock forward by d, so a retry backoff or running
// deadline that has not yet elapsed can be made to expire. Successive advances
// accumulate.
func (p *fakeInvocationState) advance(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prev := p.now
	p.now = func() time.Time { return prev().Add(d) }
}

func (p *fakeInvocationState) IsComplete(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[invocation]
}

func (p *fakeInvocationState) MarkComplete(invocation string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done[invocation] = true
	delete(p.running, invocation)
	delete(p.nextAt, invocation)
	delete(p.exhausted, invocation)
	p.marks = append(p.marks, invocation)
}

// TryStart claims the invocation for a new execution unless it is complete,
// exhausted, or protected by an active running deadline or retry backoff.
func (p *fakeInvocationState) TryStart(invocation string, timeout time.Duration) (started bool, attempt int, wait time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done[invocation] {
		return false, 0, 0
	}
	if n, ok := p.exhausted[invocation]; ok {
		return false, n, 0
	}
	if dl, ok := p.running[invocation]; ok && p.now().Before(dl) {
		return false, p.attempts[invocation], dl.Sub(p.now())
	}
	if dl, ok := p.nextAt[invocation]; ok && p.now().Before(dl) {
		return false, p.attempts[invocation], dl.Sub(p.now())
	}
	attempt = p.attempts[invocation] + 1
	p.attempts[invocation] = attempt
	p.running[invocation] = p.now().Add(timeout)
	return true, attempt, 0
}

// RecordFailure records the retry backoff for a failed attempt, gating the
// invocation until now+backoff.
func (p *fakeInvocationState) RecordFailure(invocation string, backoff time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures = append(p.failures, backoff)
	delete(p.running, invocation)
	p.nextAt[invocation] = p.now().Add(backoff)
}

// MarkExhausted records that the invocation's attempts are exhausted.
func (p *fakeInvocationState) MarkExhausted(invocation string, attempts int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.running, invocation)
	delete(p.nextAt, invocation)
	p.exhausted[invocation] = attempts
}

// IsTerminal reports whether the invocation is complete or exhausted.
func (p *fakeInvocationState) IsTerminal(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[invocation] || p.exhausted[invocation] > 0
}

// runningDeadline returns the persisted running deadline for an invocation, for
// tests to inspect the fake's internals.
func (p *fakeInvocationState) runningDeadline(invocation string) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	dl, ok := p.running[invocation]
	return dl, ok
}
