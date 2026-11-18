package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/state"
	"relay/internal/testutil"
	"time"
)

// startupLogRecord is the parsed shape of one captured JSON log line for
// lifecycle-ordering assertions: the message, level, and the per-app fields
// the startup lifecycle lines carry.
type startupLogRecord struct {
	Msg         string `json:"msg"`
	Level       string `json:"level"`
	App         string `json:"app"`
	Fingerprint string `json:"fingerprint"`
	Outcome     string `json:"outcome"`
	Error       string `json:"error"`
}

// decodeStartupLog parses captured JSON log output, skipping any line that is
// not a structured record. Callers run after the producing goroutines settled,
// so every line is a complete record.
func decodeStartupLog(raw string) []startupLogRecord {
	var records []startupLogRecord
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec startupLogRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		records = append(records, rec)
	}
	return records
}

// parseStartupLog is decodeStartupLog for the test goroutine: a line that is
// not a structured record fails the test instead of silently dropping it.
func parseStartupLog(t *testing.T, raw string) []startupLogRecord {
	t.Helper()
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !json.Valid([]byte(line)) {
			t.Fatalf("captured log line is not JSON: %q", line)
		}
	}
	return decodeStartupLog(raw)
}

// captureStartupLog returns a debug-level JSON logger writing into a
// concurrency-safe buffer, for tests that assert lifecycle messages, fields,
// and ordering.
func captureStartupLog() (*slog.Logger, *testutil.SyncBuffer) {
	logs := &testutil.SyncBuffer{}
	return slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})), logs
}

// startupLogHas reports whether the captured log already holds the message
// for the app. Fake goroutines call it to observe ordering; it never fails the
// test, so an unparseable line counts as "not present".
func startupLogHas(raw, msg, app string) bool {
	for _, rec := range decodeStartupLog(raw) {
		if rec.Msg == msg && rec.App == app {
			return true
		}
	}
	return false
}

// startupRecordsForMsg filters captured records by exact message.
func startupRecordsForMsg(records []startupLogRecord, msg string) []startupLogRecord {
	var out []startupLogRecord
	for _, rec := range records {
		if rec.Msg == msg {
			out = append(out, rec)
		}
	}
	return out
}

// startupRecordsForApp filters captured records by app field.
func startupRecordsForApp(records []startupLogRecord, app string) []startupLogRecord {
	var out []startupLogRecord
	for _, rec := range records {
		if rec.App == app {
			out = append(out, rec)
		}
	}
	return out
}

// triageLevels are the levels an operator must triage: shutdown must never
// raise a startup transition into one of them.
var triageLevels = map[string]bool{"WARN": true, "ERROR": true, "CRITICAL": true}

// triageRecords filters captured records to WARN and above.
func triageRecords(records []startupLogRecord) []startupLogRecord {
	var out []startupLogRecord
	for _, rec := range records {
		if triageLevels[rec.Level] {
			out = append(out, rec)
		}
	}
	return out
}

// appsOf returns the sorted app field of each record, for exact-set
// comparisons of "which apps got this line".
func appsOf(records []startupLogRecord) []string {
	names := make([]string, 0, len(records))
	for _, rec := range records {
		names = append(names, rec.App)
	}
	sort.Strings(names)
	return names
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// lifecycleSpyPreparer stands in for the Manager behind the appPreparer seam.
// Every Prepare call snapshots whether the app's `App: preparing` line was
// ALREADY in the captured log at call entry, so a test can pin that the
// lifecycle start transition is emitted before the build starts. Scripted
// failures and parked preparations reuse the startup pool test's gate
// semantics. The log handle is read-only; recording fields are mutex-guarded
// because the pool may call Prepare concurrently.
type lifecycleSpyPreparer struct {
	logs  *testutil.SyncBuffer
	parks map[string]*poolPark
	fail  map[string]string

	mu               sync.Mutex
	preparingAtEntry map[string]bool
}

func newLifecycleSpyPreparer(logs *testutil.SyncBuffer) *lifecycleSpyPreparer {
	return &lifecycleSpyPreparer{
		logs:             logs,
		parks:            map[string]*poolPark{},
		fail:             map[string]string{},
		preparingAtEntry: map[string]bool{},
	}
}

func (p *lifecycleSpyPreparer) PrepareWithFingerprintAndSelection(
	ctx context.Context,
	fn app.App,
	_ string,
	_ *source.Selection,
) (*runtime.Prepared, error) {
	logged := startupLogHas(p.logs.String(), "App: preparing", fn.Name)
	p.mu.Lock()
	p.preparingAtEntry[fn.Name] = logged
	p.mu.Unlock()

	if msg := p.fail[fn.Name]; msg != "" {
		return nil, errors.New(msg)
	}
	if park := p.parks[fn.Name]; park != nil {
		park.markArrived()
		if err := park.await(ctx); err != nil {
			park.markReturned()
			return nil, err
		}
		park.markReturned()
	}
	return &runtime.Prepared{
		Name:        fn.Name,
		Image:       "img-" + fn.Name,
		Fingerprint: "built-" + fn.Name,
	}, nil
}

func (p *lifecycleSpyPreparer) MaxConcurrentBuilds() int { return 1 }

func (p *lifecycleSpyPreparer) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

func (p *lifecycleSpyPreparer) preparingLoggedAtEntry(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.preparingAtEntry[name]
}

// TestStartupPrepareLogsLifecycleStartFailureAndCancellation pins the
// per-app startup preparation lifecycle lines: the start transition is Info
// with the desired fingerprint before the build runs; a genuine failure is
// exactly ONE terminal per-app Error summary carrying the desired fingerprint
// and the error; a shutdown interruption stays below the triage levels.
func TestStartupPrepareLogsLifecycleStartFailureAndCancellation(t *testing.T) {
	t.Run("preparing is logged before Prepare with the desired fingerprint", func(t *testing.T) {
		// given a captured startup log and one app about to be prepared
		logger, logs := captureStartupLog()
		spy := newLifecycleSpyPreparer(logs)

		// when the startup pool prepares it
		prepareApps(context.Background(), spy, startupAppsFor("warm-a"), nil, nil, logger)

		// then the line was already present when Prepare was invoked, and it
		// carries the desired identity at Info with the startup outcome
		if !spy.preparingLoggedAtEntry("warm-a") {
			t.Error("App: preparing must be in the log before Prepare is invoked")
		}
		records := startupRecordsForMsg(parseStartupLog(t, logs.String()), "App: preparing")
		if len(records) != 1 {
			t.Fatalf("App: preparing records = %+v, want exactly one", records)
		}
		rec := records[0]
		if rec.App != "warm-a" || rec.Level != "INFO" {
			t.Errorf("preparing record = %+v, want app=warm-a at INFO", rec)
		}
		if rec.Fingerprint != "fp-warm-a" {
			t.Errorf("preparing fingerprint = %q, want %q", rec.Fingerprint, "fp-warm-a")
		}
		if rec.Outcome != "startup" {
			t.Errorf("preparing outcome = %q, want %q", rec.Outcome, "startup")
		}
	})

	t.Run("a failed preparation reports exactly one terminal error", func(t *testing.T) {
		// given a startup preparation whose build fails
		logger, logs := captureStartupLog()
		spy := newLifecycleSpyPreparer(logs)
		spy.fail["broken-app"] = "build exploded"

		prepareApps(context.Background(), spy, startupAppsFor("broken-app"), nil, nil, logger)

		// then exactly ONE record is at a triage level, and it is the terminal
		// per-app Error summary with the desired fingerprint and the error —
		// no second failure line restates the outcome
		triage := triageRecords(parseStartupLog(t, logs.String()))
		if len(triage) != 1 {
			t.Fatalf("triage-level records = %+v, want exactly one terminal summary", triage)
		}
		rec := triage[0]
		if rec.App != "broken-app" || rec.Level != "ERROR" {
			t.Errorf("failure record = %+v, want app=broken-app at ERROR", rec)
		}
		if rec.Fingerprint != "fp-broken-app" {
			t.Errorf("failure fingerprint = %q, want %q", rec.Fingerprint, "fp-broken-app")
		}
		if !strings.Contains(rec.Error, "build exploded") {
			t.Errorf("failure error = %q, want it to contain the build error", rec.Error)
		}
	})

	t.Run("shutdown cancellation of a preparation stays below the triage levels", func(t *testing.T) {
		// given a preparation parked inside its build when the lifecycle is
		// cancelled
		logger, logs := captureStartupLog()
		park := newPoolPark()
		spy := newLifecycleSpyPreparer(logs)
		spy.parks["cancel-app"] = park

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan []*runner.PreparedApp, 1)
		go func() {
			done <- prepareApps(ctx, spy, startupAppsFor("cancel-app"), nil, nil, logger)
		}()
		waitArrivedIn(t, spy.parks, "cancel-app")

		// when the shutdown cancels the in-flight preparation
		cancel()
		park.free()
		collectPrepared(t, done, []string{"cancel-app"}, map[string]bool{"cancel-app": true})

		// then the interruption raises no warning or error anywhere in the log,
		// and the app's own interruption line remains observable at Debug
		records := parseStartupLog(t, logs.String())
		if triage := triageRecords(records); len(triage) != 0 {
			t.Errorf("cancelled startup preparation raised triage-level records: %+v", triage)
		}
		sawDebug := false
		for _, rec := range startupRecordsForApp(records, "cancel-app") {
			if rec.Level == "DEBUG" {
				sawDebug = true
			}
		}
		if !sawDebug {
			t.Error("the shutdown interruption must remain observable at DEBUG for the app")
		}
	})
}

// observedPublicationFake is the startup selection publisher with an entry
// probe: it records, per accepted app, whether its `App: image published` line
// was already in the log when PublishPrepared was invoked, so a test can pin
// that publication is logged AFTER the fence accepts the generation.
type observedPublicationFake struct {
	current                map[string]string
	logs                   *testutil.SyncBuffer
	publishedLoggedAtEntry map[string]bool
}

func (f *observedPublicationFake) PublishPrepared(
	_ context.Context, prepared *runtime.Prepared, publish func() error,
) error {
	f.publishedLoggedAtEntry[prepared.Name] = startupLogHas(
		f.logs.String(), "App: image published", prepared.Name)
	if prepared.ImageSelection != f.current[prepared.Runtime] {
		return runtime.ErrRuntimeImageSelectionStale
	}
	return publish()
}

func (f *observedPublicationFake) RefreshRuntimeImageSelections(context.Context) ([]string, error) {
	return nil, nil
}

func (f *observedPublicationFake) RuntimeImageSelectionIdentity(runtimeID string) string {
	return f.current[runtimeID]
}

// observedPublicationStateFake records the startup success sink with the same
// entry probe for `App: ready`, so a test can pin that readiness is logged
// AFTER the accepted no-service state write, never before it.
type observedPublicationStateFake struct {
	logs               *testutil.SyncBuffer
	readyLoggedAtEntry map[string]bool
	successes          []string
}

func (f *observedPublicationStateFake) RecordReconcileSuccess(
	_ context.Context, name, _, _ string, _ time.Time, _ app.App, _ state.FencingToken,
) (bool, error) {
	f.readyLoggedAtEntry[name] = startupLogHas(f.logs.String(), "App: ready", name)
	f.successes = append(f.successes, name)
	return true, nil
}

func (f *observedPublicationStateFake) RecordReconcileFailure(
	_ context.Context, _ string, _ error, _ state.FencingToken,
) (bool, error) {
	return true, nil
}

// TestStartupPublicationLogsAcceptedImagesAndDefersReady pins the publication
// half of the startup lifecycle: `App: image published` is Info for exactly
// the generations the selection fence accepted — after acceptance, never for
// a rejected stale build or a preparation-failure placeholder — while no app
// reports ready at publication time. Readiness belongs to the asynchronous
// service completion, including for empty desired service sets.
func TestStartupPublicationLogsAcceptedImagesAndDefersReady(t *testing.T) {
	// given one accepted no-service app, one accepted service app, a stale
	// build the fence rejects, and a preparation-failure placeholder
	logger, logs := captureStartupLog()
	selection := "node:24-alpine@sha256:new"
	accepted := startupPreparedCandidate(
		startupTestApp(t, "pub-none", false), "relay-app-pub-none:v1", selection)
	acceptedSvc := startupPreparedCandidate(
		startupTestApp(t, "pub-svc", true), "relay-app-pub-svc:v1", selection)
	stale := startupPreparedCandidate(
		startupTestApp(t, "pub-stale", true), "relay-app-stale:v1", "older-pin")
	placeholder := runner.NewUnavailable(startupTestApp(t, "pub-failed", true))

	registry := &runner.Registry{}
	publisher := &observedPublicationFake{
		current:                map[string]string{"nodejs24.x": selection},
		logs:                   logs,
		publishedLoggedAtEntry: map[string]bool{},
	}
	stateSink := &observedPublicationStateFake{
		logs:               logs,
		readyLoggedAtEntry: map[string]bool{},
	}
	startup := startupAppSet{fencingTokens: map[string]state.FencingToken{
		"pub-none": 1, "pub-svc": 2, "pub-stale": 3, "pub-failed": 4,
	}}

	// when startup publication runs
	_, _ = publishStartupApps(
		context.Background(),
		[]*runner.PreparedApp{accepted, acceptedSvc, stale, placeholder},
		registry, publisher, stateSink, startup, logger,
	)

	records := parseStartupLog(t, logs.String())

	// then image publication is logged for exactly the accepted generations,
	// each after PublishPrepared accepted it — never for the stale build or
	// the preparation-failure placeholder
	published := startupRecordsForMsg(records, "App: image published")
	if got, want := appsOf(published), []string{"pub-none", "pub-svc"}; !equalStrings(got, want) {
		t.Fatalf("published apps = %v, want %v", got, want)
	}
	for _, name := range []string{"pub-none", "pub-svc"} {
		if publisher.publishedLoggedAtEntry[name] {
			t.Errorf("%s: App: image published was logged before PublishPrepared accepted the generation", name)
		}
	}
	for _, rec := range published {
		if rec.Level != "INFO" {
			t.Errorf("published record = %+v, want INFO", rec)
		}
	}

	// then no app reports ready during publication; every prepared app reports
	// readiness from the asynchronous completion callback
	ready := startupRecordsForMsg(records, "App: ready")
	if len(ready) != 0 {
		t.Fatalf("App: ready logged during publication: %+v", ready)
	}
	if len(stateSink.successes) != 0 {
		t.Errorf("startup success writes during publication = %v, want none before service completion", stateSink.successes)
	}
}

// startupRawRecords parses captured JSON output into raw attribute maps for
// every record carrying the exact message, failing on any line that is not a
// structured record. Raw maps distinguish an ABSENT key from an empty value.
func startupRawRecords(t *testing.T, raw, msg string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("captured log line is not JSON: %q: %v", line, err)
		}
		if record["msg"] == msg {
			records = append(records, record)
		}
	}
	return records
}

// TestStartupPublicationLogsImageOnlyWhenTheGenerationHasOne pins the image
// rule on the startup publication line: an accepted managed generation reports
// the image it was built from, while an accepted no-runtime generation — its
// external-image services bring their own — omits the attribute entirely
// rather than logging an empty identity. Both keep fingerprint and outcome.
func TestStartupPublicationLogsImageOnlyWhenTheGenerationHasOne(t *testing.T) {
	// given one accepted managed and one accepted no-runtime generation
	logger, logs := captureStartupLog()
	selection := "node:24-alpine@sha256:new"
	managed := startupPreparedCandidate(
		startupTestApp(t, "pub-managed", false),
		"relay-app-pub-managed:v1", selection,
	)
	noRuntimeApp := app.App{Name: "pub-external", Template: &app.Template{}}
	noRuntime := runner.NewPrepared(noRuntimeApp, &runtime.Prepared{
		Name: noRuntimeApp.Name, Fingerprint: "fp-pub-external",
	}, nil)
	registry := &runner.Registry{}
	publisher := &startupPublicationFake{
		current: map[string]string{"nodejs24.x": selection},
	}
	startup := startupAppSet{fencingTokens: map[string]state.FencingToken{
		"pub-managed": 1, "pub-external": 2,
	}}

	// when startup publication runs
	_, _ = publishStartupApps(
		context.Background(),
		[]*runner.PreparedApp{managed, noRuntime},
		registry, publisher, nil, startup, logger,
	)

	// then each accepted line carries its own image rule
	byApp := map[string]map[string]any{}
	for _, record := range startupRawRecords(
		t, logs.String(), "App: image published",
	) {
		name, _ := record["app"].(string)
		byApp[name] = record
	}
	managedRec := byApp["pub-managed"]
	if managedRec == nil || managedRec["image"] != "relay-app-pub-managed:v1" {
		t.Fatalf("managed publication = %+v, want the built image", managedRec)
	}
	externalRec := byApp["pub-external"]
	if externalRec == nil {
		t.Fatalf("no-runtime publication missing: %+v", byApp)
	}
	if value, ok := externalRec["image"]; ok {
		t.Errorf("no-runtime publication carries image=%v; omit the attribute",
			value)
	}
	for _, rec := range []map[string]any{managedRec, externalRec} {
		if rec["fingerprint"] == "" || rec["outcome"] != "discovered" {
			t.Errorf(
				"publication = %+v, want fingerprint and outcome=discovered",
				rec,
			)
		}
	}
}
