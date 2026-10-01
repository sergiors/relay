package cli

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/state"
)

// normWS collapses runs of whitespace (including newlines) in s to single
// spaces, so tabwriter column padding changes from label-length edits do not
// break assertions that care about label/value content rather than alignment.
// Exact-alignment contract anchors are kept in the dedicated render tests
// (e.g. TestPrintStats).
func normWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// openTempState opens a fresh temp state DB under per-test dependencies and
// returns it with those deps. It replaces the former package-level statePath
// global: each test owns its explicit temp locations, so no shared filesystem
// state is mutated or restored.
func openTempState(t *testing.T) (*state.State, Dependencies) {
	t.Helper()
	deps := testDeps(t)
	st, err := state.Open(deps.StatePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, deps
}

// seedTestState creates a temp state DB under test dependencies and populates a
// ready app and a pending one. It returns the opened state DB and the deps
// whose StatePath points at it, so command tests run with runCLIWithDeps read
// exactly what the test seeded.
func seedTestState(t *testing.T) (*state.State, Dependencies) {
	t.Helper()
	st, deps := openTempState(t)

	tmpl, _ := app.ParseTemplate([]byte(`runtime: python3.14
resources:
  memory: 256MiB
  cpus: 0.5
  pids: 64
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
    timeout: 6s
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
    timeout: 20s
`))
	readyFn := app.App{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl}
	st.RecordReconcileSuccess("user-events-python", "relay-app-user-events-python", "abc123hash", time.Now(), readyFn)

	nodeTmpl, _ := app.ParseTemplate([]byte(`runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`))
	nodeFn := app.App{Name: "welcome-email-node", Dir: filepath.Join(t.TempDir(), "y"), Template: nodeTmpl}
	st.RecordDiscovered(nodeFn)

	return st, deps
}

// ls prints the concise header plus both rows with correct status, sorted. The
// inventory carries exactly NAME, RUNTIME, STATUS, UPDATED — the old HANDLERS
// column (and any workload-count replacement) is deliberately gone.
func TestAppListColumns(t *testing.T) {
	st, _ := seedTestState(t)
	var w bytes.Buffer
	if err := printList(&w, st); err != nil {
		t.Fatalf("printList: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(w.String()), "\n")
	hdr := lines[0]
	for _, col := range []string{"NAME", "RUNTIME", "STATUS", "UPDATED"} {
		if !strings.Contains(hdr, col) {
			t.Fatalf("header missing %q: %q", col, hdr)
		}
	}
	if strings.Contains(hdr, "HANDLERS") {
		t.Fatalf("header must not carry HANDLERS: %q", hdr)
	}
	var userRow, nodeRow string
	for _, l := range lines[1:] {
		if strings.HasPrefix(l, "user-events-python") {
			userRow = l
		}
		if strings.HasPrefix(l, "welcome-email-node") {
			nodeRow = l
		}
	}
	if !strings.Contains(userRow, "python3.14") || !strings.Contains(userRow, state.StatusReady) {
		t.Fatalf("user row wrong: %q", userRow)
	}
	if !strings.Contains(nodeRow, "node24") || !strings.Contains(nodeRow, state.StatusPreparing) {
		t.Fatalf("node row wrong: %q", nodeRow)
	}
}

// TestAppListStatusesConsistentWithInspect pins that `ls` and `inspect`
// render the SAME persisted status for every public lifecycle value, so the two
// views can never disagree about an app's state. It exercises the full
// public set — preparing, building, reconciling, ready, degraded, unavailable —
// through the two renderers.
func TestAppListStatusesConsistentWithInspect(t *testing.T) {
	st, _ := openTempState(t)
	tmpl, err := app.ParseTemplate([]byte("runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	fn := func(name string) app.App {
		return app.App{Name: name, Dir: filepath.Join(t.TempDir(), name), Template: tmpl}
	}

	// Seed each status through its production write path.
	st.RecordDiscovered(fn("st-preparing")) // preparing
	prepFn := fn("st-building")
	st.RecordDiscovered(prepFn)
	st.RecordReconcileBuilding("st-building")
	reconFn := fn("st-reconciling")
	st.RecordDiscovered(reconFn)
	st.RecordReconciling("st-reconciling")
	st.RecordReconcileSuccess("st-ready", "img", "fp", time.Now(), fn("st-ready"))
	degFn := fn("st-degraded")
	st.RecordReconcileSuccess("st-degraded", "img", "fp", time.Now(), degFn)
	st.RecordServiceFailure("st-degraded", errors.New("service failed"))
	unavFn := fn("st-unavailable")
	st.RecordDiscovered(unavFn)
	st.RecordReconcileFailure("st-unavailable", errors.New("build failed"))

	want := map[string]string{
		"st-preparing":   state.StatusPreparing,
		"st-building":    state.StatusBuilding,
		"st-reconciling": state.StatusReconciling,
		"st-ready":       state.StatusReady,
		"st-degraded":    state.StatusDegraded,
		"st-unavailable": state.StatusUnavailable,
	}

	// ls: each row carries its status column verbatim.
	var lw bytes.Buffer
	if err := printList(&lw, st); err != nil {
		t.Fatalf("printList: %v", err)
	}
	rows := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(lw.String()), "\n")[1:] {
		fields := strings.Fields(l)
		if len(fields) >= 3 {
			rows[fields[0]] = fields[2]
		}
	}
	for name, status := range want {
		if got := rows[name]; got != status {
			t.Errorf("ls status for %s = %q, want %q", name, got, status)
		}
		// inspect: the same persisted snapshot renders the same status.
		detail, ok := st.GetApp(name)
		if !ok {
			t.Fatalf("expected function %s", name)
		}
		var iw bytes.Buffer
		printInspect(&iw, st, detail)
		if !strings.Contains(normWS(iw.String()), "Status: "+status) {
			t.Errorf("inspect status for %s missing %q:\n%s", name, status, iw.String())
		}
	}
}

// inspect returns full detail including handlers and timeouts.
func TestAppInspectDetail(t *testing.T) {
	st, _ := seedTestState(t)
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := normWS(w.String())
	// Whitespace is normalized: this test pins which detail rows render, not
	// their tabwriter column alignment (that is anchored in TestPrintStats).
	for _, want := range []string{
		"Name: user-events-python",
		"Runtime: python3.14",
		"Status: ready",
		"Image: relay-app-user-events-python",
		"Fingerprint: abc123hash",
		"Prepared:",
		"Last reconcile:",
		"Resources: memory=256MiB cpus=0.5 pids=64",
		"events.created.handler",
		"timeout=6s",
		"events.updated.handler",
		"timeout=20s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// A pending app omits Image/Fingerprint/Prepared.
func TestAppInspectPendingOmitsActiveFields(t *testing.T) {
	st, _ := seedTestState(t)
	detail, ok := st.GetApp("welcome-email-node")
	if !ok {
		t.Fatal("expected pending function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := w.String()
	if strings.Contains(out, "Image:") || strings.Contains(out, "Prepared:") || strings.Contains(out, "Fingerprint:") {
		t.Fatalf("pending function should omit Image/Fingerprint/Prepared:\n%s", out)
	}
}

// printInspect appends a per-app stats section between the metadata block
// and the Handlers section, using the same wider padding as Handlers for visual
// grouping. The long label "Handler successes:" drives the value column.
func TestAppInspectStatsSection(t *testing.T) {
	st, _ := seedTestState(t)
	st.RecordAppStats(state.AppStats{
		App:                 "user-events-python",
		EventsMatchedTotal:  12493,
		HandlerSuccessTotal: 12470,
		HandlerFailureTotal: 23,
		RetryTotal:          17,
		DLQTotal:            2,
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := normWS(w.String())
	for _, want := range []string{
		"Stats:",
		"Events matched: 12493",
		"Handler successes: 12470",
		"Handler failures: 23",
		"Handler retries: 17",
		"Invocations exhausted: 2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// The Stats section sits between the metadata and Events, so Events still
	// renders after it.
	si, hi := strings.Index(out, "Stats:"), strings.Index(out, "Events:")
	if si == -1 || hi == -1 || si > hi {
		t.Fatalf("Stats section should precede Events:\n%s", out)
	}
}

// An app with no recorded stats still renders a predictable zero Stats
// section rather than omitting it.
func TestAppInspectStatsZeroWithoutRow(t *testing.T) {
	st, _ := seedTestState(t)
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := normWS(w.String())
	for _, want := range []string{
		"Stats:",
		"Events matched: 0",
		"Handler successes: 0",
		"Handler failures: 0",
		"Handler retries: 0",
		"Invocations exhausted: 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// TestAppInspectShowsEnvSecretsMappings verifies inspect renders the env
// NAMES (values redacted) and secret REFERENCE names, but never a literal env
// value and never a secret value.
func TestAppInspectShowsEnvSecretsMappings(t *testing.T) {
	st, _ := openTempState(t)

	tmpl, _ := app.ParseTemplate([]byte(`runtime: python3.14
env:
  API_URL: https://api.example.com
secrets:
  DATABASE_URL: database-url
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`))
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		app.App{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := w.String()
	for _, want := range []string{
		"Environment:",
		"API_URL=[redacted]",
		"Secrets:",
		"DATABASE_URL=database-url",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// Inspect shows env NAMES only: the literal env value and any secret value
	// must never appear.
	for _, leaked := range []string{"https://api.example.com", "postgres://", "database-url-secret"} {
		if strings.Contains(out, leaked) {
			t.Errorf("inspect leaked a value %q:\n%s", leaked, out)
		}
	}
}

// An app with schedules renders a Schedules section after Events, showing
// the verbatim cron expression, effective timezone, and resolved timeout.
func TestAppInspectSchedulesSection(t *testing.T) {
	st, _ := openTempState(t)

	tmpl, _ := app.ParseTemplate([]byte(`runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
  - name: jobs.report.handler
    handler: jobs.report.handler
    cron: "0 8 * * 1-5"
    timezone: Europe/Rome
    timeout: 20s
`))
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		app.App{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := w.String()

	// The Schedules section follows Events.
	ei, si := strings.Index(out, "Events:"), strings.Index(out, "Schedules:")
	if ei == -1 || si == -1 || ei > si {
		t.Fatalf("Schedules should follow Events:\n%s", out)
	}
	for _, want := range []string{
		`jobs.cleanup.handler   handler=jobs.cleanup.handler cron="0 3 * * *" (At 03:00) timezone=UTC timeout=6s`,
		`jobs.report.handler    handler=jobs.report.handler cron="0 8 * * 1-5" (At 08:00, Monday through Friday) timezone=Europe/Rome timeout=20s`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// inspectSchedules is a helper that seeds a temp state DB with an app whose
// template schedules match scheds (yaml fragments), then returns the rendered
// inspect Schedules output.
func inspectSchedules(t *testing.T, scheds string) string {
	t.Helper()
	st, _ := openTempState(t)

	tmpl, err := app.ParseTemplate([]byte(`runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
` + scheds))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		app.App{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	return w.String()
}

// A 6-field (seconds) cron is rejected at template parse, so it can never be
// rendered by inspect (and never registered by the scheduler).
func TestAppInspectSchedulesRejectsSixField(t *testing.T) {
	_, err := app.ParseTemplate([]byte(`runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "30 0 0 * * *"
`))
	if err == nil || !strings.Contains(err.Error(), "6-field (seconds) cron is not supported") {
		t.Fatalf("err = %v, want a clear six-field rejection", err)
	}
}

// Descriptions always use 24-hour time: the 08:30 schedule must not contain
// AM/PM.
func TestAppInspectSchedulesNoAMPM(t *testing.T) {
	out := inspectSchedules(t, `  - name: report
    handler: jobs.report.handler
    cron: "30 8 * * 1-5"
`)
	if !strings.Contains(out, `cron="30 8 * * 1-5" (At 08:30, Monday through Friday) timezone=UTC`) {
		t.Errorf("inspect output missing 24h description:\n%s", out)
	}
	if strings.Contains(out, "AM") || strings.Contains(out, "PM") {
		t.Errorf("inspect output must not contain AM/PM:\n%s", out)
	}
}

// The description is independent of the schedule's timezone: two rows that
// differ only in timezone render the identical description text.
func TestAppInspectSchedulesTimezoneIndependence(t *testing.T) {
	out := inspectSchedules(t, `  - name: a
    handler: jobs.a.handler
    cron: "0 0 * * *"
    timezone: America/Sao_Paulo
  - name: b
    handler: jobs.b.handler
    cron: "0 0 * * *"
    timezone: Europe/Rome
`)
	// Extract both description substrings and assert equality. The description
	// is purely an app of the cron; the timezone column must not alter it.
	if !strings.Contains(out, `cron="0 0 * * *" (At 00:00) timezone=America/Sao_Paulo`) {
		t.Errorf("inspect output missing first row description:\n%s", out)
	}
	if !strings.Contains(out, `cron="0 0 * * *" (At 00:00) timezone=Europe/Rome`) {
		t.Errorf("inspect output missing second row description:\n%s", out)
	}
}

// When description generation fails, inspect falls back to the raw cron output
// (no parentheses) and never fails. The error path is exercised by injecting a
// descriptor that always fails via the describeCronFunc seam.
func TestAppInspectSchedulesDescriptionFallback(t *testing.T) {
	orig := describeCronFunc
	describeCronFunc = func(string) (string, bool) { return "", false }
	defer func() { describeCronFunc = orig }()

	out := inspectSchedules(t, `  - name: cleanup
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
`)
	if !strings.Contains(out, `cleanup   handler=jobs.cleanup.handler cron="0 3 * * *" timezone=UTC timeout=6s`) {
		t.Errorf("inspect fallback output missing raw cron row:\n%s", out)
	}
	// The schedule row must not carry a parenthesized description. (Note: the
	// whole output may contain parens elsewhere, e.g. "Last reconcile (0s ago)",
	// so scope the check to the schedule row only.)
	if strings.Contains(out, `cron="0 3 * * *" (`) {
		t.Errorf("inspect fallback output must not contain a parenthesized description:\n%s", out)
	}
}

// A template without schedules renders no Schedules: header.
func TestAppInspectNoSchedulesHeader(t *testing.T) {
	st, _ := seedTestState(t)
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	if strings.Contains(w.String(), "Schedules:") {
		t.Fatalf("inspect must omit Schedules: for a template without schedules:\n%s", w.String())
	}
}

// An app with services renders a Services section after Schedules (or
// after Events when no schedules) and before Environment, showing the effective
// entrypoint file, port, and replica count (defaults applied).
func TestAppInspectServicesSection(t *testing.T) {
	out := inspectServicesWithEnv(t, `  - name: service
    entrypoint: service.js
`, true)
	for _, want := range []string{"Services:", "service", "source=service.js", "port=80", "replicas=1"} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	ei, si, env := strings.Index(out, "Events:"), strings.Index(out, "Services:"), strings.Index(out, "Environment:")
	if ei == -1 || si == -1 || ei > si {
		t.Fatalf("Services should follow Events:\n%s", out)
	}
	if env == -1 || env < si {
		t.Fatalf("Environment should follow Services:\n%s", out)
	}
}

// Explicit port/replicas override the defaults.
func TestAppInspectServicesExplicit(t *testing.T) {
	out := inspectServices(t, `  - name: api
    entrypoint: api.js
    port: 3000
    replicas: 3
`)
	for _, want := range []string{"Services:", "api", "source=api.js", "port=3000", "replicas=3"} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// A configured path renders alongside port/replicas; a host-only service keeps
// the pre-path rendering with no path token.
func TestAppInspectServicesPath(t *testing.T) {
	out := inspectServices(t, `  - name: v2
    entrypoint: v2.js
    host: api.example.com
    path: /v2
    port: 3000
  - name: plain
    entrypoint: plain.js
    port: 80
`)
	if !strings.Contains(out, "path=/v2") {
		t.Errorf("inspect output missing path=/v2\n%s", out)
	}
	if !strings.Contains(out, "plain.js") {
		t.Errorf("inspect output missing plain.js\n%s", out)
	}
	if strings.Contains(out, "plain.js\tport=80 replicas=1 path=") {
		t.Errorf("host-only service must not render a path token\n%s", out)
	}
}

// Services render after Schedules when both are present.
func TestAppInspectServicesAfterSchedules(t *testing.T) {
	st, _ := openTempState(t)

	tmpl, err := app.ParseTemplate([]byte(`runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
services:
  - name: service
    entrypoint: service.js
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		app.App{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := w.String()
	si, se := strings.Index(out, "Schedules:"), strings.Index(out, "Services:")
	if si == -1 || se == -1 || si > se {
		t.Fatalf("Services should follow Schedules:\n%s", out)
	}
}

// A template without services renders no Services: header.
func TestAppInspectNoServicesHeader(t *testing.T) {
	st, _ := seedTestState(t)
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	if strings.Contains(w.String(), "Services:") {
		t.Fatalf("inspect must omit Services: for a template without services:\n%s", w.String())
	}
}

// inspectServices is a helper that seeds a temp state DB with an app whose
// template services match svcs (yaml fragments), then returns the rendered
// inspect output. When withEnv is true the template also defines an env var so
// the Environment section renders (to assert ordering).
func inspectServices(t *testing.T, svcs string) string {
	t.Helper()
	return inspectServicesWithEnv(t, svcs, false)
}

func inspectServicesWithEnv(t *testing.T, svcs string, withEnv bool) string {
	t.Helper()
	st, _ := openTempState(t)

	tmplBody := `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`
	if withEnv {
		tmplBody += "env:\n  FOO: bar\n"
	}
	tmpl, err := app.ParseTemplate([]byte(tmplBody + "services:\n" + svcs))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		app.App{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	return w.String()
}

// Arg handling: usage errors and the unknown-app error are returned with
// their messages (cmd/main.go prints them and exits 1). `app` alone no
// longer errors — it shows help (see TestAppBareShowsHelp). An unknown
// token and missing/extra arguments still return errors carrying the expected
// message.
func TestAppCommandErrors(t *testing.T) {
	_, deps := seedTestState(t)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"app", "bogus"}, "unknown command: relay app bogus"},
		{[]string{"app", "ls", "extra"}, "app ls: too many arguments"},
		{[]string{"app", "inspect"}, "not provided"},
		{[]string{"app", "inspect", "ghost"}, `unknown app "ghost"`},
	} {
		_, _, err := runCLIWithDeps(t, deps, "", tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args %v: err = %v, want message containing %q", tc.args, err, tc.want)
		}
	}
}

// A bare `relay app` shows the subcommand help on stdout and exits 0,
// listing the app subcommands.
func TestAppBareShowsHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "app")
	if err != nil {
		t.Fatalf("function alone: err = %v, want nil", err)
	}
	for _, want := range []string{"ls", "inspect"} {
		if !strings.Contains(out, want) {
			t.Fatalf("function help missing %q:\n%s", want, out)
		}
	}
}

// An unknown app subcommand returns the friendly Docker-style usage error
// naming the full command path.
func TestAppUnknownCommandFriendly(t *testing.T) {
	_, _, err := runCLI(t, "", "app", "bogus")
	if err == nil {
		t.Fatal("function bogus: err = nil, want usage error")
	}
	for _, want := range []string{
		"relay: unknown command: relay app bogus",
		"Run 'relay app --help' for more information",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("function bogus err missing %q: %v", want, err)
		}
	}
}

// An inspect of an unknown app writes the expected message to the
// returned error (the ExitErrHandler is a silent no-op); cmd/main.go prints it
// and exits 1.
func TestAppInspectUnknownMessage(t *testing.T) {
	_, deps := seedTestState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "app", "inspect", "no-such-fn")
	if err == nil || !strings.Contains(err.Error(), `unknown app "no-such-fn"`) {
		t.Fatalf("returned error missing unknown-function message: %v", err)
	}
}

// `relay app --help` prints the app help to stdout and exits 0.
func TestAppHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "app", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{
		"ls",
		"inspect",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

// `relay app ls` renders the table via the command path and exits 0.
func TestAppLsCommand(t *testing.T) {
	_, deps := seedTestState(t)
	out, _, err := runCLIWithDeps(t, deps, "", "app", "ls")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "user-events-python") || !strings.Contains(out, "welcome-email-node") {
		t.Fatalf("ls missing rows:\n%s", out)
	}
}

// `relay app inspect NAME` renders the detail via the command path.
func TestAppInspectCommand(t *testing.T) {
	_, deps := seedTestState(t)
	out, _, err := runCLIWithDeps(t, deps, "", "app", "inspect", "user-events-python")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(normWS(out), "Name: user-events-python") {
		t.Fatalf("inspect missing detail:\n%s", out)
	}
}

// `relay app inspect name extra` is a usage error (exit 2).
func TestAppInspectTooManyArgs(t *testing.T) {
	_, deps := seedTestState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "app", "inspect", "user-events-python", "extra")
	if err == nil || !strings.Contains(err.Error(), "app inspect: too many arguments") {
		t.Fatalf("returned error missing usage error: %v", err)
	}
}

// `relay app ls --help` and `relay app inspect --help` exit 0.
func TestAppCommandHelp(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"app", "ls", "--help"}, "app ls"},
		{[]string{"app", "inspect", "--help"}, "relay app inspect NAME"},
	}
	for _, c := range cases {
		out, _, err := runCLI(t, "", c.args...)
		if err != nil {
			t.Fatalf("%v: err = %v, want nil", c.args, err)
		}
		if !strings.Contains(out, c.want) {
			t.Fatalf("%v: stdout missing %q:\n%s", c.args, c.want, out)
		}
	}
}

// The Stats section renders the four per-app execution-history timestamps
// as relative ages ("2s ago"-style rows) after the Invocations exhausted line.
func TestAppInspectStatsTimestamps(t *testing.T) {
	st, _ := seedTestState(t)
	exec := time.Now().Add(-2 * time.Second).UTC()
	failure := time.Now().Add(-90 * time.Second).UTC()
	dlq := time.Now().Add(-48 * time.Hour).UTC()
	st.RecordAppStats(state.AppStats{
		App:                 "user-events-python",
		EventsMatchedTotal:  10,
		HandlerSuccessTotal: 8,
		HandlerFailureTotal: 2,
		RetryTotal:          4,
		DLQTotal:            1,
		LastExecutionAt:     exec.Format(time.RFC3339),
		LastSuccessAt:       exec.Format(time.RFC3339),
		LastFailureAt:       failure.Format(time.RFC3339),
		LastDLQAt:           dlq.Format(time.RFC3339),
	})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := normWS(w.String())
	for _, want := range []string{
		"Last execution: 2s ago",
		"Last success: 2s ago",
		"Last failure: 1m ago",
		"Last exhaustion: 2d ago",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// The rows come after "Invocations exhausted:".
	after := strings.Index(out, "Invocations exhausted:")
	for _, row := range []string{"Last execution:", "Last success:", "Last failure:", "Last exhaustion:"} {
		i := strings.Index(out, row)
		if i == -1 || i < after {
			t.Errorf("%s must follow Invocations exhausted (order):\n%s", row, out)
		}
	}
}

// An app whose timestamps were never observed renders "never" instead of
// an empty relative age.
func TestAppInspectStatsTimestampsNever(t *testing.T) {
	st, _ := seedTestState(t)
	st.RecordAppStats(state.AppStats{App: "user-events-python", EventsMatchedTotal: 3})
	detail, ok := st.GetApp("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := normWS(w.String())
	for _, want := range []string{
		"Last execution: never",
		"Last success: never",
		"Last failure: never",
		"Last exhaustion: never",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// TestAppBuildingStatusAndEmptyRuntimeRendering pins the state-reporting
// contract for an app caught mid-managed-runtime-build: `ls` and `inspect`
// render the same persisted "building" status verbatim, and a service-only
// template (which parses with no runtime) renders its empty runtime as "-"
// rather than a blank cell in both views. This is the CLI-side counterpart of
// the persistence test that resets a stale building status on restart. The
// service path never publishes building (services have no Relay-owned Dockerfile
// build), so the persisted building status is exercised on a runtime app.
func TestAppBuildingStatusAndEmptyRuntimeRendering(t *testing.T) {
	st, _ := openTempState(t)

	// A runtime app with a persisted building status (written at the
	// managed-runtime image build boundary).
	runtimeTmpl, err := app.ParseTemplate([]byte(`runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`))
	if err != nil {
		t.Fatalf("parse runtime template: %v", err)
	}
	st.RecordDiscovered(app.App{Name: "runtime-building", Dir: filepath.Join(t.TempDir(), "rb"), Template: runtimeTmpl})
	st.RecordReconcileBuilding("runtime-building")

	// A template whose only workload is an image-backed service: the runtime may
	// be omitted entirely, so the persisted Runtime is empty.
	tmpl, err := app.ParseTemplate([]byte(`services:
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 8080
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	fn := app.App{Name: "svc-only", Dir: filepath.Join(t.TempDir(), "svc"), Template: tmpl}
	st.RecordDiscovered(fn)

	// ls: the building row carries the building status; the service-only row
	// carries the "-" runtime placeholder (columns are NAME RUNTIME STATUS
	// UPDATED, so the placeholder is field 1).
	var lw bytes.Buffer
	if err := printList(&lw, st); err != nil {
		t.Fatalf("printList: %v", err)
	}
	rows := map[string][]string{}
	for _, l := range strings.Split(strings.TrimSpace(lw.String()), "\n")[1:] {
		fields := strings.Fields(l)
		if len(fields) > 0 {
			rows[fields[0]] = fields
		}
	}
	if f := rows["runtime-building"]; len(f) < 3 || f[2] != state.StatusBuilding {
		t.Fatalf("runtime row = %v, want status %q", f, state.StatusBuilding)
	}
	if f := rows["svc-only"]; len(f) < 3 || f[1] != "-" {
		t.Fatalf("svc-only row = %v, want runtime placeholder '-'", f)
	}

	// inspect: the same persisted snapshots render the same status and the same
	// runtime placeholder.
	detail, ok := st.GetApp("runtime-building")
	if !ok {
		t.Fatal("expected runtime-building function")
	}
	var iw bytes.Buffer
	printInspect(&iw, st, detail)
	out := normWS(iw.String())
	if !strings.Contains(out, "Status: "+state.StatusBuilding) {
		t.Errorf("inspect output missing building status:\n%s", out)
	}

	svcDetail, ok := st.GetApp("svc-only")
	if !ok {
		t.Fatal("expected svc-only function")
	}
	var sw bytes.Buffer
	printInspect(&sw, st, svcDetail)
	if !strings.Contains(normWS(sw.String()), "Runtime: -") {
		t.Errorf("inspect output missing 'Runtime: -':\n%s", sw.String())
	}
}

// Services using entrypoint and image sources render with their source kind
// prefix, so an operator can tell how each service is produced.
func TestAppInspectServicesSourceKinds(t *testing.T) {
	st, _ := openTempState(t)
	tmpl, err := app.ParseTemplate([]byte(`runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
services:
  - name: service
    entrypoint: service.js
    port: 3000
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 9090
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	st.RecordReconcileSuccess("src-kinds", "img", "fp", time.Now(),
		app.App{Name: "src-kinds", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})
	detail, ok := st.GetApp("src-kinds")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, detail)
	out := w.String()
	for _, want := range []string{
		"service.js", "port=3000",
		"image:ghcr.io/acme/api:1.2", "port=9090",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}
