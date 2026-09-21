package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
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
// ready function and a pending one. It returns the opened state DB and the deps
// whose StatePath points at it, so command tests run with runCLIWithDeps read
// exactly what the test seeded.
func seedTestState(t *testing.T) (*state.State, Dependencies) {
	t.Helper()
	st, deps := openTempState(t)

	tmpl, _ := function.ParseTemplate([]byte(`runtime: python3.14
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
	readyFn := function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl}
	st.RecordReconcileSuccess("user-events-python", "relay-fn-user-events-python", "abc123hash", time.Now(), readyFn)

	nodeTmpl, _ := function.ParseTemplate([]byte(`runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`))
	nodeFn := function.Function{Name: "welcome-email-node", Dir: filepath.Join(t.TempDir(), "y"), Template: nodeTmpl}
	st.RecordDiscovered(nodeFn)

	return st, deps
}

// ls prints the concise header plus both rows with correct status, sorted. The
// inventory carries exactly NAME, RUNTIME, STATUS, UPDATED — the old HANDLERS
// column (and any workload-count replacement) is deliberately gone.
func TestFunctionListColumns(t *testing.T) {
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
	if !strings.Contains(userRow, "python3.14") || !strings.Contains(userRow, "ready") {
		t.Fatalf("user row wrong: %q", userRow)
	}
	if !strings.Contains(nodeRow, "node24") || !strings.Contains(nodeRow, "pending") {
		t.Fatalf("node row wrong: %q", nodeRow)
	}
}

// inspect returns full detail including handlers and timeouts.
func TestFunctionInspectDetail(t *testing.T) {
	st, _ := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := normWS(w.String())
	// Whitespace is normalized: this test pins which detail rows render, not
	// their tabwriter column alignment (that is anchored in TestPrintStats).
	for _, want := range []string{
		"Name: user-events-python",
		"Runtime: python3.14",
		"Status: ready",
		"Image: relay-fn-user-events-python",
		"Fingerprint: abc123hash",
		"Prepared:",
		"Last reconcile:",
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

// A pending function omits Image/Fingerprint/Prepared.
func TestFunctionInspectPendingOmitsActiveFields(t *testing.T) {
	st, _ := seedTestState(t)
	d, ok := st.GetFunction("welcome-email-node")
	if !ok {
		t.Fatal("expected pending function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := w.String()
	if strings.Contains(out, "Image:") || strings.Contains(out, "Prepared:") || strings.Contains(out, "Fingerprint:") {
		t.Fatalf("pending function should omit Image/Fingerprint/Prepared:\n%s", out)
	}
}

// printInspect appends a per-function stats section between the metadata block
// and the Handlers section, using the same wider padding as Handlers for visual
// grouping. The long label "Handler successes:" drives the value column.
func TestFunctionInspectStatsSection(t *testing.T) {
	st, _ := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{
		Function:             "user-events-python",
		EventsProcessedTotal: 12493,
		HandlerSuccessTotal:  12470,
		HandlerFailureTotal:  23,
		RetryTotal:           17,
		DLQTotal:             2,
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := normWS(w.String())
	for _, want := range []string{
		"Stats:",
		"Events processed: 12493",
		"Handler successes: 12470",
		"Handler failures: 23",
		"Retries: 17",
		"DLQ entries: 2",
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

// A function with no recorded stats still renders a predictable zero Stats
// section rather than omitting it.
func TestFunctionInspectStatsZeroWithoutRow(t *testing.T) {
	st, _ := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := normWS(w.String())
	for _, want := range []string{
		"Stats:",
		"Events processed: 0",
		"Handler successes: 0",
		"Handler failures: 0",
		"Retries: 0",
		"DLQ entries: 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// TestFunctionInspectShowsEnvSecretsMappings verifies inspect renders the
// env/secret MAPPINGS (literal env values and secret references) but never a
// secret VALUE.
func TestFunctionInspectShowsEnvSecretsMappings(t *testing.T) {
	st, _ := openTempState(t)

	tmpl, _ := function.ParseTemplate([]byte(`runtime: python3.14
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
		function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := w.String()
	for _, want := range []string{
		"Environment:",
		"API_URL=https://api.example.com",
		"Secrets:",
		"DATABASE_URL=database-url",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// The secret VALUE must never appear — only the reference.
	if strings.Contains(out, "postgres://") {
		t.Errorf("inspect leaked a secret value:\n%s", out)
	}
}

// A function with schedules renders a Schedules section after Events, showing
// the verbatim cron expression, effective timezone, and resolved timeout.
func TestFunctionInspectSchedulesSection(t *testing.T) {
	st, _ := openTempState(t)

	tmpl, _ := function.ParseTemplate([]byte(`runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - handler: jobs.cleanup.handler
    cron: "0 3 * * *"
  - handler: jobs.report.handler
    cron: "0 8 * * 1-5"
    timezone: Europe/Rome
    timeout: 20s
`))
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := w.String()

	// The Schedules section follows Events.
	ei, si := strings.Index(out, "Events:"), strings.Index(out, "Schedules:")
	if ei == -1 || si == -1 || ei > si {
		t.Fatalf("Schedules should follow Events:\n%s", out)
	}
	for _, want := range []string{
		`jobs.cleanup.handler   cron="0 3 * * *" (At 03:00) timezone=UTC timeout=6s`,
		`jobs.report.handler    cron="0 8 * * 1-5" (At 08:00, Monday through Friday) timezone=Europe/Rome timeout=20s`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// inspectSchedules is a helper that seeds a temp state DB with a function whose
// template schedules match scheds (yaml fragments), then returns the rendered
// inspect Schedules output.
func inspectSchedules(t *testing.T, scheds string) string {
	t.Helper()
	st, _ := openTempState(t)

	tmpl, err := function.ParseTemplate([]byte(`runtime: python3.14
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
		function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	return w.String()
}

// A 6-field (seconds) cron renders a human-readable description in the same
// format as 5-field ones.
func TestFunctionInspectSchedulesSixFieldDescription(t *testing.T) {
	out := inspectSchedules(t, `  - handler: jobs.cleanup.handler
    cron: "30 0 0 * * *"
`)
	if !strings.Contains(out, `cron="30 0 0 * * *" (At 00:00:30) timezone=UTC`) {
		t.Errorf("inspect output missing 6-field description:\n%s", out)
	}
}

// Descriptions always use 24-hour time: the 08:30 schedule must not contain
// AM/PM.
func TestFunctionInspectSchedulesNoAMPM(t *testing.T) {
	out := inspectSchedules(t, `  - handler: jobs.report.handler
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
func TestFunctionInspectSchedulesTimezoneIndependence(t *testing.T) {
	out := inspectSchedules(t, `  - handler: jobs.a.handler
    cron: "0 0 * * *"
    timezone: America/Sao_Paulo
  - handler: jobs.b.handler
    cron: "0 0 * * *"
    timezone: Europe/Rome
`)
	// Extract both description substrings and assert equality. The description
	// is purely a function of the cron; the timezone column must not alter it.
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
func TestFunctionInspectSchedulesDescriptionFallback(t *testing.T) {
	orig := describeCronFunc
	describeCronFunc = func(string) (string, bool) { return "", false }
	defer func() { describeCronFunc = orig }()

	out := inspectSchedules(t, `  - handler: jobs.cleanup.handler
    cron: "0 3 * * *"
`)
	if !strings.Contains(out, `jobs.cleanup.handler   cron="0 3 * * *" timezone=UTC timeout=6s`) {
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
func TestFunctionInspectNoSchedulesHeader(t *testing.T) {
	st, _ := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	if strings.Contains(w.String(), "Schedules:") {
		t.Fatalf("inspect must omit Schedules: for a template without schedules:\n%s", w.String())
	}
}

// A function with services renders a Services section after Schedules (or
// after Events when no schedules) and before Environment, showing the effective
// entrypoint file, port, and replica count (defaults applied).
func TestFunctionInspectServicesSection(t *testing.T) {
	out := inspectServicesWithEnv(t, `  - entrypoint: service.js
`, true)
	for _, want := range []string{"Services:", "service.js", "port=80", "replicas=1"} {
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
func TestFunctionInspectServicesExplicit(t *testing.T) {
	out := inspectServices(t, `  - entrypoint: api.js
    port: 3000
    replicas: 3
`)
	for _, want := range []string{"Services:", "api.js", "port=3000", "replicas=3"} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}

// A configured path renders alongside port/replicas; a host-only service keeps
// the pre-path rendering with no path token.
func TestFunctionInspectServicesPath(t *testing.T) {
	out := inspectServices(t, `  - entrypoint: v2.js
    host: api.example.com
    path: /v2
    port: 3000
  - entrypoint: plain.js
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
func TestFunctionInspectServicesAfterSchedules(t *testing.T) {
	st, _ := openTempState(t)

	tmpl, err := function.ParseTemplate([]byte(`runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - handler: jobs.cleanup.handler
    cron: "0 3 * * *"
services:
  - entrypoint: service.js
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := w.String()
	si, se := strings.Index(out, "Schedules:"), strings.Index(out, "Services:")
	if si == -1 || se == -1 || si > se {
		t.Fatalf("Services should follow Schedules:\n%s", out)
	}
}

// A template without services renders no Services: header.
func TestFunctionInspectNoServicesHeader(t *testing.T) {
	st, _ := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	if strings.Contains(w.String(), "Services:") {
		t.Fatalf("inspect must omit Services: for a template without services:\n%s", w.String())
	}
}

// inspectServices is a helper that seeds a temp state DB with a function whose
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
	tmpl, err := function.ParseTemplate([]byte(tmplBody + "services:\n" + svcs))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	st.RecordReconcileSuccess("user-events-python", "img", "fp", time.Now(),
		function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl})

	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	return w.String()
}

// Arg handling: usage errors and the unknown-function error are returned with
// their messages (cmd/main.go prints them and exits 1). `function` alone no
// longer errors — it shows help (see TestFunctionBareShowsHelp). An unknown
// token and missing/extra arguments still return errors carrying the expected
// message.
func TestFunctionCommandErrors(t *testing.T) {
	_, deps := seedTestState(t)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"function", "bogus"}, "unknown command: relay function bogus"},
		{[]string{"function", "ls", "extra"}, "function ls: too many arguments"},
		{[]string{"function", "inspect"}, "not provided"},
		{[]string{"function", "inspect", "ghost"}, `unknown function "ghost"`},
	} {
		_, _, err := runCLIWithDeps(t, deps, "", tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args %v: err = %v, want message containing %q", tc.args, err, tc.want)
		}
	}
}

// A bare `relay function` shows the subcommand help on stdout and exits 0,
// listing the function subcommands.
func TestFunctionBareShowsHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "function")
	if err != nil {
		t.Fatalf("function alone: err = %v, want nil", err)
	}
	for _, want := range []string{"ls", "inspect"} {
		if !strings.Contains(out, want) {
			t.Fatalf("function help missing %q:\n%s", want, out)
		}
	}
}

// An unknown function subcommand returns the friendly Docker-style usage error
// naming the full command path.
func TestFunctionUnknownCommandFriendly(t *testing.T) {
	_, _, err := runCLI(t, "", "function", "bogus")
	if err == nil {
		t.Fatal("function bogus: err = nil, want usage error")
	}
	for _, want := range []string{
		"relay: unknown command: relay function bogus",
		"Run 'relay function --help' for more information",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("function bogus err missing %q: %v", want, err)
		}
	}
}

// An inspect of an unknown function writes the expected message to the
// returned error (the ExitErrHandler is a silent no-op); cmd/main.go prints it
// and exits 1.
func TestFunctionInspectUnknownMessage(t *testing.T) {
	_, deps := seedTestState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "function", "inspect", "no-such-fn")
	if err == nil || !strings.Contains(err.Error(), `unknown function "no-such-fn"`) {
		t.Fatalf("returned error missing unknown-function message: %v", err)
	}
}

// `relay function --help` prints the function help to stdout and exits 0.
func TestFunctionHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "function", "--help")
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

// `relay function ls` renders the table via the command path and exits 0.
func TestFunctionLsCommand(t *testing.T) {
	_, deps := seedTestState(t)
	out, _, err := runCLIWithDeps(t, deps, "", "function", "ls")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "user-events-python") || !strings.Contains(out, "welcome-email-node") {
		t.Fatalf("ls missing rows:\n%s", out)
	}
}

// `relay function inspect NAME` renders the detail via the command path.
func TestFunctionInspectCommand(t *testing.T) {
	_, deps := seedTestState(t)
	out, _, err := runCLIWithDeps(t, deps, "", "function", "inspect", "user-events-python")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(normWS(out), "Name: user-events-python") {
		t.Fatalf("inspect missing detail:\n%s", out)
	}
}

// `relay function inspect name extra` is a usage error (exit 2).
func TestFunctionInspectTooManyArgs(t *testing.T) {
	_, deps := seedTestState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "function", "inspect", "user-events-python", "extra")
	if err == nil || !strings.Contains(err.Error(), "function inspect: too many arguments") {
		t.Fatalf("returned error missing usage error: %v", err)
	}
}

// `relay function ls --help` and `relay function inspect --help` exit 0.
func TestFunctionCommandHelp(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"function", "ls", "--help"}, "function ls"},
		{[]string{"function", "inspect", "--help"}, "relay function inspect NAME"},
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

// The Stats section renders the four per-function execution-history timestamps
// as relative ages ("2s ago"-style rows) after the DLQ entries line.
func TestFunctionInspectStatsTimestamps(t *testing.T) {
	st, _ := seedTestState(t)
	exec := time.Now().Add(-2 * time.Second).UTC()
	failure := time.Now().Add(-90 * time.Second).UTC()
	dlq := time.Now().Add(-48 * time.Hour).UTC()
	st.RecordFunctionStats(state.FunctionStats{
		Function:             "user-events-python",
		EventsProcessedTotal: 10,
		HandlerSuccessTotal:  8,
		HandlerFailureTotal:  2,
		RetryTotal:           4,
		DLQTotal:             1,
		LastExecutionAt:      exec.Format(time.RFC3339),
		LastSuccessAt:        exec.Format(time.RFC3339),
		LastFailureAt:        failure.Format(time.RFC3339),
		LastDLQAt:            dlq.Format(time.RFC3339),
	})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := normWS(w.String())
	for _, want := range []string{
		"Last execution: 2s ago",
		"Last success: 2s ago",
		"Last failure: 1m ago",
		"Last DLQ: 2d ago",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// The rows come after "DLQ entries:".
	after := strings.Index(out, "DLQ entries:")
	for _, row := range []string{"Last execution:", "Last success:", "Last failure:", "Last DLQ:"} {
		i := strings.Index(out, row)
		if i == -1 || i < after {
			t.Errorf("%s must follow DLQ entries (order):\n%s", row, out)
		}
	}
}

// A function whose timestamps were never observed renders "never" instead of
// an empty relative age.
func TestFunctionInspectStatsTimestampsNever(t *testing.T) {
	st, _ := seedTestState(t)
	st.RecordFunctionStats(state.FunctionStats{Function: "user-events-python", EventsProcessedTotal: 3})
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	var w bytes.Buffer
	printInspect(&w, st, d)
	out := normWS(w.String())
	for _, want := range []string{
		"Last execution: never",
		"Last success: never",
		"Last failure: never",
		"Last DLQ: never",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
}
