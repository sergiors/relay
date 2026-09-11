package cli

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/state"
)

// seedTestState creates a temp state DB, redirects the package CLI path
// (statePath) to it, and populates a ready function and a pending one. It
// returns the opened state DB; later command/handler calls read the same path.
func seedTestState(t *testing.T) *state.State {
	t.Helper()
	statePath = filepath.Join(t.TempDir(), "db.sqlite3")
	st, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	tmpl, _ := function.ParseTemplate([]byte("runtime: python3.14\nevents:\n  - handler: events.created.handler\n    pattern:\n      event_name: [INSERT]\n    timeout: 6s\n  - handler: events.updated.handler\n    pattern:\n      event_name: [MODIFY]\n    timeout: 20s\n"))
	readyFn := function.Function{Name: "user-events-python", Dir: filepath.Join(t.TempDir(), "x"), Template: tmpl}
	st.RecordReconcileSuccess("user-events-python", "relay-fn-user-events-python", "abc123hash", time.Now(), readyFn)

	nodeTmpl, _ := function.ParseTemplate([]byte("runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"))
	st.RecordDiscovered(function.Function{Name: "welcome-email-node", Dir: filepath.Join(t.TempDir(), "y"), Template: nodeTmpl})

	return st
}

// openForTest opens the state DB the test just seeded at statePath.
func openForTest(t *testing.T) *state.State {
	t.Helper()
	st, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// ls prints the header plus both rows with correct status/handlers, sorted.
func TestFunctionListColumns(t *testing.T) {
	seedTestState(t)
	out := capture(t, func() {
		if err := printList(openForTest(t)); err != nil {
			t.Fatalf("printList: %v", err)
		}
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	hdr := lines[0]
	for _, col := range []string{"NAME", "RUNTIME", "STATUS", "HANDLERS", "UPDATED"} {
		if !strings.Contains(hdr, col) {
			t.Fatalf("header missing %q: %q", col, hdr)
		}
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
	if !strings.Contains(userRow, "python3.14") || !strings.Contains(userRow, "ready") || !strings.Contains(userRow, "2") {
		t.Fatalf("user row wrong: %q", userRow)
	}
	if !strings.Contains(nodeRow, "pending") || !strings.Contains(nodeRow, "1") {
		t.Fatalf("node row wrong: %q", nodeRow)
	}
}

// inspect returns full detail including handlers and timeouts.
func TestFunctionInspectDetail(t *testing.T) {
	st := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	out := capture(t, func() { printInspect(st, d) })
	// tabwriter pads to the longest label ("Last reconcile:") + minwidth
	for _, want := range []string{
		"Name:            user-events-python",
		"Runtime:         python3.14",
		"Status:          ready",
		"Image:           relay-fn-user-events-python",
		"Fingerprint:     abc123hash",
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
	st := seedTestState(t)
	d, ok := st.GetFunction("welcome-email-node")
	if !ok {
		t.Fatal("expected pending function")
	}
	out := capture(t, func() { printInspect(st, d) })
	if strings.Contains(out, "Image:") || strings.Contains(out, "Prepared:") || strings.Contains(out, "Fingerprint:") {
		t.Fatalf("pending function should omit Image/Fingerprint/Prepared:\n%s", out)
	}
}

// printInspect appends a per-function stats section between the metadata block
// and the Handlers section, using the same wider padding as Handlers for visual
// grouping. The long label "Handler successes:" drives the value column.
func TestFunctionInspectStatsSection(t *testing.T) {
	st := seedTestState(t)
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
	out := capture(t, func() { printInspect(st, d) })
	for _, want := range []string{
		"Stats:",
		"  Events processed:    12493",
		"  Handler successes:   12470",
		"  Handler failures:    23",
		"  Retries:             17",
		"  DLQ entries:         2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	// The Stats section sits between the metadata and Handlers, so Handlers
	// still renders after it.
	si, hi := strings.Index(out, "Stats:"), strings.Index(out, "Handlers:")
	if si == -1 || hi == -1 || si > hi {
		t.Fatalf("Stats section should precede Handlers:\n%s", out)
	}
}

// A function with no recorded stats still renders a predictable zero Stats
// section rather than omitting it.
func TestFunctionInspectStatsZeroWithoutRow(t *testing.T) {
	st := seedTestState(t)
	d, ok := st.GetFunction("user-events-python")
	if !ok {
		t.Fatal("expected function")
	}
	out := capture(t, func() { printInspect(st, d) })
	for _, want := range []string{
		"Stats:",
		"  Events processed:    0",
		"  Handler successes:   0",
		"  Handler failures:    0",
		"  Retries:             0",
		"  DLQ entries:         0",
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
	statePath = filepath.Join(t.TempDir(), "db.sqlite3")
	st, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

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
	out := capture(t, func() { printInspect(st, d) })
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

// Arg handling: bad args -> exit 2; unknown function -> exit 1 + message.
func TestFunctionCommandExitCodes(t *testing.T) {
	_ = seedTestState(t)

	if code := runFunctionCommand(nil); code != 2 {
		t.Fatalf("no args: exit = %d, want 2", code)
	}
	if code := runFunctionCommand([]string{"bogus"}); code != 2 {
		t.Fatalf("unknown subcommand: exit = %d, want 2", code)
	}
	if code := runFunctionCommand([]string{"ls", "extra"}); code != 2 {
		t.Fatalf("ls extra arg: exit = %d, want 2", code)
	}
	if code := runFunctionCommand([]string{"inspect"}); code != 2 {
		t.Fatalf("inspect no name: exit = %d, want 2", code)
	}

	if code := runFunctionCommand([]string{"inspect", "ghost"}); code != 1 {
		t.Fatalf("unknown function: exit = %d, want 1", code)
	}
}

// An inspect of an unknown function writes the expected message to stderr.
func TestFunctionInspectUnknownMessage(t *testing.T) {
	_ = seedTestState(t)
	errOut := captureErr(t, func() {
		_ = runFunctionCommand([]string{"inspect", "no-such-fn"})
	})
	if !strings.Contains(errOut, `Error: unknown function "no-such-fn"`) {
		t.Fatalf("stderr missing unknown-function message: %q", errOut)
	}
}

// `relay function --help` prints the function help to stdout and exits 0.
func TestFunctionHelp(t *testing.T) {
	var code int
	out := capture(t, func() {
		code = runFunctionCommand([]string{"--help"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	for _, want := range []string{
		"relay function COMMAND",
		"ls",
		"inspect",
		"Run 'relay function COMMAND --help' for more information on a command.",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

// `relay function ls --help` and `relay function inspect --help` print their
// focused usage to stdout and exit 0.
func TestFunctionCommandHelp(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"ls", "--help"}, "relay function ls"},
		{[]string{"inspect", "--help"}, "relay function inspect NAME"},
	}
	for _, c := range cases {
		var code int
		out := capture(t, func() {
			code = runFunctionCommand(c.args)
		})
		if code != 0 {
			t.Fatalf("%v: exit = %d, want 0", c.args, code)
		}
		if !strings.Contains(out, c.want) {
			t.Fatalf("%v: stdout missing %q:\n%s", c.args, c.want, out)
		}
	}
}

// Misplaced --help in a function subcommand is a usage error: exit 2 with
// error+usage on stderr.
func TestFunctionHelpMisplaced(t *testing.T) {
	cases := [][]string{
		{"ls", "--help", "extra"},
		{"inspect", "--help", "name"},
		{"--help", "bogus"},
	}
	for _, args := range cases {
		var code int
		errOut := captureErr(t, func() {
			code = runFunctionCommand(args)
		})
		if code != 2 {
			t.Fatalf("%v: exit = %d, want 2", args, code)
		}
		if !strings.Contains(errOut, "Error:") {
			t.Fatalf("%v: stderr missing error line: %q", args, errOut)
		}
	}
}

// `relay health --help` prints the health usage to stdout and exits 0.
func TestHealthHelp(t *testing.T) {
	var code int
	out := capture(t, func() {
		code = Run([]string{"health", "--help"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(out, "relay health") {
		t.Fatalf("stdout missing health usage:\n%s", out)
	}
}

// `relay health` takes no positional args; any arg is a usage error.
func TestHealthArgError(t *testing.T) {
	var code int
	errOut := captureErr(t, func() {
		code = Run([]string{"health", "extra"})
	})
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(errOut, "relay health") {
		t.Fatalf("stderr missing health usage: %q", errOut)
	}
}
