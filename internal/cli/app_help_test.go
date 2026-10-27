package cli

import (
	"strings"
	"testing"

	"relay/internal/app"
)

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

// normWS collapses runs of whitespace (including newlines) in s to single
// spaces, so tabwriter column padding changes from label-length edits do not
// break assertions that care about label/value content rather than alignment.
// Exact-alignment contract anchors are kept in the dedicated render tests
// (e.g. TestPrintStats).
func normWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
