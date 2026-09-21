package cli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/lnquy/cron"
	"github.com/urfave/cli/v3"
)

// namespaceTestCommand builds a minimal grouping command using the shared
// namespaceAction, so the two branches (bare invocation and unknown token) can
// be exercised directly rather than only through a concrete command family.
func namespaceTestCommand(out io.Writer) *cli.Command {
	return &cli.Command{
		Name:   "ns",
		Writer: out,
		// Mirror New's silent ExitErrHandler so cli.Exit is returned, not
		// printed-and-exited by urfave's default handler.
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
		Action:         namespaceAction(),
		Commands: []*cli.Command{
			{Name: "sub", Usage: "a subcommand"},
		},
	}
}

// TestNamespaceActionBareShowsSubcommandHelp verifies a bare grouping invocation
// shows the subcommand help and returns nil.
func TestNamespaceActionBareShowsSubcommandHelp(t *testing.T) {
	var out bytes.Buffer
	cmd := namespaceTestCommand(&out)
	if err := cmd.Run(context.Background(), []string{"ns"}); err != nil {
		t.Fatalf("bare invocation err = %v, want nil", err)
	}
	if !strings.Contains(out.String(), "sub") {
		t.Fatalf("bare invocation did not show subcommand help:\n%s", out.String())
	}
}

// TestNamespaceActionUnknownTokenUsageError verifies an unknown first token
// returns the friendly Docker-style usage error naming the full path, built from
// the whole remaining argument slice.
func TestNamespaceActionUnknownTokenUsageError(t *testing.T) {
	cmd := namespaceTestCommand(io.Discard)
	err := cmd.Run(context.Background(), []string{"ns", "foo", "bar"})
	if err == nil {
		t.Fatal("unknown token: err = nil, want usage error")
	}
	for _, want := range []string{
		"relay: unknown command: ns foo bar",
		"Usage: ns",
		"Run 'ns --help' for more information",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
}

// TestDefaultDescribeCronNilDescriptorFallsBack verifies defaultDescribeCron
// reports (false) when the lazily-built shared descriptor is nil, so inspect
// renders the raw expression and never fails. The nil descriptor stands in for
// the transient locale-loader failure path.
func TestDefaultDescribeCronNilDescriptorFallsBack(t *testing.T) {
	prev := cronDescriptor
	cronDescriptor = func() *cron.ExpressionDescriptor { return nil }
	t.Cleanup(func() { cronDescriptor = prev })

	if desc, ok := defaultDescribeCron("0 3 * * *"); ok || desc != "" {
		t.Fatalf("defaultDescribeCron(nil descriptor) = (%q, %v), want (\"\", false)", desc, ok)
	}
}

// TestDefaultDescribeCronValidAndInvalid verifies the production descriptor
// returns a 24-hour description for a valid expression and reports (false) for a
// malformed one, never returning an error.
func TestDefaultDescribeCronValidAndInvalid(t *testing.T) {
	if desc, ok := defaultDescribeCron("0 3 * * *"); !ok || desc != "At 03:00" {
		t.Fatalf("defaultDescribeCron(valid) = (%q, %v), want (\"At 03:00\", true)", desc, ok)
	}
	if desc, ok := defaultDescribeCron("not a cron"); ok || desc != "" {
		t.Fatalf("defaultDescribeCron(invalid) = (%q, %v), want (\"\", false)", desc, ok)
	}
}
