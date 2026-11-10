package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/urfave/cli/v3"

	"relay/internal/runtime"
	"relay/internal/state"
)

// Runtime CLI contracts that need no database and no resolver: argument
// handling and usage errors run against the database-free fixture, state-open
// failures propagate honestly, and the renderers' observable output/status
// rules are pinned on fixtures.

// flattened collapses all whitespace runs to single spaces, so assertions on
// tabwriter-rendered tables stay independent of column padding.
func flattened(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// runRuntime runs one `relay runtime ...` invocation against the default
// fixture and returns its captured stdout and the command error.
func runRuntime(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, _, err := runCLI(t, "", append([]string{"runtime"}, args...)...)
	return out, err
}

// requireExitCode2 pins that a usage error is the cli.Exit(msg, 2) form: the
// message is exactly what cmd/main.go would print and the exit code is the
// usage-error code, not a generic failure.
func requireExitCode2(t *testing.T, err error, wantMessage string) {
	t.Helper()
	if err == nil || err.Error() != wantMessage {
		t.Fatalf("err = %v, want %q", err, wantMessage)
	}
	var coder cli.ExitCoder
	if !errors.As(err, &coder) || coder.ExitCode() != 2 {
		t.Fatalf("err = %v (%T), want a cli.Exit code 2 usage error", err, coder)
	}
}

// TestRuntimeBareInvocationShowsSubcommandHelp pins the namespace command: a
// bare `relay runtime` shows the subcommand help (the command list IS the
// answer) and touches neither state nor a resolver.
func TestRuntimeBareInvocationShowsSubcommandHelp(t *testing.T) {
	out, err := runRuntime(t)
	if err != nil {
		t.Fatalf("bare runtime: err = %v, want the subcommand help", err)
	}
	for _, sub := range []string{"ls", "inspect", "check", "update"} {
		if !strings.Contains(out, sub) {
			t.Fatalf("bare runtime help missing %q:\n%s", sub, out)
		}
	}
}

// TestRuntimeUnknownSubcommandIsUsageError pins the Docker-style typo error
// for an unknown `relay runtime <word>` first token.
func TestRuntimeUnknownSubcommandIsUsageError(t *testing.T) {
	_, err := runRuntime(t, "bogus")
	if err == nil || !strings.Contains(err.Error(),
		"relay: unknown command: relay runtime bogus") {
		t.Fatalf("unknown subcommand err = %v, want the unknown-command usage error", err)
	}
}

// TestRuntimeLsRejectsExtraArguments pins `runtime ls` arity: the argument
// check runs before any state access, so the fixture's state tripwire would
// fail the test if a regression touched the database for a usage error.
func TestRuntimeLsRejectsExtraArguments(t *testing.T) {
	_, err := runRuntime(t, "ls", "python3.14")
	requireExitCode2(t, err, "runtime ls: too many arguments")
}

// TestRuntimeInspectArgumentArity pins `runtime inspect` arity: a missing
// RUNTIME fails the required-argument validation and extra arguments fail the
// too-many-arguments check, both before any state access.
func TestRuntimeInspectArgumentArity(t *testing.T) {
	_, err := runRuntime(t, "inspect")
	if err == nil {
		t.Fatal("inspect without a runtime was accepted")
	}
	if strings.Contains(err.Error(), "state") {
		t.Fatalf("missing-argument err = %v, want an argument validation error", err)
	}

	_, err = runRuntime(t, "inspect", "python3.14", "extra")
	if err == nil {
		t.Fatal("inspect with two runtimes was accepted")
	}
	if strings.Contains(err.Error(), "state") {
		t.Fatalf("extra-argument err = %v, want an argument validation error", err)
	}
}

// TestRuntimeCheckRejectsTwoRuntimes pins `runtime check` arity (zero or one
// runtime) before any state or resolver access.
func TestRuntimeCheckRejectsTwoRuntimes(t *testing.T) {
	_, err := runRuntime(t, "check", "python3.14", "nodejs24.x")
	if err == nil {
		t.Fatal("check with two runtimes was accepted")
	}
	if strings.Contains(err.Error(), "state") {
		t.Fatalf("extra-argument err = %v, want an argument validation error", err)
	}
}

// TestRuntimeUpdateArgumentHandling pins the update argument matrix: no
// runtime (or --all) is an explicit usage error, a runtime plus --all is one,
// two runtimes fail arity, and --help works without state.
func TestRuntimeUpdateArgumentHandling(t *testing.T) {
	_, err := runRuntime(t, "update")
	requireExitCode2(t, err, "runtime update: missing runtime (or use --all)")

	_, err = runRuntime(t, "update", "--all", "python3.14")
	if err == nil {
		t.Fatal("update --all with a runtime was accepted")
	}
	if strings.Contains(err.Error(), "state") {
		t.Fatalf("combined err = %v, want an argument validation error", err)
	}

	_, err = runRuntime(t, "update", "python3.14", "nodejs24.x")
	if err == nil {
		t.Fatal("update with two runtimes was accepted")
	}
	if strings.Contains(err.Error(), "state") {
		t.Fatalf("two-runtimes err = %v, want an argument validation error", err)
	}

	if _, _, err := runCLI(t, "", "runtime", "update", "--help"); err != nil {
		t.Fatalf("update --help: err = %v, want nil", err)
	}
}

// TestRuntimeUpdateShortAllFlagMatchesAll verifies `runtime update -a` is the
// exact short form of `--all`: it takes the every-runtime path with identical
// output and the same failure summary, and it inherits the --all argument
// rules (a runtime argument stays the same usage error).
func TestRuntimeUpdateShortAllFlagMatchesAll(t *testing.T) {
	longOut, _, longErr := runCLIWithDeps(t, testDeps(t), "", "runtime", "update", "--all")
	shortOut, _, shortErr := runCLIWithDeps(t, testDeps(t), "", "runtime", "update", "-a")

	if longErr == nil {
		t.Fatal("update --all: err = nil against the failing state fixture")
	}
	if shortErr == nil {
		t.Fatal("update -a: err = nil, want the every-runtime failure summary")
	}
	// -a reached the every-runtime path, not the missing-runtime usage error.
	if !strings.Contains(shortErr.Error(), "runtime update --all:") {
		t.Fatalf("update -a err = %v, want the --all failure summary", shortErr)
	}
	if shortErr.Error() != longErr.Error() {
		t.Fatalf("update -a err = %v, want exactly the --all error %v", shortErr, longErr)
	}
	if shortOut != longOut {
		t.Fatalf("update -a output differs from --all:\n-a:\n%s\n--all:\n%s",
			shortOut, longOut)
	}
	for _, info := range runtime.Runtimes() {
		if !strings.Contains(shortOut, info.ID) {
			t.Fatalf("update -a output missing runtime %q:\n%s", info.ID, shortOut)
		}
	}
	_, err := runRuntime(t, "update", "-a", "python3.14")
	requireExitCode2(t, err, "runtime update: cannot combine --all with a runtime")
}

// TestRuntimeUnknownRuntimeRejectedWithoutStateAccess pins that resolving an
// unknown RUNTIME id is an error in inspect, check, and update BEFORE the
// fixture's OpenState would be consulted (an accidental database open for an
// unknown runtime would fail the test with the fixture tripwire instead).
func TestRuntimeUnknownRuntimeRejectedWithoutStateAccess(t *testing.T) {
	for _, args := range [][]string{
		{"inspect", "bogus"},
		{"check", "bogus"},
		{"update", "bogus"},
	} {
		_, err := runRuntime(t, args...)
		if err == nil {
			t.Fatalf("runtime %v was accepted", args)
		}
		if !strings.Contains(err.Error(), `unknown runtime "bogus"`) {
			t.Fatalf("runtime %v err = %v, want the unknown-runtime error", args, err)
		}
	}
}

// TestRuntimeLsPropagatesStateOpenFailure pins that a state-open failure in
// `runtime ls` is returned unchanged to the caller (cmd/main.go owns printing)
// rather than rendering an empty listing.
func TestRuntimeLsPropagatesStateOpenFailure(t *testing.T) {
	deps := testDeps(t)
	_, _, err := runCLIWithDeps(t, deps, "", "runtime", "ls")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("ls with failing state opener err = %v, want the opener error", err)
	}
}

// TestRuntimeReadCommandsPropagateStateOpenFailure pins that inspect and check
// return the state-open failure instead of pretending the selection snapshot
// is empty: an honest empty reading would tell the operator everything is
// unpinned when the tooling simply could not look.
func TestRuntimeReadCommandsPropagateStateOpenFailure(t *testing.T) {
	deps := testDeps(t)
	_, _, err := runCLIWithDeps(t, deps, "", "runtime", "inspect", "python3.14")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("inspect err = %v, want the opener error", err)
	}
	_, _, err = runCLIWithDeps(t, deps, "", "runtime", "check")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("check err = %v, want the opener error", err)
	}
}

// TestRuntimeUpdatePropagatesStateOpenFailure pins that a single update fails
// with the opener error before any resolution happens; the fake resolver
// opener must never be opened in that path.
func TestRuntimeUpdatePropagatesStateOpenFailure(t *testing.T) {
	deps := testDeps(t)
	opened := false
	deps.OpenRuntimeImageResolver = func(context.Context) (RuntimeImageResolver, func(), error) {
		opened = true
		return nil, nil, nil
	}
	_, _, err := runCLIWithDeps(t, deps, "", "runtime", "update", "python3.14")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("update err = %v, want the opener error", err)
	}
	if opened {
		t.Fatal("update opened the registry resolver even though state was unavailable")
	}
}

// TestRuntimeUpdateAllFailsEveryRuntimeWhenStateUnavailable pins the --all
// failure behavior with the failing fixture opener: every runtime row is
// reported as failed with the opener error, the previous state is rendered as
// unknown ("-"), and the command exits nonzero — it never renders a partial
// success.
func TestRuntimeUpdateAllFailsEveryRuntimeWhenStateUnavailable(t *testing.T) {
	deps := testDeps(t)
	out, _, err := runCLIWithDeps(t, deps, "", "runtime", "update", "--all")
	if err == nil || !strings.Contains(err.Error(),
		"runtime update --all: all runtime(s) failed") &&
		!strings.Contains(err.Error(), "runtime(s) failed") {
		t.Fatalf("update --all err = %v, want the failed summary error", err)
	}
	for _, info := range runtime.Runtimes() {
		if !strings.Contains(out, info.ID) {
			t.Fatalf("update --all output missing runtime %q:\n%s", info.ID, out)
		}
	}
	flat := flattened(out)
	if !strings.Contains(flat, "PREVIOUS") {
		t.Fatalf("update --all does not render the previous-state column:\n%s", out)
	}
	// Every runtime's row must be an honest failure with an unknown "previous"
	// (the state could not be read, so no unpinned claim either).
	if want := len(runtime.Runtimes()); strings.Count(flat, "failed - -") != want {
		t.Fatalf("update --all rows = %d 'failed - -' entries, want %d:\n%s",
			strings.Count(flat, "failed - -"), want, out)
	}
}

// TestRuntimeCheckStatusClassification pins the operator-facing status rules
// of `runtime check` exactly: a valid stored selection compares against the
// remote digest (matching/different), a missing one is "available" for update
// when the remote resolves, a remote failure is shown as failed (pinned) or
// unpinned-honest (unpinned), and an invalid stored row is "invalid".
func TestRuntimeCheckStatusClassification(t *testing.T) {
	pinned := state.RuntimeImageSelection{
		RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: pinnedCLI,
	}
	remoteErr := errors.New("registry boom")
	selectionErr := errors.New("selection mismatch")
	cases := []struct {
		name string
		row  runtimeCheckRow
		want string
	}{
		{
			name: "digests equal", want: "matching",
			row: runtimeCheckRow{
				Runtime: infoFor(t, "python3.14"), Selection: &pinned, Pinned: true,
				RemoteDigest: pinnedCLI,
			},
		},
		{
			name: "digests differ", want: "different",
			row: runtimeCheckRow{
				Runtime: infoFor(t, "python3.14"), Selection: &pinned, Pinned: true,
				RemoteDigest: otherDigestCLI,
			},
		},
		{
			name: "no selection and remote resolved", want: "available",
			row: runtimeCheckRow{
				Runtime:      infoFor(t, "python3.14"),
				RemoteDigest: otherDigestCLI,
			},
		},
		{
			name: "no selection and remote failed", want: "unpinned",
			row: runtimeCheckRow{
				Runtime:   infoFor(t, "python3.14"),
				RemoteErr: remoteErr,
			},
		},
		{
			name: "pinned and remote failed", want: "failed",
			row: runtimeCheckRow{
				Runtime: infoFor(t, "python3.14"), Selection: &pinned, Pinned: true,
				RemoteErr: remoteErr,
			},
		},
		{
			name: "stored row invalid", want: "invalid",
			row: runtimeCheckRow{
				Runtime: infoFor(t, "python3.14"), Selection: &pinned, Pinned: true,
				SelectionErr: selectionErr,
				RemoteDigest: pinnedCLI,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := runtimeCheckStatus(tc.row); got != tc.want {
				t.Fatalf("status = %q, want %q", got, tc.want)
			}
		})
	}
}

// pinnedCLI and otherDigestCLI are shape-valid digest fixtures for renderer
// unit tests (the CLI package has no runtime-package digest helper).
var (
	pinnedCLI      = "sha256:" + strings.Repeat("a", 64)
	otherDigestCLI = "sha256:" + strings.Repeat("b", 64)
)

// infoFor resolves one RuntimeInfo fixture by ID.
func infoFor(t *testing.T, runtimeID string) runtime.RuntimeInfo {
	t.Helper()
	info, ok := runtime.LookupRuntime(runtimeID)
	if !ok {
		t.Fatalf("fixture setup: %q is not a known runtime", runtimeID)
	}
	return info
}

// TestRuntimeCheckRenderShowsDigestsAndFailureDetails pins the check table
// rendering: selected and remote digests per row, "-" for an unreadable remote,
// and combined selection/registry details.
func TestRuntimeCheckRenderShowsDigestsAndFailureDetails(t *testing.T) {
	pinned := state.RuntimeImageSelection{
		RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: pinnedCLI,
	}
	rows := []runtimeCheckRow{
		{
			Runtime: infoFor(t, "python3.14"), Selection: &pinned, Pinned: true,
			RemoteDigest: pinnedCLI, Status: "matching",
		},
		{
			Runtime: infoFor(t, "python3.14"), Selection: &pinned, Pinned: true,
			RemoteDigest: otherDigestCLI, Status: "different",
		},
		{
			Runtime:   infoFor(t, "python3.14"),
			RemoteErr: errors.New("registry unreachable"), Status: "unpinned",
		},
	}
	var out strings.Builder
	if err := renderRuntimeCheck(&out, rows); err != nil {
		t.Fatalf("render check: %v", err)
	}
	for _, want := range []string{
		"RUNTIME",
		"matching",
		"different",
		"registry: registry unreachable",
		"-",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("check render missing %q:\n%s", want, out.String())
		}
	}
	rows[2].Selection = &pinned
	rows[2].Pinned = true
	rows[2].Status = "failed"
	rows[2].SelectionErr = errors.New("selection: source image drift")
	out.Reset()
	if err := renderRuntimeCheck(&out, rows); err != nil {
		t.Fatalf("render check with details: %v", err)
	}
	if !strings.Contains(out.String(), "selection: source image drift; registry: registry unreachable") {
		t.Fatalf("check render does not join both details:\n%s", out.String())
	}
}

// TestRuntimeUpdateRenderShowsPreviousSelectedAndNote pins the single-update
// rendering: the previous identity is "-", "unpinned", or the old digest; the
// note explains whether anything changed.
func TestRuntimeUpdateRenderShowsPreviousSelectedAndNote(t *testing.T) {
	var out strings.Builder
	if err := renderRuntimeUpdate(&out, runtimeUpdateOutcome{
		RuntimeID: "python3.14", Status: runtimeUpdateUpdated,
		PreviousKnown: true, Digest: pinnedCLI, Changed: true,
	}); err != nil {
		t.Fatalf("render update: %v", err)
	}
	if got := flattened(out.String()); !strings.Contains(got, "Previous: unpinned") ||
		!strings.Contains(got, "Selected: "+pinnedCLI) ||
		!strings.Contains(got, "Status: updated") {
		t.Fatalf("updated rendering wrong:\n%s", got)
	}
	out.Reset()
	if err := renderRuntimeUpdateNote(&out, true); err != nil {
		t.Fatalf("render note: %v", err)
	}
	if !strings.Contains(out.String(), "Selection saved.") {
		t.Fatalf("changed note missing:\n%s", out.String())
	}
	out.Reset()
	if err := renderRuntimeUpdateNote(&out, false); err != nil {
		t.Fatalf("render unchanged note: %v", err)
	}
	if !strings.Contains(out.String(), "No selection change") {
		t.Fatalf("unchanged note missing:\n%s", out.String())
	}

	// Previous "-", "unpinned", and a digest are distinct operator signals.
	if label := previousSelectionLabel(runtimeUpdateOutcome{}); label != "-" {
		t.Fatalf("unknown previous label = %q, want \"-\"", label)
	}
	if label := previousSelectionLabel(runtimeUpdateOutcome{PreviousKnown: true}); label != "unpinned" {
		t.Fatalf("unpinned previous label = %q, want \"unpinned\"", label)
	}
	pinnedOutcome := runtimeUpdateOutcome{
		PreviousKnown: true, PreviousPinned: true, PreviousDigest: otherDigestCLI,
	}
	if label := previousSelectionLabel(pinnedOutcome); label != otherDigestCLI {
		t.Fatalf("pinned previous label = %q, want the old digest", label)
	}
}

// TestRuntimeUpdateAllRenderCountsAndContinues pins the --all table: per-row
// status, previous and new digests, the summary sentence, and the worker note
// only when at least one runtime changed.
func TestRuntimeUpdateAllRenderCountsAndContinues(t *testing.T) {
	outcomes := []runtimeUpdateOutcome{
		{RuntimeID: "nodejs24.x", Status: runtimeUpdateUpdated, Digest: otherDigestCLI,
			PreviousKnown: true, PreviousPinned: true, PreviousDigest: pinnedCLI, Changed: true},
		{RuntimeID: "nodejs26.x", Status: runtimeUpdateUnchanged, Digest: otherDigestCLI,
			PreviousKnown: true, PreviousPinned: true, PreviousDigest: otherDigestCLI},
		{RuntimeID: "python3.14", Status: runtimeUpdateUpdated, Digest: pinnedCLI,
			PreviousKnown: true, Changed: true},
		{RuntimeID: "python3.15", Status: runtimeUpdateFailed,
			Err: errors.New("registry unavailable")},
	}
	var out strings.Builder
	if err := renderRuntimeUpdateAll(&out, outcomes); err != nil {
		t.Fatalf("render update --all: %v", err)
	}
	got := flattened(out.String())
	for _, want := range []string{
		"nodejs24.x updated",
		"nodejs26.x unchanged",
		"python3.14 updated",
		"python3.15 failed",
		"registry unavailable",
		"Updated 2, unchanged 1, failed 1.",
		"Workers apply updated selections",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("update --all render missing %q:\n%s", want, got)
		}
	}
	if !strings.Contains(got, "nodejs26.x unchanged "+otherDigestCLI+" "+otherDigestCLI+" -") {
		t.Fatalf("unchanged row does not show previous == new digest:\n%s", got)
	}

	// With no updated row the worker note is omitted.
	out.Reset()
	if err := renderRuntimeUpdateAll(&out, outcomes[3:]); err != nil {
		t.Fatalf("render update --all failures only: %v", err)
	}
	got = flattened(out.String())
	if !strings.Contains(got, "Updated 0, unchanged 0, failed 1.") {
		t.Fatalf("failure-only summary wrong:\n%s", got)
	}
	if strings.Contains(got, "Workers apply updated selections") {
		t.Fatalf("failure-only render claims workers apply updates:\n%s", got)
	}
}

// TestRuntimeListRenderShowsUnpinnedPinnedAndInvalid pins `runtime ls`
// rendering: every configured runtime appears with its base image and state;
// pinned rows carry generation, selection time, and digest; invalid rows are
// labeled instead of silently looking pinned.
func TestRuntimeListRenderShowsUnpinnedPinnedAndInvalid(t *testing.T) {
	infos := runtime.Runtimes()
	if len(infos) < 3 {
		t.Fatalf("fixture setup: expected at least three managed runtimes, got %d", len(infos))
	}
	selections := map[string]state.RuntimeImageSelection{
		infos[1].ID: {
			RuntimeID: infos[1].ID, SourceImage: infos[1].BaseImage,
			Digest: pinnedCLI, Generation: 3, SelectedAt: "2026-11-01T10:00:00Z",
		},
		infos[2].ID: {
			RuntimeID: infos[2].ID, SourceImage: "drifted:" + infos[2].BaseImage,
			Digest: otherDigestCLI, Generation: 9, SelectedAt: "2026-11-02T10:00:00Z",
		},
	}
	var out strings.Builder
	if err := renderRuntimeList(&out, infos, selections); err != nil {
		t.Fatalf("render list: %v", err)
	}
	got := flattened(out.String())
	if !strings.Contains(got, "RUNTIME") ||
		!strings.Contains(got,
			infos[0].ID+" "+infos[0].BaseImage+" unpinned - - -") {
		t.Fatalf("list does not show the unpinned runtime honestly:\n%s", got)
	}
	if !strings.Contains(got,
		infos[1].ID+" "+infos[1].BaseImage+" pinned 3 2026-11-01T10:00:00Z "+pinnedCLI) {
		t.Fatalf("list does not show the pinned row details:\n%s", got)
	}
	if !strings.Contains(got, infos[2].ID+" "+infos[2].BaseImage+" invalid 9 ") {
		t.Fatalf("list does not flag the invalid row:\n%s", got)
	}
}

// TestRuntimeInspectRenderPinnedUnpinnedAndInvalid pins `runtime inspect`
// rendering: unpinned shows the bare tag and an honest note; pinned shows the
// digest, generation, and selected time; an invalid row shows the problem.
func TestRuntimeInspectRenderPinnedUnpinnedAndInvalid(t *testing.T) {
	info := infoFor(t, "python3.14")
	var out strings.Builder
	if err := renderRuntimeInspect(&out, info, nil, false); err != nil {
		t.Fatalf("render inspect unpinned: %v", err)
	}
	got := flattened(out.String())
	for _, want := range []string{
		"Runtime: python3.14",
		"Selection: unpinned",
		"Effective image: python:3.14-slim",
		"This runtime is unpinned",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("unpinned inspect missing %q:\n%s", want, got)
		}
	}

	pinned := state.RuntimeImageSelection{
		RuntimeID: "python3.14", SourceImage: "python:3.14-slim",
		Digest: pinnedCLI, Generation: 7, SelectedAt: "2026-11-01T10:00:00Z",
	}
	out.Reset()
	if err := renderRuntimeInspect(&out, info, &pinned, true); err != nil {
		t.Fatalf("render inspect pinned: %v", err)
	}
	got = flattened(out.String())
	for _, want := range []string{
		"Selection: pinned",
		"Digest: " + pinnedCLI,
		"Generation: 7",
		"Selected: 2026-11-01T10:00:00Z",
		"Effective image: python:3.14-slim@" + pinnedCLI,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("pinned inspect missing %q:\n%s", want, got)
		}
	}

	invalid := pinned
	invalid.SourceImage = "python:9.9-slim"
	out.Reset()
	if err := renderRuntimeInspect(&out, info, &invalid, true); err != nil {
		t.Fatalf("render inspect invalid: %v", err)
	}
	got = flattened(out.String())
	if !strings.Contains(got, "Selection: invalid") ||
		!strings.Contains(got, "does not match configured base image") {
		t.Fatalf("invalid inspect not reported:\n%s", got)
	}
}
