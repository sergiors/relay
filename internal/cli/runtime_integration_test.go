//go:build integration

package cli

// `relay runtime` command behavior against the real PostgreSQL selection
// store, with the Docker Engine resolver replaced by a fake via the injected
// OpenRuntimeImageResolver seam: check must stay read-only, update must CAS, and
// --all must continue past partial failures.
//
// The file also holds the end-to-end composition test below: the REAL CLI
// command, the REAL Docker Engine resolver, a REAL runtime Manager and
// Reconciler wired to the SAME isolated PostgreSQL schema, proving a persisted
// selection update converges live apps through the existing periodic
// reconciliation path.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// indexMediaTypeCLI is the OCI index media type the fake resolver reports; it
// must equal the value runtime.ValidateBaseImageResolution accepts.
const indexMediaTypeCLI = "application/vnd.oci.image.index.v1+json"

// thirdDigestCLI is a third shape-valid digest fixture, distinct from the
// shared pinnedCLI/otherDigestCLI render fixtures.
var thirdDigestCLI = "sha256:" + strings.Repeat("c", 64)

// fakeRuntimeResolver is the scripted RuntimeImageResolver: every reference
// maps to a preconfigured resolution or error; onResolve runs before each
// scripted answer so a test can race the persisted row between the CLI's
// snapshot read and its compare-and-swap (the production no-op confirm and CAS
// conflict paths).
type fakeRuntimeResolver struct {
	resolutions map[string]runtime.BaseImageResolution
	errs        map[string]error
	onResolve   func(ref string)
	calls       []string
}

func (f *fakeRuntimeResolver) ResolveBaseImage(
	_ context.Context, ref string,
) (runtime.BaseImageResolution, error) {
	f.calls = append(f.calls, ref)
	if f.onResolve != nil {
		f.onResolve(ref)
	}
	if err, ok := f.errs[ref]; ok {
		return runtime.BaseImageResolution{}, err
	}
	if resolution, ok := f.resolutions[ref]; ok {
		return resolution, nil
	}
	return runtime.BaseImageResolution{}, fmt.Errorf("no resolution scripted for %q", ref)
}

// resolverFixture is the injected opener: it counts opens/closes (check and
// update must release the resource they open) and can be scripted to fail.
type resolverFixture struct {
	resolver *fakeRuntimeResolver
	openErr  error
	opens    int
	closes   int
}

func (f *resolverFixture) open(context.Context) (RuntimeImageResolver, func(), error) {
	f.opens++
	if f.openErr != nil {
		return nil, nil, f.openErr
	}
	return f.resolver, func() { f.closes++ }, nil
}

// runtimeDeps is stateTestDeps plus the injected fake resolver opener (a nil
// fixture injects no opener at all, pinning the configuration-error path).
func runtimeDeps(t *testing.T, fixture *resolverFixture) Dependencies {
	t.Helper()
	deps := stateTestDeps(t)
	if fixture != nil {
		deps.OpenRuntimeImageResolver = fixture.open
	}
	return deps
}

// pinnedResolution builds a structurally valid multi-platform resolution for
// ref, exactly the shape the production resolver validates and returns.
func pinnedResolution(ref, digest string) runtime.BaseImageResolution {
	return runtime.BaseImageResolution{
		Ref: ref, Digest: digest,
		MediaType: indexMediaTypeCLI,
		Platforms: []ocispec.Platform{
			{OS: "linux", Architecture: "amd64"},
			{OS: "linux", Architecture: "arm64", Variant: "v8"},
		},
	}
}

// runRuntimeCLI runs one `relay runtime ...` invocation on the given deps.
func runRuntimeCLI(t *testing.T, deps Dependencies, args ...string) (string, error) {
	t.Helper()
	out, _, err := runCLIWithDeps(t, deps, "", append([]string{"runtime"}, args...)...)
	return out, err
}

// openFixtureState opens an isolated state handle for direct seeding/reading.
func openFixtureState(t *testing.T, deps Dependencies) *state.State {
	t.Helper()
	st, cleanup, err := deps.OpenState(context.Background())
	if err != nil {
		t.Fatalf("open fixture state: %v", err)
	}
	t.Cleanup(cleanup)
	return st
}

// seedSelection writes one selection row directly through the state CAS.
func seedSelection(
	t *testing.T, ctx context.Context, st *state.State, runtimeID, digest string,
) {
	t.Helper()
	info, ok := runtime.LookupRuntime(runtimeID)
	if !ok {
		t.Fatalf("seed setup: unknown runtime %q", runtimeID)
	}
	if _, err := st.CompareAndSwapRuntimeImageSelection(ctx, state.RuntimeImageSelection{
		RuntimeID: runtimeID, SourceImage: info.BaseImage, Digest: digest,
	}, 0); err != nil {
		t.Fatalf("seed selection for %s: %v", runtimeID, err)
	}
}

// readSelections returns the current persisted selections as a map.
func readSelections(
	t *testing.T, ctx context.Context, st *state.State,
) map[string]state.RuntimeImageSelection {
	t.Helper()
	rows, err := st.ReadRuntimeImageSelections(ctx)
	if err != nil {
		t.Fatalf("read selections: %v", err)
	}
	out := make(map[string]state.RuntimeImageSelection, len(rows))
	for _, row := range rows {
		out[row.RuntimeID] = row
	}
	return out
}

// TestRuntimeListReadsSelectionsWithoutResolving pins that `runtime ls`
// renders persisted selections read-only from PostgreSQL: pinned and unpinned
// rows are shown, and the resolver opener is never invoked (ls needs no Docker
// connection).
func TestRuntimeListReadsSelectionsWithoutResolving(t *testing.T) {
	ctx := context.Background()
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{}}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "nodejs24.x", pinnedCLI)
	selectedAt := readSelections(t, ctx, st)["nodejs24.x"].SelectedAt

	out, err := runRuntimeCLI(t, deps, "ls")
	if err != nil {
		t.Fatalf("runtime ls: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat,
		"nodejs24.x node:24-alpine pinned 1 "+selectedAt+" "+pinnedCLI) {
		t.Fatalf("ls does not show the pinned node row:\n%s", out)
	}
	if !strings.Contains(flat, "python3.14 python:3.14-slim unpinned - - -") {
		t.Fatalf("ls does not show python unpinned:\n%s", out)
	}
	if fixture.opens != 0 {
		t.Fatalf("ls opened the resolver %d time(s); ls is a local read-only command", fixture.opens)
	}
}

// TestRuntimeInspectShowsSelectionAndUnpinnedHonesty pins `runtime inspect`:
// the pinned runtime shows digest/generation/selection and the effective
// ref@digest image; an unseeded runtime is shown as honestly unpinned with the
// configured tag; the resolver opener is never invoked.
func TestRuntimeInspectShowsSelectionAndUnpinnedHonesty(t *testing.T) {
	ctx := context.Background()
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{}}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "nodejs24.x", pinnedCLI)
	rows := readSelections(t, ctx, st)
	pinnedAt := rows["nodejs24.x"].SelectedAt

	out, err := runRuntimeCLI(t, deps, "inspect", "nodejs24.x")
	if err != nil {
		t.Fatalf("inspect nodejs24.x: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "Selection: pinned") ||
		!strings.Contains(flat, "Digest: "+pinnedCLI) ||
		!strings.Contains(flat, "Generation: 1") ||
		!strings.Contains(flat, "Effective image: node:24-alpine@"+pinnedCLI) {
		t.Fatalf("pinned inspect rendering wrong:\n%s", out)
	}
	if !strings.Contains(flat, "Selected: "+pinnedAt) {
		t.Fatalf("inspect does not show the persisted selection time:\n%s", out)
	}

	out, err = runRuntimeCLI(t, deps, "inspect", "python3.14")
	if err != nil {
		t.Fatalf("inspect python3.14: err = %v", err)
	}
	flat = flattened(out)
	if !strings.Contains(flat, "Selection: unpinned") ||
		!strings.Contains(flat, "Effective image: python:3.14-slim") ||
		!strings.Contains(flat, "This runtime is unpinned") {
		t.Fatalf("unpinned inspect rendering wrong:\n%s", out)
	}
	if fixture.opens != 0 {
		t.Fatalf("inspect opened the resolver %d time(s); inspect is local-only", fixture.opens)
	}
}

// TestRuntimeCheckComparesWithoutWriting pins the read-only check contract end
// to end: matching, different, and unpinned-but-resolvable runtimes are each
// labeled honestly, every runtime is resolved exactly once, and NO persisted
// row changes (same rows, same generations).
func TestRuntimeCheckComparesWithoutWriting(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"node:24-alpine":   pinnedResolution("node:24-alpine", pinnedCLI),
		"node:26-alpine":   pinnedResolution("node:26-alpine", otherDigestCLI),
		"python:3.14-slim": pinnedResolution("python:3.14-slim", otherDigestCLI),
		"python:3.15-slim": pinnedResolution("python:3.15-slim", thirdDigestCLI),
	}
	fixture := &resolverFixture{
		resolver: &fakeRuntimeResolver{resolutions: resolutions},
	}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "nodejs24.x", pinnedCLI) // == remote: matching
	seedSelection(t, ctx, st, "python3.14", pinnedCLI) // != remote: different
	// python3.15 and nodejs26.x stay unpinned → "available".

	out, err := runRuntimeCLI(t, deps, "check")
	if err != nil {
		t.Fatalf("runtime check: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "nodejs24.x matching "+pinnedCLI+" "+pinnedCLI+" -") {
		t.Fatalf("check does not report the matching selection:\n%s", out)
	}
	if !strings.Contains(flat,
		"python3.14 different "+pinnedCLI+" "+otherDigestCLI+" -") {
		t.Fatalf("check does not report the different digest:\n%s", out)
	}
	if !strings.Contains(flat, "python3.15 available unpinned "+thirdDigestCLI+" -") {
		t.Fatalf("check does not report update availability honestly:\n%s", out)
	}

	// Exactly one read-only resolution per configured runtime.
	if want := len(runtime.Runtimes()); len(fixture.resolver.calls) != want {
		t.Fatalf("resolver calls = %d, want one per runtime (%d): %v",
			len(fixture.resolver.calls), want, fixture.resolver.calls)
	}
	if fixture.closes != 1 {
		t.Fatalf("resolver closes = %d, want the opened handle released once", fixture.closes)
	}

	// Nothing was written: rows and generations unchanged.
	rows := readSelections(t, ctx, st)
	if len(rows) != 2 || rows["nodejs24.x"].Digest != pinnedCLI ||
		rows["nodejs24.x"].Generation != 1 || rows["python3.14"].Generation != 1 {
		t.Fatalf("check wrote state: rows = %+v", rows)
	}
}

// TestRuntimeCheckFailsOnRegistryLookups pins the registry-failure contract: a
// pinned runtime reports "failed", an unpinned one stays honestly "unpinned"
// with the registry detail, and the command exits nonzero naming the failures.
func TestRuntimeCheckFailsOnRegistryLookups(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("registry unreachable")
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{
		errs: map[string]error{"python:3.14-slim": boom},
	}}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "python3.14", pinnedCLI)

	out, err := runRuntimeCLI(t, deps, "check", "python3.14")
	if err == nil || !strings.Contains(err.Error(), "runtime check: 1 runtime(s) failed") {
		t.Fatalf("pinned check err = %v, want the failed summary", err)
	}
	if !strings.Contains(
		flattened(out),
		"python3.14 failed "+pinnedCLI+" - registry: registry unreachable",
	) {
		t.Fatalf("pinned failure row wrong:\n%s", out)
	}

	// An unpinned runtime is never turned into a pin candidate on failure.
	out, err = runRuntimeCLI(t, deps, "check", "nodejs26.x")
	if err == nil || !strings.Contains(err.Error(), "1 runtime(s) failed") {
		t.Fatalf("unpinned failing check err = %v, want the failed summary", err)
	}
	if !strings.Contains(
		flattened(out),
		"nodejs26.x unpinned unpinned - registry: no resolution scripted for",
	) {
		t.Fatalf("unpinned failure row wrong:\n%s", out)
	}
}

// TestRuntimeCheckReportsInvalidStoredSelection pins that a persisted row the
// worker would reject (a source-image drift) is surfaced as "invalid" with the
// reason, and the command exits nonzero.
func TestRuntimeCheckReportsInvalidStoredSelection(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"python:3.14-slim": pinnedResolution("python:3.14-slim", pinnedCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{resolutions: resolutions}}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	if _, err := st.CompareAndSwapRuntimeImageSelection(ctx, state.RuntimeImageSelection{
		RuntimeID: "python3.14", SourceImage: "python:9.9-slim", Digest: pinnedCLI,
	}, 0); err != nil {
		t.Fatalf("seed drifted selection: %v", err)
	}

	out, err := runRuntimeCLI(t, deps, "check", "python3.14")
	if err == nil || !strings.Contains(err.Error(), "runtime check: 1 runtime(s) failed") {
		t.Fatalf("invalid-selection check err = %v, want the failed summary", err)
	}
	if !strings.Contains(out, "invalid") ||
		!strings.Contains(out, "does not match configured base image") {
		t.Fatalf("invalid selection not reported as such:\n%s", out)
	}
}

// TestRuntimeCheckRendersResolverOpenFailureForEveryRuntime pins that a
// resolver-open failure renders per runtime (all-mode still reports every
// configured runtime) and exits nonzero; no runtime is silently skipped.
func TestRuntimeCheckRendersResolverOpenFailureForEveryRuntime(t *testing.T) {
	fixture := &resolverFixture{openErr: errors.New("no docker socket")}
	deps := runtimeDeps(t, fixture)

	out, err := runRuntimeCLI(t, deps, "check")
	if err == nil || !strings.Contains(err.Error(),
		fmt.Sprintf("runtime check: %d runtime(s) failed", len(runtime.Runtimes()))) {
		t.Fatalf("check err = %v, want the all-failed summary", err)
	}
	flat := flattened(out)
	for _, info := range runtime.Runtimes() {
		if !strings.Contains(flat,
			info.ID+" unpinned unpinned - registry: no docker socket") {
			t.Fatalf("check did not report the open failure for %s:\n%s", info.ID, out)
		}
	}
	if fixture.opens != 1 {
		t.Fatalf("resolver opens = %d, want exactly one failed attempt", fixture.opens)
	}
}

// TestRuntimeUpdatePinsFromUnpinned pins the happy-path update: the resolved
// multi-platform index digest is persisted as generation 1 under the exact
// configured source image, the output shows updated/unpinned/selected and the
// worker note, and the resolver handle is released.
func TestRuntimeUpdatePinsFromUnpinned(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"python:3.14-slim": pinnedResolution("python:3.14-slim", otherDigestCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{resolutions: resolutions}}
	deps := runtimeDeps(t, fixture)

	out, err := runRuntimeCLI(t, deps, "update", "python3.14")
	if err != nil {
		t.Fatalf("update python3.14: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "Status: updated") ||
		!strings.Contains(flat, "Previous: unpinned") ||
		!strings.Contains(flat, "Selected: "+otherDigestCLI) ||
		!strings.Contains(flat, "Selection saved.") {
		t.Fatalf("updated output wrong:\n%s", out)
	}
	if fixture.closes != 1 {
		t.Fatalf("resolver closes = %d, want the handle released once", fixture.closes)
	}

	rows := readSelections(t, ctx, openFixtureState(t, deps))
	selection, ok := rows["python3.14"]
	if !ok || selection.SourceImage != "python:3.14-slim" ||
		selection.Digest != otherDigestCLI || selection.Generation != 1 ||
		selection.SelectedAt == "" {
		t.Fatalf("persisted selection = %+v, want the resolved digest as generation 1", selection)
	}
}

// TestRuntimeUpdateSameDigestIsNoOp pins the no-op contract: resolving the
// already-selected digest rewrites NOTHING (generation and selection time stay
// untouched) and the output says unchanged with the no-change note.
func TestRuntimeUpdateSameDigestIsNoOp(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"python:3.14-slim": pinnedResolution("python:3.14-slim", otherDigestCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{resolutions: resolutions}}
	deps := runtimeDeps(t, fixture)

	if _, err := runRuntimeCLI(t, deps, "update", "python3.14"); err != nil {
		t.Fatalf("first update: %v", err)
	}
	before := readSelections(t, ctx, openFixtureState(t, deps))["python3.14"]

	out, err := runRuntimeCLI(t, deps, "update", "python3.14")
	if err != nil {
		t.Fatalf("second update: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "Status: unchanged") ||
		!strings.Contains(flat, "No selection change") {
		t.Fatalf("no-op output wrong:\n%s", out)
	}

	after := readSelections(t, ctx, openFixtureState(t, deps))["python3.14"]
	if after != before {
		t.Fatalf("no-op rewrote the row: before %+v, after %+v", before, after)
	}
}

// TestRuntimeUpdateNewDigestAdvancesGeneration pins the re-pin contract: a new
// registry digest advances the generation by one under the same source image
// and the output shows the previous digest explicitly.
func TestRuntimeUpdateNewDigestAdvancesGeneration(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"python:3.14-slim": pinnedResolution("python:3.14-slim", otherDigestCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{resolutions: resolutions}}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "python3.14", pinnedCLI)

	out, err := runRuntimeCLI(t, deps, "update", "python3.14")
	if err != nil {
		t.Fatalf("re-pin update: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "Status: updated") ||
		!strings.Contains(flat, "Previous: "+pinnedCLI) ||
		!strings.Contains(flat, "Selected: "+otherDigestCLI) {
		t.Fatalf("re-pin output wrong:\n%s", out)
	}

	after := readSelections(t, ctx, st)["python3.14"]
	if after.Digest != otherDigestCLI || after.Generation != 2 ||
		after.SourceImage != "python:3.14-slim" {
		t.Fatalf("re-pin row = %+v, want the new digest as generation 2", after)
	}
}

// TestRuntimeUpdateDetectsConcurrentChangeOnConfirm pins the no-op-confirmation
// fence: when another writer advanced the row between the CLI's snapshot read
// and its under-lock confirmation, the update FAILS (never overwrites) and
// tells the operator to rerun against the current pin.
func TestRuntimeUpdateDetectsConcurrentChangeOnConfirm(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"python:3.14-slim": pinnedResolution("python:3.14-slim", pinnedCLI),
	}
	deps := runtimeDeps(t, nil)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "python3.14", pinnedCLI)

	// A concurrent writer re-pins the runtime while this update is resolving:
	// the snapshot digest is about to be stale. The hook runs on the same
	// goroutine, before the update takes its under-lock confirmation.
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{
		resolutions: resolutions,
		onResolve: func(ref string) {
			if ref != "python:3.14-slim" {
				return
			}
			if _, err := st.CompareAndSwapRuntimeImageSelection(
				ctx, state.RuntimeImageSelection{
					RuntimeID:   "python3.14",
					SourceImage: "python:3.14-slim",
					Digest:      thirdDigestCLI,
				}, 1); err != nil {
				t.Errorf("hook racing write: %v", err)
			}
		},
	}}
	deps.OpenRuntimeImageResolver = fixture.open

	out, err := runRuntimeCLI(t, deps, "update", "python3.14")
	if err == nil {
		t.Fatal("concurrent change was accepted as a no-op")
	}
	if !strings.Contains(err.Error(), "selection changed concurrently to") {
		t.Fatalf("confirm-path err = %v, want the concurrent-change error", err)
	}
	// A failed single update still renders the known previous identity, the
	// attempted remote digest, and the failure detail, but never labels the
	// attempted digest as a persisted selection.
	if !strings.Contains(err.Error(),
		"runtime update python3.14: selection changed concurrently to "+thirdDigestCLI) {
		t.Fatalf("confirm-path err = %v, want the concurrent-change report naming the current pin", err)
	}
	flat := flattened(out)
	for _, want := range []string{
		"Status: failed",
		"Previous: " + pinnedCLI,
		"Attempted: " + pinnedCLI,
		"Error: selection changed concurrently to " + thirdDigestCLI,
	} {
		if !strings.Contains(flat, want) {
			t.Fatalf("failed update render missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(flat, "Selected:") {
		t.Fatalf("failed update claimed a persisted selection:\n%s", out)
	}

	// The concurrent writer's row is intact: the update did not overwrite it.
	after := readSelections(t, ctx, st)["python3.14"]
	if after.Digest != thirdDigestCLI || after.Generation != 2 {
		t.Fatalf("update overwrote the concurrent row: %+v", after)
	}
}

// TestRuntimeUpdateDetectsStaleGenerationConflict pins the compare-and-swap
// fence for real re-pins: resolving a NEW digest with a stale generation loses
// the race explicitly and never overwrites the concurrent writer's row.
func TestRuntimeUpdateDetectsStaleGenerationConflict(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"python:3.14-slim": pinnedResolution("python:3.14-slim", otherDigestCLI),
	}
	deps := runtimeDeps(t, nil)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "python3.14", pinnedCLI)

	// The racing writer advances the row before this update's CAS.
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{
		resolutions: resolutions,
		onResolve: func(ref string) {
			if ref != "python:3.14-slim" {
				return
			}
			if _, err := st.CompareAndSwapRuntimeImageSelection(
				ctx, state.RuntimeImageSelection{
					RuntimeID:   "python3.14",
					SourceImage: "python:3.14-slim",
					Digest:      thirdDigestCLI,
				}, 1); err != nil {
				t.Errorf("hook racing write: %v", err)
			}
		},
	}}
	deps.OpenRuntimeImageResolver = fixture.open

	_, err := runRuntimeCLI(t, deps, "update", "python3.14")
	if err == nil {
		t.Fatal("stale-generation write succeeded")
	}
	if !strings.Contains(err.Error(),
		"selection changed concurrently (expected generation 1)") {
		t.Fatalf("CAS conflict err = %v, want the stale-generation error", err)
	}

	after := readSelections(t, ctx, st)["python3.14"]
	if after.Digest != thirdDigestCLI || after.Generation != 2 {
		t.Fatalf("update overwrote the concurrent row: %+v", after)
	}
}

// TestRuntimeUpdateFailsWithoutWritingWhenOpenerFails pins that update fails
// clearly when the Docker resolver cannot be opened and persists NOTHING.
func TestRuntimeUpdateFailsWithoutWritingWhenOpenerFails(t *testing.T) {
	fixture := &resolverFixture{openErr: errors.New("no docker socket")}
	deps := runtimeDeps(t, fixture)

	_, err := runRuntimeCLI(t, deps, "update", "python3.14")
	if err == nil || !strings.Contains(err.Error(), "no docker socket") {
		t.Fatalf("opener failure err = %v, want the wrapped open error", err)
	}
	rows := readSelections(t, context.Background(), openFixtureState(t, deps))
	if _, ok := rows["python3.14"]; ok {
		t.Fatalf("failed update wrote a row: %+v", rows["python3.14"])
	}
}

// TestRuntimeUpdateAllPinsEveryRuntimeIndependently pins --all on an empty
// store: every configured runtime is resolved and persisted independently
// (generation 1 each), per-runtime rows and the summary render honestly, and
// the command exits 0.
func TestRuntimeUpdateAllPinsEveryRuntimeIndependently(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"node:24-alpine":   pinnedResolution("node:24-alpine", pinnedCLI),
		"node:26-alpine":   pinnedResolution("node:26-alpine", otherDigestCLI),
		"python:3.14-slim": pinnedResolution("python:3.14-slim", thirdDigestCLI),
		"python:3.15-slim": pinnedResolution("python:3.15-slim", pinnedCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{resolutions: resolutions}}
	deps := runtimeDeps(t, fixture)

	out, err := runRuntimeCLI(t, deps, "update", "--all")
	if err != nil {
		t.Fatalf("update --all: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "Updated 4, unchanged 0, failed 0.") ||
		!strings.Contains(flat, "Workers apply updated selections") {
		t.Fatalf("--all summary wrong:\n%s", out)
	}
	for _, info := range runtime.Runtimes() {
		if !strings.Contains(flat, info.ID+" updated unpinned") {
			t.Fatalf("--all row for %s not updated:\n%s", info.ID, out)
		}
	}

	rows := readSelections(t, ctx, openFixtureState(t, deps))
	if want := len(runtime.Runtimes()); len(rows) != want {
		t.Fatalf("persisted rows = %d, want %d", len(rows), want)
	}
	for runtimeID, row := range rows {
		if row.Generation != 1 || row.SourceImage == "" || row.Digest == "" {
			t.Fatalf("row %s = %+v, want the resolved pin as generation 1", runtimeID, row)
		}
	}
}

// TestRuntimeUpdateAllContinuesAfterPartialFailures pins the --all resilience
// contract: one runtime's resolution failure marks exactly that runtime failed
// with its detail, the OTHER runtimes still update, and the command exits
// nonzero with the failure count.
func TestRuntimeUpdateAllContinuesAfterPartialFailures(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"node:24-alpine":   pinnedResolution("node:24-alpine", pinnedCLI),
		"node:26-alpine":   pinnedResolution("node:26-alpine", otherDigestCLI),
		"python:3.14-slim": pinnedResolution("python:3.14-slim", thirdDigestCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{
		resolutions: resolutions,
		errs:        map[string]error{"python:3.15-slim": errors.New("registry down")},
	}}
	deps := runtimeDeps(t, fixture)

	out, err := runRuntimeCLI(t, deps, "update", "--all")
	if err == nil || !strings.Contains(err.Error(),
		"runtime update --all: 1 runtime(s) failed") {
		t.Fatalf("partial failure err = %v, want the failed summary", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "python3.15 failed unpinned - registry down") ||
		!strings.Contains(flat, "Updated 3, unchanged 0, failed 1.") {
		t.Fatalf("partial-failure rendering wrong:\n%s", out)
	}

	rows := readSelections(t, ctx, openFixtureState(t, deps))
	for _, runtimeID := range []string{"nodejs24.x", "nodejs26.x", "python3.14"} {
		if row, ok := rows[runtimeID]; !ok || row.Generation != 1 {
			t.Fatalf("runtime %s did not update despite a sibling failure: %+v", runtimeID, row)
		}
	}
	if row, ok := rows["python3.15"]; ok {
		t.Fatalf("failed runtime persisted a row: %+v", row)
	}
}

// TestRuntimeUpdateAllShowsUnchangedAndPrevious pins --all against a pre-pinned
// store: a runtime already at the registry digest is unchanged (generation
// untouched, previous shown), unpinned runtimes show their previous state, and
// the summary counts mix correctly.
func TestRuntimeUpdateAllShowsUnchangedAndPrevious(t *testing.T) {
	ctx := context.Background()
	resolutions := map[string]runtime.BaseImageResolution{
		"node:24-alpine":   pinnedResolution("node:24-alpine", pinnedCLI),
		"node:26-alpine":   pinnedResolution("node:26-alpine", otherDigestCLI),
		"python:3.14-slim": pinnedResolution("python:3.14-slim", thirdDigestCLI),
		"python:3.15-slim": pinnedResolution("python:3.15-slim", pinnedCLI),
	}
	fixture := &resolverFixture{resolver: &fakeRuntimeResolver{resolutions: resolutions}}
	deps := runtimeDeps(t, fixture)
	st := openFixtureState(t, deps)
	seedSelection(t, ctx, st, "nodejs24.x", pinnedCLI) // unchanged after --all

	out, err := runRuntimeCLI(t, deps, "update", "--all")
	if err != nil {
		t.Fatalf("update --all: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat,
		"nodejs24.x unchanged "+pinnedCLI+" "+pinnedCLI+" -") {
		t.Fatalf("unchanged row does not show its previous digest:\n%s", out)
	}
	if !strings.Contains(flat, "Updated 3, unchanged 1, failed 0.") ||
		!strings.Contains(flat, "Workers apply updated selections") {
		t.Fatalf("--all summary wrong:\n%s", out)
	}

	after := readSelections(t, ctx, st)["nodejs24.x"]
	if after.Generation != 1 {
		t.Fatalf("unchanged row generation moved: %+v", after)
	}
}

// The end-to-end composition below mirrors the worker runtime wiring with the
// documented small adapters packages outside runtime/worker are allowed to
// carry (see internal/runtime/cross_process_dep_gc_integration_test.go): the
// SAME isolated PostgreSQL schema backs the CLI opener, the Manager's shared
// selection store (the cross-process advisory fence), and the reconciler's
// state view.

// e2eSelectionStore adapts the shared state handle to the runtime Manager's
// selection seam (the worker's stateRuntimeImageSelectionStore adapter minus
// the conversions, which the state and runtime read models share structurally;
// every snapshot read and the per-runtime advisory lock run through the REAL
// state methods).
type e2eSelectionStore struct {
	st *state.State
}

func (s e2eSelectionStore) ReadRuntimeImageSelections(
	ctx context.Context,
) ([]runtime.RuntimeImageSelection, error) {
	rows, err := s.st.ReadRuntimeImageSelections(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]runtime.RuntimeImageSelection, 0, len(rows))
	for _, row := range rows {
		out = append(out, runtime.RuntimeImageSelection{
			RuntimeID: row.RuntimeID, SourceImage: row.SourceImage,
			Digest: row.Digest, Generation: row.Generation,
			SelectedAt: row.SelectedAt,
		})
	}
	return out, nil
}

func (s e2eSelectionStore) WithRuntimeImageSelectionLock(
	ctx context.Context,
	runtimeID string,
	fn func(context.Context, *runtime.RuntimeImageSelection) error,
) error {
	return s.st.WithRuntimeImageSelectionLock(ctx, runtimeID,
		func(ctx context.Context, current *state.RuntimeImageSelection) error {
			var converted *runtime.RuntimeImageSelection
			if current != nil {
				converted = &runtime.RuntimeImageSelection{
					RuntimeID: current.RuntimeID, SourceImage: current.SourceImage,
					Digest: current.Digest, Generation: current.Generation,
					SelectedAt: current.SelectedAt,
				}
			}
			return fn(ctx, converted)
		})
}

// e2eDepCoordinator adapts the shared state handle to the Manager's
// dependency-image lease API, exactly like the worker's coordinator adapter.
type e2eDepCoordinator struct {
	st *state.State
}

func (c e2eDepCoordinator) AcquireUse(
	ctx context.Context, depRef string,
) (runtime.DependencyImageLease, error) {
	inner, err := c.st.AcquireDependencyImageUse(ctx, depRef)
	if err != nil {
		return nil, err
	}
	return e2eDepLease{inner: inner}, nil
}

func (c e2eDepCoordinator) TryAcquireGC(
	ctx context.Context, depRef string,
) (runtime.DependencyImageLease, bool, error) {
	inner, ok, err := c.st.TryAcquireDependencyImageGC(ctx, depRef)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return e2eDepLease{inner: inner}, true, nil
}

type e2eDepLease struct {
	inner state.DependencyImageLease
}

func (l e2eDepLease) Verify(ctx context.Context) error { return l.inner.Verify(ctx) }

func (l e2eDepLease) Release() error { return l.inner.Release() }

// writeNodeRuntimeApp materializes a minimal Node handler app for the given
// managed runtime, parsed exactly like startup discovery parses it.
func writeNodeRuntimeApp(t *testing.T, root, name, runtimeID string) app.App {
	t.Helper()
	dir := filepath.Join(root, name)
	templateText := "runtime: " + runtimeID + "\nevents:\n" +
		"  - handler: index.handler\n    pattern:\n      event_name: [INSERT]\n"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create app dir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte(templateText), 0o644); err != nil {
		t.Fatalf("write template for %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function handler() { return 1; }\n"), 0o644); err != nil {
		t.Fatalf("write handler for %s: %v", name, err)
	}
	tmpl, err := app.ParseTemplate([]byte(templateText))
	if err != nil {
		t.Fatalf("parse template for %s: %v", name, err)
	}
	return app.App{Name: name, Dir: dir, Template: tmpl}
}

// removeAppImagesByOwnershipLabel force-removes the test's app images by the
// daemon-side relay.type/relay.app label pair, re-checking each listed image's
// ownership labels before removal — never a repo prefix and never an unfiltered
// whole-daemon enumeration (the same strict classification as the runtime
// tests' cleanupImagePrefixes helper).
func removeAppImagesByOwnershipLabel(t *testing.T, cl *client.Client, appNames []string) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, name := range appNames {
		imgs, err := cl.ImageList(cleanupCtx, client.ImageListOptions{
			Filters: make(client.Filters).
				Add("label", "relay.type=app", "relay.app="+name),
		})
		if err != nil {
			t.Errorf("cleanup: list images for %s: %v", name, err)
			continue
		}
		for _, img := range imgs.Items {
			if img.Labels["relay.type"] != "app" || img.Labels["relay.app"] != name {
				continue // not this test's app image; untouchable
			}
			for _, tag := range img.RepoTags {
				if _, err := cl.ImageRemove(cleanupCtx, tag,
					client.ImageRemoveOptions{Force: true}); err != nil &&
					!cerrdefs.IsNotFound(err) {
					t.Errorf("cleanup: remove app image tag %s: %v", tag, err)
				}
			}
		}
	}
}

// TestIntegrationRuntimeUpdateConvergesRunningAppsThroughReconciler proves the
// end-to-end path across REAL components sharing ONE PostgreSQL schema: the
// REAL `relay runtime update nodejs24.x` CLI command (its own state handle and
// the PRODUCTION Docker Engine resolver opener) resolves the configured
// node:24-alpine tag and persists the multi-platform index digest (generation
// 1); a REAL reconciler running the existing periodic path refreshes its
// selection snapshot from the shared store through the Manager, detects that
// the running UNPINNED Node 24 app's built selection identity no longer
// matches, reprepares it from the pinned index under the Manager's
// advisory-lock publication fence, and swaps it — while the unrelated Node 26
// app (a different managed runtime, still unpinned) is never rebuilt: the same
// registry generation and the same image. No direct SQL, no worker restart, no
// separate deployment coordinator.
func TestIntegrationRuntimeUpdateConvergesRunningAppsThroughReconciler(t *testing.T) {
	cl := testutil.RequireDocker(t)
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	const (
		app24 = "pinconv-node24" // runtime nodejs24.x — the app the CLI pin affects
		app26 = "pinconv-node26" // runtime nodejs26.x — the unrelated control app
	)

	// Image cleanup is registered FIRST (so it runs last): every app image is
	// Relay-owned and label-scoped; leases are retired before removal below.
	t.Cleanup(func() { removeAppImagesByOwnershipLabel(t, cl, []string{app24, app26}) })

	// The production CLI seams, pointed at the shared schema: OpenState writes
	// through its OWN handle (the CLI is a separate process in production) and
	// the resolver opener is the production Docker Engine distribution opener.
	deps := testDeps(t)
	deps.OpenState = func(ctx context.Context) (*state.State, func(), error) {
		st, err := state.Open(ctx, state.Options{DSN: dsn})
		if err != nil {
			return nil, nil, err
		}
		return st, func() { _ = st.Close() }, nil
	}
	deps.OpenRuntimeImageResolver = openDistributionResolver

	root := t.TempDir()
	fn24 := writeNodeRuntimeApp(t, root, app24, "nodejs24.x")
	fn26 := writeNodeRuntimeApp(t, root, app26, "nodejs26.x")

	var builtImages []string

	// The live runtime Manager, wired like the worker: the shared authoritative
	// selection store (advisory-fenced publication) and the state-backed
	// dependency-image coordinator; maintenance stays deferred so the test
	// drives everything explicitly.
	manager, err := runtime.NewManager(
		testutil.DiscardLogger(), nil, "pinconv-e2e",
		runtime.WithLifecycleContext(ctx),
		runtime.WithDeferredMaintenance(),
		runtime.WithRuntimeImageSelectionStore(e2eSelectionStore{st: st}),
		runtime.WithDependencyImageCoordinator(e2eDepCoordinator{st: st}),
	)
	if err != nil {
		t.Fatalf("open runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	// The reconciler drives the real periodic path against the shared registry
	// and store. A short interval keeps convergence prompt without sleeps; the
	// debounce default is irrelevant (no fsnotify events are triggered).
	reg := &runner.Registry{}
	rec := reconciler.New(reconciler.Config{
		Root:     root,
		Interval: 200 * time.Millisecond,
		Debounce: 50 * time.Millisecond,
		State:    st,
	}, reg, manager, testutil.DiscardLogger())

	// Startup-style seeding: discovery row, selection-aware unpinned build,
	// ADVISORY-FENCED publication of the live generation, the success row, and
	// the reconciler's built source/selection identities (exactly what the
	// worker hands over after startup publication).
	published := map[string]*runner.PreparedApp{}
	seed := func(fn app.App) *runtime.Prepared {
		t.Helper()
		fencingToken, err := st.RecordDiscovered(ctx, fn)
		if err != nil {
			t.Fatalf("discover %s: %v", fn.Name, err)
		}
		selection, fingerprint, err := app.SelectAndFingerprintApp(fn.Dir, fn.Template)
		if err != nil {
			t.Fatalf("fingerprint %s: %v", fn.Name, err)
		}
		built, err := manager.PrepareWithFingerprintAndSelection(ctx, fn, fingerprint, selection)
		if err != nil {
			t.Fatalf("prepare %s unpinned: %v", fn.Name, err)
		}
		if built.ImageSelection != "" {
			t.Fatalf("prepared %s selection = %q, want the unpinned identity",
				fn.Name, built.ImageSelection)
		}
		builtImages = append(builtImages, built.Image)
		t.Cleanup(func() {
			leaseCtx, leaseCancel := context.WithTimeout(context.Background(), time.Minute)
			defer leaseCancel()
			_, _ = manager.RetireImageLease(leaseCtx, built.Image)
		})
		published[fn.Name] = runner.NewPrepared(fn, built, manager)
		if err := manager.PublishPrepared(ctx, built, func() error {
			reg.Replace(fn.Name, published[fn.Name])
			return nil
		}); err != nil {
			t.Fatalf("publish %s: %v", fn.Name, err)
		}
		if _, err := st.RecordReconcileSuccess(ctx, fn.Name, built.Image, built.Fingerprint, time.Now(), fn, fencingToken); err != nil {
			t.Fatalf("record initial success for %s: %v", fn.Name, err)
		}
		rec.Seed(fn, built.Fingerprint)
		rec.SeedImageSelection(fn.Name, built.ImageSelection)
		rec.SeedFencingToken(fn.Name, fencingToken)
		return built
	}
	built24 := seed(fn24)
	built26 := seed(fn26)

	// The live manager serves both apps under the UNPINNED identities after
	// startup publication (a warm pool materializes lazily on the first
	// invocation, so the pool snapshot is empty by design here).
	if got := manager.RuntimeImageSelectionIdentity("nodejs24.x"); got != "" {
		t.Fatalf("nodejs24.x identity after startup publication = %q, want unpinned", got)
	}
	if got := manager.RuntimeImageSelectionIdentity("nodejs26.x"); got != "" {
		t.Fatalf("nodejs26.x identity after startup publication = %q, want unpinned", got)
	}

	// Start the REAL periodic reconciliation loop and join it on shutdown.
	recCtx, recCancel := context.WithCancel(ctx)
	startDone := make(chan error, 1)
	go func() { startDone <- rec.Start(recCtx) }()
	t.Cleanup(func() {
		recCancel()
		select {
		case err := <-startDone:
			if err != nil {
				t.Errorf("reconciler Start returned %v", err)
			}
		case <-time.After(time.Minute):
			t.Error("reconciler Start did not join within a minute of cancellation")
		}
		// Drop the registry entries so the image leases held by publications
		// release before the manager closes and stale images are removed.
		reg.Replace(app24, nil)
		reg.Replace(app26, nil)
		for _, image := range builtImages {
			leaseCtx, leaseCancel := context.WithTimeout(context.Background(), time.Minute)
			_, _ = manager.RetireImageLease(leaseCtx, image)
			leaseCancel()
		}
	})

	before24 := reg.GetByName(app24)
	before26 := reg.GetByName(app26)
	if before24 == nil || before24.Prepared() == nil ||
		before26 == nil || before26.Prepared() == nil {
		t.Fatalf("fixture precondition: both apps must be published and available (%+v / %+v)",
			before24, before26)
	}
	if got := before24.Prepared().ImageSelection; got != "" {
		t.Fatalf("fixture precondition: node24 built identity = %q, want unpinned", got)
	}

	// The REAL CLI update: resolves node:24-alpine through the configured
	// Docker Engine Distribution API and CAS-pins generation 1.
	out, err := runRuntimeCLI(t, deps, "update", "nodejs24.x")
	if err != nil {
		t.Fatalf("relay runtime update nodejs24.x: err = %v", err)
	}
	flat := flattened(out)
	if !strings.Contains(flat, "Status: updated") ||
		!strings.Contains(flat, "Previous: unpinned") ||
		!strings.Contains(flat, "Selection saved.") {
		t.Fatalf("update output not a successful first pin:\n%s", out)
	}

	// The CLI persisted the selection into the SHARED store (the reconciler's
	// next refresh reads exactly this row).
	selections := readSelections(t, ctx, st)
	row, ok := selections["nodejs24.x"]
	if !ok || row.Generation != 1 || row.SourceImage != "node:24-alpine" ||
		!runtime.ValidImageDigest(row.Digest) {
		t.Fatalf("persisted selection = %+v, want node:24-alpine pinned at generation 1", row)
	}
	if _, exists := selections["nodejs26.x"]; exists {
		t.Fatalf("unrelated nodejs26.x runtime was pinned: %+v", selections["nodejs26.x"])
	}
	pinned := "node:24-alpine@" + row.Digest
	pinnedImage := built24.Image

	// Deterministic convergence through the periodic path: wait for the Node 24
	// app's PUBLISHED generation to carry the persisted digest, the new pin-
	// folded image, and the persisted success row — and for the Node 26 app to
	// keep the EXACT registry generation instance and image it started with.
	testutil.WaitFor(t, 5*time.Minute, "the Node 24 app to reconcile onto the pinned selection", func() bool {
		pf := reg.GetByName(app24)
		if pf == nil || pf == before24 || pf.Prepared() == nil {
			return false
		}
		if pf.Prepared().ImageSelection != pinned {
			return false
		}
		pinnedImage = pf.Prepared().Image
		if pinnedImage == built24.Image {
			return false // the identity must fold the pin in; never the old tag
		}
		detail, found, err := st.ReadApp(ctx, app24)
		return err == nil && found &&
			detail.Image == pinnedImage &&
			detail.Fingerprint == built24.Fingerprint &&
			detail.Status == state.StatusReady
	})

	// The Node 26 app is untouched: same instance in the registry (no rebuild
	// was ever prepared or swapped), same image, and its state row is byte-
	// identical to the seeded one.
	if got := reg.GetByName(app26); got != before26 {
		t.Fatalf("the unrelated Node 26 app was re-published: same instance expected, got %p", got)
	}
	if got := before26.Prepared().ImageSelection; got != "" {
		t.Fatalf("the unrelated Node 26 app adopted a selection identity %q; it must stay unpinned", got)
	}
	detail26, found, err := st.ReadApp(ctx, app26)
	if err != nil || !found {
		t.Fatalf("read node26 row after convergence: found=%v err=%v", found, err)
	}
	if detail26.Image != built26.Image || detail26.Fingerprint != built26.Fingerprint ||
		detail26.Status != state.StatusReady || detail26.LastReconcileAt == "" {
		t.Fatalf("the unrelated Node 26 app's persisted generation changed: %+v", detail26.Row)
	}

	// The reconciler's snapshot also proves the unrelated runtime is unpinned
	// while the pinned runtime carries the persisted identity.
	if got := manager.RuntimeImageSelectionIdentity("nodejs26.x"); got != "" {
		t.Fatalf("nodejs26.x identity = %q, want unpinned", got)
	}
	if got := manager.RuntimeImageSelectionIdentity("nodejs24.x"); got != pinned {
		t.Fatalf("nodejs24.x identity = %q, want the persisted pin %q", got, pinned)
	}
}
