//go:build integration

// Cross-process dependency-image GC regression (IR-004), against the real
// Docker daemon and real PostgreSQL directly-backed dependency lease state.
//
// Confirmed failure window (established by three prior real-daemon runs): two
// independent workers on one host share one daemon; with a process-local lease
// coordinator, worker B's GC could not see worker A's ADMITTED dependency
// lease, so B removed the dependency image while A's Prepare sat between its
// completed dependency build and its app build request — the app build then
// issued its FROM against a vanished tag.
//
// The deterministic shape:
//
//   - Manager A: a production NewManager whose dependency lease coordinator is
//     the State-backed adapter over state handle A. Its afterBuildPermit seam
//     parks on call #2 — dependency build complete, app build request NOT yet
//     issued — the exact window.
//   - Manager B: invokes the production CleanupUnusedDependencies through a
//     transport that (a) filters every Docker image listing to ONLY that exact
//     unique dependency image (so GC's candidate set is exactly one) and (b)
//     refuses EVERY Docker DELETE except a removal of that one reference. A
//     forbidden DELETE is recorded and fails the test: even a listing-guard
//     bug can only ever surface as a refused removal, never collateral
//     deletion.
//
// With a State-backed coordinator, B's GC must skip (the refusal comes from
// the lease gate before any DELETE), the dependency tag must survive, A must
// resume and Prepare must succeed. After A's app lease is released and the
// unique app image is removed, B's GC must eventually remove the dependency —
// while never having issued any other DELETE.
//
// Test construction injects the coordinator explicitly into BOTH managers: a
// manager without a coordinator must fail closed for dependency-bearing
// Prepare and GC, so no test camouflage exists. Prior-checked real-daemon
// behavior is preserved: the removal itself stays FORCE-FREE production GC;
// only the fixture's own app image is force-removed by the test directly.
//
// Contract note: DependencyImageCoordinator/DependencyImageLease and the
// WithDependencyImageCoordinator option are implemented by the production
// tree (internal/runtime/dep_coord.go; the worker wires its State-backed
// adapter in internal/worker/dep_coordinator.go). The small State adapter
// below is the runtime-test counterpart of that worker adapter — it stays in
// the test rather than importing internal/worker into runtime tests.
//
// Supported lifecycle order (documented for State.Close semantics): every
// Manager using a State-backed coordinator CLOSES and joins BEFORE the State
// handle closes; State.Close releasing session locks is the process-cleanup
// backstop, and using coordination after its handle closed fails closed —
// never "silently safe".
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/state"
	"relay/internal/testutil"
)

// stateDepCoordinator adapts state.State's dependency-image lease API to
// runtime.DependencyImageCoordinator, so the managers below coordinate through
// REAL cross-process PostgreSQL state rather than a process-local fake. This
// is the small adapter the contract allows for packages outside runtime.
type stateDepCoordinator struct {
	st *state.State
}

func (c stateDepCoordinator) AcquireUse(
	ctx context.Context, depRef string,
) (DependencyImageLease, error) {
	inner, err := c.st.AcquireDependencyImageUse(ctx, depRef)
	if err != nil {
		return nil, err
	}
	return stateDepLease{inner: inner}, nil
}

func (c stateDepCoordinator) TryAcquireGC(
	ctx context.Context, depRef string,
) (DependencyImageLease, bool, error) {
	inner, ok, err := c.st.TryAcquireDependencyImageGC(ctx, depRef)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return stateDepLease{inner: inner}, true, nil
}

// stateDepLease adapts the state lease; Verify/Release are the runtime
// contract's method shapes.
type stateDepLease struct {
	inner state.DependencyImageLease
}

func (l stateDepLease) Verify(ctx context.Context) error { return l.inner.Verify(ctx) }

func (l stateDepLease) Release() error { return l.inner.Release() }

// gcGuardTransport is Manager B's transport: it reaches the REAL daemon with
// exactly two deviations — the listing filter and the DELETE guard below.
type gcGuardTransport struct {
	base   http.RoundTripper
	depRef string // untagged repo reference, as depImageRef/labels carry it
	depTag string // daemon-normalized ":latest" form of depRef

	mu        sync.Mutex
	depDelete int
	forbidden []string
}

// RoundTrip fails every HTTP DELETE closed: the ONLY permitted DELETE is the
// exact dependency-image removal (by ref, its ":latest" normalization, or the
// same reference under any URL encoding the daemon path carries). Every other
// DELETE — any other image, any other daemon route that ever issues a DELETE —
// is REFUSED at the transport and recorded, so an ownership guard bug in
// production can only ever surface as a refused removal, never as collateral
// deletion.
func (g *gcGuardTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete {
		const imagesPath = "/images/"
		var name string
		if i := strings.Index(req.URL.Path, imagesPath); i >= 0 {
			decoded, err := url.PathUnescape(req.URL.Path[i+len(imagesPath):])
			if err == nil {
				name = decoded
			}
		}
		if name != "" && (name == g.depRef || name == g.depTag) {
			g.mu.Lock()
			g.depDelete++
			g.mu.Unlock()
			return g.base.RoundTrip(req)
		}
		// Fail closed: record the full route (method + path) so the failure
		// message identifies what the guard refused, drain any request body
		// exactly as a real transport would, and never reach the daemon.
		g.mu.Lock()
		g.forbidden = append(g.forbidden, req.Method+" "+req.URL.Path)
		g.mu.Unlock()
		if req.Body != nil {
			_, _ = io.Copy(io.Discard, req.Body)
		}
		return &http.Response{
			StatusCode: http.StatusConflict,
			Body: io.NopCloser(strings.NewReader(
				`{"message":"IR-004 guard: refused delete of unexpected route"}`)),
			Header:  http.Header{"Content-Type": []string{"application/json"}},
			Request: req,
		}, nil
	}

	// Listing filter: GC sees ONLY the candidate image (the daemon appends
	// ":latest" to a plain repo tag, so match the normalized form too). This
	// keeps the candidate set unique to this test even when the daemon hosts
	// sibling test layers, service images, or a live Relay worker's images.
	if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/images/json") {
		resp, err := g.base.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var items []map[string]any
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, fmt.Errorf("decode image list: %w", err)
		}
		out := make([]map[string]any, 0, 1)
		for _, item := range items {
			tags, _ := item["RepoTags"].([]any)
			for _, raw := range tags {
				if tag, _ := raw.(string); tag == g.depRef || tag == g.depTag {
					out = append(out, item)
					break
				}
			}
		}
		respBody, err := json.Marshal(out)
		if err != nil {
			return nil, fmt.Errorf("encode filtered image list: %w", err)
		}
		resp.Body = io.NopCloser(strings.NewReader(string(respBody)))
		resp.ContentLength = int64(len(respBody))
		return resp, nil
	}

	return g.base.RoundTrip(req)
}

func (g *gcGuardTransport) sawDepDelete() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.depDelete > 0
}

// forbiddenDeletes returns a copy so a caller's assertion message cannot race
// a concurrent write.
func (g *gcGuardTransport) forbiddenDeletes() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.forbidden...)
}

// guardDockerClient returns a Docker client that reaches the real daemon
// through the env-configured dialer, wrapped in the GC guard transport (the
// wiring mirrors faultBuildClient).
func guardDockerClient(
	t *testing.T, depRef string,
) (*client.Client, *gcGuardTransport) {
	t.Helper()
	envCli, err := client.New(client.FromEnv)
	if err != nil {
		t.Fatalf("docker client: %v", err)
	}
	t.Cleanup(func() { _ = envCli.Close() })
	dial := envCli.Dialer()
	base := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dial(ctx)
		},
	}
	t.Cleanup(base.CloseIdleConnections)
	guard := &gcGuardTransport{base: base, depRef: depRef, depTag: depRef + ":latest"}
	wrapped, err := client.New(client.WithHTTPClient(&http.Client{Transport: guard}))
	if err != nil {
		t.Fatalf("guard docker client: %v", err)
	}
	t.Cleanup(func() { _ = wrapped.Close() })
	return wrapped, guard
}

// TestIntegrationCrossProcessDependencyGCOrdering is the IR-004 regression:
// two independent Managers with separate State handles on ONE database, one
// parked mid-Prepare, one running the production dependency GC, prove the
// ordering holds end to end.
func TestIntegrationCrossProcessDependencyGCOrdering(t *testing.T) {
	testutil.RequireStateDatabase(t)
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// A per-run manifest marker gives the dependency fingerprint bytes unique
	// to this run, making both the non-removal and the eventual-removal
	// assertions deterministic even on a daemon shared with sibling tests and
	// a live worker (same pattern as deps_cleanup_integration_test.go). The
	// app identity itself is also per-invocation unique, so concurrent test
	// binaries never share the app image, its lease key, or its cleanup scope.
	appName := testutil.UniqueName(t, "dep-ir004")
	marker := "# ir004 " + time.Now().Format(time.RFC3339Nano) + "\n"
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	writeFile(t, dir, "requirements.txt", marker+"six==1.16.0\n")
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	depRef := expectedDependencyRef(t, fn)
	fp, err := app.Fingerprint(dir)
	if err != nil {
		t.Fatalf("app fingerprint: %v", err)
	}
	appImage := ImageRef(appName, fp)
	if imageExistsInDaemon(cli, ctx, depRef) {
		t.Fatalf("unique dependency layer %s must not pre-exist", depRef)
	}
	// Test-side cleanup touches ONLY this run's fixtures: the unique app-image
	// prefix (name + fingerprint are per-invocation) and the unique dependency
	// reference.
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-"+appName+":", depRef))

	// Two OWNERFUL State handles on ONE database — the exact production branch:
	// a worker's State pins its node-lock connection and every dependency lease
	// runs on it (withDependencyCoordConn's owner branch). The two identities
	// are unique per Manager and per run (different NODE_IDs stay compatible
	// through the shared TEST_DATABASE_URL database; only re-using the SAME
	// NodeID concurrently is refused).
	dsn := testutil.NewStateDSN(t)
	stA := testutil.OpenState(t, dsn, state.Options{
		NodeID:   testutil.UniqueName(t, "ir004-wk-a"),
		Instance: "inst-a-" + time.Now().Format(time.RFC3339Nano),
	})
	stB := testutil.OpenState(t, dsn, state.Options{
		NodeID:   testutil.UniqueName(t, "ir004-wk-b"),
		Instance: "inst-b-" + time.Now().Format(time.RFC3339Nano),
	})
	coordA := stateDepCoordinator{st: stA}
	coordB := stateDepCoordinator{st: stB}

	// Manager A: production construction with the coordinator injected
	// explicitly (no fallback to nil-lazied coordination), parked at the seam.
	mA, err := NewManager(
		testutil.DiscardLogger(), nil, "test-host-a",
		WithDeferredMaintenance(),
		WithDependencyImageCoordinator(coordA),
	)
	if err != nil {
		t.Fatalf("manager A: %v", err)
	}
	t.Cleanup(func() { _ = mA.Close() })

	gate := newRaceGate()
	// Barrier hygiene: on EVERY failure path (including t.Fatal while parked)
	// the parked Prepare goroutine is unblocked deterministically, so it cannot
	// outlive the Manager/state closures (the garbage collector runs during the
	// Manager's own Close cancellation otherwise).
	t.Cleanup(gate.releaseAll)
	var permits atomic.Int64
	mA.afterBuildPermit = func() {
		if permits.Add(1) == 2 {
			gate.enteredOnce.Do(func() { close(gate.entered) })
			// Park: release only through the test's deterministic close (or
			// the bounded context unwinding a wedged window).
			select {
			case <-gate.release:
			case <-ctx.Done():
			}
		}
	}

	// Manager B: production construction with its State-backed coordinator,
	// then ONLY its Docker client is swapped for the guarded dialer — the
	// production Manager lifecycle wiring (operation gate, shutdown, close
	// joins) is what the GC call is exercised through, not a hand-built
	// struct. The swapped-out client is closed immediately (it performed no
	// work and delivers nothing).
	mB, err := NewManager(
		testutil.DiscardLogger(), nil, "test-host-b",
		WithDeferredMaintenance(),
		WithDependencyImageCoordinator(coordB),
	)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}
	t.Cleanup(func() { _ = mB.Close() })
	guardCli, guard := guardDockerClient(t, depRef)
	prevCli := mB.cli
	mB.cli = guardCli
	_ = prevCli.Close()

	// Manager A's Prepare, parked inside the exact window.
	errA := make(chan error, 1)
	var prepared *Prepared
	go func() {
		p, err := mA.Prepare(ctx, fn)
		if err == nil {
			prepared = p
		}
		errA <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Minute):
		// A deadlock bound only: the parked signal arrives through the
		// production seam, never on a timer.
		t.Fatal("Manager A never parked between the dependency build and the app build")
	}

	// Barrier validity check on the UNFILTERED daemon view: right now NO tagged
	// managed image on the whole daemon carries relay.dependency == depRef, so
	// the GC listing filter below is demonstrably equivalent for this candidate
	// — it removes images that cannot contribute a reference for depRef, not
	// evidence. (Before Manager A's Prepare completes, nothing is built FROM
	// the layer; after it completes and the app image is removed, the fresh
	// revalidation inside GC re-makes the same decision under the exclusive
	// lease.)
	{
		allImages, err := cli.ImageList(ctx, client.ImageListOptions{})
		if err != nil {
			t.Fatalf("unfiltered daemon image list at the barrier: %v", err)
		}
		for _, img := range allImages.Items {
			if got := img.Labels[labelDependency]; got == depRef {
				t.Fatalf("tagged managed image %v already references %s at the barrier", img.RepoTags, depRef)
			}
		}
	}

	// Window assertions, under B's guarded daemon view.
	removed, gcErr := mB.CleanupUnusedDependencies(ctx)
	if gcErr != nil {
		t.Fatalf("parent-window GC: %v", gcErr)
	}
	if removed != 0 {
		t.Fatalf("parent-window GC removed %d dependency images, want 0 (the admitted dependency lease must win)", removed)
	}
	if forbidden := guard.forbiddenDeletes(); len(forbidden) != 0 {
		t.Fatalf("the parked-window GC must observe no unexpected DELETEs, saw %v", forbidden)
	}
	if guard.sawDepDelete() {
		t.Fatal("the parked-window GC must not even issue the dependency DELETE (refusal must come from the lease gate)")
	}
	if !imageExistsInDaemon(cli, ctx, depRef) {
		t.Fatal("the dependency image must survive the parked-window GC")
	}

	// Resume Manager A; Prepare must now succeed end to end.
	gate.releaseAll()
	select {
	case err := <-errA:
		if err != nil {
			t.Fatalf("Prepare after the guarded GC skip: %v", err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("Manager A's Prepare never resumed after the unblock")
	}
	if prepared == nil {
		t.Fatal("Prepare succeeded but returned no result")
	}
	defer prepared.ReleaseLease()
	if prepared.Dependency != depRef {
		t.Fatalf("Prepare dependency = %q, want %q", prepared.Dependency, depRef)
	}
	if prepared.Image != appImage {
		t.Fatalf("Prepare image = %q, want %q", prepared.Image, appImage)
	}
	if !imageExistsInDaemon(cli, ctx, depRef) {
		t.Fatal("the dependency image must survive a successful Prepare")
	}

	// Eventually-removable phase: release the app lease, remove the unique app
	// image directly (NOT Manager.RemoveImage, so no lifecycle GC preempts the
	// ordering under test), then B's production GC must remove the dependency
	// — with the guard proving no other DELETE was ever issued.
	prepared.ReleaseLease()
	if _, err := cli.ImageRemove(
		ctx, prepared.Image, client.ImageRemoveOptions{Force: true},
	); err != nil {
		t.Fatalf("remove the unique app image %s: %v", prepared.Image, err)
	}
	var gcLastErr error
	if !pollUntil(ctx, 90*time.Second, func() bool {
		_, err := mB.CleanupUnusedDependencies(ctx)
		if err != nil {
			gcLastErr = err
			return false
		}
		return !imageExistsInDaemon(cli, ctx, depRef)
	}) {
		t.Fatalf("the dependency image %s was never removed by the released gate's GC "+
			"(last GC error: %v, dependency deletes: %v, forbidden deletes: %v)",
			depRef, gcLastErr, guard.sawDepDelete(), guard.forbiddenDeletes())
	}
	if !guard.sawDepDelete() {
		t.Fatal("the dependency removal must have been issued by B's guarded GC")
	}
	if forbidden := guard.forbiddenDeletes(); len(forbidden) != 0 {
		t.Fatalf("unexpected DELETEs beyond the unique dependency: %v", forbidden)
	}
	if imageExistsInDaemon(cli, ctx, prepared.Image) {
		t.Fatal("the removed app image must stay removed")
	}

	// Join everything explicitly (cleanup also covers failure paths).
	if err := mB.Close(); err != nil {
		t.Logf("close manager B: %v", err)
	}
	if err := mA.Close(); err != nil {
		t.Logf("close manager A: %v", err)
	}
	if err := stB.Close(); err != nil {
		t.Logf("close state B: %v", err)
	}
	if err := stA.Close(); err != nil {
		t.Logf("close state A: %v", err)
	}
}
