package runtime

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/source"
)

// TestMaxConcurrentBuildsMatchesLimiterCapacity pins the startup sizing
// contract: MaxConcurrentBuilds reports the limiter's ACTUAL capacity — the
// explicitly configured one (the worker sizes its startup pool from it), the
// lazily-initialized one for a configured-but-directly-constructed Manager, and
// the package default for a bare Manager.
func TestMaxConcurrentBuildsMatchesLimiterCapacity(t *testing.T) {
	configured := buildCapManager(t, 3, `{"stream":"ok"}`, nil)
	if got := configured.MaxConcurrentBuilds(); got != 3 {
		t.Fatalf("configured Manager MaxConcurrentBuilds = %d, want 3", got)
	}

	direct := &Manager{maxConcurrentBuilds: 7}
	if got := direct.MaxConcurrentBuilds(); got != 7 {
		t.Fatalf("direct Manager MaxConcurrentBuilds = %d, want 7 (its lazily-built limiter)", got)
	}

	bare := &Manager{}
	if got := bare.MaxConcurrentBuilds(); got != DefaultMaxConcurrentBuilds {
		t.Fatalf("bare Manager MaxConcurrentBuilds = %d, want the default %d", got, DefaultMaxConcurrentBuilds)
	}
}

// TestPrepareBuildLimitSharedByStartupAndLiveEntryPoints pins that the
// per-Manager build limiter is ONE limiter for every caller: a startup-shaped
// preparation (PrepareWithFingerprintAndSelection with a caller-resolved
// selection) and a live-shaped one (Prepare) together occupy at most the cap's
// worth of bounded-section slots, a waiting caller is admitted one at a time as
// slots free without a batch barrier, and the cap returns to zero at the end.
func TestPrepareBuildLimitSharedByStartupAndLiveEntryPoints(t *testing.T) {
	park := newPermitPark(8)
	defer park.releaseAll()

	var builds atomic.Int64
	m := buildCapManager(t, 2, `{"stream":"ok"}`, &builds)
	m.afterBuildPermit = park.enter

	liveApp := writeBuildApp(t, "share-live-a")
	startupApp := writeBuildApp(t, "share-startup-b")
	startupSelection, err := source.ForDir(startupApp.Dir)
	if err != nil {
		t.Fatalf("resolve startup selection: %v", err)
	}

	// One live-shaped (Prepare) and one startup-shaped preparation hold the two
	// available slots TOGETHER: the bound is per Manager, not per entry point.
	liveDone := prepareAsync(m, liveApp, context.Background())
	startupDone := make(chan error, 1)
	go func() {
		_, err := m.PrepareWithFingerprintAndSelection(
			context.Background(), startupApp, "", startupSelection,
		)
		startupDone <- err
	}()
	park.waitArrival(t)
	park.waitArrival(t)
	if got := park.peakActive(); got != 2 {
		t.Fatalf("peak in-section preparations = %d, want the cap 2", got)
	}

	// A third preparation — live shaped again — must wait for a slot.
	third := prepareAsync(m, writeBuildApp(t, "share-live-c"), context.Background())
	park.assertNoArrival(t)

	// Free one slot: exactly one waiting caller is admitted, never both.
	close(park.slot(0))
	park.waitArrival(t)
	park.assertNoArrival(t)

	park.releaseAll()
	for i, done := range []<-chan error{liveDone, startupDone, third} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("preparation %d failed: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("preparation %d did not finish", i)
		}
	}
	if got := park.peakActive(); got > 2 {
		t.Fatalf("observed peak %d in section, want <= 2", got)
	}
	if got := len(m.buildLimiterFor().permits); got != 0 {
		t.Fatalf("permits in use after every preparation returned = %d, want 0", got)
	}
}

// joinWitness returns the callback installed as the Manager's
// afterDependencyJoin seam together with the channel it signals. The channel
// carries the depRef the moment a preparation actually JOINED the in-flight
// dependency dedup build (instead of claiming its own), so a test holds the
// leader parked mid-build and proves the follower joined BEFORE the leader is
// released — the exactly-one-shared-build invariant without sleeps and without
// racing the leader's completion. The channel is buffered so a joining waiter
// never blocks on the test.
func joinWitness() (func(depRef string), <-chan string) {
	seen := make(chan string, 8)
	return func(depRef string) {
		select {
		case seen <- depRef:
		default:
		}
	}, seen
}

// sharedDepRoutes builds the scripted-daemon routes for two concurrently
// prepared apps that resolve the SAME content-addressed dependency layer. It
// simulates real Docker visibility: the shared layer's inspect is NotFound
// until a dependency build COMPLETES (the parked leader's POST closes over
// `exists`), after which the layer is present and any later inspect reuses it —
// so even a preparation that somehow misses the dedup join cannot issue a
// second install. Build POSTs are counted per tag at request entry; onBuild
// runs before a counted build may complete so a test can park it mid-flight.
func sharedDepRoutes(
	depRef, imageA, imageB string,
	exists *atomic.Bool,
	depBuilds, appBuilds *atomic.Int64,
	onBuild func(req *http.Request, tag string),
) []dockerRoute {
	return []dockerRoute{
		{
			// Every image inspect: the shared layer is absent until built;
			// the app images are always absent in this fake daemon.
			method: http.MethodGet, path: "/images/",
			serve: func(req *http.Request) (int, string) {
				if req.URL.Path == "/images/"+depRef+"/json" && exists.Load() {
					return http.StatusOK, "{}"
				}
				return http.StatusNotFound, `{"message":"no such image"}`
			},
		},
		{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(req *http.Request) {
				tag := req.URL.Query().Get("t")
				switch tag {
				case depRef:
					depBuilds.Add(1)
				case imageA, imageB:
					appBuilds.Add(1)
				}
				// onBuild may park this build mid-flight (its permit is
				// already acquired); after it returns the build COMPLETES,
				// and a completed shared-layer build is then visible.
				onBuild(req, tag)
				if tag == depRef {
					exists.Store(true)
				}
			},
		},
	}
}

// TestPrepareDependencySharedBuildNotDuplicatedUnderCapOne proves that a
// content-addressed dependency layer shared by two concurrently prepared apps
// is built EXACTLY once even when MAX_CONCURRENT_BUILDS is 1: the leader parks
// mid-build holding the SOLE permit, the follower JOINED that in-flight build
// (witnessed by the afterDependencyJoin seam while the leader is still parked)
// and holds no permit, and afterwards the two app-image builds serialize on
// the same solo slot so both preparations complete — no deadlock. No duplicate
// install ever runs, and the fake daemon's inventory shows the built layer.
func TestPrepareDependencySharedBuildNotDuplicatedUnderCapOne(t *testing.T) {
	fnA, imageA, depRef := dependencyLifecycleFixture(t, "dep-capone-a")
	fnB, imageB, depRefB := dependencyLifecycleFixture(t, "dep-capone-b")
	if depRefB != depRef {
		t.Fatalf("A and B must share one dependency layer, got %s and %s", depRef, depRefB)
	}

	var depBuilds, appBuilds atomic.Int64
	var depExists atomic.Bool

	// The leader's shared dependency build parks at the route: the parked
	// permit IS the observable bounded-section occupancy under cap 1.
	leaderPark := newRaceGate()
	defer leaderPark.releaseAll()
	joinSeam, joined := joinWitness()

	cli := newScriptedDockerClient(t, sharedDepRoutes(
		depRef, imageA, imageB, &depExists, &depBuilds, &appBuilds,
		func(req *http.Request, tag string) {
			if tag == depRef {
				leaderPark.enteredOnce.Do(func() { close(leaderPark.entered) })
				leaderPark.parked(req)
			}
		},
	)...)
	m := newLifecycleManager(t, cli, context.Background())
	m.buildLimit.Store(newBuildLimiter(1))
	m.afterDependencyJoin = joinSeam

	doneA := prepareAsync(m, fnA, context.Background())
	doneB := prepareAsync(m, fnB, context.Background())

	// The leader is parked mid-shared-build; it holds the ONLY permit.
	leaderPark.wait(t, "the shared dependency build")
	if got := depBuilds.Load(); got != 1 {
		t.Fatalf("dependency builds issued while the leader parks = %d, want exactly 1", got)
	}
	if got := len(m.buildLimiterFor().permits); got != 1 {
		t.Fatalf("permits in use while the leader parks = %d, want exactly 1 (the solo slot)", got)
	}

	// The follower JOINED that in-flight build instead of claiming another: the
	// seam fires while the leader is still parked.
	if joinedDepRef := <-joined; joinedDepRef != depRef {
		t.Fatalf("joined depRef = %q, want the shared %q", joinedDepRef, depRef)
	}
	// The joined follower HOLDS NO PERMIT while it waits for the outcome.
	if got := len(m.buildLimiterFor().permits); got != 1 {
		t.Fatalf("permits in use while the follower waits = %d, want exactly 1 (only the leader)", got)
	}

	// Let the shared dependency build complete: the layer becomes visible, the
	// follower's join resolves with the same success, and with cap 1 the two
	// app image builds serialize through the same solo slot.
	leaderPark.releaseAll()
	for i, done := range []<-chan error{doneA, doneB} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("prepare %d failed: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("prepare %d did not finish (a build-slot deadlock would look like this)", i)
		}
	}
	if got := depBuilds.Load(); got != 1 {
		t.Fatalf("dependency builds issued = %d, want exactly 1 (the shared layer must not be duplicated)", got)
	}
	if got := appBuilds.Load(); got != 2 {
		t.Fatalf("app image builds issued = %d, want 2 (one per app)", got)
	}
	if !depExists.Load() {
		t.Fatal("the fake daemon never made the built dependency layer visible")
	}
	if got := len(m.buildLimiterFor().permits); got != 0 {
		t.Fatalf("permits in use after both preparations returned = %d, want 0", got)
	}
}

// TestPrepareDependencyFollowerJoinsWithoutSlot is the direct complement of the
// cap-one proof above with a larger cap (2): the follower still JOINED the
// in-flight shared dependency build (join-witnessed before the leader is
// released) and holds no permit; one dependency build runs, and both
// preparations then run their app image builds concurrently to the cap.
func TestPrepareDependencyFollowerJoinsWithoutSlot(t *testing.T) {
	fnA, imageA, depRef := dependencyLifecycleFixture(t, "dep-two-a")
	fnB, imageB, depRefB := dependencyLifecycleFixture(t, "dep-two-b")
	if depRefB != depRef {
		t.Fatalf("A and B must share one dependency layer, got %s and %s", depRef, depRefB)
	}

	var depBuilds, appBuilds atomic.Int64
	var depExists atomic.Bool

	leaderPark := newRaceGate()
	defer leaderPark.releaseAll()
	joinSeam, joined := joinWitness()
	appGate := newCountingRaceGate(2)
	defer appGate.releaseAll()

	cli := newScriptedDockerClient(t, sharedDepRoutes(
		depRef, imageA, imageB, &depExists, &depBuilds, &appBuilds,
		func(req *http.Request, tag string) {
			switch tag {
			case depRef:
				leaderPark.enteredOnce.Do(func() { close(leaderPark.entered) })
				leaderPark.parked(req)
			default:
				appGate.parked(req, appGate.arrive())
			}
		},
	)...)
	m := newLifecycleManager(t, cli, context.Background())
	m.afterDependencyJoin = joinSeam

	doneA := prepareAsync(m, fnA, context.Background())
	doneB := prepareAsync(m, fnB, context.Background())

	// The leader parks mid-shared-build; the follower JOINED the in-flight
	// generation (witnessed while the leader stays parked) and holds no slot.
	leaderPark.wait(t, "the shared dependency build")
	if joinedDepRef := <-joined; joinedDepRef != depRef {
		t.Fatalf("joined depRef = %q, want %q", joinedDepRef, depRef)
	}
	if got := depBuilds.Load(); got != 1 {
		t.Fatalf("dependency builds issued while the leader parks = %d, want exactly 1", got)
	}
	if got := len(m.buildLimiterFor().permits); got != 1 {
		t.Fatalf("permits in use while the follower waits = %d, want exactly 1 (only the leader)", got)
	}

	// Releasing the leader completes the shared dependency build; the joined
	// follower then proceeds to its own app image build, and BOTH app image
	// builds run concurrently (the cap is 2).
	leaderPark.releaseAll()
	appGate.waitEntered(t, "both app image builds parked together")
	if got := depBuilds.Load(); got != 1 {
		t.Fatalf("dependency builds issued = %d, want exactly 1", got)
	}
	if got := appBuilds.Load(); got != 2 {
		t.Fatalf("app image builds issued = %d, want 2", got)
	}

	appGate.releaseAll()
	for i, done := range []<-chan error{doneA, doneB} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("prepare %d failed: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("prepare %d did not finish", i)
		}
	}
}
