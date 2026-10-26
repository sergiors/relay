package runner

import (
	"sort"
	"sync"
	"testing"

	"relay/internal/testutil"
)

// The image-cleaner resolver's cache is POSITIVE-ONLY: an early lookup that
// finds no ImageCleaner capability (an empty registry during startup, or an
// app whose executor is a test fake) must NOT disable image retirement for
// good, because a capable executor can be published later on the live
// registry. Once a cleaner IS resolved, it is retained across registry
// removals so the removal's own cleanup still has a cleaner to use.

// TestRetirementAfterLateCapabilityPublication pins the miss-retry contract:
// resolver lookups made before any capable executor is published yield nil
// without caching the miss, so a later publication re-enables retirement and
// RetireImage actually removes the superseded image.
func TestRetirementAfterLateCapabilityPublication(t *testing.T) {
	t.Run("empty registry", func(t *testing.T) {
		// Given: an empty registry (the worker's startup window).
		r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
		if got := r.resolver(); got != nil {
			t.Fatalf("resolver on an empty registry = %v, want nil", got)
		}

		// When/Then: a capable app is published late and its images retire.
		assertLatePublicationRetires(t, r)
	})

	t.Run("incapable executor published first", func(t *testing.T) {
		// Given: a registry whose only executor cannot clean images.
		r := NewWithMetrics(
			[]*PreparedApp{newFn(t, "a")}, testutil.DiscardLogger(), nil,
		)
		if got := r.resolver(); got != nil {
			t.Fatalf("resolver over an incapable executor = %v, want nil", got)
		}

		// When/Then: the app's executor is swapped to a capable one.
		assertLatePublicationRetires(t, r)
	})
}

// assertLatePublicationRetires publishes a capable executor as app "a" on r's
// registry and requires a retirement to resolve the cleaner and remove the
// image exactly once. It is the observable half of both late-publication
// scenarios above, so the miss is proven NOT to have stuck.
func assertLatePublicationRetires(t *testing.T, r *Runner) {
	t.Helper()
	exec := &blockingExecutor{}
	r.reg.Replace("a", fpClean(t, "a", currentImage, exec))

	// The watcher must be registered before the retirement so its one removal
	// attempt cannot complete unnoticed.
	w := newAttemptWatcher(r, 1)
	r.RetireImage(retiredImage)
	w.wait(t, 1)

	got := exec.removedImages()
	if len(got) != 1 || got[0] != retiredImage {
		t.Fatalf(
			"removed after late publication = %v, want exactly [%s]",
			got, retiredImage,
		)
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("a removed image must not stay deferred, got %v", got)
	}
}

// TestRemoveAppImagesAfterLastAppRemovedStillCleans pins the retained-cleaner
// contract: the reconciler removes the app's registry entry BEFORE calling
// RemoveAppImages, so the cleanup must run on the cleaner cached while the app
// was still published — an empty registry must not turn app cleanup into a
// silent no-op.
func TestRemoveAppImagesAfterLastAppRemovedStillCleans(t *testing.T) {
	exec := &blockingExecutor{tags: map[string][]string{
		"a": {"relay-app-a:aaaa", "relay-app-a:bbbb"},
	}}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	// Given: the cleaner is resolved while app "a" is still published.
	if r.resolver() == nil {
		t.Fatal("resolver must find the published capable executor")
	}

	// When: the app is removed from the registry first (the reconciler's
	// order), and then its images are cleaned up.
	r.reg.Replace("a", nil)
	if r.resolver() == nil {
		t.Fatal("the cached cleaner must be retained after the last app is removed")
	}

	w := newAttemptWatcher(r, 2)
	r.RemoveAppImages("a")
	w.wait(t, 1)
	w.wait(t, 2)

	// Then: every recorded version was listed through the retained cleaner and
	// removed exactly once each. The removal chain can still consult the
	// registry (ServesImage on the now-empty registry is false) and the
	// cleaner stays available for later cleanups.
	got := exec.removedImages()
	sort.Strings(got)
	want := []string{"relay-app-a:aaaa", "relay-app-a:bbbb"}
	if len(got) != len(want) {
		t.Fatalf("removed after app removal = %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("removed after app removal = %v, want exactly %v", got, want)
		}
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("successfully removed images must not stay deferred, got %v", got)
	}
	if r.resolver() == nil {
		t.Fatal("the cleaner must stay resolvable for later cleanups")
	}
}

// TestResolverConcurrentLookupsAcrossPublication pins the resolver's
// concurrency contract: concurrent lookups race only on the mutex-guarded
// positive cache, no racing lookup caches a miss, and every lookup ordered
// after a capable publication resolves to that executor.
func TestResolverConcurrentLookupsAcrossPublication(t *testing.T) {
	const lookers = 8

	// Given: a registry whose only executor cannot clean images, probed by
	// concurrent lookups. All of them must observe nil.
	r := NewWithMetrics(
		[]*PreparedApp{newFn(t, "a")}, testutil.DiscardLogger(), nil,
	)
	misses := make([]ImageCleaner, lookers)
	var wg sync.WaitGroup
	for i := 0; i < lookers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			misses[i] = r.resolver()
		}(i)
	}
	wg.Wait()
	for i, got := range misses {
		if got != nil {
			t.Fatalf(
				"concurrent lookup %d over an incapable registry = %v, want nil",
				i, got,
			)
		}
	}

	// When: the capable executor is published (a live registry replacement)
	// while more lookups keep racing against the swap. Each racer resolves one
	// final time after observing the stop, and that final lookup is ordered
	// after the publication (the stop close follows the swap).
	exec := &blockingExecutor{}
	finals := make([]ImageCleaner, lookers)
	stop := make(chan struct{})
	for i := 0; i < lookers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					finals[i] = r.resolver()
					return
				default:
					r.resolver()
				}
			}
		}(i)
	}
	r.reg.Replace("a", fpClean(t, "a", currentImage, exec))
	close(stop)
	wg.Wait()

	// Then: every post-publication final lookup resolved the published
	// cleaner; no racing lookup cached an earlier miss.
	for i, got := range finals {
		if got != exec {
			t.Fatalf(
				"final lookup %d = %v, want the published cleaner (%v)",
				i, got, exec,
			)
		}
	}
}
