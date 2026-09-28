package runtime

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"relay/internal/function"
	"relay/internal/testutil"
)

// TestImageIdentityKeyDistinguishesContent pins the immutable content identity
// contract: the same reference with a different Docker image ID is a DIFFERENT
// identity (so warm generations rotate on a moved tag), while the same content is
// the SAME identity (so warm containers are reused). Relay's fingerprint metadata
// is preferred but never required: an image without labels (an external service
// image) still gets a content-addressed identity from its ID alone.
func TestImageIdentityKeyDistinguishesContent(t *testing.T) {
	const ref = "relay-fn-fn:tag"

	same := imageIdentityKey(ref, "sha256:v1", "fp1")
	if again := imageIdentityKey(ref, "sha256:v1", "fp1"); same != again {
		t.Fatalf("identical content produced different identities: %q != %q", same, again)
	}

	moved := imageIdentityKey(ref, "sha256:v2", "fp1")
	if moved == same {
		t.Fatal("the same reference with a new image ID must rotate the identity")
	}

	// The reference is always recoverable, so reference-scoped retirement can
	// match every content identity of a tag.
	if got := identityRef(same); got != ref {
		t.Fatalf("identityRef(%q) = %q, want %q", same, got, ref)
	}

	// No labels required: an external image with only a content ID still yields a
	// distinct, content-addressed identity.
	ext := imageIdentityKey("ghcr.io/acme/api:1", "sha256:ext", "")
	if ext == imageIdentityKey("ghcr.io/acme/api:1", "sha256:ext2", "") {
		t.Fatal("content IDs must distinguish identities even without Relay labels")
	}
	if got := identityRef(ext); got != "ghcr.io/acme/api:1" {
		t.Fatalf("identityRef(external) = %q, want the external reference", got)
	}

	// A reference-only identity (unresolvable image) is its own reference so a
	// Docker-less caller degrades to the historical behavior.
	fallback := imageIdentityKey(ref, "", "")
	if fallback != ref {
		t.Fatalf("reference-only identity = %q, want %q", fallback, ref)
	}
	if got := identityRef(fallback); got != ref {
		t.Fatalf("identityRef(fallback) = %q, want %q", got, ref)
	}
}

// TestResolvedImageCreatePrefersContentID pins the create-side half of the design:
// the container is created from the exact resolved content ID when it is known (so
// a create can never run bytes other than those leased), and falls back to the
// mutable reference only when resolution was unavailable.
func TestResolvedImageCreatePrefersContentID(t *testing.T) {
	if got := (resolvedImage{ref: "relay-fn-a:t", id: "sha256:v1"}).createImage(); got != "sha256:v1" {
		t.Fatalf("createImage with content id = %q, want the content id", got)
	}
	if got := (resolvedImage{ref: "relay-fn-a:t"}).createImage(); got != "relay-fn-a:t" {
		t.Fatalf("createImage without content id = %q, want the reference", got)
	}
}

// TestImageInspectContentLabelsOptional proves the identity helper never requires
// Relay labels: an external image whose inspect response carries no Config.Labels
// still yields its content ID (with an empty fingerprint), and a managed image's
// relay.fingerprint label is extracted when present.
func TestImageInspectContentLabelsOptional(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/ghcr.io/acme/api:1/json", body: `{"Id":"sha256:ext"}`},
		dockerRoute{method: http.MethodGet, path: "/images/relay-fn-a:t/json", body: `{"Id":"sha256:fn","Config":{"Labels":{"relay.fingerprint":"abc123"}}}`},
		dockerRoute{method: http.MethodGet, path: "/images/missing:t/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	id, fp, present, err := m.imageInspectContent(context.Background(), "ghcr.io/acme/api:1")
	if err != nil || !present || id != "sha256:ext" || fp != "" {
		t.Fatalf("external inspect = (%q, %q, %v, %v), want the content id and an empty fingerprint with no labels", id, fp, present, err)
	}
	id, fp, present, err = m.imageInspectContent(context.Background(), "relay-fn-a:t")
	if err != nil || !present || id != "sha256:fn" || fp != "abc123" {
		t.Fatalf("managed inspect = (%q, %q, %v, %v), want the content id and relay.fingerprint", id, fp, present, err)
	}
	_, _, present, err = m.imageInspectContent(context.Background(), "missing:t")
	if err != nil || present {
		t.Fatalf("missing inspect = (present %v, err %v), want absent with no error", present, err)
	}
}

// TestResolveImageIdentityDegradesWithoutDaemon proves resolution is never fatal:
// a Manager with no Docker client (a direct/test caller) and a Manager with a
// non-not-found inspect failure both degrade to the reference identity, and a
// successful inspect promotes to the content identity with the prepared
// fingerprint as the metadata fallback when the image carries no Relay label.
func TestResolveImageIdentityDegradesWithoutDaemon(t *testing.T) {
	noDaemon := &Manager{log: testutil.DiscardLogger()}
	img := noDaemon.resolveImageIdentity(context.Background(), "relay-fn-a:t", "prepared-fp")
	if img.id != "" || img.createImage() != "relay-fn-a:t" {
		t.Fatalf("no-daemon resolution = %+v, want no content id and the reference create", img)
	}
	// The prepared fingerprint is retained as identity metadata even without a
	// content id, so a Docker-less caller is still content-aware; the mutable
	// reference remains recoverable for retirement.
	if img.fingerprint != "prepared-fp" || identityRef(img.identity()) != "relay-fn-a:t" {
		t.Fatalf("no-daemon identity = %q (fingerprint %q), want the reference with the prepared fingerprint", img.identity(), img.fingerprint)
	}
	if img.identity() == "relay-fn-a:t" {
		t.Fatal("a supplied fingerprint must still be part of the identity")
	}

	broken := &Manager{cli: newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/relay-fn-a:t/json", status: http.StatusInternalServerError, body: `{"message":"daemon down"}`},
	), log: testutil.DiscardLogger()}
	img = broken.resolveImageIdentity(context.Background(), "relay-fn-a:t", "prepared-fp")
	if img.id != "" || img.createImage() != "relay-fn-a:t" || identityRef(img.identity()) != "relay-fn-a:t" {
		t.Fatalf("failed-inspect resolution = %+v, want the reference (with the prepared fingerprint) and no content id", img)
	}

	ok := &Manager{cli: newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/relay-fn-a:t/json", body: `{"Id":"sha256:v1"}`},
	), log: testutil.DiscardLogger()}
	img = ok.resolveImageIdentity(context.Background(), "relay-fn-a:t", "prepared-fp")
	if img.id != "sha256:v1" || img.fingerprint != "prepared-fp" || img.createImage() != "sha256:v1" {
		t.Fatalf("resolved = %+v, want content id with the prepared fingerprint fallback", img)
	}
	if img.identity() == "relay-fn-a:t" {
		t.Fatal("a resolved image must not degrade to the reference identity")
	}
}

// newIdentityManager builds a Docker-less Manager whose image resolution and
// container start are both injected, so the Execute path's generation keying on
// image CONTENT can be exercised with no daemon.
func newIdentityManager(t *testing.T) (*Manager, *identityResolver, *startRecorder) {
	t.Helper()
	res := &identityResolver{}
	start := &startRecorder{}
	m := &Manager{log: testutil.DiscardLogger(), maxConcurrency: 4, containers: newContainerCache()}
	m.resolveImageIdentityFn = res.resolve
	m.startContainerFn = start.start
	start.created = make(chan *fakeContainer, 8)
	t.Cleanup(func() { _ = m.Close() })
	return m, res, start
}

// identityResolver is the injected image-resolution seam. current is the image
// content ID every resolution returns; setting it models a tag being moved to new
// bytes. unique, when set, makes each resolution return a fresh content ID
// ("sha256:vN") so a concurrency test can prove each create ran the exact content
// its own resolution returned. calls counts resolutions so a test can prove
// resolution happens BEFORE the lease/create (and exactly once per execute).
type identityResolver struct {
	mu      sync.Mutex
	current string
	unique  bool
	next    int
	calls   int
	refs    []string
}

func (r *identityResolver) resolve(_ context.Context, ref, fingerprint string) (resolvedImage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.refs = append(r.refs, ref)
	id := r.current
	if r.unique {
		r.next++
		id = "sha256:v" + strconv.Itoa(r.next)
	}
	return resolvedImage{ref: ref, id: id, fingerprint: fingerprint}, nil
}

func (r *identityResolver) set(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = id
}

func (r *identityResolver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// startRecorder is the injected container-start seam. It records the container it
// returns and the exact image reference createImage produced, so a test can prove
// the create used the resolved content identity.
type startRecorder struct {
	mu       sync.Mutex
	images   []string
	built    []*fakeContainer
	block    chan struct{}
	buildErr error
	// created receives every container as it is built so a test can synchronize
	// on a lazy start without polling. It is created by newIdentityManager when a
	// test needs it.
	created chan *fakeContainer
}

func (s *startRecorder) start(_ context.Context, _ string, img resolvedImage, _ []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
	s.mu.Lock()
	if s.buildErr != nil {
		err := s.buildErr
		s.buildErr = nil
		s.mu.Unlock()
		return nil, err
	}
	s.images = append(s.images, img.createImage())
	block := s.block
	var c *fakeContainer
	if block != nil {
		c = newBlockingContainer(1)
	} else {
		c = &fakeContainer{}
	}
	s.built = append(s.built, c)
	created := s.created
	s.mu.Unlock()
	if created != nil {
		created <- c
	}
	return c, nil
}

func (s *startRecorder) lastImage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.images) == 0 {
		return ""
	}
	return s.images[len(s.images)-1]
}

func (s *startRecorder) lastContainer() *fakeContainer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.built) == 0 {
		return nil
	}
	return s.built[len(s.built)-1]
}

func (s *startRecorder) starts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.images)
}

// TestExecuteRotatesGenerationOnMovedTag is the core regression: the SAME image
// reference whose resolved content ID changes must rotate its warm generation —
// the idle old container is discarded immediately, a new container is created
// from the new content ID, and a later invocation for the same content is warm.
// It also proves resolution precedes the lease/create and drives the create image.
func TestExecuteRotatesGenerationOnMovedTag(t *testing.T) {
	m, res, start := newIdentityManager(t)
	res.set("sha256:v1")
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 1}

	exec := func() {
		t.Helper()
		if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
			t.Fatalf("execute: %v", err)
		}
	}

	exec()
	if start.starts() != 1 || start.lastImage() != "sha256:v1" {
		t.Fatalf("first create image = %q (starts %d), want the resolved content id sha256:v1", start.lastImage(), start.starts())
	}
	if res.count() != 1 {
		t.Fatalf("resolutions after one execute = %d, want 1 (resolved before the lease/create)", res.count())
	}
	old := start.lastContainer()

	// Same content: warm reuse, no new create, one resolution per execute.
	exec()
	if start.starts() != 1 {
		t.Fatalf("unchanged content started a new container: starts = %d", start.starts())
	}

	// Same tag, new bytes: the idle old container is discarded immediately and a
	// new container is created from the new content id.
	res.set("sha256:v2")
	exec()
	if start.starts() != 2 || start.lastImage() != "sha256:v2" {
		t.Fatalf("moved-tag create image = %q (starts %d), want sha256:v2", start.lastImage(), start.starts())
	}
	if got := old.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("superseded idle container discards = %v, want [%s]", got, reasonImageChanged)
	}

	// Same (new) content again: warm.
	before := start.starts()
	exec()
	if start.starts() != before {
		t.Fatalf("reuse of v2 started a new container: starts %d -> %d", before, start.starts())
	}
}

// TestExecuteMovedTagBusyDrainsAndStaleCannotReacquire proves the full moved-tag
// contract with a busy container: the busy old-content container is never
// discarded mid-invocation but drains on release, and a stale request that
// resolves back to the retired content is served on a throwaway that is never
// pooled.
func TestExecuteMovedTagBusyDrainsAndStaleCannotReacquire(t *testing.T) {
	m, res, start := newIdentityManager(t)
	start.block = make(chan struct{})
	res.set("sha256:v1")
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 2}

	busyDone := make(chan error, 1)
	go func() { busyDone <- m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil) }()
	busy := <-start.created
	<-busy.entered

	// The tag moves while the v1 container is busy. Later starts are no longer
	// blocking, so the new content is served by a fresh container while the busy
	// one is untouched.
	start.block = nil
	res.set("sha256:v2")
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("busy old-content container discarded mid-invocation: %v", got)
	}
	if start.lastImage() != "sha256:v2" {
		t.Fatalf("v2 create image = %q, want sha256:v2", start.lastImage())
	}

	// The busy invocation ends: the superseded container is discarded, not pooled.
	close(busy.release)
	if err := <-busyDone; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("drained old-content discards = %v, want [%s]", got, reasonImageChanged)
	}

	// A stale request that resolves back to the retired v1 content must NOT
	// reacquire a pooled v1 container: it runs on a throwaway, and v2 stays warm.
	res.set("sha256:v1")
	beforeStarts := start.starts()
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("stale v1 execute: %v", err)
	}
	if start.starts() != beforeStarts+1 {
		t.Fatalf("stale v1 must start exactly one throwaway: starts %d -> %d", beforeStarts, start.starts())
	}
	if got := start.lastContainer().reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("stale v1 throwaway discard = %v, want [%s]", got, reasonImageChanged)
	}

	// v2 is still the active generation: a v2 execute reuses its warm container.
	res.set("sha256:v2")
	warmStarts := start.starts()
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("v2 reuse execute: %v", err)
	}
	if start.starts() != warmStarts {
		t.Fatalf("v2 no longer warm after a stale v1 request: starts %d -> %d", warmStarts, start.starts())
	}
}

// TestExecuteResolutionPrecedesConcurrentCreates proves the concurrency contract:
// each concurrent Execute resolves the image content, and the container it creates
// is built from exactly the content that SAME resolution returned (never another
// resolution's bytes). Each resolution is unique here, and the start seam blocks
// so both invocations hold their resolved identity through their creates; the
// created content IDs must therefore be exactly the resolved ones.
func TestExecuteResolutionPrecedesConcurrentCreates(t *testing.T) {
	m, res, start := newIdentityManager(t)
	res.unique = true
	start.block = make(chan struct{})
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 2}

	const n = 2
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() { done <- m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil) }()
	}
	// Both cold starts park in the (blocking) start seam, each already holding its
	// own resolved content identity.
	c1 := <-start.created
	c2 := <-start.created
	if c1 == c2 {
		t.Fatal("pool handed the same container to two concurrent creates")
	}
	if res.count() < n {
		t.Fatalf("resolutions = %d, want >= %d (resolved before each lease/create)", res.count(), n)
	}
	start.mu.Lock()
	images := append([]string(nil), start.images...)
	start.mu.Unlock()
	if len(images) != n {
		t.Fatalf("create images = %v, want %d distinct", images, n)
	}
	seen := map[string]bool{}
	for _, img := range images {
		if img == prepared.Image {
			t.Fatalf("create used the bare reference %q, not a resolved content id", img)
		}
		if seen[img] {
			t.Fatalf("two creates used the same content id %q despite unique resolutions", img)
		}
		seen[img] = true
	}
	close(c1.release)
	close(c2.release)
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent execute: %v", err)
		}
	}
}

// TestExecuteIdentityHelperReusedWithServices is the alignment guard: the same
// resolvedImage helper an external service image uses never requires Relay labels,
// so a function execution against an image whose inspect carries none still gets a
// content-addressed identity and a content-ID create.
func TestExecuteIdentityHelperReusedWithServices(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/ghcr.io/acme/api:1/json", body: `{"Id":"sha256:ext"}`},
	)
	m := &Manager{log: testutil.DiscardLogger(), cli: cli, containers: newContainerCache()}
	var created []string
	m.startContainerFn = func(_ context.Context, _ string, img resolvedImage, _ []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		created = append(created, img.createImage())
		return &fakeContainer{}, nil
	}
	prepared := &Prepared{Name: "fn", Image: "ghcr.io/acme/api:1", Concurrency: 1}

	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute external image: %v", err)
	}
	if len(created) != 1 || created[0] != "sha256:ext" {
		t.Fatalf("external create image = %v, want the resolved content id sha256:ext (no Relay labels required)", created)
	}
}

// TestExecuteSameContentResourceChangeStillPooled is a guard that the
// identity change is what rotates, not merely an execute: a second execute that
// resolves the SAME content ID does not churn (no new resolution identity), while
// a changed resource config still rotates with reasonResourcesChanged and leaves
// the image identity untouched.
func TestExecuteSameContentResourceChangeStillPooled(t *testing.T) {
	m, res, start := newIdentityManager(t)
	res.set("sha256:v1")
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 1}

	m.SetFunctionResources("fn", function.DefaultResourceLimits())
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute defaults: %v", err)
	}
	if start.starts() != 1 {
		t.Fatalf("starts = %d, want 1", start.starts())
	}

	// An unchanged resource config does not churn.
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute again: %v", err)
	}
	if start.starts() != 1 {
		t.Fatalf("unchanged resources churned containers: starts = %d", start.starts())
	}

	// A resource-only change rotates the generation with the resource reason and
	// does not retire the image identity, so the same content warms afterwards.
	old := start.lastContainer()
	m.SetFunctionResources("fn", function.ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 64})
	if got := old.reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Fatalf("resource change discards = %v, want [%s]", got, reasonResourcesChanged)
	}
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute after resource change: %v", err)
	}
	if start.starts() != 2 || start.lastImage() != "sha256:v1" {
		t.Fatalf("resource change create = %q (starts %d), want the same content id sha256:v1", start.lastImage(), start.starts())
	}
	before := start.starts()
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute reuse after resource change: %v", err)
	}
	if start.starts() != before {
		t.Fatalf("same content after a resource change did not warm: starts %d -> %d", before, start.starts())
	}
}

// TestExecuteIdentityResolutionFailureStillServes proves a resolution failure
// degrades to the reference identity rather than failing the invocation: the
// container is created from the reference (no content ID) and still served.
func TestExecuteIdentityResolutionFailureStillServes(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger(), maxConcurrency: 2, containers: newContainerCache()}
	m.resolveImageIdentityFn = func(_ context.Context, ref, _ string) (resolvedImage, error) {
		return resolvedImage{}, errors.New("resolution boom")
	}
	var created []string
	m.startContainerFn = func(_ context.Context, _ string, img resolvedImage, _ []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		created = append(created, img.createImage())
		return &fakeContainer{}, nil
	}
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 1}
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute with resolution failure: %v", err)
	}
	if len(created) != 1 || created[0] != "relay-fn-fn:tag" {
		t.Fatalf("create image = %v, want the reference fallback", created)
	}
}

// TestCacheInvalidateImageRetiresEveryContentOfReference is the cache-level guard
// that InvalidateImage is reference-scoped: retiring a tag retires EVERY content
// identity that tag can resolve to, so a stale request resolving the same tag to
// a new image ID is served on a throwaway and can never reacquire/pool warm
// state.
func TestCacheInvalidateImageRetiresEveryContentOfReference(t *testing.T) {
	cc, ff := newTestCache()
	const ref = "relay-fn-fn-a:v1"
	idV1 := imageIdentityKey(ref, "sha256:v1", "fp")
	idV2 := imageIdentityKey(ref, "sha256:v2", "fp")

	// Seed a warm container for the v1 content.
	if err := cc.execute(context.Background(), "fn-a", idV1, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	old := ff.lastContainer()

	// Retire by REFERENCE (the runner/removal path).
	cc.invalidateImage(ref)
	if got := old.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("idle invalidated container discards = %v, want [%s]", got, reasonImageChanged)
	}

	// A stale request resolving the SAME tag to a NEW image ID is still retired:
	// throwaway, never pooled.
	if err := cc.execute(context.Background(), "fn-a", idV2, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("stale new-content acquire: %v", err)
	}
	if got := ff.lastContainer().reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("stale new-content container discards = %v, want [%s]", got, reasonImageChanged)
	}

	// A second stale request must start a NEW throwaway (the first was not pooled).
	before := ff.count()
	if err := cc.execute(context.Background(), "fn-a", idV2, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("second stale acquire: %v", err)
	}
	if ff.count() != before+1 {
		t.Fatalf("creations = %d, want %d (retired reference must never be pooled)", ff.count(), before+1)
	}
}

// TestCacheRetagThenActivateRevertWarms proves the revert-to-old-content path: a
// forward transition that retags the same reference to new bytes retires only the
// OLD content identity (which cannot be reacquired), and a later activation of the
// reference clears that retirement so the reverted content warms again.
func TestCacheRetagThenActivateRevertWarms(t *testing.T) {
	cc, ff := newTestCache()
	const ref = "relay-fn-fn-a:v1"
	idV1 := imageIdentityKey(ref, "sha256:v1", "fp")
	idV2 := imageIdentityKey(ref, "sha256:v2", "fp")

	if err := cc.execute(context.Background(), "fn-a", idV1, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	old := ff.lastContainer()

	// Retag to v2: the old idle container is discarded, v2 warms.
	if err := cc.execute(context.Background(), "fn-a", idV2, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if got := old.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("old-content discard = %v, want [%s]", got, reasonImageChanged)
	}

	// A stale v1 request is a throwaway (the old content is retired).
	if err := cc.execute(context.Background(), "fn-a", idV1, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("stale v1: %v", err)
	}
	if got := ff.lastContainer().reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("stale v1 discard = %v, want [%s]", got, reasonImageChanged)
	}

	// Reverting the content and activating the reference must un-retire v1 so it
	// warms again.
	cc.activateFunction("fn-a", ref)
	before := ff.count()
	if err := cc.execute(context.Background(), "fn-a", idV1, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("reverted v1: %v", err)
	}
	if got := ff.lastContainer().reasons(); len(got) != 0 {
		t.Fatalf("reverted content must warm, but was discarded: %v", got)
	}
	if err := cc.execute(context.Background(), "fn-a", idV1, 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("reverted v1 reuse: %v", err)
	}
	if ff.count() != before+1 {
		t.Fatalf("reverted content did not warm: creations = %d, want %d", ff.count(), before+1)
	}
}
