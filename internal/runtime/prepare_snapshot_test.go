package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
)

// stagedContextContents reads a build-context tar captured from the daemon
// request into a path -> bytes map. It is the content-level companion to
// stagedContextFiles (which only reports presence), so a test can assert the
// EXACT bytes staged, not merely which files appeared.
func stagedContextContents(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	contents := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read build context tar: %v", err)
		}
		if hdr.Typeflag == tar.TypeDir {
			continue
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read %s from build context tar: %v", hdr.Name, err)
		}
		contents[hdr.Name] = string(b)
	}
	return contents
}

// TestPrepareTagAndStagedBytesShareOneSnapshot is the deterministic regression
// for the fingerprint-then-stage TOCTOU: the source is mutated at the exact
// boundary between the snapshot capture and the staging (the afterSourceSnapshot
// seam, which fires just before the fingerprint is derived), yet the image tag
// and the staged bytes still describe the SAME content — the captured snapshot,
// not the mutated live tree. There are no sleeps: the mutation happens
// synchronously inside Prepare.
//
// It proves all three identities derive from one read:
//   - Prepared.Fingerprint equals the digest of the ORIGINAL bytes;
//   - Prepared.Image is tagged with that digest;
//   - the staged build context contains the ORIGINAL bytes, not the edit.
func TestPrepareTagAndStagedBytesShareOneSnapshot(t *testing.T) {
	dir := t.TempDir()
	original := []byte("export function h(){ return 'original'; }\n")
	if err := os.WriteFile(filepath.Join(dir, "index.js"), original, 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	// The reference digest over the original selected source: exactly what the
	// snapshot captured before the mutation must yield.
	reference, err := app.CaptureSourceSnapshot(mustSelect(t, dir))
	if err != nil {
		t.Fatalf("capture reference snapshot: %v", err)
	}
	wantFP := reference.Fingerprint()
	reference.Discard()

	var contextTar []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{}`,
			onBody: func(b []byte) { contextTar = append([]byte(nil), b...) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	// Mutate BOTH the tracked file and an ignored one at the capture/stage
	// boundary: the edit must not appear in the tag or the staged context. A
	// fresh .gitignore also appears, but because it is NOT part of the captured
	// policy it must not be staged either (the snapshot reflects the captured
	// selection, not a re-walk).
	m.afterSourceSnapshot = func(_ *app.SourceSnapshot) {
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){ return 'mutated'; }\n"), 0o644); err != nil {
			t.Fatalf("mutate source: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("*.tmp\n"), 0o644); err != nil {
			t.Fatalf("mutate gitignore: %v", err)
		}
	}

	fn := app.App{Name: "snapshot-coherent", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	got, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if got.Fingerprint != wantFP {
		t.Fatalf("Prepared.Fingerprint = %q, want the captured-snapshot digest %q", got.Fingerprint, wantFP)
	}
	if want := ImageRef(fn.Name, wantFP); got.Image != want {
		t.Fatalf("Prepared.Image = %q, want %q (tag must be the snapshot digest)", got.Image, want)
	}

	staged := stagedContextContents(t, contextTar)
	if got := staged["index.js"]; got != string(original) {
		t.Fatalf("staged index.js = %q, want the captured original %q (the mutation must not leak into the image)", got, original)
	}
	if _, ok := staged[".gitignore"]; ok {
		t.Fatalf("a .gitignore created after the snapshot must not be staged, got %v", staged)
	}
	// The runtime-generated plan file (the Node bootstrap) is written into the
	// context SEPARATELY, after the snapshot, and template.yaml never enters it.
	if _, ok := staged["relay/bootstrap.mjs"]; !ok {
		t.Fatalf("generated bootstrap must be staged into the context, got %v", staged)
	}
	if _, ok := staged["template.yaml"]; ok {
		t.Fatalf("template.yaml must never enter the build context, got %v", staged)
	}
}

// TestPrepareSnapshotFailureRejectsBuild proves a source that cannot be fully
// captured never builds under an unverified identity: an unreadable selected file
// fails Prepare before any ImageBuild is issued, and no empty-fingerprint
// live-copy fallback exists.
func TestPrepareSnapshotFailureRejectsBuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.Chmod(filepath.Join(dir, "index.js"), 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "index.js"), 0o644) })

	builds := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodPost, path: "/build", body: `{}`, onMatch: func() { builds++ }},
	)
	m := newLifecycleManager(t, cli, context.Background())

	fn := app.App{Name: "unreadable", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("Prepare must fail when a selected file cannot be captured")
	}
	if builds != 0 {
		t.Fatalf("ImageBuild issued %d times for an uncapturable source, want 0", builds)
	}
}

// TestPrepareMutationDuringBlockedBuildKeepsCapturedIdentity is the deterministic
// "edit during a long build" regression. The build is blocked on the daemon
// request AFTER the snapshot was captured and the context tarred, the source is
// edited on disk while it is blocked, and only then is the build allowed to
// finish. The built identity and the tag must remain the digest of the bytes the
// build actually staged (the captured snapshot), never the edited tree: a
// post-build rescan of the live tree differs, which is exactly why callers must
// persist the RETURNED Prepared.Fingerprint rather than recomputing one.
//
// There are no sleeps: the mutation happens while the scripted ImageBuild request
// is blocked, and the build is released only after it lands.
func TestPrepareMutationDuringBlockedBuildKeepsCapturedIdentity(t *testing.T) {
	dir := t.TempDir()
	original := []byte("export function h(){ return 'original'; }\n")
	if err := os.WriteFile(filepath.Join(dir, "index.js"), original, 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	// Reference digest over the original selected source: what the capture held.
	reference, err := app.CaptureSourceSnapshot(mustSelect(t, dir))
	if err != nil {
		t.Fatalf("capture reference snapshot: %v", err)
	}
	wantFP := reference.Fingerprint()
	reference.Discard()

	// The build is held here until the mutation has landed, so the edit is
	// guaranteed to be on disk WHILE the daemon build is in flight.
	release := make(chan struct{})
	entered := make(chan struct{})
	var contextTar []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{}`,
			onBody: func(b []byte) { contextTar = append([]byte(nil), b...) },
			fail: func(req *http.Request) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-req.Context().Done():
					return req.Context().Err()
				}
			},
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	fn := app.App{Name: "blocked-build", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	type result struct {
		prepared *Prepared
		err      error
	}
	done := make(chan result, 1)
	go func() {
		p, err := m.Prepare(context.Background(), fn)
		done <- result{prepared: p, err: err}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ImageBuild was not entered")
	}

	// Mutate the tracked source while the build is blocked.
	mutated := []byte("export function h(){ return 'mutated'; }\n")
	if err := os.WriteFile(filepath.Join(dir, "index.js"), mutated, 0o644); err != nil {
		t.Fatalf("mutate source: %v", err)
	}
	// The live tree now hashes differently: a post-build rescan would produce
	// this value, which is NOT what the in-flight build staged.
	onDisk, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("on-disk fingerprint: %v", err)
	}
	if onDisk == wantFP {
		t.Fatal("test setup: the on-disk digest must differ from the captured digest")
	}

	close(release)
	res := <-done
	if res.err != nil {
		t.Fatalf("prepare: %v", res.err)
	}
	got := res.prepared
	if got.Fingerprint != wantFP {
		t.Fatalf("Prepared.Fingerprint = %q, want the captured digest %q (not the mutated on-disk %q)",
			got.Fingerprint, wantFP, onDisk)
	}
	if want := ImageRef(fn.Name, wantFP); got.Image != want {
		t.Fatalf("Prepared.Image = %q, want %q (the tag must be the captured digest)", got.Image, want)
	}
	staged := stagedContextContents(t, contextTar)
	if staged["index.js"] != string(original) {
		t.Fatalf("staged index.js = %q, want the captured original %q", staged["index.js"], original)
	}
}

// TestPrepareReleasesSnapshotOnSuccessAndFailure pins that the preparation
// releases the captured source bytes on every path: the snapshot retained via the
// afterSourceSnapshot seam has its entries cleared once Prepare returns, whether
// the build succeeded or failed. The deferred Discard is what keeps a long-lived
// process from retaining every prepared app's source in memory.
func TestPrepareReleasesSnapshotOnSuccessAndFailure(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "success", body: `{}`},
		{name: "failure", body: `{"errorDetail":{"message":"boom"}}`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
				t.Fatalf("write source: %v", err)
			}
			cli := newScriptedDockerClient(t,
				dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
				dockerRoute{method: http.MethodPost, path: "/build", body: tc.body},
			)
			m := newLifecycleManager(t, cli, context.Background())

			var captured *app.SourceSnapshot
			var capturedRoot string
			m.afterSourceSnapshot = func(s *app.SourceSnapshot) {
				captured = s
				capturedRoot = s.Root()
			}

			fn := app.App{Name: "release-" + tc.name, Dir: dir, Template: &app.Template{Runtime: "node24"}}
			_, err := m.Prepare(context.Background(), fn)
			if tc.wantErr && err == nil {
				t.Fatal("expected the scripted build failure to surface")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if captured == nil {
				t.Fatal("the snapshot seam did not fire; nothing to assert")
			}
			// Discard clears both the entry slice and each captured byte slice, so
			// no entries remaining is the observable release on this pointer.
			if n := len(captured.Entries()); n != 0 {
				t.Fatalf("%d snapshot entries retained after Prepare returned; the deferred Discard must release them", n)
			}
			if capturedRoot == "" {
				t.Fatal("test setup: the captured snapshot had no root")
			}
			if _, err := os.Stat(capturedRoot); !os.IsNotExist(err) {
				t.Fatalf("snapshot root %q retained after Prepare returned; the deferred Discard must remove it", capturedRoot)
			}
		})
	}
}

// TestPrepareReleasesSnapshotOnCancellation pins the cancellation half: when the
// build is cancelled mid-flight, the deferred Discard still releases the captured
// source. The snapshot is retained through the seam and checked after Prepare
// returns, so the assertion covers the actual cancellation path (not a synthetic
// Discard call).
func TestPrepareReleasesSnapshotOnCancellation(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	entered := make(chan struct{})
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build",
			onRequest: func(*http.Request) { close(entered) },
			fail: func(req *http.Request) error {
				<-req.Context().Done()
				return req.Context().Err()
			},
		},
	)
	m := newLifecycleManager(t, cli, lifecycle)

	var captured *app.SourceSnapshot
	var capturedRoot string
	m.afterSourceSnapshot = func(s *app.SourceSnapshot) {
		captured = s
		capturedRoot = s.Root()
	}

	fn := app.App{Name: "release-cancel", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	done := make(chan error, 1)
	go func() {
		_, err := m.Prepare(context.Background(), fn)
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ImageBuild was not entered")
	}
	cancelLifecycle()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("build error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the active build promptly")
	}
	if captured == nil {
		t.Fatal("the snapshot seam did not fire; nothing to assert")
	}
	// Discard clears both the entry slice and each captured byte slice, so no
	// entries remaining is the observable release on this pointer.
	if n := len(captured.Entries()); n != 0 {
		t.Fatalf("%d snapshot entries retained after a cancelled Prepare; the deferred Discard must release them", n)
	}
	if capturedRoot == "" {
		t.Fatal("test setup: the captured snapshot had no root")
	}
	if _, err := os.Stat(capturedRoot); !os.IsNotExist(err) {
		t.Fatalf("snapshot root %q retained after a cancelled Prepare; the deferred Discard must remove it", capturedRoot)
	}
}
