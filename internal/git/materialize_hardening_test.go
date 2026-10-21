package git

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
)

// hardeningTemplate is a parseable template carrying marker as a comment, so a
// test can distinguish generations with the real app loader.
func hardeningTemplate(marker string) string {
	return "runtime: node24\n# " + marker +
		"\nevents:\n  - handler: h.handler\n    pattern:\n      event_name: [INSERT]\n"
}

// requireAtomicExchange skips a test when the temp filesystem cannot swap two
// directory entries. update tests assert the production primitive's real
// behavior, so they are meaningful only where the platform supports it.
func requireAtomicExchange(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	for _, path := range []string{a, b} {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatalf("mkdir exchange probe %q: %v", path, err)
		}
	}
	if err := platformExchange(a, b); err != nil {
		t.Skipf("filesystem has no atomic directory exchange: %v", err)
	}
}

// swapExchange replaces the exchange seam for one test and restores it after.
// The package var is a test seam only; no test in this package calls t.Parallel.
func swapExchange(t *testing.T, fn func(a, b string) error) {
	t.Helper()
	previous := exchangeDirs
	exchangeDirs = fn
	t.Cleanup(func() { exchangeDirs = previous })
}

// mustReadFile reads path and fails the test when it is absent or unreadable.
func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	return string(b)
}

// assertAppNotRemoved mirrors the reconciler's reload decision: a missing
// directory (fs.ErrNotExist) or a missing template (app.ErrNotReady) removes a
// known app, so either outcome during an update would be an observable removal.
func assertAppNotRemoved(t *testing.T, root, name string) {
	t.Helper()
	if _, err := app.LoadSingle(root, name); err != nil {
		if errors.Is(err, os.ErrNotExist) ||
			errors.Is(err, app.ErrNotReady) {
			t.Fatalf("reconciler would remove app %q during update: %v",
				name, err)
		}
		t.Fatalf("load app %q during update: %v", name, err)
	}
}

// assertNoReservedEntries fails when any Relay-owned staging entry remains
// under dir. A successful or cleanly failed materialize owns all of its stages.
func assertNoReservedEntries(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("read apps dir %q: %v", dir, err)
	}
	for _, entry := range entries {
		if app.IsReservedDir(entry.Name()) {
			t.Fatalf("reserved staging entry %q left in %q", entry.Name(), dir)
		}
	}
}

// TestMaterializeUpdateNeverExposesAbsence pins the core hardening property for
// replacing an existing app: the live name resolves to a complete old or new
// tree on both sides of the atomic exchange, never to a missing path, a missing
// template, or a partial copy. The reconciler's own LoadSingle removal test is
// applied at the exchange barrier, so a remove-then-rename regression cannot
// pass merely by leaving the helper functions intact.
func TestMaterializeUpdateNeverExposesAbsence(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		hardeningTemplate("new-alpha"))
	writeFile(t, filepath.Join(src, "alpha", "handler.js"),
		"export function newHandler(){}\n")
	writeFile(t, filepath.Join(src, "alpha", "new-only.txt"), "new only\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		hardeningTemplate("old-alpha"))
	writeFile(t, filepath.Join(apps, "alpha", "handler.js"),
		"export function oldHandler(){}\n")
	writeFile(t, filepath.Join(apps, "alpha", "old-only.txt"), "old only\n")

	liveExchanges := 0
	swapExchange(t, func(a, b string) error {
		if app.IsReservedDir(filepath.Base(b)) {
			return platformExchange(a, b)
		}
		// Before the swap the live name must hold the complete old tree.
		assertAppNotRemoved(t, apps, "alpha")
		if got := mustReadFile(t,
			filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "old-alpha") {
			t.Fatalf("template before exchange = %q, want old generation", got)
		}
		if got := mustReadFile(t,
			filepath.Join(apps, "alpha", "handler.js")); !strings.Contains(got, "oldHandler") {
			t.Fatalf("handler before exchange = %q, want old generation", got)
		}

		if err := platformExchange(a, b); err != nil {
			return err
		}

		// After the swap the same live name must hold the complete new tree,
		// while the staging path holds the complete old tree for disposal.
		assertAppNotRemoved(t, apps, "alpha")
		if got := mustReadFile(t,
			filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "new-alpha") {
			t.Fatalf("template after exchange = %q, want new generation", got)
		}
		if got := mustReadFile(t,
			filepath.Join(apps, "alpha", "new-only.txt")); got != "new only\n" {
			t.Fatalf("new-only.txt after exchange = %q", got)
		}
		if _, err := os.Stat(filepath.Join(apps, "alpha", "old-only.txt")); !os.IsNotExist(err) {
			t.Fatalf("old-only.txt still under the live name after exchange: %v", err)
		}
		if got := mustReadFile(t,
			filepath.Join(a, "old-only.txt")); got != "old only\n" {
			t.Fatalf("staged old tree lost old-only.txt: %q", got)
		}
		liveExchanges++
		return nil
	})

	mat, rem, err := materialize(mustSelect(t, src, src), apps)
	if err != nil {
		t.Fatalf("materialize update: %v", err)
	}
	if len(mat) != 1 || mat[0] != "alpha" {
		t.Fatalf("materialized = %v, want [alpha]", mat)
	}
	if len(rem) != 0 {
		t.Fatalf("removed = %v, want none", rem)
	}
	if liveExchanges != 1 {
		t.Fatalf("live exchanges = %d, want exactly one atomic replacement",
			liveExchanges)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "new-alpha") {
		t.Fatalf("final template = %q, want new generation", got)
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeAddsNewAppAndRemovesAbsentSourceApp pins the unchanged
// per-name behavior around the hardening: an app absent from /apps is added
// with a plain atomic rename (no exchange needed), and a live directory absent
// from the source is genuinely removed. The real loader must see exactly the
// source set afterwards.
func TestMaterializeAddsNewAppAndRemovesAbsentSourceApp(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "newapp", "template.yaml"),
		hardeningTemplate("newapp"))

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "stale", "template.yaml"),
		"runtime: node24\n# stale\n")
	writeFile(t, filepath.Join(apps, "stale", "old.txt"), "old\n")

	mat, rem, err := materialize(mustSelect(t, src, src), apps)
	if err != nil {
		t.Fatalf("materialize add/remove: %v", err)
	}
	if len(mat) != 1 || mat[0] != "newapp" {
		t.Fatalf("materialized = %v, want [newapp]", mat)
	}
	if len(rem) != 1 || rem[0] != "stale" {
		t.Fatalf("removed = %v, want [stale]", rem)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "newapp", "template.yaml")); !strings.Contains(got, "node24") {
		t.Fatalf("new app template = %q", got)
	}
	if _, err := os.Stat(filepath.Join(apps, "stale")); !os.IsNotExist(err) {
		t.Fatalf("genuinely removed source app still live: %v", err)
	}

	loaded, err := app.NewLoader(apps, testLog()).Load()
	if err != nil {
		t.Fatalf("load materialized apps: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Name != "newapp" {
		t.Fatalf("loaded apps = %+v, want [newapp]", loaded)
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeStagingFailureLeavesLiveAppsUntouched proves every candidate
// is staged before the first live mutation. The second app's stage fails after
// the first app is already staged and validated, yet /apps keeps both its
// existing app bytes and its stale directory; no exchange is even probed.
//
// The injected failure is a directory named .gitignore created in the second
// source app AFTER the selection policy is resolved. Copying it succeeds, but
// preserving the local ignore policy then tries to read a directory, which
// fails deterministically even when tests run as root.
func TestMaterializeStagingFailureLeavesLiveAppsUntouched(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, ".gitignore"), "*.ignored\n")
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		"runtime: node24\n# new-alpha\n")
	writeFile(t, filepath.Join(src, "zeta", "template.yaml"),
		"runtime: node24\n# zeta\n")
	selection := mustSelect(t, src, src)
	if err := os.MkdirAll(filepath.Join(src, "zeta", ".gitignore"), 0o755); err != nil {
		t.Fatalf("seed non-file .gitignore: %v", err)
	}

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		"runtime: node24\n# old-alpha\n")
	writeFile(t, filepath.Join(apps, "alpha", "old-only.txt"), "old only\n")
	writeFile(t, filepath.Join(apps, "stale", "template.yaml"),
		"runtime: node24\n# stale\n")

	exchanges := 0
	swapExchange(t, func(a, b string) error {
		exchanges++
		return platformExchange(a, b)
	})

	_, _, err := materialize(selection, apps)
	if err == nil || !strings.Contains(err.Error(), "read local ignore file") {
		t.Fatalf("materialize err = %v, want injected staging failure", err)
	}
	if exchanges != 0 {
		t.Fatalf("exchange attempts = %d, want 0 before all stages succeed",
			exchanges)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "old-alpha") {
		t.Fatalf("existing app template changed after staging failure: %q", got)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "old-only.txt")); got != "old only\n" {
		t.Fatalf("existing app bytes changed after staging failure: %q", got)
	}
	if _, err := os.Stat(filepath.Join(apps, "alpha", "new-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("staged content leaked into the live app: %v", err)
	}
	if _, err := os.Stat(filepath.Join(apps, "zeta")); !os.IsNotExist(err) {
		t.Fatalf("partially staged app became live: %v", err)
	}
	if _, err := os.Stat(filepath.Join(apps, "stale")); err != nil {
		t.Fatalf("stale app removed by a failed staging pass: %v", err)
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeUnsupportedExchangeFailsBeforeMutation pins fail-closed
// behavior: if the filesystem cannot exchange directories, the capability probe
// fails while staging names are the only mutation targets. There is no
// remove-then-rename fallback, so the existing app, the stale app, and the
// staging namespace all stay exactly as they were.
func TestMaterializeUnsupportedExchangeFailsBeforeMutation(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		"runtime: node24\n# new-alpha\n")
	writeFile(t, filepath.Join(src, "added", "template.yaml"),
		"runtime: node24\n# added\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		"runtime: node24\n# old-alpha\n")
	writeFile(t, filepath.Join(apps, "alpha", "old-only.txt"), "old only\n")
	writeFile(t, filepath.Join(apps, "stale", "template.yaml"),
		"runtime: node24\n# stale\n")

	unsupported := errors.New("injected unsupported exchange")
	var targets []string
	swapExchange(t, func(a, b string) error {
		targets = append(targets, filepath.Base(b))
		return unsupported
	})

	mat, rem, err := materialize(mustSelect(t, src, src), apps)
	if !errors.Is(err, unsupported) {
		t.Fatalf("materialize err = %v, want injected unsupported exchange", err)
	}
	if !strings.Contains(err.Error(), "does not support atomic directory exchange") {
		t.Fatalf("err = %v, want unsupported-exchange explanation", err)
	}
	if len(mat) != 0 || len(rem) != 0 {
		t.Fatalf("materialized/removed = %v/%v, want none on probe failure",
			mat, rem)
	}
	if len(targets) != 1 || !app.IsReservedDir(targets[0]) {
		t.Fatalf("exchange targets = %v, want only the reserved probe", targets)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "old-alpha") {
		t.Fatalf("live app changed on unsupported exchange: %q", got)
	}
	if _, err := os.Stat(filepath.Join(apps, "alpha", "old-only.txt")); err != nil {
		t.Fatalf("live app bytes changed on unsupported exchange: %v", err)
	}
	if _, err := os.Stat(filepath.Join(apps, "added")); !os.IsNotExist(err) {
		t.Fatalf("new app committed before exchange support was proven: %v", err)
	}
	if _, err := os.Stat(filepath.Join(apps, "stale")); err != nil {
		t.Fatalf("stale app removed before exchange support was proven: %v", err)
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeCommitFailureRollsBackPreviousSwap pins reverse-order
// rollback: alpha's exchange commits first, beta's commit fails, and the old
// alpha tree is swapped back from its still-staged backup before the error is
// returned.
func TestMaterializeCommitFailureRollsBackPreviousSwap(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		"runtime: node24\n# new-alpha\n")
	writeFile(t, filepath.Join(src, "alpha", "new-only.txt"), "new\n")
	writeFile(t, filepath.Join(src, "beta", "template.yaml"),
		"runtime: node24\n# new-beta\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		"runtime: node24\n# old-alpha\n")
	writeFile(t, filepath.Join(apps, "alpha", "old-only.txt"), "old\n")
	writeFile(t, filepath.Join(apps, "beta", "template.yaml"),
		"runtime: node24\n# old-beta\n")
	writeFile(t, filepath.Join(apps, "beta", "old-only.txt"), "old\n")

	commitFailure := errors.New("injected commit failure")
	var liveTargets []string
	swapExchange(t, func(a, b string) error {
		target := filepath.Base(b)
		if app.IsReservedDir(target) {
			return platformExchange(a, b)
		}
		liveTargets = append(liveTargets, target)
		if target == "beta" {
			return commitFailure
		}
		return platformExchange(a, b)
	})

	mat, rem, err := materialize(mustSelect(t, src, src), apps)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("materialize err = %v, want injected commit failure", err)
	}
	if !strings.Contains(err.Error(), `commit app dir "beta"`) {
		t.Fatalf("err = %v, want beta commit failure", err)
	}
	if len(mat) != 0 || len(rem) != 0 {
		t.Fatalf("materialized/removed = %v/%v, want none after rollback",
			mat, rem)
	}
	wantTargets := []string{"alpha", "beta", "alpha"}
	if len(liveTargets) != len(wantTargets) {
		t.Fatalf("live exchange targets = %v, want %v", liveTargets, wantTargets)
	}
	for i, want := range wantTargets {
		if liveTargets[i] != want {
			t.Fatalf("live exchange targets = %v, want %v", liveTargets, wantTargets)
		}
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "old-alpha") {
		t.Fatalf("alpha not rolled back: %q", got)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "old-only.txt")); got != "old\n" {
		t.Fatalf("alpha old bytes not restored: %q", got)
	}
	if _, err := os.Stat(filepath.Join(apps, "alpha", "new-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("alpha rollback left new content live: %v", err)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "beta", "template.yaml")); !strings.Contains(got, "old-beta") {
		t.Fatalf("beta changed despite failed commit: %q", got)
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeCommitFailureRollsBackNewApp pins the new-name half of
// rollback: a newly added app committed before a later failure is renamed back
// out of the live namespace, so an aborted sync leaves no new app behind.
func TestMaterializeCommitFailureRollsBackNewApp(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		"runtime: node24\n# new-alpha\n")
	writeFile(t, filepath.Join(src, "beta", "template.yaml"),
		"runtime: node24\n# new-beta\n")
	writeFile(t, filepath.Join(src, "gamma", "template.yaml"),
		"runtime: node24\n# new-gamma\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		"runtime: node24\n# old-alpha\n")
	writeFile(t, filepath.Join(apps, "gamma", "template.yaml"),
		"runtime: node24\n# old-gamma\n")

	commitFailure := errors.New("injected gamma commit failure")
	var liveTargets []string
	swapExchange(t, func(a, b string) error {
		target := filepath.Base(b)
		if app.IsReservedDir(target) {
			return platformExchange(a, b)
		}
		liveTargets = append(liveTargets, target)
		if target == "gamma" {
			return commitFailure
		}
		return platformExchange(a, b)
	})

	_, _, err := materialize(mustSelect(t, src, src), apps)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("materialize err = %v, want injected gamma commit failure", err)
	}
	if _, err := os.Stat(filepath.Join(apps, "beta")); !os.IsNotExist(err) {
		t.Fatalf("new app beta survived rollback: %v", err)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "old-alpha") {
		t.Fatalf("alpha not rolled back: %q", got)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "gamma", "template.yaml")); !strings.Contains(got, "old-gamma") {
		t.Fatalf("gamma changed despite failed commit: %q", got)
	}
	wantTargets := []string{"alpha", "gamma", "alpha"}
	if len(liveTargets) != len(wantTargets) {
		t.Fatalf("live exchange targets = %v, want %v", liveTargets, wantTargets)
	}
	for i, want := range wantTargets {
		if liveTargets[i] != want {
			t.Fatalf("live exchange targets = %v, want %v", liveTargets, wantTargets)
		}
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeRollbackFailurePreservesOldTreeBackup pins the recovery
// contract when rollback itself fails: the aborted update reports both the
// original commit failure and the rollback failure, the live app remains a
// complete new tree, and the only old snapshot is left in a reserved .sync-*
// path after deferred cleanup instead of being deleted.
func TestMaterializeRollbackFailurePreservesOldTreeBackup(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		hardeningTemplate("new-alpha"))
	writeFile(t, filepath.Join(src, "alpha", "new-only.txt"), "new only\n")
	writeFile(t, filepath.Join(src, "beta", "template.yaml"),
		"runtime: node24\n# new-beta\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		hardeningTemplate("old-alpha"))
	writeFile(t, filepath.Join(apps, "alpha", "old-only.txt"), "old only\n")
	writeFile(t, filepath.Join(apps, "beta", "template.yaml"),
		"runtime: node24\n# old-beta\n")

	commitFailure := errors.New("injected beta commit failure")
	rollbackFailure := errors.New("injected alpha rollback failure")
	var liveTargets []string
	var alphaStage string
	alphaExchanges := 0
	swapExchange(t, func(a, b string) error {
		target := filepath.Base(b)
		// The capability probe uses reserved names and must succeed.
		if app.IsReservedDir(target) {
			return platformExchange(a, b)
		}
		liveTargets = append(liveTargets, target)
		switch target {
		case "alpha":
			alphaExchanges++
			// First alpha exchange commits the update; the second is the
			// rollback and must fail, leaving the old tree at a.
			if alphaExchanges == 1 {
				alphaStage = a
				return platformExchange(a, b)
			}
			return rollbackFailure
		case "beta":
			return commitFailure
		default:
			t.Fatalf("unexpected live exchange target %q", target)
			return nil
		}
	})

	_, _, err := materialize(mustSelect(t, src, src), apps)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("materialize err = %v, want beta commit failure", err)
	}
	if !errors.Is(err, rollbackFailure) {
		t.Fatalf("materialize err = %v, want alpha rollback failure", err)
	}
	for _, want := range []string{
		"materialization rollback",
		`restore app "alpha"`,
		"injected alpha rollback failure",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("materialize err = %v, want %q", err, want)
		}
	}

	wantTargets := []string{"alpha", "beta", "alpha"}
	if len(liveTargets) != len(wantTargets) {
		t.Fatalf("live exchange targets = %v, want %v", liveTargets, wantTargets)
	}
	for i, want := range wantTargets {
		if liveTargets[i] != want {
			t.Fatalf("live exchange targets = %v, want %v", liveTargets, wantTargets)
		}
	}

	// The live name still resolves to a complete, loadable new tree.
	assertAppNotRemoved(t, apps, "alpha")
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "template.yaml")); !strings.Contains(got, "new-alpha") {
		t.Fatalf("live alpha template = %q, want complete new generation", got)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "new-only.txt")); got != "new only\n" {
		t.Fatalf("live alpha new-only.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(apps, "alpha", "old-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("live alpha still mixes old content: %v", err)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "beta", "template.yaml")); !strings.Contains(got, "old-beta") {
		t.Fatalf("beta changed despite failed commit: %q", got)
	}

	// Exactly the failed rollback's old tree survives, in the reserved
	// namespace, with its bytes intact after materialize's deferred cleanup.
	if alphaStage == "" {
		t.Fatal("alpha commit exchange was not observed")
	}
	if !app.IsReservedDir(filepath.Base(alphaStage)) {
		t.Fatalf("preserved alpha backup %q is not in the reserved namespace",
			alphaStage)
	}
	if got := mustReadFile(t,
		filepath.Join(alphaStage, "template.yaml")); !strings.Contains(got, "old-alpha") {
		t.Fatalf("preserved alpha backup template = %q, want old generation", got)
	}
	if got := mustReadFile(t,
		filepath.Join(alphaStage, "old-only.txt")); got != "old only\n" {
		t.Fatalf("preserved alpha backup old-only.txt = %q", got)
	}

	var reserved []string
	entries, readErr := os.ReadDir(apps)
	if readErr != nil {
		t.Fatalf("read apps dir: %v", readErr)
	}
	for _, entry := range entries {
		if app.IsReservedDir(entry.Name()) {
			reserved = append(reserved, filepath.Join(apps, entry.Name()))
		}
	}
	if len(reserved) != 1 || reserved[0] != alphaStage {
		t.Fatalf("reserved entries after failed rollback = %v, want [%s]",
			reserved, alphaStage)
	}
}

// TestRollbackRemovalsRestoresDetachedTree covers the removal rollback the
// materialize transaction needs after a later removal rename fails: a tree
// already detached into the reserved namespace is renamed back to its live name
// and the trash path disappears.
func TestRollbackRemovalsRestoresDetachedTree(t *testing.T) {
	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "gone", "template.yaml"),
		"runtime: node24\n# gone\n")

	detached, err := detachStaleApp(apps, "gone")
	if err != nil {
		t.Fatalf("detach stale app: %v", err)
	}
	if !app.IsReservedDir(filepath.Base(detached.trashPath)) {
		t.Fatalf("trash path %q is not in the reserved namespace",
			detached.trashPath)
	}
	if _, err := os.Stat(filepath.Join(apps, "gone")); !os.IsNotExist(err) {
		t.Fatalf("detached app still has its live name: %v", err)
	}

	if err := rollbackRemovals([]stagedRemoval{detached}, apps); err != nil {
		t.Fatalf("rollbackRemovals: %v", err)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "gone", "template.yaml")); !strings.Contains(got, "# gone") {
		t.Fatalf("restored app content = %q", got)
	}
	if _, err := os.Stat(detached.trashPath); !os.IsNotExist(err) {
		t.Fatalf("detached trash path survived restoration: %v", err)
	}
}

// TestRollbackRemovalsRefusesOccupiedLiveName pins the no-overwrite guard: a
// name occupied by a foreign writer is reported, the live tree is untouched,
// and the detached old tree remains available instead of being silently lost.
func TestRollbackRemovalsRefusesOccupiedLiveName(t *testing.T) {
	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "gone", "template.yaml"),
		"runtime: node24\n# old gone\n")
	detached, err := detachStaleApp(apps, "gone")
	if err != nil {
		t.Fatalf("detach stale app: %v", err)
	}
	writeFile(t, filepath.Join(apps, "gone", "foreign.txt"), "foreign\n")

	err = rollbackRemovals([]stagedRemoval{detached}, apps)
	if err == nil || !strings.Contains(err.Error(), "target already exists") {
		t.Fatalf("rollbackRemovals err = %v, want occupied-target error", err)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "gone", "foreign.txt")); got != "foreign\n" {
		t.Fatalf("occupied live tree overwritten: %q", got)
	}
	if got := mustReadFile(t,
		filepath.Join(detached.trashPath, "template.yaml")); !strings.Contains(got, "# old gone") {
		t.Fatalf("detached old tree lost: %q", got)
	}
}

// TestClearStaleStagesRemovesOnlyReservedEntries pins the cleanup boundary:
// every Relay-owned leftover (directory or file) is removed, while a live app
// dir is never touched. A missing apps root is normal.
func TestClearStaleStagesRemovesOnlyReservedEntries(t *testing.T) {
	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "keep", "template.yaml"),
		"runtime: node24\n")
	writeFile(t, filepath.Join(apps, "keep", "handler.js"), "export const x=1\n")
	writeFile(t, filepath.Join(apps, ".sync-orphan", "template.yaml"),
		"runtime: node24\n")
	writeFile(t, filepath.Join(apps, ".sync-removed-dead", "partial.txt"),
		"partial\n")
	writeFile(t, filepath.Join(apps, ".sync-leftover-file"), "junk\n")

	if err := clearStaleStages(apps); err != nil {
		t.Fatalf("clearStaleStages: %v", err)
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "keep", "template.yaml")); !strings.Contains(got, "node24") {
		t.Fatalf("live app content changed: %q", got)
	}
	if _, err := os.Stat(filepath.Join(apps, "keep", "handler.js")); err != nil {
		t.Fatalf("live app file removed: %v", err)
	}
	assertNoReservedEntries(t, apps)

	missing := filepath.Join(apps, "does-not-exist")
	if err := clearStaleStages(missing); err != nil {
		t.Fatalf("clearStaleStages(missing) = %v, want nil", err)
	}
}

// TestSyncReclaimsStaleStagesBeforeMaterializing pins the Sync ordering: a
// leftover from an interrupted sync is removed by the next sync (under its
// writer lock), and the current source is still materialized.
func TestSyncReclaimsStaleStagesBeforeMaterializing(t *testing.T) {
	e := fixture(t, false)
	writeFile(t, filepath.Join(e.apps, ".sync-interrupted", "template.yaml"),
		"runtime: node24\n")
	writeFile(t, filepath.Join(e.apps, ".sync-interrupted", "partial.txt"),
		"partial\n")

	mustSync(t, e, Config{Repository: "git@github.com:acme/r.git", Ref: "main"})

	if _, err := os.Stat(filepath.Join(e.apps, ".sync-interrupted")); !os.IsNotExist(err) {
		t.Fatalf("stale stage survived sync: %v", err)
	}
	if _, err := os.Stat(filepath.Join(e.apps, "fn", "template.yaml")); err != nil {
		t.Fatalf("source app not materialized: %v", err)
	}
	assertNoReservedEntries(t, e.apps)
}
