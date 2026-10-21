package git

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"relay/internal/app"
	"relay/internal/source"
)

// exchangeDirs atomically swaps two directory entries on the same filesystem.
// It is a variable only so tests can exercise the unsupported-exchange and
// commit-failure paths; production always resolves to platformExchange (see the
// exchange_<platform>.go files).
var exchangeDirs = platformExchange

// discoverApps returns the sorted set of app directories under the selected
// source: each direct subdirectory with an app.ValidName name, an included
// template.yaml path, and a stat-able template.yaml. It selects by presence,
// not template contents; parsing and schema validation belong to the
// reconciler. Subdirectories without a template.yaml, files, and directories
// failing app.ValidName are ignored. An ignored directory is skipped just as
// the loader would (git never descends into it), so an app directory excluded
// by .gitignore is not materialized.
func discoverApps(selection *source.Selection) ([]string, error) {
	entries, err := os.ReadDir(selection.Dir())
	if err != nil {
		// A missing source dir means the validated monorepo path (or the whole
		// checkout) is absent from the checked-out ref. That is a hard sync
		// failure: silently producing an empty /apps would look like a
		// successful "there are no apps yet" sync and could erase valid
		// materialized apps through the deterministic-removal rule.
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("git: source dir %q does not exist in the checkout", selection.Dir())
		}
		return nil, fmt.Errorf("git: read source dir %q: %w", selection.Dir(), err)
	}
	// A successfully read but empty directory is a valid, deliberate "no
	// apps here" source, NOT an error: materialize still runs the
	// deterministic-removal pass so stale dirs are cleared. We return the empty
	// slice (non-nil not required) and the caller distinguishes it from an error
	// purely by the nil err.
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if app.ValidName(name) != nil {
			continue
		}
		if !selection.IncludesPath(filepath.Join(selection.Dir(), name, "template.yaml"), false) {
			continue // excluded by the source-selection policy
		}
		if _, err := os.Stat(filepath.Join(selection.Dir(), name, "template.yaml")); err != nil {
			continue // no template.yaml, or unreadable: not an app
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// materialize is the package-internal compatibility entry point for
// materializeApps that returns only the added/updated and removed app names.
// The returned slices are sorted and nil on failure.
func materialize(selection *source.Selection, dstDir string) (materialized, removed []string, err error) {
	result, err := materializeApps(selection, dstDir)
	if err != nil {
		return nil, nil, err
	}
	return result.materialized, result.removed, nil
}

// materializeResult is one materialization pass's outcome.
type materializeResult struct {
	// desired is the number of app directories discovered in the source. It is
	// independent of how many needed replacement, so a no-op sync can still
	// report whether the source itself was empty.
	desired int
	// materialized lists the apps that were newly added or whose live tree
	// differed from the staged candidate and were therefore atomically installed
	// or replaced. Unchanged apps are deliberately absent. Sorted.
	materialized []string
	// removed lists live directories removed because the source no longer
	// contains them. Sorted.
	removed []string
}

// materializeApps rewrites dstDir (the /apps root) so that it contains exactly
// the app directories discovered under selection, recursively and
// deterministically:
//
//   - Every desired app is first copied into a Relay-owned staging directory
//     beside dstDir (app.StagingPrefix, e.g. ".sync-*", which discovery ignores).
//     No live app is touched until every copy and completeness check has
//     succeeded, so a staging/copy failure leaves the previous /apps tree
//     intact. This checks template.yaml presence, not template validity; the
//     reconciler handles template parsing and schema validation after commit.
//   - After all candidates are staged, each existing app's staged tree is
//     compared with its live tree (entries, directory modes, file modes, file
//     bytes, and therefore empty directories). An unchanged app is NOT replaced:
//     its live directory, inode, and watches stay exactly as they are. The
//     duplicate stage is discarded. Comparison errors fail the sync; they are
//     never treated as "unchanged". Only added/changed apps are reported as
//     materialized.
//   - Only then is each changed live app replaced by an atomic directory
//     exchange (renameat2(RENAME_EXCHANGE) on Linux, renamex_np(RENAME_SWAP) on
//     macOS). An updated app name is therefore never absent and never partially
//     copied. A newly added app is an atomic rename into an absent name. If a
//     changed existing app must be replaced and the filesystem cannot exchange
//     directories, materialize fails closed BEFORE any live mutation; there is
//     deliberately no remove-then-rename fallback. The exchange-support probe is
//     skipped entirely when no changed app already exists.
//   - The commit boundary is PER APP, not a whole-revision snapshot. /apps is a
//     fixed bind mount and is never renamed as a root, so a concurrent observer
//     may see a mixture of old and new apps while the per-app swaps proceed. An
//     app name always resolves to a complete tree, never a partially copied
//     directory; a reader whose walk spans a swap can still see files from both
//     generations, which the next reconcile's fingerprint handles. Per-app
//     atomicity is not whole-revision atomicity.
//   - Files excluded by the source-selection policy (the .gitignore rules
//     anchored at the checkout root) are NOT copied. The applicable .gitignore
//     files themselves ARE copied: they are the selection policy, so they must
//     travel with the app for the build context and fingerprint to keep
//     applying the same rules against /apps.
//   - Any pre-existing DIRECTORY in dstDir whose name is not in the new set is
//     removed, so /apps reflects exactly the configured source (the
//     deterministic-replace rule). Removal first renames the directory into the
//     reserved staging namespace, so its live name disappears atomically and a
//     crash during deletion cannot expose a partially deleted candidate. Only
//     directories are removed, never files, and nothing outside dstDir is ever
//     touched. A Relay-owned staging directory (app.IsReservedDir) is not
//     governed by this rule: the creator owns its lifecycle, so it is never
//     reported as removed nor deleted here.
//
// If a commit or removal fails after some live apps were already replaced,
// materialize rolls the replacements and removals back in reverse order from the
// still-staged old trees. A rollback failure is reported. An updated app's live
// name still holds a complete tree (the old or the new one), never a partial one;
// a removal whose restoration fails stays detached in the reserved namespace,
// never partially deleted under its live name. Crash behavior: a crash during
// staging leaves only ignored .sync-* directories;
// a crash during the per-app commit leaves each live app complete (old or new)
// and the next locked sync converges from the checkout. Stale .sync-* leftovers
// are removed by that next sync under the writer lock (see clearStaleStages).
//
// The caller must serialize writers (Sync does so with a process lock). No
// fingerprints are derived or persisted here: comparison is against the actual
// staged representation of this sync.
//
// Ownership note: once a git source is configured and synced, /apps is
// managed by git. Operator-placed app directories are subject to the same
// deterministic rule: a sync removes any directory not in the source. This is
// documented in docs/operations.md "Git" and in Sync's doc comment.
func materializeApps(selection *source.Selection, dstDir string) (materializeResult, error) {
	var result materializeResult
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return materializeResult{}, fmt.Errorf("git: create apps dir %q: %w", dstDir, err)
	}

	newSet, err := discoverApps(selection)
	if err != nil {
		return materializeResult{}, err
	}
	want := make(map[string]bool, len(newSet))
	for _, n := range newSet {
		want[n] = true
	}

	// Stage every desired app before mutating any live app. Each stagePath is
	// owned by this call; after a successful exchange it holds the previous live
	// tree, so the deferred cleanup doubles as old-tree disposal and crash-safe
	// failure cleanup. Unchanged apps keep their stage until this cleanup too.
	staged := make([]*stagedApp, 0, len(newSet))
	defer func() {
		for _, s := range staged {
			if s.stagePath != "" && !s.preserveStage {
				_ = os.RemoveAll(s.stagePath)
			}
		}
	}()
	for _, name := range newSet {
		fnSelection, err := selection.Sub(filepath.Join(selection.Dir(), name))
		if err != nil {
			return materializeResult{}, err
		}
		stagePath, err := stageAppDir(fnSelection, dstDir, name)
		if err != nil {
			return materializeResult{}, err
		}
		existed, err := pathExists(filepath.Join(dstDir, name))
		if err != nil {
			_ = os.RemoveAll(stagePath)
			return materializeResult{}, fmt.Errorf("git: inspect app dir %q: %w", name, err)
		}
		staged = append(staged, &stagedApp{name: name, stagePath: stagePath, existed: existed})
	}
	result.desired = len(staged)

	// Compare AFTER every candidate is staged and BEFORE any live mutation.
	// Unchanged live apps are left completely alone; only added/changed apps
	// proceed to the commit phase. A comparison error aborts the sync rather than
	// guessing "unchanged", because a wrong guess would silently leave stale
	// content live.
	changed := make([]*stagedApp, 0, len(staged))
	for _, s := range staged {
		if !s.existed {
			changed = append(changed, s)
			continue
		}
		equal, err := sameStagedTree(s.stagePath, filepath.Join(dstDir, s.name))
		if err != nil {
			return materializeResult{}, fmt.Errorf("git: compare staged app dir %q with live tree: %w", s.name, err)
		}
		if equal {
			continue
		}
		changed = append(changed, s)
	}

	// Determine the removal set BEFORE any live mutation, so a listing failure
	// cannot strand a half-applied revision. Reserved staging directories are
	// never removal candidates.
	stale, err := staleAppNames(dstDir, want)
	if err != nil {
		return materializeResult{}, err
	}

	// Replacement of an existing directory requires an atomic exchange. Probe
	// the capability now, while every live app is still untouched, so an
	// unsupported filesystem fails closed instead of exposing a removal window.
	// An all-new or all-unchanged desired set needs no exchange probe.
	for _, s := range changed {
		if s.existed {
			if err := checkExchangeSupport(dstDir); err != nil {
				return materializeResult{}, err
			}
			break
		}
	}

	// Commit phase: per-app atomic exchange/rename for changed apps only. The old
	// tree of an updated app remains at stagePath until the deferred cleanup,
	// which is what makes rollback possible without ever unlinking the live name.
	committed := make([]*stagedApp, 0, len(changed))
	for _, s := range changed {
		target := filepath.Join(dstDir, s.name)
		if s.existed {
			if err := exchangeDirs(s.stagePath, target); err != nil {
				rbErr := rollbackApps(committed, dstDir)
				return materializeResult{}, withRollback(fmt.Errorf("git: commit app dir %q: %w", s.name, err), rbErr)
			}
		} else {
			// The name was absent when staged. A plain rename is an atomic
			// creation; if a foreign writer created the name meanwhile, rename
			// fails (it never replaces a non-empty directory) and the
			// transaction rolls back rather than exposing a partial tree.
			if err := os.Rename(s.stagePath, target); err != nil {
				rbErr := rollbackApps(committed, dstDir)
				return materializeResult{}, withRollback(fmt.Errorf("git: commit app dir %q: %w", s.name, err), rbErr)
			}
			s.stagePath = "" // renamed into place: no old tree to clean up
		}
		committed = append(committed, s)
		result.materialized = append(result.materialized, s.name)
	}

	// Removal phase: first rename each stale directory into the reserved
	// namespace (an atomic disappearance of the live name), then delete the
	// detached trees. If a rename fails, restore every detached directory and
	// every committed app so /apps stays on the previous revision.
	removals := make([]stagedRemoval, 0, len(stale))
	for _, name := range stale {
		removal, err := detachStaleApp(dstDir, name)
		if err != nil {
			rbErr := errors.Join(rollbackRemovals(removals, dstDir), rollbackApps(committed, dstDir))
			return materializeResult{}, withRollback(fmt.Errorf("git: remove stale app dir %q: %w", name, err), rbErr)
		}
		removals = append(removals, removal)
		result.removed = append(result.removed, name)
	}
	sort.Strings(result.removed)

	// Deleting the detached trees is cleanup, not a visibility step: their live
	// names are already gone. A failure leaves only reserved .sync-* leftovers,
	// which the next locked sync reclaims (clearStaleStages).
	for _, r := range removals {
		_ = os.RemoveAll(r.trashPath)
	}
	return result, nil
}

// stagedApp is one desired app's staged replacement. stagePath is its staging
// directory before commit; after an atomic exchange it holds the previous live
// tree (the rollback source). existed records whether a live directory was
// present before commit.
type stagedApp struct {
	name          string
	stagePath     string
	existed       bool
	preserveStage bool
}

// stagedRemoval is a stale app directory atomically renamed into the reserved
// staging namespace, awaiting deletion.
type stagedRemoval struct {
	name      string
	trashPath string
}

// stageAppDir copies the whole tree at selection into a new reserved staging
// directory directly under dst, validates that the staged candidate is
// complete, and returns its path. Nothing live is touched. Files the
// source-selection policy excludes are not copied; the applicable .gitignore
// files are (they are the policy and must travel with the app). File modes are
// preserved.
func stageAppDir(selection *source.Selection, dst, name string) (string, error) {
	tmp, err := os.MkdirTemp(dst, app.StagingPrefix+"*")
	if err != nil {
		return "", fmt.Errorf("git: create temp dir: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(tmp)
		}
	}()

	if err := copyTree(selection, tmp); err != nil {
		return "", err
	}
	if err := preserveAncestorIgnoreFiles(selection, tmp); err != nil {
		return "", err
	}
	// A staged app must be a complete candidate: the same template.yaml
	// presence that made discovery accept it in the checkout. This catches a
	// partial copy before commit; template CONTENTS are still the reconciler's
	// concern (a present-but-invalid template follows its normal invalid-desired
	// path), so only presence is checked here.
	if _, err := os.Stat(filepath.Join(tmp, app.TemplateFile)); err != nil {
		return "", fmt.Errorf("git: staged app dir %q is incomplete: %w", name, err)
	}
	ok = true
	return tmp, nil
}

// sameStagedTree reports whether the live directory already has exactly the
// staged candidate's materialized representation. It compares the actual trees
// (not mtimes or a build fingerprint): every entry name and type, directory
// permissions, file permissions, and file bytes. Empty directories therefore
// participate naturally, and an extra/missing entry makes the trees differ.
//
// The staged tree is produced by copyTree, which dereferences nothing: it skips
// symlinks and special files. A live entry of such a type (or any type the stage
// does not contain) can never match the staged representation, so it is reported
// as changed and replaced by the atomic exchange. Any read/stat error is returned
// to the caller: comparison must fail the sync, never claim equality.
func sameStagedTree(staged, live string) (bool, error) {
	stagedInfo, err := os.Lstat(staged)
	if err != nil {
		return false, fmt.Errorf("stat staged tree %q: %w", staged, err)
	}
	liveInfo, err := os.Lstat(live)
	if err != nil {
		return false, fmt.Errorf("stat live tree %q: %w", live, err)
	}
	if !stagedInfo.IsDir() || !liveInfo.IsDir() {
		return false, nil
	}
	if stagedInfo.Mode().Perm() != liveInfo.Mode().Perm() {
		return false, nil
	}
	return sameDirContents(staged, live)
}

// sameDirContents compares two directories entry by entry. os.ReadDir returns
// entries sorted by name, so walking both in lockstep is deterministic and a
// differing name at the same index means the trees are not equal. It does not
// follow symlinks: entry metadata comes from the directory entry itself.
func sameDirContents(stagedDir, liveDir string) (bool, error) {
	stagedEntries, err := os.ReadDir(stagedDir)
	if err != nil {
		return false, fmt.Errorf("read staged dir %q: %w", stagedDir, err)
	}
	liveEntries, err := os.ReadDir(liveDir)
	if err != nil {
		return false, fmt.Errorf("read live dir %q: %w", liveDir, err)
	}
	if len(stagedEntries) != len(liveEntries) {
		return false, nil
	}
	for i, stagedEntry := range stagedEntries {
		liveEntry := liveEntries[i]
		if stagedEntry.Name() != liveEntry.Name() {
			return false, nil
		}
		equal, err := sameTreeEntry(
			filepath.Join(stagedDir, stagedEntry.Name()),
			filepath.Join(liveDir, liveEntry.Name()),
			stagedEntry,
			liveEntry,
		)
		if err != nil {
			return false, err
		}
		if !equal {
			return false, nil
		}
	}
	return true, nil
}

// sameTreeEntry compares one staged entry with the live entry of the same name.
// Directory modes and file modes are the permission bits copyTree/copyFile
// preserve; special mode bits are not part of the materialized representation.
func sameTreeEntry(stagedPath, livePath string, stagedEntry, liveEntry fs.DirEntry) (bool, error) {
	stagedInfo, err := stagedEntry.Info()
	if err != nil {
		return false, fmt.Errorf("stat staged entry %q: %w", stagedPath, err)
	}
	liveInfo, err := liveEntry.Info()
	if err != nil {
		return false, fmt.Errorf("stat live entry %q: %w", livePath, err)
	}
	switch {
	case stagedInfo.IsDir():
		if !liveInfo.IsDir() || stagedInfo.Mode().Perm() != liveInfo.Mode().Perm() {
			return false, nil
		}
		return sameDirContents(stagedPath, livePath)
	case stagedInfo.Mode().IsRegular():
		if !liveInfo.Mode().IsRegular() ||
			stagedInfo.Mode().Perm() != liveInfo.Mode().Perm() ||
			stagedInfo.Size() != liveInfo.Size() {
			return false, nil
		}
		return sameFileContents(stagedPath, livePath)
	default:
		// stageAppDir only creates directories and regular files (symlinks and
		// special files are skipped on copy), so no live entry can match this.
		return false, nil
	}
}

// sameFileContents streams two regular files and compares their bytes. Sizes are
// checked before the open, so a mismatch in length is already known; the stream
// comparison avoids loading whole app files into memory.
func sameFileContents(stagedPath, livePath string) (bool, error) {
	stagedFile, err := os.Open(stagedPath)
	if err != nil {
		return false, fmt.Errorf("open staged file %q: %w", stagedPath, err)
	}
	defer stagedFile.Close()
	liveFile, err := os.Open(livePath)
	if err != nil {
		return false, fmt.Errorf("open live file %q: %w", livePath, err)
	}
	defer liveFile.Close()

	stagedBuf := make([]byte, 64*1024)
	liveBuf := make([]byte, 64*1024)
	for {
		stagedN, stagedErr := io.ReadFull(stagedFile, stagedBuf)
		if stagedErr != nil && !errors.Is(stagedErr, io.EOF) && !errors.Is(stagedErr, io.ErrUnexpectedEOF) {
			return false, fmt.Errorf("read staged file %q: %w", stagedPath, stagedErr)
		}
		liveN, liveErr := io.ReadFull(liveFile, liveBuf)
		if liveErr != nil && !errors.Is(liveErr, io.EOF) && !errors.Is(liveErr, io.ErrUnexpectedEOF) {
			return false, fmt.Errorf("read live file %q: %w", livePath, liveErr)
		}
		if stagedN != liveN || !bytes.Equal(stagedBuf[:stagedN], liveBuf[:liveN]) {
			return false, nil
		}
		if errors.Is(stagedErr, io.EOF) || errors.Is(stagedErr, io.ErrUnexpectedEOF) {
			// Both reads returned the same number of bytes, so the live file is
			// at the same end position; the size check already proved no trailing
			// bytes.
			return true, nil
		}
	}
}

// staleAppNames returns the sorted names of real directories under dst that are
// not in want and are not Relay-owned staging directories. Only directory
// entries are considered; files and symlinks are left alone.
func staleAppNames(dstDir string, want map[string]bool) ([]string, error) {
	dstEntries, err := os.ReadDir(dstDir)
	if err != nil {
		return nil, fmt.Errorf("git: list apps dir %q: %w", dstDir, err)
	}
	var stale []string
	for _, e := range dstEntries {
		if !e.IsDir() {
			continue
		}
		if app.IsReservedDir(e.Name()) {
			// Relay-owned staging (git's ".sync-*"): the creator owns its
			// lifecycle. It is never reported as removed nor deleted here.
			continue
		}
		if want[e.Name()] {
			continue
		}
		stale = append(stale, e.Name())
	}
	sort.Strings(stale)
	return stale, nil
}

// checkExchangeSupport proves, before any live app is touched, that dstDir's
// filesystem supports the atomic two-directory exchange materialize needs to
// replace an existing app. The probe uses reserved staging names, which
// discovery ignores.
func checkExchangeSupport(dstDir string) error {
	a, err := os.MkdirTemp(dstDir, app.StagingPrefix+"probe-a-")
	if err != nil {
		return fmt.Errorf("git: create exchange probe: %w", err)
	}
	defer os.RemoveAll(a)
	b, err := os.MkdirTemp(dstDir, app.StagingPrefix+"probe-b-")
	if err != nil {
		return fmt.Errorf("git: create exchange probe: %w", err)
	}
	defer os.RemoveAll(b)
	if err := exchangeDirs(a, b); err != nil {
		return fmt.Errorf("git: /apps filesystem %q does not support atomic directory exchange: %w; "+
			"refusing to replace live apps non-atomically (Linux requires "+
			"renameat2(RENAME_EXCHANGE), macOS requires renamex_np(RENAME_SWAP))", dstDir, err)
	}
	return nil
}

// detachStaleApp atomically renames dst/name into a reserved staging path, so
// the live app name disappears in one step and no partial candidate is ever
// visible under it. The returned trashPath is then deleted by the caller.
func detachStaleApp(dstDir, name string) (stagedRemoval, error) {
	trashPath, err := newStagingPath(dstDir, "removed-")
	if err != nil {
		return stagedRemoval{}, err
	}
	if err := os.Rename(filepath.Join(dstDir, name), trashPath); err != nil {
		return stagedRemoval{}, err
	}
	return stagedRemoval{name: name, trashPath: trashPath}, nil
}

// newStagingPath reserves a unique path in the reserved staging namespace
// without leaving a directory there. The caller immediately renames something
// onto it, so the path is absent between the reservation and the rename.
func newStagingPath(dstDir, role string) (string, error) {
	dir, err := os.MkdirTemp(dstDir, app.StagingPrefix+role)
	if err != nil {
		return "", fmt.Errorf("git: create staging path: %w", err)
	}
	if err := os.Remove(dir); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("git: prepare staging path: %w", err)
	}
	return dir, nil
}

// pathExists reports whether path exists as any filesystem entry (including a
// symlink). Lstat is deliberate: the commit decision must not follow a link.
func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

// rollbackApps restores the apps committed so far, newest first. An updated app
// is swapped back with the same atomic exchange, leaving its new tree in the
// staging path for deferred cleanup; a newly created app is atomically renamed
// back out of the live namespace. A failed rollback is reported but never
// unlinks the live name.
func rollbackApps(committed []*stagedApp, dstDir string) error {
	var errs []error
	for i := len(committed) - 1; i >= 0; i-- {
		s := committed[i]
		target := filepath.Join(dstDir, s.name)
		if s.existed {
			if err := exchangeDirs(s.stagePath, target); err != nil {
				// stagePath still contains the previous app tree. Preserve it for
				// inspection/recovery rather than letting materialize's cleanup
				// destroy the only remaining snapshot.
				s.preserveStage = true
				errs = append(errs, fmt.Errorf("restore app %q: %w", s.name, err))
			}
			continue
		}
		// The app did not exist before this sync: move its new tree back into the
		// reserved namespace so cleanup deletes it and the live name is absent
		// again (its pre-sync state).
		stagePath, err := newStagingPath(dstDir, "rollback-")
		if err != nil {
			errs = append(errs, fmt.Errorf("restore app %q: %w", s.name, err))
			continue
		}
		if err := os.Rename(target, stagePath); err != nil {
			errs = append(errs, fmt.Errorf("restore app %q: %w", s.name, err))
			continue
		}
		s.stagePath = stagePath
	}
	return errors.Join(errs...)
}

// rollbackRemovals restores detached stale directories, newest first. The
// original live name must be free; a concurrent occupancy is reported rather
// than overwritten.
func rollbackRemovals(removals []stagedRemoval, dstDir string) error {
	var errs []error
	for i := len(removals) - 1; i >= 0; i-- {
		r := removals[i]
		target := filepath.Join(dstDir, r.name)
		if exists, err := pathExists(target); err != nil {
			errs = append(errs, fmt.Errorf("restore removed app %q: %w", r.name, err))
			continue
		} else if exists {
			errs = append(errs, fmt.Errorf("restore removed app %q: target already exists", r.name))
			continue
		}
		if err := os.Rename(r.trashPath, target); err != nil {
			errs = append(errs, fmt.Errorf("restore removed app %q: %w", r.name, err))
		}
	}
	return errors.Join(errs...)
}

// withRollback joins a materialization failure with any rollback failure. Both
// causes remain discoverable with errors.Is.
func withRollback(cause, rollback error) error {
	if rollback == nil {
		return cause
	}
	return errors.Join(cause, fmt.Errorf("git: materialization rollback: %w", rollback))
}

// clearStaleStages removes every Relay-owned staging entry from dstDir. It may
// only be called by a serialized writer (Sync holds the process lock), because
// otherwise it could delete a stage another sync is still filling. A missing
// dstDir is normal (nothing to clean).
func clearStaleStages(dstDir string) error {
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("git: list apps dir %q: %w", dstDir, err)
	}
	for _, e := range entries {
		if !app.IsReservedDir(e.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dstDir, e.Name())); err != nil {
			return fmt.Errorf("git: clean stale staging entry %q: %w", e.Name(), err)
		}
	}
	return nil
}

// preserveAncestorIgnoreFiles carries checkout-level policy into the
// materialized app. Without this, a subsequent runtime fingerprint would
// forget an ancestor .gitignore when the ancestor rule happened not to change
// the selected files.
func preserveAncestorIgnoreFiles(selection *source.Selection, dst string) error {
	local := filepath.Join(selection.Dir(), source.IgnoreFile)
	var contents []byte
	for _, path := range selection.ApplicableIgnoreFiles() {
		if path == local || strings.HasPrefix(path, selection.Dir()+string(filepath.Separator)) {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("git: read ancestor ignore file %q: %w", path, err)
		}
		contents = append(contents, b...)
		if len(b) > 0 && b[len(b)-1] != '\n' {
			contents = append(contents, '\n')
		}
	}
	if len(contents) == 0 {
		return nil
	}
	localBytes, err := os.ReadFile(local)
	if err == nil {
		contents = append(contents, localBytes...)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("git: read local ignore file %q: %w", local, err)
	}
	return os.WriteFile(filepath.Join(dst, source.IgnoreFile), contents, 0o644)
}

// copyTree recursively copies the selected source tree at selection into dst
// (which must already exist), preserving file permissions. Excluded files (the
// .gitignore rules) and ".git" are skipped by the selection itself. Symlinks and
// other non-regular entries are skipped: WalkDir does not follow links, so the
// staged representation contains only directories and regular files.
func copyTree(selection *source.Selection, dst string) error {
	return selection.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(selection.Dir(), path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, filepath.Join(dst, rel), info.Mode().Perm())
	})
}

// copyFile copies a single regular file, preserving its mode.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("git: open source file %q: %w", src, err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("git: create file %q: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("git: copy file %q: %w", dst, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("git: close file %q: %w", dst, err)
	}
	return nil
}
