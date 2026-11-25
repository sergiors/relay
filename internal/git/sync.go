package git

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"relay/internal/processlock"
)

// gitOps is the seam isolating the transport operations sync drives (clone,
// fetch, resolve, checkout, head) behind a small interface. The production
// implementation, goGitOps, wraps go-git. Tests inject a local-transport driver
// (or simply let goGitOps run against a file:// / local path) so the sync core
// is fully exercised with zero network and zero SSH key. Production only ever
// provides the SSH-wired config; the seam keeps that wiring out of the core
// materialization logic and lets tests reuse the true go-git path with a local
// clone URL.
type gitOps interface {
	// clone performs a fresh clone of url into path. It returns nil when the
	// checkout is created and the worktree is ready for checkout.
	clone(ctx context.Context, url, path string, auth gitssh.AuthMethod) error
	// open opens an existing repository at path.
	open(path string) (gitRepo, error)
}

// gitRepo is the seam's repository view, hiding repository specifics from sync.
type gitRepo interface {
	// fetch pulls remote "origin" from url with force+prune and all tags.
	fetch(ctx context.Context, url string, auth gitssh.AuthMethod) error
	// resolveRef resolves ref to a commit hash, making the freshly fetched
	// refs/remotes/origin/<ref> authoritative for bare branch names so a stale
	// local refs/heads/<ref> never wins after a fetch (see resolveRef).
	resolveRef(ref string) (plumbing.Hash, error)
	// checkoutForce performs a hard-reset (Force) checkout to the given hash
	// (detached HEAD); the remote is the source of truth, so a Force hard reset
	// throws away any local state.
	checkoutForce(hash plumbing.Hash) error
	// head returns the current HEAD commit hash.
	head() (plumbing.Hash, error)
}

// Sync orchestrates a manual synchronization of the configured git source into
// the /apps target. It:
//
//  1. Loads the persisted config; errors clearly if none is configured.
//  2. Builds (or receives) the SSH auth and enforces host-key verification via
//     Trust On First Use over Relay's own known_hosts (see sshAuthFor). When
//     opts.Auth is already set (the test/local seam), it is used as-is; in
//     production SyncFromConfig builds it from the SSHDir key.
//  3. Ensures the checkout: clones fresh (default branch, single-branch=false)
//     on first sync, otherwise opens the existing checkout; if the configured
//     remote differs from the checkout's configured origin it re-clones. Then it
//     fetches origin with Force+Prune+AllTags so removed branches/tags drop.
//  4. Resolves the configured ref to a commit and hard-checks-out it (detached
//     HEAD, Force). NEVER pulls — a pull could produce merge state.
//  5. Locates the apps source (repo root or validated monorepo Path). Each app
//     directory is its own source-selection root (its own and nested .gitignore
//     rules; see internal/source); ignore files above it never apply.
//  6. Stages every desired app (skipping files excluded by the app-local
//     selection policy) before touching /apps, compares each staged candidate
//     with its live tree, and atomically exchanges ONLY per-app trees that were
//     added or actually changed. Unchanged apps keep their existing live
//     directory (and inode) untouched, so their watcher handles stay valid.
//     Any directory absent from the source is removed (see materializeApps).
//     /apps itself is a fixed bind mount and is never replaced as a root.
//  7. Records LastSyncedCommit/LastSyncedAt/Synced in the persisted config,
//     atomically, after materialization succeeds. No content comparison
//     fingerprint is persisted; each sync compares its actual staged output.
//
// Writers are serialized with an advisory flock (internal/processlock) on a
// sibling of the managed checkout, so a manual CLI sync, a webhook-triggered
// sync, or another process using the same checkout cannot interleave their
// fetch/checkout/materialization. The lock is held from before the checkout work
// through the metadata write. It is NOT a whole-revision transaction: the
// materialization commit boundary is per app, so a concurrent reader can observe
// a mixture of old and new apps while the swaps proceed; any individual app it
// observes is complete, and an updated app's name is never absent.
//
// Sync bounds the whole network operation (including waiting for the writer
// lock) with a fixed syncTimeout context. Progress output is a tidy per-step
// line, never raw go-git sideband.
//
// Determinism contract: with a git source configured AND synced, /apps is
// owned by git. Sync rewrites it to reflect exactly the repository/path — a
// directory present in /apps but absent from the source is removed,
// including operator-placed ones. This is the documented behavior. Filesystem
// support for atomic directory exchange is required to replace any CHANGED
// existing app; unchanged apps are not replaced or probed. When an exchange is
// required but unavailable, Sync fails before mutating a live app rather than
// falling back to remove-then-rename.
func Sync(ctx context.Context, opts SyncOptions) error {
	cfg, err := LoadConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	return syncWithGit(ctx, opts, cfg, newGoGitOps(), newGoGitAuthFn(opts))
}

// SyncFromConfig runs a sync using a fully-provided Config (used by the CLI after
// `git set` has persisted it identically, and by tests that construct a Config
// directly without touching a config file). It is the test-friendly entry that
// still honors the seam: when opts.Auth is non-nil it bypasses the SSH/TOFU
// known_hosts path entirely (local filesystem sources in tests).
func SyncFromConfig(ctx context.Context, opts SyncOptions, cfg Config) error {
	return syncWithGit(ctx, opts, cfg, newGoGitOps(), newGoGitAuthFn(opts))
}

// SyncCommit materializes one exact immutable commit instead of resolving a
// mutable ref. It is the coordinated-deployment entry: the shared desired
// revision is a pinned SHA, and every node independently fetches and checks out
// that object with the same source/materialization policy as normal sync. An
// abbreviated hash, a branch/tag name, or an object missing after the fetch is a
// hard error, so a node never silently materializes a different revision.
func SyncCommit(ctx context.Context, opts SyncOptions, cfg Config, commit string) error {
	opts.Commit = commit
	return syncWithGit(ctx, opts, cfg, newGoGitOps(), newGoGitAuthFn(opts))
}

// SyncCommitFromConfig is SyncCommit with both the transport seam and a
// directly-provided config, mirroring SyncFromConfig.
func SyncCommitFromConfig(ctx context.Context, opts SyncOptions, cfg Config, commit string) error {
	opts.Commit = commit
	return syncWithGit(ctx, opts, cfg, newGoGitOps(), newGoGitAuthFn(opts))
}

// ResolveRef fetches the configured source and resolves its configured ref (or
// an option override) to a full immutable commit SHA WITHOUT checking out or
// materializing /apps. It is the branch-publication half of `relay git sync`:
// the caller can commit that SHA to the shared desired revision before this
// node materializes it through SyncCommit. The writer lock is released before
// returning, so publication is never performed while holding the checkout lock.
//
// It uses the same canonical config/override validation, transport auth, TOFU
// host-key verification, checkout preparation, and fetch semantics as Sync. An
// abbreviated/branch value is resolved to the exact object here; the returned
// value is always a full lowercase SHA suitable for coordinated publication.
func ResolveRef(ctx context.Context, opts SyncOptions, cfg Config) (string, error) {
	return ResolveRefFromConfig(ctx, opts, cfg, newGoGitOps(), newGoGitAuthFn(opts))
}

// ResolveRefFromConfig is ResolveRef with injectable transport seams for tests
// and local filesystem sources. It never writes the sync bookkeeping or /apps.
func ResolveRefFromConfig(
	ctx context.Context,
	opts SyncOptions,
	cfg Config,
	ops gitOps,
	auth builder,
) (string, error) {
	normalized, err := NormalizeAndValidateConfig(cfg)
	if err != nil {
		return "", err
	}
	cfg = normalized
	if opts.RepositoryURL != "" {
		if err := ValidateRepositoryURL(opts.RepositoryURL); err != nil {
			return "", err
		}
	}
	if opts.Ref != "" {
		if err := validateRef(opts.Ref); err != nil {
			return "", err
		}
	}
	if opts.Path != nil {
		if err := validatePath(*opts.Path); err != nil {
			return "", err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	cloneURL := cfg.Repository
	if opts.RepositoryURL != "" {
		cloneURL = opts.RepositoryURL
	}
	if opts.CloneURL != "" {
		cloneURL = opts.CloneURL
	}
	ref := cfg.Ref
	if opts.Ref != "" {
		ref = opts.Ref
	}
	am, err := auth.authFor(cloneURL)
	if err != nil {
		return "", err
	}
	if lockPath := syncLockPath(opts.CheckoutDir); lockPath != "" {
		lock, err := processlock.AcquireBlocking(ctx, lockPath)
		if err != nil {
			return "", fmt.Errorf("git: acquire sync lock %s: %w", lockPath, err)
		}
		defer lock.Close()
	}
	repo, _, err := ensureCheckout(ctx, ops, opts.CheckoutDir, cloneURL, am)
	if err != nil {
		return "", err
	}
	if err := repo.fetch(ctx, cloneURL, am); err != nil {
		return "", fmt.Errorf("git: fetch: %w", err)
	}
	hash, err := repo.resolveRef(ref)
	if err != nil {
		return "", fmt.Errorf("git: ref %q not found as branch, tag, or commit: %w", ref, err)
	}
	if opts.Log != nil {
		opts.Log.Debug("Remote ref resolved",
			"ref", ref, "commit", hash.String())
	}
	return hash.String(), nil
}

// newGoGitAuthFn returns the auth-builder for the transport. Production resolves
// auth from the SSHDir key (with TOFU host-key verification over Relay's own
// known_hosts) when the source is SSH; tests supply opts.Auth and/or point at a
// local source (nil auth).
func newGoGitAuthFn(opts SyncOptions) gitOpsAuth {
	return gitOpsAuth{opts: opts}
}

// gitOpsAuth builds the SSH auth for go-git based on the options and the
// effective source URL.
type gitOpsAuth struct {
	opts SyncOptions
}

// authFor returns the transport auth method for the given source URL. An
// explicitly supplied opts.Auth wins (the local test seam). Otherwise, when the
// source is an SSH URL (the production case) the SSHDir key is loaded and wired
// for TOFU host-key verification over Relay's own known_hosts file — any failure
// that would skip verification (missing key, unreadable known_hosts) is a hard
// error. Non-ssh sources (file:// or a plain local path, as tests and monorepo
// fixtures use) need no auth and get nil.
func (a gitOpsAuth) authFor(sourceURL string) (gitssh.AuthMethod, error) {
	if a.opts.Auth != nil {
		return a.opts.Auth, nil
	}
	ep, err := parseEndpoint(sourceURL)
	if err != nil {
		return nil, fmt.Errorf("git: parse source endpoint: %w", err)
	}
	if ep.Protocol != "ssh" {
		// Local/filesystem source (tests, or a future non-SSH transport): no
		// credentials needed.
		return nil, nil
	}
	if a.opts.SSHDir == "" {
		return nil, fmt.Errorf("git: ssh source requires the SSHDir option (production sets /var/lib/relay/ssh)")
	}
	// Thread the parsed endpoint and the two reporting seams (out/log) down to
	// the TOFU callback so a first-trust surfaces to whichever is set.
	return sshAuthFor(a.opts.SSHDir, ep, a.opts.Out, a.opts.Log)
}

// syncWithGit is the shared implementation behind Sync and SyncFromConfig. It
// takes the transport seam (ops) and the auth builder so both entries behave
// identically.
//
// Before any timeout, auth build, network, or filesystem action it validates the
// config and every effective override through the one canonical path
// (NormalizeAndValidateConfig for the config, plus the same per-field validators
// for the RepositoryURL/Ref/Path overrides). A hand-edited source.json or a
// directly constructed Config passed to SyncFromConfig can therefore never reach
// gitOps/network or materialization with a bad repository, ref, path, or webhook
// secret reference. opts.CloneURL is the documented local/filesystem transport
// seam for tests and local sources and is deliberately exempt from the persisted
// SSH-URL rule; a RepositoryURL override IS validated as the persisted URL is.
func syncWithGit(ctx context.Context, opts SyncOptions, cfg Config, ops gitOps, auth builder) error {
	// NormalizeAndValidateConfig also defaults an empty cfg.Ref to DefaultRef, so
	// the effective-ref logic below always sees a concrete ref.
	normalized, err := NormalizeAndValidateConfig(cfg)
	if err != nil {
		return err
	}
	cfg = normalized
	if opts.RepositoryURL != "" {
		if err := ValidateRepositoryURL(opts.RepositoryURL); err != nil {
			return err
		}
	}
	if opts.Ref != "" {
		if err := validateRef(opts.Ref); err != nil {
			return err
		}
	}
	if opts.Commit != "" {
		if err := ValidateCommit(opts.Commit); err != nil {
			return err
		}
	}
	if opts.Path != nil {
		if err := validatePath(*opts.Path); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	log := opts.Log
	out := opts.Out
	// writeLine reports a user-facing step line on the command writer (the
	// manual CLI sync). It does not touch Log: the two channels serve different
	// callers. The CLI sets Out only, so its presentation prints exactly once;
	// the background worker/webhook sync sets Log only (Out nil), where the
	// structured Debug records below are the operational trail.
	writeLine := func(format string, args ...any) {
		if out != nil {
			fmt.Fprintf(out, format+"\n", args...)
		}
	}
	// debug is the nil-tolerant structured diagnostic half. Its messages are
	// distinct from the writer lines and carry internals the presentation omits.
	debug := func(msg string, args ...any) {
		if log != nil {
			log.Debug(msg, args...)
		}
	}

	// The actual clone source. Production uses the configured Repository; tests
	// redirect with CloneURL so they never hit the network/SSH.
	cloneURL := cfg.Repository
	if opts.RepositoryURL != "" {
		cloneURL = opts.RepositoryURL
	}
	if opts.CloneURL != "" {
		cloneURL = opts.CloneURL
	}

	ref := cfg.Ref
	if opts.Ref != "" {
		ref = opts.Ref
	}
	path := cfg.Path
	if opts.Path != nil {
		path = *opts.Path
	}

	// Build the transport auth (SSH in production, none for local test sources).
	am, err := auth.authFor(cloneURL)
	if err != nil {
		return err
	}

	writeLine("Updating apps on this node...")

	// Serialize writers before touching the checkout or /apps. The lock file is
	// a sibling of the checkout, never inside it: ensureCheckout may remove and
	// re-clone the checkout, which would delete an in-checkout lock file while
	// it is held and let a second process lock a fresh file independently.
	// Waiting is bounded by the sync context (and therefore by syncTimeout).
	if lockPath := syncLockPath(opts.CheckoutDir); lockPath != "" {
		lock, err := processlock.AcquireBlocking(ctx, lockPath)
		if err != nil {
			return fmt.Errorf("git: acquire sync lock %s: %w", lockPath, err)
		}
		defer lock.Close()
		debug("Sync writer lock acquired", "path", lockPath)
	}

	// Ensure the checkout: re-clone when the origin URL changed, else clone/open.
	repo, fresh, err := ensureCheckout(ctx, ops, opts.CheckoutDir, cloneURL, am)
	if err != nil {
		return err
	}
	if fresh {
		debug("Checkout clone completed", "path", opts.CheckoutDir)
	} else {
		debug("Checkout reuse", "path", opts.CheckoutDir)
	}

	// Fetch remote "origin" so refs (branches, tags) reflect the remote. Force
	// + Prune drops removed refs; AllTags brings tags (which ResolveRevision
	// uses for tag refs).
	if err := repo.fetch(ctx, cloneURL, am); err != nil {
		return fmt.Errorf("git: fetch: %w", err)
	}
	debug("Remote fetch completed", "url", cloneURL)

	// Resolve the configured ref to a commit. A coordinated deployment supplies
	// an exact immutable commit instead, so no mutable ref is resolved. A bare
	// branch name resolves against the freshly fetched refs/remotes/origin/<ref>
	// FIRST — never the possibly-stale local refs/heads/<ref>, which a clone
	// always creates for the default branch — so a post-fetch sync reflects the
	// remote tip.
	var hash plumbing.Hash
	if opts.Commit != "" {
		hash = plumbing.NewHash(strings.ToLower(opts.Commit))
		debug("Exact commit selected", "commit", hash.String())
	} else {
		hash, err = repo.resolveRef(ref)
		if err != nil {
			return fmt.Errorf("git: ref %q not found as branch, tag, or commit: %w", ref, err)
		}
		debug("Ref resolution", "ref", ref, "commit", hash.String())
	}

	// Hard checkout (detached HEAD, Force = hard reset). The remote is truth.
	// For an exact commit this also verifies the object is present after the
	// fetch; a missing object is a hard error rather than a fallback to a
	// different revision.
	if err := repo.checkoutForce(hash); err != nil {
		if opts.Commit != "" {
			return fmt.Errorf("git: commit %s not available in the checkout: %w", hash.String(), err)
		}
		return fmt.Errorf("git: checkout %s: %w", hash.String()[:7], err)
	}
	headHash, err := repo.head()
	if err != nil {
		return fmt.Errorf("git: read HEAD: %w", err)
	}
	if opts.Commit != "" && headHash != hash {
		return fmt.Errorf("git: checkout produced HEAD %s, want %s", headHash.String(), hash.String())
	}

	// Source dir: checkout root or validated monorepo path.
	srcDir, err := resolveSourceDir(opts.CheckoutDir, path)
	if err != nil {
		return err
	}

	// Source selection is scoped to each app directory during materialization:
	// only the app's own .gitignore and nested non-ignored rules apply. A
	// repository-root or monorepo ancestor rule never affects an app's source.
	// A missing source dir is the hard missing-source failure discovery reports;
	// a missing .gitignore is not an error.

	// We hold the writer lock, so every reserved staging entry under /apps is a
	// leftover from an interrupted earlier sync (never an active stage of a
	// concurrent writer). Reclaim them before staging this revision.
	if err := clearStaleStages(opts.AppsDir); err != nil {
		return err
	}
	debug("Cleared stale git staging entries", "apps_dir", opts.AppsDir)

	// Materialize /apps deterministically. Only added/changed apps are reported
	// as materialized; an identical revision performs no live replacement.
	result, err := materializeApps(srcDir, opts.AppsDir)
	if err != nil {
		return err
	}
	switch {
	case result.desired == 0:
		// materializeApps already hard-errors on a missing source dir, so an
		// empty desired set here means the source existed but held no app dirs.
		// The deterministic-removal pass still ran and cleared stale dirs.
		writeLine("No apps found in the configured source; all existing apps were removed")
	case len(result.materialized) == 0 && len(result.removed) == 0:
		writeLine("No app changes: %d app(s) already up to date", result.desired)
	default:
		writeLine("Updated %d app(s): %s", len(result.materialized), joinNames(result.materialized))
	}
	if len(result.removed) > 0 {
		writeLine("Removed %d app(s): %s", len(result.removed), joinNames(result.removed))
	}
	debug("Materialized apps", "source", srcDir,
		"desired", result.desired,
		"materialized", len(result.materialized),
		"removed", len(result.removed))

	// Record the sync bookkeeping atomically, only after materialization.
	cfg.LastSyncedCommit = headHash.String()
	cfg.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
	cfg.Synced = true
	cfg.Ref = ref
	cfg.Path = path
	if err := writeConfig(cfg, opts.ConfigPath); err != nil {
		return err
	}

	writeLine("This node's apps are up to date")
	return nil
}

// syncLockPath is the advisory lock serializing writers that share a managed
// checkout. It is a SIBLING of the checkout, in the same git data directory as
// the config and checkout: a re-clone removes the checkout directory, so a lock
// file inside it would be deleted while held. An empty CheckoutDir yields no
// lock path (defensive; production and tests always provide one, and
// ensureCheckout would fail anyway).
func syncLockPath(checkoutDir string) string {
	if checkoutDir == "" {
		return ""
	}
	clean := filepath.Clean(checkoutDir)
	// The "relay-" prefix keeps the lock name out of the reserved ".sync-*"
	// namespace even for an unusual checkout name, so staging cleanup can never
	// mistake the held lock file for a stage.
	return filepath.Join(filepath.Dir(clean), "relay-"+filepath.Base(clean)+".sync.lock")
}

// builder produces the transport auth.
type builder interface {
	authFor(sourceURL string) (gitssh.AuthMethod, error)
}

// ensureCheckout makes the checkout exist and match the requested url. On first
// sync (or when the existing checkout's origin URL differs — sign of a changed
// source) it removes the checkout and clones fresh; otherwise it opens the
// existing checkout. A fresh clone uses the default branch (ReferenceName empty)
// and SingleBranch=false so all remote branches/tags come down for ref
// resolution. It returns the repository view and whether it was freshly cloned.
func ensureCheckout(
	ctx context.Context, ops gitOps, checkoutDir, url string, am gitssh.AuthMethod,
) (gitRepo, bool, error) {
	// Origin URL drift => re-clone to avoid stale remote state.
	if _, err := os.Stat(filepath.Join(checkoutDir, ".git")); err == nil {
		repo, oerr := ops.open(checkoutDir)
		if oerr == nil {
			originURL, uerr := remoteOriginURL(repo)
			if uerr == nil && originURL == url {
				return repo, false, nil
			}
		}
		// Either it isn't a repo or the origin changed: fall through to re-clone.
		if err := os.RemoveAll(checkoutDir); err != nil {
			return nil, false, fmt.Errorf("git: reset checkout: %w", err)
		}
	}
	if err := os.MkdirAll(checkoutDir, 0o755); err != nil {
		return nil, false, fmt.Errorf("git: create checkout dir: %w", err)
	}
	if err := ops.clone(ctx, url, checkoutDir, am); err != nil {
		return nil, false, fmt.Errorf("git: clone %s: %w", url, err)
	}
	repo, err := ops.open(checkoutDir)
	if err != nil {
		return nil, false, fmt.Errorf("git: open checkout: %w", err)
	}
	return repo, true, nil
}

// remoteOriginURL reads the origin URL from a repository view. Repositories that
// expose a RemoteURL (the go-git-backed implementation does) return it; any other
// (e.g. a test fake) returns "", nil. It is only called on an already-opened
// checkout.
func remoteOriginURL(repo gitRepo) (string, error) {
	type cfgReader interface {
		RemoteURL() (string, bool)
	}
	if r, ok := repo.(cfgReader); ok {
		url, has := r.RemoteURL()
		if !has {
			return "", nil
		}
		return url, nil
	}
	return "", nil
}

// joinNames renders a sorted slice for summaries.
func joinNames(names []string) string {
	s := ""
	for i, n := range names {
		if i > 0 {
			s += ", "
		}
		s += n
	}
	return s
}
