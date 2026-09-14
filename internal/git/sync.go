package git

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
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
	// resolveRef resolves ref to a commit hash, trying the bare ref name first
	// and then the origin remote-tracking form (see resolveCommit).
	resolveRef(ref string) (plumbing.Hash, error)
	// checkoutForce performs a hard-reset (Force) checkout to the given hash
	// (detached HEAD); the remote is the source of truth, so a Force hard reset
	// throws away any local state.
	checkoutForce(hash plumbing.Hash) error
	// head returns the current HEAD commit hash.
	head() (plumbing.Hash, error)
}

// Sync orchestrates a manual synchronization of the configured git source into
// the /functions target. It:
//
//  1. Loads the persisted config; errors clearly if none is configured.
//  2. Builds (or receives) the SSH auth and enforces host-key verification. When
//     opts.Auth is already set (the test/local seam), it is used as-is; in
//     production SyncFromConfig builds it from the SSHDir key.
//  3. Ensures the checkout: clones fresh (default branch, single-branch=false)
//     on first sync, otherwise opens the existing checkout; if the configured
//     remote differs from the checkout's configured origin it re-clones. Then it
//     fetches origin with Force+Prune+AllTags so removed branches/tags drop.
//  4. Resolves the configured ref to a commit and hard-checks-out it (detached
//     HEAD, Force). NEVER pulls — a pull could produce merge state.
//  5. Locates the functions source (repo root or validated monorepo Path).
//  6. Materializes /functions deterministically: copy/refresh each function
//     directory, remove any directory not in the source (see materialize).
//  7. Records LastSyncedCommit/LastSyncedAt/Synced in the persisted config,
//     atomically, after materialization succeeds.
//
// Sync never holds a lock (the CLI process exits when done); it bounds the whole
// network operation with a fixed syncTimeout context. Progress output is a tidy
// per-step line, never raw go-git sideband.
//
// Determinism contract: with a git source configured AND synced, /functions is
// owned by git. Sync rewrites it to reflect exactly the repository/path — a
// directory present in /functions but absent from the source is removed,
// including operator-placed ones. This is the documented behavior.
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
// still honors the seam: when opts.Auth is non-nil it bypasses the SSH/known
// hosts path entirely (local filesystem sources in tests).
func SyncFromConfig(ctx context.Context, opts SyncOptions, cfg Config) error {
	return syncWithGit(ctx, opts, cfg, newGoGitOps(), newGoGitAuthFn(opts))
}

// newGoGitAuthFn returns the auth-builder for the transport. Production resolves
// auth from the SSHDir key (with host-key verification) when the source is SSH;
// tests supply opts.Auth and/or point at a local source (nil auth).
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
// source is an SSH URL (the production case) the SSHDir key is loaded with
// known_hosts verification — and any failure (missing key, no known_hosts) is a
// hard error so host-key verification is never skipped. Non-ssh sources (file://
// or a plain local path, as tests and monorepo fixtures use) need no auth and get
// nil.
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
	return sshAuthFor(a.opts.SSHDir)
}

// syncWithGit is the shared implementation behind Sync and SyncFromConfig. It
// takes the transport seam (ops) and the auth builder so both entries behave
// identically.
func syncWithGit(ctx context.Context, opts SyncOptions, cfg Config, ops gitOps, auth builder) error {
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	log := opts.Log
	out := opts.Out
	// writeLine reports each sync step on BOTH channels, honoring two rules:
	//
	//  1. opts.Log is DI: the caller's logger (the CLI passes the process logger
	//     from cmd/main.go). nil means silent — this package never constructs a
	//     fallback logger (that helper was deliberately removed; nil-tolerance
	//     lives at the single call site instead).
	//  2. The slog side is deliberately Debug, NOT Info: the process logger
	//     (cmd/main.go) is a tint handler writing to stdout at the configured
	//     level, and opts.Out is also stdout for CLI runs — an Info mirror would
	//     print every sync step line twice on the operator's terminal. Debug
	//     keeps default (LOG_LEVEL=INFO) output single-channel while
	//     LOG_LEVEL=DEBUG reveals the full trail.
	writeLine := func(format string, args ...any) {
		if out != nil {
			fmt.Fprintf(out, format+"\n", args...)
		}
		if log != nil {
			log.Debug(fmt.Sprintf(format, args...))
		}
	}

	if err := validatePath(cfg.Path); err != nil {
		return err
	}
	if cfg.Repository == "" {
		return fmt.Errorf("git: configured repository is empty")
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

	writeLine("Syncing...")

	// Ensure the checkout: re-clone when the origin URL changed, else clone/open.
	repo, fresh, err := ensureCheckout(ctx, ops, opts.CheckoutDir, cloneURL, am)
	if err != nil {
		return err
	}
	if log != nil {
		if fresh {
			log.Debug("Cloned fresh checkout", "url", cloneURL)
		} else {
			log.Debug("Reusing existing checkout", "url", cloneURL)
		}
	}

	// Fetch remote "origin" so refs (branches, tags) reflect the remote. Force
	// + Prune drops removed refs; AllTags brings tags (which ResolveRevision
	// uses for tag refs).
	if err := repo.fetch(ctx, cloneURL, am); err != nil {
		return fmt.Errorf("git: fetch: %w", err)
	}

	// Resolve the configured ref to a commit. Bare branch names do not resolve
	// via go-git's default expansion (it uses refs/remotes/<ref>, never
	// refs/remotes/origin/<ref>), so try the origin-tracking form explicitly.
	hash, err := repo.resolveRef(ref)
	if err != nil {
		return fmt.Errorf("git: ref %q not found as branch, tag, or commit: %w", ref, err)
	}

	// Hard checkout (detached HEAD, Force = hard reset). The remote is truth.
	if err := repo.checkoutForce(hash); err != nil {
		return fmt.Errorf("git: checkout %s: %w", hash.String()[:7], err)
	}
	headHash, err := repo.head()
	if err != nil {
		return fmt.Errorf("git: read HEAD: %w", err)
	}
	writeLine("Resolved %q to %s", ref, headHash.String()[:7])

	// Source dir: checkout root or validated monorepo path.
	srcDir, err := resolveSourceDir(opts.CheckoutDir, path)
	if err != nil {
		return err
	}

	// Materialize /functions deterministically.
	materialized, removed, err := materialize(srcDir, opts.FunctionsDir)
	if err != nil {
		return err
	}
	if len(materialized) == 0 {
		// Allowable ONLY when the source dir genuinely existed and simply had no
		// valid function directories (a missing source dir is a hard error thrown
		// earlier by materialize, before any config bookkeeping is touched). The
		// deterministic-removal pass still ran and cleared stale dirs; /functions
		// ends up empty by design.
		writeLine("Source contained no function directories; /functions is now empty")
	} else {
		writeLine("Materialized %d function(s): %s", len(materialized), joinNames(materialized))
	}
	if len(removed) > 0 {
		writeLine("Removed %d function(s): %s", len(removed), joinNames(removed))
	}

	// Record the sync bookkeeping atomically, only after materialization.
	cfg.LastSyncedCommit = headHash.String()
	cfg.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
	cfg.Synced = true
	cfg.Ref = ref
	cfg.Path = path
	if err := writeConfig(cfg, opts.ConfigPath); err != nil {
		return err
	}

	writeLine("Sync complete")
	return nil
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
func ensureCheckout(ctx context.Context, ops gitOps, checkoutDir, url string, am gitssh.AuthMethod) (gitRepo, bool, error) {
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
