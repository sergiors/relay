// Package git provides the manual, operator-driven Git synchronization
// workflow for Relay.
//
// Git is NOT an automatic part of the runtime. `relay start` never polls, never
// watches a repository, and never fetches: the worker only watches /apps
// for filesystem changes (see internal/reconciler). Git synchronization exists
// as a separate, explicit operator command that downloads a repository once,
// deterministically rewrites /apps to match a configured ref, and then
// lets the existing reconciler pick up the resulting directory tree. The flow is
// always manual:
//
//	relay git keygen     # generate an SSH deploy key once
//	relay git set <repo> # remember the SSH repository to sync
//	relay git sync       # when desired, materialize the repo into /apps
//	relay git status     # inspect the current sync state
//	relay git remove     # forget the source and drop the checkout
//
// Only `relay git sync` ever modifies /apps. When a git source is
// configured and a sync has run, /apps is treated as fully managed by git:
// a sync rewrites it to reflect exactly the configured repository/path, removing
// any directory it does not contain (the "deterministic replace" rule). Source
// selection honors .gitignore rules via the shared internal/source policy: an
// ignored app directory is not materialized and an ignored file inside a
// app is not copied, while the applicable .gitignore files themselves are
// copied (they are the policy). See Sync's documentation and docs/operations.md
// "Git" for the precise contract.
//
// Materialization is staged before it is committed. Every desired app tree is
// copied into a reserved .sync-* directory directly under /apps (which the
// loader, watcher, reconciler, and state view all ignore). Candidate trees are
// checked only for copy completeness and template.yaml presence; template
// contents are not validated, so malformed templates are still committed and
// handled by the reconciler. A staging/copy failure leaves the previous /apps
// tree intact. Each existing app is then replaced with an atomic directory
// exchange — renameat2(RENAME_EXCHANGE) on Linux, renamex_np(RENAME_SWAP) on
// macOS — so an updated app name is never absent and never partially copied.
// New apps are an atomic rename into an absent name. Removed apps are atomically
// renamed into the reserved namespace before deletion. /apps is a fixed bind
// mount and is never renamed as a root; the commit boundary is per app, not a
// whole-revision snapshot, so a concurrent observer may see a mixture across
// different apps. An app name always resolves to a complete tree, never a
// partially copied directory; a reader whose walk spans a swap can still see
// files from both generations. If an existing app must be replaced and the
// filesystem cannot exchange directories, sync fails closed before mutating a
// live app rather than using a remove-then-rename fallback.
//
// Sync serializes writers sharing the managed checkout with an advisory flock
// on a sibling of the checkout. Stale .sync-* directories from an interrupted
// sync are reclaimed only by that locked path.
//
// Transport model: production sync authenticates over SSH only, using a local
// ed25519 deploy key whose host verification uses Trust On First Use (TOFU)
// over Relay's OWN known_hosts file (host-key verification is NEVER disabled).
// The first time a host is reached it is trusted and its fingerprint persists;
// every later sync verifies against that pinned key, and a changed key fails
// loudly and is never auto-replaced. Relay is provider-neutral — it works with
// GitHub, GitLab, or any SSH git server — and never needs ssh-keyscan or the
// operator's ~/.ssh/known_hosts. The sync core is still transport-neutral: it
// accepts a RepositoryURL and an AuthMethod, both injected by the caller. The
// CLI entry wires the SSH URL and the key; tests drive the same core against a
// local filesystem path with a nil auth, which requires no key and no network.
//
// Storage layout (fixed application conventions, mirroring state and secrets):
//
//	/var/lib/relay/ssh/id_ed25519            the private deploy key (0700/0600)
//	/var/lib/relay/ssh/known_hosts           Relay's TOFU host-key pins (0600)
//	/var/lib/relay/git/source.json           the persisted sync config (0600)
//	/var/lib/relay/git/checkout              the managed git checkout/worktree
//
// Relay never shells out to the git binary; all operations use go-git.
package git
