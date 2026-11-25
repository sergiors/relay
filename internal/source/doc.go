// Package source provides Relay's shared source-selection policy: which files
// under an app directory are part of the app's "source" and which are excluded
// by .gitignore rules.
//
// It exists because three layers must agree on exactly which files are source,
// and previously each answered the question differently:
//
//   - Git materialization (internal/git) copies an app directory from a
//     configured repository into /apps.
//   - Build-context staging (internal/runtime) stages the selected source into
//     an image build context from one immutable snapshot of the selection, so
//     the bytes baked into an image are the same bytes its fingerprint covers.
//   - Content fingerprinting (internal/app) hashes an app directory to
//     gate rebuilds.
//
// An app directory is the selection root. The policy is:
//
//   - Rules come from the app's own .gitignore plus every .gitignore in its
//     non-ignored descendants. Deeper files override shallower ones, matching
//     git: the matcher is built in increasing priority order.
//   - Ignore files above the app directory never apply. A repository-root,
//     monorepo, or checkout-level rule cannot hide an app or exclude files from
//     its copied source, fingerprint, or build context.
//   - The app's template.yaml is always source even if a pattern would ignore
//     it: discovery and the loader require it regardless of ignore rules.
//   - Git never descends into an ignored directory, so a nested .gitignore can
//     never re-include a path whose parent was ignored. The walk mirrors that by
//     skipping ignored directories outright.
//   - ".git" is never source.
//   - Applicable .gitignore files are always reported by IgnoreFiles (even if a
//     pattern would ignore them) because the policy itself is an input to
//     selection: callers fold their content into the fingerprint so a rule edit
//     is a source change, and copy them so the policy travels with the tree.
//
// Selection is intentionally self-contained: it reads the filesystem but never
// builds, executes, or writes anything, so the fingerprint and build layers can
// depend on it without pulling in git transport or Docker.
package source
