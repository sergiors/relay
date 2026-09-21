// Package source provides Relay's shared source-selection policy: which files
// under a function or checkout tree are part of the function's "source" and
// which are excluded by .gitignore rules.
//
// It exists because three layers must agree on exactly which files are source,
// and previously each answered the question differently:
//
//   - Git materialization (internal/git) copies a configured repository subtree
//     into /functions.
//   - Build-context staging (internal/runtime) copies a materialized function
//     directory into an image build context.
//   - Content fingerprinting (internal/function) hashes a function directory to
//     gate rebuilds.
//
// A Selection carries the resolved root relationship (a rule "scope" — normally
// the checkout root — plus the selected directory) and the .gitignore rules that
// apply to paths within it. The policy is:
//
//   - Rules come from the .gitignore file in every directory from the scope down
//     to the selected directory (inclusive), plus every .gitignore in its
//     non-ignored descendants. Deeper files override shallower ones, matching
//     git: the matcher is built in increasing priority order.
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
