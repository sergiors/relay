package git

import (
	"fmt"
	"io"
	"os"
)

// Remove drops the persisted git config and the managed checkout directory. It
// NEVER touches /functions (materialized function directories are left exactly
// as they are; a later manual choice determines their fate) and NEVER deletes
// the SSH key.
//
// Why the key survives: the operator registers the public key with their git
// provider as a Deploy Key. Deleting the private key silently would orphan that
// registration and break any other tooling relying on it, and regenerating a
// key is an explicit, separate operation (`relay git keygen`). Removing the
// source config is unrelated to the key lifecycle, so remove leaves the key in
// place. (Operators rotate a key by removing the file directly and re-keying.)
//
// Removal decides whether a config is present by stat'ing the filesystem, never
// by reading or validating its contents. That is deliberate: `git remove` is the
// escape hatch for a hand-edited source.json that LoadConfig rejects (bad JSON,
// a non-SSH repository, or a bad ref/path/secret reference), so a broken config
// must stay removable. The file's bytes are never parsed, trusted, or acted on.
//
// Remove is idempotent: when nothing is configured (or the checkout is already
// gone) it succeeds, reporting "not configured" or simply doing nothing. Missing
// directories are not errors. A genuine inspect/removal failure is still
// reported, but it never prevents the checkout from being cleaned up: the
// checkout is attempted regardless and the first error (if any) is returned.
func Remove(cfgPath, checkoutDir string, out io.Writer) error {
	report := func(format string, args ...any) {
		if out != nil {
			fmt.Fprintf(out, format+"\n", args...)
		}
	}

	// firstErr keeps the first genuine failure without aborting the rest of the
	// cleanup, so a config problem can never block dropping the checkout.
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Existence, not validity, decides whether a config is present: a
	// hand-edited file that LoadConfig rejects is still removed here because
	// remove never parses the contents. A missing file is the normal "nothing
	// configured" state (the idempotent second run), not an error.
	switch _, statErr := os.Stat(cfgPath); {
	case statErr == nil:
		if err := os.Remove(cfgPath); err != nil && !os.IsNotExist(err) {
			record(fmt.Errorf("git: remove config: %w", err))
		} else {
			report("Removed git source config")
		}
	case os.IsNotExist(statErr):
		report("Not configured; nothing to remove")
	default:
		// The file may exist but be unstat-able (e.g. a parent-dir permission
		// error). Surface it, but still attempt the checkout cleanup below.
		record(fmt.Errorf("git: inspect config: %w", statErr))
	}

	if _, err := os.Stat(checkoutDir); err == nil {
		if err := os.RemoveAll(checkoutDir); err != nil {
			record(fmt.Errorf("git: remove checkout: %w", err))
		} else {
			report("Removed checkout")
		}
	} else if !os.IsNotExist(err) {
		record(fmt.Errorf("git: inspect checkout: %w", err))
	}
	return firstErr
}
