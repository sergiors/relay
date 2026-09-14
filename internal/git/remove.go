package git

import (
	"errors"
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
// Remove is idempotent: when nothing is configured (or the checkout is already
// gone) it succeeds, reporting "not configured" or simply doing nothing. Missing
// directories are not errors.
func Remove(cfgPath, checkoutDir string, out io.Writer) error {
	report := func(format string, args ...any) {
		if out != nil {
			fmt.Fprintf(out, format+"\n", args...)
		}
	}

	_, err := LoadConfig(cfgPath)
	configured := err == nil
	if err != nil && !isErrNoSource(err) {
		// A corrupt/unreadable config should still not block cleanup of the
		// checkout, but surface the read issue. A missing config (ErrConfigNotFound
		// via errors.Is) is the normal "nothing to remove" state, not an error.
		return fmt.Errorf("git: read config: %w", err)
	}

	if configured {
		if err := os.Remove(cfgPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("git: remove config: %w", err)
		}
		report("Removed git source config")
	} else {
		report("Not configured; nothing to remove")
	}

	if _, err := os.Stat(checkoutDir); err == nil {
		if err := os.RemoveAll(checkoutDir); err != nil {
			return fmt.Errorf("git: remove checkout: %w", err)
		}
		report("Removed checkout")
	}
	return nil
}

// isErrNoSource reports whether err is the "no git source configured" sentinel.
// It uses errors.Is (not a bare ==) so it also catches the sentinel wrapped by
// LoadConfig, keeping the check identical to every other call site.
func isErrNoSource(err error) bool {
	return errors.Is(err, ErrConfigNotFound())
}
