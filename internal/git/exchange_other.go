//go:build !linux && !darwin

package git

import "errors"

// errExchangeUnsupported reports that this build target has no atomic
// two-directory exchange primitive. materialize treats it as a hard,
// pre-mutation failure rather than falling back to remove-then-rename, which
// would expose an absent app directory to the reconciler.
var errExchangeUnsupported = errors.New("atomic directory exchange is not supported on this platform")

// platformExchange is the unsupported-platform stub. It never mutates either
// path.
func platformExchange(a, b string) error {
	return errExchangeUnsupported
}
