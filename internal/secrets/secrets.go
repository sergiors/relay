// Package secrets resolves secret references to values at execution time.
//
// A secret is a named value stored outside the function's template and image.
// Templates reference secrets by name (see internal/function.SecretRef); this
// package turns that reference into the value the runner injects into an
// execution container's environment, immediately before the container is
// created. Resolved values are never persisted, never logged, never baked into
// images, and never stored in the state database.
//
// The Provider interface is the single seam between the runner and whatever
// stores secrets. The only concrete implementation is LocalProvider, which
// reads secrets from a directory on disk (one file per secret, named by the
// secret reference). The interface is deliberately tiny so a future external
// provider (Vault, a secrets API, ...) can be added without changing templates
// or the runner.
package secrets

import "context"

// SecretsDir is the fixed application-convention root the local provider reads
// secrets from. It is an application convention, not env-configurable: the
// compose file volume-mounts /var/lib/relay so the directory survives container
// restarts, and the CLI writes secrets there. Deleting the volume deletes the
// secrets.
const SecretsDir = "/var/lib/relay/secrets"

// Provider resolves a secret reference to its value. Implementations must never
// include a secret's value in an error; a not-found error carries the name only.
type Provider interface {
	// Resolve returns the value for the named secret. A missing secret returns
	// an error wrapping os.ErrNotExist with the name; the value is never part
	// of any error.
	Resolve(ctx context.Context, name string) (string, error)
}
