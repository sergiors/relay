// Package secrets resolves secret references to values at execution time.
//
// A secret is a named value stored outside the app's template and image.
// Templates reference secrets by name (see internal/app.SecretRef); this
// package turns that reference into the value the runner injects into an
// execution container's environment, immediately before the container is
// created. Resolved values are never persisted, never logged, never baked into
// images, and never stored in the state database.
//
// The Provider interface is the single seam between the runner and whatever
// stores secrets. LocalProvider reads secrets from a directory on disk (one
// file per secret, named by the secret reference). InfisicalProvider resolves
// the same references from an Infisical project using Universal Auth, with a
// per-instance TTL cache and typed failures. The interface stays deliberately
// tiny so providers are interchangeable without changing templates or the
// runner.
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
// Resolve honors ctx cancellation and deadline for any remote lookup.
type Provider interface {
	// Resolve returns the value for the named secret. A missing secret returns
	// an error wrapping os.ErrNotExist (also available as ErrNotFound) with the
	// name; the value is never part of any error. External providers may return
	// ErrPermission, ErrAuth, or ErrUnavailable for their respective failures.
	Resolve(ctx context.Context, name string) (string, error)
}

// Lister is the optional provider capability for enumerating the secret names
// visible at the provider's configured scope. Provider deliberately stays
// minimal, so a resolver need not implement listing; callers that need it
// type-assert to Lister.
type Lister interface {
	// List returns the secret names at the provider's configured scope,
	// deterministically sorted. It never returns secret values. A missing local
	// directory or an empty remote scope returns an empty list, not an error.
	// Errors carry names and scope only, never a secret value. External
	// providers may return ErrPermission, ErrAuth, ErrNotFound, or
	// ErrUnavailable for their respective failures.
	List(ctx context.Context) ([]string, error)
}
