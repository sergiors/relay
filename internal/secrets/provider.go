package secrets

import "fmt"

// ProviderOptions selects and configures a Provider. Provider is the normalized
// selector ("local" or "infisical"); an empty value selects the local default.
// LocalDir roots the local file provider. Infisical configures the remote
// provider and is ignored when the local provider is selected.
type ProviderOptions struct {
	Provider  string
	LocalDir  string
	Infisical InfisicalOptions
}

// NewProvider constructs the provider selected by opts.Provider. Construction
// validates configuration only: the Infisical provider performs no network
// login here, so startup and standalone commands stay resilient to a
// temporarily unavailable remote. An unknown selector is an error, never a
// silent local fallback.
//
// The selector literals mirror config.SecretsProvider*; this leaf package does
// not import config (which owns environment resolution), so the already
// normalized values are matched here.
func NewProvider(opts ProviderOptions) (Provider, error) {
	switch opts.Provider {
	case "", "local":
		return NewLocalProvider(opts.LocalDir)
	case "infisical":
		return NewInfisicalProvider(opts.Infisical)
	default:
		return nil, fmt.Errorf("unsupported secret provider %q", opts.Provider)
	}
}
