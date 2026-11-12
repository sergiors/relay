package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Secrets-provider values and defaults. SECRETS_PROVIDER is deliberately a
// closed set: an unknown value is a startup error, never a silent local
// fallback. The default site URL is Infisical Cloud; self-hosted deployments
// set INFISICAL_SITE_URL to their own https origin. The site URL and secret
// path are non-secret configuration, while the client credentials are supplied
// only to the provider and never forwarded to workloads.
const (
	SecretsProviderLocal     = "local"
	SecretsProviderInfisical = "infisical"

	DefaultSecretsProvider     = SecretsProviderLocal
	DefaultInfisicalSiteURL    = "https://app.infisical.com"
	DefaultInfisicalSecretPath = "/"
)

// SecretsConfig groups the secret-provider configuration resolved from
// SECRETS_PROVIDER and the INFISICAL_* variables. Load embeds it as Config's
// Secrets field, and the administrative CLI resolves it through the narrow
// LoadSecretsConfig loader without requiring any Redis, Docker, or PostgreSQL
// configuration. Infisical is populated only when Provider is
// SecretsProviderInfisical. The client secret is credential material and is
// never logged.
type SecretsConfig struct {
	Provider  string
	Infisical InfisicalConfig
}

// InfisicalConfig holds the Infisical provider settings. SiteURL defaults to
// DefaultInfisicalSiteURL and must be an absolute https:// URL without
// credentials; SecretPath defaults to "/" and must begin with "/". Project,
// environment, client ID, and client secret are required by the provider. The
// client secret is never placed in app/service environments and never persisted
// to state or images.
type InfisicalConfig struct {
	SiteURL      string
	ProjectID    string
	Environment  string
	SecretPath   string
	ClientID     string
	ClientSecret string
}

// LoadSecretsConfig resolves and validates SECRETS_PROVIDER and, only when the
// Infisical provider is selected, its required settings. It reads no other
// environment configuration, so a standalone command can select the active
// secret provider without requiring the worker's Redis, Docker, or database
// settings. The default remains the local provider, preserving existing
// deployments exactly. An unknown provider is rejected so a typo can never
// silently fall back to local storage (or vice versa). In Infisical mode the
// site URL must be https, because Relay never disables TLS verification, and
// every required credential is named in the returned error without echoing its
// value.
func LoadSecretsConfig() (SecretsConfig, error) {
	raw := strings.TrimSpace(os.Getenv("SECRETS_PROVIDER"))
	if raw == "" {
		return SecretsConfig{Provider: DefaultSecretsProvider}, nil
	}
	provider := strings.ToLower(raw)
	switch provider {
	case SecretsProviderLocal:
		return SecretsConfig{Provider: SecretsProviderLocal}, nil
	case SecretsProviderInfisical:
	default:
		return SecretsConfig{}, fmt.Errorf(
			"invalid SECRETS_PROVIDER %q: must be %q or %q",
			raw, SecretsProviderLocal, SecretsProviderInfisical,
		)
	}

	siteURL := strings.TrimRight(
		strings.TrimSpace(getEnv("INFISICAL_SITE_URL", DefaultInfisicalSiteURL)),
		"/",
	)
	if err := validateInfisicalSiteURL(siteURL); err != nil {
		return SecretsConfig{}, err
	}

	projectID, err := requiredInfisicalEnv("INFISICAL_PROJECT_ID")
	if err != nil {
		return SecretsConfig{}, err
	}
	environment, err := requiredInfisicalEnv("INFISICAL_ENVIRONMENT")
	if err != nil {
		return SecretsConfig{}, err
	}
	clientID, err := requiredInfisicalEnv("INFISICAL_CLIENT_ID")
	if err != nil {
		return SecretsConfig{}, err
	}
	clientSecret, err := requiredInfisicalEnv("INFISICAL_CLIENT_SECRET")
	if err != nil {
		return SecretsConfig{}, err
	}

	secretPath := strings.TrimSpace(getEnv("INFISICAL_SECRET_PATH", DefaultInfisicalSecretPath))
	if secretPath == "" {
		secretPath = DefaultInfisicalSecretPath
	}
	if !strings.HasPrefix(secretPath, "/") {
		return SecretsConfig{}, fmt.Errorf(
			"invalid INFISICAL_SECRET_PATH %q: must begin with '/'", secretPath,
		)
	}

	return SecretsConfig{
		Provider: SecretsProviderInfisical,
		Infisical: InfisicalConfig{
			SiteURL:      siteURL,
			ProjectID:    projectID,
			Environment:  environment,
			SecretPath:   secretPath,
			ClientID:     clientID,
			ClientSecret: clientSecret,
		},
	}, nil
}

// requiredInfisicalEnv resolves a required Infisical setting, naming both the
// variable and the selected provider in the error. The value is returned
// verbatim (whitespace-only counts as unset) so an operator-supplied client
// secret is never altered.
func requiredInfisicalEnv(key string) (string, error) {
	value := os.Getenv(key)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf(
			"required environment variable %s is not set when SECRETS_PROVIDER=%s",
			key, SecretsProviderInfisical,
		)
	}
	return value, nil
}

// validateInfisicalSiteURL enforces the transport security invariant: the
// site base is an absolute https URL with a host and without embedded
// credentials. TLS verification itself is never configurable. Parsing errors
// are not wrapped because url.Parse echoes the input, which could contain
// credential material an operator accidentally placed in the URL.
func validateInfisicalSiteURL(siteURL string) error {
	u, err := url.Parse(siteURL)
	if err != nil || !strings.EqualFold(u.Scheme, "https") ||
		u.Host == "" || u.User != nil {
		return fmt.Errorf(
			"invalid INFISICAL_SITE_URL: must be an absolute https:// URL without credentials",
		)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf(
			"invalid INFISICAL_SITE_URL: must not contain a query or fragment",
		)
	}
	return nil
}
