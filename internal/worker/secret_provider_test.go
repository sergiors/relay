package worker

import (
	"strings"
	"testing"

	"relay/internal/config"
	"relay/internal/secrets"
)

// validInfisicalConfig returns a Config selecting the Infisical provider with
// every required setting present. Constructing the provider performs no
// network login, so this stays a pure unit test.
func validInfisicalConfig() config.Config {
	return config.Config{
		Secrets: config.SecretsConfig{
			Provider: config.SecretsProviderInfisical,
			Infisical: config.InfisicalConfig{
				SiteURL:      "https://app.infisical.com",
				ProjectID:    "proj-relay-tests",
				Environment:  "staging",
				SecretPath:   "/relay/secrets",
				ClientID:     "universal-auth-client-id",
				ClientSecret: "universal-auth-client-secret",
			},
		},
	}
}

// TestNewSecretProviderSelectsConfiguredProvider pins the worker's wiring of
// SECRETS_PROVIDER: an empty Config behaves like the documented local default,
// "local" selects the file provider, and "infisical" selects the Infisical
// provider — never the other way around.
func TestNewSecretProviderSelectsConfiguredProvider(t *testing.T) {
	t.Run("empty config falls back to the local default", func(t *testing.T) {
		provider, err := newSecretProvider(config.Config{})
		if err != nil {
			t.Fatalf("newSecretProvider: %v", err)
		}
		if _, ok := provider.(*secrets.LocalProvider); !ok {
			t.Fatalf("provider = %T, want *secrets.LocalProvider", provider)
		}
	})

	t.Run("local selects the local file provider", func(t *testing.T) {
		provider, err := newSecretProvider(config.Config{
			Secrets: config.SecretsConfig{Provider: config.SecretsProviderLocal},
		})
		if err != nil {
			t.Fatalf("newSecretProvider: %v", err)
		}
		if _, ok := provider.(*secrets.LocalProvider); !ok {
			t.Fatalf("provider = %T, want *secrets.LocalProvider", provider)
		}
	})

	t.Run("infisical selects the Infisical provider", func(t *testing.T) {
		provider, err := newSecretProvider(validInfisicalConfig())
		if err != nil {
			t.Fatalf("newSecretProvider: %v", err)
		}
		if _, ok := provider.(*secrets.InfisicalProvider); !ok {
			t.Fatalf("provider = %T, want the Infisical provider", provider)
		}
	})
}

// TestNewSecretProviderRejectsUnknownProvider pins that an unsupported
// SECRETS_PROVIDER fails startup instead of silently falling back to local
// secret files, which would read secrets from the wrong place.
func TestNewSecretProviderRejectsUnknownProvider(t *testing.T) {
	provider, err := newSecretProvider(config.Config{Secrets: config.SecretsConfig{Provider: "vault"}})
	if err == nil {
		t.Fatalf("newSecretProvider = (%v, nil), want an error", provider)
	}
	if !strings.Contains(err.Error(), `unsupported secret provider "vault"`) {
		t.Fatalf("error = %v, want it to name the unsupported value", err)
	}
}

// TestNewSecretProviderInfisicalConfigErrorIsFatal pins that invalid Infisical
// settings fail worker startup with the provider's configuration error, and
// that the error never quotes the client secret.
func TestNewSecretProviderInfisicalConfigErrorIsFatal(t *testing.T) {
	cfg := validInfisicalConfig()
	cfg.Secrets.Infisical.Environment = ""

	provider, err := newSecretProvider(cfg)
	if err == nil {
		t.Fatalf("newSecretProvider = (%v, nil), want a configuration error", provider)
	}
	if !strings.Contains(err.Error(), "environment") {
		t.Fatalf("error = %v, want it to name the missing setting", err)
	}
	if strings.Contains(err.Error(), cfg.Secrets.Infisical.ClientSecret) {
		t.Fatalf("error leaked the client secret: %v", err)
	}
}
