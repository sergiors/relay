package config

import (
	"os"
	"strings"
	"testing"
)

// clearSecretsEnv forces every SECRETS_PROVIDER/INFISICAL_* variable to a
// truly-UNSET state so ambient operator configuration on the test machine can
// never change what a Load test observes (t.Setenv registers restoration).
func clearSecretsEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SECRETS_PROVIDER",
		"INFISICAL_SITE_URL",
		"INFISICAL_PROJECT_ID",
		"INFISICAL_ENVIRONMENT",
		"INFISICAL_SECRET_PATH",
		"INFISICAL_CLIENT_ID",
		"INFISICAL_CLIENT_SECRET",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

// setInfisicalEnv pins a complete, valid Infisical configuration whose site
// URL and secret path are unset (so they resolve to their defaults), leaving
// each test free to vary exactly the variable it exercises.
func setInfisicalEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SECRETS_PROVIDER", "infisical")
	t.Setenv("INFISICAL_SITE_URL", "")
	os.Unsetenv("INFISICAL_SITE_URL")
	t.Setenv("INFISICAL_PROJECT_ID", "proj-relay-tests")
	t.Setenv("INFISICAL_ENVIRONMENT", "staging")
	t.Setenv("INFISICAL_SECRET_PATH", "")
	os.Unsetenv("INFISICAL_SECRET_PATH")
	t.Setenv("INFISICAL_CLIENT_ID", "universal-auth-client-id")
	t.Setenv("INFISICAL_CLIENT_SECRET", "universal-auth-client-secret")
}

// TestLoadSecretsProviderDefaultsToLocal pins that an unset SECRETS_PROVIDER
// selects the local file provider (the historical behavior) and leaves every
// Infisical setting empty.
func TestLoadSecretsProviderDefaultsToLocal(t *testing.T) {
	setRequiredEnv(t)
	clearSecretsEnv(t)

	cfg := mustLoad(t)
	if cfg.Secrets.Provider != SecretsProviderLocal {
		t.Fatalf("SecretsProvider = %q, want the local default", cfg.Secrets.Provider)
	}
	if DefaultSecretsProvider != SecretsProviderLocal {
		t.Fatalf("DefaultSecretsProvider = %q, want local", DefaultSecretsProvider)
	}
	if cfg.Secrets.Infisical.SiteURL != "" || cfg.Secrets.Infisical.ProjectID != "" ||
		cfg.Secrets.Infisical.Environment != "" || cfg.Secrets.Infisical.SecretPath != "" ||
		cfg.Secrets.Infisical.ClientID != "" || cfg.Secrets.Infisical.ClientSecret != "" {
		t.Fatalf("Infisical fields = %+v, want all empty in local mode", cfg)
	}
}

// TestLoadSecretsProviderLocalIgnoresInfisicalSettings pins that selecting the
// local provider never validates Infisical settings: leftover INFISICAL_* values
// (even unusable ones) must not break startup of a local deployment.
func TestLoadSecretsProviderLocalIgnoresInfisicalSettings(t *testing.T) {
	setRequiredEnv(t)
	clearSecretsEnv(t)
	t.Setenv("SECRETS_PROVIDER", "local")
	t.Setenv("INFISICAL_SITE_URL", "http://not-validated.example")
	t.Setenv("INFISICAL_PROJECT_ID", "")
	os.Unsetenv("INFISICAL_PROJECT_ID")

	cfg := mustLoad(t)
	if cfg.Secrets.Provider != SecretsProviderLocal {
		t.Fatalf("SecretsProvider = %q, want local", cfg.Secrets.Provider)
	}
	if cfg.Secrets.Infisical.SiteURL != "" || cfg.Secrets.Infisical.ProjectID != "" {
		t.Fatalf("Infisical fields = %+v, want empty in local mode", cfg)
	}
}

// TestLoadSecretsProviderLocalAcceptsSpellingVariants pins that the provider
// selector is case-insensitive and whitespace-tolerant for the local value.
func TestLoadSecretsProviderLocalAcceptsSpellingVariants(t *testing.T) {
	for _, value := range []string{"local", "LOCAL", "  local  "} {
		setRequiredEnv(t)
		clearSecretsEnv(t)
		t.Setenv("SECRETS_PROVIDER", value)
		if cfg := mustLoad(t); cfg.Secrets.Provider != SecretsProviderLocal {
			t.Fatalf("SECRETS_PROVIDER=%q resolved to %q, want local", value, cfg.Secrets.Provider)
		}
	}
}

// TestLoadUnknownSecretsProviderFails pins that an unrecognized
// SECRETS_PROVIDER is a startup error naming the offending value instead of a
// silent fallback to local secret files (a typo must never change where
// secrets are read from).
func TestLoadUnknownSecretsProviderFails(t *testing.T) {
	for _, value := range []string{"vault", "INFISICAL_FILES", "infra"} {
		setRequiredEnv(t)
		clearSecretsEnv(t)
		t.Setenv("SECRETS_PROVIDER", value)

		cfg, err := Load(discardLogger())
		if err == nil {
			t.Fatalf("SECRETS_PROVIDER=%q loaded (%+v), want an error", value, cfg)
		}
		if !strings.Contains(err.Error(), value) {
			t.Fatalf("error = %v, want it to name %q", err, value)
		}
		if !strings.Contains(err.Error(), SecretsProviderLocal) ||
			!strings.Contains(err.Error(), SecretsProviderInfisical) {
			t.Fatalf("error = %v, want it to list both allowed providers", err)
		}
	}
}

// TestLoadInfisicalRequiresEveryCredential pins that each required Infisical
// variable fails startup when unset or whitespace-only, with an error naming
// the variable and the selected provider — and never quoting the client
// secret, which is set in every case that can carry it.
func TestLoadInfisicalRequiresEveryCredential(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string // empty means truly unset
	}{
		{"project ID unset", "INFISICAL_PROJECT_ID", ""},
		{"environment whitespace", "INFISICAL_ENVIRONMENT", "   "},
		{"client ID unset", "INFISICAL_CLIENT_ID", ""},
		{"client secret whitespace", "INFISICAL_CLIENT_SECRET", "   "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			setInfisicalEnv(t)
			t.Setenv(tt.key, tt.value)
			if tt.value == "" {
				os.Unsetenv(tt.key)
			}

			cfg, err := Load(discardLogger())
			if err == nil {
				t.Fatalf("Load = (%+v, nil), want an error for a missing %s", cfg, tt.key)
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("error = %v, want it to name %s", err, tt.key)
			}
			if !strings.Contains(err.Error(), "SECRETS_PROVIDER="+SecretsProviderInfisical) {
				t.Fatalf("error = %v, want it to name the selected provider", err)
			}
			assertConfigErrorHidesSecret(t, err)
		})
	}
}

// assertConfigErrorHidesSecret fails if a configuration error quotes the
// fixture client secret.
func assertConfigErrorHidesSecret(t *testing.T, err error) {
	t.Helper()
	if strings.Contains(err.Error(), "universal-auth-client-secret") {
		t.Fatalf("configuration error leaked the client secret: %v", err)
	}
}

// TestLoadInfisicalDefaults pins the Infisical mode defaults: the site URL
// falls back to the Infisical Cloud origin, the secret path to "/", and every
// required value passes through verbatim — including a whitespace-padded
// client secret, which must not be altered.
func TestLoadInfisicalDefaults(t *testing.T) {
	setRequiredEnv(t)
	setInfisicalEnv(t)
	t.Setenv("INFISICAL_CLIENT_SECRET", "  universal-auth-client-secret  ")

	cfg := mustLoad(t)
	if cfg.Secrets.Provider != SecretsProviderInfisical {
		t.Fatalf("SecretsProvider = %q, want infisical", cfg.Secrets.Provider)
	}
	if cfg.Secrets.Infisical.SiteURL != DefaultInfisicalSiteURL {
		t.Fatalf("InfisicalSiteURL = %q, want %q",
			cfg.Secrets.Infisical.SiteURL, DefaultInfisicalSiteURL)
	}
	if cfg.Secrets.Infisical.SecretPath != DefaultInfisicalSecretPath {
		t.Fatalf("InfisicalSecretPath = %q, want %q",
			cfg.Secrets.Infisical.SecretPath, DefaultInfisicalSecretPath)
	}
	if cfg.Secrets.Infisical.ProjectID != "proj-relay-tests" ||
		cfg.Secrets.Infisical.Environment != "staging" ||
		cfg.Secrets.Infisical.ClientID != "universal-auth-client-id" {
		t.Fatalf("Infisical identity = (%q, %q, %q), want the fixture values",
			cfg.Secrets.Infisical.ProjectID, cfg.Secrets.Infisical.Environment, cfg.Secrets.Infisical.ClientID)
	}
	if cfg.Secrets.Infisical.ClientSecret != "  universal-auth-client-secret  " {
		t.Fatalf("InfisicalClientSecret = %q, want the value verbatim",
			cfg.Secrets.Infisical.ClientSecret)
	}
}

// TestLoadInfisicalExplicitSettingsPassThrough pins that a self-hosted site
// URL is accepted with its trailing slash trimmed (matching the provider's
// base URL) and that an explicit secret path is passed through unchanged.
func TestLoadInfisicalExplicitSettingsPassThrough(t *testing.T) {
	setRequiredEnv(t)
	setInfisicalEnv(t)
	t.Setenv("INFISICAL_SITE_URL", "https://vault.internal.example/")
	t.Setenv("INFISICAL_SECRET_PATH", "/team/app")

	cfg := mustLoad(t)
	if cfg.Secrets.Infisical.SiteURL != "https://vault.internal.example" {
		t.Fatalf("InfisicalSiteURL = %q, want the trailing slash trimmed", cfg.Secrets.Infisical.SiteURL)
	}
	if cfg.Secrets.Infisical.SecretPath != "/team/app" {
		t.Fatalf("InfisicalSecretPath = %q, want /team/app", cfg.Secrets.Infisical.SecretPath)
	}
}

// TestLoadInfisicalRejectsUnsafeSiteURL pins the transport invariant: in
// Infisical mode the site URL must be an absolute https URL without
// credentials, query, or fragment — and neither the offending input nor any
// credential embedded in it may be echoed by the error.
func TestLoadInfisicalRejectsUnsafeSiteURL(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		forbid string
	}{
		{"plain http", "http://vault.internal.example", ""},
		{"embedded credentials", "https://operator:hunter2@vault.internal.example", "hunter2"},
		{"no scheme", "vault.internal.example", ""},
		{"no host", "https://", ""},
		{"query", "https://vault.internal.example/?token=abc", "token=abc"},
		{"fragment", "https://vault.internal.example/#supersecret", "supersecret"},
		{"malformed", "https://exa mple.com", "exa mple"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			setInfisicalEnv(t)
			t.Setenv("INFISICAL_SITE_URL", tt.value)

			cfg, err := Load(discardLogger())
			if err == nil {
				t.Fatalf("Load = (%+v, nil), want an error for %q", cfg, tt.value)
			}
			if !strings.Contains(err.Error(), "INFISICAL_SITE_URL") {
				t.Fatalf("error = %v, want it to name INFISICAL_SITE_URL", err)
			}
			if tt.forbid != "" && strings.Contains(err.Error(), tt.forbid) {
				t.Fatalf("error echoed %q: %v", tt.forbid, err)
			}
			assertConfigErrorHidesSecret(t, err)
		})
	}
}

// TestLoadInfisicalRejectsRelativeSecretPath pins that a secret path must be
// absolute, while a whitespace-only path falls back to the default "/".
func TestLoadInfisicalRejectsRelativeSecretPath(t *testing.T) {
	t.Run("relative path fails naming the variable", func(t *testing.T) {
		setRequiredEnv(t)
		setInfisicalEnv(t)
		t.Setenv("INFISICAL_SECRET_PATH", "team/app")

		cfg, err := Load(discardLogger())
		if err == nil {
			t.Fatalf("Load = (%+v, nil), want an error", cfg)
		}
		if !strings.Contains(err.Error(), "INFISICAL_SECRET_PATH") {
			t.Fatalf("error = %v, want it to name INFISICAL_SECRET_PATH", err)
		}
	})

	t.Run("whitespace path falls back to the default", func(t *testing.T) {
		setRequiredEnv(t)
		setInfisicalEnv(t)
		t.Setenv("INFISICAL_SECRET_PATH", "   ")

		cfg := mustLoad(t)
		if cfg.Secrets.Infisical.SecretPath != DefaultInfisicalSecretPath {
			t.Fatalf("InfisicalSecretPath = %q, want %q",
				cfg.Secrets.Infisical.SecretPath, DefaultInfisicalSecretPath)
		}
	})
}

// TestLoadSecretsConfigNeedsNoWorkerEnvironment pins the narrow loader's
// standalone contract: without the worker's required Redis, database, and
// Docker environment (setRequiredEnv is deliberately not called) it still
// resolves the local default with empty Infisical fields, so an
// administrative command like `relay secret ls` needs no worker
// configuration.
func TestLoadSecretsConfigNeedsNoWorkerEnvironment(t *testing.T) {
	clearSecretsEnv(t)

	cfg, err := LoadSecretsConfig()
	if err != nil {
		t.Fatalf("LoadSecretsConfig: %v", err)
	}
	if cfg.Provider != SecretsProviderLocal {
		t.Fatalf("Provider = %q, want the local default", cfg.Provider)
	}
	if cfg.Infisical.SiteURL != "" || cfg.Infisical.ProjectID != "" ||
		cfg.Infisical.Environment != "" || cfg.Infisical.SecretPath != "" ||
		cfg.Infisical.ClientID != "" || cfg.Infisical.ClientSecret != "" {
		t.Fatalf("Infisical fields = %+v, want all empty in local mode", cfg)
	}
}

// TestLoadSecretsConfigResolvesInfisicalSubset pins that the narrow loader
// returns exactly the provider subset Load embeds into Config: identity and
// credentials verbatim (including a whitespace-padded client secret), with
// the documented site URL and secret path defaults applied.
func TestLoadSecretsConfigResolvesInfisicalSubset(t *testing.T) {
	clearSecretsEnv(t)
	setInfisicalEnv(t)
	t.Setenv("INFISICAL_CLIENT_SECRET", "  universal-auth-client-secret  ")

	cfg, err := LoadSecretsConfig()
	if err != nil {
		t.Fatalf("LoadSecretsConfig: %v", err)
	}
	if cfg.Provider != SecretsProviderInfisical {
		t.Fatalf("Provider = %q, want infisical", cfg.Provider)
	}
	if cfg.Infisical.SiteURL != DefaultInfisicalSiteURL {
		t.Fatalf("InfisicalSiteURL = %q, want %q",
			cfg.Infisical.SiteURL, DefaultInfisicalSiteURL)
	}
	if cfg.Infisical.SecretPath != DefaultInfisicalSecretPath {
		t.Fatalf("InfisicalSecretPath = %q, want %q",
			cfg.Infisical.SecretPath, DefaultInfisicalSecretPath)
	}
	if cfg.Infisical.ProjectID != "proj-relay-tests" ||
		cfg.Infisical.Environment != "staging" ||
		cfg.Infisical.ClientID != "universal-auth-client-id" {
		t.Fatalf("Infisical identity = (%q, %q, %q), want the fixture values",
			cfg.Infisical.ProjectID, cfg.Infisical.Environment, cfg.Infisical.ClientID)
	}
	if cfg.Infisical.ClientSecret != "  universal-auth-client-secret  " {
		t.Fatalf("InfisicalClientSecret = %q, want the value verbatim",
			cfg.Infisical.ClientSecret)
	}
}

// TestLoadSecretsConfigRejectsInvalidSelection pins that the narrow loader
// performs the same validation as Load: an unrecognized provider names the
// offending value and both allowed providers, and a missing credential names
// the variable and the selected provider — never quoting the client secret.
func TestLoadSecretsConfigRejectsInvalidSelection(t *testing.T) {
	t.Run("unknown provider", func(t *testing.T) {
		clearSecretsEnv(t)
		t.Setenv("SECRETS_PROVIDER", "vault")

		cfg, err := LoadSecretsConfig()
		if err == nil {
			t.Fatalf("LoadSecretsConfig = (%+v, nil), want an error", cfg)
		}
		if !strings.Contains(err.Error(), "vault") ||
			!strings.Contains(err.Error(), SecretsProviderLocal) ||
			!strings.Contains(err.Error(), SecretsProviderInfisical) {
			t.Fatalf("error = %v, want it to name the value and both providers", err)
		}
	})

	t.Run("missing client secret", func(t *testing.T) {
		clearSecretsEnv(t)
		setInfisicalEnv(t)
		unsetInfisicalEnvVar(t, "INFISICAL_CLIENT_SECRET")

		_, err := LoadSecretsConfig()
		if err == nil {
			t.Fatal("LoadSecretsConfig = nil error, want a missing-credential error")
		}
		if !strings.Contains(err.Error(), "INFISICAL_CLIENT_SECRET") ||
			!strings.Contains(err.Error(), "SECRETS_PROVIDER="+SecretsProviderInfisical) {
			t.Fatalf("error = %v, want it to name the variable and provider", err)
		}
		assertConfigErrorHidesSecret(t, err)
	})
}

// unsetInfisicalEnvVar clears one provider variable after setInfisicalEnv has
// pinned the rest, for incomplete-configuration scenarios.
func unsetInfisicalEnvVar(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	os.Unsetenv(key)
}
