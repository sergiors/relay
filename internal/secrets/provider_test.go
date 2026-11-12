package secrets

import (
	"path/filepath"
	"strings"
	"testing"
)

// validInfisicalTestOptions returns a complete Infisical configuration for
// selector tests; construction performs no network login, so these stay pure
// unit tests.
func validInfisicalTestOptions() InfisicalOptions {
	return InfisicalOptions{
		ProjectID:    infisicalTestProject,
		Environment:  infisicalTestEnvironment,
		ClientID:     infisicalTestClientID,
		ClientSecret: infisicalTestClientSecret,
	}
}

// TestNewProviderSelectsProviderBySelector pins the single selection path the
// worker and the `relay secret` commands share: the empty selector and
// "local" both construct the file provider rooted at LocalDir (with listing
// reading that directory), and "infisical" constructs the remote provider.
func TestNewProviderSelectsProviderBySelector(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "secrets")
	store, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	for _, name := range []string{"zeta", "alpha"} {
		if err := store.Set(t.Context(), name, "v"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	for _, selector := range []string{"", "local"} {
		t.Run("selector "+selector+" selects local files", func(t *testing.T) {
			p, err := NewProvider(ProviderOptions{
				Provider:  selector,
				LocalDir:  dir,
				Infisical: validInfisicalTestOptions(),
			})
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			if _, ok := p.(*LocalProvider); !ok {
				t.Fatalf("provider = %T, want *LocalProvider", p)
			}
			lister, ok := p.(Lister)
			if !ok {
				t.Fatalf("provider = %T, want the Lister capability", p)
			}
			names, err := lister.List(t.Context())
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if got := strings.Join(names, ","); got != "alpha,zeta" {
				t.Fatalf("names = %q, want the LocalDir contents alpha,zeta", got)
			}
		})
	}

	t.Run("selector infisical selects the remote provider", func(t *testing.T) {
		p, err := NewProvider(ProviderOptions{
			Provider:  "infisical",
			LocalDir:  dir,
			Infisical: validInfisicalTestOptions(),
		})
		if err != nil {
			t.Fatalf("NewProvider: %v", err)
		}
		if _, ok := p.(*InfisicalProvider); !ok {
			t.Fatalf("provider = %T, want *InfisicalProvider", p)
		}
	})
}

// TestNewProviderRejectsUnknownSelectorWithoutFallback pins that an
// unsupported selector is an error naming the offending value — never a
// silent fallback to local files, which would read secrets from the wrong
// place.
func TestNewProviderRejectsUnknownSelectorWithoutFallback(t *testing.T) {
	provider, err := NewProvider(ProviderOptions{
		Provider:  "vault",
		LocalDir:  filepath.Join(t.TempDir(), "secrets"),
		Infisical: validInfisicalTestOptions(),
	})
	if err == nil {
		t.Fatalf("NewProvider = (%v, nil), want an error", provider)
	}
	if provider != nil {
		t.Fatalf("provider = %v, want nil alongside the error", provider)
	}
	if !strings.Contains(err.Error(), `unsupported secret provider "vault"`) {
		t.Fatalf("error = %v, want it to name the unsupported value", err)
	}
}

// TestNewProviderRejectsIncompleteInfisicalConfig pins that construction
// validates the Infisical settings themselves, so a standalone command fails
// on configuration without any network access — and a whitespace-only
// credential counts as missing without the raw value being echoed.
func TestNewProviderRejectsIncompleteInfisicalConfig(t *testing.T) {
	opts := validInfisicalTestOptions()
	opts.ClientSecret = "   "

	provider, err := NewProvider(ProviderOptions{Provider: "infisical", Infisical: opts})
	if err == nil {
		t.Fatalf("NewProvider = (%v, nil), want a configuration error", provider)
	}
	if !strings.Contains(err.Error(), "client secret") {
		t.Fatalf("error = %v, want it to name the missing setting", err)
	}
}
