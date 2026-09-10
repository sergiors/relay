package secrets

import (
	"context"
	"testing"

	"relay/internal/function"
)

// TestSecretNameValidationMatchesFunctionRules pins the equivalence of the two
// duplicated secret-name validators: internal/secrets.ValidateName and
// internal/function.ValidSecretName must agree on every valid and invalid name,
// so a secret reference accepted by a template is always accepted by the store
// (and vice versa). The duplication is deliberate — function is a leaf package
// and cannot import secrets — and this test is the cross-check that keeps them
// in sync.
func TestSecretNameValidationMatchesFunctionRules(t *testing.T) {
	valid := []string{
		"db", "database-url", "api_key", "jobs.v2", "a", "a1", "a-b_c.d",
	}
	invalid := []string{
		"", "UPPER", "has space", "a/b", "../etc", "/abs", "a\\b", "trail.",
		"-lead", ".lead", "a.b.",
	}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want nil", name, err)
		}
		if err := function.ValidSecretName(name); err != nil {
			t.Errorf("function.ValidSecretName(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range invalid {
		if err := ValidateName(name); err == nil {
			t.Errorf("ValidateName(%q) = nil, want error", name)
		}
		if err := function.ValidSecretName(name); err == nil {
			t.Errorf("function.ValidSecretName(%q) = nil, want error", name)
		}
	}
}

// TestLocalProviderResolve verifies the Provider interface is satisfied and
// resolves through the store.
func TestLocalProviderResolve(t *testing.T) {
	s := newTestStore(t)
	if err := s.Set(context.Background(), "tok", "v"); err != nil {
		t.Fatalf("set: %v", err)
	}
	p, err := NewLocalProvider(s.Dir())
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	got, err := p.Resolve(context.Background(), "tok")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "v" {
		t.Fatalf("resolved = %q, want v", got)
	}
}
