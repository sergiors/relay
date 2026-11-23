package routing

import (
	"strings"
	"testing"
)

// ValidateHostOverride accepts valid hostnames, rejects malformed ones with an
// error naming ROUTING_HOST_OVERRIDE, and treats unset/whitespace-only as "no
// override" (valid by omission). The value is used VERBATIM as the effective
// route host, so the full hostname contract applies to the value itself.
func TestValidateHostOverride(t *testing.T) {
	for _, valid := range []string{"localhost", "a.test", "123.io", "sub.domain.example.com"} {
		if err := ValidateHostOverride(valid); err != nil {
			t.Errorf("ValidateHostOverride(%q) = %v, want nil", valid, err)
		}
	}
	for _, invalid := range []string{"-bad", "bad-", "a..b", "a_b", "a b", "bad/name", "a.", ".a"} {
		err := ValidateHostOverride(invalid)
		if err == nil {
			t.Errorf("ValidateHostOverride(%q) = nil, want an error", invalid)
			continue
		}
		if !strings.Contains(err.Error(), "ROUTING_HOST_OVERRIDE") {
			t.Errorf("ValidateHostOverride(%q) error %v does not name ROUTING_HOST_OVERRIDE",
				invalid, err)
		}
	}
	// Unset and whitespace-only both mean "no override" and are valid.
	if err := ValidateHostOverride(""); err != nil {
		t.Fatalf("ValidateHostOverride(\"\") = %v, want nil", err)
	}
	if err := ValidateHostOverride("   \t"); err != nil {
		t.Fatalf("ValidateHostOverride(whitespace) = %v, want nil", err)
	}
	// The override itself must fit the 253-character hostname limit.
	err := ValidateHostOverride(strings.Repeat("a", 254))
	if err == nil {
		t.Fatal("ValidateHostOverride(254 chars) = nil, want the length error")
	}
	if !strings.Contains(err.Error(), "253-character hostname limit") ||
		!strings.Contains(err.Error(), "ROUTING_HOST_OVERRIDE") {
		t.Fatalf("long-override error = %v, want the 253-character error naming "+
			"ROUTING_HOST_OVERRIDE", err)
	}
}

// HostOverrideConfigured: unset and whitespace-only mean no override.
func TestHostOverrideConfigured(t *testing.T) {
	for _, unset := range []string{"", "   ", "\t\n "} {
		if HostOverrideConfigured(unset) {
			t.Errorf("HostOverrideConfigured(%q) = true, want false", unset)
		}
	}
	for _, set := range []string{"localhost", "x", " portal.test "} {
		if !HostOverrideConfigured(set) {
			t.Errorf("HostOverrideConfigured(%q) = false, want true", set)
		}
	}
}

// EffectiveHost is an EXACT full-host replacement: the declared host verbatim
// when the override is unset or whitespace-only, otherwise the override value
// verbatim — never a suffix mapping or a left-label join.
func TestEffectiveHost(t *testing.T) {
	for _, tt := range []struct {
		name     string
		host     string
		override string
		want     string
	}{
		{"unset keeps the declared host", "issuer.example.com", "",
			"issuer.example.com"},
		{"whitespace-only keeps the declared host", "issuer.example.com", "   ",
			"issuer.example.com"},
		{"exact replacement is never a suffix mapping",
			"issuer.example.com", "localhost", "localhost"},
		{"multi-label override replaces wholesale",
			"issuer.example.com", "portal.internal.test", "portal.internal.test"},
		{"single-label declared host replaced", "issuer", "local.test", "local.test"},
		{"declared host is passed by value, never mutated",
			"my-app.example.com", "my-app.example.com", "my-app.example.com"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := EffectiveHost(tt.host, tt.override); got != tt.want {
				t.Fatalf("EffectiveHost(%q, %q) = %q, want %q",
					tt.host, tt.override, got, tt.want)
			}
		})
	}
}
