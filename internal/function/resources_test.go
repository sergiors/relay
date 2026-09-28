package function

import (
	"strings"
	"testing"
)

// baseResourceTemplate wraps the given resources block in a minimal valid
// template (one event rule) so parsing exercises resources resolution alongside
// an otherwise-valid document.
func baseResourceTemplate(resources string) string {
	body := "runtime: node24\n"
	if resources != "" {
		body += "resources:\n" + resources
	}
	return body + `events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`
}

// TestParseTemplateResourceDefaults pins that a template with no `resources`
// key resolves to the package defaults, and that an empty mapping does too.
func TestParseTemplateResourceDefaults(t *testing.T) {
	def := DefaultResourceLimits()
	for _, name := range []string{"omitted", "empty"} {
		t.Run(name, func(t *testing.T) {
			resources := ""
			if name == "empty" {
				resources = "  {}\n"
			}
			tmpl, err := ParseTemplate([]byte(baseResourceTemplate(resources)))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if tmpl.Resources != def {
				t.Fatalf("resources = %+v, want defaults %+v", tmpl.Resources, def)
			}
			// The accessor agrees and is nil-safe.
			if got := tmpl.ResourceLimits(); got != def {
				t.Fatalf("ResourceLimits() = %+v, want %+v", got, def)
			}
		})
	}
}

// TestParseTemplateResourcePartialOverride pins that each field resolves
// independently: a partial override keeps the defaults for the fields it omits.
func TestParseTemplateResourcePartialOverride(t *testing.T) {
	def := DefaultResourceLimits()

	tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  memory: 256MiB\n")))
	if err != nil {
		t.Fatalf("parse memory only: %v", err)
	}
	if tmpl.Resources.MemoryBytes != 256<<20 {
		t.Errorf("memory = %d, want 256MiB", tmpl.Resources.MemoryBytes)
	}
	if tmpl.Resources.NanoCPUs != def.NanoCPUs || tmpl.Resources.PidsLimit != def.PidsLimit {
		t.Errorf("cpus/pids = %d/%d, want defaults %d/%d",
			tmpl.Resources.NanoCPUs, tmpl.Resources.PidsLimit, def.NanoCPUs, def.PidsLimit)
	}

	tmpl, err = ParseTemplate([]byte(baseResourceTemplate("  cpus: 0.5\n")))
	if err != nil {
		t.Fatalf("parse cpus only: %v", err)
	}
	if tmpl.Resources.NanoCPUs != 500_000_000 {
		t.Errorf("cpus = %d, want 500,000,000 nano", tmpl.Resources.NanoCPUs)
	}
	if tmpl.Resources.MemoryBytes != def.MemoryBytes || tmpl.Resources.PidsLimit != def.PidsLimit {
		t.Errorf("memory/pids = %d/%d, want defaults",
			tmpl.Resources.MemoryBytes, tmpl.Resources.PidsLimit)
	}

	tmpl, err = ParseTemplate([]byte(baseResourceTemplate("  pids: 64\n")))
	if err != nil {
		t.Fatalf("parse pids only: %v", err)
	}
	if tmpl.Resources.PidsLimit != 64 {
		t.Errorf("pids = %d, want 64", tmpl.Resources.PidsLimit)
	}
	if tmpl.Resources.MemoryBytes != def.MemoryBytes || tmpl.Resources.NanoCPUs != def.NanoCPUs {
		t.Errorf("memory/cpus = %d/%d, want defaults",
			tmpl.Resources.MemoryBytes, tmpl.Resources.NanoCPUs)
	}

	// All three at once, including fractional cpus and a GiB memory.
	tmpl, err = ParseTemplate([]byte(baseResourceTemplate("  memory: 1GiB\n  cpus: 2.5\n  pids: 256\n")))
	if err != nil {
		t.Fatalf("parse full: %v", err)
	}
	if tmpl.Resources.MemoryBytes != 1<<30 || tmpl.Resources.NanoCPUs != 2_500_000_000 || tmpl.Resources.PidsLimit != 256 {
		t.Fatalf("resources = %+v, want 1GiB/2.5CPU/256", tmpl.Resources)
	}
	if got := tmpl.Resources.CPUs(); got != 2.5 {
		t.Errorf("CPUs() = %v, want 2.5", got)
	}
}

// TestParseTemplateResourceMemoryUnits pins the accepted binary units (KiB/MiB/
// GiB) and rejects decimal units, a bare number, zero, and a negative.
func TestParseTemplateResourceMemoryUnits(t *testing.T) {
	ok := map[string]int64{
		"64KiB":  64 << 10,
		"256MiB": 256 << 20,
		"2GiB":   2 << 30,
	}
	for raw, want := range ok {
		tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  memory: " + raw + "\n")))
		if err != nil {
			t.Errorf("memory %q: %v", raw, err)
			continue
		}
		if tmpl.Resources.MemoryBytes != want {
			t.Errorf("memory %q = %d, want %d", raw, tmpl.Resources.MemoryBytes, want)
		}
	}

	bad := []string{"512KB", "512MB", "1GB", "512", "-1MiB", "0MiB", "MiB", "1.5MiB", "1 MiB", "1mib", "1GIB"}
	for _, raw := range bad {
		if _, err := ParseTemplate([]byte(baseResourceTemplate("  memory: " + raw + "\n"))); err == nil {
			t.Errorf("memory %q must be rejected", raw)
		}
	}
	// A non-string (a bare YAML number) is rejected too.
	if _, err := ParseTemplate([]byte(baseResourceTemplate("  memory: 268435456\n"))); err == nil {
		t.Error("a numeric memory must be rejected (suffix required)")
	}
}

// TestParseTemplateResourceMemoryOverflow pins that a syntactically valid
// memory size whose byte value would overflow int64 is rejected with a clear
// error instead of wrapping to a negative or truncated limit. The tokens are
// large enough that the integer itself parses (so the overflow is caught by the
// multiplication guard, not the positive-size check), and the binary suffix is
// what pushes the product past MaxInt64.
func TestParseTemplateResourceMemoryOverflow(t *testing.T) {
	// Each product is > MaxInt64: the parsed integer is <= MaxInt64, so the
	// multiplication guard in resolveResourceMemory is what rejects it.
	bad := []string{
		"9223372036854775807GiB", // ~7.9e27 bytes
		"9223372036854775807MiB",
		"9223372036854775807KiB",
		"18014398509481984GiB", // 2^54 GiB == 2^84 bytes, exact power of two
	}
	for _, raw := range bad {
		_, err := ParseTemplate([]byte(baseResourceTemplate("  memory: " + raw + "\n")))
		if err == nil {
			t.Errorf("memory %q must be rejected as overflowing", raw)
			continue
		}
		if !strings.Contains(err.Error(), "overflows") {
			t.Errorf("memory %q error = %q, want an overflow explanation", raw, err)
		}
	}
}

// TestParseTemplateResourceCPUsOverflow pins that a finite CPU value so large
// that its NanoCPUs conversion exceeds int64 is rejected clearly (rather than
// saturating, wrapping negative, or being rounded into a bogus limit).
func TestParseTemplateResourceCPUsOverflow(t *testing.T) {
	for _, raw := range []string{"1e20", "1e19", "1e30", "1e300"} {
		_, err := ParseTemplate([]byte(baseResourceTemplate("  cpus: " + raw + "\n")))
		if err == nil {
			t.Errorf("cpus %q must be rejected as too large", raw)
			continue
		}
		if !strings.Contains(err.Error(), "too large") {
			t.Errorf("cpus %q error = %q, want a too-large explanation", raw, err)
		}
	}
	// A large but in-range value is still accepted, so the overflow guard
	// rejects only genuine overflows rather than everything "big": 9e9 CPUs is
	// 9e18 nano, below MaxInt64.
	tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  cpus: 9e9\n")))
	if err != nil {
		t.Fatalf("large in-range cpus 9e9 must be accepted: %v", err)
	}
	if tmpl.Resources.NanoCPUs != 9_000_000_000_000_000_000 {
		t.Fatalf("cpus 9e9 resolved to %d nano, want 9e18", tmpl.Resources.NanoCPUs)
	}
}

// TestParseTemplateResourcePidsUnsignedOverflow pins that a YAML integer above
// MaxInt64 (which yaml.v3 decodes into `any` as a uint64, not an int64) is
// rejected cleanly: it must never be cast or wrapped into a negative or
// truncated limit. The exact error is asserted so a future widening of the
// accepted integer family is a deliberate change.
func TestParseTemplateResourcePidsUnsignedOverflow(t *testing.T) {
	for _, raw := range []string{
		"9223372036854775808",  // 2^63, the first value above MaxInt64
		"18446744073709551615", // MaxUint64
	} {
		_, err := ParseTemplate([]byte(baseResourceTemplate("  pids: " + raw + "\n")))
		if err == nil {
			t.Errorf("pids %q must be rejected (it is not a positive int64)", raw)
			continue
		}
		if !strings.Contains(err.Error(), "resources.pids must be a positive integer") {
			t.Errorf("pids %q error = %q, want the positive-integer rejection", raw, err)
		}
	}
}

// TestParseTemplateResourceCPUsValid pins accepted CPU forms (int and float,
// fractional included) and rejects strings, bools, zero, negatives, and
// non-finite values.
func TestParseTemplateResourceCPUsValid(t *testing.T) {
	ok := map[string]int64{
		"1":    1_000_000_000,
		"0.5":  500_000_000,
		"2":    2_000_000_000,
		"1.25": 1_250_000_000,
	}
	for raw, want := range ok {
		tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  cpus: " + raw + "\n")))
		if err != nil {
			t.Errorf("cpus %q: %v", raw, err)
			continue
		}
		if tmpl.Resources.NanoCPUs != want {
			t.Errorf("cpus %q = %d, want %d", raw, tmpl.Resources.NanoCPUs, want)
		}
	}

	bad := []string{"0", "-1", "-0.5", `"1"`, "abc", "true", "false", ".nan", ".inf", "-.inf"}
	// Bare string spellings of the non-finite constants are YAML strings, not
	// numbers, so they are rejected as non-numeric (a distinct path from the
	// float .nan/.inf forms above, which are rejected as non-finite). Both are
	// pinned so a future coercion cannot let either through.
	bad = append(bad, "NaN", "Inf", "+Inf", "-Inf", "inf", "nan")
	for _, raw := range bad {
		if _, err := ParseTemplate([]byte(baseResourceTemplate("  cpus: " + raw + "\n"))); err == nil {
			t.Errorf("cpus %q must be rejected", raw)
		}
	}
	// An explicit null is a no-value, exactly like omitting the field, so it
	// resolves to the default rather than failing.
	tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  cpus: null\n")))
	if err != nil {
		t.Fatalf("cpus null: %v", err)
	}
	if tmpl.Resources.NanoCPUs != DefaultResourceNanoCPUs {
		t.Errorf("cpus null = %d, want the default", tmpl.Resources.NanoCPUs)
	}
}

// TestParseTemplateResourcePidsValid pins positive integers, including
// boundaries, and rejects floats, strings, bools, zero, and negatives.
func TestParseTemplateResourcePidsValid(t *testing.T) {
	for _, raw := range []string{"1", "128", "4096"} {
		tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  pids: " + raw + "\n")))
		if err != nil {
			t.Errorf("pids %q: %v", raw, err)
			continue
		}
		if tmpl.Resources.PidsLimit <= 0 {
			t.Errorf("pids %q resolved to %d", raw, tmpl.Resources.PidsLimit)
		}
	}

	bad := []string{"0", "-1", "1.5", `"128"`, "true", "abc"}
	for _, raw := range bad {
		if _, err := ParseTemplate([]byte(baseResourceTemplate("  pids: " + raw + "\n"))); err == nil {
			t.Errorf("pids %q must be rejected", raw)
		}
	}
	// An explicit null is a no-value, exactly like omitting the field.
	tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  pids: null\n")))
	if err != nil {
		t.Fatalf("pids null: %v", err)
	}
	if tmpl.Resources.PidsLimit != DefaultResourcePidsLimit {
		t.Errorf("pids null = %d, want the default", tmpl.Resources.PidsLimit)
	}
}

// TestParseTemplateResourceMalformed pins that a structurally malformed
// `resources` value (a scalar or list instead of a mapping) is rejected rather
// than silently ignored or coerced, while an unknown mapping key is tolerated.
func TestParseTemplateResourceMalformed(t *testing.T) {
	// A list or scalar instead of a mapping is a hard error.
	for _, resources := range []string{
		"  - 1\n  - 2\n",
		"  5\n",
	} {
		if _, err := ParseTemplate([]byte(baseResourceTemplate(resources))); err == nil {
			t.Errorf("malformed resources %q must be rejected", resources)
		}
	}

	// An unknown field is tolerated by yaml.v3 (ignored), so this remains
	// valid; assert it parses to the specified values so the behavior is pinned
	// rather than accidental.
	tmpl, err := ParseTemplate([]byte(baseResourceTemplate("  memory: 1MiB\n  cpus: 1\n  pids: 1\n  extra: 1\n")))
	if err != nil {
		t.Fatalf("unknown field must be ignored: %v", err)
	}
	if tmpl.Resources.MemoryBytes != 1<<20 {
		t.Fatalf("memory = %d, want 1MiB", tmpl.Resources.MemoryBytes)
	}
}

// TestTemplateResourceLimitsNilSafe pins the accessor contract for a nil
// template and a zero value, which the runtime relies on for hand-built values.
func TestTemplateResourceLimitsNilSafe(t *testing.T) {
	var tmpl *Template
	if got := tmpl.ResourceLimits(); got != DefaultResourceLimits() {
		t.Fatalf("nil template resources = %+v, want defaults", got)
	}
	if got := (ResourceLimits{}).OrDefault(); got != DefaultResourceLimits() {
		t.Fatalf("zero limits OrDefault = %+v, want defaults", got)
	}
	// A partial zero value normalizes field-by-field.
	partial := ResourceLimits{MemoryBytes: 64 << 20}
	got := partial.OrDefault()
	if got.MemoryBytes != 64<<20 || got.NanoCPUs != DefaultResourceNanoCPUs || got.PidsLimit != DefaultResourcePidsLimit {
		t.Fatalf("partial OrDefault = %+v", got)
	}
}

// TestResourceLimitsFingerprint pins the resource fingerprint's key properties:
// deterministic, distinct per field, value-free (fixed hex length), independent
// of zero-vs-default representation (both normalize), and stable.
func TestResourceLimitsFingerprint(t *testing.T) {
	a := ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 128}
	b := ResourceLimits{MemoryBytes: 512 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 128}
	c := ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 2_000_000_000, PidsLimit: 128}
	d := ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 64}

	fp := a.Fingerprint()
	if fp == "" || len(fp) != resourceFingerprintLen {
		t.Fatalf("fingerprint = %q, want %d hex chars", fp, resourceFingerprintLen)
	}
	if fp != a.Fingerprint() {
		t.Fatal("fingerprint must be deterministic")
	}
	for name, other := range map[string]ResourceLimits{"memory": b, "cpus": c, "pids": d} {
		if fp == other.Fingerprint() {
			t.Errorf("changing %s must change the fingerprint", name)
		}
	}
	// A zero value and the explicit defaults fingerprint identically.
	if (ResourceLimits{}).Fingerprint() != DefaultResourceLimits().Fingerprint() {
		t.Error("zero value must fingerprint as the defaults")
	}
	// CPU display conversion.
	if (ResourceLimits{NanoCPUs: 1_500_000_000}).CPUs() != 1.5 {
		t.Error("CPUs() must convert nano to cores")
	}
}
