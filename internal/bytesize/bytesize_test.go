package bytesize

import (
	"math"
	"strings"
	"testing"
)

// TestParseSize pins the general binary-size notation: B, KiB, MiB, and GiB with
// binary multipliers, plus a bare integer byte count for backwards
// compatibility, with surrounding whitespace trimmed. Decimal units, fractions,
// signs, zero, and malformed input are rejected.
func TestParseSize(t *testing.T) {
	ok := []struct {
		value string
		want  int64
	}{
		{"1B", 1},
		{"256B", 256},
		{"1KiB", 1 << 10},
		{"256KiB", 256 << 10},
		{"1MiB", 1 << 20},
		{"2GiB", 2 << 30},
		{" 256KiB ", 256 << 10},
		{"262144", 262144},   // bare bytes (legacy integer form)
		{"1048576", 1 << 20}, // bare bytes at the cap
	}
	for _, tc := range ok {
		got, err := ParseSize(tc.value)
		if err != nil {
			t.Errorf("ParseSize(%q) error: %v", tc.value, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tc.value, got, tc.want)
		}
	}

	bad := []string{
		"", "  ", "0", "0B", "0KiB", "-1", "-1KiB", "1.5MiB",
		"512KB", "1MB", "1GB", "1kb", "1kib", "1mib", "MiB", "B", "abc",
		"1 KiB", "256KiBx", "1e3",
	}
	for _, value := range bad {
		if _, err := ParseSize(value); err == nil {
			t.Errorf("ParseSize(%q) must be rejected", value)
		}
	}
}

// TestParseSizeOverflow pins that a syntactically valid size whose byte value
// overflows int64 is rejected rather than wrapping negative.
func TestParseSizeOverflow(t *testing.T) {
	for _, value := range []string{
		"9223372036854775807GiB",
		"18014398509481984GiB", // 2^54 GiB == 2^84 bytes
		"9223372036854775807KiB",
	} {
		if _, err := ParseSize(value); err == nil {
			t.Errorf("ParseSize(%q) must be rejected as overflowing", value)
		} else if !strings.Contains(err.Error(), "overflow") {
			t.Errorf("ParseSize(%q) error = %q, want an overflow explanation", value, err)
		}
	}
}

// TestParseMemory pins the container-memory notation: only an integer with an
// explicit KiB/MiB/GiB suffix is accepted; the byte unit (B) and a bare number
// are rejected, keeping the template's accepted notation unchanged.
func TestParseMemory(t *testing.T) {
	ok := map[string]int64{
		"64KiB":  64 << 10,
		"256MiB": 256 << 20,
		"2GiB":   2 << 30,
		" 1GiB ": 1 << 30,
	}
	for value, want := range ok {
		got, err := ParseMemory(value)
		if err != nil {
			t.Errorf("ParseMemory(%q) error: %v", value, err)
			continue
		}
		if got != want {
			t.Errorf("ParseMemory(%q) = %d, want %d", value, got, want)
		}
	}

	bad := []string{
		"512KB", "512MB", "1GB", "512", "256B", "1B", "-1MiB", "0MiB",
		"MiB", "1.5MiB", "1 MiB", "1mib", "1GIB", "",
	}
	for _, value := range bad {
		if _, err := ParseMemory(value); err == nil {
			t.Errorf("ParseMemory(%q) must be rejected", value)
		}
	}
}

// TestParseMemoryOverflow pins the memory overflow guard.
func TestParseMemoryOverflow(t *testing.T) {
	for _, value := range []string{
		"9223372036854775807GiB",
		"9223372036854775807MiB",
		"9223372036854775807KiB",
		"18014398509481984GiB",
	} {
		if _, err := ParseMemory(value); err == nil {
			t.Errorf("ParseMemory(%q) must be rejected as overflowing", value)
		} else if !strings.Contains(err.Error(), "overflow") {
			t.Errorf("ParseMemory(%q) error = %q, want an overflow explanation", value, err)
		}
	}
}

// TestFormat pins that Format renders the largest exact binary unit (so the
// value round-trips through the parser) and falls back to plain bytes for a
// value that is not a whole binary unit.
func TestFormat(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{256, "256B"},
		{1 << 10, "1KiB"},
		{256 << 10, "256KiB"},
		{1 << 20, "1MiB"},
		{2 << 30, "2GiB"},
		{300000, "300000B"},
		{0, "0B"},
		{-1, "-1B"},
	}
	for _, tc := range tests {
		if got := Format(tc.in); got != tc.want {
			t.Errorf("Format(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestFormatRoundTrips pins the property that matters to operators: a formatted
// whole-unit value parses back to the same byte count through both notations
// that accept it.
func TestFormatRoundTrips(t *testing.T) {
	for _, n := range []int64{1, 1 << 10, 256 << 10, 1 << 20, 2 << 30, math.MaxInt64 &^ ((1 << 30) - 1)} {
		if n <= 0 {
			continue
		}
		s := Format(n)
		if got, err := ParseSize(s); err != nil || got != n {
			t.Errorf("ParseSize(Format(%d)=%q) = %d, %v; want round trip", n, s, got, err)
		}
	}
}
