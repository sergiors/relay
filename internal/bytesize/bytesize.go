package bytesize

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// sizeUnits maps the accepted binary suffixes to their byte multiplier. It is
// the single source of truth for both accepted notation and scaling: B is the
// byte unit and KiB/MiB/GiB are the binary multiples (1<<10, 1<<20, 1<<30).
var sizeUnits = map[string]int64{
	"B":   1,
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
}

// memoryUnits is sizeUnits restricted to the container-memory notation (no B):
// a memory limit always names an explicit binary multiple, so a bare byte count
// or a "B" suffix is rejected rather than silently accepted.
var memoryUnits = map[string]int64{
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
}

// formatUnits lists the binary suffixes largest-first so Format picks the
// largest unit that divides a value exactly.
var formatUnits = []string{"GiB", "MiB", "KiB"}

// sizePattern matches a base-10 integer with an optional case-sensitive unit
// suffix and nothing else. The suffix set is validated against the caller's
// accepted units, so the pattern stays permissive while the accepted notation
// remains data.
var sizePattern = regexp.MustCompile(`^([0-9]+)([A-Za-z]*)$`)

// ParseSize parses a general binary size: a positive base-10 integer
// immediately followed by B, KiB, MiB, or GiB (case-sensitive), or a bare
// positive integer interpreted as a plain byte count. The bare form preserves
// legacy integer byte values (e.g. MAX_EVENT_BYTES=262144), which the human-
// readable B/KiB/MiB/GiB suffixes extend rather than replace.
//
// The value is trimmed of surrounding whitespace. Empty input, an unaccepted
// suffix (a decimal unit such as KB/MB/GB or a misspelling), a fractional or
// signed value, zero, and any byte value that overflows an int64 are errors.
func ParseSize(value string) (int64, error) {
	return parse(value, sizeUnits, true, "a positive integer optionally followed by B, KiB, MiB, or GiB")
}

// ParseMemory parses a container memory limit: a positive base-10 integer
// immediately followed by KiB, MiB, or GiB (case-sensitive). The byte unit (B)
// and a bare number are deliberately rejected: a memory limit must name an
// explicit binary multiple.
//
// The value is trimmed of surrounding whitespace. Decimal suffixes (KB/MB/GB),
// fractional or signed values, zero, and any byte value that overflows an int64
// are errors.
func ParseMemory(value string) (int64, error) {
	return parse(value, memoryUnits, false, "a positive integer followed by KiB, MiB, or GiB")
}

// parse is the shared strict parser. units is the accepted suffix set, form is
// the human-readable required form rendered in every error, and allowBare
// permits a suffix-less integer interpreted as bytes.
func parse(value string, units map[string]int64, allowBare bool, form string) (int64, error) {
	m := sizePattern.FindStringSubmatch(strings.TrimSpace(value))
	if m == nil {
		return 0, fmt.Errorf("must be %s", form)
	}
	scale, ok := units[m[2]]
	if !ok {
		if m[2] != "" || !allowBare {
			return 0, fmt.Errorf("must be %s", form)
		}
		scale = 1
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("must be %s", form)
	}
	if n > math.MaxInt64/scale {
		return 0, fmt.Errorf("size overflows the maximum supported value")
	}
	return n * scale, nil
}

// Format renders n bytes as a binary size using the largest unit that divides it
// exactly (GiB, MiB, KiB), so a whole-unit value round-trips through ParseSize
// or ParseMemory; a value not divisible by any unit is rendered as plain bytes
// (e.g. "300000B"). Zero and negative values render as plain bytes.
func Format(n int64) string {
	for _, suffix := range formatUnits {
		scale := sizeUnits[suffix]
		if n > 0 && n%scale == 0 {
			return fmt.Sprintf("%d%s", n/scale, suffix)
		}
	}
	return fmt.Sprintf("%dB", n)
}
