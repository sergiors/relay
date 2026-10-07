// Package bytesize provides strict, positive binary-size parsing and
// formatting shared by Relay's configuration and template resource limits.
//
// The parser accepts only binary units — B, KiB, MiB, and GiB (1, 1<<10,
// 1<<20, 1<<30) — as an integer immediately followed by the case-sensitive
// suffix, with no fraction, sign, or whitespace, and rejects any value that is
// not strictly positive or that overflows an int64. Decimal suffixes (KB, MB,
// GB) are deliberately rejected so "1MB" and "1MiB" can never be ambiguous.
//
// Two entry points expose the two accepted notations: ParseSize accepts the
// full set plus a bare byte count (used by MAX_EVENT_BYTES, where a legacy
// integer value stays valid), while ParseMemory accepts only the explicit
// binary multiples KiB/MiB/GiB (the container-memory notation, unchanged from
// the template parser it replaces). Format renders a byte count back into the
// largest exact binary unit so operator-facing output round-trips through the
// parser.
package bytesize
