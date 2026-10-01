package app

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Default per-container resource limits applied to an app whose template
// omits the `resources` key (or omits individual fields). They are the values the
// runtime historically hard-coded: 128 MiB memory, 1 CPU, 128 PIDs. Keeping them
// here lets the parser, the runtime's Docker mapping, and the docs share one
// definition.
const (
	// DefaultResourceMemoryBytes is the default container memory limit (128 MiB).
	DefaultResourceMemoryBytes int64 = 128 << 20
	// DefaultResourceNanoCPUs is the default container CPU limit in
	// billionths of a CPU (1 CPU).
	DefaultResourceNanoCPUs int64 = 1_000_000_000
	// DefaultResourcePidsLimit is the default container PID limit.
	DefaultResourcePidsLimit int64 = 128
)

// resourceFingerprintLen is the number of hex characters in a resource
// fingerprint. 16 hex chars = 64 bits, matching the service identity/env hash
// convention (serviceIdentityHashLen) so resource labels share one shape.
const resourceFingerprintLen = 16

// memoryPattern matches a binary-size memory limit: a positive integer followed
// by exactly one of the binary suffixes KiB, MiB, or GiB. Decimal suffixes
// (KB/MB/GB) and un-suffixed numbers are deliberately rejected: Docker's
// memory limit is binary, and accepting both families would make "1MB" and
// "1MiB" ambiguous to a template author.
var memoryPattern = regexp.MustCompile(`^([0-9]+)(KiB|MiB|GiB)$`)

// memoryUnitBytes maps the accepted binary suffix to its byte multiplier.
var memoryUnitBytes = map[string]int64{
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
}

// ResourceLimits is an app's EFFECTIVE per-container resource configuration:
// every field is resolved (never zero) after ParseTemplate. It is a plain value
// type so it can be copied, compared, hashed, and logged without allocation.
//
// The limits are per CONTAINER, not per app: an app running N
// concurrent invocations keeps N warm containers, each bounded by these values,
// so the app's aggregate ceiling is these values multiplied by the number
// of concurrently running containers (bounded by the app's effective
// concurrency).
type ResourceLimits struct {
	// MemoryBytes is the container memory limit in bytes.
	MemoryBytes int64
	// NanoCPUs is the CPU limit in billionths of a CPU (Docker's NanoCPUs).
	NanoCPUs int64
	// PidsLimit is the maximum number of processes/threads in the container.
	PidsLimit int64
}

// DefaultResourceLimits returns the effective defaults applied when a template
// omits resource configuration (or individual fields).
func DefaultResourceLimits() ResourceLimits {
	return ResourceLimits{
		MemoryBytes: DefaultResourceMemoryBytes,
		NanoCPUs:    DefaultResourceNanoCPUs,
		PidsLimit:   DefaultResourcePidsLimit,
	}
}

// OrDefault returns the limits with any non-positive field replaced by its
// default. It makes a hand-built zero ResourceLimits (tests, direct callers)
// behave exactly like an omitted template, so the runtime never maps a
// zero/negative limit into a Docker HostConfig.
func (r ResourceLimits) OrDefault() ResourceLimits {
	d := DefaultResourceLimits()
	if r.MemoryBytes <= 0 {
		r.MemoryBytes = d.MemoryBytes
	}
	if r.NanoCPUs <= 0 {
		r.NanoCPUs = d.NanoCPUs
	}
	if r.PidsLimit <= 0 {
		r.PidsLimit = d.PidsLimit
	}
	return r
}

// CPUs returns the CPU limit as a floating-point core count (NanoCPUs / 1e9).
// It is for display only; the runtime always maps the exact NanoCPUs value.
func (r ResourceLimits) CPUs() float64 {
	return float64(r.OrDefault().NanoCPUs) / 1e9
}

// Fingerprint returns a deterministic, value-free short digest of the effective
// limits. It is the identity the runtime appends to its container generation so
// a resource-only change yields a new generation (draining old warm containers)
// without touching the image reference, and the label the service reconciler
// stamps (relay.resources) to detect a changed per-container resource
// configuration. A zero ResourceLimits fingerprints as the defaults.
func (r ResourceLimits) Fingerprint() string {
	r = r.OrDefault()
	h := sha256.New()
	io.WriteString(h, "relay.resources")
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d\x00%d\x00%d", r.MemoryBytes, r.NanoCPUs, r.PidsLimit)
	return hex.EncodeToString(h.Sum(nil))[:resourceFingerprintLen]
}

// ResourceLimits returns the template's EFFECTIVE per-container resource limits.
// A nil template (or a hand-built template that omitted Resources) yields the
// defaults, so callers never have to normalize themselves.
func (t *Template) ResourceLimits() ResourceLimits {
	if t == nil {
		return DefaultResourceLimits()
	}
	return t.Resources.OrDefault()
}

// rawResourceLimits is the decoded `resources` mapping. Each field is decoded as
// `any` (not a concrete type) so a malformed value — a number for memory, a
// string for cpus, a float for pids — is distinguishable from an omitted one and
// rejected with a clear message instead of being silently coerced by yaml.v3.
type rawResourceLimits struct {
	Memory any `yaml:"memory"`
	CPUs   any `yaml:"cpus"`
	Pids   any `yaml:"pids"`
}

// resolveResourceLimits resolves the optional `resources` mapping. A nil mapping
// (the key omitted, empty, or null) yields the defaults; a present mapping
// resolves each field independently, so a partial override keeps the defaults
// for the fields it omits. Resolution is strict: malformed, zero, negative, or
// out-of-range values are rejected rather than normalized.
func resolveResourceLimits(raw *rawResourceLimits) (ResourceLimits, error) {
	if raw == nil {
		return DefaultResourceLimits(), nil
	}
	memory, err := resolveResourceMemory(raw.Memory)
	if err != nil {
		return ResourceLimits{}, err
	}
	cpus, err := resolveResourceCPUs(raw.CPUs)
	if err != nil {
		return ResourceLimits{}, err
	}
	pids, err := resolveResourcePids(raw.Pids)
	if err != nil {
		return ResourceLimits{}, err
	}
	return ResourceLimits{MemoryBytes: memory, NanoCPUs: cpus, PidsLimit: pids}, nil
}

// resolveResourceMemory parses the optional `resources.memory`. A nil value
// yields the default; otherwise it must be a string of the form
// "<positive integer>(KiB|MiB|GiB)" (e.g. "256MiB"). Decimal suffixes
// (KB/MB/GB), a bare number, a non-string type, zero, or an overflow are
// rejected.
func resolveResourceMemory(raw any) (int64, error) {
	if raw == nil {
		return DefaultResourceMemoryBytes, nil
	}
	s, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("resources.memory must be a size string such as %q, got %v", "256MiB", raw)
	}
	m := memoryPattern.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("resources.memory %q must be an integer followed by KiB, MiB, or GiB (e.g. %q)", s, "256MiB")
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("resources.memory %q must be a positive size", s)
	}
	unit := memoryUnitBytes[m[2]]
	if n > math.MaxInt64/unit {
		return 0, fmt.Errorf("resources.memory %q overflows the maximum supported size", s)
	}
	return n * unit, nil
}

// resolveResourceCPUs parses the optional `resources.cpus`. A nil value yields
// the default; otherwise it must be a finite number greater than zero (an int or
// a float, including a fractional core count such as 0.5). A string, a bool,
// zero, a negative, NaN, and Inf are rejected. The value is converted to Docker
// NanoCPUs; a value that rounds to zero is rejected as too small.
func resolveResourceCPUs(raw any) (int64, error) {
	if raw == nil {
		return DefaultResourceNanoCPUs, nil
	}
	f, ok := numericToFloat(raw)
	if !ok {
		return 0, fmt.Errorf("resources.cpus must be a number greater than zero, got %v", raw)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 {
		return 0, fmt.Errorf("resources.cpus must be a finite number greater than zero, got %v", raw)
	}
	nano := f * 1e9
	if nano > math.MaxInt64 {
		return 0, fmt.Errorf("resources.cpus %v is too large", raw)
	}
	v := int64(math.Round(nano))
	if v <= 0 {
		return 0, fmt.Errorf("resources.cpus %v is too small", raw)
	}
	return v, nil
}

// resolveResourcePids parses the optional `resources.pids`. A nil value yields
// the default; otherwise it must be a positive integer. A float, a string, a
// bool, zero, or a negative is rejected. yaml.v3 decodes an integer above
// MaxInt64 into `any` as a uint64 (not an int64), so that case is deliberately
// rejected by the type switch below rather than cast: casting would wrap a
// value like 9223372036854775808 into a negative limit.
func resolveResourcePids(raw any) (int64, error) {
	if raw == nil {
		return DefaultResourcePidsLimit, nil
	}
	var n int64
	switch v := raw.(type) {
	case int:
		n = int64(v)
	case int64:
		n = v
	default:
		// Includes uint64 (an integer above MaxInt64) and float64 (a
		// fractional or beyond-MaxUint64 literal): neither is a valid positive
		// int64, and casting either would silently corrupt the limit.
		return 0, fmt.Errorf("resources.pids must be a positive integer, got %v", raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("resources.pids must be a positive integer, got %v", raw)
	}
	return n, nil
}

// numericToFloat converts a decoded YAML numeric value to float64. yaml.v3
// decodes an integer literal as int and a fractional/exponent literal as
// float64. Any non-numeric type (string, bool, map, list, null) reports false.
func numericToFloat(raw any) (float64, bool) {
	switch v := raw.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	default:
		return 0, false
	}
}

// stripTemplateResources removes the top-level `resources` key from a
// template.yaml document and re-serializes it. It is how the content fingerprint
// intentionally EXCLUDES resource configuration: a resource-only hot change must
// not look like a source change, so it must not change the app's image
// fingerprint (and therefore must not trigger a rebuild). The rest of the
// document is preserved verbatim by operating on the YAML node tree rather than
// re-marshalling a decoded Go value, so comments, key order, and inline-vs-block
// style survive and a resource-only edit yields byte-identical output.
//
// It is best-effort: a document that is not a YAML mapping, or that fails to
// parse/re-serialize, is returned unchanged (hashing proceeds over the raw
// bytes). Only the ROOT `resources` mapping is stripped — a nested file named
// template.yaml is not touched, because callers apply this only to the root
// template.
func stripTemplateResources(raw []byte) []byte {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil || len(doc.Content) == 0 {
		return raw
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return raw
	}
	kept := make([]*yaml.Node, 0, len(root.Content))
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "resources" {
			continue
		}
		kept = append(kept, root.Content[i], root.Content[i+1])
	}
	root.Content = kept
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return raw
	}
	return out
}
