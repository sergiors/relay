package function

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"gopkg.in/yaml.v3"
)

// DefaultTimeout is applied to a rule that omits an explicit timeout.
const DefaultTimeout = 6 * time.Second

// DefaultRetries is the number of additional executions a rule attempts after
// its initial attempt (0 = only the initial attempt). A rule that omits
// `retries` resolves to this value, so by default a failing invocation is
// attempted 1 + DefaultRetries = 5 times in total before it is considered
// exhausted and the message is routed to the DLQ.
const DefaultRetries = 4

// DefaultConcurrency is the per-function concurrency applied to a template that
// omits the top-level `concurrency` key. It bounds how many invocations of THIS
// function's handlers may execute concurrently within a single Relay worker
// (the worker-global cap in MAX_CONCURRENCY is a separate, broader limit). A
// template that specifies `concurrency` must be a positive integer; zero,
// negative, or non-integer values fail validation.
const DefaultConcurrency = 2

// DefaultServicePort is the port applied to a service that omits an explicit
// `port`. It is the standard HTTP port.
const DefaultServicePort = 80

// DefaultServiceReplicas is the replica count applied to a service that omits a
// `replicas` key: Relay maintains a single long-running instance by default.
const DefaultServiceReplicas = 1

// MaxTimeout is the upper bound on any rule's handler timeout. It is the same
// value as stream.MaxRuleTimeout (kept in sync; function is a leaf package and
// stream may import it, not the reverse). The stream layer derives its
// MinPendingIdle reclaim threshold from this cap so a handler that legitimately
// runs up to the cap is never reclaimed mid-flight.
const MaxTimeout = 5 * time.Minute

// operatorKeys are the only keys treated as operators when they are the only
// keys present in a map; any other key is treated as a nested field.
var operatorKeys = map[string]bool{
	"equals": true,
	"prefix": true,
	"suffix": true,
	"exists": true,
	"gt":     true,
	"gte":    true,
	"lt":     true,
	"lte":    true,
}

var supportedRuntimes = map[string]bool{
	"python3.14": true,
	"node24":     true,
}

// SecretRef is a reference to a secret by name. It is a distinct type from a
// plain string so a secret reference can never be confused with a literal env
// value: the two maps (Env and Secrets) are structurally different, and the
// compiler enforces it. String() returns the reference name.
type SecretRef string

// String returns the secret reference name.
func (s SecretRef) String() string { return string(s) }

// EnvVar is one literal environment variable: the env-var name and its literal
// string value. It is returned (name-ordered) by Template.EnvList.
type EnvVar struct {
	Name  string
	Value string
}

// SecretBinding is one secret binding: the env-var name it is exposed as and
// the secret reference it resolves to. It is returned (name-ordered) by
// Template.SecretList.
type SecretBinding struct {
	Name string
	Ref  SecretRef
}

// Template is a parsed template.yaml file: the runtime plus a list of event
// rules, each pairing a handler with a pattern invoked when the pattern
// matches, and an optional list of cron schedules.
type Template struct {
	Runtime string
	Events  []EventRule
	// Concurrency bounds how many of this function's handler invocations may
	// execute concurrently within a single Relay worker (per-function, per
	// worker). It is always >= 1 after ParseTemplate; a Template constructed
	// without parsing (tests) may be 0, and the runner treats <=0 as
	// DefaultConcurrency.
	Concurrency int
	// Env maps an env-var name to a literal string value, injected into every
	// execution container at runtime. It is never baked into the image.
	Env map[string]string
	// Secrets maps an env-var name to a secret reference name, resolved to a
	// value immediately before each execution and injected into the container
	// at runtime. The reference name (never the resolved value) is part of the
	// function's configuration; the value lives outside template.yaml.
	Secrets map[string]SecretRef
	// Schedules lists the function's cron-triggered handlers. It is nil when
	// the template defines no `schedules` key. Each Schedule carries its
	// resolved timezone and timeout (never zero after ParseTemplate).
	Schedules []Schedule
	// Services lists the function's persistent long-running services. It is nil
	// when the template defines no `services` key. Port and Replicas carry their
	// resolved defaults (never zero) after ParseTemplate, and Path is
	// canonicalized (see Service.Path).
	Services []Service
	// Networks lists the Docker networks every execution container (event and
	// schedule) and every persistent service container of this function joins.
	// It is generic template config: a plain list of Docker network names, never
	// a Traefik-specific term. It is nil when the template omits `networks`.
	// After ParseTemplate the list is normalized: each entry trimmed, empty
	// entries rejected, duplicates removed, and the result sorted, so it is
	// deterministic regardless of authoring order. The networks themselves are
	// infrastructure owned OUTSIDE Relay — Relay only verifies they exist and
	// never creates them.
	Networks []string
}

// Schedule is one cron schedule from the template's `schedules` list: the
// handler it invokes, the standard 5-field cron expression (verbatim), the
// effective IANA timezone (always non-nil after ParseTemplate; omitted
// timezones resolve to UTC), and the resolved per-invocation timeout and retry
// count (same rules as event rules).
type Schedule struct {
	Handler  string
	Cron     string
	Location *time.Location
	Timeout  time.Duration
	// Retries is the number of additional executions attempted after the
	// initial one (0 = only the initial attempt). Schedule entries carry the
	// same semantics as EventRule.Retries: it is always non-negative after
	// ParseTemplate; omitted schedules default to DefaultRetries. The total
	// number of attempts for a failing invocation is 1 + Retries.
	Retries int
}

// ServiceSource names which of a service's three mutually exclusive sources is
// configured. A service declares EXACTLY ONE source, so Source() is total for a
// parsed service.
type ServiceSource string

const (
	// ServiceSourceEntrypoint is a runtime-managed application entrypoint file:
	// the function image is built by the runtime engine and its invocation
	// bootstrap entrypoint is overridden per container to run the file.
	ServiceSourceEntrypoint ServiceSource = "entrypoint"
	// ServiceSourceBuild is a user-supplied Dockerfile (path relative to the
	// function directory). Relay builds an image from the function's selected
	// source (the same .gitignore-driven selection that fingerprints a
	// function) using the Docker Engine API, and runs it preserving the
	// Dockerfile's own ENTRYPOINT/CMD.
	ServiceSourceBuild ServiceSource = "build"
	// ServiceSourceImage is an external image reference. Relay inspects the
	// local image and pulls it from its registry when needed; the image's own
	// ENTRYPOINT/CMD are preserved. External images are never removed by
	// Relay's image cleanup (Relay only ever cleans its relay-fn-* namespace).
	ServiceSourceImage ServiceSource = "image"
)

// Service is one persistent HTTP service from the template's `services`
// list. It declares EXACTLY ONE source (entrypoint, build, or image), the
// internal TCP port the application listens on, and the desired replica count
// Relay maintains. Port and Replicas are always effective (non-zero) after
// ParseTemplate.
//
// The configured source descriptor (SourceRef) is the service's stable
// identity: the entrypoint file, the Dockerfile path, or the external image
// reference. It is deliberately NOT a synthetic entrypoint string, so every
// source kind has an honest identity that survives reconciliation across
// restarts. It is the grouping key for containers, the routing id input, and
// the persisted service key. Source descriptors are unique within a function
// (rejected at parse time), because they must key containers and routing
// deterministically.
type Service struct {
	// Entrypoint is the runtime-managed entrypoint source: an application
	// entrypoint file (e.g. "service.js" or "app/main.py"), a relative path
	// inside the application directory, NOT the module.function handler form.
	Entrypoint string
	// Build is the Dockerfile source: a path to a Dockerfile relative to the
	// function directory (e.g. "Dockerfile" or "docker/Dockerfile.prod"). The
	// build context is the function's selected source.
	Build string
	// Image is the external image source reference (e.g. "ghcr.io/acme/api:1.2"),
	// inspected locally and pulled when needed. Relay never cleans it up.
	Image string
	// Host is the optional hostname (e.g. "api.example.com") this service is
	// exposed through the routing layer (Traefik, at the wiring level); empty
	// means an internal unrouted service with no routing labels. It is generic
	// template config (a plain hostname), never a Traefik-specific term. It
	// must be a valid hostname and is validated at parse time.
	Host string
	// Path is the optional URL path prefix this service is exposed under on its
	// Host (e.g. "/v2"). It is generic template config (a URL path), never a
	// Traefik-specific term. Empty (omitted or explicitly "") means the service
	// is routed by host alone, exactly as before paths existed. When set, the
	// routing layer adds a PathPrefix rule and a StripPrefix middleware. A
	// configured path requires a host: path without host is rejected at parse
	// time. After ParseTemplate the value is canonicalized: it starts with "/",
	// has no trailing slash except for the root "/", and contains no empty
	// ("//") segments — a non-root value therefore never has a trailing slash.
	Path     string
	Port     int
	Replicas int
}

// NeedsRuntime reports whether Relay must know how to launch this template:
// any event rule or cron schedule runs through a runtime, and any
// `entrypoint`-source service is launched by a runtime-specific command. An
// explicitly configured runtime is also honored (it is what the operator asked
// Relay to build and run through), so it always counts as needed. Only a
// template that declares NO runtime and whose only services use `build` or
// `image` sources needs none — those images carry their own ENTRYPOINT/CMD.
func (t *Template) NeedsRuntime() bool {
	if t.Runtime != "" {
		return true
	}
	if len(t.Events) > 0 || len(t.Schedules) > 0 {
		return true
	}
	for _, s := range t.Services {
		if s.Source() == ServiceSourceEntrypoint {
			return true
		}
	}
	return false
}

// RuntimeGeneration returns a deterministic, short digest of the template's
// RUNTIME-ONLY configuration: the configuration that shapes how a container is
// created but not what image it runs. Today that is exactly the top-level
// `networks` list (already normalized). It is the runtime counterpart to the
// content fingerprint: editing `networks` changes this digest but NOT the image
// fingerprint (see ImageFingerprint), so the reconciler replaces warm/service
// containers for the new networks without rebuilding the image.
//
// It is deterministic and order-independent because the network list is
// normalized (sorted, deduped) at parse time; a template assembled by hand
// (tests, direct callers) is hashed in its given order. An empty generation
// (no networks) returns the empty string, which is the "no runtime config"
// value callers compare against.
func (t *Template) RuntimeGeneration() string {
	if t == nil || len(t.Networks) == 0 {
		return ""
	}
	h := sha256.New()
	for _, n := range t.Networks {
		h.Write([]byte(n))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Source reports which of the three mutually exclusive sources is configured.
// Exactly one is set after ParseTemplate.
func (s Service) Source() ServiceSource {
	switch {
	case s.Build != "":
		return ServiceSourceBuild
	case s.Image != "":
		return ServiceSourceImage
	default:
		return ServiceSourceEntrypoint
	}
}

// SourceRef returns the configured source's descriptor: the entrypoint file,
// the Dockerfile path, or the external image reference. It is the value that
// identifies the source bytes, and it is also the service's stable identity —
// the container grouping key, routing id input, and persisted key (see
// Service's type comment). Callers derive identity from this, never from a
// separate field.
func (s Service) SourceRef() string {
	switch s.Source() {
	case ServiceSourceBuild:
		return s.Build
	case ServiceSourceImage:
		return s.Image
	default:
		return s.Entrypoint
	}
}

// EventRule pairs a handler (module.function) with a matching pattern and a
// resolved invocation timeout. Timeout is always non-zero after ParseTemplate;
// omitted event rules default to DefaultTimeout.
type EventRule struct {
	Handler string
	Pattern Pattern
	// Timeout bounds a single invocation of this event rule's handler. It is
	// always positive and never exceeds MaxTimeout after ParseTemplate.
	Timeout time.Duration
	// Retries is the number of additional executions attempted after the
	// initial one (0 = only the initial attempt). It is always non-negative
	// after ParseTemplate; omitted event rules default to DefaultRetries. The
	// total number of attempts for a failing invocation is 1 + Retries.
	Retries int
}

// Pattern maps top-level event fields to their conditions.
type Pattern map[string]FieldCondition

// FieldCondition is how one field is evaluated: value operators are
// alternatives (OR), and child conditions AND with each other and the operators.
type FieldCondition struct {
	Operators []ValueMatcher
	Children  map[string]FieldCondition
}

type ValueMatcher interface {
	Match(value any) bool
}

// equalityMatcher matches values equal to any of the given values. Comparison
// is type-preserving except that numeric kinds are compared numerically.
type equalityMatcher struct {
	values []any
}

func (m equalityMatcher) Match(value any) bool {
	for _, want := range m.values {
		if valuesEqual(value, want) {
			return true
		}
	}
	return false
}

// prefixMatcher matches string values that start with any of the given
// prefixes. Non-string values never match.
type prefixMatcher struct {
	prefixes []string
}

func (m prefixMatcher) Match(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	for _, p := range m.prefixes {
		if len(s) >= len(p) && s[:len(p)] == p {
			return true
		}
	}
	return false
}

// suffixMatcher matches string values that end with any of the given suffixes.
// Non-string values never match.
type suffixMatcher struct {
	suffixes []string
}

func (m suffixMatcher) Match(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	for _, suf := range m.suffixes {
		if len(s) >= len(suf) && s[len(s)-len(suf):] == suf {
			return true
		}
	}
	return false
}

// existsMatcher matches key presence only. It ignores the value entirely:
// null, false, 0, "", {}, [] all count as existing. The boolean records the
// polarity: true = key present, false = key absent.
//
// The matcher never inspects the value because the dispatcher loop routes it
// through presenceMatcher (MatchPresent), which resolves the polarity from the
// presence flag alone — the value is irrelevant to a presence test. Match here
// is only reachable when the key is present (an absent key never reaches a
// value operator), so returning want outright yields the correct result: a
// present key with want=true matches and want=false does not.
type existsMatcher struct{ want bool }

// Match fulfills ValueMatcher. See the type comment: this is value-independent
// and is only invoked on a present key, so it returns the polarity directly.
func (m existsMatcher) Match(value any) bool { return m.want }

// MatchPresent fulfills presenceMatcher. An absent key with want=false (exists:
// false) matches; an absent key with want=true (exists: true) does not, and a
// present key matches exactly when want is true.
func (m existsMatcher) MatchPresent(present bool) bool { return present == m.want }

// comparisonKind selects how a comparisonMatcher treats its operand: either as a
// fixed numeric threshold, or as a `now()`-relative cutoff re-evaluated from the
// clock at match time.
type comparisonKind int

const (
	numericComparison comparisonKind = iota
	nowComparison
)

// comparisonMatcher implements one of the ordering operators gt/gte/lt/lte. Its
// operand list is OR: the value matches if ANY operand compares true. When the
// operand is temporal (a `now()`-relative string), the cutoff is recomputed from
// the injected clock at match time — never at template load — so the rule stays
// dynamic.
//
// Numeric operands compare numerically (via toFloat): a numeric event value
// greater/equal/less than the threshold. A non-numeric event value never
// matches. Temporal operands require the event value to be an RFC3339 string
// and compare instants — not strings.
type comparisonMatcher struct {
	kind     comparisonKind
	op       comparisonOp
	numBound float64          // fixed numeric threshold (kind == numericComparison)
	durNow   time.Duration    // relative clock offset, signed (kind == nowComparison)
	now      func() time.Time // clock consulted at match time (kind == nowComparison)
}

// comparisonOp names the ordering operator; both numeric and temporal matching
// branch on it so the "does equality pass" rule stays in one place.
type comparisonOp int

const (
	opGt comparisonOp = iota
	opGte
	opLt
	opLte
)

func (m comparisonMatcher) pred(a, b float64) bool {
	switch m.op {
	case opGt:
		return a > b
	case opGte:
		return a >= b
	case opLt:
		return a < b
	case opLte:
		return a <= b
	default:
		return false
	}
}

// predTime applies the same ordering predicate to two instants, comparing them
// as instants (After/Before/Equal), never lexicographically.
func (m comparisonMatcher) predTime(a, b time.Time) bool {
	switch m.op {
	case opGt:
		return a.After(b)
	case opGte:
		return a.After(b) || a.Equal(b)
	case opLt:
		return a.Before(b)
	case opLte:
		return a.Before(b) || a.Equal(b)
	default:
		return false
	}
}

func (m comparisonMatcher) Match(value any) bool {
	if m.kind == nowComparison {
		// The cutoff is computed from the injected clock at this instant, then
		// compared. The clock is guaranteed non-nil for temporal matchers: it is
		// always supplied by the parse entry point, which falls back to
		// time.Now().UTC() when one is not provided. The fallback below is a
		// defensive guard only and is unreachable through the parse path.
		now := m.now
		if now == nil {
			now = time.Now().UTC
		}
		want := now().Add(m.durNow)
		s, ok := value.(string)
		if !ok {
			// Missing, null, or non-string values never match (and never panic).
			return false
		}
		inst, err := time.Parse(time.RFC3339, s)
		if err != nil {
			// A non-RFC3339 string is not a valid instant.
			return false
		}
		return m.predTime(inst, want)
	}
	af, ok := toFloat(value)
	if !ok {
		// Non-numeric event value never matches a numeric threshold.
		return false
	}
	return m.pred(af, m.numBound)
}

// ParseTemplate builds a Template from raw YAML, validating the runtime and
// every rule's handler. It is the production entry point and uses the real wall
// clock for temporal (`now()`-relative) comparisons, computed per match at
// run time. Tests that need a pinned clock call parseTemplateWithClock instead.
func ParseTemplate(data []byte) (*Template, error) {
	return parseTemplateWithClock(data, time.Now().UTC)
}

// parseTemplateWithClock is ParseTemplate with an explicit clock for `now()`
// -relative comparison operators. It is the single construction point for the
// temporal clock seam: every comparisonMatcher derives its clock from the one
// supplied here (or defaults to time.Now().UTC when nil), so temporal rules
// re-evaluate dynamically per match and never capture the parse-time instant.
func parseTemplateWithClock(data []byte, now func() time.Time) (*Template, error) {
	var raw struct {
		Runtime string            `yaml:"runtime"`
		Env     map[string]string `yaml:"env"`
		Secrets map[string]string `yaml:"secrets"`
		// Networks is decoded as `[]any` so a non-string entry (a number, bool,
		// map, or list) is distinguishable from a valid network name and
		// rejected with a clear message instead of being silently coerced by
		// yaml.v3.
		Networks []any `yaml:"networks"`
		// Concurrency is decoded as `any` (not `*int`) so a non-integer value
		// (e.g. "abc", "1.5", true) is distinguishable from an omitted one and
		// rejected with a clear message instead of being silently truncated or
		// coerced by yaml.v3.
		Concurrency any `yaml:"concurrency"`
		Events      []struct {
			Handler string         `yaml:"handler"`
			Pattern map[string]any `yaml:"pattern"`
			Timeout string         `yaml:"timeout"`
			// Retries is decoded as `any` (not `*int`) so a non-integer value
			// (e.g. "abc", "1.5", true) is distinguishable from an omitted one
			// and rejected with a clear message instead of being silently
			// truncated or coerced by yaml.v3.
			Retries any `yaml:"retries"`
		} `yaml:"events"`
		Schedules []struct {
			Handler  string `yaml:"handler"`
			Cron     string `yaml:"cron"`
			Timezone string `yaml:"timezone"`
			Timeout  string `yaml:"timeout"`
			// Retries is decoded as `any` (not `*int`) so a non-integer value
			// (e.g. "abc", "1.5", true) is distinguishable from an omitted one
			// and rejected with a clear message instead of being silently
			// truncated or coerced by yaml.v3.
			Retries any `yaml:"retries"`
		} `yaml:"schedules"`
		Services []struct {
			Entrypoint string `yaml:"entrypoint"`
			// Build is the optional Dockerfile source, a path relative to the
			// function directory. Exactly one of Entrypoint/Build/Image must be
			// set.
			Build string `yaml:"build"`
			// Image is the optional external image source reference. Exactly one
			// of Entrypoint/Build/Image must be set.
			Image string `yaml:"image"`
			Host  string `yaml:"host"`
			// Path is optional; empty (omitted or "") means host-only routing.
			// It is decoded as a plain string so an omitted and an explicit ""
			// are indistinguishable, matching the "empty = omitted" rule.
			Path string `yaml:"path"`
			// Port and Replicas are decoded as `any` so a non-integer value
			// (e.g. "abc", "1.5", true) is distinguishable from an omitted one
			// and rejected with a clear message (see resolveServicePort /
			// resolveServiceReplicas).
			Port     any `yaml:"port"`
			Replicas any `yaml:"replicas"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse template yaml: %w", err)
	}

	t := &Template{Runtime: raw.Runtime}

	// Parse and validate concurrency. It is optional; when non-nil it must be a
	// positive integer (see resolveConcurrency).
	concurrency, err := resolveConcurrency(raw.Concurrency)
	if err != nil {
		return nil, err
	}
	t.Concurrency = concurrency

	// Parse and validate env/secrets. Both are optional; when present, every
	// key must be a valid env-var name, every secret reference a valid secret
	// name, and no variable may be defined in both maps (a duplicate would be
	// ambiguous — which value wins?).
	if err := parseEnvSecrets(t, raw.Env, raw.Secrets); err != nil {
		return nil, err
	}

	// Parse the optional top-level `networks` list. Every execution container
	// and every persistent service container of this function joins these
	// networks; each must be a non-empty Docker network name. Normalization is
	// deterministic: entries are trimmed, empty entries rejected, duplicates
	// removed, and the result sorted, so authoring order and cosmetic
	// whitespace never churn container configuration.
	networks, err := normalizeNetworks(raw.Networks)
	if err != nil {
		return nil, err
	}
	t.Networks = networks

	// A template must do SOMETHING: it must carry at least one event rule or at
	// least one persistent service. A template with neither is inert and almost
	// certainly a template-authoring mistake, so it is rejected rather than
	// silently loading a function that can never run. A services-only template
	// is legitimate: a persistent HTTP service does not consume events.
	if len(raw.Events) == 0 && len(raw.Services) == 0 {
		return nil, fmt.Errorf("template must contain at least one event rule or one service")
	}

	// The runtime is required whenever Relay must know how to launch the
	// function: event/schedule handlers always run through a runtime, and an
	// `entrypoint` service is launched by a runtime-specific command. A
	// services-only template whose services ALL use `build` or `image` sources
	// needs no runtime at all — those images carry their own ENTRYPOINT/CMD —
	// so runtime is optional there. A mixed template (events or schedules
	// alongside services) still requires it.
	needsRuntime := len(raw.Events) > 0 || len(raw.Schedules) > 0
	for _, s := range raw.Services {
		if s.Entrypoint != "" {
			needsRuntime = true
		}
	}
	if t.Runtime == "" {
		if needsRuntime {
			return nil, fmt.Errorf("runtime is required")
		}
	} else if !supportedRuntimes[t.Runtime] {
		// An explicitly configured runtime is validated even when it is not
		// strictly needed, so a typo in an otherwise build/image-only template is
		// still a configuration error rather than silently ignored.
		return nil, fmt.Errorf("unsupported runtime %q", t.Runtime)
	}

	for _, ev := range raw.Events {
		if ev.Handler == "" {
			return nil, fmt.Errorf("rule is missing a handler")
		}
		if ev.Pattern == nil {
			return nil, fmt.Errorf("rule %q is missing a pattern", ev.Handler)
		}
		if err := validateHandler(ev.Handler); err != nil {
			return nil, err
		}
		timeout, err := resolveTimeout(ev.Timeout)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", ev.Handler, err)
		}
		retries, err := resolveRetries(ev.Retries)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", ev.Handler, err)
		}
		pattern := make(Pattern, len(ev.Pattern))
		for field, cond := range ev.Pattern {
			parsed, err := parseFieldCondition(field, cond, now)
			if err != nil {
				return nil, fmt.Errorf("rule %q: %w", ev.Handler, err)
			}
			pattern[field] = parsed
		}
		t.Events = append(t.Events, EventRule{Handler: ev.Handler, Pattern: pattern, Timeout: timeout, Retries: retries})
	}

	// Parse and validate the optional cron schedules. Each entry requires a
	// handler (module.function) and a 5-field cron expression. The timezone is
	// optional (defaults to UTC); the timeout follows the same rules as event
	// rules. Schedules are optional — a template with events and no schedules
	// key parses exactly as before.
	for _, s := range raw.Schedules {
		if s.Handler == "" {
			return nil, fmt.Errorf("schedule is missing a handler")
		}
		if err := validateHandler(s.Handler); err != nil {
			return nil, fmt.Errorf("schedule %q: %w", s.Handler, err)
		}
		if s.Cron == "" {
			return nil, fmt.Errorf("schedule %q: cron is required", s.Handler)
		}
		// Timezone is a separate template field: an embedded TZ=/CRON_TZ=
		// prefix would silently override it, so reject the ambiguity outright.
		if strings.Contains(s.Cron, "TZ=") {
			return nil, fmt.Errorf("schedule %q: cron expression must not embed TZ=/CRON_TZ=; use the timezone field", s.Handler)
		}
		loc := time.UTC
		if s.Timezone != "" {
			if s.Timezone == "Local" {
				return nil, fmt.Errorf("schedule %q: timezone %q must be an IANA location", s.Handler, s.Timezone)
			}
			l, err := time.LoadLocation(s.Timezone)
			if err != nil {
				return nil, fmt.Errorf("schedule %q: invalid timezone %q: %w", s.Handler, s.Timezone, err)
			}
			loc = l
		}
		timeout, err := resolveTimeout(s.Timeout)
		if err != nil {
			return nil, fmt.Errorf("schedule %q: %w", s.Handler, err)
		}
		retries, err := resolveRetries(s.Retries)
		if err != nil {
			return nil, fmt.Errorf("schedule %q: %w", s.Handler, err)
		}
		if err := validateCron(s.Cron, loc); err != nil {
			return nil, fmt.Errorf("schedule %q: invalid cron expression %q: %w", s.Handler, s.Cron, err)
		}
		t.Schedules = append(t.Schedules, Schedule{
			Handler:  s.Handler,
			Cron:     s.Cron,
			Location: loc,
			Timeout:  timeout,
			Retries:  retries,
		})
	}

	// Parse and validate the optional persistent services. Each entry requires
	// EXACTLY ONE source: `entrypoint` (a runtime-managed application entrypoint
	// file, e.g. "service.js" or "app/main.py", NOT the module.function
	// event-handler form, so validateHandler is intentionally NOT applied),
	// `build` (a Dockerfile path relative to the function directory), or
	// `image` (an external image reference). The source descriptor is the
	// service's identity: duplicates within the function would be ambiguous for
	// reconciliation, so they are rejected. Port and replicas are optional with
	// defaults (DefaultServicePort / DefaultServiceReplicas). Host and path are
	// optional and independently omitted-preserving: an omitted/empty path
	// leaves the service routed by host alone, and a path without a host is
	// rejected. Services are optional — a template without the `services` key
	// parses exactly as before.
	seen := make(map[string]bool, len(raw.Services))
	for _, s := range raw.Services {
		entrypoint, build, image := s.Entrypoint, s.Build, s.Image
		set := 0
		for _, v := range []string{entrypoint, build, image} {
			if v != "" {
				set++
			}
		}
		if set == 0 {
			return nil, fmt.Errorf("service is missing a source (entrypoint, build, or image)")
		}
		if set > 1 {
			return nil, fmt.Errorf("service declares multiple sources: exactly one of entrypoint, build, or image is allowed")
		}
		// A source descriptor is a single whitespace-free token for every kind:
		// an entrypoint is a path, a build is a path, and an image reference is a
		// registry reference — none may contain whitespace.
		source := entrypoint
		if build != "" {
			source = build
		}
		if image != "" {
			source = image
		}
		if strings.ContainsAny(source, " \t\r\n") {
			return nil, fmt.Errorf("service %q: source contains whitespace", source)
		}
		if seen[source] {
			return nil, fmt.Errorf("duplicate service %q", source)
		}
		seen[source] = true
		if build != "" {
			if err := validateServiceBuild(build); err != nil {
				return nil, fmt.Errorf("service %q: %w", build, err)
			}
		}
		if image != "" {
			if err := validateServiceImage(image); err != nil {
				return nil, fmt.Errorf("service %q: %w", image, err)
			}
		}
		port, err := resolveServicePort(s.Port)
		if err != nil {
			return nil, fmt.Errorf("service %q: %w", source, err)
		}
		replicas, err := resolveServiceReplicas(s.Replicas)
		if err != nil {
			return nil, fmt.Errorf("service %q: %w", source, err)
		}
		if err := validateServiceHost(s.Host); err != nil {
			return nil, fmt.Errorf("service %q: %w", source, err)
		}
		// A configured path only makes sense with a host: PathPrefix alone is
		// not an externally addressable route, and preserving the existing
		// no-host behavior (an unrouted, label-free service) requires rejecting
		// the combination rather than silently ignoring the path.
		if s.Path != "" && s.Host == "" {
			return nil, fmt.Errorf("service %q: path requires host", source)
		}
		path, err := canonicalizeServicePath(s.Path)
		if err != nil {
			return nil, fmt.Errorf("service %q: %w", source, err)
		}
		t.Services = append(t.Services, Service{
			Entrypoint: entrypoint,
			Build:      build,
			Image:      image,
			Host:       s.Host,
			Path:       path,
			Port:       port,
			Replicas:   replicas,
		})
	}
	return t, nil
}

// validateCron reports whether cronExpr is a valid cron expression evaluated in
// loc, delegating parsing/validation to gocron/v2 (no Relay-specific cron
// regexes). Accepted forms are the 5-field `minute hour day-of-month month
// day-of-week` and the 6-field `second minute hour day-of-month month
// day-of-week`; gocron/robfig's seconds-optional parser handles both. A
// throwaway scheduler is created per validation because gocron exposes
// validation through NewJob.
func validateCron(cronExpr string, loc *time.Location) error {
	sch, err := gocron.NewScheduler(gocron.WithLocation(loc))
	if err != nil {
		return err
	}
	defer func() { _ = sch.Shutdown() }()
	if _, err := sch.NewJob(gocron.CronJob(cronExpr, true), gocron.NewTask(func() {})); err != nil {
		return err
	}
	return nil
}

// envVarNamePattern restricts the characters an env-var name may contain. It is
// the POSIX-ish shell variable charset: a letter or underscore first, then
// letters, digits, or underscores. This is deliberately stricter than what the
// OS would accept so a template cannot smuggle a name that a shell or a
// container runtime would interpret differently (e.g. one containing '=' or a
// path separator).
var envVarNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnvVarNames are env-var names Relay owns; a template may never set
// them. RELAY_HANDLER carries the rule's handler identity from the platform —
// letting a template override it would let the template redefine which handler
// the runtime invokes, subverting the per-rule invocation contract.
var reservedEnvVarNames = map[string]bool{"RELAY_HANDLER": true}

// parseEnvSecrets validates and stores the template's env and secrets maps.
// Both are optional. Rules:
//   - env keys must be valid env-var names; values are literal strings (empty
//     values are allowed — flag-like variables are legitimate).
//   - secrets keys must be valid env-var names; the reference (the value) must
//     be a valid secret name and non-empty.
//   - a variable may not be defined in both env and secrets.
//   - Relay-reserved variable names (RELAY_HANDLER) may not be set.
//
// All errors are value-free: they name the offending key/reference but never
// any secret value.
func parseEnvSecrets(t *Template, env, secrets map[string]string) error {
	if len(env) > 0 {
		t.Env = make(map[string]string, len(env))
		for name, value := range env {
			if err := validateEnvVarName(name); err != nil {
				return err
			}
			if reservedEnvVarNames[name] {
				return fmt.Errorf("env var %q is reserved by Relay", name)
			}
			t.Env[name] = value
		}
	}
	if len(secrets) > 0 {
		t.Secrets = make(map[string]SecretRef, len(secrets))
		for name, ref := range secrets {
			if err := validateEnvVarName(name); err != nil {
				return err
			}
			if reservedEnvVarNames[name] {
				return fmt.Errorf("secret variable %q is reserved by Relay", name)
			}
			if err := ValidSecretName(ref); err != nil {
				return fmt.Errorf("secret reference for %q: %w", name, err)
			}
			t.Secrets[name] = SecretRef(ref)
		}
	}
	// A variable defined in both maps is ambiguous: reject it.
	for name := range t.Env {
		if _, dup := t.Secrets[name]; dup {
			return fmt.Errorf("env and secrets define the same variable %q", name)
		}
	}
	return nil
}

// normalizeNetworks parses and normalizes the optional top-level `networks`
// list: each entry is trimmed, empty entries are rejected, duplicates are
// removed, and the result is sorted. Nil/omitted input yields a nil slice. A
// non-string entry (a number, bool, map, or list) is rejected rather than
// coerced, so a template authoring mistake surfaces at parse time.
//
// The returned slice is deterministic: two templates that list the same set of
// networks in any order, with or without surrounding whitespace, normalize to
// the same slice, so reconciliation never churns on cosmetic differences.
func normalizeNetworks(raw []any) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		name, ok := entry.(string)
		if !ok {
			return nil, fmt.Errorf("network name must be a string, got %v", entry)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("network name must not be empty")
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// validateEnvVarName reports whether name is a legal env-var name. It rejects
// empty names and anything outside the POSIX-ish charset.
func validateEnvVarName(name string) error {
	if name == "" {
		return fmt.Errorf("env var name is empty")
	}
	if !envVarNamePattern.MatchString(name) {
		return fmt.Errorf("env var name %q is invalid", name)
	}
	return nil
}

// EnvList returns the template's literal env variables as a name-ordered slice,
// so callers iterate deterministically regardless of YAML map ordering.
func (t *Template) EnvList() []EnvVar {
	if len(t.Env) == 0 {
		return nil
	}
	names := make([]string, 0, len(t.Env))
	for name := range t.Env {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]EnvVar, 0, len(names))
	for _, name := range names {
		out = append(out, EnvVar{Name: name, Value: t.Env[name]})
	}
	return out
}

// SecretList returns the template's secret bindings as a name-ordered slice, so
// callers iterate deterministically regardless of YAML map ordering.
func (t *Template) SecretList() []SecretBinding {
	if len(t.Secrets) == 0 {
		return nil
	}
	names := make([]string, 0, len(t.Secrets))
	for name := range t.Secrets {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]SecretBinding, 0, len(names))
	for _, name := range names {
		out = append(out, SecretBinding{Name: name, Ref: t.Secrets[name]})
	}
	return out
}

// resolveTimeout parses an optional rule timeout. An empty string yields the
// default; zero, negative, unparseable, or over-MaxTimeout values are rejected.
func resolveTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return DefaultTimeout, nil
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout %q", raw)
	}
	if timeout <= 0 {
		return 0, fmt.Errorf("timeout %q must be positive", raw)
	}
	if timeout > MaxTimeout {
		return 0, fmt.Errorf("timeout %q exceeds max %s", raw, MaxTimeout)
	}
	return timeout, nil
}

// resolveConcurrency parses the optional per-function `concurrency` key. A nil
// value (omitted) yields the default; any non-integer value (a string, a float,
// a bool, ...), zero, or a negative integer is rejected. Concurrency must be a
// positive integer (a value of 0 does not mean "unbounded").
func resolveConcurrency(raw any) (int, error) {
	if raw == nil {
		return DefaultConcurrency, nil
	}
	n, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("concurrency must be a positive integer, got %v", raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("concurrency must be a positive integer, got %v", raw)
	}
	return n, nil
}

// resolveRetries parses an optional rule retry count. A nil value (omitted)
// yields the default; any non-integer value (a string, a float, a bool, ...) or
// a negative integer is rejected. Zero is valid (only the initial attempt).
func resolveRetries(raw any) (int, error) {
	if raw == nil {
		return DefaultRetries, nil
	}
	n, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("retries must be a non-negative integer, got %v", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("retries %d must be non-negative", n)
	}
	return n, nil
}

// resolveServicePort parses an optional service `port`. A nil value (omitted)
// yields DefaultServicePort; any non-integer value (a string, a float, a bool,
// ...) or an integer outside [1, 65535] is rejected.
func resolveServicePort(raw any) (int, error) {
	if raw == nil {
		return DefaultServicePort, nil
	}
	n, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("port must be an integer, got %v", raw)
	}
	if n < 1 || n > 65535 {
		return 0, fmt.Errorf("port %d must be between 1 and 65535", n)
	}
	return n, nil
}

// resolveServiceReplicas parses an optional service `replicas`. A nil value
// (omitted) yields DefaultServiceReplicas; any non-integer value (a string, a
// float, a bool, ...) or a non-positive integer is rejected.
func resolveServiceReplicas(raw any) (int, error) {
	if raw == nil {
		return DefaultServiceReplicas, nil
	}
	n, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("replicas must be a positive integer, got %v", raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("replicas %d must be a positive integer", n)
	}
	return n, nil
}

// validateServiceBuild validates a service `build` Dockerfile path. It must be a
// RELATIVE path inside the function directory (e.g. "Dockerfile" or
// "docker/Dockerfile.prod"): non-empty, no whitespace, no backslash, not
// absolute, and every "/" -separated element non-empty, not "."/ ".." and not
// dot-leading. The Dockerfile is read from the function directory at build
// time; confining it there keeps the build self-contained.
func validateServiceBuild(build string) error {
	if build == "" {
		return fmt.Errorf("build path is empty")
	}
	if strings.ContainsAny(build, " \t\r\n") {
		return fmt.Errorf("build path %q must not contain whitespace", build)
	}
	if strings.ContainsAny(build, "\\") {
		return fmt.Errorf("build path %q must not contain backslashes", build)
	}
	if strings.HasPrefix(build, "/") {
		return fmt.Errorf("build path %q must be a relative path inside the function directory", build)
	}
	for _, el := range strings.Split(build, "/") {
		if el == "" {
			return fmt.Errorf("build path %q must not contain empty path elements", build)
		}
		if el == ".." || strings.HasPrefix(el, ".") {
			return fmt.Errorf("build path %q: invalid path element %q (path elements must not start with \".\")", build, el)
		}
	}
	return nil
}

// serviceImagePattern is a permissive image-reference syntax check: a
// registry/repository reference with an optional tag or digest. It rejects the
// characters that would make a reference unparseable by the Docker client
// (whitespace is already rejected earlier; this additionally refuses an empty
// repository or a malformed tag separator). Full validation is left to the
// Docker client, which is the authority; this only catches obvious typos at
// template parse time.
var serviceImagePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*(@sha256:[a-fA-F0-9]{64})?$`)

// validateServiceImage validates a service `image` reference. It must be a
// non-empty container image reference (e.g. "nginx:1.27",
// "ghcr.io/acme/api@sha256:..."). The final authority is the Docker client's
// reference parser; this check rejects the obviously malformed cases early so a
// typo surfaces as a template error rather than a build-time pull failure.
func validateServiceImage(image string) error {
	if image == "" {
		return fmt.Errorf("image reference is empty")
	}
	if !serviceImagePattern.MatchString(image) {
		return fmt.Errorf("image reference %q is not a valid container image reference", image)
	}
	return nil
}

// hostnamePattern is an RFC-1123-style hostname: case-insensitive alphanumeric
// labels separated by dots, each label 1-63 chars and not hyphen-bounded.
// Validation is additionally gated on the total length (<= 253) and early
// whitespace rejection below.
var hostnamePattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$`)

// validateServiceHost validates the optional service `host`. Empty is valid
// (an internal unrouted service); anything else must be a valid hostname,
// because the value is handed to the routing layer verbatim, and a malformed
// host would silently never match any request.
func validateServiceHost(host string) error {
	if host == "" {
		return nil
	}
	if len(host) > 253 {
		return fmt.Errorf("host %q exceeds the 253-character hostname limit", host)
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return fmt.Errorf("host %q contains whitespace", host)
	}
	if !hostnamePattern.MatchString(host) {
		return fmt.Errorf("host %q is not a valid hostname", host)
	}
	return nil
}

// canonicalizeServicePath validates the optional service `path` and returns its
// canonical form. Empty is valid (host-only routing, exactly as before).
//
// A configured value must be an absolute URL path: a leading "/" is REQUIRED.
// Whitespace, a query ("?"), a fragment ("#"), and a backslash are rejected —
// each would make the PathPrefix rule ambiguous or unparseable. Empty ("//")
// segments are rejected too, so a canonical value never contains "//" (the root
// "/" is the one value that starts and ends with "/").
//
// Canonicalization is deliberately minimal and stable: a non-root value loses
// its trailing slashes ("/v2/" -> "/v2", "/v2///" -> "/v2"), while the root
// stays exactly "/". This makes "/v2" and "/v2/" the SAME configured path, so
// reconciliation does not churn containers over a cosmetic trailing slash.
func canonicalizeServicePath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	if !strings.HasPrefix(path, "/") {
		return "", fmt.Errorf("path %q must start with %q", path, "/")
	}
	if strings.ContainsAny(path, " \t\r\n") {
		return "", fmt.Errorf("path %q contains whitespace", path)
	}
	if strings.ContainsAny(path, "?#\\") {
		return "", fmt.Errorf("path %q must not contain a query, fragment, or backslash", path)
	}
	canonical := strings.TrimRight(path, "/")
	if canonical == "" {
		// The input was all slashes ("/", "//", "///"): only the root is valid.
		if path != "/" {
			return "", fmt.Errorf("path %q contains an empty path segment", path)
		}
		return "/", nil
	}
	if strings.Contains(canonical, "//") {
		return "", fmt.Errorf("path %q contains an empty path segment", path)
	}
	return canonical, nil
}

// validateHandler requires the form module.function (splitting at the last dot)
// with both parts non-empty and no whitespace.
func validateHandler(handler string) error {
	if handler == "" {
		return fmt.Errorf("handler is empty")
	}
	if strings.ContainsAny(handler, " \t\r\n") {
		return fmt.Errorf("handler %q contains whitespace", handler)
	}
	idx := strings.LastIndex(handler, ".")
	if idx < 0 {
		return fmt.Errorf("handler %q must be of the form module.function", handler)
	}
	module := handler[:idx]
	fn := handler[idx+1:]
	if module == "" {
		return fmt.Errorf("handler %q has an empty module", handler)
	}
	if fn == "" {
		return fmt.Errorf("handler %q has an empty function", handler)
	}
	return nil
}

// parseFieldCondition converts a decoded YAML node into a FieldCondition.
//   - a plain list -> implicit equality (OR across the values)
//   - a map with only operator keys (equals/prefix/suffix/exists) -> operators
//   - a map with other keys -> nested field conditions (AND with siblings)
//
// path is the dotted field path (e.g. "new_image.cnpj") used to give errors from
// strict operator validation (exists) enough context to locate the offending
// value. It must be non-empty for every field; nested fields append their name.
//
// now is the temporal clock for `now`-relative comparison operators, passed
// down unchanged so it is identical for the whole template.
func parseFieldCondition(path string, v any, now func() time.Time) (FieldCondition, error) {
	switch val := v.(type) {
	case []any:
		return FieldCondition{Operators: []ValueMatcher{equalityMatcher{values: val}}}, nil
	case map[string]any:
		if isOperatorMap(val) {
			return buildOperators(val, path, now)
		}
		children := make(map[string]FieldCondition, len(val))
		for field, child := range val {
			childCond, err := parseFieldCondition(path+"."+field, child, now)
			if err != nil {
				return FieldCondition{}, err
			}
			children[field] = childCond
		}
		return FieldCondition{Children: children}, nil
	default:
		// A bare scalar is treated as implicit equality with a single value.
		return FieldCondition{Operators: []ValueMatcher{equalityMatcher{values: []any{val}}}}, nil
	}
}

// buildOperators converts an operator-only map into a FieldCondition of
// operators. equals/prefix/suffix reuse the legacy silent-skip convention: a
// malformed value (e.g. prefix: "x" instead of a list) is dropped, never an
// error. exists is validated strictly instead: its value must be a YAML boolean
// (a Go bool after yaml.v3 decode — YAML true/false), and anything else is
// rejected with a pointing error rather than silently ignored. This asymmetry is
// deliberate: a missing operator key is a legitimate "this operator not used";
// a non-boolean exists value is almost certainly a template authoring mistake.
//
// gt/gte/lt/lte are NEW operators with no legacy convention to preserve, so they
// are validated strictly like exists. Each operand must be either a number (any
// numeric kind — compared numerically) or a `now()`-relative expression string
// (validated with parseNowOperand). Anything else — null, a bool, a map, a
// plain non-now string such as "hello" or a literal RFC3339 timestamp — is
// rejected rather than silently ignored. A literal timestamp string is NOT
// treated as a date; only the exact `now()`/`now()±duration` syntax triggers
// temporal comparison, so rejecting anything-but is the honest, fail-fast choice.
func buildOperators(m map[string]any, path string, now func() time.Time) (FieldCondition, error) {
	var matchers []ValueMatcher
	if vals, ok := m["equals"].([]any); ok {
		matchers = append(matchers, equalityMatcher{values: vals})
	}
	if vals, ok := m["prefix"].([]any); ok {
		matchers = append(matchers, prefixMatcher{prefixes: toStrings(vals)})
	}
	if vals, ok := m["suffix"].([]any); ok {
		matchers = append(matchers, suffixMatcher{suffixes: toStrings(vals)})
	}
	if raw, ok := m["exists"]; ok {
		want, ok := raw.(bool)
		if !ok {
			// yaml.v3 decodes YAML booleans into Go bool, so any non-bool here is
			// a "true", 1, 1.5, null, list, or similar — none is a valid polarity.
			return FieldCondition{}, fmt.Errorf("%s: exists must be a boolean, got %v", path, raw)
		}
		matchers = append(matchers, existsMatcher{want: want})
	}
	for _, op := range []struct {
		key string
		op  comparisonOp
	}{{key: "gt", op: opGt}, {key: "gte", op: opGte}, {key: "lt", op: opLt}, {key: "lte", op: opLte}} {
		if raw, ok := m[op.key]; ok {
			built, err := buildComparison(op.key, op.op, raw, path, now)
			if err != nil {
				return FieldCondition{}, err
			}
			matchers = append(matchers, built...)
		}
	}
	return FieldCondition{Operators: matchers}, nil
}

// buildComparison converts a gt/gte/lt/lte operand into one comparisonMatcher per
// operand (a list means OR across operands — the same field-level OR that
// match() already applies to multiple matchers, so emitting several is the
// natural fit). Validation is strict and names the field path on error.
func buildComparison(key string, op comparisonOp, raw any, path string, now func() time.Time) ([]ValueMatcher, error) {
	// A scalar is treated as a single-element operand list.
	norms := make([]any, 0, 1)
	switch v := raw.(type) {
	case []any:
		if len(v) == 0 {
			return nil, fmt.Errorf(`%s: %s operand must be a number or a "now()" / "now()±duration" expression, got an empty list`,
				path, key)
		}
		norms = v
	case nil:
		return nil, fmt.Errorf(`%s: %s operand must be a number or a "now()" / "now()±duration" expression, got null`,
			path, key)
	default:
		norms = append(norms, v)
	}

	for _, bound := range norms {
		switch bound := bound.(type) {
		case string:
			// The ONLY strings allowed are valid `now()`/`now()±duration`
			// expressions. Anything else — a literal timestamp, "hello", "abc",
			// or the removed bare `now` syntax — is rejected rather than
			// silently never-matching. Validation is pure syntax: it records
			// only the relative offset and never consults a clock.
			_, temporal, err := parseNowOperand(bound)
			if err != nil {
				return nil, fmt.Errorf("%s: %s: %w", path, key, err)
			}
			if !temporal {
				return nil, fmt.Errorf(`%s: %s operand must be a number or a "now()" / "now()±duration" expression, got %q`,
					path, key, bound)
			}
		case nil:
			return nil, fmt.Errorf(`%s: %s operand must be a number or a "now()" / "now()±duration" expression, got null`,
				path, key)
		default:
			if !isNumeric(bound) {
				return nil, fmt.Errorf(`%s: %s operand must be a number or a "now()" / "now()±duration" expression, got %v`,
					path, key, bound)
			}
		}
	}

	matchers := make([]ValueMatcher, 0, len(norms))
	for _, bound := range norms {
		m := comparisonMatcher{op: op}
		if s, ok := bound.(string); ok {
			// Validated above; re-parsing cannot fail. The clock is stored so the
			// cutoff is computed from it at match time; the relative duration's
			// sign is already baked in, so now.Add(durNow) shifts the cutoff
			// correctly (now()-5m -> the cutoff is 5 minutes in the past).
			offset, _, _ := parseNowOperand(s)
			m.kind = nowComparison
			m.durNow = offset
			m.now = now
		} else {
			m.kind = numericComparison
			m.numBound, _ = toFloat(bound)
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}

// isOperatorMap reports whether the map has only operator keys. If so, it is a
// set of operators rather than nested field conditions.
func isOperatorMap(m map[string]any) bool {
	if len(m) == 0 {
		return false
	}
	for k := range m {
		if !operatorKeys[k] {
			return false
		}
	}
	return true
}

// toStrings converts decoded YAML values to strings, skipping non-strings.
func toStrings(vals []any) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// parseNowOperand interprets a comparison operand string as either a plain
// string (returns (0, false, nil)) or a `now()`-relative expression. It is a
// pure syntax function and never consults a clock: it validates the `now()`
// syntax and records only the RELATIVE offset, with the sign baked in.
// Computing the actual cutoff (clock().Add(offset)) is deferred to match time
// via comparisonMatcher.
//
// Accepted forms (no whitespace anywhere):
//
//	"now()"     -> (0, true) — zero offset; cutoff = clock()
//	"now()±DUR" -> (±DUR, true), where DUR is a non-empty time.ParseDuration
//	                            and the sign is baked in (e.g. "now()-5m" -> -5m)
//
// Everything else is "not a temporal operand":
//
//	"hello", "now", "now-5m", "NOW()", "2026-09-12T10:00:00Z" -> (0, false, nil)
//
// Note: the removed bare `now`/`now±duration` syntax (without the parentheses)
// no longer parses as temporal — it falls into the "not a temporal operand"
// branch above. The caller (buildComparison) rejects it as a non-number,
// non-`now()` string, so `gt: "now-5m"` is a parse-time validation error rather
// than a rule that silently never matches or is interpreted as a date.
//
// Malformed expressions that look like a `now()` operand are an error rather
// than silently non-temporal, so a typo is caught at parse time instead of
// becoming a rule that never matches:
//
//	"now()-"   -> error ("-" followed by nothing)
//	"now()+"   -> error
//	"now()-foo" -> error (duration "foo" fails ParseDuration)
//	"now() - 5m" -> error (the expression must have no interior whitespace)
//	"now()--5m" -> error (a sign directly after the leading sign is not a valid
//	                      duration; now()--5m would otherwise silently mean
//	                      now()+5m)
//	"now()5m"  -> error (a duration must be preceded by '+' or '-')
//	"now() "   -> error (trailing whitespace is not a valid duration)
func parseNowOperand(value string) (time.Duration, bool, error) {
	if value == "now()" {
		return 0, true, nil
	}
	if !strings.HasPrefix(value, "now()") {
		return 0, false, nil
	}
	// Only "+" or "-" may follow "now()"; anything else (a space, a letter) is
	// malformed. value[5:] keeps the sign as part of the duration text so the
	// minus of now()-5m and plus of now()+5m survive: ParseDuration("-5m") =
	// -5m, ParseDuration("+5m") = 5m. A bare "-"/"+", and the double-sign typo
	// now()--5m ("--5m"), all fail ParseDuration and are rejected rather than
	// silently mis-arithmetic.
	if len(value) < 6 || (value[5] != '+' && value[5] != '-') {
		return 0, true, fmt.Errorf("invalid now() expression %q: expected %q or %q followed by a duration",
			value, "now()", "now()±duration")
	}
	offset, err := time.ParseDuration(value[5:])
	if err != nil {
		return 0, true, fmt.Errorf("invalid now() expression %q: %w", value, err)
	}
	return offset, true, nil
}
