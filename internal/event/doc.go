// Package event is the event-pattern matching layer for Relay apps.
//
// It evaluates decoded events against the declarative patterns a parsed
// template carries (internal/app) and pre-filters rules with a conservative
// candidate index:
//
//   - MatchingEventRules evaluates every rule's pattern against an event and
//     returns the matching rules in declaration order. A pattern's top-level
//     fields are ANDed; a field's alternatives are OR; a field's nested
//     children are ANDed with each other and with the field's alternatives.
//     There is no deduplication: each matching rule is a distinct invocation.
//   - RuleIndex is an immutable, false-positive-only candidate index over one
//     template's rules, built once per published app generation. It never
//     changes which rules match: it narrows which rules are exact-tested via a
//     conservative necessary ("anchor") condition and always defers the final
//     decision to the exact matcher. Because it caches nothing, temporal
//     (`now()`-relative) rules stay dynamic and are re-evaluated per call.
//
// The package owns no discovery, validation, parsing, scheduling, or execution.
// The declarative model (Template/EventRule/Pattern and the parser that produces
// them) belongs to internal/app; this package consumes those types read-only and
// app never imports event.
package event
