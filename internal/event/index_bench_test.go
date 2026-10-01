package event

import (
	"fmt"
	"strings"
	"testing"

	"relay/internal/app"
)

// benchTemplate builds a template with n event rules in a realistic operator mix:
// ~70% equality (each with a distinct unique value, so at most one is a
// candidate), ~20% positive exists on distinct fields, and ~10% prefix fallback
// (always a candidate). The event below matches exactly one equality rule and one
// exists rule; the fallback rules are all candidates, so the candidate set is
// ~10% of n. This models the production shape the index is meant to prune.
func benchTemplate(b *testing.B, n int) *app.Template {
	b.Helper()
	var sb strings.Builder
	sb.WriteString("runtime: python3.14\nevents:\n")
	for i := 0; i < n; i++ {
		sb.WriteString("  - handler: handler.r")
		sb.WriteString(fmt.Sprint(i))
		sb.WriteString("\n    pattern:\n")
		switch i % 10 {
		case 7, 8: // exists, on a distinct field per rule
			fmt.Fprintf(&sb, "      present_%d: [{exists: true}]\n", i)
		case 9: // prefix fallback
			fmt.Fprintf(&sb, "      id_%d: [{prefix: \"pfx-%d\"}]\n", i, i)
		default: // equality on a shared field with a unique value
			fmt.Fprintf(&sb, "      status: [v%d]\n", i)
		}
	}
	tmpl, err := app.ParseTemplate([]byte(sb.String()))
	if err != nil {
		b.Fatalf("parse bench template: %v", err)
	}
	return tmpl
}

// benchEvent matches exactly one equality rule (rule 5: status v5) and one exists
// rule (rule 8: present_8), so an index that works returns a tiny candidate set
// regardless of n.
func benchEvent() map[string]any {
	return map[string]any{"status": "v5", "present_8": 1, "unrelated": "x"}
}

// BenchmarkIndexCandidates measures only the candidate lookup (anchor probe plus
// posting merge), with no exact verification.
func BenchmarkIndexCandidates(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 5000} {
		tmpl := benchTemplate(b, n)
		ix := NewRuleIndex(tmpl.Events)
		event := benchEvent()
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := ix.Candidates(event); len(got) == 0 {
					b.Fatal("no candidates")
				}
			}
		})
	}
}

// BenchmarkIndexedMatch measures candidate lookup PLUS exact verification of every
// candidate — the indexed replacement for a full scan.
func BenchmarkIndexedMatch(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 5000} {
		tmpl := benchTemplate(b, n)
		ix := NewRuleIndex(tmpl.Events)
		event := benchEvent()
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := ix.MatchingEventRules(event); len(got) != 2 {
					b.Fatalf("indexed match = %d rules, want 2", len(got))
				}
			}
		})
	}
}

// BenchmarkFullScan is the baseline: a full exact scan over every rule, the
// behavior the index replaces. It is the plain matcher, not a production
// full-scan helper.
func BenchmarkFullScan(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 5000} {
		tmpl := benchTemplate(b, n)
		event := benchEvent()
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := MatchingEventRules(tmpl.Events, event); len(got) != 2 {
					b.Fatalf("full scan = %d rules, want 2", len(got))
				}
			}
		})
	}
}
