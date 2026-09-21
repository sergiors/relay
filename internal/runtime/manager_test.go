package runtime

import (
	"reflect"
	"testing"

	"relay/internal/function"
)

// TestEnvMapParsesKeyValueAndLaterWins verifies the per-invocation env parsing:
// each "K=V" becomes an entry, a later entry overrides an earlier one of the
// same name (so a rotated secret takes effect without a rebuild), a value may
// itself contain '=' (only the first '=' splits), and an entry with no '=' is
// ignored. An empty slice yields a nil map.
func TestEnvMapParsesKeyValueAndLaterWins(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want map[string]string
	}{
		{"empty is nil", nil, nil},
		{"single", []string{"A=1"}, map[string]string{"A": "1"}},
		{"later wins", []string{"A=1", "A=2"}, map[string]string{"A": "2"}},
		{
			"first equals splits only",
			[]string{"TOKEN=a=b=c"},
			map[string]string{"TOKEN": "a=b=c"},
		},
		{
			"missing equals ignored",
			[]string{"NOVALUE", "A=1"},
			map[string]string{"A": "1"},
		},
		{
			"empty value preserved",
			[]string{"EMPTY="},
			map[string]string{"EMPTY": ""},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := envMap(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("envMap(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestTemplateHandlersExtraction pins the handler-module extraction: module
// parts only (everything before the LAST dot), collected from events and
// schedules, sorted and deduped, with malformed entries skipped and a nil
// template yielding nil.
func TestTemplateHandlersExtraction(t *testing.T) {
	cases := []struct {
		name string
		fn   function.Function
		want []string
	}{
		{name: "nil template", fn: function.Function{}},
		{
			name: "event modules",
			fn: function.Function{Template: &function.Template{
				Events: []function.EventRule{
					{Handler: "handler.handler"},
					{Handler: "src.order.handler"},
				},
			}},
			want: []string{"handler", "src.order"},
		},
		{
			name: "schedules deduped and sorted with events",
			fn: function.Function{Template: &function.Template{
				Events: []function.EventRule{
					{Handler: "src.order.handler"},
					{Handler: "handler.handler"},
				},
				Schedules: []function.Schedule{
					{Handler: "src.order.cleanup"},
					{Handler: "jobs.report.handler"},
				},
			}},
			want: []string{"handler", "jobs.report", "src.order"},
		},
		{
			name: "malformed handlers skipped",
			fn: function.Function{Template: &function.Template{
				Events: []function.EventRule{
					{Handler: "nodot"},
					{Handler: ".fn"},
					{Handler: "ok.handler"},
				},
			}},
			want: []string{"ok"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := templateHandlers(tc.fn)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("templateHandlers() = %v, want %v", got, tc.want)
			}
		})
	}
}
