package runtime

import (
	"reflect"
	"testing"
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
