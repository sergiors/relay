package logging

import "testing"

func TestFields(t *testing.T) {
	cases := []struct {
		name string
		kv   []any
		want string
	}{
		{"empty", nil, ""},
		{"noFields", []any{}, ""},
		{"simple", []any{"function", "user-events", "handler", "events.created.handler"}, " function=user-events handler=events.created.handler"},
		{"quotedValue", []any{"msg", "hello world"}, ` msg="hello world"`},
		{"skipsEmpty", []any{"function", "x", "event_id", ""}, " function=x"},
		{"formatsNonString", []any{"attempt", 2, "duration", "1.2s"}, " attempt=2 duration=1.2s"},
		{"ignoresTrailingOdd", []any{"a", "1", "b"}, " a=1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Fields(c.kv...); got != c.want {
				t.Errorf("Fields(%v) = %q, want %q", c.kv, got, c.want)
			}
		})
	}
}
