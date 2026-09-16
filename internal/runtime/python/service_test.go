package python

import (
	"reflect"
	"strings"
	"testing"
)

func TestServiceCommand(t *testing.T) {
	for _, tc := range []struct {
		name       string
		entrypoint string
		want       []string
		wantErr    string // substring; empty means success
	}{
		{"top-level module", "main.py", []string{"python", "-m", "main"}, ""},
		{"one-package module", "app/main.py", []string{"python", "-m", "app.main"}, ""},
		{"deep package module", "app/http/server.py", []string{"python", "-m", "app.http.server"}, ""},
		{"src nested", "src/http/server.py", []string{"python", "-m", "src.http.server"}, ""},

		{"not a py file", "app/main.js", nil, "require a .py module file"},
		{"no suffix", "app/main", nil, "require a .py module file"},
		{"double extension", "app/main.pyx", nil, "require a .py module file"},
		{"uppercase suffix is rejected", "main.PY", nil, "require a .py module file"},
		{"digit-leading element", "app/1main.py", nil, "not a valid Python module name"},
		{"hyphen filename not importable", "app/my-file.py", nil, "not a valid Python module name"},
		{"hidden element", "app/.hidden.py", nil, "not a valid Python module name"},
		{"digit-leading module after conversion", "app/2020.py", nil, "not a valid Python module name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ServiceCommand(tc.entrypoint)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("entry = %v, want %v", got, tc.want)
			}
		})
	}
}
