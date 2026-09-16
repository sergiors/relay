package runtime

import (
	"fmt"
	"strings"
	"testing"
)

func TestServiceLabels(t *testing.T) {
	got := serviceLabels(ServiceSpec{
		Function:   "user-events",
		Entrypoint: "service.js",
		Port:       3000,
		Image:      "relay-fn-user-events:632aca75fa306911",
	}, "worker-1", 2)

	want := map[string]string{
		labelType:       ContainerTypeService,
		labelFunction:   "user-events",
		labelEntrypoint: "service.js",
		labelImage:      "relay-fn-user-events:632aca75fa306911",
		labelHostname:   "worker-1",
		labelPort:       "3000",
		labelReplica:    "2",
	}
	if len(got) != len(want) {
		t.Fatalf("label count = %d, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("label %q = %q, want %q", k, got[k], v)
		}
	}
	for k := range got {
		if k == labelType && got[k] != ContainerTypeService {
			t.Errorf("relay.type must be %q, got %q", ContainerTypeService, got[k])
		}
	}
	// Service containers carry relay.entrypoint and NO relay.handler and NO
	// relay.service label.
	if _, ok := got[labelHandler]; ok {
		t.Errorf("service labels must NOT carry relay.handler, got %v", got)
	}
	if _, ok := got["relay.service"]; ok {
		t.Errorf("service labels must NOT carry relay.service, got %v", got)
	}
}

func TestServiceContainerName(t *testing.T) {
	for _, tc := range []struct {
		name       string
		function   string
		entrypoint string
		replica    int
		want       string
	}{
		{"plain", "user-events", "service.js", 0, "relay-svc-user-events-service.js-0"},
		{"nested", "fn", "app/service.js", 0, "relay-svc-fn-app-service.js-0"},
		{"sanitize entrypoint", "fn", "my service@v1", 1, "relay-svc-fn-my-service-v1-1"},
		{"cap over 100", "averylongfunctionname", strings.Repeat("x", 200), 99, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := serviceContainerName(tc.function, tc.entrypoint, tc.replica)
			if len(got) > 100 {
				t.Fatalf("name length %d exceeds cap 100", len(got))
			}
			if tc.want != "" && got != tc.want {
				t.Errorf("name = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestServiceContainerNameDeterministic(t *testing.T) {
	a := serviceContainerName("fn", "service.js", 3)
	b := serviceContainerName("fn", "service.js", 3)
	if a != b {
		t.Fatalf("name not deterministic: %q vs %q", a, b)
	}
}

func TestValidateServiceEntrypoint(t *testing.T) {
	for _, tc := range []struct {
		entrypoint string
		wantErr    string
	}{
		{"service.js", ""},
		{"api.js", ""},
		{"app/service.js", ""},
		{"app/main.py", ""},
		{"a/b/c.js", ""},
		{"", "empty"},
		{"a b.js", "whitespace"},
		{"app service.js", "whitespace"},
		{"/etc/passwd", "relative path"},
		{"../escape.js", "must not contain"},
		{"..hidden.js", "path elements must not"},
		{".hidden.js", "path elements must not"},
		{"a//b.js", "empty path elements"},
		{"a/./b.js", "path elements must not"},
		{"back\\slash.js", "backslashes"},
		{"a/..hidden/b.js", "path elements must not"},
	} {
		t.Run(fmt.Sprintf("%q", tc.entrypoint), func(t *testing.T) {
			err := validateServiceEntrypoint(tc.entrypoint)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestServiceEntry(t *testing.T) {
	for _, tc := range []struct {
		runtime    string
		entrypoint string
		want       []string
		wantErr    string // substring; empty means success
	}{
		{"node24", "service.js", []string{"node", "/app/service.js"}, ""},
		{"python3.14", "service.py", []string{"python", "-m", "service"}, ""},
		{"node24", "app/service.js", []string{"node", "/app/app/service.js"}, ""},
		{"python3.14", "app/main.py", []string{"python", "-m", "app.main"}, ""},
		{"python3.14", "app/http/server.py", []string{"python", "-m", "app.http.server"}, ""},
		{"node24", "a/b/c.js", []string{"node", "/app/a/b/c.js"}, ""},
		// Python entrypoints that are valid files but not importable modules fail
		// with a python-specific error (the conversion runs after validation).
		{"python3.14", "app/main.js", nil, "require a .py module file"},
		{"python3.14", "app/my-file.py", nil, "not a valid Python module name"},
		{"unknown", "service.js", nil, "unsupported runtime"},
		// The generic path rules reject these for BOTH runtimes, before any
		// runtime-specific translation.
		{"node24", "../../etc/passwd", nil, "must not contain"},
		{"python3.14", "../main.py", nil, "must not contain"},
		{"python3.14", "/app/main.py", nil, "relative"},
		{"python3.14", "a//b.py", nil, "empty path elements"},
		{"python3.14", ".hidden.py", nil, "path elements must not"},
	} {
		t.Run(tc.runtime+"/"+tc.entrypoint, func(t *testing.T) {
			got, err := ServiceEntry(tc.runtime, tc.entrypoint)
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
			if len(got) != len(tc.want) {
				t.Fatalf("entry = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("entry[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestSweepSkipsServiceContainers verifies sweepSkips (the decision helper
// SweepOrphanContainers uses for every container) excludes service containers
// even though they carry relay.function + relay.hostname and would otherwise be
// Relay-owned — a service must never be swept as an orphan.
func TestSweepSkipsServiceContainers(t *testing.T) {
	svc := serviceLabels(ServiceSpec{Function: "f", Entrypoint: "svc", Image: "img"}, "test-host", 0)
	if !sweepSkips(svc) {
		t.Error("sweepSkips(service labels) = false, want true (services are persistent, reconciler-owned)")
	}
	// A typed invocation container (relay.type=event) must NOT be skipped.
	exec := runLabels(RunMeta{Type: ContainerTypeEvent, Function: "f", Hostname: "test-host"})
	if sweepSkips(exec) {
		t.Error("sweepSkips(event invocation labels) = true, want false")
	}
	// A non-Relay container (no known relay.type, no relay.function) is skipped.
	if !sweepSkips(map[string]string{"app": "x"}) {
		t.Error("sweepSkips(non-relay) = false, want true")
	}
	// A container carrying relay.function but NO relay.type is strictly NOT
	// Relay-owned under the new model and must be skipped (backward compat is
	// not required).
	if !sweepSkips(map[string]string{labelFunction: "f", labelHostname: "h"}) {
		t.Error("sweepSkips(relay.function but no relay.type) = false, want true")
	}
}
