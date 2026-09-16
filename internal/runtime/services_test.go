package runtime

import (
	"fmt"
	"strings"
	"testing"
)

func TestServiceLabels(t *testing.T) {
	got := serviceLabels(ServiceSpec{
		Function: "user-events",
		Handler:  "service.js",
		Port:     3000,
		Image:    "relay-fn-user-events:632aca75fa306911",
	}, "worker-1", 2)

	want := map[string]string{
		labelType:     ContainerTypeService,
		labelFunction: "user-events",
		labelHandler:  "service.js",
		labelImage:    "relay-fn-user-events:632aca75fa306911",
		labelHostname: "worker-1",
		labelPort:     "3000",
		labelReplica:  "2",
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
}

func TestServiceContainerName(t *testing.T) {
	for _, tc := range []struct {
		name     string
		function string
		handler  string
		replica  int
		want     string
	}{
		{"plain", "user-events", "service.js", 0, "relay-svc-user-events-service.js-0"},
		{"sanitize handler", "fn", "my service@v1", 1, "relay-svc-fn-my-service-v1-1"},
		{"cap over 100", "averylongfunctionname", strings.Repeat("x", 200), 99, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := serviceContainerName(tc.function, tc.handler, tc.replica)
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

func TestValidateServiceHandler(t *testing.T) {
	for _, tc := range []struct {
		handler string
		wantErr string
	}{
		{"service.js", ""},
		{"api.js", ""},
		{"", "empty"},
		{"a b.js", "no spaces"},
		{"dir/service.js", "path separators"},
		{"/etc/passwd", "path separators"},
		{"../escape.js", "path separators"},
		{"..hidden.js", "path"},
		{".hidden.js", "path"},
	} {
		t.Run(fmt.Sprintf("%q", tc.handler), func(t *testing.T) {
			err := validateServiceHandler(tc.handler)
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
		runtime string
		handler string
		want    []string
		wantErr bool
	}{
		{"node24", "service.js", []string{"node", "/app/service.js"}, false},
		{"python3.14", "server.py", []string{"python", "/app/server.py"}, false},
		{"unknown", "service.js", nil, true},
		{"node24", "../../etc/passwd", nil, true},
	} {
		t.Run(tc.runtime+"/"+tc.handler, func(t *testing.T) {
			got, err := ServiceEntry(tc.runtime, tc.handler)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
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
	svc := serviceLabels(ServiceSpec{Function: "f", Handler: "svc", Image: "img"}, "test-host", 0)
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
