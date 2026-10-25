package reconciler

import (
	"maps"
	"testing"

	"relay/internal/runtime"
)

// Relay Docker label keys the service-container cleanup filters on. They mirror
// the unexported constants in internal/runtime/labels.go: this package cannot
// import them, and the strict label set is the ownership contract, so the
// literals are pinned by TestServiceContainerOwnedBy.
const (
	testLabelType     = "relay.type"
	testLabelApp      = "relay.app"
	testLabelHostname = "relay.hostname"
)

// serviceContainerOwnedBy reports whether labels identify a service container
// belonging to app on the given manager/worker identity. Ownership is strict and
// label-derived: relay.type must be exactly the Relay service type (the
// Relay-owned service population), and both relay.app and relay.hostname must
// match. A container with a foreign or missing type, another app, or another
// worker's identity is never selected, so cleanup cannot stop or remove an
// unrelated container.
func serviceContainerOwnedBy(labels map[string]string, app, hostname string) bool {
	if app == "" || hostname == "" {
		return false
	}
	return labels[testLabelType] == runtime.ContainerTypeService &&
		labels[testLabelApp] == app &&
		labels[testLabelHostname] == hostname
}

// TestServiceContainerOwnedBy pins the strict ownership predicate used by the
// Docker integration cleanup. Matching a returned container by App or name alone
// is not enough: only the complete relay.type + relay.app + relay.hostname label
// set proves this test owns it.
func TestServiceContainerOwnedBy(t *testing.T) {
	const (
		app      = "svc-env-abc"
		hostname = "test-host"
	)
	own := map[string]string{
		testLabelType:     "service",
		testLabelApp:      app,
		testLabelHostname: hostname,
		"relay.service":   "app-service",
	}
	with := func(key, value string) map[string]string {
		labels := maps.Clone(own)
		labels[key] = value
		return labels
	}
	without := func(key string) map[string]string {
		labels := maps.Clone(own)
		delete(labels, key)
		return labels
	}

	tests := []struct {
		name     string
		labels   map[string]string
		app      string
		hostname string
		want     bool
	}{
		{name: "own service container", labels: own, app: app, hostname: hostname, want: true},
		{name: "nil labels", labels: nil, app: app, hostname: hostname},
		{name: "invocation container with same app", labels: with(testLabelType, "event"), app: app, hostname: hostname},
		{name: "unknown container type", labels: with(testLabelType, "foreign"), app: app, hostname: hostname},
		{name: "missing container type", labels: without(testLabelType), app: app, hostname: hostname},
		{name: "same type, different app", labels: with(testLabelApp, "other-app"), app: app, hostname: hostname},
		{name: "missing app label", labels: without(testLabelApp), app: app, hostname: hostname},
		{name: "same app, different worker identity", labels: with(testLabelHostname, "other-host"), app: app, hostname: hostname},
		{name: "missing hostname label", labels: without(testLabelHostname), app: app, hostname: hostname},
		{name: "empty app argument", labels: with(testLabelApp, ""), app: "", hostname: hostname},
		{name: "empty hostname argument", labels: with(testLabelHostname, ""), app: app, hostname: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := serviceContainerOwnedBy(tt.labels, tt.app, tt.hostname); got != tt.want {
				t.Errorf("serviceContainerOwnedBy(%v, %q, %q) = %t, want %t", tt.labels, tt.app, tt.hostname, got, tt.want)
			}
		})
	}
}
