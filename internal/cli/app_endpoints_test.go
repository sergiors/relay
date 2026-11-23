package cli

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/state"
	"relay/internal/worker"
)

func renderServiceEndpointsForTest(view *serviceEndpointView) string {
	var buf bytes.Buffer
	renderAppServiceEndpoints(newOutput(&buf), view)
	return buf.String()
}

func TestRenderAppServiceEndpoints(t *testing.T) {
	got := renderServiceEndpointsForTest(&serviceEndpointView{
		BindAddr:      "127.0.0.1",
		AdvertiseAddr: "10.0.0.5",
		Live:          true,
		Endpoints: []worker.ServiceEndpoint{
			{
				Service: "web", Replica: 0, State: "running", Health: "healthy",
				Port: 8080, HostIP: "127.0.0.1", HostPort: 49152,
			},
			{
				// A stopped container that still reports a stale healthy probe
				// must never be presented as healthy.
				Service: "web", Replica: 1, State: "exited", Health: "healthy",
				Port: 8080,
			},
			{
				Service: "api", Replica: 0, State: "running", Health: "starting",
				Port: 9000, HostIP: "127.0.0.1", HostPort: 49153,
			},
			{Service: "api", Replica: 1, Missing: true, Port: 9000},
			{
				Service: "v6", Replica: 0, State: "running",
				Port: 8443, HostIP: "2001:db8::1", HostPort: 32768,
			},
		},
	})

	for _, want := range []string{
		"Service endpoints:",
		"Bind:",
		"127.0.0.1",
		"Advertise:",
		"10.0.0.5",
		"web[0]",
		"running health=healthy",
		"bind=127.0.0.1:49152 advertise=10.0.0.5:49152 -> 8080/tcp",
		"web[1]",
		"exited",
		"not published",
		"api[0]",
		"health=starting",
		"api[1]",
		"not found",
		"v6[0]",
		"bind=[2001:db8::1]:32768 advertise=10.0.0.5:32768 -> 8443/tcp",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "web[1]") && strings.Contains(got, "exited health=healthy") {
		t.Errorf("a stopped container was marked healthy:\n%s", got)
	}
}

func TestRenderAppServiceEndpointsUnavailable(t *testing.T) {
	got := renderServiceEndpointsForTest(&serviceEndpointView{
		BindAddr:      "127.0.0.1",
		AdvertiseAddr: "10.0.0.5",
	})
	for _, want := range []string{
		"Service endpoints:",
		"127.0.0.1",
		"unavailable (worker or Docker unavailable)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "not published") {
		t.Errorf("unavailable view must not render guessed endpoints:\n%s", got)
	}
}

// An unset, empty, or whitespace-only SERVICE_ADVERTISE_ADDR resolves to the
// bind address, so the rendered view advertises the same effective endpoint.
func TestRenderAppServiceEndpointsAdvertiseDefaultsToBind(t *testing.T) {
	got := renderServiceEndpointsForTest(&serviceEndpointView{
		BindAddr:      "127.0.0.1",
		AdvertiseAddr: "127.0.0.1",
		Live:          true,
		Endpoints: []worker.ServiceEndpoint{{
			Service: "web", Replica: 0, State: "running",
			Port: 8080, HostIP: "127.0.0.1", HostPort: 49152,
		}},
	})
	if !strings.Contains(got, "bind=127.0.0.1:49152 advertise=127.0.0.1:49152 -> 8080/tcp") {
		t.Errorf("output missing the effective bind and advertised endpoint:\n%s", got)
	}
	if !strings.Contains(got, "Advertise:") {
		t.Errorf("output missing the advertise row:\n%s", got)
	}
}

func TestRenderAppServiceEndpointsNilIsNoOp(t *testing.T) {
	if got := renderServiceEndpointsForTest(nil); got != "" {
		t.Fatalf("nil view rendered %q, want no output", got)
	}
}

func TestResolveServiceEndpointsDisabled(t *testing.T) {
	view, err := resolveServiceEndpoints("fn", Dependencies{
		LoadServicePublication: func() (string, string, error) { return "", "", nil },
	})
	if err != nil {
		t.Fatalf("resolveServiceEndpoints: %v", err)
	}
	if view != nil {
		t.Fatalf("view = %+v, want nil when publication is disabled", view)
	}
}

func TestResolveServiceEndpointsWorkerUnavailable(t *testing.T) {
	view, err := resolveServiceEndpoints("fn", Dependencies{
		LoadServicePublication: func() (string, string, error) {
			return "127.0.0.1", "10.0.0.5", nil
		},
		SocketPath: filepath.Join(t.TempDir(), "missing.sock"),
	})
	if err != nil {
		t.Fatalf("resolveServiceEndpoints: %v", err)
	}
	if view == nil || view.Live {
		t.Fatalf("view = %+v, want a non-live configured view", view)
	}
	if view.BindAddr != "127.0.0.1" || view.AdvertiseAddr != "10.0.0.5" {
		t.Fatalf("addresses = %q/%q, want the configured values", view.BindAddr, view.AdvertiseAddr)
	}
	if len(view.Endpoints) != 0 {
		t.Fatalf("endpoints = %+v, want none when the worker is unavailable", view.Endpoints)
	}
}

func TestResolveServiceEndpointsConfigError(t *testing.T) {
	want := errors.New("bad bind")
	if _, err := resolveServiceEndpoints("fn", Dependencies{
		LoadServicePublication: func() (string, string, error) { return "", "", want },
	}); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

// The production CLI seam resolves the same effective publication configuration
// the worker uses: an explicitly empty SERVICE_ADVERTISE_ADDR inherits the bind
// address, so a non-live inspect view reports identical bind and advertised
// addresses instead of suppressing the advertised one.
func TestResolveServiceEndpointsSharesEffectivePublicationConfig(t *testing.T) {
	t.Setenv("SERVICE_BIND_ADDR", "127.0.0.1")
	t.Setenv("SERVICE_ADVERTISE_ADDR", "")

	deps := DefaultDependencies()
	deps.SocketPath = filepath.Join(t.TempDir(), "missing.sock")

	view, err := resolveServiceEndpoints("fn", deps)
	if err != nil {
		t.Fatalf("resolveServiceEndpoints: %v", err)
	}
	if view == nil {
		t.Fatal("view = nil, want a publication view while the bind address is set")
	}
	if view.BindAddr != "127.0.0.1" || view.AdvertiseAddr != "127.0.0.1" {
		t.Fatalf("addresses = %q/%q, want the effective bind address twice",
			view.BindAddr, view.AdvertiseAddr)
	}
}

// Health and state render exactly as Docker reports them: an unhealthy running
// replica stays visible as unhealthy, a starting (pending) container shows its
// state with no endpoint, and an unknown replica index renders the bare service
// name rather than a negative index. IPv6 addresses bracket in both the bind
// and the advertise endpoint, which share the same actual host port.
func TestRenderAppServiceEndpointsHealthStatesAndIPv6Advertise(t *testing.T) {
	got := renderServiceEndpointsForTest(&serviceEndpointView{
		BindAddr:      "2001:db8::1",
		AdvertiseAddr: "2001:db8::2",
		Live:          true,
		Endpoints: []worker.ServiceEndpoint{
			{
				Service: "web", Replica: 0, State: "running", Health: "unhealthy",
				Port: 8080, HostIP: "2001:db8::1", HostPort: 32768,
			},
			{Service: "web", Replica: 1, State: "created", Port: 8080},
			{Service: "web", Replica: -1, State: "running", Port: 8080},
		},
	})

	for _, want := range []string{
		"running health=unhealthy",
		"bind=[2001:db8::1]:32768 advertise=[2001:db8::2]:32768 -> 8080/tcp",
		"created",
		"not published",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "web[-1]") {
		t.Errorf("an unknown replica index rendered a negative index:\n%s", got)
	}
}

// The endpoints section appears in the full `app inspect` rendering only when a
// publication view exists; without one the output keeps the historical
// internal-only shape.
func TestRenderAppDetailIncludesServiceEndpointsSection(t *testing.T) {
	view := &serviceEndpointView{
		BindAddr:      "127.0.0.1",
		AdvertiseAddr: "127.0.0.1",
		Live:          true,
		Endpoints: []worker.ServiceEndpoint{{
			Service: "web", Replica: 0, State: "running", Health: "healthy",
			Port: 8080, HostIP: "127.0.0.1", HostPort: 49152,
		}},
	}
	var w bytes.Buffer
	if err := renderAppDetailWithProvenance(
		&w, provenanceTestDetail(), state.AppStats{}, appProvenance{}, view,
	); err != nil {
		t.Fatalf("render app detail: %v", err)
	}
	out := w.String()
	for _, want := range []string{"Service endpoints:", "bind=127.0.0.1:49152"} {
		if !strings.Contains(out, want) {
			t.Errorf("detail output missing %q:\n%s", want, out)
		}
	}

	w.Reset()
	if err := renderAppDetailWithProvenance(
		&w, provenanceTestDetail(), state.AppStats{}, appProvenance{}, nil,
	); err != nil {
		t.Fatalf("render app detail: %v", err)
	}
	if strings.Contains(w.String(), "Service endpoints:") {
		t.Errorf("detail output rendered endpoints without a publication view:\n%s", w.String())
	}
}
