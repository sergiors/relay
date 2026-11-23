//go:build integration

package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"relay/internal/state"
	"relay/internal/testutil"
)

// publishFixture returns a registry-valid endpoint for publication; the
// advertised address a reader sees comes from the node lease metadata, not
// from the published row itself.
func publishFixture(
	app, service, container, host, path string, hostPort int,
) state.ServiceRegistryEndpoint {
	endpoint := registryEndpoint(app, service, host, path, "", hostPort)
	endpoint.ContainerID = container
	endpoint.HostIP = "10.0.0.1"
	endpoint.InternalPort = 8080
	endpoint.Generation = "gen-" + container
	return endpoint
}

// openDiscoveryFixture claims this test's node lease against an isolated
// schema database (testutil fails — never skips — when PostgreSQL at
// TEST_DATABASE_URL is unreachable).
func openDiscoveryFixture(
	t *testing.T, ttl time.Duration,
) (*state.State, state.NodeLease) {
	t.Helper()
	testutil.RequireStateDatabase(t)
	st := testutil.NewState(t, state.Options{
		NodeID:   testutil.UniqueName(t, "node"),
		Instance: testutil.UniqueName(t, "inst"),
		Logger:   testutil.DiscardLogger(),
	})
	lease, err := st.ClaimNodeLease(context.Background(),
		state.NodeLeaseMetadata{
			BindAddr:      "127.0.0.1",
			AdvertiseAddr: "192.0.2.55",
		}, ttl)
	if err != nil {
		t.Fatalf("claim node lease: %v", err)
	}
	return st, lease
}

// Given a real state database holding one published, eligible endpoint, when
// the discovery handler polls, then the served configuration routes the
// published host/path to the node's advertised address end-to-end (registry →
// projection → Traefik transform); after the graceful lease release the same
// poll serves an empty configuration.
func TestDiscoveryServesPublishedRegistryRoutes(t *testing.T) {
	ctx := context.Background()
	st, lease := openDiscoveryFixture(t, time.Minute)

	if err := st.PublishServiceRegistryEndpoints(ctx, lease,
		[]state.ServiceRegistryEndpoint{
			publishFixture("alpha", "web", "ctr-1", "api.example.com", "/v2", 41000),
		}); err != nil {
		t.Fatalf("publish endpoints: %v", err)
	}

	s := NewServer("127.0.0.1:0", st, testutil.DiscardLogger())
	rec := pollTraefik(t, s)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET after publish = %d, want 200", rec.Code)
	}
	var cfg traefikConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("response is not a Traefik configuration: %v", err)
	}
	_, router := onlyRouter(t, cfg)
	if router.Rule != "Host(`api.example.com`) && PathPrefix(`/v2`)" {
		t.Fatalf("rule = %q, want the published host/path", router.Rule)
	}
	service, ok := cfg.HTTP.Services[router.Service]
	if !ok {
		t.Fatalf("router service %q missing from services", router.Service)
	}
	want := []string{"http://192.0.2.55:41000"}
	if got := serverURLs(service); !slices.Equal(got, want) {
		t.Fatalf("servers = %v, want %v (lease-advertised address + published port)",
			got, want)
	}

	// Graceful withdrawal: releasing the lease removes this incarnation's
	// endpoints, so the next poll serves an empty configuration.
	if err := st.ReleaseNodeLease(ctx, lease); err != nil {
		t.Fatalf("release node lease: %v", err)
	}
	rec = pollTraefik(t, s)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET after release = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"http":{}}` {
		t.Fatalf("body after release = %s, want an empty configuration", got)
	}
}

// Given a published endpoint whose node lease and rows expire on the database
// clock, when the discovery handler polls past that expiry, then the served
// configuration is empty. Only the discovery-visible effect is asserted here;
// the fencing and expiry mechanics are covered by internal/state's integration
// suite.
func TestDiscoveryEmptyAfterRegistryLeaseExpiry(t *testing.T) {
	const ttl = 2 * time.Second
	st, lease := openDiscoveryFixture(t, ttl)

	if err := st.PublishServiceRegistryEndpoints(context.Background(), lease,
		[]state.ServiceRegistryEndpoint{
			publishFixture("alpha", "web", "ctr-1", "api.example.com", "", 41000),
		}); err != nil {
		t.Fatalf("publish endpoints: %v", err)
	}

	s := NewServer("127.0.0.1:0", st, testutil.DiscardLogger())
	rec := pollTraefik(t, s)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET before expiry = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got == `{"http":{}}` {
		t.Fatal("published endpoint was never visible before its lease expired")
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		rec := pollTraefik(t, s)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET after publish = %d, want 200", rec.Code)
		}
		body := rec.Body.String()
		if body == `{"http":{}}` {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("route still served past a %v lease (waited %v): %s",
				ttl, time.Since(deadline), body)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
