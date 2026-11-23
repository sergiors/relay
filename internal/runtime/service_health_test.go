package runtime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/testutil"
)

// healthcheckSpec is a resolved service probe in its default shape: the
// template parser guarantees these values before a spec reaches StartService.
func healthcheckSpec() *app.ServiceHealthcheck {
	return &app.ServiceHealthcheck{
		Path:        "/health",
		Interval:    10 * time.Second,
		Timeout:     3 * time.Second,
		Retries:     3,
		StartPeriod: 10 * time.Second,
	}
}

// The healthcheck identity label is a stable, value-free digest: it is nil-safe,
// reproducible, fixed length, and changes whenever any probe field changes so a
// probe-only edit replaces the running container.
func TestServiceHealthcheckLabelIdentity(t *testing.T) {
	if got := ServiceHealthcheckLabel(nil); got != "" {
		t.Fatalf("nil healthcheck label = %q, want empty (no label)", got)
	}
	base := ServiceHealthcheckLabel(healthcheckSpec())
	if len(base) != serviceIdentityHashLen {
		t.Fatalf("label length = %d, want %d", len(base), serviceIdentityHashLen)
	}
	if again := ServiceHealthcheckLabel(healthcheckSpec()); again != base {
		t.Fatalf("label not deterministic: %q != %q", again, base)
	}
	variants := map[string]*app.ServiceHealthcheck{}
	for name, mutate := range map[string]func(*app.ServiceHealthcheck){
		"path":         func(hc *app.ServiceHealthcheck) { hc.Path = "/ready" },
		"interval":     func(hc *app.ServiceHealthcheck) { hc.Interval = 20 * time.Second },
		"timeout":      func(hc *app.ServiceHealthcheck) { hc.Timeout = 2 * time.Second },
		"retries":      func(hc *app.ServiceHealthcheck) { hc.Retries = 4 },
		"start_period": func(hc *app.ServiceHealthcheck) { hc.StartPeriod = 20 * time.Second },
	} {
		hc := healthcheckSpec()
		mutate(hc)
		variants[name] = hc
	}
	for name, hc := range variants {
		if got := ServiceHealthcheckLabel(hc); got == base {
			t.Errorf("%s change did not change the healthcheck identity", name)
		}
	}
}

// The Docker HEALTHCHECK is built from the runtime's own executable in exec
// form — no shell — with the configured probe path, port, and timings mapped
// onto Docker's native fields.
func TestServiceHealthcheckConfigMapsProbeToDocker(t *testing.T) {
	for _, tc := range []struct {
		name       string
		runtime    string
		executable string
		flag       string
		timeoutArg string
	}{
		{"node", "nodejs24.x", "node", "-e", "3000"},
		{"python", "python3.14", "python", "-c", "3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hc := healthcheckSpec()
			cfg, err := serviceHealthConfig(tc.runtime, 8080, hc)
			if err != nil {
				t.Fatalf("serviceHealthConfig: %v", err)
			}
			test := cfg.Test
			if len(test) != 7 || test[0] != "CMD" || test[1] != tc.executable ||
				test[2] != tc.flag {
				t.Fatalf("test = %q, want CMD %s %s <script> ...",
					test, tc.executable, tc.flag)
			}
			if len(test[3]) == 0 || !strings.Contains(test[3], "http") {
				t.Fatalf("probe program = %q, want an HTTP client program", test[3])
			}
			if test[4] != "8080" || test[5] != "/health" || test[6] != tc.timeoutArg {
				t.Fatalf("probe args = %q, want port 8080, path /health, timeout %s",
					test[4:], tc.timeoutArg)
			}
			if cfg.Interval != hc.Interval || cfg.Timeout != hc.Timeout ||
				cfg.StartPeriod != hc.StartPeriod || cfg.Retries != hc.Retries {
				t.Fatalf("timings = %+v, want the configured %+v", cfg, hc)
			}
		})
	}
}

// A nil probe builds no Docker healthcheck, and a direct ServiceSpec caller
// cannot install a configuration the template parser would reject.
func TestServiceHealthcheckConfigRejectsInvalidDirectSpec(t *testing.T) {
	if cfg, err := serviceHealthConfig("nodejs24.x", 80, nil); err != nil || cfg != nil {
		t.Fatalf("nil healthcheck = (%v, %v), want (nil, nil)", cfg, err)
	}

	valid := healthcheckSpec()
	broken := map[string]func(*app.ServiceHealthcheck){
		"relative path":  func(hc *app.ServiceHealthcheck) { hc.Path = "health" },
		"query in path":  func(hc *app.ServiceHealthcheck) { hc.Path = "/health?x=1" },
		"zero interval":  func(hc *app.ServiceHealthcheck) { hc.Interval = 0 },
		"zero timeout":   func(hc *app.ServiceHealthcheck) { hc.Timeout = 0 },
		"timeout over":   func(hc *app.ServiceHealthcheck) { hc.Timeout = 30 * time.Second },
		"zero retries":   func(hc *app.ServiceHealthcheck) { hc.Retries = 0 },
		"too many":       func(hc *app.ServiceHealthcheck) { hc.Retries = 11 },
		"negative start": func(hc *app.ServiceHealthcheck) { hc.StartPeriod = -time.Second },
	}
	for name, mutate := range broken {
		t.Run(name, func(t *testing.T) {
			hc := healthcheckSpec()
			mutate(hc)
			if _, err := serviceHealthConfig("nodejs24.x", 80, hc); err == nil ||
				!strings.Contains(err.Error(), "healthcheck") {
				t.Fatalf("error = %v, want a healthcheck rejection", err)
			}
		})
	}
	if _, err := serviceHealthConfig("ruby3.x", 80, valid); err == nil ||
		!strings.Contains(err.Error(), "unsupported runtime") {
		t.Fatalf("unknown runtime error = %v, want an unsupported-runtime rejection", err)
	}
	if _, err := serviceHealthConfig("nodejs24.x", 0, valid); err == nil ||
		!strings.Contains(err.Error(), "invalid service port") {
		t.Fatalf("port 0 error = %v, want an invalid-port rejection", err)
	}
}

// startServiceWithHealth runs StartService against a scripted daemon whose
// post-start inspect returns the given JSON state, capturing the create body.
// No DELETE route is scripted: a pending or failed start that discarded the
// container would surface as an unhandled daemon request.
func startServiceWithHealth(
	t *testing.T, state string,
) (string, error, []byte) {
	t.Helper()
	var createBody []byte
	cli := newScriptedDockerClient(t,
		imageWithoutDeclaredVolumesRoute("img"),
		dockerRoute{method: http.MethodPost, path: "/containers/create",
			body:   `{"Id":"cid-1"}`,
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) }},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			body: state},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	id, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: "service.js", Port: 8080,
		Image: "img", Env: []string{"PORT=8080"},
		Runtime: "nodejs24.x", Entry: []string{"node", "/app/service.js"},
		Healthcheck: healthcheckSpec(), HealthcheckAttempt: 2,
	}, 0)
	return id, err, createBody
}

// The create request carries Docker's native HEALTHCHECK plus the identity and
// retry-attempt labels, so a probe-only change replaces the container and a
// restart can reconstruct the bounded retry window.
func TestStartServiceSendsNativeHealthcheckAndIdentityLabels(t *testing.T) {
	_, err, body := startServiceWithHealth(t,
		`{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"healthy"}}}`)
	if err != nil {
		t.Fatalf("StartService: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("no /containers/create body captured")
	}
	cfg := decodeCreateConfig(t, body)
	if cfg.Healthcheck == nil {
		t.Fatal("create Config.Healthcheck is nil, want the native probe")
	}
	want := ServiceHealthcheckLabel(healthcheckSpec())
	if got := cfg.Labels[labelHealthcheck]; got != want {
		t.Errorf("relay.healthcheck = %q, want %q", got, want)
	}
	if got := cfg.Labels[labelHealthcheckAttempt]; got != "2" {
		t.Errorf("relay.healthcheck_attempt = %q, want 2", got)
	}
	if cfg.Healthcheck.Retries != 3 || cfg.Healthcheck.Interval != 10*time.Second {
		t.Fatalf("Docker healthcheck = %+v, want the configured probe", cfg.Healthcheck)
	}
}

// A healthcheck candidate is not a viable swap target until Docker reports it
// healthy: starting, unhealthy, and exited-before-readiness results all report
// ErrServicePending WITH the created container ID and never discard it, so the
// reconciler can observe the candidate and preserve the old generation.
func TestStartServiceHealthcheckStatesReportPendingAndRetainCandidate(t *testing.T) {
	cases := []struct {
		name  string
		state string
	}{
		{"starting", `{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"starting"}}}`},
		{"unhealthy", `{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"unhealthy"}}}`},
		{"no health summary yet", `{"Id":"cid-1","State":{"Running":true}}`},
		{"early exit", `{"Id":"cid-1","State":{"Running":false,"Status":"exited"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err, _ := startServiceWithHealth(t, tc.state)
			if !errors.Is(err, ErrServicePending) {
				t.Fatalf("error = %v, want ErrServicePending", err)
			}
			if id != "cid-1" {
				t.Fatalf("id = %q, want the created candidate cid-1", id)
			}
		})
	}

	t.Run("healthy converges", func(t *testing.T) {
		id, err, _ := startServiceWithHealth(t,
			`{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"healthy"}}}`)
		if err != nil {
			t.Fatalf("StartService = %v, want success for a healthy candidate", err)
		}
		if id != "cid-1" {
			t.Fatalf("id = %q, want cid-1", id)
		}
	})
}

// Publication verification never disturbs the healthcheck candidate contract.
// A RUNNING published candidate with a VERIFIED binding that is not yet healthy
// still reports ErrServicePending with its ID and no discard; an EXCITED
// published candidate (whose live mapping is gone with its sandbox) still
// reports pending and is retained — the mapping gate runs only after the
// running gate; and a RUNNING candidate whose mapping is NOT verified is
// discarded with the hard verification error instead of being observed as
// pending forever.
func TestStartServicePublishedCandidatePendingRetention(t *testing.T) {
	startPublished := func(t *testing.T, inspect string) (string, error, int) {
		t.Helper()
		removed := 0
		cli := newScriptedDockerClient(t,
			imageWithoutDeclaredVolumesRoute("img"),
			dockerRoute{method: http.MethodPost, path: "/containers/create",
				body: `{"Id":"cid-1"}`},
			dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
			dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
				body: inspect},
			dockerRoute{method: http.MethodDelete, path: "/containers/cid-1",
				body: `{}`, onMatch: func() { removed++ }},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
		id, err := m.StartService(context.Background(), ServiceSpec{
			App: "fn", Name: "svc", SourceRef: "service.js", Port: 8080,
			Image: "img", Env: []string{"PORT=8080"}, BindAddr: "127.0.0.1",
			Runtime: "nodejs24.x", Entry: []string{"node", "/app/service.js"},
			Healthcheck: healthcheckSpec(), HealthcheckAttempt: 2,
		}, 0)
		return id, err, removed
	}
	const exposed = `"Config":{"ExposedPorts":{"8080/tcp":{}}}`
	const verified = `"NetworkSettings":{"Ports":{"8080/tcp":[` +
		`{"HostIp":"127.0.0.1","HostPort":"49152"}]}}`
	const noMapping = `"NetworkSettings":{"Ports":{}}`

	t.Run("running with verified binding reports pending", func(t *testing.T) {
		id, err, removed := startPublished(t,
			`{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"starting"}},`+
				exposed+`,`+verified+`}`)
		if !errors.Is(err, ErrServicePending) {
			t.Fatalf("error = %v, want ErrServicePending", err)
		}
		if id != "cid-1" {
			t.Fatalf("id = %q, want the created candidate cid-1", id)
		}
		if removed != 0 {
			t.Fatalf("discard removals = %d, want 0 (candidate retained)", removed)
		}
	})

	t.Run("exited before readiness still retains the candidate", func(t *testing.T) {
		id, err, removed := startPublished(t,
			`{"Id":"cid-1","State":{"Running":false,"Status":"exited"},`+
				exposed+`,`+noMapping+`}`)
		if !errors.Is(err, ErrServicePending) {
			t.Fatalf("error = %v, want ErrServicePending (the running gate decides first)", err)
		}
		if id != "cid-1" {
			t.Fatalf("id = %q, want the created candidate cid-1", id)
		}
		if removed != 0 {
			t.Fatalf("discard removals = %d, want 0 (candidate retained)", removed)
		}
	})

	t.Run("healthy with verified binding converges", func(t *testing.T) {
		id, err, removed := startPublished(t,
			`{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"healthy"}},`+
				exposed+`,`+verified+`}`)
		if err != nil {
			t.Fatalf("StartService = %v, want success for a healthy published candidate", err)
		}
		if id != "cid-1" {
			t.Fatalf("id = %q, want cid-1", id)
		}
		if removed != 0 {
			t.Fatalf("discard removals = %d, want 0", removed)
		}
	})

	t.Run("running without a verified mapping is discarded", func(t *testing.T) {
		id, err, removed := startPublished(t,
			`{"Id":"cid-1","State":{"Running":true,"Health":{"Status":"starting"}},`+
				exposed+`,`+noMapping+`}`)
		if err == nil || errors.Is(err, ErrServicePending) {
			t.Fatalf("error = %v, want the hard mapping failure, not pending", err)
		}
		if !strings.Contains(err.Error(),
			"did not report exactly one 8080/tcp host binding on 127.0.0.1") {
			t.Fatalf("error = %v, want the actionable mapping-verification message", err)
		}
		if id != "" {
			t.Fatalf("id = %q, want empty on a discarded candidate", id)
		}
		if removed != 1 {
			t.Fatalf("discard removals = %d, want 1 (an unverifiable candidate must not survive)",
				removed)
		}
	})
}

// A direct ServiceSpec caller with a probe but no entrypoint source fails
// closed before any daemon call, mirroring the template parser's rejection of
// a probe on an external image.
func TestStartServiceHealthcheckRequiresEntrypointSource(t *testing.T) {
	creates := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create",
			body: `{"Id":"unexpected"}`, onMatch: func() { creates++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	_, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: "nginx:1.27", Port: 80,
		Image: "nginx:1.27", Healthcheck: healthcheckSpec(),
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "entrypoint source") {
		t.Fatalf("error = %v, want the entrypoint-source rejection", err)
	}
	if creates != 0 {
		t.Fatalf("creates = %d, want 0 (rejected before create)", creates)
	}
}

// A HOT_RELOAD source mount bypasses Relay healthcheck behavior entirely: no
// Docker probe and no healthcheck identity labels are applied, so a
// healthcheck-only template edit can never replace or gate an in-place
// source restart.
func TestStartServiceHotReloadBypassesHealthcheck(t *testing.T) {
	var createBody []byte
	cli := newScriptedDockerClient(t,
		imageWithoutDeclaredVolumesRoute("img"),
		dockerRoute{method: http.MethodPost, path: "/containers/create",
			body:   `{"Id":"cid-1"}`,
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) }},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			body: `{"Id":"cid-1","State":{"Running":true}}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	if _, err := m.StartService(context.Background(), ServiceSpec{
		App: "fn", Name: "svc", SourceRef: "service.js", Port: 8080,
		Image: "img", Env: []string{"PORT=8080"},
		Runtime: "nodejs24.x", Entry: []string{"node", "/app/service.js"},
		Healthcheck: healthcheckSpec(),
		SourceMount: &SourceMount{
			HostPath: "/apps/fn", Target: "/app", Identity: "fp1",
		},
	}, 0); err != nil {
		t.Fatalf("StartService: %v", err)
	}
	cfg := decodeCreateConfig(t, createBody)
	if cfg.Healthcheck != nil {
		t.Fatalf("HOT_RELOAD create sent a Docker healthcheck: %+v", cfg.Healthcheck)
	}
	if got := cfg.Labels[labelHealthcheck]; got != "" {
		t.Errorf("relay.healthcheck = %q, want empty under HOT_RELOAD", got)
	}
	if got := cfg.Labels[labelHealthcheckAttempt]; got != "" {
		t.Errorf("relay.healthcheck_attempt = %q, want empty under HOT_RELOAD", got)
	}
}

// Discovery derives readiness ONLY from Docker's health summary for containers
// carrying Relay's own healthcheck label: an image's own HEALTHCHECK (or a
// missing summary) keeps the historical running-is-converged behavior with an
// unknown readiness.
func TestServiceContainerListDerivesHealthReadiness(t *testing.T) {
	const healthLabel = `"relay.healthcheck":"abc123"`
	const attemptLabel = `"relay.healthcheck_attempt":"4"`
	body := `[
	  {"Id":"c1","Created":1700000000,
	   "Labels":{"relay.type":"service","relay.app":"fn","relay.service":"web",` + healthLabel + `,` + attemptLabel + `},
	   "Health":{"Status":"starting"}},
	  {"Id":"c2",
	   "Labels":{"relay.type":"service","relay.app":"fn","relay.service":"web",` + healthLabel + `},
	   "Health":{"Status":"healthy"}},
	  {"Id":"c3",
	   "Labels":{"relay.type":"service","relay.app":"fn","relay.service":"web"},
	   "Health":{"Status":"unhealthy"}},
	  {"Id":"c4",
	   "Labels":{"relay.type":"service","relay.app":"fn","relay.service":"web",` + healthLabel + `}},
	  {"Id":"c5",
	   "Labels":{"relay.type":"service","relay.app":"fn","relay.service":"web",` + healthLabel + `},
	   "Health":{"Status":"none"}}
	]`
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: body})
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}
	list, err := m.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("ServiceContainerList: %v", err)
	}
	byID := map[string]ServiceContainer{}
	for _, ctr := range list {
		byID[ctr.ID] = ctr
	}

	starting := byID["c1"]
	if starting.Health != container.Starting {
		t.Errorf("c1 health = %q, want starting", starting.Health)
	}
	if starting.Ready == nil || *starting.Ready {
		t.Errorf("c1 ready = %v, want false while starting", starting.Ready)
	}
	if starting.HealthcheckAttempt != 4 {
		t.Errorf("c1 attempt = %d, want the parsed 4", starting.HealthcheckAttempt)
	}
	wantCreated := time.Unix(1700000000, 0)
	if !starting.CreatedAt.Equal(wantCreated) {
		t.Errorf("c1 created = %s, want %s", starting.CreatedAt, wantCreated)
	}

	healthy := byID["c2"]
	if healthy.Health != container.Healthy || healthy.Ready == nil || !*healthy.Ready {
		t.Errorf("c2 = (%q, %v), want healthy and ready", healthy.Health, healthy.Ready)
	}

	// c3 carries an image-declared healthcheck only: it is not Relay's contract,
	// so neither health nor readiness is reported.
	external := byID["c3"]
	if external.Health != "" || external.Ready != nil {
		t.Errorf("c3 = (%q, %v), want no Relay health/readiness", external.Health, external.Ready)
	}

	// c4 has Relay's label but Docker reports no summary yet: unknown readiness.
	noSummary := byID["c4"]
	if noSummary.Health != "" || noSummary.Ready != nil {
		t.Errorf("c4 = (%q, %v), want unknown readiness", noSummary.Health, noSummary.Ready)
	}

	// c5's image explicitly disables healthchecks: never a readiness signal.
	disabled := byID["c5"]
	if disabled.Health != "" || disabled.Ready != nil {
		t.Errorf("c5 = (%q, %v), want no readiness for a disabled healthcheck",
			disabled.Health, disabled.Ready)
	}
}

// Observation reports a definitive missing container as Missing, keeps a
// healthy container's state, and surfaces a transient inspect failure through
// the call error with that ID omitted (never as a confirmed removal).
func TestObserveServiceHealthMissingVersusTemporaryError(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/ok/json",
			body: `{"Id":"ok","State":{"Status":"running","Health":{"Status":"unhealthy"}}}`},
		dockerRoute{method: http.MethodGet, path: "/containers/gone/json",
			status: http.StatusNotFound, body: `{"message":"No such container: gone"}`},
		dockerRoute{method: http.MethodGet, path: "/containers/blip/json",
			status: http.StatusInternalServerError, body: `{"message":"daemon blip"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	got, err := m.ObserveServiceHealth(context.Background(),
		[]string{"ok", "gone", "blip"})
	if err == nil {
		t.Fatal("a transient inspect failure must be reported as an error")
	}
	obs, ok := got["ok"]
	if !ok || obs.Missing || obs.State != container.StateRunning ||
		obs.Health != container.Unhealthy {
		t.Fatalf("ok observation = (%+v, %v), want the running unhealthy state", obs, ok)
	}
	if gone, ok := got["gone"]; !ok || !gone.Missing {
		t.Fatalf("gone observation = (%+v, %v), want a definitive Missing", gone, ok)
	}
	if _, ok := got["blip"]; ok {
		t.Fatal("a transient failure must leave its ID out of the result map")
	}
}

// A candidate's startup deadline is derived from Docker's durable START instant
// plus the desired grace window: create time is deliberately not used, because a
// container can be created long before it is started. The start instant is
// honored at sub-second precision. A candidate that never reports health or has
// no usable start instant relies on Docker's own unhealthy transition, and a
// service without a probe is never expired.
func TestServiceHealthCandidateExpiredUsesDurableDeadline(t *testing.T) {
	hc := healthcheckSpec()
	grace := hc.StartPeriod + time.Duration(hc.Retries+1)*hc.Interval
	fresh := time.Now()
	aged := fresh.Add(-grace - time.Second)

	if ServiceHealthCandidateExpired(ServiceContainer{}, nil) {
		t.Error("a service without a healthcheck must never be expired")
	}
	if ServiceHealthCandidateExpired(
		ServiceContainer{Health: container.Healthy, StartedAt: aged}, hc) {
		t.Error("a healthy candidate must never be expired")
	}
	if !ServiceHealthCandidateExpired(
		ServiceContainer{Health: container.Unhealthy, StartedAt: fresh}, hc) {
		t.Error("Docker reporting unhealthy must expire the candidate immediately")
	}
	if ServiceHealthCandidateExpired(
		ServiceContainer{Health: container.Starting}, hc) {
		t.Error("a starting candidate without a durable start instant defers to Docker's transition")
	}
	if ServiceHealthCandidateExpired(
		ServiceContainer{Health: container.Starting, CreatedAt: aged}, hc) {
		t.Error("create time must not stand in for an unavailable start instant")
	}
	if ServiceHealthCandidateExpired(
		ServiceContainer{Health: container.Starting, StartedAt: fresh}, hc) {
		t.Error("a starting candidate inside its startup grace must not be expired")
	}
	if !ServiceHealthCandidateExpired(
		ServiceContainer{Health: container.Starting, StartedAt: aged}, hc) {
		t.Error("a starting candidate past its durable deadline must be expired")
	}
	if ServiceHealthCandidateExpired(
		ServiceContainer{
			Health: container.Starting, CreatedAt: aged, StartedAt: fresh,
		}, hc) {
		t.Error("a delayed create->start must not expire a freshly started candidate early")
	}
	if ServiceHealthCandidateExpired(
		ServiceContainer{CreatedAt: fresh}, hc) {
		t.Error("an unlabeled-health candidate inside its grace must not be expired")
	}
	if !ServiceHealthCandidateExpired(
		ServiceContainer{CreatedAt: fresh, StartedAt: aged}, hc) {
		t.Error("an unlabeled-health candidate past the deadline must be expired")
	}
	// Sub-second precision: truncating the start instant to whole seconds would
	// flip both of these.
	if ServiceHealthCandidateExpired(ServiceContainer{
		Health:    container.Starting,
		StartedAt: fresh.Add(-grace + 500*time.Millisecond),
	}, hc) {
		t.Error("a candidate 500ms inside its deadline must not be expired")
	}
	if !ServiceHealthCandidateExpired(ServiceContainer{
		Health:    container.Starting,
		StartedAt: fresh.Add(-grace - 500*time.Millisecond),
	}, hc) {
		t.Error("a candidate 500ms past its deadline must be expired")
	}
}

// Docker reports State.StartedAt with nanosecond precision and the durable
// anchor keeps every digit; an empty, unparseable, or year-1 value is not a
// usable start instant and yields the zero time instead of substituting create
// time.
func TestParseDockerStartInstantPreservesNanoseconds(t *testing.T) {
	got := parseDockerStartInstant("2026-11-21T11:37:01.057756123Z")
	want := time.Date(2026, 11, 21, 11, 37, 1, 57756123, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("parsed = %v, want %v", got, want)
	}
	if got.Nanosecond() != 57756123 {
		t.Fatalf("nanoseconds = %d, want the exact 57756123", got.Nanosecond())
	}
	for name, value := range map[string]string{
		"empty":       "",
		"unparseable": "not-a-time",
		"zero year":   "0001-01-01T00:00:00Z",
	} {
		t.Run(name, func(t *testing.T) {
			if got := parseDockerStartInstant(value); !got.IsZero() {
				t.Fatalf("parsed %q = %v, want the zero time", value, got)
			}
		})
	}
}

// ServiceContainerList reads the durable start instant only for the candidates
// whose startup grace a caller can still be measuring: a still-starting probe
// candidate is inspected (honoring nanosecond precision and a delayed
// create->start), a converged healthy container adds no Docker call, and a
// failed inspect leaves StartedAt zero rather than failing discovery.
func TestServiceContainerListFillsStartedAtOnlyForPendingCandidates(t *testing.T) {
	const healthLabel = `"relay.healthcheck":"abc123"`
	const labels = `"relay.type":"service","relay.app":"fn","relay.service":"web",` + healthLabel
	body := `[
	  {"Id":"starting","Created":1600000000,"State":"running",
	   "Labels":{` + labels + `},
	   "Health":{"Status":"starting"}},
	  {"Id":"healthy","State":"running",
	   "Labels":{` + labels + `},
	   "Health":{"Status":"healthy"}},
	  {"Id":"blip","State":"running",
	   "Labels":{` + labels + `},
	   "Health":{"Status":"starting"}}
	]`
	inspects := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: body},
		dockerRoute{method: http.MethodGet, path: "/containers/starting/json",
			body: `{"Id":"starting","State":` +
				`{"Status":"running","StartedAt":"2026-11-21T11:37:01.057756123Z"}}`,
			onMatch: func() { inspects++ }},
		dockerRoute{method: http.MethodGet, path: "/containers/blip/json",
			status: http.StatusInternalServerError, body: `{"message":"daemon blip"}`,
			onMatch: func() { inspects++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	list, err := m.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("ServiceContainerList: %v", err)
	}
	byID := map[string]ServiceContainer{}
	for _, ctr := range list {
		byID[ctr.ID] = ctr
	}

	// The create->start gap is visible and irrelevant: the start instant comes
	// from the inspect with its exact nanoseconds.
	starting := byID["starting"]
	if want := time.Unix(1600000000, 0); !starting.CreatedAt.Equal(want) {
		t.Errorf("starting created = %s, want %s", starting.CreatedAt, want)
	}
	wantStarted := time.Date(2026, 11, 21, 11, 37, 1, 57756123, time.UTC)
	if !starting.StartedAt.Equal(wantStarted) {
		t.Errorf("starting started = %v, want %v", starting.StartedAt, wantStarted)
	}

	// The healthy candidate needs no start instant, so it is never inspected —
	// the absence of its scripted route is what fails loudly if it were.
	if healthy := byID["healthy"]; !healthy.StartedAt.IsZero() {
		t.Errorf("healthy started = %v, want zero (no inspect for a converged container)",
			healthy.StartedAt)
	}

	// A failed inspect keeps discovery authoritative: the container is listed,
	// its start instant stays zero, and the caller treats the window as
	// unprovable instead of failing.
	blip := byID["blip"]
	if blip.ID == "" {
		t.Fatal("blip missing from the list after an inspect failure")
	}
	if !blip.StartedAt.IsZero() {
		t.Errorf("blip started = %v, want zero after a failed inspect", blip.StartedAt)
	}
	if inspects != 2 {
		t.Errorf("inspect calls = %d, want 2 (starting and blip only)", inspects)
	}
}

// The next retry generation is the highest durable attempt among the failed
// healthcheck candidates, so a restart never resets the bounded backoff, and a
// container without a probe never contributes one.
func TestServiceHealthAttemptNeverLowersAcrossRestart(t *testing.T) {
	if got := ServiceHealthAttempt(nil); got != 1 {
		t.Fatalf("empty candidates attempt = %d, want 1", got)
	}
	candidates := []ServiceContainer{
		{ID: "a", Healthcheck: "abc", HealthcheckAttempt: 3},
		{ID: "b", Healthcheck: "abc", HealthcheckAttempt: 0},
		{ID: "c", Healthcheck: "abc", HealthcheckAttempt: 5},
		{ID: "d", HealthcheckAttempt: 9},
	}
	if got := ServiceHealthAttempt(candidates); got != 6 {
		t.Fatalf("attempt = %d, want 6 (highest probe attempt + 1)", got)
	}
}
