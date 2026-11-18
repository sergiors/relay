package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
)

// logRecordsForMsg parses captured JSON log output and returns the raw
// attribute map of every record carrying the exact message, failing the test
// on any line that is not a structured record.
func logRecordsForMsg(t *testing.T, raw, msg string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid log line %q: %v", line, err)
		}
		if record["msg"] == msg {
			records = append(records, record)
		}
	}
	return records
}

// startServiceRoutes scripts the daemon calls one successful StartService makes
// for the given image: the volume guard inspect, a create returning a fresh id,
// the start, and the running-state confirm.
func startServiceRoutes(image string, seq *int) []dockerRoute {
	return []dockerRoute{
		imageWithoutDeclaredVolumesRoute(image),
		dockerRoute{method: http.MethodPost, path: "/containers/create",
			serve: func(*http.Request) (int, string) {
				*seq++
				return http.StatusOK, fmt.Sprintf(`{"Id":"cid-%d"}`, *seq)
			}},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/",
			body: `{"State":{"Running":true}}`},
	}
}

// TestServiceStartedLogCarriesTheServiceImage pins the per-container start line
// to the image that container actually runs: each service reports its OWN
// resolved image — an external reference, or the app image behind an entrypoint
// service — so one service's image can never be read as another's.
func TestServiceStartedLogCarriesTheServiceImage(t *testing.T) {
	for _, tc := range []struct {
		name  string
		specs []ServiceSpec
	}{
		{
			name: "one external service",
			specs: []ServiceSpec{{
				App: "fn", Name: "api", SourceRef: "ghcr.io/acme/api:1.2",
				Port: 8080, Image: "ghcr.io/acme/api:1.2", Env: []string{"PORT=8080"},
			}},
		},
		{
			name: "two external services with different images",
			specs: []ServiceSpec{
				{
					App: "fn", Name: "api", SourceRef: "ghcr.io/acme/api:1.2",
					Port: 8080, Image: "ghcr.io/acme/api:1.2", Env: []string{"PORT=8080"},
				},
				{
					App: "fn", Name: "worker", SourceRef: "ghcr.io/acme/worker:2.0",
					Port: 9090, Image: "ghcr.io/acme/worker:2.0", Env: []string{"PORT=9090"},
				},
			},
		},
		{
			name: "entrypoint service beside an external service",
			specs: []ServiceSpec{
				{
					App: "fn", Name: "web", SourceRef: "service.js",
					Port: 8080, Image: "relay-app-fn:v1",
					Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=8080"},
				},
				{
					App: "fn", Name: "api", SourceRef: "ghcr.io/acme/api:1.2",
					Port: 8081, Image: "ghcr.io/acme/api:1.2", Env: []string{"PORT=8081"},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			seq := 0
			routes := make([]dockerRoute, 0, len(tc.specs)*4)
			seen := map[string]bool{}
			for _, spec := range tc.specs {
				if seen[spec.Image] {
					continue
				}
				seen[spec.Image] = true
				routes = append(routes, startServiceRoutes(spec.Image, &seq)...)
			}
			m := &Manager{
				cli:    newScriptedDockerClient(t, routes...),
				log:    slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
				nodeID: "h",
			}

			// when each service replica starts
			for replica, spec := range tc.specs {
				if _, err := m.StartService(context.Background(), spec, replica); err != nil {
					t.Fatalf("StartService(%s): %v", spec.Name, err)
				}
			}

			// then exactly one start line per replica carries that replica's
			// own image, never an empty or app-level stand-in
			records := logRecordsForMsg(t, logs.String(), "Service: started")
			if len(records) != len(tc.specs) {
				t.Fatalf("Service: started records = %d, want %d: %+v", len(records), len(tc.specs), records)
			}
			for replica, spec := range tc.specs {
				var record map[string]any
				for _, candidate := range records {
					if candidate["service"] == spec.Name && candidate["replica"] == float64(replica) {
						record = candidate
						break
					}
				}
				if record == nil {
					t.Fatalf("no Service: started record for %s replica %d: %+v", spec.Name, replica, records)
				}
				image, ok := record["image"].(string)
				if !ok || image == "" {
					t.Errorf("%s replica %d image = %v, want the resolved image reference", spec.Name, replica, record["image"])
				} else if image != spec.Image {
					t.Errorf("%s replica %d image = %q, want %q", spec.Name, replica, image, spec.Image)
				}
				if record["app"] != spec.App || record["outcome"] != "started" {
					t.Errorf("%s replica %d attrs = %+v, want app=%s outcome=started", spec.Name, replica, record, spec.App)
				}
			}
		})
	}
}

// TestServiceRestartedLogIdentityComesFromContainerLabels pins that the
// in-place restart line identifies the container from the ownership labels the
// restart inspect already carries, and that a container stamped before those
// labels existed omits the identity rather than logging empty values.
func TestServiceRestartedLogIdentityComesFromContainerLabels(t *testing.T) {
	t.Run("labelled container", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodPost, path: "/containers/cid-1/restart", body: `{}`},
			dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
				body: `{"Id":"cid-1","State":{"Running":true},"Config":{"Labels":{` +
					`"relay.app":"fn","relay.service":"api",` +
					`"relay.image":"ghcr.io/acme/api:1.2","relay.replica":"1"}}}`},
		)
		var logs bytes.Buffer
		m := &Manager{
			cli:    cli,
			log:    slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			nodeID: "h",
		}
		if err := m.RestartServiceContainer(context.Background(), "cid-1"); err != nil {
			t.Fatalf("RestartServiceContainer: %v", err)
		}

		records := logRecordsForMsg(t, logs.String(), "Service: restarted")
		if len(records) != 1 {
			t.Fatalf("Service: restarted records = %d, want 1: %+v", len(records), records)
		}
		record := records[0]
		if record["image"] != "ghcr.io/acme/api:1.2" || record["service"] != "api" ||
			record["app"] != "fn" || record["replica"] != float64(1) {
			t.Errorf("restart identity = %+v, want the container's own app/service/image/replica", record)
		}
		if record["container"] != "cid-1" || record["outcome"] != "restarted" {
			t.Errorf("restart attrs = %+v, want container=cid-1 outcome=restarted", record)
		}
	})

	t.Run("container without ownership labels", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodPost, path: "/containers/cid-1/restart", body: `{}`},
			dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
				body: `{"Id":"cid-1","State":{"Running":true}}`},
		)
		var logs bytes.Buffer
		m := &Manager{
			cli:    cli,
			log:    slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			nodeID: "h",
		}
		if err := m.RestartServiceContainer(context.Background(), "cid-1"); err != nil {
			t.Fatalf("RestartServiceContainer: %v", err)
		}

		records := logRecordsForMsg(t, logs.String(), "Service: restarted")
		if len(records) != 1 {
			t.Fatalf("Service: restarted records = %d, want 1: %+v", len(records), records)
		}
		record := records[0]
		for _, key := range []string{"image", "app", "service", "replica"} {
			if value, ok := record[key]; ok {
				t.Errorf("unlabelled restart carries %s=%v; unknown identity must be omitted, not empty", key, value)
			}
		}
		if record["container"] != "cid-1" || record["outcome"] != "restarted" {
			t.Errorf("restart attrs = %+v, want container=cid-1 outcome=restarted", record)
		}
	})
}

// TestServiceStopFailureLogsCarryContainerIdentity pins the per-container
// failure warnings to the container that failed: the warning carries that
// replica's app, service, image, and replica index, so a stop or removal
// failure can be tied back to the exact service without an extra lookup.
func TestServiceStopFailureLogsCarryContainerIdentity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state container.ContainerState
		route dockerRoute
		msg   string
	}{
		{
			name:  "stop failure",
			state: container.StateRunning,
			route: dockerRoute{
				method: http.MethodPost, path: "/containers/cid-1/stop",
				status: http.StatusInternalServerError,
				body:   `{"message":"stop refused"}`,
			},
			msg: "Service: stop container failed",
		},
		{
			name:  "remove failure",
			state: container.StateExited,
			route: dockerRoute{
				method: http.MethodDelete, path: "/containers/cid-1",
				status: http.StatusInternalServerError,
				body:   `{"message":"remove refused"}`,
			},
			msg: "Service: remove container failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			m := &Manager{
				cli: newScriptedDockerClient(t, tc.route),
				log: slog.New(slog.NewJSONHandler(&logs,
					&slog.HandlerOptions{Level: slog.LevelDebug})),
			}
			failing := ServiceContainer{
				ID: "cid-1", App: "fn", Name: "api",
				Image: "ghcr.io/acme/api:1.2", Replica: 1, State: tc.state,
			}

			// when stopping the replica fails at the daemon
			if err := m.StopServiceContainers(
				context.Background(), []ServiceContainer{failing},
			); err == nil {
				t.Fatal("StopServiceContainers unexpectedly succeeded")
			}

			// then exactly one warning identifies the failing replica
			records := logRecordsForMsg(t, logs.String(), tc.msg)
			if len(records) != 1 {
				t.Fatalf("%s records = %d, want 1: %+v",
					tc.msg, len(records), records)
			}
			record := records[0]
			if record["level"] != "WARN" {
				t.Errorf("%s level = %v, want WARN", tc.msg, record["level"])
			}
			if record["app"] != "fn" || record["service"] != "api" ||
				record["replica"] != float64(1) ||
				record["image"] != "ghcr.io/acme/api:1.2" {
				t.Errorf("%s identity = %+v, want the replica's app/service/replica/image",
					tc.msg, record)
			}
			if record["container"] != "cid-1" {
				t.Errorf("%s container = %v, want cid-1",
					tc.msg, record["container"])
			}
		})
	}
}
