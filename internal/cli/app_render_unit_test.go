// This file holds the default-suite unit coverage for the `app ls` UPDATED
// column and the `app inspect` generation/fingerprint rows rendered by
// app_render.go. Every test drives the renderers with plain state structs, so
// none of them needs PostgreSQL: coverage that requires a persisted lifecycle
// transition lives in app_test.go behind the integration tag.

package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"relay/internal/state"
)

// renderAppListOutput renders rows through the `app ls` table and returns the
// complete listing.
func renderAppListOutput(t *testing.T, rows ...state.Row) string {
	t.Helper()
	var w bytes.Buffer
	if err := renderAppList(&w, rows); err != nil {
		t.Fatalf("render app list: %v", err)
	}
	return w.String()
}

// appListRow returns the rendered table line whose first field is name.
func appListRow(t *testing.T, out, name string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == name {
			return line
		}
	}
	t.Fatalf("listing has no row for %q:\n%s", name, out)
	return ""
}

// renderInspectOutput renders a detail through the inspect renderer (no
// provenance, no persisted stats) and returns whitespace-normalized output.
func renderInspectOutput(t *testing.T, detail state.Detail) string {
	t.Helper()
	var w bytes.Buffer
	if err := renderAppDetail(&w, detail, state.AppStats{}); err != nil {
		t.Fatalf("render app detail: %v", err)
	}
	return normWS(w.String())
}

// assertNoActiveNaming fails when a rendered view resurrects the retired
// "Active ..." labels for the serving generation.
func assertNoActiveNaming(t *testing.T, view, out string) {
	t.Helper()
	if strings.Contains(out, "Active") || strings.Contains(out, "ACTIVE") {
		t.Errorf("%s output must not use Active naming:\n%s", view, out)
	}
}

// The UPDATED column reports the latest persisted state change (updated_at),
// not the older prepared_at of the serving generation: an app rediscovered or
// failed long after its last successful prepare must show the recent change.
// The ages sit far from their relative-bucket edges (2h within [1h,24h), 48h
// within [24h,72h)) so test-run latency cannot flip them.
func TestAppListUpdatedUsesLatestPersistedTimestamp(t *testing.T) {
	// given
	now := time.Now()
	row := state.Row{
		Name:       "user-events-python",
		Runtime:    "python3.14",
		Status:     state.StatusDegraded,
		PreparedAt: now.Add(-48 * time.Hour).UTC().Format(time.RFC3339),
		UpdatedAt:  now.Add(-2 * time.Hour).UTC().Format(time.RFC3339),
	}

	// when
	out := renderAppListOutput(t, row)

	// then
	got := appListRow(t, out, row.Name)
	if !strings.Contains(got, "2h ago") {
		t.Errorf("UPDATED row = %q, want the updated_at age %q", got, "2h ago")
	}
	if strings.Contains(got, "2d ago") {
		t.Errorf("UPDATED row = %q, must not fall back to the prepared_at age", got)
	}
}

// A row without relational updated_at metadata still renders a meaningful age
// from prepared_at rather than a blank cell.
func TestAppListUpdatedFallsBackToPreparedAt(t *testing.T) {
	// given
	row := state.Row{
		Name:       "legacy-app",
		Runtime:    "nodejs24.x",
		Status:     state.StatusReady,
		PreparedAt: time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339),
	}

	// when
	out := renderAppListOutput(t, row)

	// then
	got := appListRow(t, out, row.Name)
	if !strings.Contains(got, "3h ago") {
		t.Fatalf("UPDATED row = %q, want the prepared_at age %q", got, "3h ago")
	}
}

// A row with neither persisted timestamp renders the "-" placeholder: the
// column stays populated and aligned instead of collapsing to an empty cell.
func TestAppListUpdatedRendersPlaceholderWithoutTimestamps(t *testing.T) {
	// given
	row := state.Row{
		Name:    "timestampless-app",
		Runtime: "python3.14",
		Status:  state.StatusPreparing,
	}

	// when
	out := renderAppListOutput(t, row)

	// then
	fields := strings.Fields(appListRow(t, out, row.Name))
	if len(fields) != 4 {
		t.Fatalf("row fields = %v, want NAME RUNTIME STATUS UPDATED", fields)
	}
	if got := fields[3]; got != "-" {
		t.Fatalf("UPDATED cell = %q, want the %q placeholder", got, "-")
	}
}

// A rediscovered app whose desired generation differs from the published one
// shows BOTH fingerprints: the published generation stays visible and the
// pending desired value is called out after it.
func TestAppInspectShowsDesiredFingerprintWhenItDiffers(t *testing.T) {
	// given
	detail := state.Detail{
		Row: state.Row{
			Name:    "user-events-python",
			Runtime: "python3.14",
			Status:  state.StatusPreparing,
		},
		Image:              "relay-app-user-events-python",
		Fingerprint:        "published-fp",
		DesiredFingerprint: "desired-fp",
	}

	// when
	out := renderInspectOutput(t, detail)

	// then
	for _, want := range []string{
		"Status: preparing",
		"Fingerprint: sha256:published-fp",
		"Desired fingerprint: sha256:desired-fp",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	pi, di := strings.Index(out, "Fingerprint:"),
		strings.Index(out, "Desired fingerprint:")
	if pi == -1 || di == -1 || pi > di {
		t.Fatalf("published fingerprint row must precede the desired row:\n%s", out)
	}
	assertNoActiveNaming(t, "inspect", out)
}

// When the desired fingerprint identifies the same generation as the published
// one — including a prefix-variant spelling — inspect shows only the single
// published Fingerprint row; a legacy row with no persisted desired value is
// likewise rendered without an empty desired row.
func TestAppInspectOmitsDesiredFingerprintWhenItMatchesPublished(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published string
		desired   string
	}{
		{"identical stored values", "abc123hash", "abc123hash"},
		{"same value with mixed prefix spelling",
			"sha256:abc123hash", "abc123hash"},
		{"published generation without a persisted desired value",
			"abc123hash", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			detail := state.Detail{
				Row: state.Row{
					Name:    "user-events-python",
					Runtime: "python3.14",
					Status:  state.StatusReady,
				},
				Image:              "relay-app-user-events-python",
				Fingerprint:        tc.published,
				DesiredFingerprint: tc.desired,
			}

			// when
			out := renderInspectOutput(t, detail)

			// then
			want := "Fingerprint: sha256:abc123hash"
			if !strings.Contains(out, want) {
				t.Errorf("inspect output missing %q\n%s", want, out)
			}
			if strings.Contains(out, "Desired fingerprint:") {
				t.Errorf("matching or absent desired fingerprint must be omitted:\n%s",
					out)
			}
			assertNoActiveNaming(t, "inspect", out)
		})
	}
}

// Initial discovery persists only a desired fingerprint: inspect renders it so
// the operator can see the pending generation, without inventing an empty
// published Fingerprint row.
func TestAppInspectShowsInitialDesiredFingerprint(t *testing.T) {
	// given
	detail := state.Detail{
		Row: state.Row{
			Name:    "welcome-email-node",
			Runtime: "nodejs24.x",
			Status:  state.StatusPreparing,
		},
		DesiredFingerprint: "first-desired-fp",
	}

	// when
	out := renderInspectOutput(t, detail)

	// then
	want := "Desired fingerprint: sha256:first-desired-fp"
	if !strings.Contains(out, want) {
		t.Errorf("inspect output missing %q\n%s", want, out)
	}
	// The published row is labelled "Fingerprint:" (capital F); the desired
	// row alone must not make a published generation look present.
	if strings.Contains(out, "Fingerprint:") {
		t.Errorf("inspect must not render a published Fingerprint row:\n%s", out)
	}
	assertNoActiveNaming(t, "inspect", out)
}

// After a new desired generation fails, the detail still describes the last
// serving generation (image, fingerprint, prepared instant) while the failure
// surfaces through Last reconcile and Last error, the failed desired
// fingerprint stays visible as a differing row, and the resource row renders
// unchanged.
func TestAppInspectFailedNewDeploymentKeepsPublishedGeneration(t *testing.T) {
	// given
	preparedAt := time.Now().Add(-4 * time.Hour).UTC().Format(time.RFC3339)
	detail := state.Detail{
		Row: state.Row{
			Name:                "user-events-python",
			Runtime:             "python3.14",
			Status:              state.StatusDegraded,
			PreparedAt:          preparedAt,
			LastReconcileStatus: state.ReconcileFailed,
		},
		Image:              "relay-app-user-events-python",
		Fingerprint:        "published-fp",
		DesiredFingerprint: "failed-desired-fp",
		LastReconcileAt: time.Now().Add(-5 * time.Minute).
			UTC().Format(time.RFC3339),
		LastError: "build failed: base image pull refused",
		Resources: &state.Resources{
			MemoryBytes: 256 << 20,
			CPUs:        0.5,
			Pids:        64,
		},
	}

	// when
	out := renderInspectOutput(t, detail)

	// then
	for _, want := range []string{
		"Status: degraded",
		"Image: relay-app-user-events-python",
		"Fingerprint: sha256:published-fp",
		"Desired fingerprint: sha256:failed-desired-fp",
		"Prepared: " + preparedAt,
		"Last reconcile: failed (5m ago)",
		"Last error: build failed: base image pull refused",
		"Resources: memory=256MiB cpus=0.5 pids=64",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	assertNoActiveNaming(t, "inspect", out)
}

// A detail with every optional field absent still renders the compatible
// overview and zero Stats block: no blank fingerprint/timestamp rows, no
// optional section headers, and no retired "Active" naming.
func TestAppInspectIncompleteDetailRendersCompatibleOutput(t *testing.T) {
	// given
	detail := state.Detail{
		Row: state.Row{Name: "bare-app", Runtime: "", Status: state.StatusPreparing},
	}

	// when
	out := renderInspectOutput(t, detail)

	// then
	for _, want := range []string{
		"Name: bare-app",
		"Runtime: -",
		"Status: preparing",
		"Stats:",
		"Events matched: 0",
		"Last execution: never",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	for _, absent := range []string{
		"Base image:", "Image:", "Fingerprint:", "Desired fingerprint:",
		"Prepared:", "Last reconcile:", "Last error:", "Resources:",
		"Events:", "Schedules:", "Services:", "Environment:", "Secrets:",
	} {
		if strings.Contains(out, absent) {
			t.Errorf("inspect output must omit %q for absent fields:\n%s",
				absent, out)
		}
	}
	assertNoActiveNaming(t, "inspect", out)
}

// The Environment section renders the fixed redaction marker for every name:
// even a snapshot that (incorrectly) carries a literal value never prints it.
func TestAppInspectRedactsEnvValuesFromSnapshot(t *testing.T) {
	// given
	detail := state.Detail{
		Row: state.Row{
			Name:    "env-app",
			Runtime: "python3.14",
			Status:  state.StatusReady,
		},
		Env: map[string]string{
			"API_URL": "https://api.example.com/secret-path",
			"MODE":    state.RedactedEnvValue,
		},
	}

	// when
	out := renderInspectOutput(t, detail)

	// then
	for _, want := range []string{
		"Environment:",
		"API_URL=" + state.RedactedEnvValue,
		"MODE=" + state.RedactedEnvValue,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "https://api.example.com") {
		t.Errorf("inspect leaked a literal env value:\n%s", out)
	}
}
