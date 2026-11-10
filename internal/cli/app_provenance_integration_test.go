//go:build integration

// This file holds the command-level provenance coverage for `relay app
// inspect`: the injected ReadImageProvenance seam is faked so no test contacts
// Docker, while the persisted detail row comes from the real state fixture.
// The renderer- and lookup-level unit coverage lives in
// app_provenance_test.go (default suite).

package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"relay/internal/runtime"
)

// `app inspect` reads the provenance of the app's persisted ACTIVE image
// through the injected seam — exactly once, with that image reference — and
// renders it as the Base image row without consulting any configured runtime.
func TestAppInspectReadsProvenanceFromActiveImage(t *testing.T) {
	// given
	_, deps := seedTestState(t)
	var gotImage string
	lookups := 0
	deps.ReadImageProvenance = func(
		_ context.Context, image string,
	) (runtime.ImageProvenance, error) {
		lookups++
		gotImage = image
		return runtime.ImageProvenance{
			RuntimeName: "python3.14",
			SourceImage: "python:3.14-slim",
			Digest:      "sha256:0123abcd",
			Platform:    "linux/amd64",
		}, nil
	}

	// when
	out, _, err := runCLIWithDeps(
		t, deps, "", "app", "inspect", "user-events-python")

	// then
	if err != nil {
		t.Fatalf("inspect err = %v, want nil", err)
	}
	if lookups != 1 {
		t.Fatalf("provenance lookups = %d, want exactly 1", lookups)
	}
	if gotImage != "relay-app-user-events-python" {
		t.Fatalf("lookup received image %q, want the active image",
			gotImage)
	}
	want := "Base image: python:3.14-slim@sha256:0123abcd"
	if !strings.Contains(normWS(out), want) {
		t.Fatalf("inspect output missing %q\n%s", want, out)
	}
	if strings.Contains(out, "Base image: python3.14") {
		t.Fatalf("must not fall back to the configured runtime:\n%s", out)
	}
}

// A failed provenance lookup is best-effort: inspect still exits 0, renders the
// rest of the record, reports the unavailable reason, and shows nothing from
// the failed lookup — no configured-runtime fallback.
func TestAppInspectProvenanceFailureKeepsCommandSuccessful(t *testing.T) {
	// given
	_, deps := seedTestState(t)
	deps.ReadImageProvenance = func(
		context.Context, string,
	) (runtime.ImageProvenance, error) {
		return runtime.ImageProvenance{
				RuntimeName: "python3.14",
				SourceImage: "python:3.14-slim",
			},
			errors.New("docker daemon unreachable")
	}

	// when
	out, _, err := runCLIWithDeps(
		t, deps, "", "app", "inspect", "user-events-python")

	// then
	if err != nil {
		t.Fatalf("inspect err = %v, want nil despite the lookup failure", err)
	}
	normed := normWS(out)
	for _, want := range []string{
		"Name: user-events-python",
		"Base image: unavailable (active image could not be inspected)",
	} {
		if !strings.Contains(normed, want) {
			t.Fatalf("inspect output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(normed, "Base image: python") {
		t.Fatalf("failed lookup must not render runtime or source data:\n%s", out)
	}
}

// An app with no active image never reaches the lookup seam: inspect skips the
// Docker-bound read entirely and omits the Base image row.
func TestAppInspectSkipsProvenanceLookupWithoutActiveImage(t *testing.T) {
	// given
	_, deps := seedTestState(t)
	lookups := 0
	deps.ReadImageProvenance = func(
		context.Context, string,
	) (runtime.ImageProvenance, error) {
		lookups++
		return runtime.ImageProvenance{}, nil
	}

	// when
	out, _, err := runCLIWithDeps(
		t, deps, "", "app", "inspect", "welcome-email-node")

	// then
	if err != nil {
		t.Fatalf("inspect err = %v, want nil", err)
	}
	if lookups != 0 {
		t.Fatalf("provenance lookups = %d, want 0 for an app without an active image",
			lookups)
	}
	if strings.Contains(out, "Base image:") {
		t.Fatalf("pending app must omit the Base image row:\n%s", out)
	}
}

// With no configured lookup the command still succeeds and reports why
// provenance is unavailable, instead of failing or rendering a blank row.
func TestAppInspectWithoutConfiguredLookupReportsUnavailable(t *testing.T) {
	// given: seedTestState's default dependencies carry no
	// ReadImageProvenance (Docker-backed lookup not configured).
	_, deps := seedTestState(t)
	deps.ReadImageProvenance = nil

	// when
	out, _, err := runCLIWithDeps(
		t, deps, "", "app", "inspect", "user-events-python")

	// then
	if err != nil {
		t.Fatalf("inspect err = %v, want nil without a configured lookup", err)
	}
	want := "Base image: unavailable (image inspection not configured)"
	if !strings.Contains(normWS(out), want) {
		t.Fatalf("inspect output missing %q\n%s", want, out)
	}
}
