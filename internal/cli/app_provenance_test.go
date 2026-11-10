// This file holds the default-suite unit coverage for the `app inspect`
// provenance and fingerprint presentation: lookupAppProvenance is exercised
// through the injected ImageProvenanceLookup seam with a transport-free fake,
// and the renderer through plain detail structs, so no test here needs
// PostgreSQL or Docker. The command-level wiring lives in
// app_provenance_integration_test.go.

package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/state"
)

// provenanceTestDetail returns the active-generation detail shape an inspect
// provenance row is rendered for: a ready app with a persisted image and
// fingerprint.
func provenanceTestDetail() state.Detail {
	return state.Detail{
		Row: state.Row{
			Name:    "user-events-python",
			Runtime: "python3.14",
			Status:  state.StatusReady,
		},
		Image:       "relay-app-user-events-python",
		Fingerprint: "abc123hash",
	}
}

// renderInspectWithProvenance renders the detail with the given provenance and
// returns whitespace-normalized output, mirroring the render step of appInspect
// for renderer-level assertions.
func renderInspectWithProvenance(
	t *testing.T, detail state.Detail, provenance appProvenance,
) string {
	t.Helper()
	var w bytes.Buffer
	if err := renderAppDetailWithProvenance(
		&w, detail, state.AppStats{}, provenance,
	); err != nil {
		t.Fatalf("render app detail: %v", err)
	}
	return normWS(w.String())
}

// A configured lookup returning a pinned build renders the exact base
// reference source@digest after Runtime, and the lookup receives the app's
// persisted active image rather than any configured runtime selection.
func TestLookupAppProvenancePinnedSourceAndDigest(t *testing.T) {
	// given
	var gotImage string
	deps := Dependencies{
		ReadImageProvenance: func(
			_ context.Context, image string,
		) (runtime.ImageProvenance, error) {
			gotImage = image
			return runtime.ImageProvenance{
				RuntimeName: "python3.14",
				SourceImage: "python:3.14-slim",
				Digest:      "sha256:0123abcd",
				Platform:    "linux/amd64",
			}, nil
		},
	}

	// when
	got := lookupAppProvenance(
		context.Background(), "relay-app-user-events-python", deps,
	)

	// then
	if !got.available {
		t.Fatalf("provenance = %+v, want available", got)
	}
	if display := got.display(); display != "python:3.14-slim@sha256:0123abcd" {
		t.Fatalf("display() = %q, want the pinned source@digest reference", display)
	}
	if gotImage != "relay-app-user-events-python" {
		t.Fatalf("lookup received image %q, want the active image", gotImage)
	}
}

// An unpinned build has no digest: the base reference is the source alone.
func TestLookupAppProvenanceUnpinnedSource(t *testing.T) {
	// given
	deps := Dependencies{
		ReadImageProvenance: func(
			context.Context, string,
		) (runtime.ImageProvenance, error) {
			return runtime.ImageProvenance{
				RuntimeName: "nodejs24.x",
				SourceImage: "node:24-alpine",
			}, nil
		},
	}

	// when
	got := lookupAppProvenance(context.Background(), "relay-app-welcome", deps)

	// then
	if !got.available {
		t.Fatalf("provenance = %+v, want available", got)
	}
	if display := got.display(); display != "node:24-alpine" {
		t.Fatalf("display() = %q, want the source reference alone", display)
	}
}

// A failed lookup is reported as unavailable provenance with a fixed reason,
// and never falls back to the runtime the app is currently configured with:
// even a lookup that returns runtime metadata alongside its error must not
// surface that runtime as the base image.
func TestLookupAppProvenanceErrorReportsUnavailable(t *testing.T) {
	// given
	deps := Dependencies{
		ReadImageProvenance: func(
			context.Context, string,
		) (runtime.ImageProvenance, error) {
			return runtime.ImageProvenance{
					RuntimeName: "python3.14",
					SourceImage: "python:3.14-slim",
				},
				errors.New("docker daemon unreachable")
		},
	}

	// when
	got := lookupAppProvenance(context.Background(), "relay-app-x", deps)

	// then
	if got.available {
		t.Fatalf("provenance = %+v, want unavailable after a lookup error", got)
	}
	want := "unavailable (active image could not be inspected)"
	if display := got.display(); display != want {
		t.Fatalf("display() = %q, want %q", display, want)
	}
	if strings.Contains(got.display(), "python3.14") ||
		strings.Contains(got.display(), "python:3.14-slim") {
		t.Fatalf("display() = %q, must not fall back to configured runtime data",
			got.display())
	}
}

// With no configured lookup the provenance is unavailable and says so, instead
// of failing inspect or inventing a reference.
func TestLookupAppProvenanceWithoutConfiguredLookup(t *testing.T) {
	// given
	deps := Dependencies{}

	// when
	got := lookupAppProvenance(context.Background(), "relay-app-x", deps)

	// then
	if got.available {
		t.Fatalf("provenance = %+v, want unavailable without a lookup", got)
	}
	want := "unavailable (image inspection not configured)"
	if display := got.display(); display != want {
		t.Fatalf("display() = %q, want %q", display, want)
	}
}

// An image that predates the provenance labels yields an empty reference, so
// the missing provenance is reported as its own reason rather than as an
// inspection failure.
func TestLookupAppProvenanceMissingProvenance(t *testing.T) {
	// given
	deps := Dependencies{
		ReadImageProvenance: func(
			context.Context, string,
		) (runtime.ImageProvenance, error) {
			return runtime.ImageProvenance{}, nil
		},
	}

	// when
	got := lookupAppProvenance(context.Background(), "relay-app-x", deps)

	// then
	if got.available {
		t.Fatalf("provenance = %+v, want unavailable without recorded labels", got)
	}
	want := "unavailable (active image has no recorded provenance)"
	if display := got.display(); display != want {
		t.Fatalf("display() = %q, want %q", display, want)
	}
}

// The single image inspect is bounded: the lookup runs under a deadline, so an
// unresponsive Docker daemon degrades into unavailable provenance instead of
// stalling the command.
func TestLookupAppProvenanceBoundsInspectContext(t *testing.T) {
	// given
	var deadline time.Time
	deps := Dependencies{
		ReadImageProvenance: func(
			ctx context.Context, _ string,
		) (runtime.ImageProvenance, error) {
			deadline, _ = ctx.Deadline()
			return runtime.ImageProvenance{SourceImage: "python:3.14-slim"}, nil
		},
	}

	// when
	lookupAppProvenance(context.Background(), "relay-app-x", deps)

	// then
	if deadline.IsZero() {
		t.Fatal("lookup ran without a deadline; the image inspect must be bounded")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > appImageProvenanceTimeout {
		t.Fatalf("inspect deadline allows %v, want (0, %s]",
			remaining, appImageProvenanceTimeout)
	}
}

// The stored fingerprint stays unprefixed and is rendered with its hash
// algorithm; an already-prefixed value keeps exactly one prefix.
func TestDisplayFingerprintRendersSha256PrefixOnce(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fingerprint string
		want        string
	}{
		{"unprefixed value gains the prefix", "abc123hash", "sha256:abc123hash"},
		{"prefixed value is unchanged", "sha256:abc123hash", "sha256:abc123hash"},
		{"full hex digest gains the prefix",
			strings.Repeat("a", 64), "sha256:" + strings.Repeat("a", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// when
			got := displayFingerprint(tc.fingerprint)

			// then
			if got != tc.want {
				t.Fatalf("displayFingerprint(%q) = %q, want %q",
					tc.fingerprint, got, tc.want)
			}
			if strings.Contains(got, "sha256:sha256:") {
				t.Fatalf("displayFingerprint(%q) = %q, duplicate prefix",
					tc.fingerprint, got)
			}
		})
	}
}

// An app with an active image renders the resolved provenance in the Base
// image row directly after Runtime and before Status.
func TestAppDetailRendersBaseImageProvenance(t *testing.T) {
	// given
	provenance := appProvenance{
		reference: "python:3.14-slim@sha256:0123abcd",
		available: true,
	}

	// when
	out := renderInspectWithProvenance(t, provenanceTestDetail(), provenance)

	// then
	want := "Base image: python:3.14-slim@sha256:0123abcd"
	if !strings.Contains(out, want) {
		t.Fatalf("inspect output missing %q\n%s", want, out)
	}
	ri, bi, si := strings.Index(out, "Runtime:"),
		strings.Index(out, "Base image:"), strings.Index(out, "Status:")
	if ri == -1 || bi == -1 || si == -1 || !(ri < bi && bi < si) {
		t.Fatalf("Base image row must sit between Runtime and Status:\n%s", out)
	}
}

// Unavailable provenance renders its reason in the Base image row so the
// operator sees the lookup outcome instead of a blank or invented value.
func TestAppDetailRendersUnavailableBaseImage(t *testing.T) {
	// given
	provenance := appProvenance{reason: "active image could not be inspected"}

	// when
	out := renderInspectWithProvenance(t, provenanceTestDetail(), provenance)

	// then
	want := "Base image: unavailable (active image could not be inspected)"
	if !strings.Contains(out, want) {
		t.Fatalf("inspect output missing %q\n%s", want, out)
	}
}

// A pending app has no active image, so it renders no Base image row at all —
// even when provenance was supplied for it.
func TestAppDetailOmitsBaseImageWithoutActiveImage(t *testing.T) {
	// given
	detail := state.Detail{
		Row: state.Row{
			Name:    "welcome-email-node",
			Runtime: "nodejs24.x",
			Status:  state.StatusPreparing,
		},
	}
	provenance := appProvenance{reference: "node:24-alpine", available: true}

	// when
	out := renderInspectWithProvenance(t, detail, provenance)

	// then
	if strings.Contains(out, "Base image:") {
		t.Fatalf("pending app must omit the Base image row:\n%s", out)
	}
	if strings.Contains(out, "unavailable") {
		t.Fatalf("pending app must not report unavailable provenance:\n%s", out)
	}
}

// The fingerprint row carries the sha256: prefix for the stored unprefixed
// value, and exactly one prefix when the stored value is already prefixed.
func TestAppDetailRendersFingerprintWithSha256Prefix(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fingerprint string
		want        string
	}{
		{"unprefixed fingerprint", "abc123hash", "Fingerprint: sha256:abc123hash"},
		{"prefixed fingerprint",
			"sha256:abc123hash", "Fingerprint: sha256:abc123hash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			detail := provenanceTestDetail()
			detail.Fingerprint = tc.fingerprint

			// when
			out := renderInspectWithProvenance(t, detail, appProvenance{})

			// then
			if !strings.Contains(out, tc.want) {
				t.Fatalf("inspect output missing %q\n%s", tc.want, out)
			}
			if strings.Contains(out, "sha256:sha256:") {
				t.Fatalf("inspect output carries a duplicate prefix:\n%s", out)
			}
		})
	}
}
