package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"relay/internal/app"
)

// TestLiveAppNamesProtectsPresentInvalidAndNotReady pins the startup
// service-orphan sweep's present-app set: an app that loaded, an app whose
// template the loader rejected, and an app whose directory is present but not
// ready (missing template.yaml) are all apps on disk, so their service
// containers must be preserved. A name in neither the loaded set nor the
// diagnostics is a directory that vanished while Relay was down, so its
// containers are the ones the sweep may remove.
func TestLiveAppNamesProtectsPresentInvalidAndNotReady(t *testing.T) {
	apps := []app.App{{Name: "valid-a"}, {Name: "valid-b"}}
	issues := []app.LoadIssue{
		{Name: "invalid-template", Err: errors.New("parse template: unknown runtime")},
		{Name: "not-ready", Err: app.ErrNotReady},
	}

	got := liveAppNames(apps, issues)

	for _, name := range []string{
		"valid-a",
		"valid-b",
		"invalid-template",
		"not-ready",
	} {
		if !got[name] {
			t.Errorf("liveAppNames() omitted present app %q; a present app must be protected from the orphan sweep", name)
		}
	}
	if got["removed-while-down"] {
		t.Error("liveAppNames() protected an absent app; a genuinely removed directory must be sweepable")
	}
	if len(got) != 4 {
		t.Fatalf("liveAppNames() = %v, want exactly the four present names", got)
	}
}

// TestRetryIdentitySnapshotCleanupRetriesTransientFailures proves a startup
// sweep is re-driven after a transient list/removal error, and that each retry
// starts over: a FRESH candidate snapshot and a FRESH ownership proof, never the
// previous attempt's captured set.
func TestRetryIdentitySnapshotCleanupRetriesTransientFailures(t *testing.T) {
	transient := errors.New("transient docker failure")

	tests := []struct {
		name          string
		discoverFails int
		removeFails   int
		wantDiscover  int
		wantProof     int
		wantRemove    int
		wantResult    sweepDriveResult
	}{
		{
			name:          "discovery fails once",
			discoverFails: 1,
			wantDiscover:  2,
			wantProof:     1,
			wantRemove:    1,
			wantResult:    sweepDriveResult{attempts: 2, discovered: 1, removed: 1},
		},
		{
			name:         "removal fails once",
			removeFails:  1,
			wantDiscover: 2,
			wantProof:    2,
			wantRemove:   2,
			wantResult:   sweepDriveResult{attempts: 2, discovered: 2, removed: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf strings.Builder
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			var discoverCalls, proofCalls, removeCalls int
			var removedCandidates [][]string
			proof := ownershipProof{
				loss: &ownershipLoss{},
				verify: func(context.Context) error {
					proofCalls++
					return nil
				},
			}
			discover := func(context.Context) ([]string, error) {
				discoverCalls++
				if discoverCalls <= tc.discoverFails {
					return nil, transient
				}
				return []string{fmt.Sprintf("orphan-%d", discoverCalls)}, nil
			}
			remove := func(_ context.Context, candidates []string) (int, error) {
				removeCalls++
				removedCandidates = append(removedCandidates, append([]string(nil), candidates...))
				if removeCalls <= tc.removeFails {
					return 0, transient
				}
				return len(candidates), nil
			}

			result, err := retryIdentitySnapshotCleanup(
				context.Background(), proof, logger, "test-sweep", discover, remove,
			)
			if err != nil {
				t.Fatalf("retryIdentitySnapshotCleanup() = %v, want nil after a transient failure", err)
			}
			if result != tc.wantResult {
				t.Fatalf("result = %+v, want %+v", result, tc.wantResult)
			}
			if discoverCalls != tc.wantDiscover {
				t.Fatalf("discover calls = %d, want %d (every attempt takes a fresh snapshot)", discoverCalls, tc.wantDiscover)
			}
			if proofCalls != tc.wantProof {
				t.Fatalf("proof calls = %d, want %d (every attempt that captured candidates re-proves ownership)", proofCalls, tc.wantProof)
			}
			if removeCalls != tc.wantRemove {
				t.Fatalf("remove calls = %d, want %d", removeCalls, tc.wantRemove)
			}
			if tc.removeFails > 0 {
				if len(removedCandidates) != 2 {
					t.Fatalf("removal inputs = %v, want two captured snapshots", removedCandidates)
				}
				if len(removedCandidates[0]) != 1 || removedCandidates[0][0] != "orphan-1" {
					t.Fatalf("first removal input = %v, want [orphan-1]", removedCandidates[0])
				}
				if len(removedCandidates[1]) != 1 || removedCandidates[1][0] != "orphan-2" {
					t.Fatalf("retry removal input = %v, want only [orphan-2] (not the first snapshot)", removedCandidates[1])
				}
			}
			logs := buf.String()
			if !strings.Contains(logs, "retrying identity-scoped sweep") {
				t.Errorf("logs missing the retry record:\n%s", logs)
			}
			if !strings.Contains(logs, "transient docker failure") {
				t.Errorf("logs missing the transient error:\n%s", logs)
			}
		})
	}
}

// TestRetryIdentitySnapshotCleanupStopsAfterBoundedAttempts proves the retry is
// finite: a persistent list/removal failure is surfaced after the bounded
// number of attempts instead of looping, so startup cleanup stays best-effort.
func TestRetryIdentitySnapshotCleanupStopsAfterBoundedAttempts(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sentinel := errors.New("persistent docker failure")
	var discoverCalls, removeCalls int
	proof := ownershipProof{
		loss:   &ownershipLoss{},
		verify: func(context.Context) error { return nil },
	}
	discover := func(context.Context) ([]string, error) {
		discoverCalls++
		return []string{"orphan"}, nil
	}
	remove := func(context.Context, []string) (int, error) {
		removeCalls++
		return 0, sentinel
	}

	result, err := retryIdentitySnapshotCleanup(
		context.Background(), proof, logger, "test-sweep", discover, remove,
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("retryIdentitySnapshotCleanup() = %v, want the persistent error", err)
	}
	if result.attempts != startupSweepAttempts {
		t.Fatalf("attempts = %d, want the bound %d", result.attempts, startupSweepAttempts)
	}
	if discoverCalls != startupSweepAttempts {
		t.Fatalf("discover calls = %d, want the bound %d", discoverCalls, startupSweepAttempts)
	}
	if removeCalls != startupSweepAttempts {
		t.Fatalf("remove calls = %d, want the bound %d", removeCalls, startupSweepAttempts)
	}
	if got := strings.Count(buf.String(), "retrying identity-scoped sweep"); got != startupSweepAttempts-1 {
		t.Fatalf("retry log records = %d, want %d (a record for each pause, none after the last)", got, startupSweepAttempts-1)
	}
}

// TestRetryIdentitySnapshotCleanupSkipsRemovalOnFailedProof proves a failed or
// ambiguous ownership proof still fails closed under the retry wrapper: the
// freshly captured candidates are never removed, the loss is latched, and no
// further discovery happens (a replacement owner may already exist).
func TestRetryIdentitySnapshotCleanupSkipsRemovalOnFailedProof(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss:   loss,
		verify: func(context.Context) error { return errors.New("ambiguous ownership proof") },
		onLost: func() { loss.lose() },
	}
	var discoverCalls, removeCalls int
	discover := func(context.Context) ([]string, error) {
		discoverCalls++
		return []string{"orphan"}, nil
	}
	remove := func(context.Context, []string) (int, error) {
		removeCalls++
		return 1, nil
	}

	result, err := retryIdentitySnapshotCleanup(
		context.Background(), proof, logger, "test-sweep", discover, remove,
	)
	if err != nil {
		t.Fatalf("retryIdentitySnapshotCleanup() = %v, want nil (removal suppressed, not fatal)", err)
	}
	if removeCalls != 0 {
		t.Fatalf("remove calls = %d, want 0 after a failed ownership proof", removeCalls)
	}
	if discoverCalls != 1 {
		t.Fatalf("discover calls = %d, want 1 (no re-discovery after a latched loss)", discoverCalls)
	}
	if !loss.isLost() {
		t.Fatal("a failed ownership proof must latch the loss")
	}
	if result.attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (a latched loss stops the retry loop)", result.attempts)
	}
	if !strings.Contains(buf.String(), "failed ownership verification") {
		t.Errorf("logs missing the failed-proof record:\n%s", buf.String())
	}
}
