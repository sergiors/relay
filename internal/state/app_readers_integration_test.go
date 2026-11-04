//go:build integration

package state

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// This file pins the STRICT app reader contract ReadApp/ReadAppSummaries
// expose to the CLI: an unknown app and an empty database are legitimate data
// outcomes with a nil error, while a failed read (query, scan, or decode
// error) is returned as an error — never collapsed into "unknown app" or "no
// apps".

// TestReadAppUnknownNameIsNotError pins the absence contract: an unknown name
// is found=false with a nil error and a zero Detail, so the CLI can report a
// genuine unknown app instead of masking a read failure the same way.
func TestReadAppUnknownNameIsNotError(t *testing.T) {
	c := openTestState(t)

	detail, found, err := c.ReadApp(context.Background(), "ghost")
	if err != nil {
		t.Fatalf("ReadApp for an unknown app: %v", err)
	}
	if found {
		t.Fatal("ReadApp for an unknown app must report found=false")
	}
	if detail.Name != "" {
		t.Fatalf("an absent row must not name an app, got %q", detail.Name)
	}
}

// TestReadAppFound pins the found contract: a seeded row returns found=true,
// the app's name, and its persisted detail.
func TestReadAppFound(t *testing.T) {
	c := openTestState(t)
	seedAppRow(t, c, "alpha")

	detail, found, err := c.ReadApp(context.Background(), "alpha")
	if err != nil {
		t.Fatalf("ReadApp: %v", err)
	}
	if !found {
		t.Fatal("ReadApp of a seeded app must report found=true")
	}
	if detail.Name != "alpha" {
		t.Fatalf("ReadApp detail Name = %q, want alpha", detail.Name)
	}
	if detail.Status != StatusPreparing {
		t.Fatalf("ReadApp detail Status = %q, want a freshly seeded app's preparing status", detail.Status)
	}
}

// TestReadAppCancelledContextFails pins the failed-read branch: a cancelled
// read is an error with found=false — never the unknown-app outcome, so a CLI
// cannot tell an operator their app is gone because the state view failed.
func TestReadAppCancelledContextFails(t *testing.T) {
	c := openTestState(t)
	seedAppRow(t, c, "alpha")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the read begins

	detail, found, err := c.ReadApp(ctx, "alpha")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadApp on a cancelled context: err = %v, want context.Canceled", err)
	}
	if found {
		t.Fatal("a failed read must not report found=true")
	}
	if detail.Name != "" {
		t.Fatalf("a failed read must return a zero Detail, got Name=%q", detail.Name)
	}
}

// TestReadAppSummariesEmptyDBIsEmptyNotError pins the strict listing's absence
// contract: an empty database is an empty listing with a nil error — never an
// error and never a fabricated result.
func TestReadAppSummariesEmptyDBIsEmptyNotError(t *testing.T) {
	c := openTestState(t)

	rows, err := c.ReadAppSummaries(context.Background())
	if err != nil {
		t.Fatalf("ReadAppSummaries on an empty database: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("ReadAppSummaries on an empty database = %+v, want no rows", rows)
	}
}

// TestReadAppSummariesReturnsAllRowsNameOrdered pins the success contract:
// every row is returned, ordered by name, regardless of insertion order.
func TestReadAppSummariesReturnsAllRowsNameOrdered(t *testing.T) {
	c := openTestState(t)
	seedAppRow(t, c, "beta")
	seedAppRow(t, c, "alpha")

	rows, err := c.ReadAppSummaries(context.Background())
	if err != nil {
		t.Fatalf("ReadAppSummaries: %v", err)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
	}
	if !slices.Equal(names, []string{"alpha", "beta"}) {
		t.Fatalf("ReadAppSummaries names = %v, want the complete set name-ordered", names)
	}
}

// TestReadAppSummariesCancelledContextFails pins the failed-read branch of the
// strict listing: an error is returned with no rows, so a failed read can
// never be presented to an operator as an empty-but-healthy listing.
func TestReadAppSummariesCancelledContextFails(t *testing.T) {
	c := openTestState(t)
	seedAppRow(t, c, "alpha")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the read begins

	rows, err := c.ReadAppSummaries(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAppSummaries on a cancelled context: err = %v, want context.Canceled", err)
	}
	if len(rows) != 0 {
		t.Fatalf("a failed read must return no rows, got %+v", rows)
	}
}
