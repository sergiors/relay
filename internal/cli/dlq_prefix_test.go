package cli

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/dlq"
)

// This file pins the short-ID prefix contract of the `relay dlq` admin
// commands: `dlq ls` shows only the first dlqIDShortLength characters, and
// inspect/replay/rm accept that visible prefix (or any longer unique prefix)
// by resolving it BEFORE any store side effect — a zero-, two-, byte-
// unverified, or non-hex match is refused before a claim, execution, or delete
// can happen. The exact operations (and the replay/fencing/unsettled semantics
// around them) are unchanged and covered by the other DLQ test files; the
// resolution race is covered once here against a resolvable ID that vanishes
// before the exact operation runs.

// spyDLQPrefixFinder wraps the in-memory fake with an injectable prefix
// resolver: it records every lookup (so tests can prove validation happens
// before any lookup and that full IDs bypass prefix resolution) and answers
// either dynamically or with a canned result. With padInput set it returns a
// well-formed full ID beginning with the EXACT requested prefix (the input
// zero-padded to 64 characters) — the byte-verified answer the production
// guard accepts; otherwise it returns the canned ids/err (so tests can point
// at a deliberately syntactic-but-non-matching answer too). The embedding
// preserves every other dlq.Store behavior of fakeDLQStore, so resolveDLQID's
// type assertion resolves to the override here, not to the fake's own method.
type spyDLQPrefixFinder struct {
	*fakeDLQStore

	mu         sync.Mutex
	calls      int
	lastPrefix string
	padInput   bool
	ids        []string
	err        error
}

func (f *spyDLQPrefixFinder) FindDLQEntryIDsByPrefix(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	f.calls++
	f.lastPrefix = prefix
	pad := f.padInput
	ids, err := f.ids, f.err
	f.mu.Unlock()
	if pad {
		return []string{dlqHexID(prefix)}, err
	}
	return ids, err
}

func (f *spyDLQPrefixFinder) lookupState() (int, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.lastPrefix
}

// noFinderDLQStore is a minimal dlq.Store WITHOUT the administrative prefix
// capability: it pins that implementations can stay narrow (the stream
// processor's seam) — full-ID operations must keep working through it and a
// prefix must be refused with the explicit unsupported error. Only GetDLQEntry
// is reachable: resolution never calls anything else.
type noFinderDLQStore struct {
	entries []dlq.Entry
}

func (s *noFinderDLQStore) GetDLQEntry(_ context.Context, id string) (dlq.Entry, bool, error) {
	for _, e := range s.entries {
		if e.ID == id {
			return e, true, nil
		}
	}
	return dlq.Entry{}, false, nil
}

func (s *noFinderDLQStore) SaveDLQEntry(context.Context, dlq.Entry) (bool, error) {
	panic("unexpected SaveDLQEntry on the no-finder store")
}

func (s *noFinderDLQStore) MarkDLQDeliverySettled(context.Context, string, time.Time) error {
	panic("unexpected MarkDLQDeliverySettled on the no-finder store")
}

func (s *noFinderDLQStore) ListUnsettledDLQDeliveries(context.Context, string, int) ([]dlq.DeliveryRef, error) {
	panic("unexpected ListUnsettledDLQDeliveries on the no-finder store")
}

func (s *noFinderDLQStore) ListDLQEntries(context.Context) ([]dlq.Entry, error) {
	panic("unexpected ListDLQEntries on the no-finder store")
}

func (s *noFinderDLQStore) DeleteDLQEntry(context.Context, string) (bool, error) {
	panic("unexpected DeleteDLQEntry on the no-finder store")
}

func (s *noFinderDLQStore) ClaimDLQReplay(context.Context, string, time.Duration) (dlq.ReplayClaim, error) {
	panic("unexpected ClaimDLQReplay on the no-finder store")
}

func (s *noFinderDLQStore) ReleaseDLQReplay(context.Context, string, int64) error {
	panic("unexpected ReleaseDLQReplay on the no-finder store")
}

func (s *noFinderDLQStore) DeleteDLQReplay(context.Context, string, int64) error {
	panic("unexpected DeleteDLQReplay on the no-finder store")
}

// TestDLQResolveIDGrammar pins the entry-identifier grammar the admin commands
// accept: 10 to 64 lowercase hexadecimal characters. Malformed input is
// rejected with a clear error BEFORE any prefix lookup (the finder is never
// called), a full-length ID passes through untouched, and a valid prefix is
// looked up by the exact characters given — resolving to a full ID that
// byte-starts with that input, which the production byte-verification guard
// then accepts.
func TestDLQResolveIDGrammar(t *testing.T) {
	spy := &spyDLQPrefixFinder{fakeDLQStore: &fakeDLQStore{}, padInput: true}
	resolve := func(input string) (string, error) {
		return resolveDLQID(context.Background(), spy, input)
	}

	invalid := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", "invalid DLQ entry ID"},
		{"one character", "d", "invalid DLQ entry ID"},
		{"nine characters", "cafebabef", "invalid DLQ entry ID"},
		{"sixty-five characters", dlqUnitID + "0", "invalid DLQ entry ID"},
		{"old stream-style ID", "1-0cafebab", "only lowercase hexadecimal"},
		{"uppercase", "DEADBEEF01", "only lowercase hexadecimal"},
		{"mixed case", "dEadbEEf01", "only lowercase hexadecimal"},
		{"percent wildcard", "cafebabef%", "only lowercase hexadecimal"},
		{"underscore wildcard", "cafebabef_", "only lowercase hexadecimal"},
		{"star wildcard", "cafebabef*", "only lowercase hexadecimal"},
		{"punctuation", "cafe.babef", "only lowercase hexadecimal"},
		{"space", "cafe babef", "only lowercase hexadecimal"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolve(tc.input)
			if err == nil {
				t.Fatalf("resolveDLQID(%q) = %q, want an invalid-ID error", tc.input, got)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
			if got != "" {
				t.Fatalf("a rejected input must not resolve to a target ID, got %q", got)
			}
		})
	}
	if calls, _ := spy.lookupState(); calls != 0 {
		t.Fatalf("a malformed ID must be rejected before any prefix lookup: calls = %d", calls)
	}

	// Canonical inputs: any length from the visible ten characters to the full
	// ID is accepted. Only the prefix-length inputs may consult the finder,
	// and they must pass the user's characters through unmodified (the lookup
	// is by the literal prefix, never transformed).
	prefixes := []string{
		dlqSharedPrefix,         // the ten-character `dlq ls` prefix
		dlqSharedPrefix + "1",   // between the minimum and the full length
		strings.Repeat("a", 63), // one short of full
		dlqUnitID,               // full length
	}
	for _, input := range prefixes {
		got, err := resolve(input)
		if err != nil {
			t.Fatalf("resolveDLQID(%q): %v, want a canonical-ID resolution", input, err)
		}
		if want := dlqHexID(input); got != want {
			t.Fatalf("resolveDLQID(%q) = %q, want the finder's padded full ID %q beginning with the exact input", input, got, want)
		}
	}
	// Only the prefix-length inputs resolve through the finder; the full-length
	// ID bypasses prefix resolution entirely.
	calls, last := spy.lookupState()
	if calls != len(prefixes)-1 {
		t.Fatalf("finder calls = %d, want one per prefix input (%d): the full ID must bypass prefix resolution",
			calls, len(prefixes)-1)
	}
	if last != strings.Repeat("a", 63) {
		t.Fatalf("last lookup = %q, want the final PREFIX input looked up verbatim %q"+
			" (the full-length ID must bypass prefix resolution)", last, strings.Repeat("a", 63))
	}
}

// TestDLQResolveIDPrefixMapping pins the 0/1/2+ behavior of an injected
// resolver: no match and ambiguity are refused with the offending prefix named
// (and the ambiguity naming the two competing IDs), one match resolves to that
// exact full ID, and a lookup failure surfaces unmasked.
func TestDLQResolveIDPrefixMapping(t *testing.T) {
	// input is the exactly ten-character prefix as an operator would copy it from
	// `dlq ls`; every subtest resolves through the same shared prefix.
	input := dlqSharedPrefix

	t.Run("no match", func(t *testing.T) {
		spy := &spyDLQPrefixFinder{fakeDLQStore: &fakeDLQStore{}}
		_, err := resolveDLQID(context.Background(), spy, input)
		if got := fmt.Sprintf("no DLQ entry matches ID prefix %q", input); err == nil || !strings.Contains(err.Error(), got) {
			t.Fatalf("err = %v, want the no-match error %q", err, got)
		}
	})

	t.Run("unique match", func(t *testing.T) {
		spy := &spyDLQPrefixFinder{fakeDLQStore: &fakeDLQStore{}, ids: []string{dlqSharedIDOne}}
		got, err := resolveDLQID(context.Background(), spy, input)
		if err != nil || got != dlqSharedIDOne {
			t.Fatalf("resolveDLQID = (%q, %v), want the unique match %q", got, err, dlqSharedIDOne)
		}
	})

	t.Run("ambiguous — two", func(t *testing.T) {
		spy := &spyDLQPrefixFinder{fakeDLQStore: &fakeDLQStore{}, ids: []string{dlqSharedIDOne, dlqSharedIDTwo}}
		_, err := resolveDLQID(context.Background(), spy, input)
		if err == nil {
			t.Fatal("a two-match prefix must be refused as ambiguous")
		}
		for _, want := range []string{
			"ambiguous DLQ entry ID prefix",
			fmt.Sprintf("%q: matches %s and %s; use more characters", input, dlqSharedIDOne, dlqSharedIDTwo),
			"use more characters",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want it to contain %q", err, want)
			}
		}
	})

	t.Run("ambiguous — three matches still name exactly two", func(t *testing.T) {
		// The store contract returns at most two IDs, so even a wider match
		// set is reported as the first two in ascending identity order.
		spy := &spyDLQPrefixFinder{fakeDLQStore: &fakeDLQStore{}, ids: []string{dlqSharedIDOne, dlqSharedIDTwo, dlqHexID(dlqSharedPrefix, "3")}}
		_, err := resolveDLQID(context.Background(), spy, input)
		if err == nil || !strings.Contains(err.Error(), "ambiguous") {
			t.Fatalf("err = %v, want the ambiguity refusal", err)
		}
		if strings.Contains(err.Error(), dlqHexID(dlqSharedPrefix, "3")) {
			t.Fatalf("err = %v, must name no more than the first two IDs", err)
		}
	})

	t.Run("finder failure surfaces", func(t *testing.T) {
		spy := &spyDLQPrefixFinder{fakeDLQStore: &fakeDLQStore{}, err: context.DeadlineExceeded}
		_, err := resolveDLQID(context.Background(), spy, input)
		if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("err = %v, want the resolver failure surfaced verbatim", err)
		}
	})
}

// TestDLQIdentityWithoutPrefixFinderCapability pins the narrow seam: a store
// without the prefix capability still serves FULL-ID commands (resolution is a
// pure pass-through there) and refuses a prefix with the explicit
// unsupported-capability error instead of guessing.
func TestDLQIdentityWithoutPrefixFinderCapability(t *testing.T) {
	store := &noFinderDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "events.a.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}

	// A full ID bypasses the capability entirely.
	got, err := resolveDLQID(context.Background(), store, dlqUnitID)
	if err != nil || got != dlqUnitID {
		t.Fatalf("full ID without the capability = (%q, %v), want a pass-through", got, err)
	}

	// A prefix is refused with the explicit capability error, before any
	// operation could run.
	if _, err := resolveDLQID(context.Background(), store, dlqUnitID[:dlqIDShortLength]); err == nil ||
		!strings.Contains(err.Error(), "prefix lookup is not supported by this store") {
		t.Fatalf("prefix without the capability err = %v, want the explicit refusal", err)
	}

	// And the command tree reports it the same way.
	deps := withFakeDLQStore(t, store)
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID[:dlqIDShortLength]); err == nil ||
		!strings.Contains(err.Error(), "prefix lookup is not supported by this store") {
		t.Fatalf("command-path prefix err = %v, want the explicit capability refusal", err)
	}
}

// TestDLQInspectViaUniquePrefixShowsFullID proves the inspect command resolves
// a unique prefix to the FULL stored ID and the entry's original delivery
// metadata: an operator working from `dlq ls`'s ten-character column gets the
// complete identity back on screen.
func TestDLQInspectViaUniquePrefixShowsFullID(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, "1699999999999-0", "alpha", "events.a.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqSharedIDOne, dlqSharedIDOne, "beta", "events.b.handler", `{}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqSharedIDTwo, dlqSharedIDTwo, "beta", "events.c.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", dlqUnitID[:dlqIDShortLength])
	if err != nil {
		t.Fatalf("dlq inspect via prefix: %v", err)
	}
	flat := normWS(out)
	for _, want := range []string{
		"ID: " + dlqUnitID, // the FULL stable ID, never the truncated one
		"Original stream: events",
		"Original ID: 1699999999999-0",
		"Group: relay",
		"Consumer: worker-1",
		"App: alpha",
		"Handler: events.a.handler",
	} {
		if !strings.Contains(flat, want) {
			t.Fatalf("inspect output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, dlqSharedIDOne) || strings.Contains(out, dlqSharedIDTwo) {
		t.Fatalf("inspect must resolve to exactly the unique prefix's entry:\n%s", out)
	}
}

// TestDLQReplayViaUniquePrefix verifies the replay command accepts the
// ls-visible prefix, resolves it to the exact entry, executes its recorded
// handler once, and deletes exactly that entry.
func TestDLQReplayViaUniquePrefix(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqMissingID, dlqMissingID, "user-events", "events.other.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	exec := &dlqReplayExecutor{}
	deps := dlqReplayRunner(t, store, exec)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID[:dlqIDShortLength])
	if err != nil {
		t.Fatalf("dlq replay via prefix: %v", err)
	}
	if strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("stdout = %q", out)
	}
	if handlers, _ := exec.got(); len(handlers) != 1 || handlers[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v, want exactly the resolved entry's handler", handlers)
	}
	if left := store.entries; len(left) != 1 || left[0].ID != dlqMissingID {
		t.Fatalf("the prefix replay must delete exactly its own entry, leaving %+v", left)
	}
}

// TestDLQRmViaUniquePrefix verifies the rm command accepts the ls-visible
// prefix, deletes exactly the resolved entry, and leaves every other entry in
// place.
func TestDLQRmViaUniquePrefix(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "events.a.handler", `{}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqMissingID, dlqMissingID, "beta", "events.b.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", dlqUnitID[:dlqIDShortLength])
	if err != nil {
		t.Fatalf("dlq rm via prefix: %v", err)
	}
	if strings.TrimSpace(out) != "DLQ entry removed" {
		t.Fatalf("stdout = %q", out)
	}
	if left := store.entries; len(left) != 1 || left[0].ID != dlqMissingID {
		t.Fatalf("the prefix rm must delete exactly its own entry, leaving %+v", left)
	}
}

// TestDLQAmbiguousPrefixRefusedBeforeSideEffects proves a shared prefix is
// refused for every admin command BEFORE any side effect: inspect reports the
// two competing IDs, replay never takes a claim (nothing is recorded, nothing
// is released, the replayer is never reached), and rm deletes nothing — the
// ambiguity resolution happens strictly before the exact-ID operations.
func TestDLQAmbiguousPrefixRefusedBeforeSideEffects(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqSharedIDOne, dlqSharedIDOne, "alpha", "events.a.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
		dlqTestEntry(dlqSharedIDTwo, dlqSharedIDTwo, "alpha", "events.b.handler", `{"a":1}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)
	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	for _, cmd := range []string{"inspect", "replay", "rm"} {
		_, _, err := runCLIWithDeps(t, deps, "", "dlq", cmd, dlqSharedPrefix)
		if err == nil {
			t.Fatalf("dlq %s on an ambiguous prefix must fail", cmd)
		}
		if !strings.Contains(err.Error(), "ambiguous DLQ entry ID prefix") ||
			!strings.Contains(err.Error(), dlqSharedIDOne) || !strings.Contains(err.Error(), dlqSharedIDTwo) ||
			!strings.Contains(err.Error(), "use more characters") {
			t.Fatalf("dlq %s err = %v, want the ambiguity refusal naming both candidate IDs", cmd, err)
		}
	}

	if calls, _, _ := rep.got(); calls != 0 {
		t.Fatalf("an ambiguous prefix must never reach the worker: replay calls = %d", calls)
	}
	if deleted := store.deleted; len(deleted) != 0 {
		t.Fatalf("an ambiguous prefix must never delete: deleted = %v", deleted)
	}
	for _, id := range []string{dlqSharedIDOne, dlqSharedIDTwo} {
		if _, held := store.heldToken(id); held {
			t.Fatalf("an ambiguous prefix must never take a replay claim (entry %s is held)", id)
		}
		if got := store.releasedTokensFor(id); len(got) != 0 {
			t.Fatalf("an ambiguous prefix must never release a claim: %s released %v", id, got)
		}
	}
}

// TestDLQUnknownPrefixRefusedBeforeSideEffects proves a valid but unmatched
// prefix is refused by all three commands before any side effect, with each
// command's exact message naming the prefix.
func TestDLQUnknownPrefixRefusedBeforeSideEffects(t *testing.T) {
	const unknownPrefix = "0badf00d00" // lowercase hex, no matching entry
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "events.a.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	deps := withFakeDLQ(t, store)
	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	for _, cmd := range []string{"inspect", "replay", "rm"} {
		_, _, err := runCLIWithDeps(t, deps, "", "dlq", cmd, unknownPrefix)
		if got := fmt.Sprintf("no DLQ entry matches ID prefix %q", unknownPrefix); err == nil ||
			!strings.Contains(err.Error(), got) {
			t.Fatalf("dlq %s err = %v, want the no-match error %q", cmd, err, got)
		}
	}
	if calls, _, _ := rep.got(); calls != 0 {
		t.Fatalf("an unknown prefix must never reach the worker: replay calls = %d", calls)
	}
	if deleted := store.deleted; len(deleted) != 0 {
		t.Fatalf("an unknown prefix must never delete: deleted = %v", deleted)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unknown prefix must keep the store untouched: %+v", store.entries)
	}
}

// TestDLQUnsettledPrefixRefusedBeforeWorkerExecution proves the settlement
// boundary survives prefix resolution: an unsettled entry (its source delivery
// may still be recoverable) reached by its unique prefix is refused by BOTH
// replay and rm before the worker is dialed, nothing claims or deletes, and
// the entry is kept.
func TestDLQUnsettledPrefixRefusedBeforeWorkerExecution(t *testing.T) {
	entry := dlqTestEntry(dlqUnitID, dlqUnitID, "user-events", "events.created.handler", `{"event_name":"INSERT"}`, "2026-09-23T10:00:00Z")
	entry.SettledAt = ""
	store := &fakeDLQStore{entries: []dlq.Entry{entry}}
	deps := withFakeDLQ(t, store)
	rep := &recordingReplayer{}
	startTestSocketWithReplayer(t, deps.SocketPath, rep)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", dlqUnitID[:dlqIDShortLength])
	if got := fmt.Sprintf("DLQ entry %q is not settled", dlqUnitID); err == nil ||
		!strings.Contains(err.Error(), got) {
		t.Fatalf("replay err = %v, want the not-settled refusal naming the resolved ID", err)
	}
	if calls, _, _ := rep.got(); calls != 0 {
		t.Fatalf("an unsettled entry must not execute: replay calls = %d", calls)
	}
	if _, held := store.heldToken(dlqUnitID); held {
		t.Fatal("an unsettled entry must not take a replay claim")
	}

	_, _, err = runCLIWithDeps(t, deps, "", "dlq", "rm", dlqUnitID[:dlqIDShortLength])
	if got := fmt.Sprintf("DLQ entry %q is not settled", dlqUnitID); err == nil ||
		!strings.Contains(err.Error(), got) {
		t.Fatalf("rm err = %v, want the not-settled refusal naming the resolved ID", err)
	}
	if len(store.entries) != 1 {
		t.Fatalf("an unsettled entry must be kept by both refusals: %+v", store.entries)
	}
}

// TestDLQRmVanishedResolvedIDKeepsNewRow pins the prefix-resolution race end to
// end on the destructive command: the resolver answers with one unique ID, but
// by the time the exact operation runs that row is gone and a DIFFERENT row
// with the same prefix exists (deleted, then replaced by a fresh delivery
// matching the same visible prefix). The removal must fail on the RESOLVED
// identity — reporting the vanished ID, never re-scanning the prefix — and
// must leave the new row untouched.
func TestDLQRmVanishedResolvedIDKeepsNewRow(t *testing.T) {
	// The replacement row shares the prefix the operator copied from `dlq ls`;
	// the resolved row it answers for no longer exists.
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqSharedIDTwo, dlqSharedIDTwo, "alpha", "events.a.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	spy := &spyDLQPrefixFinder{fakeDLQStore: store, ids: []string{dlqSharedIDOne}}
	deps := withFakeDLQStore(t, spy)

	_, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", dlqSharedPrefix)
	if got := fmt.Sprintf("unknown DLQ entry %q", dlqSharedIDOne); err == nil ||
		!strings.Contains(err.Error(), got) {
		t.Fatalf("rm err = %v, want the failure pinned to the resolved ID %q", err, got)
	}
	if strings.Contains(err.Error(), dlqSharedIDTwo) {
		t.Fatalf("rm err = %v, must never name the post-resolution row %q", err, dlqSharedIDTwo)
	}
	if deleted := store.deleted; len(deleted) != 0 {
		t.Fatalf("the stale resolution must delete nothing: deleted = %v", deleted)
	}
	if len(store.entries) != 1 || store.entries[0].ID != dlqSharedIDTwo {
		t.Fatalf("the post-resolution row must be untouched: %+v", store.entries)
	}
}

// TestDLQResolveIDByteVerifiesCandidates pins the candidate byte-verification
// guard: a resolver answer that is syntactically valid but does NOT begin with
// the requested prefix — the collation-widened-range hazard — fails the whole
// resolution closed, before any exact-ID operation can run. The dangerous
// shape here is deliberate: the canned answer is the identity of an UNRELATED
// settled entry, exactly the target an unguarded operation would have deleted
// or claimed on this prefix's behalf.
func TestDLQResolveIDByteVerifiesCandidates(t *testing.T) {
	store := &fakeDLQStore{entries: []dlq.Entry{
		dlqTestEntry(dlqUnitID, dlqUnitID, "alpha", "events.a.handler", `{}`, "2026-09-23T10:00:00Z"),
	}}
	// A single canned candidate that fails the byte check for its prefix.
	spy := &spyDLQPrefixFinder{fakeDLQStore: store, ids: []string{dlqUnitID}}
	deps := withFakeDLQStore(t, spy)

	t.Run("resolution refuses the non-matching candidate", func(t *testing.T) {
		_, err := resolveDLQID(context.Background(), spy, dlqSharedPrefix)
		if err == nil {
			t.Fatal("a non-matching candidate must fail resolution")
		}
		if !strings.Contains(err.Error(), fmt.Sprintf(
			"no DLQ entry safely matches ID prefix %q; use the full 64-character ID", dlqSharedPrefix)) {
			t.Fatalf("err = %v, want the fail-closed byte-verification refusal naming the requested prefix", err)
		}
	})

	// Through the command tree, the refusal must precede the exact operation:
	// rm of the poisoned prefix reports the fail-closed error and the
	// unrelated entry — the would-be wrong target — survives untouched.
	t.Run("rm fails closed before the delete", func(t *testing.T) {
		if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", dlqSharedPrefix); err == nil ||
			!strings.Contains(err.Error(), "no DLQ entry safely matches ID prefix") {
			t.Fatalf("rm err = %v, want the fail-closed resolution refusal", err)
		}
		if deleted := store.deleted; len(deleted) != 0 {
			t.Fatalf("a byte-unverified candidate must never delete: deleted = %v", deleted)
		}
		if len(store.entries) != 1 || store.entries[0].ID != dlqUnitID {
			t.Fatalf("the unrelated wrong-target entry must survive: %+v", store.entries)
		}
	})
}
