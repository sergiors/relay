package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"relay/internal/dlq"
	"relay/internal/worker"
)

// replayDLQEntry admits, executes, and settles one fenced replay.
//
// Admission is a PostgreSQL claim committed BEFORE the worker is contacted: only
// a settled entry is eligible, a monotonic fencing token is incremented, and a
// lease in database time is stamped (dlq.ReplayLease). A concurrent replay sees
// the unexpired lease, gets an explicit replay-in-progress error, and never
// dials, so two CLIs cannot both execute the same entry. The claim never spans
// execution: no database transaction is held while the handler runs.
//
// The claim is released early only when the outcome is known to have left no
// running handler: a pre-dial/model error, a worker-reported failure (including
// handler failure, unknown app, or removed handler), or a request that never
// reached the worker. An ambiguous outcome — a lost/malformed response, a read
// or write failure after send, or client cancellation — retains the lease until
// it expires; this deliberately favors not starting a second execution over
// immediate retry. Expiry is the recovery path for a crashed claimant.
//
// On success the delete is one fenced conditional statement requiring the entry
// to still be settled, to still carry this claim's token, and to have an
// unexpired lease at database time. A stale claimant can therefore never delete a
// row that was re-claimed; it reports that the entry was retained.
//
// Replay is at-least-once, not exactly-once: a handler may finish after the CLI
// disconnects (the worker's replay context is not cancelled by the disconnect) or
// after the lease expires, so the same entry can be executed again. Handlers must
// stay idempotent.
//
// A non-replayable entry is rejected after admission and its claim released: a
// malformed-message or oversized-event placeholder has no app/handler to
// re-execute. The entry is kept for inspection or removal.
//
// The handler is handed the entry's stored ReplayPayload, the exact logical
// payload produced by the same envelope decoder as live delivery (raw JSON,
// normalized Debezium event, or Occurrence.Payload for a schedule). There is no
// fallback to the diagnostic Event body: an entry with no stored payload is an
// explicit pre-dial error and releases the claim.
func replayDLQEntry(
	ctx context.Context, w io.Writer, store dlq.Store, socketPath, id string,
) error {
	claim, err := store.ClaimDLQReplay(ctx, id, dlq.ReplayLease)
	if err != nil {
		switch {
		case errors.Is(err, dlq.ErrEntryNotFound):
			return fmt.Errorf("unknown DLQ entry %q", id)
		case errors.Is(err, dlq.ErrEntryUnsettled):
			// An unsettled entry's source delivery may still be recoverable.
			// Replaying it would execute a handler whose dead letter cannot yet
			// be removed, and the later delete would be refused after the side
			// effect had already happened.
			return fmt.Errorf("DLQ entry %q is not settled: source delivery may still be recoverable", id)
		case errors.Is(err, dlq.ErrReplayInProgress):
			return fmt.Errorf("DLQ entry %q replay is already in progress; retry after the claim lease expires", id)
		default:
			return fmt.Errorf("replay %s: %w", id, err)
		}
	}
	entry := claim.Entry
	if !entry.Replayable() {
		return releaseReplayClaim(ctx, store, id, claim.Token,
			fmt.Errorf("DLQ entry %q is not replayable: no app/handler to re-execute (malformed-message placeholder or oversized-event summary)", id))
	}
	// Replay exactly the logical handler payload stored when the entry was
	// dead-lettered. It already reflects the envelope decoder's schedule
	// payload and Debezium normalization, so the diagnostic Event body is never
	// re-derived here. An entry without a stored payload must be refused before
	// dialing the worker.
	if len(entry.ReplayPayload) == 0 {
		return releaseReplayClaim(ctx, store, id, claim.Token,
			fmt.Errorf("DLQ entry %q is not replayable: no stored handler payload", id))
	}
	payload := entry.ReplayPayload

	if err := worker.ReplayDLQ(
		ctx, socketPath, entry.App, entry.Handler, payload, entry.Trace, entry.SourceMessageID,
	); err != nil {
		replayErr := fmt.Errorf("replay %s: %w", id, err)
		// A definitive worker answer means no handler is still running. A
		// request that never started is equally safe to release. Everything
		// else (lost response, I/O failure after send, cancellation) may have
		// executed and retains the lease until expiry.
		if errors.Is(err, worker.ErrInvokeFailed) || errors.Is(err, worker.ErrInvokeNotStarted) {
			return releaseReplayClaim(ctx, store, id, claim.Token, replayErr)
		}
		return replayErr
	}

	// The handler succeeded: delete exactly this entry with the fenced claim.
	// A failure here means the replay DID run, so the operator must know the
	// entry lingers; re-running would repeat the execution.
	if err := store.DeleteDLQReplay(ctx, id, claim.Token); err != nil {
		return fmt.Errorf("replay %s: handler succeeded but the DLQ entry was retained: %w", id, err)
	}
	out := newOutput(w)
	out.line("Replayed handler successfully")
	return out.err
}

// releaseReplayClaim releases a claim for an outcome known not to have left a
// running handler, preserving the replay error as the primary cause. A release
// failure is joined rather than hidden: the claim then stays until its lease
// expires, which is safe but delays the next attempt.
func releaseReplayClaim(ctx context.Context, store dlq.Store, id string, token int64, cause error) error {
	if err := store.ReleaseDLQReplay(ctx, id, token); err != nil {
		return errors.Join(cause, fmt.Errorf("release DLQ replay claim: %w", err))
	}
	return cause
}
