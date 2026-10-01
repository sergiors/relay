package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// PendingOccurrence is one durable schedule-publication retry record: the
// COMPLETE immutable occurrence intent (the derived occurrence identity plus the
// app/schedule/handler/scheduled_at it was built from) together with the
// scheduling fields used to coordinate retry observations.
//
// It is NOT execution history and NOT an execution source: /apps stays
// authoritative, and the row exists only until a later publication resolves
// (published, or a clean duplicate), at which point it is deleted. The intent
// fields are never updated after insert; only Attempts and NextAttempt change.
type PendingOccurrence struct {
	ID          string
	App         string
	Schedule    string
	Handler     string
	ScheduledAt time.Time
	// Attempts is the number of durable retry observations already recorded for
	// this occurrence; it indexes the retry backoff.
	Attempts int
	// NextAttempt is the earliest instant the record may be claimed again.
	NextAttempt time.Time
	// LeaseUntil is the instant until which a retrier holds the record; a
	// record is claimable only while both NextAttempt and LeaseUntil are in the
	// past.
	LeaseUntil time.Time
	// DecodeErr is set ONLY when the row's stored intent could not be decoded
	// (the data blob is not valid JSON, or is missing/malformed a required
	// field). When non-nil the App/Schedule/Handler/ScheduledAt fields are empty
	// and the caller MUST NOT publish the record: publishing would target a
	// different occurrence than the row represents. The ID and scheduling fields
	// are still populated so the caller can surface the failure and
	// retain/reschedule the durable row.
	DecodeErr error
}

// pendingIntent is the occurrence intent stored as the JSON object in
// schedule_pending.data, mirroring apps.data: the schema keeps only the stable
// key (id) and the scheduling columns relational, while the intent lives in the
// BLOB. It is deliberately a distinct type from PendingOccurrence so the
// scheduling fields can never leak into the persisted intent.
type pendingIntent struct {
	App         string `json:"app"`
	Schedule    string `json:"schedule"`
	Handler     string `json:"handler"`
	ScheduledAt string `json:"scheduled_at"`
}

// marshalPendingIntent encodes p's immutable intent as the JSON object written
// through jsonb(?) into schedule_pending.data. ScheduledAt is normalized to UTC
// RFC3339, the same representation occurrence identity is derived from, so a
// round-trip reconstructs the exact occurrence.
func marshalPendingIntent(p PendingOccurrence) (string, error) {
	b, err := json.Marshal(pendingIntent{
		App:         p.App,
		Schedule:    p.Schedule,
		Handler:     p.Handler,
		ScheduledAt: p.ScheduledAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return "", fmt.Errorf("marshal pending occurrence %s: %w", p.ID, err)
	}
	return string(b), nil
}

// unmarshalPendingIntent decodes a schedule_pending.data value (already rendered
// to JSON text by json(data)) into its occurrence intent. A payload that is not
// valid JSON, or that is missing a required field, is returned as an error so the
// caller can log it and RETAIN the row; it is never treated as an empty
// occurrence and never deleted.
func unmarshalPendingIntent(text string) (pendingIntent, error) {
	var in pendingIntent
	if err := json.Unmarshal([]byte(text), &in); err != nil {
		return pendingIntent{}, fmt.Errorf("unmarshal pending intent: %w", err)
	}
	if in.App == "" || in.Schedule == "" || in.Handler == "" || in.ScheduledAt == "" {
		return pendingIntent{}, fmt.Errorf("unmarshal pending intent: incomplete occurrence intent")
	}
	if _, err := time.Parse(time.RFC3339, in.ScheduledAt); err != nil {
		return pendingIntent{}, fmt.Errorf("unmarshal pending intent: scheduled_at: %w", err)
	}
	return in, nil
}

// pendingDataExpr renders schedule_pending.data to JSON text, or NULL when the
// stored blob is not valid JSON/JSONB. It mirrors jsonPayloadExpr: a row written
// through jsonb(?) always validates, so a NULL rendering is a corrupt payload the
// caller must retain rather than decode as an empty occurrence.
const pendingDataExpr = `CASE WHEN json_valid(data, 5) THEN json(data) END`

// SavePendingOccurrence atomically persists p's occurrence intent unless a row
// for the same ID already exists (the ID is the primary key and the write is a
// single INSERT ... ON CONFLICT DO NOTHING), so persistence is unique and
// idempotent under concurrency and redelivery. It returns whether a NEW row was
// inserted; inserted=false means a row already existed and its intent (and any
// retry progress) is preserved untouched.
//
// The attempt count starts at zero and the caller supplies the first due
// instant; a re-persist of an existing ID never rewinds either.
func (st *State) SavePendingOccurrence(ctx context.Context, p PendingOccurrence) (bool, error) {
	payload, err := marshalPendingIntent(p)
	if err != nil {
		return false, err
	}
	res, err := st.db.ExecContext(ctx, `
		INSERT INTO schedule_pending
			(id, data, attempts, next_attempt_ms, lease_until_ms)
		VALUES (?, jsonb(?), 0, ?, 0)
		ON CONFLICT(id) DO NOTHING`,
		p.ID,
		payload,
		p.NextAttempt.UTC().UnixMilli(),
	)
	if err != nil {
		return false, fmt.Errorf("save pending occurrence %s: %w", p.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("save pending occurrence %s: rows affected: %w", p.ID, err)
	}
	return n > 0, nil
}

// DeletePendingOccurrence removes the durable record for id. It is idempotent: a
// missing row is not an error, so a delete after an ambiguous success or a
// concurrent retrier's delete is harmless. Callers delete ONLY after a publish
// call resolved with a nil error (published or clean duplicate); a failed delete
// leaves the row in place for an idempotent retry.
func (st *State) DeletePendingOccurrence(ctx context.Context, id string) error {
	if _, err := st.db.ExecContext(ctx, `DELETE FROM schedule_pending WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete pending occurrence %s: %w", id, err)
	}
	return nil
}

// ClaimPendingOccurrences atomically leases up to limit records that are due
// (NextAttempt and any prior LeaseUntil both at or before now) and returns them.
// The lease is written in the same statement that selects the rows
// (UPDATE ... RETURNING), so two concurrent retriers in the same database can
// never claim the same record: coordination is per-row in the database, never a
// process-global lock.
//
// A claimed row whose stored intent is corrupt (not valid JSON, or missing a
// required field) is returned with DecodeErr set and empty intent fields — NOT
// silently skipped. The caller must surface the failure and retain/reschedule the
// durable row; it must never publish it (which would target the wrong occurrence)
// and never delete it (which would lose the pending work). Returning the row also
// keeps its lease advancing under the caller's backoff, so a corrupt row does not
// spin in a tight reclaim loop. A decode failure is never fatal to the batch —
// the remaining valid records are still returned.
//
// The returned records carry their persisted attempt count. A record whose lease
// later expires without resolving is reclaimed by the next scan, so a crashed or
// wedged retrier's work is automatically recovered.
func (st *State) ClaimPendingOccurrences(ctx context.Context, now, leaseUntil time.Time, limit int) ([]PendingOccurrence, error) {
	nowMs := now.UTC().UnixMilli()
	rows, err := st.db.QueryContext(ctx, `
		UPDATE schedule_pending SET lease_until_ms = ?
		WHERE id IN (
			SELECT id FROM schedule_pending
			WHERE next_attempt_ms <= ? AND lease_until_ms <= ?
			ORDER BY next_attempt_ms
			LIMIT ?
		)
		RETURNING id, `+pendingDataExpr+`, attempts, next_attempt_ms, lease_until_ms`,
		leaseUntil.UTC().UnixMilli(), nowMs, nowMs, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claim pending occurrences: %w", err)
	}
	defer rows.Close()

	var out []PendingOccurrence
	for rows.Next() {
		var (
			id       string
			data     sql.NullString
			attempts int
			nextMs   int64
			leaseMs  int64
		)
		if err := rows.Scan(&id, &data, &attempts, &nextMs, &leaseMs); err != nil {
			return nil, fmt.Errorf("scan pending occurrence: %w", err)
		}
		rec := PendingOccurrence{
			ID:          id,
			Attempts:    attempts,
			NextAttempt: time.UnixMilli(nextMs).UTC(),
			LeaseUntil:  time.UnixMilli(leaseMs).UTC(),
		}
		// A corrupt/absent rendering is surfaced via DecodeErr, not returned as an
		// empty occurrence: the caller must not publish or delete it, and must
		// reschedule it so the durable row is retained and retried/repairable.
		if !data.Valid {
			rec.DecodeErr = fmt.Errorf("pending occurrence %s: stored payload is not valid JSON", id)
			out = append(out, rec)
			continue
		}
		intent, err := unmarshalPendingIntent(data.String)
		if err != nil {
			rec.DecodeErr = err
			out = append(out, rec)
			continue
		}
		scheduledAt, err := time.Parse(time.RFC3339, intent.ScheduledAt)
		if err != nil {
			// Defensive: unmarshalPendingIntent already validated the timestamp.
			rec.DecodeErr = err
			out = append(out, rec)
			continue
		}
		rec.App = intent.App
		rec.Schedule = intent.Schedule
		rec.Handler = intent.Handler
		rec.ScheduledAt = scheduledAt.UTC()
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim pending occurrences: %w", err)
	}
	return out, nil
}

// ReschedulePendingOccurrence records one FAILED durable retry for id: it
// increments the persisted attempt count and sets the next due instant, and
// clears the lease so the record becomes claimable again once due. The increment
// is done in SQL (attempts = attempts + 1), so overlapping observations never
// lose a retry. Only a resolved (nil-error) publication deletes a row; this is
// the failure path.
func (st *State) ReschedulePendingOccurrence(ctx context.Context, id string, nextAttempt time.Time) error {
	_, err := st.db.ExecContext(ctx, `
		UPDATE schedule_pending
		SET attempts = attempts + 1, next_attempt_ms = ?, lease_until_ms = 0
		WHERE id = ?`,
		nextAttempt.UTC().UnixMilli(), id)
	if err != nil {
		return fmt.Errorf("reschedule pending occurrence %s: %w", id, err)
	}
	return nil
}

// NextPendingDue returns the earliest instant at which any pending record
// becomes claimable: the minimum of MAX(NextAttempt, LeaseUntil) across all
// rows. It is how the retry loop computes its sleep without polling. ok=false
// means the outbox is empty (the loop should wait for a wake signal). A corrupt
// row still counts, so a retained-but-unreadable record keeps the loop waking at
// its lease/backoff cadence rather than being silently forgotten.
func (st *State) NextPendingDue(ctx context.Context) (time.Time, bool, error) {
	var ms sql.NullInt64
	if err := st.db.QueryRowContext(ctx,
		`SELECT MIN(MAX(next_attempt_ms, lease_until_ms)) FROM schedule_pending`).Scan(&ms); err != nil {
		return time.Time{}, false, fmt.Errorf("next pending due: %w", err)
	}
	if !ms.Valid {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(ms.Int64).UTC(), true, nil
}
