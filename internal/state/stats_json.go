package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// This file is the single boundary between the typed stats structs (the source
// of truth) and the JSON payloads stored in the stats.data and
// app_stats.data columns. The schema keeps only the stable relational
// metadata as columns — the stats single-row id, the app_stats
// app_name key, and both updated_at columns — while every evolving
// counter/gauge/timestamp field is marshalled here. No other package marshals
// stats to JSON: the worker, CLI, runtime, and metrics layers only ever pass
// typed Stats / AppStats values.
//
// Storage format: the data column is a NOT NULL native JSONB value. Writes pass
// the marshalled JSON text as a jsonb parameter; reads render it back to JSON
// text with `data::text`. A JSONB value is always valid JSON, so a payload that
// fails to decode into the typed struct (a well-formed value of the wrong shape,
// or a bare scalar) is treated as a clear decode failure rather than silently
// decoding to zero.
//
// Payload field names are stable and self-describing (events_matched_total,
// warm_acquires_total, last_execution_at, ...). Absent fields decode to their Go
// zero value: a payload that does not carry a field reads as 0 (or "" for
// timestamps). There is no migration or backward compatibility for a payload
// written by a different schema.

// jsonPayloadExpr is the SELECT expression that renders a table's JSONB `data`
// column to JSON text. The data columns are native PostgreSQL JSONB and are
// always valid JSON (the column is NOT NULL and every writer stores the JSON
// text of a typed struct), so no separate JSON-validity guard is needed; any
// payload that does not decode into the typed struct is still surfaced as a
// decode error by the unmarshal helpers. `data::text` renders the JSONB value
// as canonical JSON text.
//
// The unmarshal helpers still treat an unexpected NULL/empty rendering as a
// decode failure, because the NOT NULL column guarantees a value: a NULL
// rendering can only mean the row was written by something other than this
// package.
const jsonPayloadExpr = `data::text`

// marshalStats encodes s's payload for the stats.data column. UpdatedAt is
// relational metadata (stats.updated_at) and is deliberately excluded by its
// json:"-" tag.
func marshalStats(s Stats) (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("marshal stats: %w", err)
	}
	return string(b), nil
}

// unmarshalStats decodes a stats.data value rendered as JSON text by
// jsonPayloadExpr. A NULL value is a corrupt payload: the column is NOT NULL and
// every writer stores a JSON object, so a NULL rendering can only mean the
// stored value is not valid JSON. A valid payload that omits fields (including a
// bare JSON null) decodes to their zero values. The non-empty invalid case
// returns the JSON error so the caller can log a useful message.
func unmarshalStats(data sql.NullString) (Stats, error) {
	var s Stats
	if !data.Valid || data.String == "" {
		return Stats{}, fmt.Errorf("unmarshal stats: stored payload is not valid JSON")
	}
	if err := json.Unmarshal([]byte(data.String), &s); err != nil {
		return Stats{}, fmt.Errorf("unmarshal stats: %w", err)
	}
	return s, nil
}

// marshalAppStats encodes fs's payload for the app_stats.data column.
// The app name is relational metadata (app_stats.app_name) and
// UpdatedAt is relational (app_stats.updated_at); both are excluded by
// their json:"-" tags. Empty execution-history timestamps are omitted
// (omitempty), which is what makes an empty incoming value PRESERVE the
// persisted timestamp when merging (see mergeAppStatsTimestamps); counters
// are always emitted — including an explicit 0 — because they are absolute
// snapshots and must be able to reset on a legitimate zero.
func marshalAppStats(fs AppStats) (string, error) {
	b, err := json.Marshal(fs)
	if err != nil {
		return "", fmt.Errorf("marshal app stats for %q: %w", fs.App, err)
	}
	return string(b), nil
}

// unmarshalAppStats decodes a app_stats.data value rendered as JSON
// text by jsonPayloadExpr. A NULL value is a corrupt payload (the column is NOT
// NULL and every writer stores a JSON object) and returns an error; a valid
// payload that omits fields decodes to their zero values.
func unmarshalAppStats(data sql.NullString) (AppStats, error) {
	var fs AppStats
	if !data.Valid || data.String == "" {
		return AppStats{}, fmt.Errorf("unmarshal app stats: stored payload is not valid JSON")
	}
	if err := json.Unmarshal([]byte(data.String), &fs); err != nil {
		return AppStats{}, fmt.Errorf("unmarshal app stats: %w", err)
	}
	return fs, nil
}

// mergeAppStatsTimestamps returns incoming with any empty
// execution-history timestamp filled from stored. Counters (including the
// warm-container pool counters) are always taken from incoming: they are
// absolute snapshots and a flush legitimately replaces them, even with 0. The
// four Last*At fields, by contrast, record "last observed at" and an empty
// incoming value means "no observation this flush" — it must never erase a
// persisted timestamp. This reproduces the CASE-guarded upsert semantics the
// explicit columns used before the payload moved into JSON.
func mergeAppStatsTimestamps(stored, incoming AppStats) AppStats {
	if incoming.LastExecutionAt == "" {
		incoming.LastExecutionAt = stored.LastExecutionAt
	}
	if incoming.LastSuccessAt == "" {
		incoming.LastSuccessAt = stored.LastSuccessAt
	}
	if incoming.LastFailureAt == "" {
		incoming.LastFailureAt = stored.LastFailureAt
	}
	if incoming.LastDLQAt == "" {
		incoming.LastDLQAt = stored.LastDLQAt
	}
	return incoming
}
