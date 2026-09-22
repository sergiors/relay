package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// This file is the single boundary between the typed stats structs (the source
// of truth) and the JSON payloads stored in the stats.data and
// function_stats.data TEXT columns. The schema keeps only the stable relational
// metadata as columns — the stats single-row id, the function_stats
// function_name key, and both updated_at columns — while every evolving
// counter/gauge/timestamp field is marshalled here. No other package marshals
// stats to JSON: the worker, CLI, runtime, and metrics layers only ever pass
// typed Stats / FunctionStats values.
//
// Payload field names mirror the previous explicit column names
// (events_matched_total, warm_acquires_total, last_execution_at, ...), so the
// storage format stays self-describing. Absent fields decode to their Go zero
// value: a payload written by an older/future writer that does not carry a
// field reads as 0 (or "" for timestamps), exactly like the old NOT NULL
// DEFAULT 0 / NULL columns.

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

// unmarshalStats decodes a stats.data value. A NULL or empty column is the
// zero payload (no fields recorded yet) and is not an error; absent fields
// likewise decode to zero. A non-empty invalid value returns the JSON error so
// the caller can log a useful message and treat the row as unreadable.
func unmarshalStats(data sql.NullString) (Stats, error) {
	var s Stats
	if !data.Valid || data.String == "" {
		return s, nil
	}
	if err := json.Unmarshal([]byte(data.String), &s); err != nil {
		return Stats{}, fmt.Errorf("unmarshal stats: %w", err)
	}
	return s, nil
}

// marshalFunctionStats encodes fs's payload for the function_stats.data column.
// The function name is relational metadata (function_stats.function_name) and
// UpdatedAt is relational (function_stats.updated_at); both are excluded by
// their json:"-" tags. Empty execution-history timestamps are omitted
// (omitempty), which is what makes an empty incoming value PRESERVE the
// persisted timestamp when merging (see mergeFunctionStatsTimestamps); counters
// are always emitted — including an explicit 0 — because they are absolute
// snapshots and must be able to reset on a legitimate zero.
func marshalFunctionStats(fs FunctionStats) (string, error) {
	b, err := json.Marshal(fs)
	if err != nil {
		return "", fmt.Errorf("marshal function stats for %q: %w", fs.Function, err)
	}
	return string(b), nil
}

// unmarshalFunctionStats decodes a function_stats.data value. A NULL or empty
// column is the zero payload and is not an error; absent fields decode to zero.
// A non-empty invalid value returns the JSON error so the caller can log it.
func unmarshalFunctionStats(data sql.NullString) (FunctionStats, error) {
	var fs FunctionStats
	if !data.Valid || data.String == "" {
		return fs, nil
	}
	if err := json.Unmarshal([]byte(data.String), &fs); err != nil {
		return FunctionStats{}, fmt.Errorf("unmarshal function stats: %w", err)
	}
	return fs, nil
}

// mergeFunctionStatsTimestamps returns incoming with any empty
// execution-history timestamp filled from stored. Counters (including the
// warm-container pool counters) are always taken from incoming: they are
// absolute snapshots and a flush legitimately replaces them, even with 0. The
// four Last*At fields, by contrast, record "last observed at" and an empty
// incoming value means "no observation this flush" — it must never erase a
// persisted timestamp. This reproduces the CASE-guarded upsert semantics the
// explicit columns used before the payload moved into JSON.
func mergeFunctionStatsTimestamps(stored, incoming FunctionStats) FunctionStats {
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
