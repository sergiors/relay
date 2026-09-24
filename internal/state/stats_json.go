package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// This file is the single boundary between the typed stats structs (the source
// of truth) and the JSON payloads stored in the stats.data and
// function_stats.data columns. The schema keeps only the stable relational
// metadata as columns — the stats single-row id, the function_stats
// function_name key, and both updated_at columns — while every evolving
// counter/gauge/timestamp field is marshalled here. No other package marshals
// stats to JSON: the worker, CLI, runtime, and metrics layers only ever pass
// typed Stats / FunctionStats values.
//
// Storage format: the data column is a NOT NULL BLOB holding SQLite's binary
// JSON (JSONB) format. Writes pass the marshalled JSON text through SQLite's
// jsonb(?) constructor, which validates and converts it; reads render it back to
// JSON text with json(data). A stored value that is not valid JSON (only
// reachable by writing the column directly, since the typed marshallers always
// emit valid JSON) is treated as a clear decode failure rather than silently
// decoding to zero.
//
// Payload field names are stable and self-describing (events_matched_total,
// warm_acquires_total, last_execution_at, ...). Absent fields decode to their Go
// zero value: a payload that does not carry a field reads as 0 (or "" for
// timestamps). There is no migration or backward compatibility for a payload
// written by a different schema.

// jsonPayloadExpr is the SELECT expression that renders a table's `data` column
// back to JSON text, yielding NULL when the stored value is not valid JSON. The
// json_valid flag 5 means "accept either JSON text (1) or JSONB (4)", so a row
// written through jsonb(?) or seeded as raw JSON text both read back, while a
// corrupt value renders NULL and surfaces as a decode error rather than a SQL
// error from json(). It is embedded in every functions/stats/function_stats read
// so the corrupt-payload handling is uniform. A NULL rendering is a decode
// failure here because every writer stores a JSON object into a NOT NULL column.
const jsonPayloadExpr = `CASE WHEN json_valid(data, 5) THEN json(data) END`

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
// stored blob is not valid JSON. A valid payload that omits fields (including a
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

// unmarshalFunctionStats decodes a function_stats.data value rendered as JSON
// text by jsonPayloadExpr. A NULL value is a corrupt payload (the column is NOT
// NULL and every writer stores a JSON object) and returns an error; a valid
// payload that omits fields decodes to their zero values.
func unmarshalFunctionStats(data sql.NullString) (FunctionStats, error) {
	var fs FunctionStats
	if !data.Valid || data.String == "" {
		return FunctionStats{}, fmt.Errorf("unmarshal function stats: stored payload is not valid JSON")
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
