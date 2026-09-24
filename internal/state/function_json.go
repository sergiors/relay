package state

import (
	"encoding/json"
	"fmt"
)

// This file is the boundary between the typed function detail model (the source
// of truth) and the JSON object stored in the functions.data BLOB column. The
// schema keeps only the stable relational metadata as columns — functions.name
// and functions.updated_at — while the whole snapshot (runtime/status/image/
// fingerprint/prepared_at/last-reconcile outcome/env/secret references/networks/
// handlers/schedules/services) is marshalled here with encoding/json and written
// through SQLite's jsonb() so it is stored in SQLite's binary JSON (JSONB)
// format. Reads render it back to JSON text with json(data).
//
// The handler/event and schedule entries carry only the fields the state model
// exposes (handler name/timeout/retries, and handler/cron/timezone/timeout/
// retries): the template parser's opaque ValueMatcher pattern interfaces are
// deliberately never part of the persisted snapshot — matching is rebuilt from
// template.yaml, never from this read-only view. Secret entries hold only the
// reference name, never a resolved value.

// marshalFunction encodes d as the JSON object stored in functions.data. Name
// and UpdatedAt are relational metadata (functions.name / functions.updated_at)
// and HandlerCount is derived, so all three are excluded by their json:"-"
// tags; every other field — including the whole handler, schedule, service, env,
// and secret-reference lists — is part of the snapshot.
func marshalFunction(d Detail) (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("marshal function %q: %w", d.Name, err)
	}
	return string(b), nil
}

// unmarshalFunction decodes a functions.data value (already rendered to JSON
// text by json(data)). A NULL value is a corrupt payload: the column is NOT
// NULL and every writer stores a JSON object, so a missing rendering can only
// mean the stored blob is not valid JSON. The error is returned so the caller
// logs a clear decode failure and treats the row as unreadable.
func unmarshalFunction(text string) (Detail, error) {
	var d Detail
	if err := json.Unmarshal([]byte(text), &d); err != nil {
		return Detail{}, fmt.Errorf("unmarshal function: %w", err)
	}
	return d, nil
}
