package state

import (
	"encoding/json"
	"fmt"
)

// This file is the boundary between the typed app detail model (the source
// of truth) and the native JSONB object stored in the apps.data column. The
// schema keeps stable relational metadata as columns — apps.name,
// apps.updated_at, and apps.fencing_token — while apps.data stores the whole
// snapshot (runtime/status/image/fingerprint/desired_fingerprint/prepared_at/
// last-reconcile outcome/env/secret
// references/handlers/schedules/services) is marshalled here with encoding/json
// and written as a PostgreSQL jsonb value. Reads render it back to JSON text
// with `data::text`.
//
// The handler/event and schedule entries carry only the fields the state model
// exposes (handler name/timeout/retries, and handler/cron/timezone/timeout/
// retries): the template parser's opaque pattern interfaces are
// deliberately never part of the persisted snapshot — matching is rebuilt from
// template.yaml (internal/event), never from this read-only view. Secret entries hold only the
// reference name, never a resolved value; env entries hold only the env-var
// name plus a fixed redaction marker, never the template's literal value (any
// operator with database access can read the shared snapshot).

// marshalApp encodes detail as the JSON object stored in apps.data. Name and
// UpdatedAt are relational metadata (apps.name / apps.updated_at), and
// HandlerCount is derived, so all three are excluded by their json:"-" tags.
// The PostgreSQL fencing token is separate from Detail and is not serialized.
func marshalApp(detail Detail) (string, error) {
	b, err := json.Marshal(detail)
	if err != nil {
		return "", fmt.Errorf("marshal app %q: %w", detail.Name, err)
	}
	return string(b), nil
}

// unmarshalApp decodes an apps.data value (already rendered to JSON text by
// `data::text`). A NULL/empty value is unexpected: the column is NOT NULL and
// every writer stores a JSON object, so an empty rendering means the row was
// written by something other than this package. The error is returned so the
// caller logs a clear decode failure and treats the row as unreadable.
func unmarshalApp(text string) (Detail, error) {
	var detail Detail
	if err := json.Unmarshal([]byte(text), &detail); err != nil {
		return Detail{}, fmt.Errorf("unmarshal app: %w", err)
	}
	return detail, nil
}
