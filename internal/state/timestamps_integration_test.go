//go:build integration

package state

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRelationalTimestampsUseNativePostgresTypes verifies the direct schema
// uses TIMESTAMPTZ for each relational updated_at/selected_at column, including
// the stats reset metadata row, and that values round-trip through database/sql
// as time.Time without losing the stored instant.
func TestRelationalTimestampsUseNativePostgresTypes(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()

	columns := []struct {
		table  string
		column string
	}{
		{"apps", "updated_at"},
		{"stats", "updated_at"},
		{"app_stats", "updated_at"},
		{"stats_meta", "updated_at"},
		{"runtime_image_selections", "selected_at"},
	}
	for _, column := range columns {
		var dataType string
		if err := c.db.QueryRowContext(ctx, `
			SELECT data_type FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`,
			column.table, column.column).Scan(&dataType); err != nil {
			t.Fatalf("read %s.%s type: %v", column.table, column.column, err)
		}
		if dataType != "timestamp with time zone" {
			t.Errorf("%s.%s type = %q, want timestamp with time zone", column.table, column.column, dataType)
		}
	}

	want := time.Date(2026, 11, 8, 23, 45, 12, 345678000, time.UTC)
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO apps (name, data, updated_at) VALUES ('timestamp-probe', '{}', $1)`, want); err != nil {
		t.Fatalf("insert apps timestamp: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO stats (node_id, instance, data, updated_at) VALUES ('timestamp-probe', '-', '{}', $1)`, want); err != nil {
		t.Fatalf("insert stats timestamp: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO app_stats (app_name, node_id, data, updated_at) VALUES ('timestamp-probe', 'timestamp-probe', '{}', $1)`, want); err != nil {
		t.Fatalf("insert app_stats timestamp: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`UPDATE stats_meta SET updated_at = $1 WHERE id = 'global'`, want); err != nil {
		t.Fatalf("update stats_meta timestamp: %v", err)
	}
	if _, err := c.db.ExecContext(ctx, `
		INSERT INTO runtime_image_selections
			(runtime_id, source_image, digest, generation, selected_at)
		VALUES ('timestamp-probe', 'python:3.14-slim', 'sha256:`+strings.Repeat("a", 64)+`', 1, $1)`, want); err != nil {
		t.Fatalf("insert runtime selection timestamp: %v", err)
	}

	reads := []struct {
		name  string
		query string
	}{
		{"apps", `SELECT updated_at FROM apps WHERE name = 'timestamp-probe'`},
		{"stats", `SELECT updated_at FROM stats WHERE node_id = 'timestamp-probe'`},
		{"app_stats", `SELECT updated_at FROM app_stats WHERE app_name = 'timestamp-probe' AND node_id = 'timestamp-probe'`},
		{"stats_meta", `SELECT updated_at FROM stats_meta WHERE id = 'global'`},
		{"runtime_image_selections", `SELECT selected_at FROM runtime_image_selections WHERE runtime_id = 'timestamp-probe'`},
	}
	for _, read := range reads {
		var got time.Time
		if err := c.db.QueryRowContext(ctx, read.query).Scan(&got); err != nil {
			t.Fatalf("read %s timestamp as time.Time: %v", read.name, err)
		}
		if !got.Equal(want) {
			t.Errorf("%s timestamp = %s, want %s", read.name, got, want)
		}
	}
}
