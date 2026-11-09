//go:build integration

package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAppTransitionUsesOneClockInstant(t *testing.T) {
	ctx := context.Background()
	name := "clock-transition"
	fn := fnFor(t, name, mustTemplate(t, twoHandlerTmpl))
	wantNow := time.Date(2026, 11, 10, 14, 15, 16, 456789000,
		time.FixedZone("test offset", 5*60*60+45*60))
	wantUTC := wantNow.UTC()
	wantRFC3339 := wantUTC.Format(time.RFC3339)

	tests := []struct {
		name  string
		write func(*State, FencingToken) error
	}{
		{
			name: "reconcile success",
			write: func(st *State, token FencingToken) error {
				wrote, err := st.RecordReconcileSuccess(ctx, name, "image", "fingerprint", wantNow, fn, token)
				if err == nil && !wrote {
					return errors.New("write did not land")
				}
				return err
			},
		},
		{
			name: "reconcile failure",
			write: func(st *State, token FencingToken) error {
				wrote, err := st.RecordReconcileFailure(ctx, name, errors.New("reconcile failed"), token)
				if err == nil && !wrote {
					return errors.New("write did not land")
				}
				return err
			},
		},
		{
			name: "service failure",
			write: func(st *State, token FencingToken) error {
				wrote, err := st.RecordServiceFailure(ctx, name, errors.New("service failed"), token)
				if err == nil && !wrote {
					return errors.New("write did not land")
				}
				return err
			},
		},
		{
			name: "invalid desired",
			write: func(st *State, _ FencingToken) error {
				token, err := st.RecordInvalidDesired(ctx, name, errors.New("invalid desired"))
				if err == nil && token <= 0 {
					return errors.New("write returned no fencing token")
				}
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := openTestState(t)
			token := seedFencingToken(t, st, fn)
			clockReads := 0
			st.nowFn = func() time.Time {
				clockReads++
				if clockReads == 1 {
					return wantNow
				}
				// A second read would make the relational and snapshot values diverge.
				return wantNow.Add(time.Hour)
			}

			if err := tt.write(st, token); err != nil {
				t.Fatalf("transition write: %v", err)
			}
			if clockReads != 1 {
				t.Fatalf("clock reads = %d, want exactly one", clockReads)
			}

			_, updatedAt, _ := rawAppData(t, st, name)
			if !updatedAt.Equal(wantUTC) {
				t.Errorf("apps.updated_at = %s, want instant %s", updatedAt, wantUTC)
			}
			var lastReconcileAt string
			if err := st.db.QueryRowContext(ctx,
				`SELECT data->>'last_reconcile_at' FROM apps WHERE name = $1`, name,
			).Scan(&lastReconcileAt); err != nil {
				t.Fatalf("read JSON last_reconcile_at: %v", err)
			}
			if lastReconcileAt != wantRFC3339 {
				t.Errorf("JSON last_reconcile_at = %q, want UTC RFC3339 %q", lastReconcileAt, wantRFC3339)
			}
			detail, ok := st.GetApp(name)
			if !ok {
				t.Fatal("app missing after transition")
			}
			if detail.UpdatedAt != wantRFC3339 {
				t.Errorf("presented updated_at = %q, want UTC RFC3339 %q", detail.UpdatedAt, wantRFC3339)
			}
		})
	}
}
