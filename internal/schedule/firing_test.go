package schedule

import (
	"testing"
	"time"
)

// TestIsFiringBasic pins exact-minute membership: a real whole-minute firing is
// accepted, while a wrong minute/hour/second and any non-whole-second instant (a
// fractional second) are rejected. A nonzero whole second for a normal UTC cron
// is rejected too, but by membership (the schedule does not fire at that second),
// not by a structural alignment rule.
func TestIsFiringBasic(t *testing.T) {
	utc := time.UTC
	tests := []struct {
		name string
		at   time.Time
		want bool
	}{
		{"exact firing", time.Date(2026, 7, 1, 3, 0, 0, 0, utc), true},
		{"fractional second within the firing minute", time.Date(2026, 7, 1, 3, 0, 0, 500_000_000, utc), false},
		{"nonzero whole second within the firing minute", time.Date(2026, 7, 1, 3, 0, 30, 0, utc), false},
		{"wrong minute", time.Date(2026, 7, 1, 3, 1, 0, 0, utc), false},
		{"wrong hour", time.Date(2026, 7, 1, 4, 0, 0, 0, utc), false},
		{"wrong second past the boundary", time.Date(2026, 7, 1, 3, 0, 1, 0, utc), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IsFiring("0 3 * * *", utc, tt.at)
			if err != nil {
				t.Fatalf("IsFiring: %v", err)
			}
			if got != tt.want {
				t.Fatalf("IsFiring(%v) = %v, want %v", tt.at, got, tt.want)
			}
		})
	}
}

// TestIsFiringEveryMinute pins the wildcard case.
func TestIsFiringEveryMinute(t *testing.T) {
	utc := time.UTC
	for _, at := range []time.Time{
		time.Date(2026, 7, 1, 0, 0, 0, 0, utc),
		time.Date(2026, 7, 1, 12, 34, 0, 0, utc),
		time.Date(2026, 7, 1, 23, 59, 0, 0, utc),
	} {
		got, err := IsFiring("* * * * *", utc, at)
		if err != nil || !got {
			t.Fatalf("IsFiring(* * * * *, %v) = (%v,%v), want true", at, got, err)
		}
	}
	// A sub-minute instant is not a firing of a wildcard minute schedule.
	got, err := IsFiring("* * * * *", utc, time.Date(2026, 7, 1, 12, 34, 30, 0, utc))
	if err != nil || got {
		t.Fatalf("IsFiring(sub-minute) = (%v,%v), want false", got, err)
	}
}

// TestIsFiringTimezone pins that the configured IANA timezone (and its offset)
// is applied: the same absolute instant is a firing in one zone and not another.
func TestIsFiringTimezone(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	// 08:00 Rome (CEST, UTC+2) is 06:00Z.
	firing := time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC)

	got, err := IsFiring("0 8 * * *", rome, firing)
	if err != nil || !got {
		t.Fatalf("IsFiring(08:00 Rome at 06:00Z) = (%v,%v), want true", got, err)
	}
	// The same instant expressed in UTC is NOT 08:00, so a UTC schedule at 08:00
	// does not fire here.
	got, err = IsFiring("0 8 * * *", time.UTC, firing)
	if err != nil || got {
		t.Fatalf("IsFiring(08:00 UTC at 06:00Z) = (%v,%v), want false", got, err)
	}
}

// TestIsFiringDSTSpringForward pins that DST is delegated to the parser: on the
// Europe/Rome spring-forward day (2026-03-29, 02:00→03:00 local), a 03:00 local
// schedule fires at 01:00Z, and 03:00 UTC is not that firing.
func TestIsFiringDSTSpringForward(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	// 03:00 CEST (UTC+2) on the spring-forward day is 01:00Z.
	firing := time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC)
	got, err := IsFiring("0 3 * * *", rome, firing)
	if err != nil || !got {
		t.Fatalf("IsFiring(03:00 Rome, spring-forward) = (%v,%v), want true", got, err)
	}
	// 02:00 local does not exist that day; the schedule is 03:00, so 00:00Z
	// (01:00 local) is not a firing.
	if got, err := IsFiring("0 3 * * *", rome, time.Date(2026, 3, 29, 0, 0, 0, 0, time.UTC)); err != nil || got {
		t.Fatalf("IsFiring(01:00 Rome, spring-forward) = (%v,%v), want false", got, err)
	}
}

// TestIsFiringUnparseable pins that a malformed cron is surfaced as an error,
// never silently treated as a non-firing.
func TestIsFiringUnparseable(t *testing.T) {
	if _, err := IsFiring("not a cron", time.UTC, time.Now()); err == nil {
		t.Fatal("expected an error for an unparseable cron")
	}
}

// TestContainsMatchesIsFiring pins the two entry points agree, and that Contains
// rejects a fractional-second instant outright while a nonzero whole-second
// instant is decided by the schedule's own membership (for a normal UTC cron it
// does not fire).
func TestContainsMatchesIsFiring(t *testing.T) {
	sch, err := ParseCron("0 3 * * *", time.UTC)
	if err != nil {
		t.Fatalf("ParseCron: %v", err)
	}
	at := time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)
	if !Contains(sch, at) {
		t.Fatal("Contains(real firing) = false")
	}
	if Contains(sch, at.Add(time.Minute)) {
		t.Fatal("Contains(non-firing) = true")
	}
	if Contains(sch, at.Add(500*time.Millisecond)) {
		t.Fatal("Contains(fractional-second instant) = true; it must not alias the firing second")
	}
	if Contains(sch, at.Add(30*time.Second)) {
		t.Fatal("Contains(nonzero whole-second instant) = true; membership must reject it")
	}
}

// TestIsFiringHistoricalOffsetNonzeroUTCSecond pins the historical-offset edge
// case the purely structural rule got wrong: a legitimate local-minute firing can
// have a nonzero UTC second when the zone's offset itself carries a seconds
// component. Europe/Rome local mean time is UTC+00:49:56, so 12:00:00 local is
// 11:10:04Z. Contains/IsFiring must accept that whole-second instant because the
// schedule's own Next produces it, while the same schedule still rejects an
// ordinary nonzero-second instant.
func TestIsFiringHistoricalOffsetNonzeroUTCSecond(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	// 1866 predates Rome's adoption of whole-hour standard time (1893-11-01), so
	// the local offset is the LMT +00:49:56 and 12:00 local is 11:10:04Z.
	firing := time.Date(1866, 6, 1, 11, 10, 4, 0, time.UTC)
	if _, off := firing.In(rome).Zone(); off%60 == 0 {
		t.Fatalf("Europe/Rome offset at %v is %ds (whole minute); test needs a seconds-bearing historical offset", firing, off)
	}
	got, err := IsFiring("0 12 * * *", rome, firing)
	if err != nil {
		t.Fatalf("IsFiring: %v", err)
	}
	if !got {
		t.Fatalf("IsFiring(12:00 Rome at %v, LMT offset) = false, want true", firing)
	}
	// The neighboring second is not a firing: membership, not a blunt
	// whole-second rule, is what accepts the LMT instant.
	if got, err := IsFiring("0 12 * * *", rome, firing.Add(time.Second)); err != nil || got {
		t.Fatalf("IsFiring(%v) = (%v,%v), want false", firing.Add(time.Second), got, err)
	}
}
