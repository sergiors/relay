package schedule

import (
	"time"

	robfigcron "github.com/robfig/cron/v3"
)

// ParseCron parses a template cron expression with the SAME parser configuration
// gocron/v2 uses for a 5-field cron job (gocron's defaultCron.IsValid calls
// cron.ParseStandard with the job's CRON_TZ-prefixed spec), so Relay's
// occurrence derivation and gocron's firing agree by construction and cannot
// drift. Only the accepted schedule grammar reaches here: the 6-field (seconds)
// form and `@every` are rejected before registration (see internal/cron and
// internal/app).
//
// It lives beside Occurrence (rather than in internal/cron) because both the
// publisher side (internal/cron) and the consumer side (internal/runner, via
// IsFiring) must derive firings with identical semantics, and internal/schedule
// is the shared, dependency-light owner of occurrence identity. internal/cron
// already imports this package, so no cycle is introduced.
func ParseCron(expr string, loc *time.Location) (robfigcron.Schedule, error) {
	return robfigcron.ParseStandard("CRON_TZ=" + loc.String() + " " + expr)
}

// wholeSecondAligned reports whether t is positioned on a whole second, i.e. has
// no sub-second component. Cron evaluation is whole-second granularity: the
// schedule's Next only ever produces whole-second instants, so a fractional-second
// timestamp can never be a real firing and is rejected structurally. A whole
// second is deliberately NOT required to have UTC Second()==0: historical IANA
// offsets can themselves carry a seconds component (Europe/Rome local mean time
// is UTC+00:49:56), so a legitimate local-minute firing can land on a nonzero UTC
// second. Whether a whole-second instant actually fires is decided by the
// schedule itself in Contains, never by an absolute-second test here.
func wholeSecondAligned(t time.Time) bool {
	return t.Nanosecond() == 0
}

// Contains reports whether at is a real firing of sch. A fractional-second
// instant is rejected outright (the schedule only ever produces whole-second
// instants, so it cannot be a firing). A whole-second instant is checked by asking
// the schedule for its first activation strictly after at-1s: a schedule fires at
// at exactly when that activation is at. Membership is therefore the schedule's
// own, following the parser's timezone and DST handling; it correctly accepts a
// local-minute firing whose UTC representation carries a nonzero second in a
// historical zone, while a nonzero UTC second in a normal zone simply fails the
// membership test.
func Contains(sch robfigcron.Schedule, at time.Time) bool {
	if !wholeSecondAligned(at) {
		return false
	}
	t := at.UTC()
	next := sch.Next(t.Add(-time.Second))
	return !next.IsZero() && next.Equal(t)
}

// IsFiring reports whether at is a real cron firing of expr evaluated in loc.
// A parse error is returned rather than treated as a non-firing, so a caller can
// distinguish a malformed schedule configuration from a forged/non-firing
// occurrence instant.
func IsFiring(expr string, loc *time.Location, at time.Time) (bool, error) {
	sch, err := ParseCron(expr, loc)
	if err != nil {
		return false, err
	}
	return Contains(sch, at), nil
}
