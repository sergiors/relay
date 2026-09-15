package cli

import (
	"sync"

	"github.com/lnquy/cron"
)

// cronDescriptor is the package-level, lazily-constructed descriptor used to
// render human-readable cron descriptions for `relay function inspect`. It is
// built once with 24-hour formatting so the description never shows AM/PM.
// ToDescription is stateless and safe for concurrent use after construction, so
// a single shared descriptor is reused across all schedule rows.
var cronDescriptor = sync.OnceValue(func() *cron.ExpressionDescriptor {
	d, err := cron.NewDescriptor(cron.Use24HourTimeFormat(true))
	if err != nil {
		// NewDescriptor can only fail transiently (locale loaders); fall back to
		// a nil descriptor so defaultDescribeCron reports (false) and inspect
		// renders the raw expression unchanged (never failing).
		return nil
	}
	return d
})

// describeCronFunc is the description resolver used by printInspect. It is a
// package var (not const) solely so tests can inject a failing implementation to
// exercise the graceful fallback; production always uses defaultDescribeCron.
var describeCronFunc = defaultDescribeCron

// defaultDescribeCron returns a human-readable description of expr in 24-hour
// time, or false if no description is available. It is display-only: it never
// returns an error and callers must never fail because of description
// generation. The raw cron expression remains the source of truth.
func defaultDescribeCron(expr string) (string, bool) {
	d := cronDescriptor()
	if d == nil {
		return "", false
	}
	desc, err := d.ToDescription(expr, cron.Locale_en)
	if err != nil {
		return "", false
	}
	// A blank description is indistinguishable from a failed one.
	if desc == "" {
		return "", false
	}
	return desc, true
}
