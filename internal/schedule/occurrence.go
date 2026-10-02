package schedule

import (
	"encoding/json"
	"fmt"
	"time"
)

// Occurrence is one logical firing of a cron schedule: the app, the
// schedule's stable name, the handler it invokes, and the scheduled instant it
// belongs to. Two workers evaluating the same cron tick produce the same
// Occurrence, so they contend on exactly one publish-if-new.
//
// Schedule is the schedule's stable configuration identity (template
// `schedules[].name`); Handler is the current handler that schedule invokes.
// Identity is (app, schedule name, scheduled instant): a schedule keeps its
// identity across a handler change, and two schedules sharing a handler are
// distinct occurrences. Handler travels in the envelope so the consumer can
// resolve the schedule by name and invoke its CURRENT handler (a name's handler
// may have changed since publication, in which case the fresh handler runs —
// there is no other handler to run for that schedule).
type Occurrence struct {
	App         string
	Schedule    string
	Handler     string
	ScheduledAt time.Time // the scheduled instant, UTC
}

// ID returns the deterministic occurrence identity:
// "schedule:<app>:<schedule name>:<scheduled_at RFC3339 UTC>".
// ScheduledAt is normalized to UTC and truncated to the second so DST offsets
// and timezone representation never change the ID: the configured timezone
// affects when the schedule fires, never the identity.
func (o Occurrence) ID() string {
	// Normalize to UTC and truncate to the second. Truncate works on the
	// absolute instant since the epoch, so it is stable across workers; the
	// timezone the cron was evaluated in is deliberately dropped here. Dropping
	// sub-second components means worker-side evaluation jitter (whether two
	// workers read the due instant a few milliseconds apart) never splits an
	// occurrence into two IDs. Seconds themselves are NOT truncated: the
	// scheduler stamps the schedule's own due instant, which is whole-second.
	// That whole second can legitimately be nonzero (a historical IANA offset
	// carrying a seconds component), so truncating it would merge distinct
	// firings; two distinct whole seconds stay distinct.
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	return "schedule:" + o.App + ":" + o.Schedule + ":" + t.Format(time.RFC3339)
}

// Payload returns the handler payload exactly as before the stream change:
// {"source":"relay.schedule","scheduled_at":"<RFC3339 UTC>"}. Only these two
// fields are exposed to the app; internal coordination fields (app,
// schedule, handler, occurrence_id) are never part of the handler payload.
func (o Occurrence) Payload() []byte {
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	b, _ := json.Marshal(map[string]string{
		"source":       "relay.schedule",
		"scheduled_at": t.Format(time.RFC3339),
	})
	return b
}

// Envelope returns the stream message body (JSON): the scheduled occurrence plus
// its derived occurrence_id, written into the stream by the publisher on
// PublishOccurrence. The handler payload and the envelope are separate: the
// envelope is what the consumer decodes to route the message; the handler only
// ever sees Payload().
func (o Occurrence) Envelope() ([]byte, error) {
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	return json.Marshal(map[string]string{
		"source":        "relay.schedule",
		"app":           o.App,
		"schedule":      o.Schedule,
		"handler":       o.Handler,
		"scheduled_at":  t.Format(time.RFC3339),
		"occurrence_id": o.ID(),
	})
}

// ClaimKind classifies a decoded event against the schedule envelope:
//
//   - NotScheduleClaim: the event does not carry the reserved
//     source=="relay.schedule" marker, so it is an ordinary external event and
//     must be left untouched by the schedule path.
//   - ValidScheduleClaim: the event is a structurally valid schedule claim
//     (app/schedule/handler non-empty, a parseable scheduled_at, and an
//     occurrence_id that matches the derived identity). It is routed to the
//     schedule runner.
//   - InvalidScheduleClaim: the event carries the reserved schedule marker but
//     is structurally incomplete or inconsistent (a missing/empty/non-string
//     field, an unparseable or sub-second scheduled_at, or an occurrence_id
//     that does not match the derived identity). It is a malformed claim that
//     must NEVER fall through to ordinary event matching and be executed/ACKed
//     as unmatched; the stream routes it to the DLQ as a non-retryable failure.
type ClaimKind int

const (
	// NotScheduleClaim is an ordinary event that does not claim schedule identity.
	NotScheduleClaim ClaimKind = iota
	// ValidScheduleClaim is a well-formed schedule occurrence envelope.
	ValidScheduleClaim
	// InvalidScheduleClaim carries the schedule marker but is malformed.
	InvalidScheduleClaim
)

// claimFields is the decoded envelope shape shared by ParseEnvelope (raw JSON)
// and ClassifyClaim (an already-decoded event map).
type claimFields struct {
	App          string
	Schedule     string
	Handler      string
	ScheduledAt  string
	OccurrenceID string
}

// validateClaim validates the envelope fields and returns the Occurrence. The
// error is descriptive and non-nil for every structural failure:
//
//   - app, schedule, handler, and occurrence_id must be present and non-empty;
//   - scheduled_at must parse as RFC3339 (normalized to UTC);
//   - scheduled_at must be whole-second. Cron evaluation is whole-second
//     granularity, so a fractional-second timestamp (e.g. "12:34:00.500Z") is a
//     malformed claim, not a real occurrence, even though Occurrence.ID would
//     truncate it to 12:34:00. Rejecting it here stops a sub-second instant from
//     masquerading as the second it falls in. A whole second is NOT required to
//     have UTC Second()==0: historical IANA offsets can carry a seconds
//     component, so a legitimate local-minute firing can have a nonzero UTC
//     second. Whether a whole-second instant is a real firing of a given
//     cron/timezone is decided semantically by Contains/IsFiring (the runner's
//     validateOccurrence), never structurally here;
//   - occurrence_id must equal the ID derived from app + schedule +
//     scheduled_at. The identity is DERIVED and cross-checked, never trusted:
//     a forged/mismatched id is rejected rather than silently recomputed.
//
// Occurrence.ID's canonical identity rules are deliberately unchanged: for a
// valid occurrence the identity is still the second-truncated RFC3339 form, and
// the sub-second truncation remains the normalization that keeps worker-side
// jitter from splitting an occurrence while whole seconds (which a historical
// timezone may legitimately produce) stay distinct.
func validateClaim(f claimFields) (Occurrence, error) {
	if f.App == "" {
		return Occurrence{}, fmt.Errorf("schedule claim: app is empty")
	}
	if f.Schedule == "" {
		return Occurrence{}, fmt.Errorf("schedule claim: schedule is empty")
	}
	if f.Handler == "" {
		return Occurrence{}, fmt.Errorf("schedule claim: handler is empty")
	}
	if f.OccurrenceID == "" {
		return Occurrence{}, fmt.Errorf("schedule claim: occurrence_id is empty")
	}
	t, err := time.Parse(time.RFC3339, f.ScheduledAt)
	if err != nil {
		return Occurrence{}, fmt.Errorf("schedule claim: scheduled_at: %w", err)
	}
	if !wholeSecondAligned(t) {
		return Occurrence{}, fmt.Errorf(
			"schedule claim: scheduled_at %s has a sub-second component; must be whole-second", f.ScheduledAt)
	}
	occ := Occurrence{App: f.App, Schedule: f.Schedule, Handler: f.Handler, ScheduledAt: t.UTC()}
	if got := occ.ID(); got != f.OccurrenceID {
		return Occurrence{}, fmt.Errorf("schedule claim: occurrence_id %q does not match derived identity %q", f.OccurrenceID, got)
	}
	return occ, nil
}

// ParseEnvelope decodes a stream envelope into its Occurrence, enforcing the
// full structural contract (see validateClaim), including that occurrence_id
// matches the derived identity. Identity is derived and cross-checked, never
// trusted from the wire, so a malformed or forged occurrence_id is rejected
// rather than redirecting an invocation.
func ParseEnvelope(raw string) (Occurrence, error) {
	var e struct {
		App          string `json:"app"`
		Schedule     string `json:"schedule"`
		Handler      string `json:"handler"`
		ScheduledAt  string `json:"scheduled_at"`
		OccurrenceID string `json:"occurrence_id"`
	}
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: %w", err)
	}
	return validateClaim(claimFields{
		App:          e.App,
		Schedule:     e.Schedule,
		Handler:      e.Handler,
		ScheduledAt:  e.ScheduledAt,
		OccurrenceID: e.OccurrenceID,
	})
}

// ClassifyClaim classifies a decoded event map against the schedule envelope.
// Schedule messages are recognized by the reserved "source" == "relay.schedule".
// A missing/empty source marker is NotScheduleClaim, so ordinary external events
// are untouched. Once the marker is present the event is a deliberate claim of
// schedule identity: a well-formed envelope is ValidScheduleClaim, and any
// structural failure (missing/empty/non-string app, schedule, handler, or
// occurrence_id; an unparseable or sub-second scheduled_at; or a
// mismatched occurrence_id) is InvalidScheduleClaim. The returned Occurrence is
// populated only for a valid claim; the error is populated only for an invalid
// one.
func ClassifyClaim(event map[string]any) (Occurrence, ClaimKind, error) {
	src, ok := event["source"].(string)
	if !ok || src != "relay.schedule" {
		return Occurrence{}, NotScheduleClaim, nil
	}
	f, err := claimFieldsFromEvent(event)
	if err != nil {
		return Occurrence{}, InvalidScheduleClaim, err
	}
	occ, err := validateClaim(f)
	if err != nil {
		return Occurrence{}, InvalidScheduleClaim, err
	}
	return occ, ValidScheduleClaim, nil
}

// claimFieldsFromEvent extracts the string envelope fields from a decoded event
// map. A missing or non-string field is a structural failure (the source marker
// is already known to be present), so an incomplete schedule claim is classified
// invalid rather than falling through to ordinary event matching.
func claimFieldsFromEvent(event map[string]any) (claimFields, error) {
	str := func(key string) (string, error) {
		v, ok := event[key].(string)
		if !ok {
			return "", fmt.Errorf("schedule claim: %s is missing or not a string", key)
		}
		return v, nil
	}
	var f claimFields
	var err error
	if f.App, err = str("app"); err != nil {
		return claimFields{}, err
	}
	if f.Schedule, err = str("schedule"); err != nil {
		return claimFields{}, err
	}
	if f.Handler, err = str("handler"); err != nil {
		return claimFields{}, err
	}
	if f.ScheduledAt, err = str("scheduled_at"); err != nil {
		return claimFields{}, err
	}
	if f.OccurrenceID, err = str("occurrence_id"); err != nil {
		return claimFields{}, err
	}
	return f, nil
}

// IsScheduleEvent reports whether a decoded event map is a STRUCTURALLY VALID
// schedule message and, when it is, returns the parsed Occurrence. It is the
// boolean convenience over ClassifyClaim: a marker-with-missing-fields event or
// a mismatched occurrence_id is NOT reported as a schedule event here (those are
// handled by the stream's ClassifyClaim-based non-retryable path). An ordinary
// user event carrying source==relay.schedule AND a complete, self-consistent
// envelope is a deliberate, acceptable claim of schedule identity; the
// occurrence id is re-derived and cross-checked, so a forged value cannot
// redirect the invocation.
func IsScheduleEvent(event map[string]any) (Occurrence, bool) {
	occ, kind, _ := ClassifyClaim(event)
	return occ, kind == ValidScheduleClaim
}
