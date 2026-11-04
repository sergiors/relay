package cli

import (
	"context"
	"fmt"
	"strings"

	"relay/internal/dlq"
)

const (
	// dlqIDLength is the full stable entry ID length: SHA-256 rendered as
	// lowercase hexadecimal by dlq.NewID.
	dlqIDLength = 64
	// dlqIDShortLength is how many leading ID characters `dlq ls` shows and
	// the minimum prefix length accepted by inspect, replay, and rm, so an
	// operator can copy the visible ID directly.
	dlqIDShortLength = 10
)

// dlqIDPrefixFinder is the narrow administrative capability of a DLQ store to
// resolve a short entry-ID prefix. It is deliberately separate from dlq.Store:
// the stream processor never needs prefix lookup, so keeping it narrow avoids
// forcing every store implementation and fake to grow an admin method.
type dlqIDPrefixFinder interface {
	// FindDLQEntryIDsByPrefix returns up to two candidate entry IDs from an
	// index range starting at prefix, in ascending ID order. The range is
	// evaluated under the database collation, so callers must verify that each
	// candidate byte-starts with prefix before acting on it.
	FindDLQEntryIDsByPrefix(ctx context.Context, prefix string) ([]string, error)
}

// findDLQEntry resolves id (a full ID or unique prefix) and loads the entry. An
// unknown or ambiguous ID is a clear error.
func findDLQEntry(ctx context.Context, store dlq.Store, id string) (dlq.Entry, error) {
	id, err := resolveDLQID(ctx, store, id)
	if err != nil {
		return dlq.Entry{}, err
	}
	entry, ok, err := store.GetDLQEntry(ctx, id)
	if err != nil {
		return dlq.Entry{}, err
	}
	if !ok {
		return dlq.Entry{}, fmt.Errorf("unknown DLQ entry %q", id)
	}
	return entry, nil
}

// resolveDLQID validates a user-supplied entry identifier and returns the full
// stable ID used by the exact-ID store operations. A full-length ID is used
// as-is; a shorter prefix must match exactly one stored entry. Candidates are
// byte-verified before use, so a collation-widened range can never resolve to a
// non-matching entry. No match, ambiguity, and malformed input are reported
// before any replay or removal is attempted.
func resolveDLQID(ctx context.Context, store dlq.Store, input string) (string, error) {
	if err := validateDLQID(input); err != nil {
		return "", err
	}
	if len(input) == dlqIDLength {
		return input, nil
	}

	finder, ok := store.(dlqIDPrefixFinder)
	if !ok {
		return "", fmt.Errorf("DLQ entry prefix lookup is not supported by this store")
	}
	ids, err := finder.FindDLQEntryIDsByPrefix(ctx, input)
	if err != nil {
		return "", err
	}

	// The store's range is evaluated under the database collation. Verify every
	// candidate byte-starts with the requested prefix before it can reach an
	// exact-ID operation. If any candidate fails, the two-row limit may also
	// have hidden a real match behind it, so refuse the whole result rather than
	// guess which entry the operator meant. No-match is safer than a wrong
	// deletion or replay.
	matches := make([]string, 0, len(ids))
	for _, id := range ids {
		if strings.HasPrefix(id, input) {
			matches = append(matches, id)
		}
	}
	if len(matches) != len(ids) {
		return "", fmt.Errorf(
			"no DLQ entry safely matches ID prefix %q; use the full 64-character ID",
			input)
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no DLQ entry matches ID prefix %q", input)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf(
			"ambiguous DLQ entry ID prefix %q: matches %s and %s; use more characters",
			input, matches[0], matches[1])
	}
}

// validateDLQID enforces the entry-identifier grammar: 10 to 64 lowercase
// hexadecimal characters. The minimum is the prefix length shown by `dlq ls`,
// and only the canonical lowercase form is accepted so the ID is literal and
// unambiguous.
func validateDLQID(id string) error {
	if len(id) < dlqIDShortLength || len(id) > dlqIDLength {
		return fmt.Errorf(
			"invalid DLQ entry ID %q: use %d to %d lowercase hexadecimal "+
				"characters or a unique prefix",
			id, dlqIDShortLength, dlqIDLength)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf(
				"invalid DLQ entry ID %q: use only lowercase hexadecimal characters (0-9, a-f)",
				id)
		}
	}
	return nil
}

// shortDLQID returns the visible ls prefix. Stored IDs are full-length, but a
// guard keeps a malformed short fixture from panicking the listing.
func shortDLQID(id string) string {
	if len(id) <= dlqIDShortLength {
		return id
	}
	return id[:dlqIDShortLength]
}
