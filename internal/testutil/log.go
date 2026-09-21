package testutil

import (
	"io"
	"log/slog"
)

// DiscardLogger returns a leveled logger that discards all output at the most
// verbose (DEBUG) level, so nothing is filtered. It is for tests that construct
// production components whose log output is irrelevant.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
