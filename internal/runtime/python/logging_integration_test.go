//go:build integration

// Application logging tests for the split Python bootstrap: the bootstrap
// installs ONE logging format before any handler module is imported, the root
// logger keeps Python's own default level, and the level stays open for the
// application's own configuration. Records are plain native container logs on
// the default logging stream, beside the lifecycle lines on stdout. See
// bootstrap.py; the Node mirror lives in the node package.
package python

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// requireFormattedRecord asserts out carries exactly one logging record in
// the bootstrap-configured format —
// "%(asctime)s %(levelname)s [%(name)s] %(message)s" with logging's default
// asctime rendering — for the given level, logger name, and message.
func requireFormattedRecord(t *testing.T, out, level, logger, message string) {
	t.Helper()
	pattern := regexp.MustCompile(
		`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} ` + level +
			` \[` + regexp.QuoteMeta(logger) + `\] ` + regexp.QuoteMeta(message) + `$`)
	matches := 0
	for _, line := range strings.Split(out, "\n") {
		if pattern.MatchString(line) {
			matches++
		}
	}
	if matches != 1 {
		t.Errorf("records matching the configured format %q = %d, want exactly 1; output:\n%s",
			pattern.String(), matches, out)
	}
}

// requireNoMarker asserts none of the markers appears in out.
func requireNoMarker(t *testing.T, stream, out string, markers ...string) {
	t.Helper()
	for _, marker := range markers {
		if strings.Contains(out, marker) {
			t.Errorf("%s must not carry %q; output:\n%s", stream, marker, out)
		}
	}
}

// awaitStderrContains waits until the child's stderr carries frag within
// bound: the os/exec copy goroutine filling the stderr collector runs
// independently of the result frame's arrival on the channel, so expected
// stderr markers are awaited instead of read immediately. Waiting for an
// invocation's LAST stderr write also anchors the absence checks for earlier
// writes on the same stream (pipe writes are ordered).
func awaitStderrContains(t *testing.T, p *pyProc, frag string) {
	t.Helper()
	bound := 2 * time.Second
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if strings.Contains(p.stderrString(), frag) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("stderr never carried %q within %v; stderr:\n%s", frag, bound, p.stderrString())
}

// TestPythonRuntimeLoggingConfiguredFormat pins the configured format: a
// handler that never configures logging itself gets its records rendered by
// the ONE format the bootstrap installs before any handler import —
// timestamp, level name, logger name, message — on the default logging
// stream, and never on stdout (the lifecycle/print stream).
func TestPythonRuntimeLoggingConfiguredFormat(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import logging

logger = logging.getLogger(__name__)


def warn(event, context):
    logger.warning("py-format-warning")
`)
	p := startPython(t, dir)

	r := p.invoke(t, "handler.warn", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	awaitStderrContains(t, p, "py-format-warning")
	requireFormattedRecord(t, p.stderrString(), "WARNING", "handler", "py-format-warning")
	requireNoMarker(t, "stdout", r.Out, "py-format-warning")
}

// TestPythonRuntimeLoggingDefaultLevelIsWarning pins the default level: with
// no application configuration, the root logger's Python default (WARNING)
// filters DEBUG and INFO records away entirely while a WARNING record flows
// in the configured format.
func TestPythonRuntimeLoggingDefaultLevelIsWarning(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import logging

logger = logging.getLogger(__name__)


def emit(event, context):
    logger.debug("py-default-debug")
    logger.info("py-default-info")
    logger.warning("py-default-warning")
`)
	p := startPython(t, dir)

	r := p.invoke(t, "handler.emit", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	awaitStderrContains(t, p, "py-default-warning")
	requireFormattedRecord(t, p.stderrString(), "WARNING", "handler", "py-default-warning")
	requireNoMarker(t, "stdout", r.Out, "py-default-info", "py-default-debug")
	requireNoMarker(t, "stderr", p.stderrString(), "py-default-info", "py-default-debug")
}

// TestPythonRuntimeLoggingApplicationLevelOverride pins the application
// override: the level is left to the application's own configuration, so a
// handler module raising the root level to INFO at import gets its INFO
// records emitted — still in the bootstrap-configured format — while DEBUG
// stays filtered below the overridden level.
func TestPythonRuntimeLoggingApplicationLevelOverride(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import logging

logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)


def emit(event, context):
    logger.debug("py-override-debug")
    logger.info("py-override-info")
`)
	p := startPython(t, dir)

	r := p.invoke(t, "handler.emit", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	// INFO is the last emit; once it arrives, a leaked earlier DEBUG write
	// would be observable on the same stream too.
	awaitStderrContains(t, p, "py-override-info")
	requireFormattedRecord(t, p.stderrString(), "INFO", "handler", "py-override-info")
	requireNoMarker(t, "stdout", r.Out, "py-override-debug")
	requireNoMarker(t, "stderr", p.stderrString(), "py-override-debug")
}

// TestPythonRuntimeLoggingApplicationOwnHandler pins the application-owned
// handler contract: the bootstrap's basicConfig runs before any handler
// import and never overrides what the application configures afterwards — a
// named logger with its own StreamHandler, formatter, level, and
// propagation disabled gets its records on its own stream in its own format,
// and never leaks into the bootstrap-configured root handler on stderr.
func TestPythonRuntimeLoggingApplicationOwnHandler(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import logging
import sys

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("custom %(levelname)s %(name)s %(message)s"))

logger = logging.getLogger("app.custom")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def emit(event, context):
    logger.debug("py-custom-debug")
    logger.warning("py-custom-warning")
    print("py-custom-stderr-anchor", file=sys.stderr)
`)
	p := startPython(t, dir)

	r := p.invoke(t, "handler.emit", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	for _, want := range []string{
		"custom DEBUG app.custom py-custom-debug",
		"custom WARNING app.custom py-custom-warning",
	} {
		count := 0
		for _, line := range strings.Split(r.Out, "\n") {
			if line == want {
				count++
			}
		}
		if count != 1 {
			t.Errorf("lines equal to %q = %d, want exactly 1 (app handler on stdout, custom format); stdout:\n%s",
				want, count, r.Out)
		}
	}

	// The app-owned records stay off the bootstrap's root handler on stderr:
	// await the handler's plain stderr anchor (printed after the emits, so a
	// leaked record would already have arrived), then require both markers
	// absent there, and no line in the bootstrap-configured format on either
	// stream.
	bootstrapFormat := regexp.MustCompile(
		`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (DEBUG|WARNING) \[app\.custom\] py-custom-(debug|warning)$`)
	awaitStderrContains(t, p, "py-custom-stderr-anchor")
	requireNoMarker(t, "stderr", p.stderrString(), "py-custom-debug", "py-custom-warning")
	for stream, out := range map[string]string{"stdout": r.Out, "stderr": p.stderrString()} {
		for _, line := range strings.Split(out, "\n") {
			if bootstrapFormat.MatchString(line) {
				t.Errorf("%s line %q must not use the bootstrap-configured format for the app-owned logger", stream, line)
			}
		}
	}
}

// TestPythonRuntimeLoggingEffectiveLoggerLevel pins the effective level a
// handler observes: an unconfigured child logger inherits the root's default
// WARNING, and after the application raises the root level the same child
// reports INFO — the effective level tracks the application's configuration
// across warm invocations.
func TestPythonRuntimeLoggingEffectiveLoggerLevel(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import logging

child = logging.getLogger("app.sub")


def effective(event, context):
    print("effective=" + logging.getLevelName(child.getEffectiveLevel()))


def raise_to_info(event, context):
    logging.getLogger().setLevel(logging.INFO)
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.effective", `{}`, nil)
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	if !strings.Contains(r1.Out, "effective=WARNING") {
		t.Errorf("default effective level out = %q, want effective=WARNING", r1.Out)
	}

	r2 := p.invoke(t, "handler.raise_to_info", `{}`, nil)
	if !r2.OK {
		t.Fatalf("level override invoke failed: %s", r2.Err)
	}

	r3 := p.invoke(t, "handler.effective", `{}`, nil)
	if !r3.OK {
		t.Fatalf("second effective invoke failed: %s", r3.Err)
	}
	if !strings.Contains(r3.Out, "effective=INFO") {
		t.Errorf("effective level after override out = %q, want effective=INFO", r3.Out)
	}
	if strings.Contains(r3.Out, "effective=WARNING") {
		t.Errorf("effective level after override must no longer report WARNING: %q", r3.Out)
	}
}
