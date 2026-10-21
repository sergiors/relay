package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v3"

	"relay/internal/worker"
)

// invokeEventFlag is the inline-JSON flag. It is the only explicit source; when
// it is not set the event is read from stdin, but only when stdin is NOT an
// interactive terminal, so a bare invocation never blocks waiting for a typed
// line.
const invokeEventFlag = "event"

// appInvokeCommand builds `relay app invoke NAME`: it resolves an
// operator-supplied event (inline JSON or piped/redirected stdin), then asks the
// running worker over its query socket to execute every event rule of NAME that
// matches the event. It never opens the state database, never touches Redis or
// Docker, and never instantiates a runtime in the CLI process: the live runner
// and its warm runtime pool live only in the worker, and the socket is the
// single seam between them. There is deliberately no offline fallback — a manual
// invocation must run against the live worker.
//
// The command is deliberately thin: argument/flag parsing and payload validation
// happen here (so a bad payload is reported without dialing), and the semantic
// work (matching and execution) belongs entirely to the worker's runner.
func appInvokeCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "invoke",
		Usage:     "Invoke an app's matching event handlers",
		UsageText: "relay app invoke NAME [--event JSON]",
		Description: "Run every event rule of NAME whose pattern matches the supplied event, " +
			"synchronously, on the RUNNING worker's live runtime pool. Supply the event as " +
			"--event JSON or stdin: --event takes precedence when set and stdin is then " +
			"never read; otherwise a piped or redirected JSON object is read (an interactive " +
			"terminal is never read). The worker reports how many handlers it invoked. " +
			"Manual invocation never writes stream/retry/DLQ state.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "name", Min: 1, Max: 1},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  invokeEventFlag,
				Usage: "inline JSON object to match and invoke (takes precedence over stdin)",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("app invoke: too many arguments", 2)
			}
			name := cmd.StringArgs("name")[0]

			rawEvent, err := resolveInvokeEvent(
				cmd.String(invokeEventFlag), cmd.IsSet(invokeEventFlag),
				cmd.Reader, stdinIsTerminal(cmd.Reader),
			)
			if err != nil {
				return err
			}

			invoked, err := worker.InvokeApp(ctx, deps.SocketPath, name, rawEvent)
			if err != nil {
				return fmt.Errorf("app invoke: %w", err)
			}
			switch invoked {
			case 0:
				fmt.Fprintln(cmd.Writer, "No matching handlers")
			case 1:
				fmt.Fprintln(cmd.Writer, "Invoked 1 handler")
			default:
				fmt.Fprintf(cmd.Writer, "Invoked %d handlers\n", invoked)
			}
			return nil
		},
	}
}

// noInvokeEventError is the shared usage error for a missing payload. It names
// both accepted sources so a bare invocation is self-explanatory.
func noInvokeEventError() error {
	return cli.Exit(
		"app invoke: no event provided (use --event JSON or pipe a JSON object on stdin)",
		2,
	)
}

// resolveInvokeEvent resolves the invocation event bytes from the explicit
// --event flag or from stdin. eventSet comes from cli.Command.IsSet so an
// explicitly empty flag value is a usage error rather than a silent fall-through
// to stdin. stdinTerminal tells the stdin path whether the reader is an
// interactive terminal, so a bare invocation errors with guidance instead of
// blocking on a typed line.
//
// --event is authoritative: when it is set, stdin is not inspected at all.
// Detecting a "simultaneously piped" payload would require reading to EOF, which
// can block indefinitely for an open stream (for example a host TTY bridged by
// `docker compose exec -T`) and could fail a perfectly valid explicit event on a
// stdin read error. Clear precedence is worth more than guessing at ambiguity.
//
// The payload MUST be a JSON object: the matcher is defined over map[string]any,
// so arrays, scalars, and null are rejected here with a clear message rather than
// being sent to the worker to fail opaquely. The returned bytes are the validated
// raw JSON, forwarded verbatim over the socket.
func resolveInvokeEvent(
	eventJSON string, eventSet bool,
	stdin io.Reader, stdinTerminal bool,
) (json.RawMessage, error) {
	if eventSet {
		raw := []byte(eventJSON)
		if len(bytes.TrimSpace(raw)) == 0 {
			return nil, noInvokeEventError()
		}
		return parseInvokeEvent(raw)
	}

	if stdinTerminal {
		// An interactive terminal with no piped payload: reading would block
		// waiting for a typed line, which is not what a bare `invoke` means.
		// Fail fast with guidance instead.
		return nil, noInvokeEventError()
	}

	if stdin == nil {
		return nil, noInvokeEventError()
	}

	raw, err := io.ReadAll(stdin)
	if err != nil {
		return nil, fmt.Errorf("app invoke: read stdin: %w", err)
	}

	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, noInvokeEventError()
	}

	return parseInvokeEvent(raw)
}

// parseInvokeEvent validates raw as a JSON object and returns the verbatim
// bytes. Malformed JSON and non-object values are reported separately so the
// operator can tell a syntax error from a shape error.
func parseInvokeEvent(raw []byte) (json.RawMessage, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("app invoke: invalid event JSON: %w", err)
	}
	switch value.(type) {
	case map[string]any:
		return json.RawMessage(raw), nil
	case nil:
		return nil, fmt.Errorf("app invoke: event must be a JSON object, not null")
	default:
		return nil, fmt.Errorf("app invoke: event must be a JSON object")
	}
}

// stdinIsTerminal reports whether r is an interactive terminal. It is a
// best-effort check used only to avoid blocking on a typed read: a non-*os.File
// reader (tests, an embedding host) is never a terminal, and an *os.File is a
// terminal when it is a character device (the classic portable test, no
// platform-specific ioctl and no extra dependency).
func stdinIsTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
