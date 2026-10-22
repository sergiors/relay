package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/urfave/cli/v3"

	"relay/internal/worker"
)

// invokeStdinArg is the positional marker that explicitly selects stdin as the
// event source. It is the ONLY way `app invoke` reads stdin: a bare NAME never
// reads it implicitly, so the command cannot block on an open stream.
const invokeStdinArg = "-"

// appInvokeCommand builds `relay app invoke NAME [JSON|-]`: it resolves an
// operator-supplied event (a positional JSON object or `-` for stdin), then asks
// the running worker over its query socket to execute every event rule of NAME
// that matches the event. It never opens the state database, never touches Redis
// or Docker, and never instantiates a runtime in the CLI process: the live
// runner and its warm runtime pool live only in the worker, and the socket is
// the single seam between them. There is deliberately no offline fallback — a
// manual invocation must run against the live worker.
//
// The command is deliberately thin: argument parsing and payload validation
// happen here (so a bad payload is reported without dialing), and the semantic
// work (matching and execution) belongs entirely to the worker's runner.
func appInvokeCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "invoke",
		Usage:     "Invoke an app's matching event handlers",
		UsageText: "relay app invoke NAME [JSON|-]",
		Description: "Run every event rule of NAME whose pattern matches the supplied event, " +
			"synchronously, on the RUNNING worker's live runtime pool. Pass the event as a " +
			"positional JSON object, or use - to read it from stdin (for example a redirected " +
			"file). A bare NAME is a usage error; stdin is never read implicitly. The worker " +
			"reports how many handlers it invoked. Manual invocation never writes " +
			"stream/retry/DLQ state.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "name", Min: 1, Max: 1, UsageText: "NAME"},
			&cli.StringArgs{Name: "event", Min: 0, Max: 1, UsageText: "[JSON|-]"},
		},
		StopOnNthArg: stopOnFirstPositional(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("app invoke: too many arguments", 2)
			}
			name := cmd.StringArgs("name")[0]

			rawEvent, err := resolveInvokeEvent(cmd.StringArgs("event"), cmd.Reader)
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
// both accepted forms so a bare invocation is self-explanatory.
func noInvokeEventError() error {
	return cli.Exit(
		"app invoke: no event provided (pass a JSON object as an argument or use - to read it from stdin)",
		2,
	)
}

// resolveInvokeEvent resolves the invocation event bytes from the optional
// positional argument. eventArgs is empty when the argument was omitted, in which
// case the command is a usage error and stdin is NOT read: implicit stdin would
// let a bare invocation block on an open stream. The literal "-" explicitly
// selects stdin and reads it to EOF; every other value is treated as inline JSON.
//
// The payload MUST be a JSON object: the matcher is defined over map[string]any,
// so arrays, scalars, and null are rejected here with a clear message rather than
// being sent to the worker to fail opaquely. The returned bytes are the validated
// raw JSON, forwarded verbatim over the socket.
func resolveInvokeEvent(eventArgs []string, stdin io.Reader) (json.RawMessage, error) {
	if len(eventArgs) == 0 {
		return nil, noInvokeEventError()
	}

	raw := []byte(eventArgs[0])
	if eventArgs[0] == invokeStdinArg {
		if stdin == nil {
			return nil, noInvokeEventError()
		}
		data, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("app invoke: read stdin: %w", err)
		}
		raw = data
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

// stopOnFirstPositional makes urfave stop flag parsing after the first
// positional argument, preserving a trailing "-" and anything after it as
// positionals. Without it, urfave swallows the remaining arguments after a lone
// "-", so `NAME - extra` would silently drop `extra` instead of rejecting it.
func stopOnFirstPositional() *int {
	n := 1
	return &n
}
