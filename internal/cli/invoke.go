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

// invokeEventFlag is the inline-JSON flag, and invokeFileFlag is the JSON-file
// flag. They are mutually exclusive; when neither is set the event is read from
// stdin (only when stdin is NOT an interactive terminal, so a bare invocation
// never blocks waiting for a typed line).
const (
	invokeEventFlag = "event"
	invokeFileFlag  = "file"
)

// functionInvokeCommand builds `relay function invoke NAME`: it resolves an
// operator-supplied event (inline JSON, a file, or piped stdin), then asks the
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
func functionInvokeCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "invoke",
		Usage:     "Invoke a function's matching event handlers",
		UsageText: "relay function invoke NAME [--event JSON | --file PATH]",
		Description: "Run every event rule of NAME whose pattern matches the supplied event, " +
			"synchronously, on the RUNNING worker's live runtime pool. Supply the event as " +
			"--event JSON, --file PATH, or piped on stdin. The worker reports how many " +
			"handlers it invoked. Manual invocation never writes stream/retry/DLQ state.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "name", Min: 1, Max: 1},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  invokeEventFlag,
				Usage: "inline JSON object to match and invoke",
			},
			&cli.StringFlag{
				Name:  invokeFileFlag,
				Usage: "path to a JSON object file to match and invoke",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("function invoke: too many arguments", 2)
			}
			name := cmd.StringArgs("name")[0]

			rawEvent, err := resolveInvokeEvent(
				cmd.String(invokeEventFlag), cmd.IsSet(invokeEventFlag),
				cmd.String(invokeFileFlag), cmd.IsSet(invokeFileFlag),
				cmd.Reader, stdinIsTerminal(cmd.Reader),
			)
			if err != nil {
				return err
			}

			invoked, err := worker.InvokeFunction(ctx, deps.SocketPath, name, rawEvent)
			if err != nil {
				return fmt.Errorf("function invoke: %w", err)
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

// resolveInvokeEvent resolves the invocation event bytes from the mutually
// exclusive sources: the --event flag, the --file flag, or stdin. eventSet and
// fileSet come from cli.Command.IsSet so an explicit empty flag value is a
// usage error rather than a silent fall-through to stdin. stdinTerminal tells
// the stdin path whether the reader is an interactive terminal, so a bare
// invocation errors with guidance instead of blocking on a typed line.
//
// The payload MUST be a JSON object: the matcher is defined over map[string]any,
// so arrays, scalars, and null are rejected here with a clear message rather than
// being sent to the worker to fail opaquely. The returned bytes are the validated
// raw JSON, forwarded verbatim over the socket.
func resolveInvokeEvent(
	eventJSON string, eventSet bool,
	filePath string, fileSet bool,
	stdin io.Reader, stdinTerminal bool,
) (json.RawMessage, error) {
	if eventSet && fileSet {
		return nil, cli.Exit("function invoke: --event and --file are mutually exclusive", 2)
	}

	var raw []byte
	switch {
	case eventSet:
		raw = []byte(eventJSON)
	case fileSet:
		b, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("function invoke: read --file: %w", err)
		}
		raw = b
	default:
		if stdinTerminal {
			// An interactive terminal with no piped payload: reading would block
			// waiting for a typed line, which is not what a bare `invoke` means.
			// Fail fast with guidance instead.
			return nil, cli.Exit(
				"function invoke: no event provided (use --event, --file, or pipe a JSON object on stdin)",
				2,
			)
		}
		b, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("function invoke: read stdin: %w", err)
		}
		raw = b
	}

	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, cli.Exit(
			"function invoke: no event provided (use --event, --file, or pipe a JSON object on stdin)",
			2,
		)
	}

	// Unmarshal into map[string]any is the object check: an array, string,
	// number, or bool fails, and a literal null decodes to a nil map.
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("function invoke: event must be a JSON object: %w", err)
	}
	if event == nil {
		return nil, fmt.Errorf("function invoke: event must be a JSON object, not null")
	}
	return json.RawMessage(raw), nil
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
