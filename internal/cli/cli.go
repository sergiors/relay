// Package cli builds the relay command tree on github.com/urfave/cli/v3. It
// owns parsing and native help only; the long-running runtime lifecycle lives
// in internal/worker (see `relay start`). Errors are RETURNED, never printed or
// turned into os.Exit here: cmd/main.go prints the returned error exactly once
// and owns the process exit. Run receives the context from main, so signal
// cancellation propagates into ctx-aware paths like the worker health query.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/urfave/cli/v3"

	"relay/internal/config"
	"relay/internal/processlock"
	"relay/internal/state"
	"relay/internal/stream"
	"relay/internal/worker"
)

// DLQStore is the narrow DLQ view the `relay dlq` commands consume. The
// Redis-backed *stream.RedisDLQStore satisfies it; tests inject a fake, so the
// command/presentation layer never touches Redis directly. Close releases
// whatever the store owns.
type DLQStore interface {
	List(ctx context.Context) ([]stream.DLQEntry, error)
	Get(ctx context.Context, id string) (stream.DLQEntry, bool, error)
	Delete(ctx context.Context, id string) (bool, error)
	Close() error
}

// OpenDLQStore resolves and opens the DLQ store for one `relay dlq` command,
// returning the store and a cleanup func. It is an app seam so the command
// tree never reads environment variables or constructs Redis clients itself,
// and tests (or an embedding host) can inject a fake without mutable globals.
// Production wires openRedisDLQStore, which follows the same Redis configuration
// conventions as `relay start` (config.LoadRedisConfig + config.RedisOptions)
// but needs no PostgreSQL state DSN, because the DLQ is Redis-only.
type OpenDLQStore func(logger *slog.Logger) (store DLQStore, cleanup func(), err error)

// StartRun is the narrow seam `relay start` delegates to for the long-running
// Relay lifecycle. It blocks until the runtime stops and returns any
// startup/runtime failure unchanged, which cmd/main.go prints (exactly once)
// while it owns process exit. Production wires worker.Run; tests inject a fake
// runner directly, so the command's dispatch is observed without a package-level
// hook and without launching the runtime.
type StartRun func(logger *slog.Logger) error

// OpenState opens the state database for the administrative commands (`app`,
// `stats`). It is a lazy seam: it is called only by the commands that need the
// database, so every other command works with no DATABASE_URL configured. It
// returns the handle and a cleanup func. Production wires openPostgresState
// (config.LoadDatabaseURL + state.Open); tests (or an embedding host) inject an
// isolated handle.
type OpenState func(ctx context.Context) (*state.State, func(), error)

// Dependencies carries the CLI's injectable process seams: the filesystem
// locations plus the state-database and DLQ-store openers the administrative
// commands need. Production wires the immutable defaults via
// DefaultDependencies, while tests (and any embedding host) construct their own.
// It deliberately replaces package-level mutable variables, so a command's paths
// and connection come from its construction rather than from global state.
type Dependencies struct {
	// OpenState opens the PostgreSQL state database the administrative commands
	// (app, stats) use. It is read-mostly, not read-only: the read paths
	// (app inspect, stats) only read, while `stats reset` writes (zeroing
	// the cumulative counters in place). Production: openPostgresState.
	OpenState OpenState
	// SocketPath is the live worker control socket: `relay health` dials it to
	// ask whether the running worker is healthy (worker readiness plus live
	// dependency health), `relay app inspect` dials it for the live
	// runtime-pool gauges, `relay app invoke` and `relay dlq replay` dial
	// it to run against the live runtime, and a running `relay stats reset`
	// dials it to reset the worker's statistics. Production: worker.SocketPath.
	SocketPath string
	// LockPath is the process-level lock file `relay start` holds for its
	// lifetime; its parent directory is created before the lock is acquired.
	// Production: processlock.DefaultPath.
	LockPath string
	// Start runs the long-running Relay lifecycle `relay start` delegates to.
	// Production: worker.Run.
	Start StartRun
	// OpenDLQ opens the DLQ store for the `relay dlq` commands. It is called
	// lazily, only by those commands, so every other command works with no Redis
	// configured. Production: openRedisDLQStore (config.LoadRedisConfig +
	// config.RedisOptions + stream.NewRedisDLQStore).
	OpenDLQ OpenDLQStore
}

// DefaultDependencies returns the fixed production seams: the filesystem
// locations, the worker lifecycle, the config-driven state- and DLQ-store
// openers. The referenced constants stay immutable application conventions;
// there are no environment overrides or setters.
func DefaultDependencies() Dependencies {
	return Dependencies{
		OpenState:  openPostgresState,
		SocketPath: worker.SocketPath,
		LockPath:   processlock.DefaultPath,
		Start:      worker.Run,
		OpenDLQ:    openRedisDLQStore,
	}
}

// openPostgresState is the production OpenState: it resolves the state DSN from
// DATABASE_URL (without requiring any REDIS_* configuration, so the admin
// commands work against a database-only environment) and opens a bounded
// PostgreSQL handle. A missing DATABASE_URL or an unreachable database is a
// clear error the command returns; the CLI's read/render paths never panic on a
// missing database.
func openPostgresState(ctx context.Context) (*state.State, func(), error) {
	dsn, err := config.LoadDatabaseURL()
	if err != nil {
		return nil, nil, fmt.Errorf("database: %w", err)
	}
	st, err := state.Open(ctx, state.Options{
		DSN:             dsn,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, nil)),
		MaxOpenConns:    config.DefaultDatabaseMaxOpenConns,
		MaxIdleConns:    config.DefaultDatabaseMaxIdleConns,
		ConnMaxLifetime: config.DefaultDatabaseConnMaxLifetime,
		ConnMaxIdleTime: config.DefaultDatabaseConnMaxIdleTime,
		ConnectTimeout:  config.DefaultDatabaseConnectTimeout,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open state: %w", err)
	}
	return st, func() { _ = st.Close() }, nil
}

// New constructs the fully configured root relay command. logger is the process
// logger relay start hands to the worker; writer is set on the root and
// propagated to subcommands (urfave inherits nil Reader/Writer/ErrWriter from
// the parent); deps supplies the injected filesystem locations. New does not
// execute the CLI, create a context, or exit.
//
// The silent ExitErrHandler MUST stay set: urfave's default HandleExitCoder
// would print the error and call os.Exit itself, but printing belongs exactly
// once to cmd/main.go.
func New(logger *slog.Logger, writer io.Writer, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:            "relay",
		Usage:           "Relay event-driven app runner",
		HideHelpCommand: true,
		HideVersion:     true,
		Writer:          writer,
		ExitErrHandler:  func(context.Context, *cli.Command, error) {},
		Commands: []*cli.Command{
			startCommand(logger, deps),
			appCommand(deps),
			appsCommand(deps),
			dlqCommand(logger, deps),
			secretCommand(),
			gitCommand(),
			statsCommand(deps),
			healthCommand(deps),
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if !cmd.Args().Present() {
				return cli.ShowAppHelp(cmd)
			}

			return fmt.Errorf(
				"relay: unknown command: relay %s\n\nRun 'relay --help' for more information",
				cmd.Args().First(),
			)
		},
	}
}

// Run executes the root command tree against args. args includes the program
// name; urfave's parser consumes it. Run neither prints the error nor exits:
// it returns the tree's error to cmd/main.go, which prints and exits once. It
// wires the production filesystem defaults; tests construct New with temp paths
// instead of calling Run.
func Run(ctx context.Context, args []string, logger *slog.Logger) error {
	return New(logger, os.Stdout, DefaultDependencies()).Run(ctx, args)
}
