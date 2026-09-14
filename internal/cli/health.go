package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v3"

	"relay/internal/config"
)

// healthTimeout bounds each dependency probe so a hung daemon or Redis does not
// stall the healthcheck indefinitely.
const healthTimeout = 2 * time.Second

// healthCommand builds the `relay health` subcommand. It checks the two
// dependencies the worker needs at startup — Redis connectivity and Docker
// daemon connectivity — and exits 0 when both are reachable, 1 otherwise. It
// never starts consumption, loads functions, builds images, or touches the
// state database; it only creates clients and pings.
func healthCommand(logger *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "health",
		Usage: "Check Relay dependencies",
		Description: "Check that the dependencies Relay needs at startup — Redis connectivity " +
			"and the Docker daemon — are reachable. Exits 0 when both are healthy, 1 otherwise.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("health: too many arguments", 2)
			}
			return runHealthCommand(ctx, cmd.Writer, logger)
		},
	}
}

// runHealthCommand implements the `relay health` subcommand body. Dependency
// probes derive their timeouts from the incoming ctx rather than a fresh
// background context, so callers control how long a hung dependency may stall.
func runHealthCommand(ctx context.Context, w io.Writer, logger *slog.Logger) error {
	// The health command loads the full configuration via config.Load(logger),
	// the same entry point the worker's `relay start` uses. A missing required
	// variable (REDIS_URI/REDIS_STREAM/REDIS_GROUP) or an unresolvable
	// hostname therefore exits via config's fail-fast logging, matching the
	// worker's behavior: the healthcheck fails hard rather than probing with
	// nothing (or half) configured. This IS a behavior change — `relay health`
	// now requires REDIS_STREAM and REDIS_GROUP to be set too, even though the
	// health command only pings Redis — and it is intentional, since health's
	// job is to verify the worker's actual startup configuration, not a
	// hand-picked subset. A malformed DSN is surfaced by RedisOptions below.
	redisCheck := func() error {
		cfg := config.Load(logger)
		redisOpts, err := config.RedisOptions(cfg.RedisURI)
		if err != nil {
			return fmt.Errorf("redis config: %w", err)
		}
		cli := redis.NewClient(redisOpts)
		defer cli.Close()
		pctx, cancel := context.WithTimeout(ctx, healthTimeout)
		defer cancel()
		if err := cli.Ping(pctx).Err(); err != nil {
			return fmt.Errorf("redis unavailable: %w", err)
		}
		return nil
	}

	dockerCheck := func() error {
		cli, err := client.New(client.FromEnv)
		if err != nil {
			return fmt.Errorf("docker unavailable: %w", err)
		}
		defer cli.Close()
		pctx, cancel := context.WithTimeout(ctx, healthTimeout)
		defer cancel()
		if _, err := cli.Ping(pctx, client.PingOptions{}); err != nil {
			return fmt.Errorf("docker unavailable: %w", err)
		}
		return nil
	}

	return checkHealth(w, redisCheck, dockerCheck)
}

// checkHealth runs the two dependency checks in a fixed order (redis then
// docker) and reports the first failure. It is separated from the real
// implementation so unit tests can inject fakes without Redis or Docker.
//
// On success it writes "healthy" to w. On failure it returns the wrapped error
// (never printing it here — the root ExitErrHandler prints it once).
func checkHealth(w io.Writer, redisCheck, dockerCheck func() error) error {
	if err := redisCheck(); err != nil {
		return err
	}
	if err := dockerCheck(); err != nil {
		return err
	}
	fmt.Fprintln(w, "healthy")
	return nil
}
