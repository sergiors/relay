package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
)

// healthTimeout bounds each dependency probe so a hung daemon or Redis does not
// stall the healthcheck indefinitely.
const healthTimeout = 2 * time.Second

// runHealthCommand implements the `relay health` subcommand. It checks the two
// dependencies the daemon needs at startup — Redis connectivity and Docker
// daemon connectivity — and exits 0 when both are reachable, 1 otherwise. It
// never starts consumption, loads functions, builds images, or touches the
// state database; it only creates clients and pings. Exit codes:
//
//	0  healthy
//	1  a dependency is unavailable
func runHealthCommand() int {
	logger := log.New(os.Stderr, "", 0)
	redisAddr := mustEnv(logger, "REDIS_ADDR")

	redisCheck := func() error {
		cli := redis.NewClient(&redis.Options{Addr: redisAddr})
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
		defer cancel()
		if err := cli.Ping(ctx).Err(); err != nil {
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
		ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
		defer cancel()
		if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
			return fmt.Errorf("docker unavailable: %w", err)
		}
		return nil
	}

	return checkHealth(redisCheck, dockerCheck)
}

// checkHealth runs the two dependency checks in a fixed order (redis then
// docker) and reports the first failure. It is separated from the real
// implementation so unit tests can inject fakes without Redis or Docker.
func checkHealth(redisCheck, dockerCheck func() error) int {
	if err := redisCheck(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := dockerCheck(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println("healthy")
	return 0
}
