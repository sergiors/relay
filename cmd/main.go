package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/function"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/stream"
)

// mustEnv reads a required environment variable during startup, before any
// other work, and fails fast if it is unset or empty.
func mustEnv(logger *log.Logger, key string) string {
	v := os.Getenv(key)
	if v == "" {
		logger.Fatalf("missing required environment variable: %s", key)
	}
	return v
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Minimal hand-rolled dispatch (no CLI framework): only the single
	// `function` subcommand family is recognized; everything else is the daemon.
	if len(os.Args) > 1 && os.Args[1] == "function" {
		os.Exit(runFunctionCommand(os.Args[2:]))
	}

	runDaemon(logger)
}

// runDaemon is the original main body: startup wiring, then the health server,
// reconciler, and stream consumer. It blocks in Consume until cancelled.
func runDaemon(logger *log.Logger) {
	cfg := struct {
		redisAddr   string
		redisStream string
		redisGroup  string
		redisCons   string
	}{
		redisAddr:   mustEnv(logger, "REDIS_ADDR"),
		redisStream: mustEnv(logger, "REDIS_STREAM"),
		redisGroup:  mustEnv(logger, "REDIS_GROUP"),
		redisCons:   mustEnv(logger, "REDIS_CONSUMER"),
	}

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer client.Close()

	loader := function.NewLoader(function.Dir, logger)
	functions, err := loader.Load()
	if err != nil {
		logger.Fatalf("load functions: %v", err)
	}
	logger.Printf("loaded %d function(s) from %s", len(functions), function.Dir)

	// The state database is a read-only local state view (see internal/state).
	// It is NOT the source of truth and never drives matching or building. Open
	// recreates a missing DB; RebuildFromFS repopulates an empty one from
	// /functions; then startup discovery records each loaded function. All
	// state errors are logged and non-fatal — Relay runs without the state DB
	// if it is broken.
	st, err := state.Open(state.DBPath)
	if err != nil {
		logger.Printf("state: open (continuing without): %v", err)
		st = nil
	}
	if st != nil {
		defer st.Close()
		if err := st.RebuildFromFS(function.Dir); err != nil {
			logger.Printf("state: rebuild from fs (continuing): %v", err)
		}
		for _, fn := range functions {
			st.RecordDiscovered(fn)
		}
	}

	// Prepare (build) each function's image. A function whose image cannot be
	// built is marked unavailable so the runner skips it; the rest continue.
	manager, err := runtime.NewManager(logger)
	if err != nil {
		logger.Fatalf("runtime: %v", err)
	}
	defer manager.Close()
	preparedCount := 0
	var prepared []*runner.PreparedFunction
	for _, fn := range functions {
		p, err := manager.Prepare(context.Background(), fn)
		if err != nil {
			logger.Printf("function %q: prepare: %v", fn.Name, err)
			if st != nil {
				st.RecordReconcileFailure(fn.Name, err)
			}
			prepared = append(prepared, runner.NewUnavailable(fn))
			continue
		}
		if st != nil {
			// Fingerprint may differ from the discovery-time value if content
			// changed between load and build; the state DB records the final state.
			fp, fperr := function.Fingerprint(fn.Dir)
			if fperr != nil {
				logger.Printf("function %q: fingerprint: %v", fn.Name, fperr)
				fp = ""
			}
			st.RecordReconcileSuccess(fn.Name, p.Image, fp, time.Now(), fn)
		}
		prepared = append(prepared, runner.NewPrepared(fn, p, manager))
		preparedCount++
	}
	logger.Printf("prepared %d function(s)", preparedCount)

	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:   client,
		Stream:   cfg.redisStream,
		Group:    cfg.redisGroup,
		Consumer: cfg.redisCons,
		Log:      logger,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Health endpoint for container orchestrators, fixed at port 80. It is an
	// application convention, not configuration.
	hs := newHealthServer(consumer.Healthy)
	hs.start(ctx)

	if err := consumer.EnsureGroup(ctx); err != nil {
		logger.Fatalf("ensure consumer group: %v", err)
	}

	run := runner.New(prepared, logger)

	// Watch /functions and reconcile functions live: rebuild changed images,
	// discover new ones, drop removed ones. The runner's registry is swapped
	// atomically behind the snapshots the consumer already uses.
	reconciler := reconciler.New(
		reconciler.Config{Root: function.Dir, State: st},
		run.Registry(),
		manager,
		logger,
	)
	for _, fn := range functions {
		reconciler.Seed(fn)
	}
	logger.Printf("watching %s for changes", function.Dir)

	// Runs in its own goroutine and stops when ctx is cancelled.
	go reconciler.Start(ctx)

	logger.Printf("consuming stream %q as group %q consumer %q",
		cfg.redisStream,
		cfg.redisGroup,
		cfg.redisCons,
	)
	if err := consumer.Consume(ctx, run.Handle); err != nil {
		logger.Fatalf("consume: %v", err)
	}
	logger.Printf("shutdown complete")
}
