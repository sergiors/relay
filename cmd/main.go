package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"relay/internal/function"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
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
			prepared = append(prepared, runner.NewUnavailable(fn))
			continue
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

	if err := consumer.EnsureGroup(ctx); err != nil {
		logger.Fatalf("ensure consumer group: %v", err)
	}

	run := runner.New(prepared, logger)

	// Watch /functions and reconcile functions live: rebuild changed images,
	// discover new ones, drop removed ones. The runner's registry is swapped
	// atomically behind the snapshots the consumer already uses.
	reconciler := reconciler.New(
		reconciler.Config{Root: function.Dir},
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
